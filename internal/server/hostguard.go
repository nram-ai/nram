package server

import (
	"context"
	"net"
	"net/http"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/netutil"
)

// HostGuardMiddleware rejects requests that arrived on a loopback local address
// while carrying a non-loopback Host header, which is the shape a DNS rebinding
// attack takes against a locally reachable server: a malicious site rebinds its
// own domain to 127.0.0.1 and drives the victim's browser at nram. Requests
// arriving on non-loopback addresses are never rejected, because the attack
// only reaches servers the browser can dial as localhost.
//
// enabled is resolved per request (server.host_rebinding_protection) so the
// toggle takes effect without a restart, matching AskGateMiddleware. It is off
// by default: the check rejects every same-host reverse proxy that forwards to
// 127.0.0.1 while preserving the public Host, which is the topology
// docs/quickstart.md recommends for TLS termination.
//
// This lives here rather than in the MCP handler because rebinding is a
// property of the connection, not of a protocol, so one guard covers the whole
// authenticated surface. mark3labs/mcp-go v0.56.0 applies its own equivalent
// check inside the Streamable HTTP handler; nram disables that (see
// server.WithDisableLocalhostProtection in internal/mcp) so the policy has
// exactly one home and one toggle.
//
// Predicate order is load-bearing for cost, not just style. This fronts every
// authenticated request, and resolving the setting takes a process-wide
// RWMutex and allocates a cache key. localAddrIsLoopback is allocation-free
// and answers false for any client that is not on the box, which is the
// overwhelming common case on the default 0.0.0.0 bind, so gating on it first
// keeps the shared lock off the hot path entirely.
func HostGuardMiddleware(enabled func(ctx context.Context) bool) func(http.Handler) http.Handler {
	if enabled == nil {
		// No resolver wired: hand back the handler unwrapped rather than pay a
		// frame and a nil test on every request.
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if localAddrIsLoopback(r) && enabled(r.Context()) && !netutil.IsLoopbackHost(r.Host) {
				api.WriteError(w, api.ErrForbidden("host header does not match the connection's local address"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// localAddrIsLoopback reports whether r was accepted on a loopback local
// address, reading http.LocalAddrContextKey as net/http populates it per
// connection.
//
// A request carrying no local address (a synthetic request, or a transport
// that does not populate the key) reports false, so the guard fails OPEN: the
// condition cannot be evaluated, and failing closed would reject every unit
// test and any non-HTTP transport wired in later.
//
// The *net.TCPAddr case is the whole point of the type switch: reading
// IP.IsLoopback() directly avoids formatting the address to a string only to
// parse it back, which is what makes this cheap enough to gate the settings
// lookup on.
func localAddrIsLoopback(r *http.Request) bool {
	switch a := r.Context().Value(http.LocalAddrContextKey).(type) {
	case *net.TCPAddr:
		return a.IP.IsLoopback()
	case net.Addr:
		return netutil.IsLoopbackHost(a.String())
	default:
		return false
	}
}
