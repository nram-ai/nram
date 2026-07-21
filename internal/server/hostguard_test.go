package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/auth"
)

// The Host-rebinding guard is the one class of transport behavior the rest of
// the suite cannot cover: httptest serves on loopback AND sends a loopback
// Host, so both sides always agree and the check never fires. The tests below
// drive a real loopback listener while forging the Host header, which is what a
// same-host reverse proxy (proxy_pass http://127.0.0.1:PORT, Host preserved)
// looks like from the server's side. See HostGuardMiddleware for the policy.

// alwaysEnabled/neverEnabled stand in for the settings resolver.
func alwaysEnabled(context.Context) bool { return true }
func neverEnabled(context.Context) bool  { return false }

// okHandler is the downstream the guard either reaches or short-circuits.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

// postOverLoopback serves h on httptest's 127.0.0.1 listener and issues one
// POST to path carrying the given Host header, returning the status. The real
// net/http server is what populates http.LocalAddrContextKey, which is the
// input the guard reads; a synthetic request would not exercise it. path must
// name a route the handler actually serves, or group middleware never runs.
func postOverLoopback(t *testing.T, h http.Handler, host, path string) int {
	t.Helper()

	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = host // what the proxy forwards, independent of where we dialed
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestHostGuardMiddleware(t *testing.T) {
	cases := []struct {
		name          string
		enabled       func(context.Context) bool
		host          string
		wantForbidden bool
	}{
		// The upgrade-safety pin: at the default, a loopback connection carrying
		// a public Host must be served. If this fails, every same-host
		// reverse-proxy deployment is 403ing on every request.
		{"off allows proxied host", neverEnabled, "nram.ai", false},

		// The toggle actually does something when an operator turns it on.
		{"on rejects proxied host", alwaysEnabled, "nram.ai", true},

		// Control: with the guard on, a genuinely local client (loopback
		// connection, loopback Host) is unaffected. Without this, the rejection
		// case above could pass merely because the guard rejects everything.
		{"on allows loopback host", alwaysEnabled, "localhost:8674", false},

		// Defensive: a nil resolver returns the handler unwrapped rather than
		// firing. Production wires a real one; RouterConfig.HostGuard is the
		// documented way to run with no guard at all.
		{"nil resolver allows everything", nil, "nram.ai", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := postOverLoopback(t, HostGuardMiddleware(tc.enabled)(okHandler), tc.host, "/")
			if forbidden := got == http.StatusForbidden; forbidden != tc.wantForbidden {
				t.Fatalf("status %d (forbidden=%v), want forbidden=%v", got, forbidden, tc.wantForbidden)
			}
		})
	}
}

// TestHostGuardRunsBeforeAuth pins the middleware ordering in NewRouter. A DNS
// rebinding request is anonymous, so if the guard were registered after
// AuthMiddleware it would never fire: auth would turn every such request into a
// 401 first and the guard would be dead code. The two cases differ only in
// whether the guard is enabled, which isolates exactly that ordering.
func TestHostGuardRunsBeforeAuth(t *testing.T) {
	// A real AuthMiddleware, but its missing-header branch returns before any
	// validator or DB access, so nil dependencies are fine here.
	authMw := auth.NewAuthMiddleware(nil, nil, []byte("test-secret"), nil)

	statusFor := func(enabled func(context.Context) bool) int {
		t.Helper()
		router := NewRouter(RouterConfig{
			AuthMiddleware: authMw,
			HostGuard:      HostGuardMiddleware(enabled),
		}, Handlers{MCP: okHandler})
		return postOverLoopback(t, router, "nram.ai", "/mcp")
	}

	if got := statusFor(alwaysEnabled); got != http.StatusForbidden {
		t.Fatalf("guard on: got %d, want 403; the guard is running after AuthMiddleware", got)
	}
	// Control: with the guard off the same anonymous request reaches auth, so a
	// 403 above cannot be explained by anything other than the guard.
	if got := statusFor(neverEnabled); got != http.StatusUnauthorized {
		t.Fatalf("guard off: got %d, want 401 from AuthMiddleware", got)
	}
}

// requestWithLocalAddr builds a synthetic request carrying the local address
// net/http would have set from the accepted connection. A nil local means the
// key is absent, which is the unevaluatable case the guard fails open on.
func requestWithLocalAddr(local net.Addr) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://nram.ai/", nil)
	r.Host = "nram.ai"
	if local == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey, local))
}

func tcpAddr(ip string) net.Addr { return &net.TCPAddr{IP: net.ParseIP(ip), Port: 8674} }

// TestGuardSkipsSettingsForRoutableLocalAddr pins the predicate order
// documented on HostGuardMiddleware, which is load-bearing for cost rather than
// style: measured 13.5ns/0 allocs for the local-address check against 122ns/3
// allocs for the string path it replaced. Reorder the operands and the whole
// authenticated API silently starts contending on the settings mutex.
func TestGuardSkipsSettingsForRoutableLocalAddr(t *testing.T) {
	var resolves int
	counting := func(context.Context) bool { resolves++; return false }
	h := HostGuardMiddleware(counting)(okHandler)

	h.ServeHTTP(httptest.NewRecorder(), requestWithLocalAddr(tcpAddr("192.168.2.43")))
	if resolves != 0 {
		t.Fatalf("settings resolved %d times for a routable local address; want 0", resolves)
	}
	// Control: the lookup does happen once the connection is loopback, so a
	// passing test above cannot mean the resolver is simply never called.
	h.ServeHTTP(httptest.NewRecorder(), requestWithLocalAddr(tcpAddr("127.0.0.1")))
	if resolves != 1 {
		t.Fatalf("settings resolved %d times for a loopback local address; want 1", resolves)
	}
}

// TestLocalAddrIsLoopback covers the branches a listener test cannot reach: a
// routable local address would mean binding a machine-varying IP, and a request
// with no local address at all cannot be produced by a real server. It also
// pins the type switch, since the *net.TCPAddr fast path is what makes this
// cheap enough to gate the settings lookup on.
func TestLocalAddrIsLoopback(t *testing.T) {
	cases := []struct {
		name  string
		local net.Addr
		want  bool
	}{
		// Fail-open: unevaluatable, so the guard must not fire. Fail-closed
		// here would reject every unit test and any non-HTTP transport wired
		// in later.
		{"no connection info", nil, false},

		// The LAN case. A connection accepted on a routable address is never
		// rebinding, because the attack only reaches servers the victim's
		// browser can dial as localhost. If this regresses, turning the setting
		// on breaks every LAN and container deployment.
		{"routable TCP address", tcpAddr("192.168.2.43"), false},

		// The fast path the type switch exists for.
		{"loopback TCP address", tcpAddr("127.0.0.1"), true},

		// Non-TCP net.Addr falls back to string parsing; *net.UnixAddr is a
		// stand-in for any transport that is not TCP.
		{"non-TCP addr falls back to parsing", &net.UnixAddr{Name: "127.0.0.1:8674", Net: "unix"}, true},
		{"non-TCP addr, routable", &net.UnixAddr{Name: "192.168.2.43:8674", Net: "unix"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := localAddrIsLoopback(requestWithLocalAddr(tc.local)); got != tc.want {
				t.Fatalf("localAddrIsLoopback = %v, want %v", got, tc.want)
			}
		})
	}
}
