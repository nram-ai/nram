// Package netutil holds small, dependency-free network primitives shared by
// packages that would otherwise each carry their own copy.
//
// It is a deliberate choice, not a forced one. internal/auth sits below
// internal/server and could host these, and internal/model is imported by
// both, but neither is a sensible owner: a host-classification primitive is
// not an auth concern and not a domain type. Keep this package to primitives
// with no nram imports; policy that acts on them belongs with its caller.
package netutil

import (
	"net"
	"net/netip"
	"strings"
)

// IsLoopbackHost reports whether addr refers to a loopback interface. addr may
// be a bare host ("localhost", "127.0.0.1", "::1", "[::1]") or a host:port pair
// ("localhost:3000", "127.0.0.1:3000", "[::1]:3000").
//
// Kept character-identical to server/http_localhost.go as of mcp-go v0.56.0 so
// a future SDK bump can be checked with a one-command diff. Do not "simplify"
// it without re-diffing upstream; each step handles a case the tests pin.
func IsLoopbackHost(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// addr might be a bare host without a port.
		host = strings.Trim(addr, "[]")
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return ip.IsLoopback()
}
