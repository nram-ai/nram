package netutil

import (
	"net"
	"net/http"
	"strings"
)

// ClientIP returns the best-effort client IP for a request. It honors
// X-Forwarded-For (first hop) when present and falls back to the host portion
// of RemoteAddr. It returns "" when neither yields an address.
//
// The X-Forwarded-For hop is trusted unconditionally: an operator running a
// reverse proxy owns sanitizing that header (stripping any client-supplied
// value and appending the real peer) before the request reaches nram. Callers
// that use the result as a rate-limit or anti-abuse key inherit that trust
// boundary. This is a single shared implementation so audit logging and the
// pre-auth throttle key on the same value.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if before, _, ok := strings.Cut(xff, ","); ok {
			return strings.TrimSpace(before)
		}
		return strings.TrimSpace(xff)
	}
	if r.RemoteAddr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
