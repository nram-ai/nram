package netutil

import (
	"net"
	"net/http"
	"strings"
)

// ClientIP returns the best-effort client IP for a request. It consults the
// forwarding headers a reverse proxy sets, in descending order of specificity:
// CF-Connecting-IP (Cloudflare, single client address), then X-Real-IP (single
// client address set by nginx and others), then the first X-Forwarded-For hop.
// When no forwarding header is present it falls back to the host portion of
// RemoteAddr. It returns "" when none yields an address.
//
// These forwarding headers are trusted unconditionally: an operator running a
// reverse proxy (nginx, a Cloudflare Tunnel, etc.) owns sanitizing them
// (stripping any client-supplied value and setting the real peer) before the
// request reaches nram. Callers that use the result as a rate-limit or
// anti-abuse key inherit that trust boundary. This is a single shared
// implementation so audit logging and the pre-auth throttle key on the same
// value.
func ClientIP(r *http.Request) string {
	// CF-Connecting-IP and X-Real-IP each carry a single client address.
	if cf := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); cf != "" {
		return cf
	}
	if xr := strings.TrimSpace(r.Header.Get("X-Real-IP")); xr != "" {
		return xr
	}
	// X-Forwarded-For is a comma-separated chain; the first hop is the client.
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
