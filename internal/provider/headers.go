package provider

import "net/http"

// applyCustomHeaders sets user-configured custom headers on an outbound request.
// It is called after the built-in headers are set, so a custom header overrides
// the built-in one of the same name, EXCEPT for any name listed in reserved
// (matched case-insensitively via http.CanonicalHeaderKey). Reserved headers are
// protocol framing that must not change (Content-Type is always JSON; the
// Anthropic provider also reserves anthropic-version).
//
// An empty header value is skipped: a blank value means "unset" at this layer
// and never clears a built-in header. Preserve-on-blank semantics for stored
// config live in the admin store, not here.
func applyCustomHeaders(req *http.Request, headers map[string]string, reserved ...string) {
	if len(headers) == 0 {
		return
	}
	reservedSet := make(map[string]struct{}, len(reserved))
	for _, name := range reserved {
		reservedSet[http.CanonicalHeaderKey(name)] = struct{}{}
	}
	for name, value := range headers {
		if name == "" || value == "" {
			continue
		}
		if _, isReserved := reservedSet[http.CanonicalHeaderKey(name)]; isReserved {
			continue
		}
		req.Header.Set(name, value)
	}
}
