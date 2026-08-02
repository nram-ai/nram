package server

import "net/http"

// SecurityHeadersMiddleware sets response hardening headers on every response:
// X-Content-Type-Options: nosniff, X-Frame-Options: DENY, and a Content-Security-
// Policy of frame-ancestors 'none'. The DENY and frame-ancestors pair are the
// legacy and modern clickjacking guards for the framing-sensitive surfaces (the
// admin UI, the OAuth consent page); the CSP sets only frame-ancestors, so it
// constrains nothing else the page loads.
//
// Two things are deliberately absent:
//
//   - No Strict-Transport-Security. nram is routinely run locally over plain
//     HTTP, and HSTS would pin such an origin to https and lock the operator out.
//     TLS termination is the deployment's responsibility, at the proxy or load
//     balancer in front of this server.
//   - No script-src / default-src. The served SPA and the API-reference page ship
//     inline bootstrap scripts, so a script-src policy would have to hash or
//     nonce them; that tightening is tracked on its own. A frame-ancestors-only
//     CSP is safe to apply unconditionally.
//
// Headers are set before calling next, so they are already in the header map if a
// downstream handler panics and the outer recovery middleware writes the 500.
func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", "frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}
