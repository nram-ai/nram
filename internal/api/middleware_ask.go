package api

import (
	"context"
	"net/http"
)

// AskGateMiddleware gates the ask endpoints on the ask.enabled feature flag.
// When the flag is off the endpoints return 404 Not Found, so the ask surface
// does not exist unless the feature is enabled. enabled is resolved per request
// (live), so toggling the flag takes effect without a server restart.
func AskGateMiddleware(enabled func(ctx context.Context) bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if enabled == nil || !enabled(r.Context()) {
				WriteError(w, ErrNotFound("not found"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
