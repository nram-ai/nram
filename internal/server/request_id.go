package server

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/provider"
)

// RequestIDHeader is the canonical header carrying the inbound correlation
// ID. Clients may set it explicitly (e.g., from an upstream tracing system);
// when absent, the middleware generates a fresh UUID so every request still
// gets a row-correlatable identifier.
const RequestIDHeader = "X-Request-ID"

// RequestIDMiddleware threads a request ID through the per-request context
// so that downstream provider calls record it on token_usage rows. It also
// echoes the ID back to the client in the response header, matching the
// shape of common request-tracing middleware.
//
// Wire this OUTSIDE auth so unauthenticated requests (login, OAuth probes)
// still get a correlation ID — useful for tying together token_usage rows
// emitted by setup-time provider probes.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := provider.WithRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
