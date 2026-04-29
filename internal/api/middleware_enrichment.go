package api

import "net/http"

// EnrichmentGateMiddleware returns middleware that rejects requests with 503
// when the enrichment + dreaming gate is closed. The gate is closed whenever
// any of the three LLM provider slots — embedding, fact, entity — is
// unconfigured. The available function is called per request so a live
// provider reload (PUT /admin/providers/{slot}) opens or closes the gate
// without restarting the process.
func EnrichmentGateMiddleware(available func() bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if available == nil || !available() {
				WriteError(w, &APIError{
					Code:    "enrichment_unavailable",
					Message: "Configure embedding, fact, and entity providers to enable enrichment",
					Status:  http.StatusServiceUnavailable,
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
