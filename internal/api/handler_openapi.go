package api

import (
	"net/http"

	"github.com/nram-ai/nram/docs"
)

// NewOpenAPIHandler returns an http.HandlerFunc that serves the embedded
// OpenAPI 3.1 specification (docs/openapi.yaml). It is mounted publicly at
// GET /openapi.yaml so API tooling can fetch the contract without
// authenticating and before initial setup completes.
func NewOpenAPIHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(docs.OpenAPISpec)
	}
}
