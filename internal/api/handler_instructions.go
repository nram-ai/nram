package api

import (
	"io"
	"net/http"

	"github.com/nram-ai/nram/internal/instructions"
)

// NewInstructionsHandler returns an http.HandlerFunc that serves the canonical
// nram memory-usage guidance as plain text. It is mounted publicly at GET
// /instructions so any client can fetch the guidance without authenticating.
//
// The ?format= query selects the flavor: "claude" or "agents" (and the empty
// default) return the full markdown body, "cursor" returns the condensed rules
// copy. Any other value is rejected with 400 rather than silently defaulting.
func NewInstructionsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		format := r.URL.Query().Get("format")
		body, ok := instructions.Lookup(format)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, "unknown format: "+format+" (valid: claude, agents, cursor)")
			return
		}
		_, _ = io.WriteString(w, body)
	}
}
