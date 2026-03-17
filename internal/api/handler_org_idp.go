package api

import (
	"net/http"
)

// NewOrgIdPHandler returns an http.HandlerFunc that handles IdP configuration
// for an organization. IdP is not yet implemented; GET returns a stub
// response and all write operations return 501 Not Implemented.
func NewOrgIdPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"provider":   "none",
				"configured": false,
			})
		default:
			writeJSON(w, http.StatusNotImplemented, map[string]string{
				"error": "IdP configuration not yet implemented",
			})
		}
	}
}
