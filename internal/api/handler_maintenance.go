package api

import (
	"net/http"
	"time"

	"github.com/nram-ai/nram/internal/maintenance"
)

type maintenanceOp struct {
	Label   string `json:"label"`
	Message string `json:"message"`
	Since   string `json:"since"`
}

type maintenanceStatusResponse struct {
	Active     bool            `json:"active"`
	Operations []maintenanceOp `json:"operations"`
}

// NewMaintenanceStatusHandler returns an http.HandlerFunc that reports whether
// any performance-degrading maintenance operation (for example a SQLite VACUUM)
// is currently running. It is a pure in-memory read with no database access, so
// it stays responsive even while a VACUUM holds SQLite's exclusive lock. Safe
// with a nil registry, in which case it always reports inactive.
func NewMaintenanceStatusHandler(reg *maintenance.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := maintenanceStatusResponse{Operations: []maintenanceOp{}}
		if reg != nil {
			active, ops := reg.Snapshot()
			resp.Active = active
			for _, op := range ops {
				resp.Operations = append(resp.Operations, maintenanceOp{
					Label:   op.Label,
					Message: op.Message,
					Since:   op.Since.Format(time.RFC3339),
				})
			}
		}
		writeJSON(w, http.StatusOK, resp)
	}
}
