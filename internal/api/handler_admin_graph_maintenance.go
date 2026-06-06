package api

import (
	"context"
	"log"
	"net/http"
	"strings"

	"github.com/nram-ai/nram/internal/service"
)

// GraphMaintainer runs system-wide knowledge-graph maintenance: reporting how
// many lost-provenance edges exist and reaping them (plus pruning the orphans
// they leave behind) on demand. Backed by LifecycleService.
type GraphMaintainer interface {
	GraphHealthStatus(ctx context.Context) (service.GraphHealth, error)
	RepairGraph(ctx context.Context) (service.GraphRepairResult, error)
}

// GraphMaintenanceConfig holds dependencies for the admin graph-maintenance
// handler. A nil Maintainer degrades gracefully (health reports zero, repair
// returns 503).
type GraphMaintenanceConfig struct {
	Maintainer GraphMaintainer
}

// NewAdminGraphMaintenanceHandler dispatches the admin graph-maintenance
// routes. Mounted under /v1/admin, which the router gates on
// RoleAdministrator, so no per-handler role check is needed:
//   - GET  /v1/admin/graph/health:  current lost-provenance edge count
//   - POST /v1/admin/graph/repair:  reap lost-provenance edges, recompute
//     mention counts, and prune dangling/orphaned graph rows
func NewAdminGraphMaintenanceHandler(cfg GraphMaintenanceConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sub := extractGraphMaintenanceSubPath(r.URL.Path)
		switch {
		case sub == "health" && r.Method == http.MethodGet:
			if cfg.Maintainer == nil {
				writeJSON(w, http.StatusOK, service.GraphHealth{})
				return
			}
			health, err := cfg.Maintainer.GraphHealthStatus(r.Context())
			if err != nil {
				log.Printf("admin graph health: %v", err)
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error": "failed to read graph health",
				})
				return
			}
			writeJSON(w, http.StatusOK, health)
		case sub == "repair" && r.Method == http.MethodPost:
			if cfg.Maintainer == nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "graph maintenance is unavailable",
				})
				return
			}
			res, err := cfg.Maintainer.RepairGraph(r.Context())
			if err != nil {
				log.Printf("admin graph repair: %v", err)
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error": "graph repair failed",
				})
				return
			}
			writeJSON(w, http.StatusOK, res)
		default:
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "unknown graph maintenance route",
			})
		}
	}
}

// extractGraphMaintenanceSubPath returns the segment after ".../graph/" (e.g.
// "health", "repair"). The handler is only mounted under /v1/admin/graph, so
// the last "/graph" occurrence is the maintenance prefix.
func extractGraphMaintenanceSubPath(path string) string {
	const marker = "/graph"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	return strings.TrimPrefix(rest, "/")
}
