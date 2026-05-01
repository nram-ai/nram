package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// DreamAdminStore abstracts storage operations for the dream admin API.
type DreamAdminStore interface {
	Status(ctx context.Context) (*DreamStatusResponse, error)
	ProjectStatus(ctx context.Context, projectID uuid.UUID) (*DreamProjectStatusResponse, error)
	ListCycles(ctx context.Context, projectID *uuid.UUID, limit int) ([]model.DreamCycle, error)
	// ListSelfCycles returns cycles whose project namespace is descended from
	// callerNS.Path (or equal to it). Used by the self-tier dreaming page to
	// list cycles across all of a caller's projects.
	ListSelfCycles(ctx context.Context, callerNS *model.Namespace, limit int) ([]model.DreamCycle, error)
	// SelfDreamingDirtyCount returns the number of caller-owned projects with
	// pending user-originated changes (dirty). Used to render the aggregate
	// "any-of-mine-dirty" badge in the self-tier dreaming view.
	SelfDreamingDirtyCount(ctx context.Context, callerNS *model.Namespace) (int, error)
	GetCycle(ctx context.Context, cycleID uuid.UUID) (*model.DreamCycle, error)
	GetCycleLogs(ctx context.Context, cycleID uuid.UUID) ([]model.DreamLog, error)
	SetEnabled(ctx context.Context, enabled bool) error
	// AbandonCycle transitions a non-terminal cycle to failed, cancelling the
	// in-flight ctx if owned by the local scheduler. Returns false iff the
	// cycle was already terminal (handler should respond 409).
	AbandonCycle(ctx context.Context, cycleID uuid.UUID, reason string) (bool, error)
}

// DreamStatusResponse is the system-wide dream status.
type DreamStatusResponse struct {
	Enabled      bool               `json:"enabled"`
	DirtyCount   int                `json:"dirty_count"`
	StuckCount   int                `json:"stuck_count"`
	RecentCycles []model.DreamCycle `json:"recent_cycles"`
}

// DreamProjectStatusResponse is the per-project dream status.
type DreamProjectStatusResponse struct {
	Enabled   bool               `json:"enabled"`
	Dirty     bool               `json:"dirty"`
	LastDream *model.DreamCycle  `json:"last_dream"`
	Cycles    []model.DreamCycle `json:"cycles"`
}

// DreamRollbacker is the interface for rolling back a dream cycle.
type DreamRollbacker interface {
	Rollback(ctx context.Context, cycleID uuid.UUID) error
}

// DreamAdminConfig holds the dependencies for the dream admin handler.
type DreamAdminConfig struct {
	Store    DreamAdminStore
	Rollback DreamRollbacker
}

// NewAdminDreamingHandler returns an http.HandlerFunc that dispatches dream
// admin requests based on method and sub-path.
//
// Routes:
//   - GET  /dreaming                       — system status
//   - GET  /dreaming/cycles                — list cycles (optional ?project_id=)
//   - GET  /dreaming/cycles/{id}           — cycle detail with logs
//   - POST /dreaming/cycles/{id}/abandon   — abandon a stuck/running cycle
//   - POST /dreaming/enable                — {"enabled": bool}
//   - POST /dreaming/rollback              — {"cycle_id": "..."}
//
// Per-project dreaming is now a Settings JSON override
// (project.settings.dreaming_enabled) saved through the project-update PATCH.
func NewAdminDreamingHandler(cfg DreamAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sub := extractDreamingSubPath(r.URL.Path)

		// Write operations require administrator role.
		if sub == "enable" || sub == "rollback" || strings.HasSuffix(sub, "/abandon") {
			ac := auth.FromContext(r.Context())
			if ac == nil || ac.Role != auth.RoleAdministrator {
				http.Error(w, "forbidden: administrator required", http.StatusForbidden)
				return
			}
		}

		switch {
		case sub == "" || sub == "status":
			handleDreamStatus(w, r, cfg)
		case sub == "cycles":
			handleDreamCyclesList(w, r, cfg)
		case strings.HasPrefix(sub, "cycles/") && strings.HasSuffix(sub, "/abandon"):
			cycleIDStr := strings.TrimSuffix(strings.TrimPrefix(sub, "cycles/"), "/abandon")
			handleDreamAbandon(w, r, cfg, cycleIDStr)
		case strings.HasPrefix(sub, "cycles/"):
			cycleIDStr := strings.TrimPrefix(sub, "cycles/")
			handleDreamCycleDetail(w, r, cfg, cycleIDStr)
		case sub == "enable":
			handleDreamEnable(w, r, cfg)
		case sub == "rollback":
			handleDreamRollback(w, r, cfg)
		default:
			WriteError(w, ErrBadRequest("unknown dreaming sub-path"))
		}
	}
}

func extractDreamingSubPath(path string) string {
	const marker = "/dreaming"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	rest = strings.TrimPrefix(rest, "/")
	return rest
}

func handleDreamStatus(w http.ResponseWriter, r *http.Request, cfg DreamAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	status, err := cfg.Store.Status(r.Context())
	if err != nil {
		WriteError(w, ErrInternal("failed to get dream status"))
		return
	}

	writeJSON(w, http.StatusOK, status)
}

func handleDreamCyclesList(w http.ResponseWriter, r *http.Request, cfg DreamAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var projectID *uuid.UUID
	if pidStr := r.URL.Query().Get("project_id"); pidStr != "" {
		pid, err := uuid.Parse(pidStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id"))
			return
		}
		projectID = &pid
	}

	cycles, err := cfg.Store.ListCycles(r.Context(), projectID, 50)
	if err != nil {
		WriteError(w, ErrInternal("failed to list dream cycles"))
		return
	}

	if cycles == nil {
		cycles = []model.DreamCycle{}
	}

	writeJSON(w, http.StatusOK, cycles)
}

func handleDreamCycleDetail(w http.ResponseWriter, r *http.Request, cfg DreamAdminConfig, cycleIDStr string) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	cycleID, err := uuid.Parse(cycleIDStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid cycle_id"))
		return
	}

	cycle, err := cfg.Store.GetCycle(r.Context(), cycleID)
	if err != nil {
		WriteError(w, ErrNotFound("dream cycle not found"))
		return
	}

	logs, err := cfg.Store.GetCycleLogs(r.Context(), cycleID)
	if err != nil {
		logs = []model.DreamLog{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"cycle": cycle,
		"logs":  logs,
	})
}

type dreamEnableRequest struct {
	Enabled bool `json:"enabled"`
}

func handleDreamEnable(w http.ResponseWriter, r *http.Request, cfg DreamAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body dreamEnableRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if err := cfg.Store.SetEnabled(r.Context(), body.Enabled); err != nil {
		WriteError(w, ErrInternal("failed to set dreaming state"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"enabled": body.Enabled})
}

func handleDreamAbandon(w http.ResponseWriter, r *http.Request, cfg DreamAdminConfig, cycleIDStr string) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	cycleID, err := uuid.Parse(cycleIDStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid cycle_id"))
		return
	}

	abandoned, err := cfg.Store.AbandonCycle(r.Context(), cycleID, "abandoned by operator via admin UI")
	if err != nil {
		WriteError(w, ErrInternal("abandon failed: "+err.Error()))
		return
	}
	if !abandoned {
		WriteError(w, ErrConflict("cycle is already in a terminal state"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "failed",
		"cycle_id": cycleID.String(),
	})
}

type dreamRollbackRequest struct {
	CycleID uuid.UUID `json:"cycle_id"`
}

func handleDreamRollback(w http.ResponseWriter, r *http.Request, cfg DreamAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body dreamRollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if cfg.Rollback == nil {
		WriteError(w, ErrInternal("rollback service not available"))
		return
	}

	if err := cfg.Rollback.Rollback(r.Context(), body.CycleID); err != nil {
		WriteError(w, ErrInternal("rollback failed: "+err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "rolled_back",
		"cycle_id": body.CycleID.String(),
	})
}
