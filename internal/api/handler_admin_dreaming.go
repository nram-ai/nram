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
	GetCycle(ctx context.Context, cycleID uuid.UUID) (*model.DreamCycle, error)
	GetCycleLogs(ctx context.Context, cycleID uuid.UUID) ([]model.DreamLog, error)
	SetEnabled(ctx context.Context, enabled bool) error
	SetProjectEnabled(ctx context.Context, projectID uuid.UUID, enabled bool) error
}

// DreamStatusResponse is the system-wide dream status.
type DreamStatusResponse struct {
	Enabled      bool               `json:"enabled"`
	DirtyCount   int                `json:"dirty_count"`
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
//   - GET  /dreaming             — system status
//   - GET  /dreaming/cycles      — list cycles (optional ?project_id=)
//   - GET  /dreaming/cycles/{id} — cycle detail with logs
//   - POST /dreaming/enable      — {"enabled": bool}
//   - POST /dreaming/project/enable — {"project_id": "...", "enabled": bool}
//   - POST /dreaming/rollback    — {"cycle_id": "..."}
func NewAdminDreamingHandler(cfg DreamAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sub := extractDreamingSubPath(r.URL.Path)

		// Write operations require administrator role.
		if sub == "enable" || sub == "rollback" || strings.HasPrefix(sub, "project/") {
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
		case strings.HasPrefix(sub, "cycles/"):
			cycleIDStr := strings.TrimPrefix(sub, "cycles/")
			handleDreamCycleDetail(w, r, cfg, cycleIDStr)
		case sub == "enable":
			handleDreamEnable(w, r, cfg)
		case sub == "project/enable":
			handleDreamProjectEnable(w, r, cfg)
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

type dreamProjectEnableRequest struct {
	ProjectID uuid.UUID `json:"project_id"`
	Enabled   bool      `json:"enabled"`
}

func handleDreamProjectEnable(w http.ResponseWriter, r *http.Request, cfg DreamAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body dreamProjectEnableRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if err := cfg.Store.SetProjectEnabled(r.Context(), body.ProjectID, body.Enabled); err != nil {
		WriteError(w, ErrInternal("failed to set project dreaming state"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"project_id": body.ProjectID,
		"enabled":    body.Enabled,
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
