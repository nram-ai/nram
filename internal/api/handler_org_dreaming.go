package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// OrgDreamStore narrows the admin store surface to the org-scoped operations
// /v1/orgs/{orgId}/dreaming needs. Implemented by storage/admin.DreamAdminStore.
type OrgDreamStore interface {
	OrgListCycles(ctx context.Context, orgID uuid.UUID, limit int) ([]model.DreamCycle, error)
	OrgDirtyCount(ctx context.Context, orgID uuid.UUID) (int, error)
	OrgStuckCount(ctx context.Context, orgID uuid.UUID) (int, error)
	GetCycle(ctx context.Context, cycleID uuid.UUID) (*model.DreamCycle, error)
	GetCycleLogs(ctx context.Context, cycleID uuid.UUID) ([]model.DreamLog, error)
	CycleInOrg(ctx context.Context, orgID uuid.UUID, cycleID uuid.UUID) (bool, error)
	AbandonCycle(ctx context.Context, cycleID uuid.UUID, reason string) (bool, error)
}

// OrgDreamStatusResponse is the org-tier dream status payload. The system-
// wide enabled flag is intentionally omitted — toggling dreaming is admin-
// only and lives on the System tab.
type OrgDreamStatusResponse struct {
	DirtyCount   int                `json:"dirty_count"`
	StuckCount   int                `json:"stuck_count"`
	RecentCycles []model.DreamCycle `json:"recent_cycles"`
}

// OrgDreamingConfig wires NewOrgDreamingHandler.
type OrgDreamingConfig struct {
	Store    OrgDreamStore
	Rollback DreamRollbacker
}

// NewOrgDreamingHandler returns the org-tier dream handler at
// /v1/orgs/{org_id}/dreaming. Authorization: caller must be administrator
// or org_owner of {org_id}. Sub-paths:
//
//	GET  /                       — org-scoped status (dirty/stuck/recent)
//	GET  /cycles                 — cycles list scoped to org
//	GET  /cycles/{id}            — cycle detail (404 if not in org)
//	POST /cycles/{id}/abandon    — abandon a stuck cycle in the org
//	POST /cycles/{id}/rollback   — rollback a completed cycle in the org
func NewOrgDreamingHandler(cfg OrgDreamingConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orgID, ok := OrgScope(r)
		if !ok {
			WriteError(w, ErrBadRequest("invalid org_id"))
			return
		}
		ac := auth.FromContext(r.Context())
		if !requireOrgOwner(ac, *orgID) {
			WriteError(w, ErrForbidden("org_owner role required for this org"))
			return
		}

		sub := extractSubPath(r.URL.Path, "/dreaming")

		switch {
		case sub == "" || sub == "status":
			handleOrgDreamStatus(w, r, cfg, *orgID)
		case sub == "cycles":
			handleOrgDreamCyclesList(w, r, cfg, *orgID)
		case strings.HasPrefix(sub, "cycles/") && strings.HasSuffix(sub, "/abandon"):
			cycleIDStr := strings.TrimSuffix(strings.TrimPrefix(sub, "cycles/"), "/abandon")
			handleOrgDreamAbandon(w, r, cfg, *orgID, cycleIDStr)
		case strings.HasPrefix(sub, "cycles/") && strings.HasSuffix(sub, "/rollback"):
			cycleIDStr := strings.TrimSuffix(strings.TrimPrefix(sub, "cycles/"), "/rollback")
			handleOrgDreamRollback(w, r, cfg, *orgID, cycleIDStr)
		case strings.HasPrefix(sub, "cycles/"):
			cycleIDStr := strings.TrimPrefix(sub, "cycles/")
			handleOrgDreamCycleDetail(w, r, cfg, *orgID, cycleIDStr)
		default:
			WriteError(w, ErrBadRequest("unknown dreaming sub-path"))
		}
	}
}

func handleOrgDreamStatus(w http.ResponseWriter, r *http.Request, cfg OrgDreamingConfig, orgID uuid.UUID) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	dirty, err := cfg.Store.OrgDirtyCount(r.Context(), orgID)
	if err != nil {
		WriteError(w, ErrInternal("failed to get dirty count"))
		return
	}
	cycles, err := cfg.Store.OrgListCycles(r.Context(), orgID, 10)
	if err != nil {
		WriteError(w, ErrInternal("failed to list recent cycles"))
		return
	}
	stuck, err := cfg.Store.OrgStuckCount(r.Context(), orgID)
	if err != nil {
		stuck = 0
	}
	if cycles == nil {
		cycles = []model.DreamCycle{}
	}
	writeJSON(w, http.StatusOK, OrgDreamStatusResponse{
		DirtyCount:   dirty,
		StuckCount:   stuck,
		RecentCycles: cycles,
	})
}

func handleOrgDreamCyclesList(w http.ResponseWriter, r *http.Request, cfg OrgDreamingConfig, orgID uuid.UUID) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	cycles, err := cfg.Store.OrgListCycles(r.Context(), orgID, 50)
	if err != nil {
		WriteError(w, ErrInternal("failed to list dream cycles"))
		return
	}
	if cycles == nil {
		cycles = []model.DreamCycle{}
	}
	writeJSON(w, http.StatusOK, cycles)
}

func handleOrgDreamCycleDetail(w http.ResponseWriter, r *http.Request, cfg OrgDreamingConfig, orgID uuid.UUID, cycleIDStr string) {
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
	in, err := cfg.Store.CycleInOrg(r.Context(), orgID, cycleID)
	if err != nil || !in {
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

func handleOrgDreamAbandon(w http.ResponseWriter, r *http.Request, cfg OrgDreamingConfig, orgID uuid.UUID, cycleIDStr string) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	cycleID, err := uuid.Parse(cycleIDStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid cycle_id"))
		return
	}
	in, err := cfg.Store.CycleInOrg(r.Context(), orgID, cycleID)
	if err != nil || !in {
		WriteError(w, ErrNotFound("dream cycle not found"))
		return
	}
	abandoned, err := cfg.Store.AbandonCycle(r.Context(), cycleID, "abandoned by org_owner")
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

func handleOrgDreamRollback(w http.ResponseWriter, r *http.Request, cfg OrgDreamingConfig, orgID uuid.UUID, cycleIDStr string) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	cycleID, err := uuid.Parse(cycleIDStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid cycle_id"))
		return
	}
	in, err := cfg.Store.CycleInOrg(r.Context(), orgID, cycleID)
	if err != nil || !in {
		WriteError(w, ErrNotFound("dream cycle not found"))
		return
	}
	if cfg.Rollback == nil {
		WriteError(w, ErrInternal("rollback service not available"))
		return
	}
	if err := cfg.Rollback.Rollback(r.Context(), cycleID); err != nil {
		WriteError(w, ErrInternal("rollback failed: "+err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "rolled_back",
		"cycle_id": cycleID.String(),
	})
}
