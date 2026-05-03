package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// MeDreamingAggregateStatus is the self-tier no-project status response: the
// number of caller-owned projects with pending changes, plus the total count
// of caller-owned projects.
type MeDreamingAggregateStatus struct {
	DirtyCount   int `json:"dirty_count"`
	ProjectCount int `json:"project_count"`
}

// MeDreamProjectAccess looks up a project to verify caller ownership and
// resolve a single project's name for the cycles list response.
type MeDreamProjectAccess interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
	CountByUser(ctx context.Context, ownerNamespaceID uuid.UUID) (int, error)
}

// MeDreamNamespaceLookup retrieves a namespace by ID for ownership chain
// traversal.
type MeDreamNamespaceLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
}

// MeDreamCycleGate is the org-scope guard the self-tier handler uses for
// abandon/rollback. Implemented by storage/admin.DreamAdminStore.
type MeDreamCycleGate interface {
	CycleInNamespacePrefix(ctx context.Context, prefix string, cycleID uuid.UUID) (bool, error)
}

// MeDreamingConfig wires NewSelfDreamingHandler.
type MeDreamingConfig struct {
	Store      DreamAdminStore
	Projects   MeDreamProjectAccess
	Namespaces MeDreamNamespaceLookup
	Users      UserGetter
	// Gate is the cycle-in-namespace guard used by abandon/rollback. May
	// be nil during tests; the handler then refuses write operations.
	Gate     MeDreamCycleGate
	Rollback DreamRollbacker
}

// NewSelfDreamingHandler returns the read-only self-tier dream handler at
// /v1/me/dreaming. Caller must specify ?project_id= and the handler
// verifies the project's namespace path is descended from the caller's
// user namespace. Returns: status, cycles list, cycle detail (with
// payloads, since the caller owns the data).
//
// Write operations (enable, abandon, rollback, project/enable) are NOT
// exposed here — those remain on /v1/admin/dreaming.
func NewSelfDreamingHandler(cfg MeDreamingConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		sub := strings.TrimPrefix(extractMeDreamSubPath(r.URL.Path), "/")

		// Resolve caller's user namespace once for ownership checks.
		user, err := cfg.Users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrInternal("failed to resolve user"))
			return
		}
		callerNS, err := cfg.Namespaces.GetByID(r.Context(), user.NamespaceID)
		if err != nil {
			WriteError(w, ErrInternal("failed to resolve user namespace"))
			return
		}

		switch {
		case sub == "" || sub == "status":
			handleMeDreamStatus(w, r, cfg, callerNS)
		case sub == "cycles":
			handleMeDreamCyclesList(w, r, cfg, callerNS)
		case strings.HasPrefix(sub, "cycles/") && strings.HasSuffix(sub, "/abandon"):
			cycleIDStr := strings.TrimSuffix(strings.TrimPrefix(sub, "cycles/"), "/abandon")
			handleMeDreamAbandon(w, r, cfg, callerNS, cycleIDStr)
		case strings.HasPrefix(sub, "cycles/") && strings.HasSuffix(sub, "/rollback"):
			cycleIDStr := strings.TrimSuffix(strings.TrimPrefix(sub, "cycles/"), "/rollback")
			handleMeDreamRollback(w, r, cfg, callerNS, cycleIDStr)
		case strings.HasPrefix(sub, "cycles/"):
			cycleIDStr := strings.TrimPrefix(sub, "cycles/")
			handleMeDreamCycleDetail(w, r, cfg, callerNS, cycleIDStr)
		default:
			WriteError(w, ErrBadRequest("unknown dreaming sub-path"))
		}
	}
}

func handleMeDreamAbandon(w http.ResponseWriter, r *http.Request, cfg MeDreamingConfig, callerNS *model.Namespace, cycleIDStr string) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	cycleID, err := uuid.Parse(cycleIDStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid cycle_id"))
		return
	}
	if cfg.Gate == nil {
		WriteError(w, ErrInternal("ownership gate unavailable"))
		return
	}
	in, err := cfg.Gate.CycleInNamespacePrefix(r.Context(), callerNS.Path, cycleID)
	if err != nil || !in {
		WriteError(w, ErrNotFound("dream cycle not found"))
		return
	}
	abandoned, err := cfg.Store.AbandonCycle(r.Context(), cycleID, "abandoned by owner")
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

func handleMeDreamRollback(w http.ResponseWriter, r *http.Request, cfg MeDreamingConfig, callerNS *model.Namespace, cycleIDStr string) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	cycleID, err := uuid.Parse(cycleIDStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid cycle_id"))
		return
	}
	if cfg.Gate == nil || cfg.Rollback == nil {
		WriteError(w, ErrInternal("rollback service not available"))
		return
	}
	in, err := cfg.Gate.CycleInNamespacePrefix(r.Context(), callerNS.Path, cycleID)
	if err != nil || !in {
		WriteError(w, ErrNotFound("dream cycle not found"))
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

func extractMeDreamSubPath(path string) string {
	const marker = "/me/dreaming"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	return path[idx+len(marker):]
}

// projectOwnedByCaller returns true iff the project's namespace_id resolves
// to a path descended from callerNS.Path (or equal to it).
func (c MeDreamingConfig) projectOwnedByCaller(ctx context.Context, projectID uuid.UUID, callerNS *model.Namespace) bool {
	proj, err := c.Projects.GetByID(ctx, projectID)
	if err != nil {
		return false
	}
	projNS, err := c.Namespaces.GetByID(ctx, proj.NamespaceID)
	if err != nil {
		return false
	}
	prefix := callerNS.Path + "/"
	return projNS.Path == callerNS.Path || strings.HasPrefix(projNS.Path, prefix)
}

func handleMeDreamStatus(w http.ResponseWriter, r *http.Request, cfg MeDreamingConfig, callerNS *model.Namespace) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	// project_id optional: when set, return per-project status; when omitted,
	// return aggregate any-dirty + project count for the caller's projects.
	pidStr := r.URL.Query().Get("project_id")
	if pidStr == "" {
		dirtyCount, err := cfg.Store.SelfDreamingDirtyCount(r.Context(), callerNS)
		if err != nil {
			WriteError(w, ErrInternal("failed to compute aggregate dreaming status"))
			return
		}
		projectCount, err := cfg.Projects.CountByUser(r.Context(), callerNS.ID)
		if err != nil {
			WriteError(w, ErrInternal("failed to count caller projects"))
			return
		}
		writeJSON(w, http.StatusOK, MeDreamingAggregateStatus{
			DirtyCount:   dirtyCount,
			ProjectCount: projectCount,
		})
		return
	}

	pid, err := uuid.Parse(pidStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid project_id"))
		return
	}
	if !cfg.projectOwnedByCaller(r.Context(), pid, callerNS) {
		WriteError(w, ErrForbidden("project is not owned by caller"))
		return
	}

	status, err := cfg.Store.ProjectStatus(r.Context(), pid)
	if err != nil {
		WriteError(w, ErrNotFound("project not found"))
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func handleMeDreamCyclesList(w http.ResponseWriter, r *http.Request, cfg MeDreamingConfig, callerNS *model.Namespace) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	// project_id optional: when set, filter to that project; when omitted,
	// list cycles across all of the caller's projects via namespace prefix.
	// Either path returns model.DreamCycle with ProjectName populated.
	var (
		cycles []model.DreamCycle
		err    error
	)
	pidStr := r.URL.Query().Get("project_id")
	if pidStr != "" {
		pid, parseErr := uuid.Parse(pidStr)
		if parseErr != nil {
			WriteError(w, ErrBadRequest("invalid project_id"))
			return
		}
		if !cfg.projectOwnedByCaller(r.Context(), pid, callerNS) {
			WriteError(w, ErrForbidden("project is not owned by caller"))
			return
		}
		cycles, err = cfg.Store.ListCycles(r.Context(), &pid, 50)
		if err == nil && len(cycles) > 0 {
			// Single-project branch: one project, one name lookup.
			if proj, perr := cfg.Projects.GetByID(r.Context(), pid); perr == nil && proj != nil {
				for i := range cycles {
					cycles[i].ProjectName = proj.Name
				}
			}
		}
	} else {
		// Multi-project branch: ListSelfCycles already populates ProjectName
		// via the JOIN in DreamCycleRepo.ListByNamespacePathPrefix.
		cycles, err = cfg.Store.ListSelfCycles(r.Context(), callerNS, 50)
	}
	if err != nil {
		WriteError(w, ErrInternal("failed to list dream cycles"))
		return
	}
	if cycles == nil {
		cycles = []model.DreamCycle{}
	}
	writeJSON(w, http.StatusOK, cycles)
}

func handleMeDreamCycleDetail(w http.ResponseWriter, r *http.Request, cfg MeDreamingConfig, callerNS *model.Namespace, cycleIDStr string) {
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
	if !cfg.projectOwnedByCaller(r.Context(), cycle.ProjectID, callerNS) {
		// 404 not 403, so caller can't probe for cycles in other projects.
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
