package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// MeDreamProjectAccess looks up a project to verify caller ownership.
type MeDreamProjectAccess interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// MeDreamNamespaceLookup retrieves a namespace by ID for ownership chain
// traversal.
type MeDreamNamespaceLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
}

// MeDreamingConfig wires NewSelfDreamingHandler.
type MeDreamingConfig struct {
	Store      DreamAdminStore
	Projects   MeDreamProjectAccess
	Namespaces MeDreamNamespaceLookup
	Users      UserGetter
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
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

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
		case strings.HasPrefix(sub, "cycles/"):
			cycleIDStr := strings.TrimPrefix(sub, "cycles/")
			handleMeDreamCycleDetail(w, r, cfg, callerNS, cycleIDStr)
		default:
			WriteError(w, ErrBadRequest("unknown dreaming sub-path"))
		}
	}
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
	// Caller's status is fundamentally a per-project query; without a
	// project_id we cannot meaningfully report a "self status". Require it.
	pidStr := r.URL.Query().Get("project_id")
	if pidStr == "" {
		WriteError(w, ErrBadRequest("project_id query parameter is required"))
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
	pidStr := r.URL.Query().Get("project_id")
	if pidStr == "" {
		WriteError(w, ErrBadRequest("project_id query parameter is required"))
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

	cycles, err := cfg.Store.ListCycles(r.Context(), &pid, 50)
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
