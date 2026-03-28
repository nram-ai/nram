package api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// MemoryLister abstracts read-only memory repository operations for the list
// and detail handlers.
type MemoryLister interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
	CountByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error)
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
}

// ProjectGetter abstracts project lookup for the list and detail handlers.
type ProjectGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// ParentIDFinder looks up parent memory IDs from the lineage table.
type ParentIDFinder interface {
	FindParentIDs(ctx context.Context, memoryIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error)
}

const (
	defaultLimit = 50
	maxLimit     = 200
)

// NewListHandler returns an http.HandlerFunc that serves
// GET /v1/projects/{project_id}/memories with paginated results.
func NewListHandler(memRepo MemoryLister, projRepo ProjectGetter, lineage ParentIDFinder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		limit := defaultLimit
		if v := r.URL.Query().Get("limit"); v != "" {
			parsed, err := strconv.Atoi(v)
			if err != nil || parsed < 0 {
				WriteError(w, ErrBadRequest("limit must be a non-negative integer"))
				return
			}
			limit = parsed
		}
		if limit > maxLimit {
			limit = maxLimit
		}

		offset := 0
		if v := r.URL.Query().Get("offset"); v != "" {
			parsed, err := strconv.Atoi(v)
			if err != nil || parsed < 0 {
				WriteError(w, ErrBadRequest("offset must be a non-negative integer"))
				return
			}
			offset = parsed
		}

		project, err := projRepo.GetByID(r.Context(), projectID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, ErrNotFound("project not found"))
				return
			}
			WriteError(w, ErrInternal("failed to look up project"))
			return
		}

		total, err := memRepo.CountByNamespace(r.Context(), project.NamespaceID)
		if err != nil {
			WriteError(w, ErrInternal("failed to count memories"))
			return
		}

		mems, err := memRepo.ListByNamespace(r.Context(), project.NamespaceID, limit, offset)
		if err != nil {
			WriteError(w, ErrInternal("failed to list memories"))
			return
		}
		if mems == nil {
			mems = []model.Memory{}
		}

		// Populate parent IDs from lineage table.
		if lineage != nil && len(mems) > 0 {
			ids := make([]uuid.UUID, len(mems))
			for i := range mems {
				ids[i] = mems[i].ID
			}
			if parentMap, err := lineage.FindParentIDs(r.Context(), ids); err == nil {
				for i := range mems {
					if pid, ok := parentMap[mems[i].ID]; ok {
						pid := pid // copy for pointer
						mems[i].ParentID = &pid
					}
				}
			}
		}

		writeJSON(w, http.StatusOK, model.PaginatedResponse[model.Memory]{
			Data: mems,
			Pagination: model.Pagination{
				Total:  total,
				Limit:  limit,
				Offset: offset,
			},
		})
	}
}

// NewDetailHandler returns an http.HandlerFunc that serves
// GET /v1/projects/{project_id}/memories/{id} returning a single memory.
func NewDetailHandler(memRepo MemoryLister, projRepo ProjectGetter, lineage ParentIDFinder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		idStr := chi.URLParam(r, "id")
		memoryID, err := uuid.Parse(idStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid id: must be a valid UUID"))
			return
		}

		project, err := projRepo.GetByID(r.Context(), projectID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, ErrNotFound("project not found"))
				return
			}
			WriteError(w, ErrInternal("failed to look up project"))
			return
		}

		mem, err := memRepo.GetByID(r.Context(), memoryID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, ErrNotFound("memory not found"))
				return
			}
			WriteError(w, ErrInternal("failed to look up memory"))
			return
		}

		if mem.NamespaceID != project.NamespaceID {
			WriteError(w, ErrNotFound("memory not found"))
			return
		}

		// Populate parent ID from lineage table.
		if lineage != nil {
			if parentMap, err := lineage.FindParentIDs(r.Context(), []uuid.UUID{mem.ID}); err == nil {
				if pid, ok := parentMap[mem.ID]; ok {
					pid := pid
					mem.ParentID = &pid
				}
			}
		}

		writeJSON(w, http.StatusOK, mem)
	}
}
