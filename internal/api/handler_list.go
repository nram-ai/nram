package api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// MemoryLister abstracts read-only memory repository operations for the list
// and detail handlers.
type MemoryLister interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
	ListByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error)
	CountByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error)
	CountByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters) (int, error)
	ListIDsByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters, maxIDs int) ([]uuid.UUID, error)
	ListParentsByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error)
	CountParentsByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters storage.MemoryListFilters) (int, error)
	FindChildrenByParents(ctx context.Context, namespaceID uuid.UUID, parentIDs []uuid.UUID, relations []string) (map[uuid.UUID][]model.Memory, error)
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
}

// ProjectGetter abstracts project lookup for the list and detail handlers.
type ProjectGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// ParentIDFinder looks up parent memory IDs from the lineage table.
type ParentIDFinder interface {
	FindParentIDs(ctx context.Context, namespaceID uuid.UUID, memoryIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error)
}

const (
	defaultLimit       = 50
	maxLimit           = 200
	defaultMaxListIDs  = 10000
	hardCapListIDs     = 50000
)

// parseMemoryFilters extracts filter parameters from a request URL. It accepts:
//   - tag (repeatable, AND semantics): ?tag=foo&tag=bar
//   - date_from / date_to (RFC3339 or YYYY-MM-DD)
//   - enriched ("true" | "false"; absent or any other value = no filter)
//   - source (case-insensitive substring)
//   - search (case-insensitive substring against content)
//   - include_superseded ("true" surfaces paraphrase/contradiction losers; default hides them)
//
// Returns an APIError on parse failure.
func parseMemoryFilters(r *http.Request) (storage.MemoryListFilters, *APIError) {
	q := r.URL.Query()
	// Default to hiding superseded rows so list/detail/listIDs stay
	// consistent with recall. Admin and debug callers opt back in.
	filters := storage.MemoryListFilters{HideSuperseded: !queryParamBool(r, includeSupersededParam)}

	if tags := q["tag"]; len(tags) > 0 {
		// Drop empty entries to be lenient.
		out := make([]string, 0, len(tags))
		for _, t := range tags {
			if t != "" {
				out = append(out, t)
			}
		}
		filters.Tags = out
	}

	if v := q.Get("date_from"); v != "" {
		t, err := parseFilterDate(v)
		if err != nil {
			return filters, ErrBadRequest("invalid date_from: " + err.Error())
		}
		filters.DateFrom = &t
	}
	if v := q.Get("date_to"); v != "" {
		t, err := parseFilterDate(v)
		if err != nil {
			return filters, ErrBadRequest("invalid date_to: " + err.Error())
		}
		// Make date_to inclusive of the entire day when only YYYY-MM-DD given.
		if len(v) == 10 {
			t = t.Add(24 * time.Hour)
		}
		filters.DateTo = &t
	}

	switch q.Get("enriched") {
	case "true":
		t := true
		filters.Enriched = &t
	case "false":
		f := false
		filters.Enriched = &f
	}

	filters.Source = q.Get("source")
	filters.Search = q.Get("search")

	return filters, nil
}

// parseFilterDate accepts both RFC3339 and YYYY-MM-DD formats.
func parseFilterDate(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, errors.New("expected RFC3339 or YYYY-MM-DD")
}

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

		filters, ferr := parseMemoryFilters(r)
		if ferr != nil {
			WriteError(w, ferr)
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

		groupByParent := queryParamBool(r, groupByParentParam)

		var (
			mems  []model.Memory
			total int
		)
		if groupByParent {
			total, err = memRepo.CountParentsByNamespaceFiltered(r.Context(), project.NamespaceID, filters)
			if err != nil {
				WriteError(w, ErrInternal("failed to count parent memories"))
				return
			}
			mems, err = memRepo.ListParentsByNamespaceFiltered(r.Context(), project.NamespaceID, filters, limit, offset)
			if err != nil {
				WriteError(w, ErrInternal("failed to list parent memories"))
				return
			}
		} else {
			total, err = memRepo.CountByNamespaceFiltered(r.Context(), project.NamespaceID, filters)
			if err != nil {
				WriteError(w, ErrInternal("failed to count memories"))
				return
			}
			mems, err = memRepo.ListByNamespaceFiltered(r.Context(), project.NamespaceID, filters, limit, offset)
			if err != nil {
				WriteError(w, ErrInternal("failed to list memories"))
				return
			}
		}
		if mems == nil {
			mems = []model.Memory{}
		}

		if groupByParent && len(mems) > 0 {
			parentIDs := make([]uuid.UUID, len(mems))
			for i := range mems {
				parentIDs[i] = mems[i].ID
			}
			children, cerr := memRepo.FindChildrenByParents(r.Context(), project.NamespaceID, parentIDs, storage.ExtractedChildRelations)
			if cerr != nil {
				WriteError(w, ErrInternal("failed to load child memories"))
				return
			}
			for i := range mems {
				if kids, ok := children[mems[i].ID]; ok {
					mems[i].Children = kids
				} else {
					mems[i].Children = []model.Memory{}
				}
			}
		} else if lineage != nil && len(mems) > 0 {
			// Flat-list mode: surface ParentID so the existing client renderer
			// can flag enrichment-derived rows as children.
			ids := make([]uuid.UUID, len(mems))
			for i := range mems {
				ids[i] = mems[i].ID
			}
			if parentMap, err := lineage.FindParentIDs(r.Context(), project.NamespaceID, ids); err == nil {
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

		// Hide superseded rows by default. The same query parameter as the
		// list endpoint flips this for admin/debug callers.
		if mem.SupersededBy != nil && !queryParamBool(r, includeSupersededParam) {
			WriteError(w, ErrNotFound("memory not found"))
			return
		}

		// Populate parent ID from lineage table.
		if lineage != nil {
			if parentMap, err := lineage.FindParentIDs(r.Context(), project.NamespaceID, []uuid.UUID{mem.ID}); err == nil {
				if pid, ok := parentMap[mem.ID]; ok {
					pid := pid
					mem.ParentID = &pid
				}
			}
		}

		writeJSON(w, http.StatusOK, mem)
	}
}

// ListIDsResponse is the JSON envelope returned by the IDs endpoint.
type ListIDsResponse struct {
	IDs           []string `json:"ids"`
	Truncated     bool     `json:"truncated"`
	TotalMatching int      `json:"total_matching"`
}

// NewListIDsHandler returns an http.HandlerFunc that serves
// GET /v1/projects/{project_id}/memories/ids returning the IDs of all
// memories matching the given filters, capped at `max` (default 10000,
// hard-capped at 50000). Used by the admin UI to power "select all matching"
// across pages.
//
// When ?group_by_parent=true, only parent IDs are returned (matching the
// list endpoint's grouped semantics). Children travel with their parent on
// bulk operations through the existing forget/enrich cascade in the service
// layer, so the UI never needs explicit child IDs to bulk-select an entire
// family.
func NewListIDsHandler(memRepo MemoryLister, projRepo ProjectGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		max := defaultMaxListIDs
		if v := r.URL.Query().Get("max"); v != "" {
			parsed, err := strconv.Atoi(v)
			if err != nil || parsed < 0 {
				WriteError(w, ErrBadRequest("max must be a non-negative integer"))
				return
			}
			max = parsed
		}
		if max > hardCapListIDs {
			max = hardCapListIDs
		}

		filters, ferr := parseMemoryFilters(r)
		if ferr != nil {
			WriteError(w, ferr)
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

		groupByParent := queryParamBool(r, groupByParentParam)

		var (
			ids   []uuid.UUID
			total int
		)
		if groupByParent {
			total, err = memRepo.CountParentsByNamespaceFiltered(r.Context(), project.NamespaceID, filters)
			if err != nil {
				WriteError(w, ErrInternal("failed to count parent memories"))
				return
			}
			parents, perr := memRepo.ListParentsByNamespaceFiltered(r.Context(), project.NamespaceID, filters, max, 0)
			if perr != nil {
				WriteError(w, ErrInternal("failed to list parent memory ids"))
				return
			}
			ids = make([]uuid.UUID, len(parents))
			for i, p := range parents {
				ids[i] = p.ID
			}
		} else {
			total, err = memRepo.CountByNamespaceFiltered(r.Context(), project.NamespaceID, filters)
			if err != nil {
				WriteError(w, ErrInternal("failed to count memories"))
				return
			}
			ids, err = memRepo.ListIDsByNamespaceFiltered(r.Context(), project.NamespaceID, filters, max)
			if err != nil {
				WriteError(w, ErrInternal("failed to list memory ids"))
				return
			}
		}

		strs := make([]string, len(ids))
		for i, id := range ids {
			strs[i] = id.String()
		}

		writeJSON(w, http.StatusOK, ListIDsResponse{
			IDs:           strs,
			Truncated:     len(ids) < total,
			TotalMatching: total,
		})
	}
}
