package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// OrgProjectStore abstracts read-only project operations scoped to an organization.
type OrgProjectStore interface {
	CountProjectsByOrg(ctx context.Context, orgID uuid.UUID) (int, error)
	ListProjectsByOrg(ctx context.Context, orgID uuid.UUID, limit, offset int) ([]model.Project, error)
}

// OrgProjectConfig holds the dependencies for org-scoped project listing.
type OrgProjectConfig struct {
	Store OrgProjectStore
}

// NewOrgProjectsHandler returns an http.HandlerFunc that lists projects
// belonging to the organization specified by {org_id} in the URL.
// Read-only — only GET is supported.
func NewOrgProjectsHandler(cfg OrgProjectConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		orgIDStr := chi.URLParam(r, "org_id")
		orgID, err := uuid.Parse(orgIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid org_id"))
			return
		}

		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				limit = n
			}
		}
		if limit > 200 {
			limit = 200
		}

		offset := 0
		if v := r.URL.Query().Get("offset"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				offset = n
			}
		}

		total, err := cfg.Store.CountProjectsByOrg(r.Context(), orgID)
		if err != nil {
			WriteError(w, ErrInternal("failed to count projects"))
			return
		}

		projects, err := cfg.Store.ListProjectsByOrg(r.Context(), orgID, limit, offset)
		if err != nil {
			WriteError(w, ErrInternal("failed to list projects"))
			return
		}
		if projects == nil {
			projects = []model.Project{}
		}

		writeJSON(w, http.StatusOK, model.PaginatedResponse[model.Project]{
			Data: projects,
			Pagination: model.Pagination{
				Total:  total,
				Limit:  limit,
				Offset: offset,
			},
		})
	}
}
