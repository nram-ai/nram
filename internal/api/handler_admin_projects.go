package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ProjectAdminStore abstracts storage for admin project operations.
type ProjectAdminStore interface {
	CountProjects(ctx context.Context) (int, error)
	ListProjects(ctx context.Context, limit, offset int) ([]model.Project, error)
	CreateProject(ctx context.Context, name, slug, description string, ownerNamespaceID uuid.UUID, defaultTags []string, settings json.RawMessage) (*model.Project, error)
}

// ProjectAdminConfig holds the dependencies for the admin projects handler.
type ProjectAdminConfig struct {
	Store ProjectAdminStore
}

// adminCreateProjectRequest is the JSON body for POST /v1/admin/projects.
type adminCreateProjectRequest struct {
	Name             string          `json:"name"`
	Slug             string          `json:"slug"`
	Description      string          `json:"description"`
	OwnerNamespaceID string          `json:"owner_namespace_id"`
	DefaultTags      []string        `json:"default_tags"`
	Settings         json.RawMessage `json:"settings"`
}

// NewAdminProjectsHandler returns an http.HandlerFunc that handles admin
// project CRUD operations. It dispatches internally based on HTTP
// method and URL path.
func NewAdminProjectsHandler(cfg ProjectAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract sub-path after "/projects".
		path := r.URL.Path
		idx := strings.LastIndex(path, "/projects")
		sub := ""
		if idx >= 0 {
			sub = path[idx+9:] // after "/projects"
		}
		sub = strings.TrimPrefix(sub, "/")

		if sub == "" {
			// Collection routes: GET (list) or POST (create).
			switch r.Method {
			case http.MethodGet:
				handleAdminListProjects(w, r, cfg.Store)
			case http.MethodPost:
				handleAdminCreateProject(w, r, cfg.Store)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		// Item routes are no longer served here — GET and PUT are self-service
		// via /v1/me/projects/{id}. Admin delete was removed previously.
		WriteError(w, ErrBadRequest("method not allowed"))
	}
}

func handleAdminListProjects(w http.ResponseWriter, r *http.Request, store ProjectAdminStore) {
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

	total, err := store.CountProjects(r.Context())
	if err != nil {
		WriteError(w, ErrInternal("failed to count projects"))
		return
	}

	projects, err := store.ListProjects(r.Context(), limit, offset)
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

func handleAdminCreateProject(w http.ResponseWriter, r *http.Request, store ProjectAdminStore) {
	var body adminCreateProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	if strings.TrimSpace(body.Name) == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}
	if strings.TrimSpace(body.Slug) == "" {
		WriteError(w, ErrBadRequest("slug is required"))
		return
	}
	if strings.TrimSpace(body.OwnerNamespaceID) == "" {
		WriteError(w, ErrBadRequest("owner_namespace_id is required"))
		return
	}

	ownerNSID, err := uuid.Parse(body.OwnerNamespaceID)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid owner_namespace_id"))
		return
	}

	if requireValidProjectSettings(w, body.Settings) {
		return
	}

	project, err := store.CreateProject(r.Context(), body.Name, body.Slug, body.Description, ownerNSID, body.DefaultTags, body.Settings)
	if err != nil {
		WriteError(w, ErrInternal("failed to create project"))
		return
	}

	writeJSON(w, http.StatusCreated, project)
}

