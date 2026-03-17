package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	GetProject(ctx context.Context, id uuid.UUID) (*model.Project, error)
	UpdateProject(ctx context.Context, id uuid.UUID, name, slug, description string, defaultTags []string, settings json.RawMessage) (*model.Project, error)
	DeleteProject(ctx context.Context, id uuid.UUID) error
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

// adminUpdateProjectRequest is the JSON body for PUT /v1/admin/projects/{id}.
type adminUpdateProjectRequest struct {
	Name        string          `json:"name"`
	Slug        string          `json:"slug"`
	Description string          `json:"description"`
	DefaultTags []string        `json:"default_tags"`
	Settings    json.RawMessage `json:"settings"`
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

		// Item routes: GET, PUT, DELETE on /projects/{id}.
		id, err := uuid.Parse(sub)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project id"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleAdminGetProject(w, r, cfg.Store, id)
		case http.MethodPut:
			handleAdminUpdateProject(w, r, cfg.Store, id)
		case http.MethodDelete:
			handleAdminDeleteProject(w, r, cfg.Store, id)
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

// isProjectNotFound returns true if the error represents a not-found condition.
func isProjectNotFound(err error) bool {
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	return strings.Contains(err.Error(), "not found")
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

	project, err := store.CreateProject(r.Context(), body.Name, body.Slug, body.Description, ownerNSID, body.DefaultTags, body.Settings)
	if err != nil {
		WriteError(w, ErrInternal("failed to create project"))
		return
	}

	writeJSON(w, http.StatusCreated, project)
}

func handleAdminGetProject(w http.ResponseWriter, r *http.Request, store ProjectAdminStore, id uuid.UUID) {
	project, err := store.GetProject(r.Context(), id)
	if err != nil {
		if isProjectNotFound(err) {
			WriteError(w, ErrNotFound("project not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get project"))
		return
	}

	writeJSON(w, http.StatusOK, project)
}

func handleAdminUpdateProject(w http.ResponseWriter, r *http.Request, store ProjectAdminStore, id uuid.UUID) {
	var body adminUpdateProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	project, err := store.UpdateProject(r.Context(), id, body.Name, body.Slug, body.Description, body.DefaultTags, body.Settings)
	if err != nil {
		if isProjectNotFound(err) {
			WriteError(w, ErrNotFound("project not found"))
			return
		}
		WriteError(w, ErrInternal("failed to update project"))
		return
	}

	writeJSON(w, http.StatusOK, project)
}

func handleAdminDeleteProject(w http.ResponseWriter, r *http.Request, store ProjectAdminStore, id uuid.UUID) {
	err := store.DeleteProject(r.Context(), id)
	if err != nil {
		if isProjectNotFound(err) {
			WriteError(w, ErrNotFound("project not found"))
			return
		}
		WriteError(w, ErrInternal("failed to delete project"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
