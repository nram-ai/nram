package api

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// ProjectLister defines operations on the projects repository needed by the
// projects handler.
type ProjectLister interface {
	ListByUser(ctx context.Context, ownerNamespaceID uuid.UUID) ([]model.Project, error)
	Create(ctx context.Context, project *model.Project) error
	GetBySlug(ctx context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error)
}

// UserGetter retrieves a user by ID.
type UserGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
}

// NamespaceCreator creates and retrieves namespaces.
type NamespaceCreator interface {
	Create(ctx context.Context, ns *model.Namespace) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
}

// createProjectRequest is the JSON body for POST /v1/me/projects.
type createProjectRequest struct {
	Name        string   `json:"name"`
	Slug        string   `json:"slug"`
	Description string   `json:"description"`
	DefaultTags []string `json:"default_tags"`
}

// slugifyRe matches one or more non-alphanumeric characters.
var slugifyRe = regexp.MustCompile(`[^a-z0-9]+`)

// slugify converts a human-readable name into a URL-safe slug.
func slugify(name string) string {
	s := strings.ToLower(name)
	s = slugifyRe.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	return s
}

// NewMeProjectsHandler returns an http.HandlerFunc that handles
// GET /v1/me/projects (list) and POST /v1/me/projects (create).
func NewMeProjectsHandler(projects ProjectLister, users UserGetter, namespaces NamespaceCreator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListProjects(w, r, projects, users)
		case http.MethodPost:
			handleCreateProject(w, r, projects, users, namespaces)
		default:
			w.Header().Set("Allow", "GET, POST")
			WriteError(w, &APIError{
				Code:    "method_not_allowed",
				Message: "method not allowed",
				Status:  http.StatusMethodNotAllowed,
			})
		}
	}
}

func handleListProjects(w http.ResponseWriter, r *http.Request, projects ProjectLister, users UserGetter) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}

	// Parse pagination query params.
	limit := parseIntParam(r, "limit", 50)
	offset := parseIntParam(r, "offset", 0)
	if limit < 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	all, err := projects.ListByUser(r.Context(), user.NamespaceID)
	if err != nil {
		WriteError(w, ErrInternal("failed to list projects"))
		return
	}

	total := len(all)

	// Apply offset/limit in handler since the repo doesn't support them.
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}
	page := all[start:end]

	resp := model.PaginatedResponse[model.Project]{
		Data: page,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	}

	writeJSON(w, http.StatusOK, resp)
}

func handleCreateProject(w http.ResponseWriter, r *http.Request, projects ProjectLister, users UserGetter, namespaces NamespaceCreator) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}

	var body createProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
		return
	}

	if strings.TrimSpace(body.Name) == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}

	slug := body.Slug
	if slug == "" {
		slug = slugify(body.Name)
	}

	// Check for slug conflicts.
	existing, err := projects.GetBySlug(r.Context(), user.NamespaceID, slug)
	if err == nil && existing != nil {
		WriteError(w, ErrConflict("a project with slug \""+slug+"\" already exists"))
		return
	}

	// Look up the user's namespace for parent info.
	parentNS, err := namespaces.GetByID(r.Context(), user.NamespaceID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user namespace"))
		return
	}

	// Create the project namespace.
	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     slug,
		Slug:     slug,
		Kind:     "project",
		ParentID: &user.NamespaceID,
		Path:     parentNS.Path + "/" + slug,
		Depth:    parentNS.Depth + 1,
	}
	if err := namespaces.Create(r.Context(), projectNS); err != nil {
		WriteError(w, ErrInternal("failed to create project namespace"))
		return
	}

	// Create the project record.
	project := &model.Project{
		NamespaceID:      projectNSID,
		OwnerNamespaceID: user.NamespaceID,
		Name:             body.Name,
		Slug:             slug,
		Description:      body.Description,
		DefaultTags:      body.DefaultTags,
	}
	if err := projects.Create(r.Context(), project); err != nil {
		WriteError(w, ErrInternal("failed to create project"))
		return
	}

	writeJSON(w, http.StatusCreated, project)
}

// ProjectItemStore defines operations needed by the self-service project item handler.
type ProjectItemStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
	Update(ctx context.Context, project *model.Project) error
}

// meUpdateProjectRequest is the JSON body for PUT /v1/me/projects/{id}.
type meUpdateProjectRequest struct {
	Name        string          `json:"name"`
	Slug        string          `json:"slug"`
	Description string          `json:"description"`
	DefaultTags []string        `json:"default_tags"`
	Settings    json.RawMessage `json:"settings"`
}

// NewMeProjectItemHandler returns an http.HandlerFunc that handles
// GET /v1/me/projects/{id} and PUT /v1/me/projects/{id}.
// Only the project owner can access their own projects.
func NewMeProjectItemHandler(projects ProjectItemStore, users UserGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleMeGetProject(w, r, projects, users)
		case http.MethodPut:
			handleMeUpdateProject(w, r, projects, users)
		default:
			w.Header().Set("Allow", "GET, PUT")
			WriteError(w, &APIError{
				Code:    "method_not_allowed",
				Message: "method not allowed",
				Status:  http.StatusMethodNotAllowed,
			})
		}
	}
}

func handleMeGetProject(w http.ResponseWriter, r *http.Request, projects ProjectItemStore, users UserGetter) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	idStr := r.PathValue("id")
	if idStr == "" {
		idStr = chi.URLParam(r, "id")
	}
	projectID, err := uuid.Parse(idStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid project id"))
		return
	}

	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}

	project, err := projects.GetByID(r.Context(), projectID)
	if err != nil {
		WriteError(w, ErrNotFound("project not found"))
		return
	}

	if project.OwnerNamespaceID != user.NamespaceID {
		WriteError(w, ErrForbidden("you can only view your own projects"))
		return
	}

	writeJSON(w, http.StatusOK, project)
}

func handleMeUpdateProject(w http.ResponseWriter, r *http.Request, projects ProjectItemStore, users UserGetter) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	idStr := r.PathValue("id")
	if idStr == "" {
		idStr = chi.URLParam(r, "id")
	}
	projectID, err := uuid.Parse(idStr)
	if err != nil {
		WriteError(w, ErrBadRequest("invalid project id"))
		return
	}

	user, err := users.GetByID(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to resolve user"))
		return
	}

	project, err := projects.GetByID(r.Context(), projectID)
	if err != nil {
		WriteError(w, ErrNotFound("project not found"))
		return
	}

	if project.OwnerNamespaceID != user.NamespaceID {
		WriteError(w, ErrForbidden("you can only update your own projects"))
		return
	}

	var body meUpdateProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
		return
	}

	if body.Name != "" {
		project.Name = body.Name
	}
	if body.Slug != "" {
		project.Slug = body.Slug
	}
	project.Description = body.Description
	if body.DefaultTags != nil {
		project.DefaultTags = body.DefaultTags
	}
	if body.Settings != nil {
		project.Settings = body.Settings
	}

	if err := projects.Update(r.Context(), project); err != nil {
		WriteError(w, ErrInternal("failed to update project"))
		return
	}

	// Re-fetch to get updated timestamps and computed fields.
	updated, err := projects.GetByID(r.Context(), projectID)
	if err != nil {
		WriteError(w, ErrInternal("failed to retrieve updated project"))
		return
	}

	writeJSON(w, http.StatusOK, updated)
}

// ProjectDeleteServicer defines the interface for the project delete service.
type ProjectDeleteServicer interface {
	Delete(ctx context.Context, req *service.ProjectDeleteRequest) (*service.ProjectDeleteResponse, error)
}

// ProjectGetterForDelete defines the project lookup needed by the delete handler.
type ProjectGetterForDelete interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// NewMeProjectDeleteHandler returns an http.HandlerFunc that handles
// DELETE /v1/me/projects/{id}. Only the project owner can delete their project.
func NewMeProjectDeleteHandler(deleteSvc ProjectDeleteServicer, projects ProjectGetterForDelete, users UserGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.Header().Set("Allow", "DELETE")
			WriteError(w, &APIError{
				Code:    "method_not_allowed",
				Message: "method not allowed",
				Status:  http.StatusMethodNotAllowed,
			})
			return
		}

		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		// Extract project ID from the URL path.
		idStr := r.PathValue("id")
		if idStr == "" {
			// Fallback: extract from chi URL params.
			idStr = chi.URLParam(r, "id")
		}
		projectID, err := uuid.Parse(idStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project id"))
			return
		}

		// Look up user to get their namespace.
		user, err := users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrInternal("failed to resolve user"))
			return
		}

		// Look up project and verify ownership.
		project, err := projects.GetByID(r.Context(), projectID)
		if err != nil {
			WriteError(w, ErrNotFound("project not found"))
			return
		}
		if project.OwnerNamespaceID != user.NamespaceID {
			WriteError(w, ErrForbidden("you can only delete your own projects"))
			return
		}

		_, err = deleteSvc.Delete(r.Context(), &service.ProjectDeleteRequest{
			ProjectID: projectID,
		})
		if err != nil {
			if strings.Contains(err.Error(), "global") {
				WriteError(w, ErrBadRequest(err.Error()))
				return
			}
			WriteError(w, ErrInternal("failed to delete project"))
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// parseIntParam parses an integer query parameter, returning def if missing or invalid.
func parseIntParam(r *http.Request, name string, def int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}
