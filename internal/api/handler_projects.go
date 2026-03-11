package api

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
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
