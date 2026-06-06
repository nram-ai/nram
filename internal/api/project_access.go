package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// ProjectLookup resolves a project by its UUID.
type ProjectLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// NamespaceLookup resolves a namespace by its UUID.
type NamespaceLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
}

// OrgByIDLookup resolves an organization by its UUID.
type OrgByIDLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Organization, error)
}

// UserByIDLookup resolves a user by its UUID.
type UserByIDLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
}

// ProjectAccessConfig holds the dependencies for the project access middleware.
type ProjectAccessConfig struct {
	Projects   ProjectLookup
	Namespaces NamespaceLookup
	Orgs       OrgByIDLookup
	Users      UserByIDLookup
}

// CheckProjectOrgAccess enforces the org-level ownership rule for a single
// project: administrators are always allowed; every other role is allowed only
// when the project's namespace path begins with the caller's org namespace path
// (i.e. the project belongs to the same org). It returns nil on success, an
// *APIError (ErrNotFound when the project is missing, ErrForbidden otherwise)
// on failure.
//
// This is the single source of truth for project ownership. ProjectAccessMiddleware
// applies it to the {project_id} URL parameter; handlers that touch a SECOND
// project not covered by the middleware (e.g. the move destination) call it
// directly so the check can never be weakened or skipped.
func CheckProjectOrgAccess(ctx context.Context, cfg ProjectAccessConfig, ac *auth.AuthContext, projectID uuid.UUID) *APIError {
	if ac == nil {
		return ErrForbidden("unauthorized")
	}

	// Administrators have global access.
	if ac.Role == auth.RoleAdministrator {
		return nil
	}

	// Look up the project.
	project, err := cfg.Projects.GetByID(ctx, projectID)
	if err != nil {
		return ErrNotFound("project not found")
	}

	// Look up the user to find their org.
	user, err := cfg.Users.GetByID(ctx, ac.UserID)
	if err != nil {
		return ErrForbidden("user not found")
	}

	// Look up the org to find its namespace.
	org, err := cfg.Orgs.GetByID(ctx, user.OrgID)
	if err != nil {
		return ErrForbidden("org not found")
	}

	// Look up the org namespace to get its path.
	orgNS, err := cfg.Namespaces.GetByID(ctx, org.NamespaceID)
	if err != nil {
		return ErrForbidden("org namespace not found")
	}

	// Look up the project namespace to get its path.
	projectNS, err := cfg.Namespaces.GetByID(ctx, project.NamespaceID)
	if err != nil {
		return ErrForbidden("project namespace not found")
	}

	// The project is in the user's org if the project's namespace path
	// starts with the org's namespace path.
	if !strings.HasPrefix(projectNS.Path, orgNS.Path) {
		return ErrForbidden("access denied: project belongs to a different organization")
	}

	return nil
}

// ProjectAccessMiddleware returns a chi middleware that enforces project-level
// access control. It must be used on route groups that expose {project_id} as a
// chi URL parameter.
//
// Access rules:
//   - administrator role → always allowed
//   - all other roles → allowed if the project's namespace path begins with the
//     user's org namespace path (i.e. the project belongs to the same org)
//
// If no project_id URL parameter is present the middleware passes through
// without any checks (e.g. /v1/me/* routes).
func ProjectAccessMiddleware(cfg ProjectAccessConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			projectIDStr := chi.URLParam(r, "project_id")
			if projectIDStr == "" {
				// No project_id in this route — pass through.
				next.ServeHTTP(w, r)
				return
			}

			projectID, err := uuid.Parse(projectIDStr)
			if err != nil {
				// Bad UUID — let the handler return its own 400.
				next.ServeHTTP(w, r)
				return
			}

			ac := auth.FromContext(r.Context())
			if ac == nil {
				// No auth context — auth middleware would have already rejected.
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			if err := CheckProjectOrgAccess(r.Context(), cfg, ac, projectID); err != nil {
				WriteError(w, err)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
