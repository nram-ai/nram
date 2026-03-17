package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// OrgAccessMiddleware returns middleware for /v1/orgs/{org_id}/* routes that
// enforces org membership:
//   - administrator → always allowed (global access)
//   - all others    → allowed only if ac.OrgID matches the {org_id} URL param
//
// This must be mounted inside an authenticated route group.
func OrgAccessMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ac := auth.FromContext(r.Context())
			if ac == nil {
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}

			orgIDStr := chi.URLParam(r, "org_id")
			if orgIDStr == "" {
				http.Error(w, "missing org_id", http.StatusBadRequest)
				return
			}

			orgID, err := uuid.Parse(orgIDStr)
			if err != nil {
				http.Error(w, "invalid org_id", http.StatusBadRequest)
				return
			}

			// Administrators have global access to all orgs.
			if ac.Role == auth.RoleAdministrator {
				next.ServeHTTP(w, r)
				return
			}

			// All other roles must belong to the requested org.
			if ac.OrgID != orgID {
				http.Error(w, "forbidden: access denied to this organization", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
