package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// QueryScope describes the data visibility for the authenticated user.
// Handlers pass this to storage methods which add WHERE clauses accordingly.
type QueryScope struct {
	OrgID   *uuid.UUID
	UserID  *uuid.UUID
	IsAdmin bool
}

// ScopeFromAuth derives a QueryScope from the authenticated user's context.
// All roles are scoped to their organization. Administrators retain the
// IsAdmin flag for admin-specific operations but their data queries are
// still org-scoped to prevent leaking other users' data.
//   - administrator → org-scoped + IsAdmin flag
//   - org_owner     → org-scoped (OrgID set)
//   - member/readonly/service → org-scoped + user-scoped (OrgID + UserID set)
func ScopeFromAuth(ac *auth.AuthContext) QueryScope {
	if ac == nil {
		return QueryScope{}
	}

	if ac.OrgID == uuid.Nil {
		return QueryScope{}
	}

	orgID := ac.OrgID

	if ac.Role == auth.RoleAdministrator {
		return QueryScope{IsAdmin: true, OrgID: &orgID}
	}

	if ac.Role == auth.RoleOrgOwner {
		return QueryScope{OrgID: &orgID}
	}

	userID := ac.UserID
	return QueryScope{OrgID: &orgID, UserID: &userID}
}

// resolveAdminScope picks the (orgID, userID) filter pair for an admin-style
// data view based on the caller's role:
//
//   - administrator: nil/nil by default. URL path {org_id} or ?org= drills
//     into one org; ?user= drills into one user (only when allowUserDrill).
//   - org_owner: pinned to own org. ?user= drills into one user when
//     allowUserDrill; widening attempts via ?org= are ignored.
//   - member/readonly/service: pinned to own org and own user. Widening
//     attempts are ignored.
//
// allowUserDrill = false is for endpoints (dashboard, activity, namespace
// tree) that have no per-user split. In that mode userID is always nil.
func resolveAdminScope(r *http.Request, allowUserDrill bool) (orgID, userID *uuid.UUID) {
	scope := ScopeFromAuth(auth.FromContext(r.Context()))

	if scope.IsAdmin {
		if urlOrg := chi.URLParam(r, "org_id"); urlOrg != "" {
			if id, err := uuid.Parse(urlOrg); err == nil {
				orgID = &id
			}
		} else if raw := r.URL.Query().Get("org"); raw != "" {
			if id, err := uuid.Parse(raw); err == nil {
				orgID = &id
			}
		}
		if allowUserDrill {
			if raw := r.URL.Query().Get("user"); raw != "" {
				if id, err := uuid.Parse(raw); err == nil {
					userID = &id
				}
			}
		}
		return
	}

	orgID = scope.OrgID
	if !allowUserDrill {
		return
	}
	if scope.UserID != nil {
		userID = scope.UserID
		return
	}
	if raw := r.URL.Query().Get("user"); raw != "" {
		if id, err := uuid.Parse(raw); err == nil {
			userID = &id
		}
	}
	return
}
