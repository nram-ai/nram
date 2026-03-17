package api

import (
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
//   - administrator → global access (IsAdmin: true)
//   - org_owner     → org-scoped (OrgID set)
//   - member/readonly/service → org-scoped + user-scoped (OrgID + UserID set)
func ScopeFromAuth(ac *auth.AuthContext) QueryScope {
	if ac == nil {
		return QueryScope{}
	}

	if ac.Role == auth.RoleAdministrator {
		return QueryScope{IsAdmin: true}
	}

	if ac.OrgID == uuid.Nil {
		return QueryScope{}
	}

	orgID := ac.OrgID
	if ac.Role == auth.RoleOrgOwner {
		return QueryScope{OrgID: &orgID}
	}

	userID := ac.UserID
	return QueryScope{OrgID: &orgID, UserID: &userID}
}
