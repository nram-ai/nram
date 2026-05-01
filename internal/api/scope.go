package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// Three explicit scope helpers, one per data-visibility tier. Each handler
// reads exactly one of these at construction or invocation time. There is
// no "widening" primitive — the path itself encodes which tier the handler
// belongs to (see plan: tier A self / tier B org-aggregate / tier C system).
//
// Removed in 2026-04-30 leak fix:
//   - resolveAdminScope (was a widening primitive that let administrators
//     drill into any org/user via ?org=/?user= query params).
//   - ScopeFromAuth+QueryScope (was the abstraction that resolveAdminScope
//     wrapped; not needed once each tier reads its own helper directly).
//
// Tier-A handlers (caller's own data) call SelfScope.
// Tier-B handlers (org aggregate) call OrgScope and gate on requireOrgOwner.
// Tier-C handlers (system aggregate) need no helper — admin gate at router
// level pins the visibility, and the response shape carries no per-tenant
// row-level data.

// SelfScope returns the data-visibility filter for the caller's own data
// (tier A). Always pins to (caller's org, caller's user) regardless of role.
// An administrator viewing /v1/dashboard sees their own data, not their
// whole org's and certainly not the system's.
func SelfScope(ac *auth.AuthContext) (orgID, userID *uuid.UUID) {
	if ac == nil || ac.OrgID == uuid.Nil {
		return nil, nil
	}
	o := ac.OrgID
	u := ac.UserID
	return &o, &u
}

// OrgScope reads the {org_id} URL param for tier-B handlers. Returns
// (nil, false) if absent or malformed. Caller must already have passed
// OrgAccessMiddleware (membership/admin check) AND requireOrgOwner before
// trusting this scope — OrgAccessMiddleware alone lets members through,
// which is too permissive for org-aggregate views.
func OrgScope(r *http.Request) (orgID *uuid.UUID, ok bool) {
	raw := chi.URLParam(r, "org_id")
	if raw == "" {
		return nil, false
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return nil, false
	}
	return &id, true
}

// requireOrgOwner returns true if the caller is administrator OR org_owner
// of the given org. Used by tier-B handlers to reject members who would
// otherwise pass OrgAccessMiddleware.
func requireOrgOwner(ac *auth.AuthContext, orgID uuid.UUID) bool {
	if ac == nil {
		return false
	}
	if ac.Role == auth.RoleAdministrator {
		return true
	}
	return ac.Role == auth.RoleOrgOwner && ac.OrgID == orgID
}
