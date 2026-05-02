package api

import (
	"net/http"

	"github.com/nram-ai/nram/internal/auth"
)

// NewOrgUsageHandler returns the tier-B usage handler at
// /v1/orgs/{org_id}/usage. Returns token-usage aggregates scoped to the
// org's namespace subtree (org-wide, not per-user).
//
// Authorization: caller must be RoleOrgOwner of {org_id} or
// RoleAdministrator. OrgAccessMiddleware admits members through, so this
// handler enforces the stricter requireOrgOwner gate before serving.
func NewOrgUsageHandler(cfg UsageConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		orgID, ok := OrgScope(r)
		if !ok {
			WriteError(w, ErrBadRequest("invalid org_id"))
			return
		}

		ac := auth.FromContext(r.Context())
		if !requireOrgOwner(ac, *orgID) {
			WriteError(w, ErrForbidden("org_owner role required for this org"))
			return
		}

		filter, apiErr := parseUsageQueryParams(r)
		if apiErr != nil {
			WriteError(w, apiErr)
			return
		}
		filter.OrgID = orgID
		// UserID stays nil — the result aggregates across every user in the org.

		report, err := cfg.Store.QueryUsage(r.Context(), filter)
		if err != nil {
			WriteError(w, ErrInternal("failed to query usage"))
			return
		}

		if report.Groups == nil {
			report.Groups = []UsageGroup{}
		}

		writeJSON(w, http.StatusOK, report)
	}
}
