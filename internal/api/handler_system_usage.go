package api

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

// NewSystemUsageHandler returns the tier-C usage handler at
// /v1/admin/system/usage. Returns token-usage aggregates with no org/user
// scope filter (system-wide). Authorization is enforced at the route level
// via RequireRole(Administrator).
func NewSystemUsageHandler(cfg UsageConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		filter, apiErr := parseUsageQueryParams(r)
		if apiErr != nil {
			WriteError(w, apiErr)
			return
		}
		// System tier: no OrgID, no UserID — query is unscoped.

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

// parseUsageQueryParams reads project/from/to/group_by/success_only from the
// query string into a UsageFilter. It does NOT set OrgID/UserID — each tier
// handler applies its own scope after calling this.
func parseUsageQueryParams(r *http.Request) (UsageFilter, *APIError) {
	q := r.URL.Query()

	var filter UsageFilter

	if raw := q.Get("project"); raw != "" {
		if id, err := uuid.Parse(raw); err == nil {
			filter.ProjectID = &id
		}
	}

	if raw := q.Get("from"); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			filter.From = &t
		}
	}

	if raw := q.Get("to"); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			filter.To = &t
		}
	}

	groupBy := q.Get("group_by")
	if groupBy == "" {
		groupBy = "operation"
	}
	if !validGroupByValues[groupBy] {
		return filter, ErrBadRequest("invalid group_by value; must be one of: org, user, project, operation, model, provider, success, error_code, request_id")
	}
	filter.GroupBy = groupBy

	if raw := q.Get("success_only"); raw != "" {
		if raw == "true" || raw == "1" {
			v := true
			filter.SuccessOnly = &v
		} else if raw == "false" || raw == "0" {
			v := false
			filter.SuccessOnly = &v
		}
	}

	return filter, nil
}
