package api

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// UsageStore abstracts access to token usage aggregation queries.
type UsageStore interface {
	QueryUsage(ctx context.Context, filter UsageFilter) (*UsageReport, error)
}

// UsageConfig holds the dependencies for the admin usage handler.
type UsageConfig struct {
	Store UsageStore
}

// UsageFilter specifies filtering and grouping criteria for usage queries.
type UsageFilter struct {
	OrgID       *uuid.UUID `json:"org_id,omitempty"`
	UserID      *uuid.UUID `json:"user_id,omitempty"`
	ProjectID   *uuid.UUID `json:"project_id,omitempty"`
	From        *time.Time `json:"from,omitempty"`
	To          *time.Time `json:"to,omitempty"`
	GroupBy     string     `json:"group_by"`
	SuccessOnly *bool      `json:"success_only,omitempty"`
}

// UsageReport contains the aggregated usage data returned by QueryUsage.
type UsageReport struct {
	Groups []UsageGroup `json:"groups"`
	Totals UsageTotals  `json:"totals"`
}

// UsageGroup represents a single aggregation bucket in a usage report. The
// cache counts are a subset of TokensInput; see provider.TokenUsage.
type UsageGroup struct {
	Key              string  `json:"key"`
	TokensInput      int64   `json:"tokens_input"`
	TokensOutput     int64   `json:"tokens_output"`
	TokensCacheRead  int64   `json:"tokens_cache_read"`
	TokensCacheWrite int64   `json:"tokens_cache_write"`
	CallCount        int64   `json:"call_count"`
	SuccessCount     int64   `json:"success_count"`
	ErrorCount       int64   `json:"error_count"`
	AvgLatencyMs     float64 `json:"avg_latency_ms"`
}

// UsageTotals holds the sum totals across all groups in a usage report.
type UsageTotals struct {
	TokensInput      int64 `json:"tokens_input"`
	TokensOutput     int64 `json:"tokens_output"`
	TokensCacheRead  int64 `json:"tokens_cache_read"`
	TokensCacheWrite int64 `json:"tokens_cache_write"`
	CallCount        int64 `json:"call_count"`
}

// validGroupByValues contains the allowed values for the group_by parameter.
var validGroupByValues = map[string]bool{
	"org":        true,
	"user":       true,
	"project":    true,
	"operation":  true,
	"model":      true,
	"provider":   true,
	"success":    true,
	"error_code": true,
	"request_id": true,
}

// NewAdminUsageHandler returns an http.HandlerFunc that serves GET /v1/admin/usage.
// It parses query parameters into a UsageFilter, calls QueryUsage on the store,
// and returns the resulting UsageReport as JSON.
func NewAdminUsageHandler(cfg UsageConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		q := r.URL.Query()

		var filter UsageFilter

		// Self-tier: caller's own usage. Admin viewing /v1/usage gets admin's
		// own usage (not system-wide). Cross-org usage drill-down moves to
		// /v1/admin/system/usage and /v1/orgs/{org_id}/usage with their own
		// handlers.
		filter.OrgID, filter.UserID = SelfScope(auth.FromContext(r.Context()))

		// Project filter allowed for all roles within their resolved scope.
		if raw := q.Get("project"); raw != "" {
			if id, err := uuid.Parse(raw); err == nil {
				filter.ProjectID = &id
			}
		}

		// Parse from date.
		if raw := q.Get("from"); raw != "" {
			if t, err := time.Parse(time.RFC3339, raw); err == nil {
				filter.From = &t
			}
		}

		// Parse to date.
		if raw := q.Get("to"); raw != "" {
			if t, err := time.Parse(time.RFC3339, raw); err == nil {
				filter.To = &t
			}
		}

		// Parse and validate group_by.
		groupBy := q.Get("group_by")
		if groupBy == "" {
			groupBy = "operation"
		}
		if !validGroupByValues[groupBy] {
			WriteError(w, ErrBadRequest("invalid group_by value; must be one of: org, user, project, operation, model, provider, success, error_code, request_id"))
			return
		}
		filter.GroupBy = groupBy

		// success_only=true filters out rows recorded for failed provider
		// calls so billing/cost rollups exclude error noise.
		if raw := q.Get("success_only"); raw != "" {
			switch raw {
			case "true", "1":
				v := true
				filter.SuccessOnly = &v
			case "false", "0":
				v := false
				filter.SuccessOnly = &v
			}
		}

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
