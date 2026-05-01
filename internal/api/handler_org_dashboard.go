package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// OrgDashboardAggregator is the storage surface consumed by tier-B
// dashboard. Implemented by storage/admin.AggregatesStore.
type OrgDashboardAggregator interface {
	OrgMemoryCounts(ctx context.Context, orgID uuid.UUID) (MemoryCountsData, error)
	UserBreakdown(ctx context.Context, orgID uuid.UUID) ([]UserAggregate, error)
	OrgEnrichmentQueueStats(ctx context.Context, orgID uuid.UUID) (*DashboardQueueStats, error)
	ActivityHistogram(ctx context.Context, orgID *uuid.UUID, days int) ([]DailyBucket, error)
}

// OrgDashboardAuditQuery wraps AuditStore.Query for the org-tier activity
// feed. Separate interface so the handler can be unit-tested without the
// full AuditStore.
type OrgDashboardAuditQuery interface {
	Query(ctx context.Context, scope AuditScope, since time.Time, limit int) ([]AuditEvent, error)
}

// OrgDashboardConfig wires NewOrgDashboardHandler.
type OrgDashboardConfig struct {
	Store OrgDashboardAggregator
	Audit OrgDashboardAuditQuery
}

// NewOrgDashboardHandler returns the tier-B dashboard handler at
// /v1/orgs/{org_id}/dashboard. Returns aggregate counts + per-project
// breakdown within the org. No row-level user data, no content fields.
func NewOrgDashboardHandler(cfg OrgDashboardConfig) http.HandlerFunc {
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

		ctx := r.Context()

		counts, err := cfg.Store.OrgMemoryCounts(ctx, *orgID)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve org dashboard"))
			return
		}

		users, err := cfg.Store.UserBreakdown(ctx, *orgID)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve user breakdown"))
			return
		}
		if users == nil {
			users = []UserAggregate{}
		}

		// Org-level totals are derivable from the per-user breakdown:
		// projects/entities are user-owned, so summing per-user counts gives
		// the org total. Memory counts come from OrgMemoryCounts above —
		// it's authoritative across the whole org subtree (not just under
		// users).
		var projCount, entityCount int
		for _, u := range users {
			projCount += u.TotalProjects
			entityCount += u.TotalEntities
		}

		queueStats, err := cfg.Store.OrgEnrichmentQueueStats(ctx, *orgID)
		if err != nil {
			queueStats = nil
		}

		writeJSON(w, http.StatusOK, OrgDashboardData{
			TotalMemories:   counts.Total,
			TotalProjects:   projCount,
			TotalUsers:      len(users),
			TotalEntities:   entityCount,
			UserBreakdown:   users,
			EnrichmentQueue: queueStats,
		})
	}
}

// NewOrgActivityHandler returns the tier-B activity feed at
// /v1/orgs/{org_id}/activity. Per-day creation histogram + audit events
// whose target_org_id matches.
func NewOrgActivityHandler(cfg OrgDashboardConfig) http.HandlerFunc {
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

		days := 30
		if raw := r.URL.Query().Get("days"); raw != "" {
			if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 365 {
				days = n
			}
		}

		ctx := r.Context()

		hist, err := cfg.Store.ActivityHistogram(ctx, orgID, days)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve activity histogram"))
			return
		}

		var events []AuditEvent
		if cfg.Audit != nil {
			events, err = cfg.Audit.Query(ctx, AuditScope{TargetOrgID: orgID}, time.Time{}, 100)
			if err != nil {
				events = []AuditEvent{}
			}
		} else {
			events = []AuditEvent{}
		}

		writeJSON(w, http.StatusOK, OrgActivityResponse{
			DailyCreation: hist,
			AuditEvents:   events,
		})
	}
}
