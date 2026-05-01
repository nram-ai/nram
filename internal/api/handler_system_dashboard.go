package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// SystemDashboardAggregator is the storage surface consumed by tier-C
// dashboard. Implemented by storage/admin.AggregatesStore.
type SystemDashboardAggregator interface {
	SystemMemoryCounts(ctx context.Context) (MemoryCountsData, error)
	OrgBreakdown(ctx context.Context) ([]OrgAggregate, error)
	ActivityHistogram(ctx context.Context, orgID *uuid.UUID, days int) ([]DailyBucket, error)
}

// SystemDashboardConfig wires NewSystemDashboardHandler.
type SystemDashboardConfig struct {
	Store SystemDashboardAggregator
	Audit OrgDashboardAuditQuery // reused — same Query signature
}

// NewSystemDashboardHandler returns the tier-C dashboard handler at
// /v1/admin/system/dashboard. Returns system totals + per-org breakdown
// rows. No per-user, no per-memory, no content.
func NewSystemDashboardHandler(cfg SystemDashboardConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		ctx := r.Context()

		counts, err := cfg.Store.SystemMemoryCounts(ctx)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve system memory counts"))
			return
		}

		orgs, err := cfg.Store.OrgBreakdown(ctx)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve org breakdown"))
			return
		}

		var totalUsers, totalProjects, totalEntities int
		for _, o := range orgs {
			totalUsers += o.TotalUsers
			totalProjects += o.TotalProjects
			totalEntities += o.TotalEntities
		}

		writeJSON(w, http.StatusOK, SystemDashboardData{
			TotalMemories: counts.Total,
			TotalProjects: totalProjects,
			TotalUsers:    totalUsers,
			TotalEntities: totalEntities,
			TotalOrgs:     len(orgs),
			OrgBreakdown:  orgs,
		})
	}
}

// NewSystemActivityHandler returns the tier-C activity feed at
// /v1/admin/system/activity. Returns system-wide daily creation histogram
// + the full audit-event stream (no scope filter).
func NewSystemActivityHandler(cfg SystemDashboardConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		days := 30
		if raw := r.URL.Query().Get("days"); raw != "" {
			if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 365 {
				days = n
			}
		}

		ctx := r.Context()

		hist, err := cfg.Store.ActivityHistogram(ctx, nil, days)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve activity histogram"))
			return
		}

		var events []AuditEvent
		if cfg.Audit != nil {
			events, err = cfg.Audit.Query(ctx, AuditScope{}, time.Time{}, 200)
			if err != nil {
				events = []AuditEvent{}
			}
		} else {
			events = []AuditEvent{}
		}

		writeJSON(w, http.StatusOK, SystemActivityResponse{
			DailyCreation: hist,
			AuditEvents:   events,
		})
	}
}
