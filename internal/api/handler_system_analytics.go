package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

// SystemAnalyticsAggregator is the storage surface consumed by tier-C
// analytics. Implemented by storage/admin.AggregatesStore.
type SystemAnalyticsAggregator interface {
	SystemMemoryCounts(ctx context.Context) (MemoryCountsData, error)
	RecallDistribution(ctx context.Context, orgID *uuid.UUID) ([]HistogramBucket, error)
	OrgBreakdown(ctx context.Context) ([]OrgAggregate, error)
	EntityTypeHistogram(ctx context.Context, orgID *uuid.UUID) ([]TypeBucket, error)
	RelationshipTypeHistogram(ctx context.Context, orgID *uuid.UUID) ([]TypeBucket, error)
	SystemEnrichmentStats(ctx context.Context) (EnrichmentStatsData, error)
}

// SystemAnalyticsConfig wires NewSystemAnalyticsHandler.
type SystemAnalyticsConfig struct {
	Store SystemAnalyticsAggregator
}

// NewSystemAnalyticsHandler returns the tier-C analytics handler at
// /v1/admin/system/analytics. Returns system-wide totals + per-org
// breakdown rows + recall-distribution + entity/relationship type
// histograms. No per-user data, no content fields.
//
// Authorization: enforced at the route level via RequireRole(Administrator).
func NewSystemAnalyticsHandler(cfg SystemAnalyticsConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		ctx := r.Context()

		counts, err := cfg.Store.SystemMemoryCounts(ctx)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve memory counts"))
			return
		}

		dist, err := cfg.Store.RecallDistribution(ctx, nil)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve recall distribution"))
			return
		}

		orgs, err := cfg.Store.OrgBreakdown(ctx)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve org breakdown"))
			return
		}

		entityHist, err := cfg.Store.EntityTypeHistogram(ctx, nil)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve entity histogram"))
			return
		}

		relHist, err := cfg.Store.RelationshipTypeHistogram(ctx, nil)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve relationship histogram"))
			return
		}

		enrichStats, err := cfg.Store.SystemEnrichmentStats(ctx)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve enrichment stats"))
			return
		}

		writeJSON(w, http.StatusOK, SystemAnalyticsData{
			TotalMemoryCounts:         counts,
			RecallDistribution:        dist,
			EnrichmentStats:           enrichStats,
			OrgBreakdown:              orgs,
			EntityTypeHistogram:       entityHist,
			RelationshipTypeHistogram: relHist,
		})
	}
}
