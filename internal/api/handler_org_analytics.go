package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// OrgAnalyticsAggregator is the storage surface consumed by tier-B
// analytics. Implemented by storage/admin.AggregatesStore.
type OrgAnalyticsAggregator interface {
	OrgMemoryCounts(ctx context.Context, orgID uuid.UUID) (MemoryCountsData, error)
	RecallDistribution(ctx context.Context, orgID *uuid.UUID) ([]HistogramBucket, error)
	UserBreakdown(ctx context.Context, orgID uuid.UUID) ([]UserAggregate, error)
	EntityTypeHistogram(ctx context.Context, orgID *uuid.UUID) ([]TypeBucket, error)
	RelationshipTypeHistogram(ctx context.Context, orgID *uuid.UUID) ([]TypeBucket, error)
}

// OrgAnalyticsConfig wires NewOrgAnalyticsHandler.
type OrgAnalyticsConfig struct {
	Store OrgAnalyticsAggregator
}

// NewOrgAnalyticsHandler returns the tier-B analytics handler at
// /v1/orgs/{org_id}/analytics. Returns aggregate counts, recall-distribution
// histogram, per-user breakdown (memories/projects/entities per user), and
// entity/relationship type histograms for the org. No row-level memory data,
// no content fields. Per-project breakdown was deliberately removed: project
// names in nram are user-owned, so a per-project view inside an org leaks
// individual users' activity to every org_owner.
//
// Authorization: caller must be RoleOrgOwner of {org_id} or RoleAdministrator.
// OrgAccessMiddleware admits members through, so this handler enforces the
// stricter requireOrgOwner gate before serving.
func NewOrgAnalyticsHandler(cfg OrgAnalyticsConfig) http.HandlerFunc {
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
			slog.Error("api: OrgAnalytics OrgMemoryCounts failed", "err", err)
			WriteError(w, ErrInternal("failed to retrieve memory counts"))
			return
		}

		dist, err := cfg.Store.RecallDistribution(ctx, orgID)
		if err != nil {
			slog.Error("api: OrgAnalytics RecallDistribution failed", "err", err)
			WriteError(w, ErrInternal("failed to retrieve recall distribution"))
			return
		}

		users, err := cfg.Store.UserBreakdown(ctx, *orgID)
		if err != nil {
			slog.Error("api: OrgAnalytics UserBreakdown failed", "err", err)
			WriteError(w, ErrInternal("failed to retrieve user breakdown"))
			return
		}

		entityHist, err := cfg.Store.EntityTypeHistogram(ctx, orgID)
		if err != nil {
			slog.Error("api: OrgAnalytics EntityTypeHistogram failed", "err", err)
			WriteError(w, ErrInternal("failed to retrieve entity histogram"))
			return
		}

		relHist, err := cfg.Store.RelationshipTypeHistogram(ctx, orgID)
		if err != nil {
			slog.Error("api: OrgAnalytics RelationshipTypeHistogram failed", "err", err)
			WriteError(w, ErrInternal("failed to retrieve relationship histogram"))
			return
		}

		writeJSON(w, http.StatusOK, OrgAnalyticsData{
			MemoryCounts:              counts,
			RecallDistribution:        dist,
			EnrichmentStats:           EnrichmentStatsData{},
			UserBreakdown:             users,
			EntityTypeHistogram:       entityHist,
			RelationshipTypeHistogram: relHist,
		})
	}
}
