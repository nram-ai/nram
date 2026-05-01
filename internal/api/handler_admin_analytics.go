package api

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// AnalyticsStore abstracts retrieval of memory analytics data.
// When orgID is non-nil, results are scoped to that organization.
// When userID is non-nil, results are further scoped to memories owned by
// that user (via the user's namespace path). Both nil = global.
type AnalyticsStore interface {
	GetAnalytics(ctx context.Context, orgID *uuid.UUID, userID *uuid.UUID) (*AnalyticsData, error)
}

// AnalyticsConfig holds the dependencies for the admin analytics handler.
type AnalyticsConfig struct {
	Store AnalyticsStore
}

// AnalyticsData contains the full analytics response payload.
type AnalyticsData struct {
	MemoryCounts    MemoryCountsData    `json:"memory_counts"`
	MostRecalled    []MemoryRankItem    `json:"most_recalled"`
	LeastRecalled   []MemoryRankItem    `json:"least_recalled"`
	DeadWeight      []MemoryRankItem    `json:"dead_weight"`
	EnrichmentStats EnrichmentStatsData `json:"enrichment_stats"`
}

// MemoryCountsData contains aggregate memory counts.
type MemoryCountsData struct {
	Total    int `json:"total"`
	Active   int `json:"active"`
	Deleted  int `json:"deleted"`
	Enriched int `json:"enriched"`
}

// MemoryRankItem represents a single memory entry in a ranked list.
//
// Privacy: this struct intentionally does NOT carry the memory body. Admin
// surfaces aggregate access patterns over memories the caller owns; rendering
// the body in a ranked-list view leaks content into the dashboard layer where
// it does not belong. Length is exposed as a size hint instead.
type MemoryRankItem struct {
	ID          uuid.UUID  `json:"id"`
	AccessCount int        `json:"access_count"`
	ProjectID   *uuid.UUID `json:"project_id,omitempty"`
	LengthChars int        `json:"length_chars"`
	CreatedAt   time.Time  `json:"created_at"`
}

// EnrichmentStatsData contains enrichment pipeline statistics.
type EnrichmentStatsData struct {
	TotalProcessed int     `json:"total_processed"`
	SuccessRate    float64 `json:"success_rate"`
	FailureRate    float64 `json:"failure_rate"`
	AvgLatencyMs   int64   `json:"avg_latency_ms"`
}

// NewAdminAnalyticsHandler returns an http.HandlerFunc that serves memory
// analytics including counts, recall rankings, dead weight, and enrichment
// statistics. Only GET is accepted.
func NewAdminAnalyticsHandler(cfg AnalyticsConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		// Self-tier: caller's own data. The widening helper (?org=/?user=)
		// was removed; admin viewing /v1/analytics gets admin's own analytics.
		orgID, userID := SelfScope(auth.FromContext(r.Context()))
		data, err := cfg.Store.GetAnalytics(r.Context(), orgID, userID)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve analytics"))
			return
		}

		if data.MostRecalled == nil {
			data.MostRecalled = []MemoryRankItem{}
		}
		if data.LeastRecalled == nil {
			data.LeastRecalled = []MemoryRankItem{}
		}
		if data.DeadWeight == nil {
			data.DeadWeight = []MemoryRankItem{}
		}

		writeJSON(w, http.StatusOK, data)
	}
}
