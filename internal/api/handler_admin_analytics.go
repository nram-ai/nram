package api

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// AnalyticsStore abstracts retrieval of memory analytics data.
// When orgID is non-nil, results are scoped to that organization.
type AnalyticsStore interface {
	GetAnalytics(ctx context.Context, orgID *uuid.UUID) (*AnalyticsData, error)
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
type MemoryRankItem struct {
	ID          uuid.UUID  `json:"id"`
	Content     string     `json:"content"`
	AccessCount int        `json:"access_count"`
	ProjectID   *uuid.UUID `json:"project_id,omitempty"`
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

		orgID := resolveOrgScope(r)

		data, err := cfg.Store.GetAnalytics(r.Context(), orgID)
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
