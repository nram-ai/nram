package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// AnalyticsStore implements api.AnalyticsStore using aggregate SQL queries.
type AnalyticsStore struct {
	db storage.DB
}

// NewAnalyticsStore creates a new AnalyticsStore.
func NewAnalyticsStore(db storage.DB) *AnalyticsStore {
	return &AnalyticsStore{db: db}
}

func (s *AnalyticsStore) GetAnalytics(ctx context.Context) (*api.AnalyticsData, error) {
	data := &api.AnalyticsData{}

	// Memory counts.
	row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM memories")
	if err := row.Scan(&data.MemoryCounts.Total); err != nil {
		return nil, fmt.Errorf("analytics total memories: %w", err)
	}

	row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM memories WHERE deleted_at IS NULL")
	if err := row.Scan(&data.MemoryCounts.Active); err != nil {
		return nil, fmt.Errorf("analytics active memories: %w", err)
	}

	row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM memories WHERE deleted_at IS NOT NULL")
	if err := row.Scan(&data.MemoryCounts.Deleted); err != nil {
		return nil, fmt.Errorf("analytics deleted memories: %w", err)
	}

	enrichedQuery := "SELECT COUNT(*) FROM memories WHERE enriched = 1 AND deleted_at IS NULL"
	if s.db.Backend() == storage.BackendPostgres {
		enrichedQuery = "SELECT COUNT(*) FROM memories WHERE enriched = true AND deleted_at IS NULL"
	}
	row = s.db.QueryRow(ctx, enrichedQuery)
	if err := row.Scan(&data.MemoryCounts.Enriched); err != nil {
		return nil, fmt.Errorf("analytics enriched memories: %w", err)
	}

	// Most recalled (top 10 by access count).
	data.MostRecalled = s.queryRankedMemories(ctx, "ORDER BY access_count DESC", 10)

	// Least recalled (bottom 10 by access count, excluding zero).
	data.LeastRecalled = s.queryRankedMemories(ctx, "AND access_count > 0 ORDER BY access_count ASC", 10)

	// Dead weight (zero access count, oldest first).
	data.DeadWeight = s.queryRankedMemories(ctx, "AND access_count = 0 ORDER BY created_at ASC", 10)

	// Enrichment stats.
	var completed, failed int
	row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'completed'")
	if err := row.Scan(&completed); err != nil {
		completed = 0
	}
	row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'failed'")
	if err := row.Scan(&failed); err != nil {
		failed = 0
	}

	total := completed + failed
	data.EnrichmentStats = api.EnrichmentStatsData{
		TotalProcessed: total,
	}
	if total > 0 {
		data.EnrichmentStats.SuccessRate = float64(completed) / float64(total) * 100
		data.EnrichmentStats.FailureRate = float64(failed) / float64(total) * 100
	}

	return data, nil
}

func (s *AnalyticsStore) queryRankedMemories(ctx context.Context, orderClause string, limit int) []api.MemoryRankItem {
	query := fmt.Sprintf(`SELECT id, content, access_count, created_at
		FROM memories WHERE deleted_at IS NULL %s LIMIT %d`, orderClause, limit)

	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var items []api.MemoryRankItem
	for rows.Next() {
		var idStr, content, createdAtStr string
		var accessCount int
		if err := rows.Scan(&idStr, &content, &accessCount, &createdAtStr); err != nil {
			continue
		}
		id, _ := uuid.Parse(idStr)
		createdAt, _ := time.Parse(time.RFC3339, createdAtStr)
		items = append(items, api.MemoryRankItem{
			ID:          id,
			Content:     content,
			AccessCount: accessCount,
			CreatedAt:   createdAt,
		})
	}
	return items
}
