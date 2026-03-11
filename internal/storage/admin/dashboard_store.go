package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// DashboardStore implements api.DashboardStore using aggregate SQL queries.
type DashboardStore struct {
	db                  storage.DB
	enrichmentQueueRepo *storage.EnrichmentQueueRepo
}

// NewDashboardStore creates a new DashboardStore.
func NewDashboardStore(db storage.DB, enrichmentQueueRepo *storage.EnrichmentQueueRepo) *DashboardStore {
	return &DashboardStore{db: db, enrichmentQueueRepo: enrichmentQueueRepo}
}

func (s *DashboardStore) DashboardStats(ctx context.Context) (*api.DashboardStatsData, error) {
	stats := &api.DashboardStatsData{}

	// Count totals.
	counts := []struct {
		table string
		dest  *int
		where string
	}{
		{"memories", &stats.TotalMemories, " WHERE deleted_at IS NULL"},
		{"projects", &stats.TotalProjects, ""},
		{"users", &stats.TotalUsers, ""},
		{"organizations", &stats.TotalOrgs, ""},
	}
	for _, c := range counts {
		row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM "+c.table+c.where)
		if err := row.Scan(c.dest); err != nil {
			return nil, fmt.Errorf("dashboard count %s: %w", c.table, err)
		}
	}

	// Memories by project.
	query := `SELECT p.id, p.name, COUNT(m.id) as count
		FROM projects p
		LEFT JOIN memories m ON m.namespace_id = p.namespace_id AND m.deleted_at IS NULL
		GROUP BY p.id, p.name
		ORDER BY count DESC
		LIMIT 10`
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("dashboard memories by project: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idStr string
		var item api.ProjectMemoryCount
		if err := rows.Scan(&idStr, &item.ProjectName, &item.Count); err != nil {
			return nil, fmt.Errorf("dashboard scan project memory count: %w", err)
		}
		item.ProjectID, _ = uuid.Parse(idStr)
		stats.MemoriesByProject = append(stats.MemoriesByProject, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dashboard memories by project iteration: %w", err)
	}

	// Enrichment queue stats.
	queueStats, err := s.enrichmentQueueRepo.CountByStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("dashboard queue stats: %w", err)
	}
	stats.EnrichmentQueue = &api.DashboardQueueStats{
		Pending:    queueStats.Pending,
		Processing: queueStats.Processing,
		Failed:     queueStats.Failed,
	}

	return stats, nil
}

func (s *DashboardStore) RecentActivity(ctx context.Context, limit int) ([]api.ActivityEvent, error) {
	// Query recent memories as activity events.
	query := `SELECT id, content, created_at FROM memories
		WHERE deleted_at IS NULL
		ORDER BY created_at DESC LIMIT ?`
	if s.db.Backend() == storage.BackendPostgres {
		query = `SELECT id, content, created_at FROM memories
			WHERE deleted_at IS NULL
			ORDER BY created_at DESC LIMIT $1`
	}

	rows, err := s.db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("recent activity: %w", err)
	}
	defer rows.Close()

	var events []api.ActivityEvent
	for rows.Next() {
		var idStr, content, createdAtStr string
		if err := rows.Scan(&idStr, &content, &createdAtStr); err != nil {
			return nil, fmt.Errorf("recent activity scan: %w", err)
		}
		ts, _ := time.Parse(time.RFC3339, createdAtStr)

		// Truncate content for summary.
		summary := content
		if len(summary) > 100 {
			summary = summary[:100] + "..."
		}

		events = append(events, api.ActivityEvent{
			ID:        idStr,
			Type:      "memory.created",
			Summary:   summary,
			Timestamp: ts,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recent activity iteration: %w", err)
	}

	return events, nil
}
