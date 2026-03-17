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

func (s *DashboardStore) DashboardStats(ctx context.Context, orgID *uuid.UUID) (*api.DashboardStatsData, error) {
	stats := &api.DashboardStatsData{
		MemoriesByProject: []api.ProjectMemoryCount{},
	}

	if orgID == nil {
		// Global counts — no org filter.
		counts := []struct {
			table string
			dest  *int
			where string
		}{
			{"memories", &stats.TotalMemories, " WHERE deleted_at IS NULL"},
			{"projects", &stats.TotalProjects, ""},
			{"users", &stats.TotalUsers, ""},
			{"entities", &stats.TotalEntities, ""},
			{"organizations", &stats.TotalOrgs, ""},
		}
		for _, c := range counts {
			row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM "+c.table+c.where)
			if err := row.Scan(c.dest); err != nil {
				return nil, fmt.Errorf("dashboard count %s: %w", c.table, err)
			}
		}
	} else {
		// Org-scoped counts.
		orgIDStr := orgID.String()

		// Users in this org.
		row := s.db.QueryRow(ctx, s.orgUserCountQuery(), orgIDStr)
		if err := row.Scan(&stats.TotalUsers); err != nil {
			return nil, fmt.Errorf("dashboard org user count: %w", err)
		}

		// Memories under this org's namespace prefix.
		row = s.db.QueryRow(ctx, s.orgMemoryCountQuery(), orgIDStr)
		if err := row.Scan(&stats.TotalMemories); err != nil {
			return nil, fmt.Errorf("dashboard org memory count: %w", err)
		}

		// Projects under this org's namespace prefix.
		row = s.db.QueryRow(ctx, s.orgProjectCountQuery(), orgIDStr)
		if err := row.Scan(&stats.TotalProjects); err != nil {
			return nil, fmt.Errorf("dashboard org project count: %w", err)
		}

		// Entities under this org's namespace prefix.
		row = s.db.QueryRow(ctx, s.orgEntityCountQuery(), orgIDStr)
		if err := row.Scan(&stats.TotalEntities); err != nil {
			return nil, fmt.Errorf("dashboard org entity count: %w", err)
		}

		// Org count is always 1 when scoped.
		stats.TotalOrgs = 1
	}

	// Memories by project.
	var memoriesByProjectQuery string
	var memoriesByProjectArgs []interface{}
	if orgID == nil {
		memoriesByProjectQuery = `SELECT p.id, p.name, COUNT(m.id) as count
			FROM projects p
			LEFT JOIN memories m ON m.namespace_id = p.namespace_id AND m.deleted_at IS NULL
			GROUP BY p.id, p.name
			ORDER BY count DESC
			LIMIT 10`
	} else {
		memoriesByProjectQuery = s.orgMemoriesByProjectQuery()
		memoriesByProjectArgs = []interface{}{orgID.String()}
	}

	rows, err := s.db.Query(ctx, memoriesByProjectQuery, memoriesByProjectArgs...)
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

func (s *DashboardStore) RecentActivity(ctx context.Context, limit int, orgID *uuid.UUID) ([]api.ActivityEvent, error) {
	var query string
	var args []interface{}

	if orgID == nil {
		query = `SELECT id, content, created_at FROM memories
			WHERE deleted_at IS NULL
			ORDER BY created_at DESC LIMIT ?`
		if s.db.Backend() == storage.BackendPostgres {
			query = `SELECT id, content, created_at FROM memories
				WHERE deleted_at IS NULL
				ORDER BY created_at DESC LIMIT $1`
		}
		args = []interface{}{limit}
	} else {
		query = s.orgRecentActivityQuery()
		args = []interface{}{orgID.String(), limit}
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("recent activity: %w", err)
	}
	defer rows.Close()

	events := []api.ActivityEvent{}
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

// --- org-scoped SQL helpers ---

func (s *DashboardStore) orgUserCountQuery() string {
	if s.db.Backend() == storage.BackendPostgres {
		return `SELECT COUNT(*) FROM users WHERE org_id = $1`
	}
	return `SELECT COUNT(*) FROM users WHERE org_id = ?`
}

func (s *DashboardStore) orgMemoryCountQuery() string {
	if s.db.Backend() == storage.BackendPostgres {
		return `SELECT COUNT(*) FROM memories m
			JOIN namespaces mn ON m.namespace_id = mn.id
			WHERE mn.path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
			AND m.deleted_at IS NULL`
	}
	return `SELECT COUNT(*) FROM memories m
		JOIN namespaces mn ON m.namespace_id = mn.id
		WHERE mn.path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
		AND m.deleted_at IS NULL`
}

func (s *DashboardStore) orgProjectCountQuery() string {
	if s.db.Backend() == storage.BackendPostgres {
		return `SELECT COUNT(*) FROM projects p
			JOIN namespaces pn ON p.namespace_id = pn.id
			WHERE pn.path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)`
	}
	return `SELECT COUNT(*) FROM projects p
		JOIN namespaces pn ON p.namespace_id = pn.id
		WHERE pn.path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)`
}

func (s *DashboardStore) orgEntityCountQuery() string {
	if s.db.Backend() == storage.BackendPostgres {
		return `SELECT COUNT(*) FROM entities e
			JOIN namespaces en ON e.namespace_id = en.id
			WHERE en.path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)`
	}
	return `SELECT COUNT(*) FROM entities e
		JOIN namespaces en ON e.namespace_id = en.id
		WHERE en.path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)`
}

func (s *DashboardStore) orgMemoriesByProjectQuery() string {
	if s.db.Backend() == storage.BackendPostgres {
		return `SELECT p.id, p.name, COUNT(m.id) as count
			FROM projects p
			JOIN namespaces pn ON p.namespace_id = pn.id
			LEFT JOIN memories m ON m.namespace_id = p.namespace_id AND m.deleted_at IS NULL
			WHERE pn.path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
			GROUP BY p.id, p.name
			ORDER BY count DESC
			LIMIT 10`
	}
	return `SELECT p.id, p.name, COUNT(m.id) as count
		FROM projects p
		JOIN namespaces pn ON p.namespace_id = pn.id
		LEFT JOIN memories m ON m.namespace_id = p.namespace_id AND m.deleted_at IS NULL
		WHERE pn.path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
		GROUP BY p.id, p.name
		ORDER BY count DESC
		LIMIT 10`
}

func (s *DashboardStore) orgRecentActivityQuery() string {
	if s.db.Backend() == storage.BackendPostgres {
		return `SELECT m.id, m.content, m.created_at FROM memories m
			JOIN namespaces mn ON m.namespace_id = mn.id
			WHERE mn.path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
			AND m.deleted_at IS NULL
			ORDER BY m.created_at DESC LIMIT $2`
	}
	return `SELECT m.id, m.content, m.created_at FROM memories m
		JOIN namespaces mn ON m.namespace_id = mn.id
		WHERE mn.path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
		AND m.deleted_at IS NULL
		ORDER BY m.created_at DESC LIMIT ?`
}
