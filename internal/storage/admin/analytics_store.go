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

func (s *AnalyticsStore) GetAnalytics(ctx context.Context, orgID *uuid.UUID, userID *uuid.UUID) (*api.AnalyticsData, error) {
	data := &api.AnalyticsData{}

	switch {
	case userID != nil:
		userIDStr := userID.String()
		row := s.db.QueryRow(ctx, s.userMemoryCountQuery(""), userIDStr)
		if err := row.Scan(&data.MemoryCounts.Total); err != nil {
			return nil, fmt.Errorf("analytics user total memories: %w", err)
		}

		row = s.db.QueryRow(ctx, s.userMemoryCountQuery("AND m.deleted_at IS NULL"), userIDStr)
		if err := row.Scan(&data.MemoryCounts.Active); err != nil {
			return nil, fmt.Errorf("analytics user active memories: %w", err)
		}

		row = s.db.QueryRow(ctx, s.userMemoryCountQuery("AND m.deleted_at IS NOT NULL"), userIDStr)
		if err := row.Scan(&data.MemoryCounts.Deleted); err != nil {
			return nil, fmt.Errorf("analytics user deleted memories: %w", err)
		}

		enrichedFilter := "AND m.enriched = 1 AND m.deleted_at IS NULL"
		if s.db.Backend() == storage.BackendPostgres {
			enrichedFilter = "AND m.enriched = true AND m.deleted_at IS NULL"
		}
		row = s.db.QueryRow(ctx, s.userMemoryCountQuery(enrichedFilter), userIDStr)
		if err := row.Scan(&data.MemoryCounts.Enriched); err != nil {
			return nil, fmt.Errorf("analytics user enriched memories: %w", err)
		}

	case orgID != nil:
		orgIDStr := orgID.String()
		row := s.db.QueryRow(ctx, s.orgMemoryCountQuery(""), orgIDStr)
		if err := row.Scan(&data.MemoryCounts.Total); err != nil {
			return nil, fmt.Errorf("analytics org total memories: %w", err)
		}

		row = s.db.QueryRow(ctx, s.orgMemoryCountQuery("AND m.deleted_at IS NULL"), orgIDStr)
		if err := row.Scan(&data.MemoryCounts.Active); err != nil {
			return nil, fmt.Errorf("analytics org active memories: %w", err)
		}

		row = s.db.QueryRow(ctx, s.orgMemoryCountQuery("AND m.deleted_at IS NOT NULL"), orgIDStr)
		if err := row.Scan(&data.MemoryCounts.Deleted); err != nil {
			return nil, fmt.Errorf("analytics org deleted memories: %w", err)
		}

		enrichedFilter := "AND m.enriched = 1 AND m.deleted_at IS NULL"
		if s.db.Backend() == storage.BackendPostgres {
			enrichedFilter = "AND m.enriched = true AND m.deleted_at IS NULL"
		}
		row = s.db.QueryRow(ctx, s.orgMemoryCountQuery(enrichedFilter), orgIDStr)
		if err := row.Scan(&data.MemoryCounts.Enriched); err != nil {
			return nil, fmt.Errorf("analytics org enriched memories: %w", err)
		}

	default:
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
	}

	// Most recalled (top 10 by access count).
	var err error
	data.MostRecalled, err = s.queryRankedMemories(ctx, "ORDER BY m.access_count DESC", 10, orgID, userID)
	if err != nil {
		return nil, fmt.Errorf("analytics most recalled: %w", err)
	}

	// Least recalled (bottom 10 by access count, excluding zero).
	data.LeastRecalled, err = s.queryRankedMemories(ctx, "AND m.access_count > 0 ORDER BY m.access_count ASC", 10, orgID, userID)
	if err != nil {
		return nil, fmt.Errorf("analytics least recalled: %w", err)
	}

	// Dead weight (zero access count, oldest first).
	data.DeadWeight, err = s.queryRankedMemories(ctx, "AND m.access_count = 0 ORDER BY m.created_at ASC", 10, orgID, userID)
	if err != nil {
		return nil, fmt.Errorf("analytics dead weight: %w", err)
	}

	// Enrichment stats. The enrichment_queue table has no per-org or per-user
	// attribution column, so meaningful stats only exist at the global tier.
	// For org and user tiers, leave the section zeroed; the UI hides the
	// EnrichmentStats card for non-admin tiers.
	if orgID == nil && userID == nil {
		var completed int
		row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'completed'")
		if err := row.Scan(&completed); err != nil {
			return nil, fmt.Errorf("analytics enrichment completed count: %w", err)
		}
		var failed int
		row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'failed'")
		if err := row.Scan(&failed); err != nil {
			return nil, fmt.Errorf("analytics enrichment failed count: %w", err)
		}

		total := completed + failed
		data.EnrichmentStats = api.EnrichmentStatsData{
			TotalProcessed: total,
		}
		if total > 0 {
			data.EnrichmentStats.SuccessRate = float64(completed) / float64(total) * 100
			data.EnrichmentStats.FailureRate = float64(failed) / float64(total) * 100
		}
	}

	return data, nil
}

func (s *AnalyticsStore) queryRankedMemories(ctx context.Context, orderClause string, limit int, orgID *uuid.UUID, userID *uuid.UUID) ([]api.MemoryRankItem, error) {
	// Privacy: SELECT computes LENGTH(m.content) instead of pulling the body.
	// The ranked-list view exposes only the size and access pattern; the text
	// itself never leaves the database.
	var query string
	var args []interface{}

	switch {
	case userID != nil:
		if s.db.Backend() == storage.BackendPostgres {
			query = fmt.Sprintf(`SELECT m.id, LENGTH(m.content), m.access_count, m.created_at
				FROM memories m
				JOIN namespaces mn ON m.namespace_id = mn.id
				WHERE m.deleted_at IS NULL
				AND mn.path LIKE (SELECT n.path || '/' || '%%' FROM namespaces n JOIN users u ON u.namespace_id = n.id WHERE u.id = $1)
				%s LIMIT %d`, orderClause, limit)
		} else {
			query = fmt.Sprintf(`SELECT m.id, LENGTH(m.content), m.access_count, m.created_at
				FROM memories m
				JOIN namespaces mn ON m.namespace_id = mn.id
				WHERE m.deleted_at IS NULL
				AND mn.path LIKE (SELECT n.path || '/%%' FROM namespaces n JOIN users u ON u.namespace_id = n.id WHERE u.id = ?)
				%s LIMIT %d`, orderClause, limit)
		}
		args = []interface{}{userID.String()}

	case orgID != nil:
		if s.db.Backend() == storage.BackendPostgres {
			query = fmt.Sprintf(`SELECT m.id, LENGTH(m.content), m.access_count, m.created_at
				FROM memories m
				JOIN namespaces mn ON m.namespace_id = mn.id
				WHERE m.deleted_at IS NULL
				AND mn.path LIKE (SELECT n.path || '/' || '%%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
				%s LIMIT %d`, orderClause, limit)
		} else {
			query = fmt.Sprintf(`SELECT m.id, LENGTH(m.content), m.access_count, m.created_at
				FROM memories m
				JOIN namespaces mn ON m.namespace_id = mn.id
				WHERE m.deleted_at IS NULL
				AND mn.path LIKE (SELECT n.path || '/%%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
				%s LIMIT %d`, orderClause, limit)
		}
		args = []interface{}{orgID.String()}

	default:
		query = fmt.Sprintf(`SELECT m.id, LENGTH(m.content), m.access_count, m.created_at
			FROM memories m WHERE m.deleted_at IS NULL %s LIMIT %d`, orderClause, limit)
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ranked memories query: %w", err)
	}
	defer rows.Close()

	items := []api.MemoryRankItem{}
	for rows.Next() {
		var idStr, createdAtStr string
		var lengthChars, accessCount int
		if err := rows.Scan(&idStr, &lengthChars, &accessCount, &createdAtStr); err != nil {
			return nil, fmt.Errorf("ranked memories scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("ranked memories parse id: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("ranked memories parse created_at: %w", err)
		}
		items = append(items, api.MemoryRankItem{
			ID:          id,
			AccessCount: accessCount,
			LengthChars: lengthChars,
			CreatedAt:   createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ranked memories iteration: %w", err)
	}
	return items, nil
}

func (s *AnalyticsStore) orgMemoryCountQuery(extraFilter string) string {
	if s.db.Backend() == storage.BackendPostgres {
		return fmt.Sprintf(`SELECT COUNT(*) FROM memories m
			JOIN namespaces mn ON m.namespace_id = mn.id
			WHERE mn.path LIKE (SELECT n.path || '/' || '%%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
			%s`, extraFilter)
	}
	return fmt.Sprintf(`SELECT COUNT(*) FROM memories m
		JOIN namespaces mn ON m.namespace_id = mn.id
		WHERE mn.path LIKE (SELECT n.path || '/%%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
		%s`, extraFilter)
}

func (s *AnalyticsStore) userMemoryCountQuery(extraFilter string) string {
	if s.db.Backend() == storage.BackendPostgres {
		return fmt.Sprintf(`SELECT COUNT(*) FROM memories m
			JOIN namespaces mn ON m.namespace_id = mn.id
			WHERE mn.path LIKE (SELECT n.path || '/' || '%%' FROM namespaces n JOIN users u ON u.namespace_id = n.id WHERE u.id = $1)
			%s`, extraFilter)
	}
	return fmt.Sprintf(`SELECT COUNT(*) FROM memories m
		JOIN namespaces mn ON m.namespace_id = mn.id
		WHERE mn.path LIKE (SELECT n.path || '/%%' FROM namespaces n JOIN users u ON u.namespace_id = n.id WHERE u.id = ?)
		%s`, extraFilter)
}
