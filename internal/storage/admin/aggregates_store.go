package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// AggregatesStore implements the aggregate-shape methods consumed by tier-B
// (org-aggregate) and tier-C (system-aggregate) handlers. These methods
// return only counts, distributions, and tenancy-metadata labels — never
// memory content, entity names, or relationship labels.
type AggregatesStore struct {
	db storage.DB
}

// NewAggregatesStore creates a new AggregatesStore.
func NewAggregatesStore(db storage.DB) *AggregatesStore {
	return &AggregatesStore{db: db}
}

// recallBuckets defines the histogram cut points for memory recall counts.
// Each row is (label, lower-inclusive, upper-inclusive). -1 upper means
// open-ended.
var recallBuckets = []struct {
	Label string
	Lo    int
	Hi    int
}{
	{"0", 0, 0},
	{"1-2", 1, 2},
	{"3-10", 3, 10},
	{"11-50", 11, 50},
	{"51+", 51, -1},
}

// RecallDistribution returns recall-bucket counts for memories matching the
// given scope. orgID nil = system-wide. One pass, one query: we use a CASE
// expression to bucket access_count and GROUP BY the resulting label.
func (s *AggregatesStore) RecallDistribution(ctx context.Context, orgID *uuid.UUID) ([]api.HistogramBucket, error) {
	bucketCase := `CASE
		WHEN m.access_count = 0 THEN '0'
		WHEN m.access_count BETWEEN 1 AND 2 THEN '1-2'
		WHEN m.access_count BETWEEN 3 AND 10 THEN '3-10'
		WHEN m.access_count BETWEEN 11 AND 50 THEN '11-50'
		ELSE '51+'
	END`

	var query string
	var args []interface{}
	if orgID == nil {
		query = "SELECT " + bucketCase + " AS bucket, COUNT(*) FROM memories m " +
			"WHERE m.deleted_at IS NULL GROUP BY bucket"
	} else {
		ph := "?"
		if s.db.Backend() == storage.BackendPostgres {
			ph = "$1"
		}
		query = "SELECT " + bucketCase + " AS bucket, COUNT(*) FROM memories m " +
			"JOIN namespaces mn ON m.namespace_id = mn.id " +
			"WHERE mn.path LIKE (SELECT n.path || '/%' FROM namespaces n " +
			"JOIN organizations o ON o.namespace_id = n.id WHERE o.id = " + ph + ") " +
			"AND m.deleted_at IS NULL GROUP BY bucket"
		args = []interface{}{orgID.String()}
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("recall distribution: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int, len(recallBuckets))
	for rows.Next() {
		var label string
		var count int
		if err := rows.Scan(&label, &count); err != nil {
			return nil, fmt.Errorf("recall distribution scan: %w", err)
		}
		counts[label] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recall distribution rows: %w", err)
	}

	// Emit buckets in declared order, zero-filling labels with no rows.
	out := make([]api.HistogramBucket, 0, len(recallBuckets))
	for _, b := range recallBuckets {
		out = append(out, api.HistogramBucket{Range: b.Label, Count: counts[b.Label]})
	}
	return out, nil
}

// OrgBreakdown returns one OrgAggregate per organization. System-wide;
// caller must already be admin-gated. No content, no per-user data.
func (s *AggregatesStore) OrgBreakdown(ctx context.Context) ([]api.OrgAggregate, error) {
	// One pass per stat, joined to organizations. The N+1 cost is bounded
	// by the org count and we trade it for SQL portability.
	q := `SELECT o.id, o.name, o.namespace_id FROM organizations o ORDER BY o.name`
	rows, err := s.db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("org breakdown list: %w", err)
	}

	type orgRow struct {
		ID    uuid.UUID
		Name  string
		NsID  uuid.UUID
	}
	var orgs []orgRow
	for rows.Next() {
		var idStr, name, nsIDStr string
		if err := rows.Scan(&idStr, &name, &nsIDStr); err != nil {
			rows.Close()
			return nil, fmt.Errorf("org breakdown scan: %w", err)
		}
		id, err1 := uuid.Parse(idStr)
		nsID, err2 := uuid.Parse(nsIDStr)
		if err1 != nil || err2 != nil {
			continue
		}
		orgs = append(orgs, orgRow{ID: id, Name: name, NsID: nsID})
	}
	rows.Close()

	out := make([]api.OrgAggregate, 0, len(orgs))
	for _, o := range orgs {
		oid := o.ID
		agg := api.OrgAggregate{OrgID: oid, OrgName: o.Name}

		var memCount, projCount, userCount, entityCount int

		// Memories under org's namespace prefix.
		ph1 := s.namespacePrefixSubquery("organizations", "o.id", "$1", "?")
		row := s.db.QueryRow(ctx,
			`SELECT COUNT(*) FROM memories m
				JOIN namespaces mn ON m.namespace_id = mn.id
				WHERE mn.path LIKE `+ph1+` AND m.deleted_at IS NULL`,
			oid.String())
		if err := row.Scan(&memCount); err == nil {
			agg.TotalMemories = memCount
		}

		// Projects under org's namespace prefix.
		row = s.db.QueryRow(ctx,
			`SELECT COUNT(*) FROM projects p
				JOIN namespaces pn ON p.namespace_id = pn.id
				WHERE pn.path LIKE `+ph1,
			oid.String())
		if err := row.Scan(&projCount); err == nil {
			agg.TotalProjects = projCount
		}

		// Users in this org.
		userPlaceholder := "?"
		if s.db.Backend() == storage.BackendPostgres {
			userPlaceholder = "$1"
		}
		row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM users WHERE org_id = "+userPlaceholder, oid.String())
		if err := row.Scan(&userCount); err == nil {
			agg.TotalUsers = userCount
		}

		// Entities under org's namespace prefix.
		row = s.db.QueryRow(ctx,
			`SELECT COUNT(*) FROM entities e
				JOIN namespaces en ON e.namespace_id = en.id
				WHERE en.path LIKE `+ph1,
			oid.String())
		if err := row.Scan(&entityCount); err == nil {
			agg.TotalEntities = entityCount
		}

		out = append(out, agg)
	}
	return out, nil
}

// namespacePrefixSubquery builds the LIKE-prefix subquery used to match
// rows whose namespace.path is descended from a given parent (org or user).
// Returns a SQL fragment to inline. parentTable is "organizations" or
// "users"; idCol is the column name on that table to filter by; pgPlace and
// sqlitePlace are the bind placeholders.
func (s *AggregatesStore) namespacePrefixSubquery(parentTable, idCol, pgPlace, sqlitePlace string) string {
	if s.db.Backend() == storage.BackendPostgres {
		return fmt.Sprintf(`(SELECT n.path || '/' || '%%' FROM namespaces n JOIN %s p ON p.namespace_id = n.id WHERE %s = %s)`,
			parentTable, idCol, pgPlace)
	}
	return fmt.Sprintf(`(SELECT n.path || '/%%' FROM namespaces n JOIN %s p ON p.namespace_id = n.id WHERE %s = %s)`,
		parentTable, idCol, sqlitePlace)
}

// EntityTypeHistogram returns counts grouped by entity_type. Scope follows
// the same convention as RecallDistribution.
func (s *AggregatesStore) EntityTypeHistogram(ctx context.Context, orgID *uuid.UUID) ([]api.TypeBucket, error) {
	var query string
	var args []interface{}

	if orgID == nil {
		query = `SELECT entity_type, COUNT(*) FROM entities GROUP BY entity_type ORDER BY COUNT(*) DESC`
	} else {
		ph := s.namespacePrefixSubquery("organizations", "o.id", "$1", "?")
		query = `SELECT e.entity_type, COUNT(*) FROM entities e
			JOIN namespaces en ON e.namespace_id = en.id
			WHERE en.path LIKE ` + ph + `
			GROUP BY e.entity_type ORDER BY COUNT(*) DESC`
		args = []interface{}{orgID.String()}
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("entity type histogram: %w", err)
	}
	defer rows.Close()

	out := []api.TypeBucket{}
	for rows.Next() {
		var t string
		var c int
		if err := rows.Scan(&t, &c); err != nil {
			return nil, fmt.Errorf("entity type histogram scan: %w", err)
		}
		out = append(out, api.TypeBucket{Type: t, Count: c})
	}
	return out, nil
}

// RelationshipTypeHistogram returns counts grouped by relation label.
func (s *AggregatesStore) RelationshipTypeHistogram(ctx context.Context, orgID *uuid.UUID) ([]api.TypeBucket, error) {
	var query string
	var args []interface{}

	if orgID == nil {
		query = `SELECT relation, COUNT(*) FROM relationships
			WHERE valid_until IS NULL
			GROUP BY relation ORDER BY COUNT(*) DESC`
	} else {
		ph := s.namespacePrefixSubquery("organizations", "o.id", "$1", "?")
		query = `SELECT r.relation, COUNT(*) FROM relationships r
			JOIN namespaces rn ON r.namespace_id = rn.id
			WHERE rn.path LIKE ` + ph + ` AND r.valid_until IS NULL
			GROUP BY r.relation ORDER BY COUNT(*) DESC`
		args = []interface{}{orgID.String()}
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("relationship type histogram: %w", err)
	}
	defer rows.Close()

	out := []api.TypeBucket{}
	for rows.Next() {
		var t string
		var c int
		if err := rows.Scan(&t, &c); err != nil {
			return nil, fmt.Errorf("relationship type histogram scan: %w", err)
		}
		out = append(out, api.TypeBucket{Type: t, Count: c})
	}
	return out, nil
}

// ProjectBreakdown returns one ProjectAggregate row per project in the
// given org. Used by tier-B (org-aggregate) handlers. No content fields
// — only project metadata + memory counts.
func (s *AggregatesStore) ProjectBreakdown(ctx context.Context, orgID uuid.UUID) ([]api.ProjectAggregate, error) {
	ph := s.namespacePrefixSubquery("organizations", "o.id", "$1", "?")
	var query string
	if s.db.Backend() == storage.BackendPostgres {
		query = `SELECT p.id, p.name, COUNT(m.id)
			FROM projects p
			JOIN namespaces pn ON p.namespace_id = pn.id
			LEFT JOIN memories m ON m.namespace_id = p.namespace_id AND m.deleted_at IS NULL
			WHERE pn.path LIKE ` + ph + `
			GROUP BY p.id, p.name
			ORDER BY COUNT(m.id) DESC`
	} else {
		query = `SELECT p.id, p.name, COUNT(m.id)
			FROM projects p
			JOIN namespaces pn ON p.namespace_id = pn.id
			LEFT JOIN memories m ON m.namespace_id = p.namespace_id AND m.deleted_at IS NULL
			WHERE pn.path LIKE ` + ph + `
			GROUP BY p.id, p.name
			ORDER BY COUNT(m.id) DESC`
	}

	rows, err := s.db.Query(ctx, query, orgID.String())
	if err != nil {
		return nil, fmt.Errorf("project breakdown: %w", err)
	}
	defer rows.Close()

	out := []api.ProjectAggregate{}
	for rows.Next() {
		var idStr, name string
		var count int
		if err := rows.Scan(&idStr, &name, &count); err != nil {
			return nil, fmt.Errorf("project breakdown scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		out = append(out, api.ProjectAggregate{
			ProjectID:     id,
			ProjectName:   name,
			TotalMemories: count,
		})
	}
	return out, nil
}

// SystemMemoryCounts returns the total/active/deleted/enriched counts
// across the entire system. Used by tier-C system dashboard.
func (s *AggregatesStore) SystemMemoryCounts(ctx context.Context) (api.MemoryCountsData, error) {
	var counts api.MemoryCountsData

	row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM memories")
	if err := row.Scan(&counts.Total); err != nil {
		return counts, fmt.Errorf("system memory total: %w", err)
	}

	row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM memories WHERE deleted_at IS NULL")
	if err := row.Scan(&counts.Active); err != nil {
		return counts, fmt.Errorf("system memory active: %w", err)
	}

	row = s.db.QueryRow(ctx, "SELECT COUNT(*) FROM memories WHERE deleted_at IS NOT NULL")
	if err := row.Scan(&counts.Deleted); err != nil {
		return counts, fmt.Errorf("system memory deleted: %w", err)
	}

	enrichedQ := "SELECT COUNT(*) FROM memories WHERE enriched = 1 AND deleted_at IS NULL"
	if s.db.Backend() == storage.BackendPostgres {
		enrichedQ = "SELECT COUNT(*) FROM memories WHERE enriched = true AND deleted_at IS NULL"
	}
	row = s.db.QueryRow(ctx, enrichedQ)
	if err := row.Scan(&counts.Enriched); err != nil {
		return counts, fmt.Errorf("system memory enriched: %w", err)
	}

	return counts, nil
}

// OrgMemoryCounts returns total/active/deleted/enriched counts within an
// org's namespace subtree.
func (s *AggregatesStore) OrgMemoryCounts(ctx context.Context, orgID uuid.UUID) (api.MemoryCountsData, error) {
	var counts api.MemoryCountsData
	ph := s.namespacePrefixSubquery("organizations", "o.id", "$1", "?")

	totalQ := `SELECT COUNT(*) FROM memories m
		JOIN namespaces mn ON m.namespace_id = mn.id
		WHERE mn.path LIKE ` + ph
	row := s.db.QueryRow(ctx, totalQ, orgID.String())
	if err := row.Scan(&counts.Total); err != nil {
		return counts, fmt.Errorf("org memory total: %w", err)
	}

	row = s.db.QueryRow(ctx, totalQ+" AND m.deleted_at IS NULL", orgID.String())
	if err := row.Scan(&counts.Active); err != nil {
		return counts, fmt.Errorf("org memory active: %w", err)
	}

	row = s.db.QueryRow(ctx, totalQ+" AND m.deleted_at IS NOT NULL", orgID.String())
	if err := row.Scan(&counts.Deleted); err != nil {
		return counts, fmt.Errorf("org memory deleted: %w", err)
	}

	enrichedFilter := " AND m.enriched = 1 AND m.deleted_at IS NULL"
	if s.db.Backend() == storage.BackendPostgres {
		enrichedFilter = " AND m.enriched = true AND m.deleted_at IS NULL"
	}
	row = s.db.QueryRow(ctx, totalQ+enrichedFilter, orgID.String())
	if err := row.Scan(&counts.Enriched); err != nil {
		return counts, fmt.Errorf("org memory enriched: %w", err)
	}

	return counts, nil
}

// SystemEnrichmentStats returns enrichment-queue success/failure rates
// across the system. Tier-C only — enrichment_queue lacks per-org
// attribution.
func (s *AggregatesStore) SystemEnrichmentStats(ctx context.Context) (api.EnrichmentStatsData, error) {
	var stats api.EnrichmentStatsData

	rows, err := s.db.Query(ctx,
		"SELECT status, COUNT(*) FROM enrichment_queue "+
			"WHERE status IN ('completed', 'failed') GROUP BY status")
	if err != nil {
		return stats, fmt.Errorf("enrichment stats: %w", err)
	}
	defer rows.Close()

	var completed, failed int
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return stats, fmt.Errorf("enrichment stats scan: %w", err)
		}
		switch status {
		case "completed":
			completed = count
		case "failed":
			failed = count
		}
	}
	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("enrichment stats rows: %w", err)
	}

	total := completed + failed
	stats.TotalProcessed = total
	if total > 0 {
		stats.SuccessRate = float64(completed) / float64(total) * 100
		stats.FailureRate = float64(failed) / float64(total) * 100
	}
	return stats, nil
}

// ActivityHistogram returns daily memory-creation counts for the last `days`
// days. Scope: orgID nil = system-wide; orgID set = within that org's
// namespace subtree.
func (s *AggregatesStore) ActivityHistogram(ctx context.Context, orgID *uuid.UUID, days int) ([]api.DailyBucket, error) {
	if days <= 0 {
		days = 30
	}
	since := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)
	sinceStr := since.Format("2006-01-02T15:04:05Z")

	var query string
	var args []interface{}

	if orgID == nil {
		if s.db.Backend() == storage.BackendPostgres {
			query = `SELECT to_char(created_at::date, 'YYYY-MM-DD'), COUNT(*)
				FROM memories WHERE created_at >= $1 AND deleted_at IS NULL
				GROUP BY created_at::date ORDER BY created_at::date`
		} else {
			query = `SELECT substr(created_at, 1, 10), COUNT(*)
				FROM memories WHERE created_at >= ? AND deleted_at IS NULL
				GROUP BY substr(created_at, 1, 10) ORDER BY substr(created_at, 1, 10)`
		}
		args = []interface{}{sinceStr}
	} else {
		ph := s.namespacePrefixSubquery("organizations", "o.id", "$2", "?")
		if s.db.Backend() == storage.BackendPostgres {
			query = `SELECT to_char(m.created_at::date, 'YYYY-MM-DD'), COUNT(*)
				FROM memories m
				JOIN namespaces mn ON m.namespace_id = mn.id
				WHERE m.created_at >= $1 AND m.deleted_at IS NULL
				AND mn.path LIKE ` + ph + `
				GROUP BY m.created_at::date ORDER BY m.created_at::date`
		} else {
			query = `SELECT substr(m.created_at, 1, 10), COUNT(*)
				FROM memories m
				JOIN namespaces mn ON m.namespace_id = mn.id
				WHERE m.created_at >= ? AND m.deleted_at IS NULL
				AND mn.path LIKE ` + ph + `
				GROUP BY substr(m.created_at, 1, 10) ORDER BY substr(m.created_at, 1, 10)`
		}
		args = []interface{}{sinceStr, orgID.String()}
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("activity histogram: %w", err)
	}
	defer rows.Close()

	out := []api.DailyBucket{}
	for rows.Next() {
		var date string
		var count int
		if err := rows.Scan(&date, &count); err != nil {
			return nil, fmt.Errorf("activity histogram scan: %w", err)
		}
		out = append(out, api.DailyBucket{Date: date, Count: count})
	}
	return out, nil
}
