package storage

import (
	"context"
	"fmt"
)

// BackfillEmbedJobs enqueues a pending enrichment job (priority -1) for every
// live memory that does not already have a pending or running job. Priority
// -1 drains after live-store jobs. Idempotent: the LEFT JOIN guard means
// repeated runs insert zero new rows.
//
// Exposed via NRAM_ENABLE_EMBED_BACKFILL=1 (startup) and
// --backfill-embeddings (CLI) on cmd/server.
func BackfillEmbedJobs(ctx context.Context, db DB) (int64, error) {
	// Short-circuit if there is nothing to enqueue. Avoids the full-table
	// INSERT ... SELECT in steady state when cron re-runs the backfill.
	present, err := hasUncoveredMemory(ctx, db)
	if err != nil {
		return 0, err
	}
	if !present {
		return 0, nil
	}

	var query string
	switch db.Backend() {
	case BackendPostgres:
		query = `INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, max_attempts, created_at, updated_at)
			SELECT gen_random_uuid(), m.id, m.namespace_id, 'pending', -1, 0, 3, now(), now()
			FROM memories m
			LEFT JOIN enrichment_queue q
			  ON q.memory_id = m.id AND q.status IN ('pending','running')
			WHERE m.deleted_at IS NULL AND q.id IS NULL`
	case BackendSQLite:
		query = `INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, max_attempts, created_at, updated_at)
			SELECT
			  lower(hex(randomblob(16))),
			  m.id,
			  m.namespace_id,
			  'pending',
			  -1,
			  0,
			  3,
			  strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
			  strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
			FROM memories m
			LEFT JOIN enrichment_queue q
			  ON q.memory_id = m.id AND q.status IN ('pending','running')
			WHERE m.deleted_at IS NULL AND q.id IS NULL`
	default:
		return 0, fmt.Errorf("backfill embed jobs: unsupported backend %s", db.Backend())
	}

	result, err := db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("backfill embed jobs: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("backfill embed jobs: rows affected: %w", err)
	}
	return n, nil
}

// hasUncoveredMemory returns true iff at least one live memory lacks a
// pending or running enrichment job.
func hasUncoveredMemory(ctx context.Context, db DB) (bool, error) {
	query := `SELECT 1 FROM memories m
		LEFT JOIN enrichment_queue q
		  ON q.memory_id = m.id AND q.status IN ('pending','running')
		WHERE m.deleted_at IS NULL AND q.id IS NULL
		LIMIT 1`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return false, fmt.Errorf("backfill embed jobs: probe: %w", err)
	}
	defer rows.Close()
	return rows.Next(), rows.Err()
}
