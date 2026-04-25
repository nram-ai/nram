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

// BackfillReembedAllJobs enqueues a pending enrichment job (priority -1) for
// every live memory unconditionally, including memories that already have a
// pending or running job. Used by the embedding-model switch cascade so the
// worker pool re-embeds the entire corpus under the new model. Returns the
// number of new rows inserted.
//
// Idempotency note: the enrichment_queue does NOT have a UNIQUE constraint
// on memory_id, so repeated calls insert duplicate rows. The worker pool
// handles duplicates by short-circuiting on memories whose embedding_dim
// already matches the current provider dim. For a one-shot cascade after a
// model switch (where embedding_dim has been NULLed) this is the desired
// behavior — every memory needs exactly one job to land.
//
// Callers MUST ensure dependent state is consistent before invoking:
// memories.embedding_dim should be NULL'd and the corresponding
// memory_vectors_* tables truncated, otherwise the worker may skip
// memories whose old vectors are still indexed under a stale dim.
func BackfillReembedAllJobs(ctx context.Context, db DB) (int64, error) {
	var query string
	switch db.Backend() {
	case BackendPostgres:
		query = `INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, max_attempts, created_at, updated_at)
			SELECT gen_random_uuid(), m.id, m.namespace_id, 'pending', -1, 0, 3, now(), now()
			FROM memories m
			WHERE m.deleted_at IS NULL`
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
			WHERE m.deleted_at IS NULL`
	default:
		return 0, fmt.Errorf("backfill reembed all jobs: unsupported backend %s", db.Backend())
	}

	result, err := db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("backfill reembed all jobs: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("backfill reembed all jobs: rows affected: %w", err)
	}
	return n, nil
}
