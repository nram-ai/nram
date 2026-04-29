package storage

import (
	"context"
	"fmt"
)

// buildEnqueueLiveMemoriesQuery returns the SQL that inserts a priority-(-1)
// pending enrichment job for every live memory. When dedupe is true, memories
// that already have a pending or running job are skipped via a LEFT JOIN
// guard. Both backends share this builder so the column list, dialect
// quirks, and `deleted_at IS NULL` filter stay in lockstep.
func buildEnqueueLiveMemoriesQuery(backend string, dedupe bool) (string, error) {
	var insertCols, idExpr, nowExpr string
	switch backend {
	case BackendPostgres:
		insertCols = "(id, memory_id, namespace_id, status, priority, attempts, max_attempts, created_at, updated_at)"
		idExpr = "gen_random_uuid()"
		nowExpr = "now()"
	case BackendSQLite:
		insertCols = "(id, memory_id, namespace_id, status, priority, attempts, max_attempts, created_at, updated_at)"
		idExpr = "lower(hex(randomblob(16)))"
		nowExpr = "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"
	default:
		return "", fmt.Errorf("unsupported backend %s", backend)
	}

	q := fmt.Sprintf(`INSERT INTO enrichment_queue %s
		SELECT %s, m.id, m.namespace_id, 'pending', -1, 0, 3, %s, %s
		FROM memories m`, insertCols, idExpr, nowExpr, nowExpr)
	if dedupe {
		q += `
		LEFT JOIN enrichment_queue q
		  ON q.memory_id = m.id AND q.status IN ('pending','running')
		WHERE m.deleted_at IS NULL AND q.id IS NULL`
	} else {
		q += `
		WHERE m.deleted_at IS NULL`
	}
	return q, nil
}

// EnqueueUncoveredMemories enqueues a priority-(-1) enrichment job for
// every live memory that does not already have a pending or running job.
// Idempotent. Exposed via NRAM_ENABLE_ENRICHMENT_BACKFILL=1 (startup) and
// --backfill-enrichment (CLI). The worker skips fact/entity extraction
// when prior lineage/relationship rows already exist for the memory, so
// re-running this against fully-enriched memories costs only the embed
// call.
func EnqueueUncoveredMemories(ctx context.Context, db DB) (int64, error) {
	// Short-circuit avoids the full-table INSERT...SELECT in steady state.
	present, err := hasUncoveredMemory(ctx, db)
	if err != nil {
		return 0, err
	}
	if !present {
		return 0, nil
	}

	query, err := buildEnqueueLiveMemoriesQuery(db.Backend(), true)
	if err != nil {
		return 0, fmt.Errorf("enqueue uncovered memories: %w", err)
	}
	result, err := db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("enqueue uncovered memories: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("enqueue uncovered memories: rows affected: %w", err)
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
		return false, fmt.Errorf("enqueue uncovered memories: probe: %w", err)
	}
	defer rows.Close()
	return rows.Next(), rows.Err()
}

// EnqueueAllLiveMemories enqueues a priority-(-1) enrichment job for
// every live memory unconditionally. Used by the embedding-model switch
// cascade after the vector tables have been truncated and embedding_dim
// NULL'd — duplicates against in-flight jobs are expected and handled by
// the worker.
func EnqueueAllLiveMemories(ctx context.Context, db DB) (int64, error) {
	query, err := buildEnqueueLiveMemoriesQuery(db.Backend(), false)
	if err != nil {
		return 0, fmt.Errorf("enqueue all live memories: %w", err)
	}
	result, err := db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("enqueue all live memories: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("enqueue all live memories: rows affected: %w", err)
	}
	return n, nil
}
