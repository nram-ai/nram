package storage

import (
	"context"
	"fmt"

	"github.com/nram-ai/nram/internal/tags"
)

// buildEnqueueLiveMemoriesQuery returns the SQL that inserts a priority-(-1)
// pending enrichment job for live memories. When dedupe is true (the uncovered
// backfill), coverage is defined by the DURABLE memories.enriched flag: only
// memories that have never been enriched (enriched = false) are candidates, and
// a LEFT JOIN additionally skips any that already hold a pending or in-flight
// (processing) job so we do not double-enqueue in-flight work. enriched is the
// only correct signal here — it is stamped true at finalizeJob (MarkEnriched)
// and survives clearing the enrichment_queue (completed rows are pruned by the
// admin clear-completed endpoint and by memory/namespace deletion), so a
// queue-row-presence test would re-flag every completed memory and re-enqueue
// the whole corpus. When dedupe is false (EnqueueAllLiveMemories, the
// embedding-model-switch cascade) every live memory is re-enqueued regardless of
// enriched. Both backends share this builder so the column list, dialect quirks,
// and filters stay in lockstep.
// boolLiteral renders a boolean as a raw-SQL literal for the backend: postgres
// uses true/false, sqlite stores booleans as 0/1. Used where the value is a
// compile-time constant interpolated into SQL rather than bound as a parameter,
// so the predicate can prove a partial index such as idx_memories_enriched
// (a bound $1 is not guaranteed to prove it under a generic plan).
func boolLiteral(backend string, v bool) string {
	if backend == BackendPostgres {
		if v {
			return "true"
		}
		return "false"
	}
	if v {
		return "1"
	}
	return "0"
}

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
		  ON q.memory_id = m.id AND q.status IN ('pending','processing')
		WHERE m.deleted_at IS NULL AND m.enriched = ` + boolLiteral(backend, false) + ` AND q.id IS NULL`
	} else {
		q += `
		WHERE m.deleted_at IS NULL`
	}
	// Respect the partial unique index idx_enrichment_queue_pending_memory:
	// memories already holding an unclaimed-pending job are skipped (the
	// existing job covers re-processing). Memories whose only job is in-flight
	// ('processing') still receive a fresh pending row. Harmless for the dedupe
	// path, which already LEFT JOINs out pending rows.
	if backend == BackendPostgres {
		q += `
		ON CONFLICT (memory_id) WHERE status = 'pending' DO NOTHING`
	} else {
		q += `
		ON CONFLICT(memory_id) WHERE status = 'pending' DO NOTHING`
	}
	return q, nil
}

// EnqueueUncoveredMemories enqueues a priority-(-1) enrichment job for every
// live memory that has never been enriched (durable memories.enriched = false)
// and does not already hold a pending or in-flight job. Idempotent, and a no-op
// in steady state (every completed memory is enriched = true). Coverage is keyed
// on the enriched flag, NOT on the presence of an enrichment_queue row: completed
// queue rows are pruned by the admin clear-completed endpoint and by
// memory/namespace deletion, so a queue-row test would re-flag every completed
// memory and re-enqueue the whole corpus. Exposed via the --backfill-enrichment
// CLI flag and the dreaming uncovered-backfill phase.
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

// UncoveredBackfiller adapts the package-level EnqueueUncoveredMemories bulk
// enqueue to a method so callers (the dreaming uncovered-backfill phase) can
// inject it behind a narrow one-method interface and substitute a fake in
// tests. It holds only the DB handle; the heavy lifting stays in
// EnqueueUncoveredMemories (including its cheap hasUncoveredMemory short-circuit).
type UncoveredBackfiller struct{ db DB }

// NewUncoveredBackfiller binds the bulk uncovered-enqueue to a DB handle.
func NewUncoveredBackfiller(db DB) *UncoveredBackfiller { return &UncoveredBackfiller{db: db} }

// EnqueueUncoveredMemories delegates to the package-level bulk enqueue.
func (b *UncoveredBackfiller) EnqueueUncoveredMemories(ctx context.Context) (int64, error) {
	return EnqueueUncoveredMemories(ctx, b.db)
}

// hasUncoveredMemory returns true iff at least one live, never-enriched memory
// (durable memories.enriched = false) lacks a pending or in-flight (processing)
// enrichment job. The enriched gate is what keeps this from matching every
// completed memory once its queue row is cleared; the LEFT JOIN only skips
// memories already being worked.
func hasUncoveredMemory(ctx context.Context, db DB) (bool, error) {
	// enriched as an interpolated literal (not a bound param) so the predicate
	// matches idx_memories_enriched and stays in lockstep with the INSERT builder.
	query := `SELECT 1 FROM memories m
		LEFT JOIN enrichment_queue q
		  ON q.memory_id = m.id AND q.status IN ('pending','processing')
		WHERE m.deleted_at IS NULL AND m.enriched = ` + boolLiteral(db.Backend(), false) + ` AND q.id IS NULL
		LIMIT 1`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return false, fmt.Errorf("enqueue uncovered memories: probe: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return rows.Next(), rows.Err()
}

// NormalizeMemoryTags rewrites the tags array on every live memory whose
// stored tags differ from tags.Normalize(stored). Idempotent: a second
// pass over a clean table reports zero rows changed. Safe to run
// concurrently with writes: a writer that lands between the SELECT and
// UPDATE wins, and writers already pass through tags.Normalize at the
// repo boundary, so the result is still clean.
func NormalizeMemoryTags(ctx context.Context, db DB) (int64, error) {
	backend := db.Backend()

	selectQuery := `SELECT id, tags FROM memories WHERE deleted_at IS NULL`
	updateQuery := `UPDATE memories SET tags = ? WHERE id = ?`
	if backend == BackendPostgres {
		updateQuery = `UPDATE memories SET tags = $1 WHERE id = $2`
	}

	rows, err := db.Query(ctx, selectQuery)
	if err != nil {
		return 0, fmt.Errorf("normalize memory tags: select: %w", err)
	}

	type pending struct {
		id      string
		encoded string
	}
	var changes []pending
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("normalize memory tags: scan: %w", err)
		}
		decoded, decErr := decodeStringArray(backend, raw)
		if decErr != nil {
			// Skip rows with malformed tags; they're already broken.
			continue
		}
		normalized := tags.Normalize(decoded)
		if normalized == nil {
			normalized = []string{}
		}
		encoded := encodeStringArray(backend, normalized)
		if encoded == raw {
			continue
		}
		changes = append(changes, pending{id: id, encoded: encoded})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("normalize memory tags: rows: %w", err)
	}
	_ = rows.Close()

	if len(changes) == 0 {
		return 0, nil
	}

	var updated int64
	for _, c := range changes {
		result, err := db.Exec(ctx, updateQuery, c.encoded, c.id)
		if err != nil {
			return updated, fmt.Errorf("normalize memory tags: update %s: %w", c.id, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return updated, fmt.Errorf("normalize memory tags: rows affected for %s: %w", c.id, err)
		}
		updated += n
	}
	return updated, nil
}

// EnqueueAllLiveMemories enqueues a priority-(-1) enrichment job for every
// live memory. Used by the embedding-model switch cascade after the vector
// tables have been truncated and embedding_dim NULL'd. A memory that already
// holds an unclaimed-pending job is skipped (ON CONFLICT DO NOTHING on the
// partial unique index) since that job will re-embed it; a memory whose only
// job is in-flight ('processing') still gets a fresh pending row, so every
// live memory ends up with at least one pending job.
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
