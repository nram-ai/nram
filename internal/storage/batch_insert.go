package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
)

// batchInsertChunkSize bounds how many rows go into one multi-row INSERT so the
// parameter count stays well under SQLite's host-parameter ceiling even for the
// widest table (memories, 22 columns → 11000 params at 500 rows).
const batchInsertChunkSize = 500

// multiRowPlaceholders builds the "VALUES (...),(...)" tuple list for a
// multi-row INSERT: numbered ($1,$2,...) for Postgres, positional (?) for
// SQLite. Parameters are numbered sequentially across all rows.
func multiRowPlaceholders(isPg bool, nrows, ncols int) string {
	var b strings.Builder
	p := 1
	for i := 0; i < nrows; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for c := 0; c < ncols; c++ {
			if c > 0 {
				b.WriteString(", ")
			}
			if isPg {
				b.WriteByte('$')
				b.WriteString(strconv.Itoa(p))
				p++
			} else {
				b.WriteByte('?')
			}
		}
		b.WriteByte(')')
	}
	return b.String()
}

// BatchCreate inserts many memories in chunked multi-row INSERTs wrapped in a
// single transaction, so it is atomic: on any error nothing is inserted and the
// caller can fall back to per-row Create without risking duplicate rows (each
// memory carries a pre-assigned primary key). Identical per-row semantics to
// Create (defaults applied via memoryInsertArgs).
func (r *MemoryRepo) BatchCreate(ctx context.Context, mems []*model.Memory) error {
	if len(mems) == 0 {
		return nil
	}
	const ncols = 22
	const cols = `(id, namespace_id, content, content_hash, embedding_dim, source, tags,
		confidence, importance, access_count, last_accessed, expires_at, superseded_by, superseded_at,
		enriched, metadata, purge_after, created_at, updated_at, augmented_queries, augmented_embedding_at, origin)`
	isPg := r.db.Backend() == BackendPostgres
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("memory batch create begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for start := 0; start < len(mems); start += batchInsertChunkSize {
		end := min(start+batchInsertChunkSize, len(mems))
		chunk := mems[start:end]
		args := make([]any, 0, ncols*len(chunk))
		for _, mem := range chunk {
			_, rowArgs := r.memoryInsertArgs(mem)
			args = append(args, rowArgs...)
		}
		query := "INSERT INTO memories " + cols + " VALUES " + multiRowPlaceholders(isPg, len(chunk), ncols)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("memory batch create: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("memory batch create commit: %w", err)
	}
	return nil
}

// BatchCreate inserts many ingestion-log rows in chunked multi-row INSERTs. It
// is the batched form of Create minus the post-insert reload (the batch-store
// path discards the row), with identical defaults.
func (r *IngestionLogRepo) BatchCreate(ctx context.Context, logs []*model.IngestionLog) error {
	if len(logs) == 0 {
		return nil
	}
	const ncols = 9
	const cols = `(id, namespace_id, source, content_hash, raw_content, memory_ids, status, error, metadata)`
	isPg := r.db.Backend() == BackendPostgres
	for start := 0; start < len(logs); start += batchInsertChunkSize {
		end := min(start+batchInsertChunkSize, len(logs))
		chunk := logs[start:end]
		args := make([]any, 0, ncols*len(chunk))
		for _, log := range chunk {
			if log.ID == uuid.Nil {
				log.ID = uuid.New()
			}
			if log.MemoryIDs == nil {
				log.MemoryIDs = []uuid.UUID{}
			}
			if log.Error == nil {
				log.Error = json.RawMessage("null")
			}
			if log.Metadata == nil {
				log.Metadata = json.RawMessage("{}")
			}
			args = append(args,
				log.ID.String(), log.NamespaceID.String(), log.Source, log.ContentHash,
				log.RawContent, encodeStringArray(r.db.Backend(), uuidsToStrings(log.MemoryIDs)),
				log.Status, string(log.Error), string(log.Metadata),
			)
		}
		query := "INSERT INTO ingestion_log " + cols + " VALUES " + multiRowPlaceholders(isPg, len(chunk), ncols)
		if _, err := r.db.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("ingestion_log batch create: %w", err)
		}
	}
	return nil
}

// BatchEnqueue inserts many enrichment jobs in chunked multi-row INSERTs,
// preserving the single-row Enqueue's ON CONFLICT(memory_id) WHERE
// status='pending' DO NOTHING dedup so a duplicate pending job for the same
// memory is skipped.
func (r *EnrichmentQueueRepo) BatchEnqueue(ctx context.Context, items []*model.EnrichmentJob) error {
	if len(items) == 0 {
		return nil
	}
	const ncols = 11
	const cols = `(id, memory_id, namespace_id, status, priority, attempts, max_attempts, last_error, steps_completed, created_at, updated_at)`
	isPg := r.db.Backend() == BackendPostgres
	now := time.Now().UTC()
	for start := 0; start < len(items); start += batchInsertChunkSize {
		end := min(start+batchInsertChunkSize, len(items))
		chunk := items[start:end]
		args := make([]any, 0, ncols*len(chunk))
		for _, item := range chunk {
			if item.ID == uuid.Nil {
				item.ID = uuid.New()
			}
			if item.Status == "" {
				item.Status = "pending"
			}
			if item.StepsCompleted == nil {
				item.StepsCompleted = json.RawMessage(`[]`)
			}
			if item.MaxAttempts == 0 {
				item.MaxAttempts = 3
			}
			if item.CreatedAt.IsZero() {
				item.CreatedAt = now
			}
			if item.UpdatedAt.IsZero() {
				item.UpdatedAt = now
			}
			var lastError any
			if item.LastError != nil && string(item.LastError) != "null" {
				lastError = string(item.LastError)
			}
			args = append(args,
				item.ID.String(), item.MemoryID.String(), item.NamespaceID.String(), item.Status,
				item.Priority, item.Attempts, item.MaxAttempts, lastError, string(item.StepsCompleted),
				item.CreatedAt.UTC().Format(time.RFC3339), item.UpdatedAt.UTC().Format(time.RFC3339),
			)
		}
		conflict := " ON CONFLICT(memory_id) WHERE status = 'pending' DO NOTHING"
		if isPg {
			conflict = " ON CONFLICT (memory_id) WHERE status = 'pending' DO NOTHING"
		}
		query := "INSERT INTO enrichment_queue " + cols + " VALUES " + multiRowPlaceholders(isPg, len(chunk), ncols) + conflict
		if _, err := r.db.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("enrichment_queue batch enqueue: %w", err)
		}
	}
	return nil
}
