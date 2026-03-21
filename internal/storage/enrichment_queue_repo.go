package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// QueueStats holds aggregate counts of enrichment queue items by status.
type QueueStats struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Failed     int `json:"failed"`
}

// EnrichmentQueueRepo provides operations for the enrichment_queue table.
type EnrichmentQueueRepo struct {
	db DB
}

// NewEnrichmentQueueRepo creates a new EnrichmentQueueRepo backed by the given DB.
func NewEnrichmentQueueRepo(db DB) *EnrichmentQueueRepo {
	return &EnrichmentQueueRepo{db: db}
}

// Enqueue inserts a new item into the enrichment queue with status "pending".
// ID is generated if zero-valued. StepsCompleted defaults to `[]` if nil.
func (r *EnrichmentQueueRepo) Enqueue(ctx context.Context, item *model.EnrichmentJob) error {
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

	var lastError interface{}
	if item.LastError != nil && string(item.LastError) != "null" {
		lastError = string(item.LastError)
	}

	query := `INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, max_attempts, last_error, steps_completed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, max_attempts, last_error, steps_completed)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	}

	_, err := r.db.Exec(ctx, query,
		item.ID.String(), item.MemoryID.String(), item.NamespaceID.String(),
		item.Status, item.Priority, item.Attempts, item.MaxAttempts,
		lastError, string(item.StepsCompleted),
	)
	if err != nil {
		return fmt.Errorf("enrichment queue enqueue: %w", err)
	}

	return r.reload(ctx, item)
}

// ClaimNext atomically claims the next pending item in the enrichment queue,
// setting its status to "processing" and assigning the given workerID.
// Items are ordered by priority DESC, created_at ASC (highest priority first,
// oldest first within same priority). Returns sql.ErrNoRows if the queue is empty.
func (r *EnrichmentQueueRepo) ClaimNext(ctx context.Context, workerID string) (*model.EnrichmentJob, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	if r.db.Backend() == BackendPostgres {
		// Postgres: use SELECT ... FOR UPDATE SKIP LOCKED in a transaction.
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("enrichment queue claim begin tx: %w", err)
		}
		defer tx.Rollback() //nolint:errcheck

		row := tx.QueryRow(
			`SELECT id FROM enrichment_queue
				WHERE status = 'pending'
				ORDER BY priority DESC, created_at ASC
				LIMIT 1
				FOR UPDATE SKIP LOCKED`,
		)
		var idStr string
		if err := row.Scan(&idStr); err != nil {
			return nil, err // sql.ErrNoRows if empty
		}

		_, err = tx.Exec(
			`UPDATE enrichment_queue SET status = 'processing', claimed_by = $1, claimed_at = $2, updated_at = $3
				WHERE id = $4`,
			workerID, now, now, idStr,
		)
		if err != nil {
			return nil, fmt.Errorf("enrichment queue claim update: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("enrichment queue claim commit: %w", err)
		}

		id, _ := uuid.Parse(idStr)
		return r.GetByID(ctx, id)
	}

	// SQLite: atomic UPDATE ... WHERE with subquery to claim exactly one row.
	result, err := r.db.Exec(ctx,
		`UPDATE enrichment_queue SET status = 'processing', claimed_by = ?, claimed_at = ?, updated_at = ?
			WHERE id = (
				SELECT id FROM enrichment_queue
				WHERE status = 'pending'
				ORDER BY priority DESC, created_at ASC
				LIMIT 1
			)`,
		workerID, now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue claim: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("enrichment queue claim rows affected: %w", err)
	}
	if rows == 0 {
		return nil, sql.ErrNoRows
	}

	// Fetch the claimed row.
	query := selectEnrichmentQueueColumns + ` FROM enrichment_queue
		WHERE status = 'processing' AND claimed_by = ?
		ORDER BY updated_at DESC LIMIT 1`
	row := r.db.QueryRow(ctx, query, workerID)
	return r.scanItem(row)
}

// Complete marks an enrichment queue item as "completed" and sets completed_at.
func (r *EnrichmentQueueRepo) Complete(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE enrichment_queue SET status = 'completed', completed_at = ?, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET status = 'completed', completed_at = $1, updated_at = $2 WHERE id = $3`
	}

	result, err := r.db.Exec(ctx, query, now, now, id.String())
	if err != nil {
		return fmt.Errorf("enrichment queue complete: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("enrichment queue complete rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// Fail marks an enrichment queue item as "failed", stores the error message,
// and increments the attempts counter.
func (r *EnrichmentQueueRepo) Fail(ctx context.Context, id uuid.UUID, errMsg string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	// last_error is JSONB in Postgres, TEXT in SQLite.
	var lastErrorVal interface{} = errMsg
	if r.db.Backend() == BackendPostgres {
		b, err := json.Marshal(errMsg)
		if err != nil {
			return fmt.Errorf("enrichment queue fail marshal error: %w", err)
		}
		lastErrorVal = string(b)
	}

	query := `UPDATE enrichment_queue SET status = 'failed', last_error = ?, attempts = attempts + 1, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET status = 'failed', last_error = $1, attempts = attempts + 1, updated_at = $2 WHERE id = $3`
	}

	result, err := r.db.Exec(ctx, query, lastErrorVal, now, id.String())
	if err != nil {
		return fmt.Errorf("enrichment queue fail: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("enrichment queue fail rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// Retry resets an enrichment queue item back to "pending" status, clears the
// worker_id and claimed_at, and increments the attempts counter.
func (r *EnrichmentQueueRepo) Retry(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, attempts = attempts + 1, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, attempts = attempts + 1, updated_at = $1 WHERE id = $2`
	}

	result, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("enrichment queue retry: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("enrichment queue retry rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DeleteByMemoryID removes all enrichment queue items for the given memory.
func (r *EnrichmentQueueRepo) DeleteByMemoryID(ctx context.Context, memoryID uuid.UUID) error {
	query := `DELETE FROM enrichment_queue WHERE memory_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM enrichment_queue WHERE memory_id = $1`
	}
	_, err := r.db.Exec(ctx, query, memoryID.String())
	if err != nil {
		return fmt.Errorf("enrichment queue delete by memory: %w", err)
	}
	return nil
}

// GetByID returns an enrichment queue item by its UUID.
func (r *EnrichmentQueueRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.EnrichmentJob, error) {
	query := selectEnrichmentQueueColumns + ` FROM enrichment_queue WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectEnrichmentQueueColumns + ` FROM enrichment_queue WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanItem(row)
}

// reload fetches the item by ID and populates the struct in place.
func (r *EnrichmentQueueRepo) reload(ctx context.Context, item *model.EnrichmentJob) error {
	fetched, err := r.GetByID(ctx, item.ID)
	if err != nil {
		return fmt.Errorf("enrichment queue reload: %w", err)
	}
	*item = *fetched
	return nil
}

const selectEnrichmentQueueColumns = `SELECT id, memory_id, namespace_id, status, priority,
	claimed_at, claimed_by, attempts, max_attempts, last_error, steps_completed,
	completed_at, created_at, updated_at`

func (r *EnrichmentQueueRepo) scanItem(row *sql.Row) (*model.EnrichmentJob, error) {
	var item model.EnrichmentJob
	var idStr, memoryIDStr, namespaceIDStr string
	var claimedAtStr, claimedBy sql.NullString
	var lastErrorStr, completedAtStr sql.NullString
	var stepsCompletedStr string
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &memoryIDStr, &namespaceIDStr, &item.Status, &item.Priority,
		&claimedAtStr, &claimedBy, &item.Attempts, &item.MaxAttempts,
		&lastErrorStr, &stepsCompletedStr,
		&completedAtStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateItem(&item, idStr, memoryIDStr, namespaceIDStr,
		claimedAtStr, claimedBy, lastErrorStr, stepsCompletedStr,
		completedAtStr, createdAtStr, updatedAtStr)
}

func (r *EnrichmentQueueRepo) populateItem(
	item *model.EnrichmentJob,
	idStr, memoryIDStr, namespaceIDStr string,
	claimedAtStr, claimedBy sql.NullString,
	lastErrorStr sql.NullString,
	stepsCompletedStr string,
	completedAtStr sql.NullString,
	createdAtStr, updatedAtStr string,
) (*model.EnrichmentJob, error) {
	var err error

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue parse id: %w", err)
	}
	item.ID = id

	memID, err := uuid.Parse(memoryIDStr)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue parse memory_id: %w", err)
	}
	item.MemoryID = memID

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue parse namespace_id: %w", err)
	}
	item.NamespaceID = nsID

	if claimedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, claimedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("enrichment queue parse claimed_at: %w", err)
		}
		item.ClaimedAt = &t
	}

	if claimedBy.Valid {
		s := claimedBy.String
		item.ClaimedBy = &s
	}

	if lastErrorStr.Valid {
		item.LastError = json.RawMessage(lastErrorStr.String)
	}

	item.StepsCompleted = json.RawMessage(stepsCompletedStr)

	if completedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, completedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("enrichment queue parse completed_at: %w", err)
		}
		item.CompletedAt = &t
	}

	item.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue parse created_at: %w", err)
	}

	item.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue parse updated_at: %w", err)
	}

	return item, nil
}

// CountByStatus returns aggregate counts of queue items grouped by status.
func (r *EnrichmentQueueRepo) CountByStatus(ctx context.Context) (*QueueStats, error) {
	query := `SELECT status, COUNT(*) FROM enrichment_queue GROUP BY status`
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue count by status: %w", err)
	}
	defer rows.Close()

	stats := &QueueStats{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("enrichment queue scan: %w", err)
		}
		switch status {
		case "pending":
			stats.Pending = count
		case "processing":
			stats.Processing = count
		case "failed":
			stats.Failed = count
		}
	}
	return stats, rows.Err()
}

func (r *EnrichmentQueueRepo) scanItemFromRows(rows *sql.Rows) (*model.EnrichmentJob, error) {
	var item model.EnrichmentJob
	var idStr, memoryIDStr, namespaceIDStr string
	var claimedAtStr, claimedBy sql.NullString
	var lastErrorStr, completedAtStr sql.NullString
	var stepsCompletedStr string
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &memoryIDStr, &namespaceIDStr, &item.Status, &item.Priority,
		&claimedAtStr, &claimedBy, &item.Attempts, &item.MaxAttempts,
		&lastErrorStr, &stepsCompletedStr,
		&completedAtStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue scan rows: %w", err)
	}

	return r.populateItem(&item, idStr, memoryIDStr, namespaceIDStr,
		claimedAtStr, claimedBy, lastErrorStr, stepsCompletedStr,
		completedAtStr, createdAtStr, updatedAtStr)
}

// ListRecent returns the most recent enrichment queue items, ordered by created_at DESC.
func (r *EnrichmentQueueRepo) ListRecent(ctx context.Context, limit int) ([]model.EnrichmentJob, error) {
	query := selectEnrichmentQueueColumns + ` FROM enrichment_queue ORDER BY created_at DESC LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		query = selectEnrichmentQueueColumns + ` FROM enrichment_queue ORDER BY created_at DESC LIMIT $1`
	}

	rows, err := r.db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue list recent: %w", err)
	}
	defer rows.Close()

	result := []model.EnrichmentJob{}
	for rows.Next() {
		item, err := r.scanItemFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enrichment queue list recent iteration: %w", err)
	}
	return result, nil
}

// RetryAllFailed resets all failed items back to pending status. Returns the number of items retried.
func (r *EnrichmentQueueRepo) RetryAllFailed(ctx context.Context) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, completed_at = NULL, updated_at = ?
		WHERE status = 'failed'`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, completed_at = NULL, updated_at = $1
			WHERE status = 'failed'`
	}

	result, err := r.db.Exec(ctx, query, now)
	if err != nil {
		return 0, fmt.Errorf("enrichment queue retry all: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("enrichment queue retry all rows affected: %w", err)
	}

	// Also reset the enriched flag on memories whose jobs are being retried,
	// so they get properly re-enriched.
	memQuery := `UPDATE memories SET enriched = 0, updated_at = ?
		WHERE enriched = 1 AND id IN (SELECT memory_id FROM enrichment_queue WHERE status = 'pending')`
	if r.db.Backend() == BackendPostgres {
		memQuery = `UPDATE memories SET enriched = false, updated_at = $1
			WHERE enriched = true AND id IN (SELECT memory_id FROM enrichment_queue WHERE status = 'pending')`
	}
	_, _ = r.db.Exec(ctx, memQuery, now)

	return int(rows), nil
}
