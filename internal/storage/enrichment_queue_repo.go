package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ErrClaimLost is returned by Complete / CompleteWithWarning / Fail / Release
// when a non-empty workerID was passed and no longer matches the row's
// claimed_by. Callers should log and drop.
var ErrClaimLost = errors.New("enrichment queue: claim lost (row was reassigned)")

// Enrichment queue status values. Mirrors the schema's CHECK-able set.
const (
	statusPending    = model.EnrichmentStatusPending
	statusProcessing = model.EnrichmentStatusProcessing
	statusCompleted  = model.EnrichmentStatusCompleted
	statusFailed     = model.EnrichmentStatusFailed
)

// isSQLiteBusy returns true if the error is a SQLITE_BUSY contention error.
func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "SQLITE_BUSY")
}

// QueueStats holds aggregate counts of enrichment queue items by status.
type QueueStats struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
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
// Zero-valued ID / CreatedAt / UpdatedAt are filled from Go; StepsCompleted
// defaults to `[]`.
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
	now := time.Now().UTC()
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

	createdAtStr := item.CreatedAt.UTC().Format(time.RFC3339)
	updatedAtStr := item.UpdatedAt.UTC().Format(time.RFC3339)

	query := `INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, max_attempts, last_error, steps_completed, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, max_attempts, last_error, steps_completed, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	}

	_, err := r.db.Exec(ctx, query,
		item.ID.String(), item.MemoryID.String(), item.NamespaceID.String(),
		item.Status, item.Priority, item.Attempts, item.MaxAttempts,
		lastError, string(item.StepsCompleted),
		createdAtStr, updatedAtStr,
	)
	if err != nil {
		return fmt.Errorf("enrichment queue enqueue: %w", err)
	}

	return nil
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
	// Retry on SQLITE_BUSY since the write pool serializes but the busy_timeout
	// may not be sufficient under heavy enrichment worker contention.
	var result sql.Result
	var err error
	for attempt := range 3 {
		result, err = r.db.Exec(ctx,
			`UPDATE enrichment_queue SET status = 'processing', claimed_by = ?, claimed_at = ?, updated_at = ?
				WHERE id = (
					SELECT id FROM enrichment_queue
					WHERE status = 'pending'
					ORDER BY priority DESC, created_at ASC
					LIMIT 1
				)`,
			workerID, now, now,
		)
		if err == nil || !isSQLiteBusy(err) {
			break
		}
		time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond) // 50ms, 100ms, 200ms
	}
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

// ClaimNextBatch atomically claims up to `max` pending items and assigns
// them to workerID. Returns sql.ErrNoRows if the queue is empty. On Postgres
// this runs in a single SELECT ... FOR UPDATE SKIP LOCKED transaction so
// concurrent workers claim disjoint sets; on SQLite the write pool
// serializes writes and we loop ClaimNext up to `max` times.
func (r *EnrichmentQueueRepo) ClaimNextBatch(ctx context.Context, workerID string, max int) ([]*model.EnrichmentJob, error) {
	if max <= 0 {
		return nil, fmt.Errorf("enrichment queue claim batch: max must be positive, got %d", max)
	}
	now := time.Now().UTC().Format(time.RFC3339)

	if r.db.Backend() == BackendPostgres {
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("enrichment queue claim batch begin tx: %w", err)
		}
		defer tx.Rollback() //nolint:errcheck

		rows, err := tx.Query(
			`SELECT id FROM enrichment_queue
				WHERE status = 'pending'
				ORDER BY priority DESC, created_at ASC
				LIMIT $1
				FOR UPDATE SKIP LOCKED`,
			max,
		)
		if err != nil {
			return nil, fmt.Errorf("enrichment queue claim batch select: %w", err)
		}

		ids := make([]string, 0, max)
		for rows.Next() {
			var idStr string
			if err := rows.Scan(&idStr); err != nil {
				rows.Close()
				return nil, fmt.Errorf("enrichment queue claim batch scan: %w", err)
			}
			ids = append(ids, idStr)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("enrichment queue claim batch rows: %w", err)
		}
		if len(ids) == 0 {
			return nil, sql.ErrNoRows
		}

		// Build the UPDATE with a placeholder list of ids.
		placeholders := make([]string, len(ids))
		args := make([]any, 0, len(ids)+3)
		args = append(args, workerID, now, now)
		for i, id := range ids {
			placeholders[i] = fmt.Sprintf("$%d", i+4)
			args = append(args, id)
		}
		updateSQL := fmt.Sprintf(
			`UPDATE enrichment_queue SET status = 'processing', claimed_by = $1, claimed_at = $2, updated_at = $3
				WHERE id IN (%s)`,
			strings.Join(placeholders, ","),
		)
		if _, err := tx.Exec(updateSQL, args...); err != nil {
			return nil, fmt.Errorf("enrichment queue claim batch update: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("enrichment queue claim batch commit: %w", err)
		}

		items := make([]*model.EnrichmentJob, 0, len(ids))
		for _, idStr := range ids {
			id, err := uuid.Parse(idStr)
			if err != nil {
				return nil, fmt.Errorf("enrichment queue claim batch parse id: %w", err)
			}
			item, err := r.GetByID(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("enrichment queue claim batch get: %w", err)
			}
			items = append(items, item)
		}
		return items, nil
	}

	items := make([]*model.EnrichmentJob, 0, max)
	for range max {
		item, err := r.ClaimNext(ctx, workerID)
		if err != nil {
			if err == sql.ErrNoRows {
				break
			}
			if len(items) == 0 {
				return nil, err
			}
			return items, nil
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, sql.ErrNoRows
	}
	return items, nil
}

// setStatusOpts groups setStatus's mode flags so the caller's intent is
// readable at call sites instead of a tuple of unlabeled booleans.
type setStatusOpts struct {
	lastError          *string // nil clears the column
	workerID           string  // "" = unguarded; non-empty adds AND claimed_by = ?
	bumpAttempts       bool
	setCompletedAt     bool
	clearRequeueReason bool
}

// setStatus is the shared body of Complete / CompleteWithWarning / Fail.
// Returns ErrClaimLost when opts.workerID is set and the row's claimed_by
// no longer matches; sql.ErrNoRows when the row is missing entirely.
func (r *EnrichmentQueueRepo) setStatus(ctx context.Context, id uuid.UUID, status string, opts setStatusOpts) error {
	now := time.Now().UTC().Format(time.RFC3339)

	parts := []string{"status = ?", "last_error = ?"}
	args := []any{status, lastErrorArg(opts.lastError)}
	if opts.bumpAttempts {
		parts = append(parts, "attempts = attempts + 1")
	}
	if opts.setCompletedAt {
		parts = append(parts, "completed_at = ?")
		args = append(args, now)
	}
	if opts.clearRequeueReason {
		parts = append(parts, "last_requeue_reason = NULL")
	}
	parts = append(parts, "updated_at = ?")
	args = append(args, now)

	where := "id = ?"
	args = append(args, id.String())
	if opts.workerID != "" {
		where += " AND claimed_by = ?"
		args = append(args, opts.workerID)
	}

	query := "UPDATE enrichment_queue SET " + strings.Join(parts, ", ") + " WHERE " + where
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}

	result, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("enrichment queue %s: %w", status, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("enrichment queue %s rows affected: %w", status, err)
	}
	if rows == 0 {
		if opts.workerID != "" {
			return ErrClaimLost
		}
		return sql.ErrNoRows
	}
	return nil
}

// lastErrorArg returns the SQL value for last_error: untyped nil clears the
// column, otherwise the string is bound directly. Both backends accept the
// JSON-encoded string — SQLite stores in TEXT, Postgres parses into JSONB.
func lastErrorArg(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// postgresPlaceholders rewrites '?' placeholders to '$N' positional form.
func postgresPlaceholders(query string) string {
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

// marshalLastError produces the JSON-encoded string stored in last_error.
// A plain string becomes a JSON string ("..."); a struct becomes a JSON
// object ({...}). One JSON form for both backends so admin views can
// JSON.parse and either render the structured fields or show the bare string.
func marshalLastError(payload any) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// Complete marks an enrichment queue item as "completed", clears stale
// last_error and last_requeue_reason, and sets completed_at. Pass workerID
// to enable the stale-write guard (returns ErrClaimLost if the row was
// reassigned to another worker since the claim); pass "" for admin-
// initiated paths that don't care about claim ownership.
func (r *EnrichmentQueueRepo) Complete(ctx context.Context, id uuid.UUID, workerID string) error {
	return r.setStatus(ctx, id, statusCompleted, setStatusOpts{
		setCompletedAt:     true,
		clearRequeueReason: true,
		workerID:           workerID,
	})
}

// CompleteWithWarning marks the row "completed" while preserving a structured
// payload on last_error so admin surfaces can flag the job as "completed but
// degraded." Same workerID semantics as Complete.
func (r *EnrichmentQueueRepo) CompleteWithWarning(ctx context.Context, id uuid.UUID, workerID string, payload any) error {
	encoded, err := marshalLastError(payload)
	if err != nil {
		return fmt.Errorf("enrichment queue complete-with-warning marshal payload: %w", err)
	}
	return r.setStatus(ctx, id, statusCompleted, setStatusOpts{
		lastError:          &encoded,
		setCompletedAt:     true,
		clearRequeueReason: true,
		workerID:           workerID,
	})
}

// Fail marks the row "failed", stores the JSON-encoded payload as last_error,
// and increments the attempts counter. Same workerID semantics as Complete.
func (r *EnrichmentQueueRepo) Fail(ctx context.Context, id uuid.UUID, workerID string, payload any) error {
	encoded, err := marshalLastError(payload)
	if err != nil {
		return fmt.Errorf("enrichment queue fail marshal payload: %w", err)
	}
	return r.setStatus(ctx, id, statusFailed, setStatusOpts{
		lastError:    &encoded,
		bumpAttempts: true,
		workerID:     workerID,
	})
}

// MarkStepCompleted appends a step name to the enrichment job's
// steps_completed array. Idempotent: a step already present is not
// duplicated. Postgres uses jsonb containment + concat in a single
// statement; SQLite reads the column, mutates the JSON in Go, and
// writes it back. Used by the worker to record per-phase progress so
// retries of a partially-failed job skip phases that already ran.
func (r *EnrichmentQueueRepo) MarkStepCompleted(ctx context.Context, id uuid.UUID, step string) error {
	if step == "" {
		return fmt.Errorf("enrichment queue mark step completed: empty step")
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if r.db.Backend() == BackendPostgres {
		// jsonb append guarded by NOT containment — atomic in one round-trip.
		query := `UPDATE enrichment_queue
			SET steps_completed = COALESCE(steps_completed, '[]'::jsonb) || to_jsonb($1::text),
			    updated_at = $2
			WHERE id = $3
			  AND NOT (COALESCE(steps_completed, '[]'::jsonb) @> to_jsonb($1::text))`
		if _, err := r.db.Exec(ctx, query, step, now, id.String()); err != nil {
			return fmt.Errorf("enrichment queue mark step completed: %w", err)
		}
		return nil
	}

	// SQLite: read-modify-write. The column is TEXT holding a JSON array.
	var raw sql.NullString
	row := r.db.QueryRow(ctx, `SELECT steps_completed FROM enrichment_queue WHERE id = ?`, id.String())
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		}
		return fmt.Errorf("enrichment queue mark step completed read: %w", err)
	}

	steps := []string{}
	if raw.Valid && raw.String != "" {
		_ = json.Unmarshal([]byte(raw.String), &steps) // tolerate malformed → reset
	}
	if slices.Contains(steps, step) {
		return nil
	}
	steps = append(steps, step)
	encoded, err := json.Marshal(steps)
	if err != nil {
		return fmt.Errorf("enrichment queue mark step completed encode: %w", err)
	}

	if _, err := r.db.Exec(ctx,
		`UPDATE enrichment_queue SET steps_completed = ?, updated_at = ? WHERE id = ?`,
		string(encoded), now, id.String(),
	); err != nil {
		return fmt.Errorf("enrichment queue mark step completed write: %w", err)
	}
	return nil
}

// SetQueryAugmentSkipReason stamps the structured cause when the
// query-augmentation step did not land in the persisted vector. Written by
// the worker's finalizeJob so the queue row captures why the step is absent
// from steps_completed (disabled, content empty, provider unavailable, LLM
// error, parse error). Empty reason clears the column for the case where a
// retry later succeeds.
//
// workerID, when non-empty, adds a claim guard: the UPDATE is filtered by
// claimed_by = workerID so a stale worker whose claim has been requeued by
// the stuck-job sweeper cannot clobber a newer worker's write. Pass "" for
// unguarded operator paths. The predicate `query_augment_skip_reason IS
// DISTINCT FROM $arg` (postgres) / `IS NOT ?` (sqlite) skips the write when
// the column already matches, avoiding a redundant updated_at bump on every
// finalize. Best-effort semantics: zero rows affected (claim lost OR value
// already matched) is not an error.
func (r *EnrichmentQueueRepo) SetQueryAugmentSkipReason(ctx context.Context, id uuid.UUID, workerID string, reason string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	var arg any
	if reason == "" {
		arg = nil
	} else {
		arg = reason
	}
	if r.db.Backend() == BackendPostgres {
		query := `UPDATE enrichment_queue SET query_augment_skip_reason = $1, updated_at = $2
			WHERE id = $3 AND query_augment_skip_reason IS DISTINCT FROM $1`
		args := []any{arg, now, id.String()}
		if workerID != "" {
			query += ` AND claimed_by = $4`
			args = append(args, workerID)
		}
		if _, err := r.db.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("enrichment queue set query_augment_skip_reason: %w", err)
		}
		return nil
	}
	query := `UPDATE enrichment_queue SET query_augment_skip_reason = ?, updated_at = ?
		WHERE id = ? AND query_augment_skip_reason IS NOT ?`
	args := []any{arg, now, id.String(), arg}
	if workerID != "" {
		query += ` AND claimed_by = ?`
		args = append(args, workerID)
	}
	if _, err := r.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("enrichment queue set query_augment_skip_reason: %w", err)
	}
	return nil
}

// Release resets a claimed enrichment queue item back to "pending" without
// bumping the attempts counter. Used when the worker defers a job (e.g. the
// enrichment-available gate flipped closed mid-batch) rather than fails it.
// Pass workerID to enable the stale-write guard; "" for unguarded.
func (r *EnrichmentQueueRepo) Release(ctx context.Context, id uuid.UUID, workerID string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	args := []any{now, id.String()}
	where := "id = ?"
	if workerID != "" {
		where += " AND claimed_by = ?"
		args = append(args, workerID)
	}
	query := "UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL, updated_at = ? WHERE " + where
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}

	result, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("enrichment queue release: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("enrichment queue release rows affected: %w", err)
	}
	if rows == 0 {
		if workerID != "" {
			return ErrClaimLost
		}
		return sql.ErrNoRows
	}
	return nil
}

// Retry resets an enrichment queue item back to "pending" status, clears the
// worker_id, claimed_at, heartbeat_at, last_requeue_reason, and any stale
// last_error from the prior attempt (so admin views show a clean slate while
// the new attempt is in flight), and increments the attempts counter.
// Operator-initiated retry — unguarded.
func (r *EnrichmentQueueRepo) Retry(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL, last_error = NULL, last_requeue_reason = NULL, attempts = attempts + 1, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL, last_error = NULL, last_requeue_reason = NULL, attempts = attempts + 1, updated_at = $1 WHERE id = $2`
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

// TickHeartbeat updates heartbeat_at AND updated_at to now() for every row
// currently held by workerID. One write per tick covers every job this
// worker holds — heartbeat is per-worker because in-flight rows for the same
// worker share the same liveness signal: if the worker is alive, all its
// rows are.
func (r *EnrichmentQueueRepo) TickHeartbeat(ctx context.Context, workerID string) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE enrichment_queue SET heartbeat_at = ?, updated_at = ?
		WHERE claimed_by = ? AND status = 'processing'`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET heartbeat_at = $1, updated_at = $2
			WHERE claimed_by = $3 AND status = 'processing'`
	}

	res, err := r.db.Exec(ctx, query, now, now, workerID)
	if err != nil {
		return 0, fmt.Errorf("enrichment queue tick heartbeat: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("enrichment queue tick heartbeat rows affected: %w", err)
	}
	return int(rows), nil
}

// ListStaleClaimed returns enrichment_queue rows in status='processing'
// matched by either of two stale-claim signals:
//   - updatedThreshold: rows whose updated_at has fallen behind by more than
//     this duration — the heartbeat goroutine is no longer ticking for the
//     claimed_by worker, so the claiming process is presumed gone (crash,
//     OOM, host reboot mid-batch).
//   - claimedAtMaxAge: rows whose claimed_at exceeds this hard cap,
//     regardless of updated_at — the heartbeat may still be ticking (e.g.
//     same process is wedged in a long LLM call, or a sibling instance is
//     refreshing under a colliding claimed_by) but the claim has lived past
//     any plausible batch runtime and is treated as wedged.
//
// Either-or matching is intentional: the cap fires even when the updated_at
// signal is suppressed by an active-but-unhelpful heartbeat. Pass 0 for
// either duration to disable that signal. limit caps the rows returned;
// 0 or negative values fall through to stuckScanLimit.
func (r *EnrichmentQueueRepo) ListStaleClaimed(ctx context.Context, updatedThreshold, claimedAtMaxAge time.Duration, limit int) ([]*model.EnrichmentJob, error) {
	if limit <= 0 {
		limit = stuckScanLimit
	}
	now := time.Now().UTC()
	updatedCutoff := now.Add(-updatedThreshold).Format(time.RFC3339)
	claimedCutoff := now.Add(-claimedAtMaxAge).Format(time.RFC3339)

	query := selectEnrichmentQueueColumns + ` FROM enrichment_queue
		WHERE status = 'processing'
		  AND (
		        (? > 0 AND updated_at < ?)
		     OR (? > 0 AND claimed_at IS NOT NULL AND claimed_at < ?)
		      )
		ORDER BY updated_at ASC LIMIT ?`
	// Bind a 0/1 flag for each signal rather than the duration itself. The
	// predicate only uses the parameter as an enable gate; the actual cutoff
	// is the timestamp parameter computed above. Binding int64(time.Duration)
	// passes nanoseconds (1.8e12 for 30m) and Postgres infers the parameter
	// type as int4 from the `> 0` comparison, which then overflows.
	updatedEnabled := 0
	if updatedThreshold > 0 {
		updatedEnabled = 1
	}
	claimedEnabled := 0
	if claimedAtMaxAge > 0 {
		claimedEnabled = 1
	}
	args := []any{
		updatedEnabled, updatedCutoff,
		claimedEnabled, claimedCutoff,
		limit,
	}
	if r.db.Backend() == BackendPostgres {
		query = selectEnrichmentQueueColumns + ` FROM enrichment_queue
			WHERE status = 'processing'
			  AND (
			        ($1 > 0 AND updated_at < $2)
			     OR ($3 > 0 AND claimed_at IS NOT NULL AND claimed_at < $4)
			      )
			ORDER BY updated_at ASC LIMIT $5`
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue list stale claimed: %w", err)
	}
	defer rows.Close()

	result := make([]*model.EnrichmentJob, 0)
	for rows.Next() {
		item, err := r.scanItemFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enrichment queue list stale claimed iteration: %w", err)
	}
	return result, nil
}

// CountStaleClaimed returns how many in-flight rows match either the
// updated_at staleness threshold OR the claimed_at hard cap. Used by the
// admin status endpoint at every poll without the slice allocation of
// ListStaleClaimed. Pass 0 for either duration to disable that signal.
func (r *EnrichmentQueueRepo) CountStaleClaimed(ctx context.Context, updatedThreshold, claimedAtMaxAge time.Duration) (int, error) {
	now := time.Now().UTC()
	updatedCutoff := now.Add(-updatedThreshold).Format(time.RFC3339)
	claimedCutoff := now.Add(-claimedAtMaxAge).Format(time.RFC3339)

	query := `SELECT COUNT(*) FROM enrichment_queue
		WHERE status = 'processing'
		  AND (
		        (? > 0 AND updated_at < ?)
		     OR (? > 0 AND claimed_at IS NOT NULL AND claimed_at < ?)
		      )`
	// See ListStaleClaimed for the rationale on binding 0/1 instead of the
	// raw duration; same Postgres int4 inference trap applies here.
	updatedEnabled := 0
	if updatedThreshold > 0 {
		updatedEnabled = 1
	}
	claimedEnabled := 0
	if claimedAtMaxAge > 0 {
		claimedEnabled = 1
	}
	args := []any{
		updatedEnabled, updatedCutoff,
		claimedEnabled, claimedCutoff,
	}
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM enrichment_queue
			WHERE status = 'processing'
			  AND (
			        ($1 > 0 AND updated_at < $2)
			     OR ($3 > 0 AND claimed_at IS NOT NULL AND claimed_at < $4)
			      )`
	}

	var n int
	row := r.db.QueryRow(ctx, query, args...)
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("enrichment queue count stale claimed: %w", err)
	}
	return n, nil
}

// RequeueStale resets a stuck in-flight row back to "pending", clears the
// claim/heartbeat/last_error fields, bumps attempts, and stamps
// last_requeue_reason for admin display. The `WHERE status='processing'`
// guard makes this idempotent: a second sweep or a worker that finished
// between ListStaleClaimed and RequeueStale produces a no-op (returns false).
func (r *EnrichmentQueueRepo) RequeueStale(ctx context.Context, id uuid.UUID, reason string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE enrichment_queue SET
		status              = 'pending',
		claimed_by          = NULL,
		claimed_at          = NULL,
		heartbeat_at        = NULL,
		last_error          = NULL,
		last_requeue_reason = ?,
		attempts            = attempts + 1,
		updated_at          = ?
		WHERE id = ? AND status = 'processing'`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET
			status              = 'pending',
			claimed_by          = NULL,
			claimed_at          = NULL,
			heartbeat_at        = NULL,
			last_error          = NULL,
			last_requeue_reason = $1,
			attempts            = attempts + 1,
			updated_at          = $2
			WHERE id = $3 AND status = 'processing'`
	}

	res, err := r.db.Exec(ctx, query, reason, now, id.String())
	if err != nil {
		return false, fmt.Errorf("enrichment queue requeue stale: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("enrichment queue requeue stale rows affected: %w", err)
	}
	return rows > 0, nil
}

// DeleteByNamespaceTx deletes all enrichment queue entries for a namespace
// inside the caller's transaction. Most rows have already been removed by the
// memory delete CASCADE; this catches any that referenced no memory.
func (r *EnrichmentQueueRepo) DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error {
	query := `DELETE FROM enrichment_queue WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM enrichment_queue WHERE namespace_id = $1`
	}
	if _, err := tx.ExecContext(ctx, query, namespaceID.String()); err != nil {
		return fmt.Errorf("enrichment queue delete by namespace: %w", err)
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

const selectEnrichmentQueueColumns = `SELECT id, memory_id, namespace_id, status, priority,
	claimed_at, claimed_by, heartbeat_at, attempts, max_attempts, last_error, last_requeue_reason,
	steps_completed, query_augment_skip_reason, completed_at, created_at, updated_at`

func (r *EnrichmentQueueRepo) scanItem(row *sql.Row) (*model.EnrichmentJob, error) {
	var item model.EnrichmentJob
	var idStr, memoryIDStr, namespaceIDStr string
	var claimedAtStr, claimedBy, heartbeatAtStr, lastRequeueReason sql.NullString
	var lastErrorStr, completedAtStr sql.NullString
	var stepsCompletedStr string
	var queryAugmentSkipReason sql.NullString
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &memoryIDStr, &namespaceIDStr, &item.Status, &item.Priority,
		&claimedAtStr, &claimedBy, &heartbeatAtStr, &item.Attempts, &item.MaxAttempts,
		&lastErrorStr, &lastRequeueReason, &stepsCompletedStr, &queryAugmentSkipReason,
		&completedAtStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateItem(&item, idStr, memoryIDStr, namespaceIDStr,
		claimedAtStr, claimedBy, heartbeatAtStr, lastErrorStr, lastRequeueReason,
		stepsCompletedStr, queryAugmentSkipReason, completedAtStr, createdAtStr, updatedAtStr)
}

func (r *EnrichmentQueueRepo) populateItem(
	item *model.EnrichmentJob,
	idStr, memoryIDStr, namespaceIDStr string,
	claimedAtStr, claimedBy, heartbeatAtStr sql.NullString,
	lastErrorStr, lastRequeueReason sql.NullString,
	stepsCompletedStr string,
	queryAugmentSkipReason sql.NullString,
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

	if heartbeatAtStr.Valid {
		t, err := time.Parse(time.RFC3339, heartbeatAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("enrichment queue parse heartbeat_at: %w", err)
		}
		item.HeartbeatAt = &t
	}

	if lastErrorStr.Valid {
		item.LastError = json.RawMessage(lastErrorStr.String)
	}

	if lastRequeueReason.Valid {
		s := lastRequeueReason.String
		item.LastRequeueReason = &s
	}

	item.StepsCompleted = json.RawMessage(stepsCompletedStr)

	if queryAugmentSkipReason.Valid {
		s := queryAugmentSkipReason.String
		item.QueryAugmentSkipReason = &s
	}

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
		case "completed":
			stats.Completed = count
		case "failed":
			stats.Failed = count
		}
	}
	return stats, rows.Err()
}

func (r *EnrichmentQueueRepo) scanItemFromRows(rows *sql.Rows) (*model.EnrichmentJob, error) {
	var item model.EnrichmentJob
	var idStr, memoryIDStr, namespaceIDStr string
	var claimedAtStr, claimedBy, heartbeatAtStr, lastRequeueReason sql.NullString
	var lastErrorStr, completedAtStr sql.NullString
	var stepsCompletedStr string
	var queryAugmentSkipReason sql.NullString
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &memoryIDStr, &namespaceIDStr, &item.Status, &item.Priority,
		&claimedAtStr, &claimedBy, &heartbeatAtStr, &item.Attempts, &item.MaxAttempts,
		&lastErrorStr, &lastRequeueReason, &stepsCompletedStr, &queryAugmentSkipReason,
		&completedAtStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("enrichment queue scan rows: %w", err)
	}

	return r.populateItem(&item, idStr, memoryIDStr, namespaceIDStr,
		claimedAtStr, claimedBy, heartbeatAtStr, lastErrorStr, lastRequeueReason,
		stepsCompletedStr, queryAugmentSkipReason, completedAtStr, createdAtStr, updatedAtStr)
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

	query := `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL, last_requeue_reason = NULL, completed_at = NULL, updated_at = ?
		WHERE status = 'failed'`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET status = 'pending', claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL, last_requeue_reason = NULL, completed_at = NULL, updated_at = $1
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
