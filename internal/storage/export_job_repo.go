package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ErrExportJobClaimLost is returned by Complete / Fail when a non-empty
// workerID was passed and no longer matches the row's claimed_by. Callers
// should log and drop — another worker (or operator action) took over.
var ErrExportJobClaimLost = errors.New("export job: claim lost")

// ExportJobRepo provides operations for the export_jobs table. Every
// user-scoped method takes userID uuid.UUID so the privacy invariant in
// privacy_invariant_test.go remains satisfied — admins cannot read another
// user's export rows by addressing a job ID they do not own.
type ExportJobRepo struct {
	db DB
}

// NewExportJobRepo constructs an ExportJobRepo over the given DB.
func NewExportJobRepo(db DB) *ExportJobRepo {
	return &ExportJobRepo{db: db}
}

// Enqueue inserts a new pending export job. Zero-valued ID / timestamps
// are filled in. project_id is bound as NULL when ProjectID is nil.
func (r *ExportJobRepo) Enqueue(ctx context.Context, job *model.ExportJob) error {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	if job.Status == "" {
		job.Status = model.ExportStatusPending
	}
	if job.Format == "" {
		job.Format = model.ExportFormatZip
	}
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}

	var projectArg any
	if job.ProjectID != nil {
		projectArg = job.ProjectID.String()
	}

	createdStr := job.CreatedAt.UTC().Format(time.RFC3339)
	updatedStr := job.UpdatedAt.UTC().Format(time.RFC3339)

	query := `INSERT INTO export_jobs
		(id, user_id, scope, project_id, format, include_superseded, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}

	_, err := r.db.Exec(ctx, query,
		job.ID.String(), job.UserID.String(), job.Scope, projectArg,
		job.Format, job.IncludeSuperseded, job.Status,
		createdStr, updatedStr,
	)
	if err != nil {
		return fmt.Errorf("export job enqueue: %w", err)
	}
	return nil
}

// ClaimNext atomically transitions the oldest pending job to processing,
// stamping claimed_by and claimed_at. Returns sql.ErrNoRows when the queue
// is empty.
func (r *ExportJobRepo) ClaimNext(ctx context.Context, workerID string) (*model.ExportJob, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	if r.db.Backend() == BackendPostgres {
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("export job claim begin tx: %w", err)
		}
		defer tx.Rollback() //nolint:errcheck

		row := tx.QueryRow(
			`SELECT id FROM export_jobs
				WHERE status = 'pending'
				ORDER BY created_at ASC
				LIMIT 1
				FOR UPDATE SKIP LOCKED`,
		)
		var idStr string
		if err := row.Scan(&idStr); err != nil {
			return nil, err // sql.ErrNoRows when empty
		}

		_, err = tx.Exec(
			`UPDATE export_jobs SET status = 'processing',
				claimed_by = $1, claimed_at = $2, started_at = $3, updated_at = $4
				WHERE id = $5`,
			workerID, now, now, now, idStr,
		)
		if err != nil {
			return nil, fmt.Errorf("export job claim update: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("export job claim commit: %w", err)
		}

		id, _ := uuid.Parse(idStr)
		return r.getByIDInternal(ctx, id)
	}

	// SQLite: atomic UPDATE … WHERE id = (SELECT … LIMIT 1) with busy retry.
	var result sql.Result
	var err error
	for attempt := range 3 {
		result, err = r.db.Exec(ctx,
			`UPDATE export_jobs SET status = 'processing',
				claimed_by = ?, claimed_at = ?, started_at = ?, updated_at = ?
				WHERE id = (
					SELECT id FROM export_jobs
					WHERE status = 'pending'
					ORDER BY created_at ASC
					LIMIT 1
				)`,
			workerID, now, now, now,
		)
		if err == nil || !isSQLiteBusy(err) {
			break
		}
		time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("export job claim: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("export job claim rows affected: %w", err)
	}
	if rows == 0 {
		return nil, sql.ErrNoRows
	}

	// Fetch the just-claimed row.
	row := r.db.QueryRow(ctx,
		selectExportJobColumns+` FROM export_jobs
			WHERE status = 'processing' AND claimed_by = ?
			ORDER BY claimed_at DESC LIMIT 1`,
		workerID,
	)
	return r.scanRow(row)
}

// Complete transitions a processing job to succeeded, captures the
// artifact's path/size/sha256/expiry, and stamps completed_at. The workerID
// claim-guard returns ErrExportJobClaimLost when the row was reassigned.
func (r *ExportJobRepo) Complete(ctx context.Context, jobID uuid.UUID, workerID string, artifactPath string, artifactBytes int64, artifactSHA256 string, expiresAt time.Time) error {
	now := time.Now().UTC().Format(time.RFC3339)
	expStr := expiresAt.UTC().Format(time.RFC3339)

	query := `UPDATE export_jobs SET
		status = 'succeeded',
		artifact_path = ?,
		artifact_bytes = ?,
		artifact_sha256 = ?,
		completed_at = ?,
		expires_at = ?,
		updated_at = ?,
		error = NULL
		WHERE id = ? AND claimed_by = ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}

	res, err := r.db.Exec(ctx, query,
		artifactPath, artifactBytes, artifactSHA256,
		now, expStr, now,
		jobID.String(), workerID,
	)
	if err != nil {
		return fmt.Errorf("export job complete: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("export job complete rows affected: %w", err)
	}
	if rows == 0 {
		return ErrExportJobClaimLost
	}
	return nil
}

// Fail transitions a processing job to failed and records the error message.
// Same claim-guard semantics as Complete.
func (r *ExportJobRepo) Fail(ctx context.Context, jobID uuid.UUID, workerID string, errorMsg string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE export_jobs SET
		status = 'failed',
		error = ?,
		completed_at = ?,
		updated_at = ?
		WHERE id = ? AND claimed_by = ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}

	res, err := r.db.Exec(ctx, query, errorMsg, now, now, jobID.String(), workerID)
	if err != nil {
		return fmt.Errorf("export job fail: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("export job fail rows affected: %w", err)
	}
	if rows == 0 {
		return ErrExportJobClaimLost
	}
	return nil
}

// GetByID returns a single job scoped to the calling user. Returns
// sql.ErrNoRows when the job either does not exist or belongs to a
// different user — the two cases are indistinguishable to the caller so an
// attacker cannot probe other users' job IDs.
func (r *ExportJobRepo) GetByID(ctx context.Context, userID, jobID uuid.UUID) (*model.ExportJob, error) {
	query := selectExportJobColumns + ` FROM export_jobs WHERE id = ? AND user_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}
	row := r.db.QueryRow(ctx, query, jobID.String(), userID.String())
	return r.scanRow(row)
}

// getByIDInternal is the worker-only lookup used after claim — the
// claim-by-worker proof obviates the user_id filter.
func (r *ExportJobRepo) getByIDInternal(ctx context.Context, jobID uuid.UUID) (*model.ExportJob, error) {
	query := selectExportJobColumns + ` FROM export_jobs WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}
	row := r.db.QueryRow(ctx, query, jobID.String())
	return r.scanRow(row)
}

// ListByUser returns the user's export jobs ordered by created_at DESC.
func (r *ExportJobRepo) ListByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]model.ExportJob, error) {
	if limit <= 0 {
		limit = 50
	}
	query := selectExportJobColumns + ` FROM export_jobs
		WHERE user_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}

	rows, err := r.db.Query(ctx, query, userID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("export job list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.ExportJob{}
	for rows.Next() {
		j, err := r.scanRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export job list iteration: %w", err)
	}
	return out, nil
}

// CountActiveByUserSince returns how many jobs the user has enqueued or
// completed since `since`. Used for per-user rate limiting (1 in-flight +
// SettingExportMaxPerUserPerDay completed per rolling 24h window).
func (r *ExportJobRepo) CountActiveByUserSince(ctx context.Context, userID uuid.UUID, since time.Time) (int, error) {
	sinceStr := since.UTC().Format(time.RFC3339)
	query := `SELECT COUNT(*) FROM export_jobs
		WHERE user_id = ? AND created_at >= ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}
	var n int
	row := r.db.QueryRow(ctx, query, userID.String(), sinceStr)
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("export job count active: %w", err)
	}
	return n, nil
}

// CountInFlightByUser returns the number of pending+processing jobs for
// the user. Used to cap concurrent in-flight exports at 1 per user.
func (r *ExportJobRepo) CountInFlightByUser(ctx context.Context, userID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM export_jobs
		WHERE user_id = ? AND status IN ('pending','processing')`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}
	var n int
	row := r.db.QueryRow(ctx, query, userID.String())
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("export job count in-flight: %w", err)
	}
	return n, nil
}

// DeleteByUserAndID removes a job row scoped to the calling user. Returns
// sql.ErrNoRows when the job either does not exist or belongs to someone
// else. The caller is responsible for removing the on-disk artifact before
// (or after) invoking this — best-effort: a stale file is reclaimed by the
// expiry sweep regardless.
func (r *ExportJobRepo) DeleteByUserAndID(ctx context.Context, userID, jobID uuid.UUID) error {
	query := `DELETE FROM export_jobs WHERE id = ? AND user_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}
	res, err := r.db.Exec(ctx, query, jobID.String(), userID.String())
	if err != nil {
		return fmt.Errorf("export job delete: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("export job delete rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// ListExpired returns succeeded jobs whose expires_at is in the past, up
// to `limit`. Used by the cleanup sweep; not user-scoped because the sweep
// runs as the worker, not on behalf of a caller.
func (r *ExportJobRepo) ListExpired(ctx context.Context, limit int) ([]model.ExportJob, error) {
	if limit <= 0 {
		limit = 100
	}
	now := time.Now().UTC().Format(time.RFC3339)
	query := selectExportJobColumns + ` FROM export_jobs
		WHERE status IN ('succeeded','failed')
		  AND expires_at IS NOT NULL
		  AND expires_at < ?
		ORDER BY expires_at ASC
		LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}
	rows, err := r.db.Query(ctx, query, now, limit)
	if err != nil {
		return nil, fmt.Errorf("export job list expired: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.ExportJob{}
	for rows.Next() {
		j, err := r.scanRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export job list expired iteration: %w", err)
	}
	return out, nil
}

// MarkExpired flips a job to status='expired' and clears the artifact path.
// Called by the cleanup sweep after the on-disk artifact has been removed.
func (r *ExportJobRepo) MarkExpired(ctx context.Context, jobID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE export_jobs SET
		status = 'expired',
		artifact_path = NULL,
		updated_at = ?
		WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = postgresPlaceholders(query)
	}
	_, err := r.db.Exec(ctx, query, now, jobID.String())
	if err != nil {
		return fmt.Errorf("export job mark expired: %w", err)
	}
	return nil
}

const selectExportJobColumns = `SELECT id, user_id, scope, project_id, format, include_superseded,
	status, artifact_path, artifact_bytes, artifact_sha256, error,
	claimed_by, claimed_at, started_at, completed_at, expires_at,
	created_at, updated_at`

func (r *ExportJobRepo) scanRow(row *sql.Row) (*model.ExportJob, error) {
	return r.populate(scanFn(row.Scan))
}

func (r *ExportJobRepo) scanRows(rows *sql.Rows) (*model.ExportJob, error) {
	return r.populate(scanFn(rows.Scan))
}

// scanFn unifies *sql.Row.Scan and *sql.Rows.Scan so populate works on both.
type scanFn func(dest ...any) error

func (r *ExportJobRepo) populate(scan scanFn) (*model.ExportJob, error) {
	var (
		idStr, userIDStr, scope, format, status                         string
		projectIDStr, artifactPath, artifactSHA256, errorMsg, claimedBy sql.NullString
		claimedAtStr, startedAtStr, completedAtStr, expiresAtStr        sql.NullString
		createdAtStr, updatedAtStr                                      string
		artifactBytes                                                   sql.NullInt64
		includeSuperseded                                               bool
	)
	if err := scan(
		&idStr, &userIDStr, &scope, &projectIDStr, &format, &includeSuperseded,
		&status, &artifactPath, &artifactBytes, &artifactSHA256, &errorMsg,
		&claimedBy, &claimedAtStr, &startedAtStr, &completedAtStr, &expiresAtStr,
		&createdAtStr, &updatedAtStr,
	); err != nil {
		return nil, err
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("export job parse id: %w", err)
	}
	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		return nil, fmt.Errorf("export job parse user_id: %w", err)
	}

	job := &model.ExportJob{
		ID:                id,
		UserID:            userID,
		Scope:             scope,
		Format:            format,
		IncludeSuperseded: includeSuperseded,
		Status:            status,
	}
	if projectIDStr.Valid {
		pid, err := uuid.Parse(projectIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("export job parse project_id: %w", err)
		}
		job.ProjectID = &pid
	}
	if artifactPath.Valid {
		s := artifactPath.String
		job.ArtifactPath = &s
	}
	if artifactBytes.Valid {
		v := artifactBytes.Int64
		job.ArtifactBytes = &v
	}
	if artifactSHA256.Valid {
		s := artifactSHA256.String
		job.ArtifactSHA256 = &s
	}
	if errorMsg.Valid {
		s := errorMsg.String
		job.Error = &s
	}
	if claimedBy.Valid {
		s := claimedBy.String
		job.ClaimedBy = &s
	}
	if claimedAtStr.Valid {
		t, err := parseRFC3339(claimedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("export job parse claimed_at: %w", err)
		}
		job.ClaimedAt = &t
	}
	if startedAtStr.Valid {
		t, err := parseRFC3339(startedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("export job parse started_at: %w", err)
		}
		job.StartedAt = &t
	}
	if completedAtStr.Valid {
		t, err := parseRFC3339(completedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("export job parse completed_at: %w", err)
		}
		job.CompletedAt = &t
	}
	if expiresAtStr.Valid {
		t, err := parseRFC3339(expiresAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("export job parse expires_at: %w", err)
		}
		job.ExpiresAt = &t
	}

	job.CreatedAt, err = parseRFC3339(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("export job parse created_at: %w", err)
	}
	job.UpdatedAt, err = parseRFC3339(updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("export job parse updated_at: %w", err)
	}
	return job, nil
}

// parseRFC3339 accepts the canonical RFC3339 layout written by both
// backends and tolerates Postgres' optional sub-second precision.
func parseRFC3339(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	// Postgres TIMESTAMPTZ may surface microseconds.
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	// Last-ditch: bare space-separated form some drivers emit.
	if t, err := time.Parse("2006-01-02 15:04:05.999999-07:00", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp %q", strings.TrimSpace(s))
}
