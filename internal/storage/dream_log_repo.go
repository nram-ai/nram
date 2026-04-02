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

// DreamLogRepo provides operations for the dream_logs and dream_log_summaries tables.
type DreamLogRepo struct {
	db DB
}

// NewDreamLogRepo creates a new DreamLogRepo backed by the given DB.
func NewDreamLogRepo(db DB) *DreamLogRepo {
	return &DreamLogRepo{db: db}
}

// Create inserts a new dream log entry.
func (r *DreamLogRepo) Create(ctx context.Context, entry *model.DreamLog) error {
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.BeforeState == nil {
		entry.BeforeState = json.RawMessage(`{}`)
	}
	if entry.AfterState == nil {
		entry.AfterState = json.RawMessage(`{}`)
	}

	query := `INSERT INTO dream_logs (id, cycle_id, project_id, phase, operation, target_type, target_id, before_state, after_state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO dream_logs (id, cycle_id, project_id, phase, operation, target_type, target_id, before_state, after_state)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	}

	_, err := r.db.Exec(ctx, query,
		entry.ID.String(), entry.CycleID.String(), entry.ProjectID.String(),
		entry.Phase, entry.Operation, entry.TargetType, entry.TargetID.String(),
		string(entry.BeforeState), string(entry.AfterState),
	)
	if err != nil {
		return fmt.Errorf("dream log create: %w", err)
	}
	return nil
}

// ListByCycle returns all log entries for a dream cycle ordered by created_at ASC.
func (r *DreamLogRepo) ListByCycle(ctx context.Context, cycleID uuid.UUID) ([]model.DreamLog, error) {
	query := selectDreamLogColumns + ` FROM dream_logs WHERE cycle_id = ? ORDER BY created_at ASC`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamLogColumns + ` FROM dream_logs WHERE cycle_id = $1 ORDER BY created_at ASC`
	}

	rows, err := r.db.Query(ctx, query, cycleID.String())
	if err != nil {
		return nil, fmt.Errorf("dream log list by cycle: %w", err)
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// ListByCycleReversed returns all log entries for a cycle in reverse chronological order.
func (r *DreamLogRepo) ListByCycleReversed(ctx context.Context, cycleID uuid.UUID) ([]model.DreamLog, error) {
	query := selectDreamLogColumns + ` FROM dream_logs WHERE cycle_id = ? ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamLogColumns + ` FROM dream_logs WHERE cycle_id = $1 ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, cycleID.String())
	if err != nil {
		return nil, fmt.Errorf("dream log list by cycle reversed: %w", err)
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// ListByProject returns log entries for a project with pagination.
func (r *DreamLogRepo) ListByProject(ctx context.Context, projectID uuid.UUID, limit, offset int) ([]model.DreamLog, error) {
	query := selectDreamLogColumns + ` FROM dream_logs WHERE project_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamLogColumns + ` FROM dream_logs WHERE project_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`
	}

	rows, err := r.db.Query(ctx, query, projectID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("dream log list by project: %w", err)
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// DeleteByCycle removes all log entries for a dream cycle.
func (r *DreamLogRepo) DeleteByCycle(ctx context.Context, cycleID uuid.UUID) error {
	query := `DELETE FROM dream_logs WHERE cycle_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM dream_logs WHERE cycle_id = $1`
	}

	_, err := r.db.Exec(ctx, query, cycleID.String())
	if err != nil {
		return fmt.Errorf("dream log delete by cycle: %w", err)
	}
	return nil
}

// DeleteOlderThan removes log entries for a project created before the given time.
// Returns the number of entries deleted.
func (r *DreamLogRepo) DeleteOlderThan(ctx context.Context, projectID uuid.UUID, before time.Time) (int64, error) {
	beforeStr := before.UTC().Format(time.RFC3339)

	query := `DELETE FROM dream_logs WHERE project_id = ? AND created_at < ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM dream_logs WHERE project_id = $1 AND created_at < $2`
	}

	result, err := r.db.Exec(ctx, query, projectID.String(), beforeStr)
	if err != nil {
		return 0, fmt.Errorf("dream log delete older than: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("dream log delete older than rows affected: %w", err)
	}
	return rows, nil
}

// CreateSummary inserts a compressed dream log summary.
func (r *DreamLogRepo) CreateSummary(ctx context.Context, summary *model.DreamLogSummary) error {
	if summary.ID == uuid.Nil {
		summary.ID = uuid.New()
	}
	if summary.Summary == nil {
		summary.Summary = json.RawMessage(`{}`)
	}

	query := `INSERT INTO dream_log_summaries (id, cycle_id, project_id, summary)
		VALUES (?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO dream_log_summaries (id, cycle_id, project_id, summary)
			VALUES ($1, $2, $3, $4)`
	}

	_, err := r.db.Exec(ctx, query,
		summary.ID.String(), summary.CycleID.String(), summary.ProjectID.String(),
		string(summary.Summary),
	)
	if err != nil {
		return fmt.Errorf("dream log summary create: %w", err)
	}
	return nil
}

// ListSummaries returns all summaries for a project ordered by created_at DESC.
func (r *DreamLogRepo) ListSummaries(ctx context.Context, projectID uuid.UUID) ([]model.DreamLogSummary, error) {
	query := `SELECT id, cycle_id, project_id, summary, created_at FROM dream_log_summaries WHERE project_id = ? ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, cycle_id, project_id, summary, created_at FROM dream_log_summaries WHERE project_id = $1 ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, projectID.String())
	if err != nil {
		return nil, fmt.Errorf("dream log summary list: %w", err)
	}
	defer rows.Close()

	var result []model.DreamLogSummary
	for rows.Next() {
		var s model.DreamLogSummary
		var idStr, cycleIDStr, projectIDStr, summaryStr, createdAtStr string
		if err := rows.Scan(&idStr, &cycleIDStr, &projectIDStr, &summaryStr, &createdAtStr); err != nil {
			return nil, fmt.Errorf("dream log summary scan: %w", err)
		}

		s.ID, _ = uuid.Parse(idStr)
		s.CycleID, _ = uuid.Parse(cycleIDStr)
		s.ProjectID, _ = uuid.Parse(projectIDStr)
		s.Summary = json.RawMessage(summaryStr)
		s.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)

		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dream log summary list iteration: %w", err)
	}
	return result, nil
}

// CountByCycle returns the number of log entries in a given cycle.
func (r *DreamLogRepo) CountByCycle(ctx context.Context, cycleID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM dream_logs WHERE cycle_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM dream_logs WHERE cycle_id = $1`
	}

	var count int
	err := r.db.QueryRow(ctx, query, cycleID.String()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("dream log count by cycle: %w", err)
	}
	return count, nil
}

const selectDreamLogColumns = `SELECT id, cycle_id, project_id, phase, operation,
	target_type, target_id, before_state, after_state, created_at`

func (r *DreamLogRepo) scanRows(rows *sql.Rows) ([]model.DreamLog, error) {
	var result []model.DreamLog
	for rows.Next() {
		var entry model.DreamLog
		var idStr, cycleIDStr, projectIDStr, targetIDStr string
		var beforeStr, afterStr, createdAtStr string

		if err := rows.Scan(
			&idStr, &cycleIDStr, &projectIDStr, &entry.Phase, &entry.Operation,
			&entry.TargetType, &targetIDStr, &beforeStr, &afterStr, &createdAtStr,
		); err != nil {
			return nil, fmt.Errorf("dream log scan: %w", err)
		}

		entry.ID, _ = uuid.Parse(idStr)
		entry.CycleID, _ = uuid.Parse(cycleIDStr)
		entry.ProjectID, _ = uuid.Parse(projectIDStr)
		entry.TargetID, _ = uuid.Parse(targetIDStr)
		entry.BeforeState = json.RawMessage(beforeStr)
		entry.AfterState = json.RawMessage(afterStr)
		entry.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)

		result = append(result, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dream log scan iteration: %w", err)
	}
	return result, nil
}
