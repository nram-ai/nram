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

// DreamCycleRepo provides operations for the dream_cycles table.
type DreamCycleRepo struct {
	db DB
}

// NewDreamCycleRepo creates a new DreamCycleRepo backed by the given DB.
func NewDreamCycleRepo(db DB) *DreamCycleRepo {
	return &DreamCycleRepo{db: db}
}

// Create inserts a new dream cycle record.
func (r *DreamCycleRepo) Create(ctx context.Context, cycle *model.DreamCycle) error {
	if cycle.ID == uuid.Nil {
		cycle.ID = uuid.New()
	}
	if cycle.Status == "" {
		cycle.Status = model.DreamStatusPending
	}
	if cycle.PhaseSummary == nil {
		cycle.PhaseSummary = json.RawMessage(`{}`)
	}

	query := `INSERT INTO dream_cycles (id, project_id, namespace_id, status, phase, tokens_used, token_budget, phase_summary)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO dream_cycles (id, project_id, namespace_id, status, phase, tokens_used, token_budget, phase_summary)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	}

	_, err := r.db.Exec(ctx, query,
		cycle.ID.String(), cycle.ProjectID.String(), cycle.NamespaceID.String(),
		cycle.Status, cycle.Phase, cycle.TokensUsed, cycle.TokenBudget,
		string(cycle.PhaseSummary),
	)
	if err != nil {
		return fmt.Errorf("dream cycle create: %w", err)
	}

	return r.reload(ctx, cycle)
}

// GetByID returns a dream cycle by its UUID.
func (r *DreamCycleRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.DreamCycle, error) {
	query := selectDreamCycleColumns + ` FROM dream_cycles WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamCycleColumns + ` FROM dream_cycles WHERE id = $1`
	}
	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanRow(row)
}

// UpdateStatus updates the status, phase, and token usage of a dream cycle.
// Guarded on status IN ('pending','running') so a phase-boundary write that
// loses to a concurrent Abandon is dropped silently rather than overwriting
// the failed state.
func (r *DreamCycleRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status, phase string, tokensUsed int) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE dream_cycles SET status = ?, phase = ?, tokens_used = ?, updated_at = ? WHERE id = ? AND status IN ('pending', 'running')`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET status = $1, phase = $2, tokens_used = $3, updated_at = $4 WHERE id = $5 AND status IN ('pending', 'running')`
	}

	_, err := r.db.Exec(ctx, query, status, phase, tokensUsed, now, id.String())
	if err != nil {
		return fmt.Errorf("dream cycle update status: %w", err)
	}
	return nil
}

// Heartbeat updates only heartbeat_at on a running cycle. The status guard
// prevents reviving a just-terminated row when a heartbeat goroutine and the
// runner's terminal write race.
func (r *DreamCycleRepo) Heartbeat(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE dream_cycles SET heartbeat_at = ? WHERE id = ? AND status = 'running'`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET heartbeat_at = $1 WHERE id = $2 AND status = 'running'`
	}

	_, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("dream cycle heartbeat: %w", err)
	}
	return nil
}

// Start marks a dream cycle as running and records the start time.
func (r *DreamCycleRepo) Start(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE dream_cycles SET status = ?, started_at = ?, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET status = $1, started_at = $2, updated_at = $3 WHERE id = $4`
	}

	_, err := r.db.Exec(ctx, query, model.DreamStatusRunning, now, now, id.String())
	if err != nil {
		return fmt.Errorf("dream cycle start: %w", err)
	}
	return nil
}

// Complete marks a dream cycle as completed with a phase summary and final
// token count. Guarded on status='running' so a late completion that loses
// to a concurrent Abandon is dropped silently.
func (r *DreamCycleRepo) Complete(ctx context.Context, id uuid.UUID, summary json.RawMessage, tokensUsed int) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE dream_cycles SET status = ?, phase_summary = ?, tokens_used = ?, completed_at = ?, updated_at = ? WHERE id = ? AND status = 'running'`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET status = $1, phase_summary = $2, tokens_used = $3, completed_at = $4, updated_at = $5 WHERE id = $6 AND status = 'running'`
	}

	_, err := r.db.Exec(ctx, query, model.DreamStatusCompleted, string(summary), tokensUsed, now, now, id.String())
	if err != nil {
		return fmt.Errorf("dream cycle complete: %w", err)
	}
	return nil
}

// Fail marks a dream cycle as failed with an error message and final token
// count. Guarded on status IN ('pending','running') so a runner-side fail
// that loses to a concurrent Abandon is dropped silently.
func (r *DreamCycleRepo) Fail(ctx context.Context, id uuid.UUID, errMsg string, tokensUsed int) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE dream_cycles SET status = ?, error = ?, tokens_used = ?, completed_at = ?, updated_at = ? WHERE id = ? AND status IN ('pending', 'running')`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET status = $1, error = $2, tokens_used = $3, completed_at = $4, updated_at = $5 WHERE id = $6 AND status IN ('pending', 'running')`
	}

	_, err := r.db.Exec(ctx, query, model.DreamStatusFailed, errMsg, tokensUsed, now, now, id.String())
	if err != nil {
		return fmt.Errorf("dream cycle fail: %w", err)
	}
	return nil
}

// Abandon transitions a non-terminal cycle to failed with the given reason
// and a completed_at stamp. Returns true iff the row was actually transitioned
// (i.e. it was pending/running at the time of the write); false means it was
// already terminal. tokens_used is intentionally NOT touched so the partial
// accounting written at the last phase boundary survives the abandon.
func (r *DreamCycleRepo) Abandon(ctx context.Context, id uuid.UUID, reason string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE dream_cycles SET status = ?, error = ?, completed_at = ?, updated_at = ? WHERE id = ? AND status IN ('pending', 'running')`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET status = $1, error = $2, completed_at = $3, updated_at = $4 WHERE id = $5 AND status IN ('pending', 'running')`
	}

	res, err := r.db.Exec(ctx, query, model.DreamStatusFailed, reason, now, now, id.String())
	if err != nil {
		return false, fmt.Errorf("dream cycle abandon: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("dream cycle abandon rows affected: %w", err)
	}
	return rows > 0, nil
}

// staleScanLimit caps a single ListStale call so a deployment that crashes
// thousands of cycles can't pull them all into memory at once. The sweeper
// runs on a periodic ticker, so anything past the limit drains on the next
// tick.
const staleScanLimit = 1000

// ListStale returns running cycles whose updated_at is older than the given
// threshold. The stuck-cycle sweeper uses this to find cycles whose worker
// is presumed gone (crash, OOM, deploy mid-cycle). Bounded by staleScanLimit.
func (r *DreamCycleRepo) ListStale(ctx context.Context, threshold time.Duration) ([]model.DreamCycle, error) {
	cutoff := time.Now().UTC().Add(-threshold).Format(time.RFC3339)

	query := selectDreamCycleColumns + ` FROM dream_cycles WHERE status = 'running' AND updated_at < ? ORDER BY updated_at ASC LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamCycleColumns + ` FROM dream_cycles WHERE status = 'running' AND updated_at < $1 ORDER BY updated_at ASC LIMIT $2`
	}

	rows, err := r.db.Query(ctx, query, cutoff, staleScanLimit)
	if err != nil {
		return nil, fmt.Errorf("dream cycle list stale: %w", err)
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// CountStale returns the number of running cycles whose updated_at is older
// than the given threshold. The admin status endpoint calls this at every
// poll (10s) — using COUNT avoids the per-poll allocation of a slice that's
// only used for its length.
func (r *DreamCycleRepo) CountStale(ctx context.Context, threshold time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-threshold).Format(time.RFC3339)

	query := `SELECT COUNT(*) FROM dream_cycles WHERE status = 'running' AND updated_at < ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM dream_cycles WHERE status = 'running' AND updated_at < $1`
	}

	var n int
	row := r.db.QueryRow(ctx, query, cutoff)
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("dream cycle count stale: %w", err)
	}
	return n, nil
}

// MarkRolledBack sets a dream cycle status to rolled_back.
func (r *DreamCycleRepo) MarkRolledBack(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE dream_cycles SET status = ?, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET status = $1, updated_at = $2 WHERE id = $3`
	}

	_, err := r.db.Exec(ctx, query, model.DreamStatusRolledBack, now, id.String())
	if err != nil {
		return fmt.Errorf("dream cycle mark rolled back: %w", err)
	}
	return nil
}

// ListByProject returns dream cycles for a project ordered by created_at DESC.
func (r *DreamCycleRepo) ListByProject(ctx context.Context, projectID uuid.UUID, limit int) ([]model.DreamCycle, error) {
	query := selectDreamCycleColumns + ` FROM dream_cycles WHERE project_id = ? ORDER BY created_at DESC LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamCycleColumns + ` FROM dream_cycles WHERE project_id = $1 ORDER BY created_at DESC LIMIT $2`
	}

	rows, err := r.db.Query(ctx, query, projectID.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("dream cycle list by project: %w", err)
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// ListRecent returns recent dream cycles across all projects.
func (r *DreamCycleRepo) ListRecent(ctx context.Context, limit int) ([]model.DreamCycle, error) {
	query := selectDreamCycleColumns + ` FROM dream_cycles ORDER BY created_at DESC LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamCycleColumns + ` FROM dream_cycles ORDER BY created_at DESC LIMIT $1`
	}

	rows, err := r.db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("dream cycle list recent: %w", err)
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// GetLastCompleted returns the most recently completed dream cycle for a project.
func (r *DreamCycleRepo) GetLastCompleted(ctx context.Context, projectID uuid.UUID) (*model.DreamCycle, error) {
	query := selectDreamCycleColumns + ` FROM dream_cycles WHERE project_id = ? AND status = ? ORDER BY completed_at DESC LIMIT 1`
	if r.db.Backend() == BackendPostgres {
		query = selectDreamCycleColumns + ` FROM dream_cycles WHERE project_id = $1 AND status = $2 ORDER BY completed_at DESC LIMIT 1`
	}

	row := r.db.QueryRow(ctx, query, projectID.String(), model.DreamStatusCompleted)
	return r.scanRow(row)
}

// DeleteByProject removes all dream cycles for a project.
func (r *DreamCycleRepo) DeleteByProject(ctx context.Context, projectID uuid.UUID) error {
	query := `DELETE FROM dream_cycles WHERE project_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM dream_cycles WHERE project_id = $1`
	}

	_, err := r.db.Exec(ctx, query, projectID.String())
	if err != nil {
		return fmt.Errorf("dream cycle delete by project: %w", err)
	}
	return nil
}

func (r *DreamCycleRepo) reload(ctx context.Context, cycle *model.DreamCycle) error {
	fetched, err := r.GetByID(ctx, cycle.ID)
	if err != nil {
		return fmt.Errorf("dream cycle reload: %w", err)
	}
	*cycle = *fetched
	return nil
}

const selectDreamCycleColumns = `SELECT id, project_id, namespace_id, status, phase,
	tokens_used, token_budget, phase_summary, error,
	started_at, completed_at, heartbeat_at, created_at, updated_at`

func (r *DreamCycleRepo) scanRow(row *sql.Row) (*model.DreamCycle, error) {
	var c model.DreamCycle
	var idStr, projectIDStr, namespaceIDStr string
	var phaseSummaryStr string
	var errorStr, startedAtStr, completedAtStr, heartbeatAtStr sql.NullString
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &projectIDStr, &namespaceIDStr, &c.Status, &c.Phase,
		&c.TokensUsed, &c.TokenBudget, &phaseSummaryStr, &errorStr,
		&startedAtStr, &completedAtStr, &heartbeatAtStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populate(&c, idStr, projectIDStr, namespaceIDStr, phaseSummaryStr,
		errorStr, startedAtStr, completedAtStr, heartbeatAtStr, createdAtStr, updatedAtStr)
}

func (r *DreamCycleRepo) scanRowFromRows(rows *sql.Rows) (*model.DreamCycle, error) {
	var c model.DreamCycle
	var idStr, projectIDStr, namespaceIDStr string
	var phaseSummaryStr string
	var errorStr, startedAtStr, completedAtStr, heartbeatAtStr sql.NullString
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &projectIDStr, &namespaceIDStr, &c.Status, &c.Phase,
		&c.TokensUsed, &c.TokenBudget, &phaseSummaryStr, &errorStr,
		&startedAtStr, &completedAtStr, &heartbeatAtStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("dream cycle scan rows: %w", err)
	}

	return r.populate(&c, idStr, projectIDStr, namespaceIDStr, phaseSummaryStr,
		errorStr, startedAtStr, completedAtStr, heartbeatAtStr, createdAtStr, updatedAtStr)
}

func (r *DreamCycleRepo) scanRows(rows *sql.Rows) ([]model.DreamCycle, error) {
	var result []model.DreamCycle
	for rows.Next() {
		c, err := r.scanRowFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dream cycle scan rows iteration: %w", err)
	}
	return result, nil
}

func (r *DreamCycleRepo) populate(
	c *model.DreamCycle,
	idStr, projectIDStr, namespaceIDStr string,
	phaseSummaryStr string,
	errorStr, startedAtStr, completedAtStr, heartbeatAtStr sql.NullString,
	createdAtStr, updatedAtStr string,
) (*model.DreamCycle, error) {
	var err error

	c.ID, err = uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("dream cycle parse id: %w", err)
	}
	c.ProjectID, err = uuid.Parse(projectIDStr)
	if err != nil {
		return nil, fmt.Errorf("dream cycle parse project_id: %w", err)
	}
	c.NamespaceID, err = uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("dream cycle parse namespace_id: %w", err)
	}

	c.PhaseSummary = json.RawMessage(phaseSummaryStr)

	if errorStr.Valid {
		c.Error = &errorStr.String
	}
	if startedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, startedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("dream cycle parse started_at: %w", err)
		}
		c.StartedAt = &t
	}
	if completedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, completedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("dream cycle parse completed_at: %w", err)
		}
		c.CompletedAt = &t
	}
	if heartbeatAtStr.Valid {
		t, err := time.Parse(time.RFC3339, heartbeatAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("dream cycle parse heartbeat_at: %w", err)
		}
		c.HeartbeatAt = &t
	}

	c.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("dream cycle parse created_at: %w", err)
	}
	c.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("dream cycle parse updated_at: %w", err)
	}

	return c, nil
}
