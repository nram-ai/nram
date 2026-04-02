package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// DreamDirtyRepo provides operations for the dream_project_dirty table,
// tracking which projects have unprocessed user-originated changes.
type DreamDirtyRepo struct {
	db DB
}

// NewDreamDirtyRepo creates a new DreamDirtyRepo backed by the given DB.
func NewDreamDirtyRepo(db DB) *DreamDirtyRepo {
	return &DreamDirtyRepo{db: db}
}

// MarkDirty upserts the dirty flag for a project. Updates dirty_since on every
// call so the cooldown tracks the most recent change, not the first.
func (r *DreamDirtyRepo) MarkDirty(ctx context.Context, projectID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	if r.db.Backend() == BackendPostgres {
		query := `INSERT INTO dream_project_dirty (project_id, dirty_since)
			VALUES ($1, $2)
			ON CONFLICT (project_id) DO UPDATE SET dirty_since = $2`
		_, err := r.db.Exec(ctx, query, projectID.String(), now)
		if err != nil {
			return fmt.Errorf("dream dirty mark: %w", err)
		}
		return nil
	}

	query := `INSERT INTO dream_project_dirty (project_id, dirty_since)
		VALUES (?, ?)
		ON CONFLICT(project_id) DO UPDATE SET dirty_since = ?`
	_, err := r.db.Exec(ctx, query, projectID.String(), now, now)
	if err != nil {
		return fmt.Errorf("dream dirty mark: %w", err)
	}
	return nil
}

// ClearDirty nulls out the dirty_since flag for a project, preserving the row
// so that last_dream_at is retained for interval gating.
func (r *DreamDirtyRepo) ClearDirty(ctx context.Context, projectID uuid.UUID) error {
	query := `UPDATE dream_project_dirty SET dirty_since = NULL WHERE project_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE dream_project_dirty SET dirty_since = NULL WHERE project_id = $1`
	}

	_, err := r.db.Exec(ctx, query, projectID.String())
	if err != nil {
		return fmt.Errorf("dream dirty clear: %w", err)
	}
	return nil
}

// SetLastDreamAt updates the last_dream_at timestamp for a project.
// Uses upsert with dirty_since explicitly set to NULL to avoid re-dirtying.
func (r *DreamDirtyRepo) SetLastDreamAt(ctx context.Context, projectID uuid.UUID, at time.Time) error {
	atStr := at.UTC().Format(time.RFC3339)

	if r.db.Backend() == BackendPostgres {
		query := `INSERT INTO dream_project_dirty (project_id, dirty_since, last_dream_at)
			VALUES ($1, NULL, $2)
			ON CONFLICT (project_id) DO UPDATE SET last_dream_at = $2`
		_, err := r.db.Exec(ctx, query, projectID.String(), atStr)
		if err != nil {
			return fmt.Errorf("dream dirty set last_dream_at: %w", err)
		}
		return nil
	}

	query := `INSERT INTO dream_project_dirty (project_id, dirty_since, last_dream_at)
		VALUES (?, NULL, ?)
		ON CONFLICT(project_id) DO UPDATE SET last_dream_at = ?`
	_, err := r.db.Exec(ctx, query, projectID.String(), atStr, atStr)
	if err != nil {
		return fmt.Errorf("dream dirty set last_dream_at: %w", err)
	}
	return nil
}

// ListDirtyProjects returns all projects with pending user-originated changes.
func (r *DreamDirtyRepo) ListDirtyProjects(ctx context.Context) ([]model.DirtyProject, error) {
	query := `SELECT project_id, dirty_since, last_dream_at FROM dream_project_dirty WHERE dirty_since IS NOT NULL`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("dream dirty list: %w", err)
	}
	defer rows.Close()

	var result []model.DirtyProject
	for rows.Next() {
		var dp model.DirtyProject
		var projectIDStr, dirtySinceStr string
		var lastDreamAtStr sql.NullString

		if err := rows.Scan(&projectIDStr, &dirtySinceStr, &lastDreamAtStr); err != nil {
			return nil, fmt.Errorf("dream dirty scan: %w", err)
		}

		dp.ProjectID, _ = uuid.Parse(projectIDStr)
		dp.DirtySince, _ = time.Parse(time.RFC3339, dirtySinceStr)

		if lastDreamAtStr.Valid {
			t, _ := time.Parse(time.RFC3339, lastDreamAtStr.String)
			dp.LastDreamAt = &t
		}

		result = append(result, dp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dream dirty list iteration: %w", err)
	}
	return result, nil
}

// IsDirty returns whether a project has pending user-originated changes.
func (r *DreamDirtyRepo) IsDirty(ctx context.Context, projectID uuid.UUID) (bool, error) {
	query := `SELECT COUNT(*) FROM dream_project_dirty WHERE project_id = ? AND dirty_since IS NOT NULL`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM dream_project_dirty WHERE project_id = $1 AND dirty_since IS NOT NULL`
	}

	var count int
	err := r.db.QueryRow(ctx, query, projectID.String()).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("dream dirty is dirty: %w", err)
	}
	return count > 0, nil
}

// CountDirty returns the total number of dirty projects.
func (r *DreamDirtyRepo) CountDirty(ctx context.Context) (int, error) {
	query := `SELECT COUNT(*) FROM dream_project_dirty WHERE dirty_since IS NOT NULL`

	var count int
	err := r.db.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("dream dirty count: %w", err)
	}
	return count, nil
}
