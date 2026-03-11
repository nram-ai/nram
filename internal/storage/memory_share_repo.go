package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// MemoryShareRepo provides CRUD operations for the memory_shares table.
type MemoryShareRepo struct {
	db DB
}

// NewMemoryShareRepo creates a new MemoryShareRepo backed by the given DB.
func NewMemoryShareRepo(db DB) *MemoryShareRepo {
	return &MemoryShareRepo{db: db}
}

// Create inserts a new memory share. ID is generated if zero-valued.
func (r *MemoryShareRepo) Create(ctx context.Context, share *model.MemoryShare) error {
	if share.ID == uuid.Nil {
		share.ID = uuid.New()
	}

	var expiresAt *string
	if share.ExpiresAt != nil {
		s := share.ExpiresAt.UTC().Format(time.RFC3339)
		expiresAt = &s
	}

	var createdBy *string
	if share.CreatedBy != nil {
		s := share.CreatedBy.String()
		createdBy = &s
	}

	query := `INSERT INTO memory_shares (id, source_ns_id, target_ns_id, permission, created_by, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO memory_shares (id, source_ns_id, target_ns_id, permission, created_by, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6)`
	}

	_, err := r.db.Exec(ctx, query,
		share.ID.String(), share.SourceNsID.String(), share.TargetNsID.String(),
		share.Permission, createdBy, expiresAt,
	)
	if err != nil {
		return fmt.Errorf("memory share create: %w", err)
	}

	return r.reload(ctx, share)
}

// GetByID returns a memory share by its UUID.
func (r *MemoryShareRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.MemoryShare, error) {
	query := selectMemoryShareColumns + ` FROM memory_shares WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryShareColumns + ` FROM memory_shares WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanMemoryShare(row)
}

// Revoke sets the revoked_at timestamp for a memory share.
func (r *MemoryShareRepo) Revoke(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE memory_shares SET revoked_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memory_shares SET revoked_at = $1 WHERE id = $2`
	}

	result, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("memory share revoke: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("memory share revoke rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// ListSharedToNamespace returns all non-revoked, non-expired shares targeting a namespace,
// ordered by created_at DESC.
func (r *MemoryShareRepo) ListSharedToNamespace(ctx context.Context, targetNamespaceID uuid.UUID) ([]model.MemoryShare, error) {
	query := selectMemoryShareColumns + ` FROM memory_shares
		WHERE target_ns_id = ? AND revoked_at IS NULL
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryShareColumns + ` FROM memory_shares
			WHERE target_ns_id = $1 AND revoked_at IS NULL
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, targetNamespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("memory share list shared to namespace: %w", err)
	}
	defer rows.Close()

	return r.scanMemoryShares(rows)
}

// reload fetches the memory share by ID and populates the struct in place.
func (r *MemoryShareRepo) reload(ctx context.Context, share *model.MemoryShare) error {
	fetched, err := r.GetByID(ctx, share.ID)
	if err != nil {
		return fmt.Errorf("memory share reload: %w", err)
	}
	*share = *fetched
	return nil
}

const selectMemoryShareColumns = `SELECT id, source_ns_id, target_ns_id, permission, created_by, expires_at, revoked_at, created_at`

func (r *MemoryShareRepo) scanMemoryShare(row *sql.Row) (*model.MemoryShare, error) {
	var share model.MemoryShare
	var idStr, sourceNsStr, targetNsStr string
	var createdByStr sql.NullString
	var expiresAtStr sql.NullString
	var revokedAtStr sql.NullString
	var createdAtStr string

	err := row.Scan(
		&idStr, &sourceNsStr, &targetNsStr, &share.Permission,
		&createdByStr, &expiresAtStr, &revokedAtStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateMemoryShare(&share, idStr, sourceNsStr, targetNsStr, createdByStr, expiresAtStr, revokedAtStr, createdAtStr)
}

func (r *MemoryShareRepo) scanMemoryShareFromRows(rows *sql.Rows) (*model.MemoryShare, error) {
	var share model.MemoryShare
	var idStr, sourceNsStr, targetNsStr string
	var createdByStr sql.NullString
	var expiresAtStr sql.NullString
	var revokedAtStr sql.NullString
	var createdAtStr string

	err := rows.Scan(
		&idStr, &sourceNsStr, &targetNsStr, &share.Permission,
		&createdByStr, &expiresAtStr, &revokedAtStr, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("memory share scan rows: %w", err)
	}

	return r.populateMemoryShare(&share, idStr, sourceNsStr, targetNsStr, createdByStr, expiresAtStr, revokedAtStr, createdAtStr)
}

func (r *MemoryShareRepo) scanMemoryShares(rows *sql.Rows) ([]model.MemoryShare, error) {
	var result []model.MemoryShare
	for rows.Next() {
		share, err := r.scanMemoryShareFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *share)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory share scan iteration: %w", err)
	}
	return result, nil
}

func (r *MemoryShareRepo) populateMemoryShare(
	share *model.MemoryShare,
	idStr, sourceNsStr, targetNsStr string,
	createdByStr, expiresAtStr, revokedAtStr sql.NullString,
	createdAtStr string,
) (*model.MemoryShare, error) {
	var err error

	share.ID, err = uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("memory share parse id: %w", err)
	}

	share.SourceNsID, err = uuid.Parse(sourceNsStr)
	if err != nil {
		return nil, fmt.Errorf("memory share parse source_ns_id: %w", err)
	}

	share.TargetNsID, err = uuid.Parse(targetNsStr)
	if err != nil {
		return nil, fmt.Errorf("memory share parse target_ns_id: %w", err)
	}

	if createdByStr.Valid {
		id, err := uuid.Parse(createdByStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory share parse created_by: %w", err)
		}
		share.CreatedBy = &id
	}

	if expiresAtStr.Valid {
		t, err := time.Parse(time.RFC3339, expiresAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory share parse expires_at: %w", err)
		}
		share.ExpiresAt = &t
	}

	if revokedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, revokedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory share parse revoked_at: %w", err)
		}
		share.RevokedAt = &t
	}

	share.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("memory share parse created_at: %w", err)
	}

	return share, nil
}
