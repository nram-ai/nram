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

// NamespaceRepo provides CRUD operations for the namespaces table.
type NamespaceRepo struct {
	db DB
}

// NewNamespaceRepo creates a new NamespaceRepo backed by the given DB.
func NewNamespaceRepo(db DB) *NamespaceRepo {
	return &NamespaceRepo{db: db}
}

// Create inserts a new namespace. The caller must set Name, Slug, Kind, ParentID, Path, Depth.
// ID is generated if zero-valued. Metadata defaults to `{}` if nil.
// CreatedAt and UpdatedAt are set by the database defaults.
func (r *NamespaceRepo) Create(ctx context.Context, ns *model.Namespace) error {
	if ns.ID == uuid.Nil {
		ns.ID = uuid.New()
	}
	if ns.Metadata == nil {
		ns.Metadata = json.RawMessage(`{}`)
	}

	var parentID *string
	if ns.ParentID != nil {
		s := ns.ParentID.String()
		parentID = &s
	}

	query := `INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	}

	_, err := r.db.Exec(ctx, query,
		ns.ID.String(), ns.Name, ns.Slug, ns.Kind,
		parentID, ns.Path, ns.Depth, string(ns.Metadata),
	)
	if err != nil {
		return fmt.Errorf("namespace create: %w", err)
	}

	// Read back the row to populate CreatedAt/UpdatedAt from DB defaults.
	return r.reload(ctx, ns)
}

// GetByID returns a namespace by its UUID.
func (r *NamespaceRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error) {
	query := `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
		FROM namespaces WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
			FROM namespaces WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanNamespace(row)
}

// GetByPath returns a namespace by its exact materialized path.
func (r *NamespaceRepo) GetByPath(ctx context.Context, path string) (*model.Namespace, error) {
	query := `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
		FROM namespaces WHERE path = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
			FROM namespaces WHERE path = $1`
	}

	row := r.db.QueryRow(ctx, query, path)
	return r.scanNamespace(row)
}

// ListByParent returns all direct children of a parent namespace.
func (r *NamespaceRepo) ListByParent(ctx context.Context, parentID uuid.UUID) ([]model.Namespace, error) {
	query := `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
		FROM namespaces WHERE parent_id = ? ORDER BY slug`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
			FROM namespaces WHERE parent_id = $1 ORDER BY slug`
	}

	rows, err := r.db.Query(ctx, query, parentID.String())
	if err != nil {
		return nil, fmt.Errorf("namespace list by parent: %w", err)
	}
	defer rows.Close()

	result := []model.Namespace{}
	for rows.Next() {
		ns, err := r.scanNamespaceFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *ns)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("namespace list by parent iteration: %w", err)
	}
	return result, nil
}

// FindBySlugUnderParent finds a namespace with the given slug under a specific parent.
func (r *NamespaceRepo) FindBySlugUnderParent(ctx context.Context, parentID uuid.UUID, slug string) (*model.Namespace, error) {
	query := `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
		FROM namespaces WHERE parent_id = ? AND slug = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
			FROM namespaces WHERE parent_id = $1 AND slug = $2`
	}

	row := r.db.QueryRow(ctx, query, parentID.String(), slug)
	return r.scanNamespace(row)
}

// CreateIfNotExists atomically creates the namespace if no namespace with the same
// (parent_id, slug) exists. Returns the namespace (existing or newly created) and
// a bool indicating if it was created.
func (r *NamespaceRepo) CreateIfNotExists(ctx context.Context, ns *model.Namespace) (*model.Namespace, bool, error) {
	if ns.ID == uuid.Nil {
		ns.ID = uuid.New()
	}
	if ns.Metadata == nil {
		ns.Metadata = json.RawMessage(`{}`)
	}

	var parentID *string
	if ns.ParentID != nil {
		s := ns.ParentID.String()
		parentID = &s
	}

	var query string
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (parent_id, slug) DO NOTHING`
	} else {
		query = `INSERT OR IGNORE INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	}

	result, err := r.db.Exec(ctx, query,
		ns.ID.String(), ns.Name, ns.Slug, ns.Kind,
		parentID, ns.Path, ns.Depth, string(ns.Metadata),
	)
	if err != nil {
		return nil, false, fmt.Errorf("namespace create if not exists: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, false, fmt.Errorf("namespace create if not exists rows affected: %w", err)
	}

	if affected == 1 {
		// Row was inserted — reload to get DB-set timestamps.
		if err := r.reload(ctx, ns); err != nil {
			return nil, false, err
		}
		return ns, true, nil
	}

	// Row already existed — fetch the existing one.
	existing, err := r.FindBySlugUnderParent(ctx, *ns.ParentID, ns.Slug)
	if err != nil {
		return nil, false, fmt.Errorf("namespace create if not exists fetch existing: %w", err)
	}
	return existing, false, nil
}

// ResolvePathPrefix returns all namespace IDs whose path starts with the given prefix.
// This includes the namespace whose path exactly equals the prefix.
func (r *NamespaceRepo) ResolvePathPrefix(ctx context.Context, prefix string) ([]uuid.UUID, error) {
	query := `SELECT id FROM namespaces WHERE path = ? OR path LIKE ? || '/%'`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id FROM namespaces WHERE path = $1 OR path LIKE $1 || '/%'`
	}

	var rows *sql.Rows
	var err error
	if r.db.Backend() == BackendPostgres {
		rows, err = r.db.Query(ctx, query, prefix)
	} else {
		rows, err = r.db.Query(ctx, query, prefix, prefix)
	}
	if err != nil {
		return nil, fmt.Errorf("namespace resolve path prefix: %w", err)
	}
	defer rows.Close()

	ids := []uuid.UUID{}
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, fmt.Errorf("namespace resolve path prefix scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("namespace resolve path prefix parse uuid: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("namespace resolve path prefix iteration: %w", err)
	}
	return ids, nil
}

// reload fetches the namespace by ID and populates the struct in place.
func (r *NamespaceRepo) reload(ctx context.Context, ns *model.Namespace) error {
	fetched, err := r.GetByID(ctx, ns.ID)
	if err != nil {
		return fmt.Errorf("namespace reload: %w", err)
	}
	*ns = *fetched
	return nil
}

// scanNamespace scans a single row into a model.Namespace.
func (r *NamespaceRepo) scanNamespace(row *sql.Row) (*model.Namespace, error) {
	var ns model.Namespace
	var idStr, parentIDStr sql.NullString
	var metadataStr string
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &ns.Name, &ns.Slug, &ns.Kind,
		&parentIDStr, &ns.Path, &ns.Depth,
		&metadataStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	id, err := uuid.Parse(idStr.String)
	if err != nil {
		return nil, fmt.Errorf("namespace scan parse id: %w", err)
	}
	ns.ID = id

	if parentIDStr.Valid {
		pid, err := uuid.Parse(parentIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("namespace scan parse parent_id: %w", err)
		}
		ns.ParentID = &pid
	}

	ns.Metadata = json.RawMessage(metadataStr)

	ns.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("namespace scan parse created_at: %w", err)
	}
	ns.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("namespace scan parse updated_at: %w", err)
	}

	return &ns, nil
}

// scanNamespaceFromRows scans the current row from sql.Rows into a model.Namespace.
func (r *NamespaceRepo) scanNamespaceFromRows(rows *sql.Rows) (*model.Namespace, error) {
	var ns model.Namespace
	var idStr, parentIDStr sql.NullString
	var metadataStr string
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &ns.Name, &ns.Slug, &ns.Kind,
		&parentIDStr, &ns.Path, &ns.Depth,
		&metadataStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("namespace scan rows: %w", err)
	}

	id, err := uuid.Parse(idStr.String)
	if err != nil {
		return nil, fmt.Errorf("namespace scan rows parse id: %w", err)
	}
	ns.ID = id

	if parentIDStr.Valid {
		pid, err := uuid.Parse(parentIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("namespace scan rows parse parent_id: %w", err)
		}
		ns.ParentID = &pid
	}

	ns.Metadata = json.RawMessage(metadataStr)

	ns.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("namespace scan rows parse created_at: %w", err)
	}
	ns.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("namespace scan rows parse updated_at: %w", err)
	}

	return &ns, nil
}
