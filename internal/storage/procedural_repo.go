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

// ProceduralRepo provides CRUD operations for the procedural_entries table:
// the verbatim procedural memory tier. It deliberately has no coupling to the
// enrichment queue, embedder, or vector store: procedural entries are stored
// and read back byte-for-byte, never embedded or consolidated.
type ProceduralRepo struct {
	db DB
}

// NewProceduralRepo creates a new ProceduralRepo backed by the given DB.
func NewProceduralRepo(db DB) *ProceduralRepo {
	return &ProceduralRepo{db: db}
}

// selectProceduralColumns is the common SELECT clause for procedural queries.
const selectProceduralColumns = `SELECT id, namespace_id, content, title, category,
	tags, priority, enabled, origin, metadata, created_at, updated_at, deleted_at`

// Create inserts a new procedural entry. ID is generated if zero-valued.
// Metadata defaults to `{}` and Tags to `[]` if nil.
func (r *ProceduralRepo) Create(ctx context.Context, e *model.ProceduralEntry) error {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	if e.Metadata == nil {
		e.Metadata = json.RawMessage(`{}`)
	}
	if e.Tags == nil {
		e.Tags = []string{}
	}
	if e.Origin == "" {
		e.Origin = string(model.OriginUser)
	}

	tagsVal := encodeStringArray(r.db.Backend(), e.Tags)
	enabledVal := EncodeBool(r.db.Backend(), e.Enabled)

	query := `INSERT INTO procedural_entries (id, namespace_id, content, title, category, tags, priority, enabled, origin, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO procedural_entries (id, namespace_id, content, title, category, tags, priority, enabled, origin, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	}

	_, err := r.db.Exec(ctx, query,
		e.ID.String(), e.NamespaceID.String(), e.Content, e.Title, e.Category,
		tagsVal, e.Priority, enabledVal, e.Origin, string(e.Metadata),
	)
	if err != nil {
		return fmt.Errorf("procedural create: %w", err)
	}

	return r.reload(ctx, e)
}

// GetByID returns a live (non-deleted) procedural entry by its UUID, bounded to
// namespaceID. An entry in a different namespace reads as sql.ErrNoRows, so
// existence is never leaked across the tenant boundary.
func (r *ProceduralRepo) GetByID(ctx context.Context, id, namespaceID uuid.UUID) (*model.ProceduralEntry, error) {
	query := selectProceduralColumns + ` FROM procedural_entries WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = selectProceduralColumns + ` FROM procedural_entries WHERE id = $1 AND namespace_id = $2 AND deleted_at IS NULL`
	}

	row := r.db.QueryRow(ctx, query, id.String(), namespaceID.String())
	return r.scanEntry(row)
}

// ListByNamespace returns all live entries owned by the given namespace,
// ordered by priority (DESC) then recency (created_at DESC). This is the
// canonical ordered fetch used by both the REST list and procedural_fetch.
func (r *ProceduralRepo) ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.ProceduralEntry, error) {
	query := selectProceduralColumns + ` FROM procedural_entries
		WHERE namespace_id = ? AND deleted_at IS NULL
		ORDER BY priority DESC, created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectProceduralColumns + ` FROM procedural_entries
			WHERE namespace_id = $1 AND deleted_at IS NULL
			ORDER BY priority DESC, created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("procedural list by namespace: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := []model.ProceduralEntry{}
	for rows.Next() {
		e, err := r.scanEntryFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("procedural list by namespace iteration: %w", err)
	}
	return result, nil
}

// Update updates an entry's mutable fields: content, title, category, tags,
// priority, enabled, metadata. Scoped by id + namespace_id so a caller can
// only mutate entries in its own namespace.
func (r *ProceduralRepo) Update(ctx context.Context, e *model.ProceduralEntry) error {
	now := time.Now().UTC().Format(time.RFC3339)

	if e.Metadata == nil {
		e.Metadata = json.RawMessage(`{}`)
	}
	if e.Tags == nil {
		e.Tags = []string{}
	}

	tagsVal := encodeStringArray(r.db.Backend(), e.Tags)
	enabledVal := EncodeBool(r.db.Backend(), e.Enabled)

	query := `UPDATE procedural_entries
		SET content = ?, title = ?, category = ?, tags = ?, priority = ?, enabled = ?, metadata = ?, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE procedural_entries
			SET content = $1, title = $2, category = $3, tags = $4, priority = $5, enabled = $6, metadata = $7, updated_at = $8
			WHERE id = $9 AND namespace_id = $10 AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query,
		e.Content, e.Title, e.Category, tagsVal, e.Priority, enabledVal,
		string(e.Metadata), now, e.ID.String(), e.NamespaceID.String(),
	)
	if err != nil {
		return fmt.Errorf("procedural update: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("procedural update rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return r.reload(ctx, e)
}

// Delete soft-deletes an entry by id + namespace_id. Returns sql.ErrNoRows if
// no live row matched.
func (r *ProceduralRepo) Delete(ctx context.Context, id, namespaceID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE procedural_entries SET deleted_at = ?, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE procedural_entries SET deleted_at = $1, updated_at = $2
			WHERE id = $3 AND namespace_id = $4 AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query, now, now, id.String(), namespaceID.String())
	if err != nil {
		return fmt.Errorf("procedural delete: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("procedural delete rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// reload fetches the entry by ID and populates the struct in place.
func (r *ProceduralRepo) reload(ctx context.Context, e *model.ProceduralEntry) error {
	fetched, err := r.GetByID(ctx, e.ID, e.NamespaceID)
	if err != nil {
		return fmt.Errorf("procedural reload: %w", err)
	}
	*e = *fetched
	return nil
}

// scanEntry scans a single row into a model.ProceduralEntry.
func (r *ProceduralRepo) scanEntry(row *sql.Row) (*model.ProceduralEntry, error) {
	var e model.ProceduralEntry
	var idStr, namespaceIDStr, tagsStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var deletedAtStr sql.NullString
	var enabledRaw any

	err := row.Scan(
		&idStr, &namespaceIDStr, &e.Content, &e.Title, &e.Category,
		&tagsStr, &e.Priority, &enabledRaw, &e.Origin, &metadataStr,
		&createdAtStr, &updatedAtStr, &deletedAtStr,
	)
	if err != nil {
		return nil, err
	}
	return r.populateEntry(&e, idStr, namespaceIDStr, tagsStr, metadataStr,
		enabledRaw, createdAtStr, updatedAtStr, deletedAtStr)
}

// scanEntryFromRows scans the current row from sql.Rows into a model.ProceduralEntry.
func (r *ProceduralRepo) scanEntryFromRows(rows *sql.Rows) (*model.ProceduralEntry, error) {
	var e model.ProceduralEntry
	var idStr, namespaceIDStr, tagsStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var deletedAtStr sql.NullString
	var enabledRaw any

	err := rows.Scan(
		&idStr, &namespaceIDStr, &e.Content, &e.Title, &e.Category,
		&tagsStr, &e.Priority, &enabledRaw, &e.Origin, &metadataStr,
		&createdAtStr, &updatedAtStr, &deletedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("procedural scan rows: %w", err)
	}
	return r.populateEntry(&e, idStr, namespaceIDStr, tagsStr, metadataStr,
		enabledRaw, createdAtStr, updatedAtStr, deletedAtStr)
}

// populateEntry parses raw scan values into a model.ProceduralEntry. enabledRaw
// is bool on Postgres and int64 on SQLite, normalized via decodeBoolVal.
func (r *ProceduralRepo) populateEntry(
	e *model.ProceduralEntry,
	idStr, namespaceIDStr, tagsStr, metadataStr string,
	enabledRaw any,
	createdAtStr, updatedAtStr string,
	deletedAtStr sql.NullString,
) (*model.ProceduralEntry, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("procedural parse id: %w", err)
	}
	e.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("procedural parse namespace_id: %w", err)
	}
	e.NamespaceID = nsID

	tags, err := decodeStringArray(r.db.Backend(), tagsStr)
	if err != nil {
		return nil, fmt.Errorf("procedural parse tags: %w", err)
	}
	if tags == nil {
		tags = []string{}
	}
	e.Tags = tags

	e.Enabled = decodeBoolVal(enabledRaw)
	e.Metadata = json.RawMessage(metadataStr)

	e.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("procedural parse created_at: %w", err)
	}
	e.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("procedural parse updated_at: %w", err)
	}
	if deletedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, deletedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("procedural parse deleted_at: %w", err)
		}
		e.DeletedAt = &t
	}

	return e, nil
}
