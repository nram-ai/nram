package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// MemoryRepo provides CRUD operations for the memories table.
type MemoryRepo struct {
	db DB
}

// NewMemoryRepo creates a new MemoryRepo backed by the given DB.
func NewMemoryRepo(db DB) *MemoryRepo {
	return &MemoryRepo{db: db}
}

// Create inserts a new memory. ID is generated if zero-valued.
// Tags defaults to `[]` if nil. Metadata defaults to `{}` if nil.
func (r *MemoryRepo) Create(ctx context.Context, mem *model.Memory) error {
	if mem.ID == uuid.Nil {
		mem.ID = uuid.New()
	}
	if mem.Tags == nil {
		mem.Tags = []string{}
	}
	if mem.Metadata == nil {
		mem.Metadata = json.RawMessage(`{}`)
	}

	tagsVal := encodeStringArray(r.db.Backend(), mem.Tags)

	var source interface{}
	if mem.Source != nil {
		source = *mem.Source
	}

	var embeddingDim interface{}
	if mem.EmbeddingDim != nil {
		embeddingDim = *mem.EmbeddingDim
	}

	var lastAccessed interface{}
	if mem.LastAccessed != nil {
		lastAccessed = mem.LastAccessed.UTC().Format(time.RFC3339)
	}

	var expiresAt interface{}
	if mem.ExpiresAt != nil {
		expiresAt = mem.ExpiresAt.UTC().Format(time.RFC3339)
	}

	var supersededBy interface{}
	if mem.SupersededBy != nil {
		supersededBy = mem.SupersededBy.String()
	}

	var purgeAfter interface{}
	if mem.PurgeAfter != nil {
		purgeAfter = mem.PurgeAfter.UTC().Format(time.RFC3339)
	}

	enrichedVal := encodeBool(r.db.Backend(), mem.Enriched)

	query := `INSERT INTO memories (id, namespace_id, content, embedding_dim, source, tags,
		confidence, importance, access_count, last_accessed, expires_at, superseded_by,
		enriched, metadata, purge_after)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO memories (id, namespace_id, content, embedding_dim, source, tags,
			confidence, importance, access_count, last_accessed, expires_at, superseded_by,
			enriched, metadata, purge_after)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
	}

	_, err := r.db.Exec(ctx, query,
		mem.ID.String(), mem.NamespaceID.String(), mem.Content, embeddingDim, source,
		tagsVal, mem.Confidence, mem.Importance, mem.AccessCount,
		lastAccessed, expiresAt, supersededBy, enrichedVal, string(mem.Metadata), purgeAfter,
	)
	if err != nil {
		return fmt.Errorf("memory create: %w", err)
	}

	return r.reload(ctx, mem)
}

// GetByID returns a memory by its UUID. Soft-deleted records are excluded.
func (r *MemoryRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error) {
	query := selectMemoryColumns + ` FROM memories WHERE id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories WHERE id = $1 AND deleted_at IS NULL`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanMemory(row)
}

// getByIDIncludeDeleted returns a memory by its UUID including soft-deleted records.
// Used internally for reload after create.
func (r *MemoryRepo) getByIDIncludeDeleted(ctx context.Context, id uuid.UUID) (*model.Memory, error) {
	query := selectMemoryColumns + ` FROM memories WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanMemory(row)
}

// GetBatch returns multiple memories by their UUIDs. Soft-deleted records are excluded.
func (r *MemoryRepo) GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	if len(ids) == 0 {
		return []model.Memory{}, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		if r.db.Backend() == BackendPostgres {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
		args[i] = id.String()
	}

	query := selectMemoryColumns + ` FROM memories WHERE id IN (` +
		strings.Join(placeholders, ", ") + `) AND deleted_at IS NULL`

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory get batch: %w", err)
	}
	defer rows.Close()

	result := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory get batch iteration: %w", err)
	}
	return result, nil
}

// ListByNamespace returns memories in a namespace, paginated, ordered by created_at DESC.
// Soft-deleted records are excluded.
func (r *MemoryRepo) ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error) {
	query := selectMemoryColumns + ` FROM memories
		WHERE namespace_id = ? AND deleted_at IS NULL
		ORDER BY created_at DESC LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories
			WHERE namespace_id = $1 AND deleted_at IS NULL
			ORDER BY created_at DESC LIMIT $2 OFFSET $3`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("memory list by namespace: %w", err)
	}
	defer rows.Close()

	result := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory list by namespace iteration: %w", err)
	}
	return result, nil
}

// CountByNamespace returns the total number of non-deleted memories in a namespace.
func (r *MemoryRepo) CountByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM memories WHERE namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM memories WHERE namespace_id = $1 AND deleted_at IS NULL`
	}
	row := r.db.QueryRow(ctx, query, namespaceID.String())
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("memory count by namespace: %w", err)
	}
	return count, nil
}

// Update updates all mutable fields of a memory and bumps updated_at.
func (r *MemoryRepo) Update(ctx context.Context, mem *model.Memory) error {
	now := time.Now().UTC().Format(time.RFC3339)

	if mem.Tags == nil {
		mem.Tags = []string{}
	}
	if mem.Metadata == nil {
		mem.Metadata = json.RawMessage(`{}`)
	}

	tagsVal := encodeStringArray(r.db.Backend(), mem.Tags)

	var source interface{}
	if mem.Source != nil {
		source = *mem.Source
	}

	var embeddingDim interface{}
	if mem.EmbeddingDim != nil {
		embeddingDim = *mem.EmbeddingDim
	}

	var lastAccessed interface{}
	if mem.LastAccessed != nil {
		lastAccessed = mem.LastAccessed.UTC().Format(time.RFC3339)
	}

	var expiresAt interface{}
	if mem.ExpiresAt != nil {
		expiresAt = mem.ExpiresAt.UTC().Format(time.RFC3339)
	}

	var supersededBy interface{}
	if mem.SupersededBy != nil {
		supersededBy = mem.SupersededBy.String()
	}

	var purgeAfter interface{}
	if mem.PurgeAfter != nil {
		purgeAfter = mem.PurgeAfter.UTC().Format(time.RFC3339)
	}

	enrichedVal := encodeBool(r.db.Backend(), mem.Enriched)

	query := `UPDATE memories SET content = ?, embedding_dim = ?, source = ?, tags = ?,
		confidence = ?, importance = ?, access_count = ?, last_accessed = ?,
		expires_at = ?, superseded_by = ?, enriched = ?, metadata = ?,
		purge_after = ?, updated_at = ?
		WHERE id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET content = $1, embedding_dim = $2, source = $3, tags = $4,
			confidence = $5, importance = $6, access_count = $7, last_accessed = $8,
			expires_at = $9, superseded_by = $10, enriched = $11, metadata = $12,
			purge_after = $13, updated_at = $14
			WHERE id = $15 AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query,
		mem.Content, embeddingDim, source, tagsVal,
		mem.Confidence, mem.Importance, mem.AccessCount, lastAccessed,
		expiresAt, supersededBy, enrichedVal, string(mem.Metadata),
		purgeAfter, now, mem.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("memory update: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("memory update rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return r.reload(ctx, mem)
}

// SoftDelete sets the deleted_at timestamp on a memory.
func (r *MemoryRepo) SoftDelete(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE memories SET deleted_at = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET deleted_at = $1, updated_at = $2 WHERE id = $3 AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query, now, now, id.String())
	if err != nil {
		return fmt.Errorf("memory soft delete: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("memory soft delete rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// HardDelete permanently removes a memory from the table.
func (r *MemoryRepo) HardDelete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM memories WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM memories WHERE id = $1`
	}

	_, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("memory hard delete: %w", err)
	}
	return nil
}

// ListExpired returns memories whose expires_at is before the given time and are not yet soft-deleted.
func (r *MemoryRepo) ListExpired(ctx context.Context, before time.Time, limit int) ([]model.Memory, error) {
	beforeStr := before.UTC().Format(time.RFC3339)

	query := selectMemoryColumns + ` FROM memories
		WHERE expires_at IS NOT NULL AND expires_at < ? AND deleted_at IS NULL
		ORDER BY expires_at ASC LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories
			WHERE expires_at IS NOT NULL AND expires_at < $1 AND deleted_at IS NULL
			ORDER BY expires_at ASC LIMIT $2`
	}

	rows, err := r.db.Query(ctx, query, beforeStr, limit)
	if err != nil {
		return nil, fmt.Errorf("memory list expired: %w", err)
	}
	defer rows.Close()

	result := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory list expired iteration: %w", err)
	}
	return result, nil
}

// ListPurgeable returns soft-deleted memories whose deleted_at is before the given time,
// making them eligible for hard deletion.
func (r *MemoryRepo) ListPurgeable(ctx context.Context, before time.Time, limit int) ([]model.Memory, error) {
	beforeStr := before.UTC().Format(time.RFC3339)

	query := selectMemoryColumns + ` FROM memories
		WHERE deleted_at IS NOT NULL AND deleted_at < ?
		ORDER BY deleted_at ASC LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories
			WHERE deleted_at IS NOT NULL AND deleted_at < $1
			ORDER BY deleted_at ASC LIMIT $2`
	}

	rows, err := r.db.Query(ctx, query, beforeStr, limit)
	if err != nil {
		return nil, fmt.Errorf("memory list purgeable: %w", err)
	}
	defer rows.Close()

	result := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory list purgeable iteration: %w", err)
	}
	return result, nil
}

// reload fetches the memory by ID and populates the struct in place.
func (r *MemoryRepo) reload(ctx context.Context, mem *model.Memory) error {
	fetched, err := r.getByIDIncludeDeleted(ctx, mem.ID)
	if err != nil {
		return fmt.Errorf("memory reload: %w", err)
	}
	*mem = *fetched
	return nil
}

const selectMemoryColumns = `SELECT id, namespace_id, content, embedding_dim, source, tags,
	confidence, importance, access_count, last_accessed, expires_at, superseded_by,
	enriched, metadata, created_at, updated_at, deleted_at, purge_after`

func (r *MemoryRepo) scanMemory(row *sql.Row) (*model.Memory, error) {
	var mem model.Memory
	var idStr, namespaceIDStr string
	var tagsStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var embeddingDim sql.NullInt64
	var source sql.NullString
	var lastAccessedStr, expiresAtStr, deletedAtStr, purgeAfterStr sql.NullString
	var supersededByStr sql.NullString
	var enrichedBool bool

	err := row.Scan(
		&idStr, &namespaceIDStr, &mem.Content, &embeddingDim, &source, &tagsStr,
		&mem.Confidence, &mem.Importance, &mem.AccessCount, &lastAccessedStr,
		&expiresAtStr, &supersededByStr, &enrichedBool, &metadataStr,
		&createdAtStr, &updatedAtStr, &deletedAtStr, &purgeAfterStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateMemory(&mem, idStr, namespaceIDStr, tagsStr, metadataStr,
		createdAtStr, updatedAtStr, embeddingDim, source, lastAccessedStr,
		expiresAtStr, supersededByStr, enrichedBool, deletedAtStr, purgeAfterStr)
}

func (r *MemoryRepo) scanMemoryFromRows(rows *sql.Rows) (*model.Memory, error) {
	var mem model.Memory
	var idStr, namespaceIDStr string
	var tagsStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var embeddingDim sql.NullInt64
	var source sql.NullString
	var lastAccessedStr, expiresAtStr, deletedAtStr, purgeAfterStr sql.NullString
	var supersededByStr sql.NullString
	var enrichedBool bool

	err := rows.Scan(
		&idStr, &namespaceIDStr, &mem.Content, &embeddingDim, &source, &tagsStr,
		&mem.Confidence, &mem.Importance, &mem.AccessCount, &lastAccessedStr,
		&expiresAtStr, &supersededByStr, &enrichedBool, &metadataStr,
		&createdAtStr, &updatedAtStr, &deletedAtStr, &purgeAfterStr,
	)
	if err != nil {
		return nil, fmt.Errorf("memory scan rows: %w", err)
	}

	return r.populateMemory(&mem, idStr, namespaceIDStr, tagsStr, metadataStr,
		createdAtStr, updatedAtStr, embeddingDim, source, lastAccessedStr,
		expiresAtStr, supersededByStr, enrichedBool, deletedAtStr, purgeAfterStr)
}

func (r *MemoryRepo) populateMemory(
	mem *model.Memory,
	idStr, namespaceIDStr, tagsStr, metadataStr, createdAtStr, updatedAtStr string,
	embeddingDim sql.NullInt64,
	source, lastAccessedStr, expiresAtStr, supersededByStr sql.NullString,
	enrichedBool bool,
	deletedAtStr, purgeAfterStr sql.NullString,
) (*model.Memory, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("memory parse id: %w", err)
	}
	mem.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("memory parse namespace_id: %w", err)
	}
	mem.NamespaceID = nsID

	tags, err := decodeStringArray(r.db.Backend(), tagsStr)
	if err != nil {
		return nil, fmt.Errorf("memory parse tags: %w", err)
	}
	if tags == nil {
		tags = []string{}
	}
	mem.Tags = tags

	if metadataStr == "" || metadataStr == "null" {
		metadataStr = "{}"
	}
	mem.Metadata = json.RawMessage(metadataStr)
	mem.Enriched = enrichedBool

	if embeddingDim.Valid {
		dim := int(embeddingDim.Int64)
		mem.EmbeddingDim = &dim
	}

	if source.Valid {
		mem.Source = &source.String
	}

	mem.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("memory parse created_at: %w", err)
	}
	mem.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("memory parse updated_at: %w", err)
	}

	if lastAccessedStr.Valid {
		t, err := time.Parse(time.RFC3339, lastAccessedStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse last_accessed: %w", err)
		}
		mem.LastAccessed = &t
	}

	if expiresAtStr.Valid {
		t, err := time.Parse(time.RFC3339, expiresAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse expires_at: %w", err)
		}
		mem.ExpiresAt = &t
	}

	if supersededByStr.Valid {
		u, err := uuid.Parse(supersededByStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse superseded_by: %w", err)
		}
		mem.SupersededBy = &u
	}

	if deletedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, deletedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse deleted_at: %w", err)
		}
		mem.DeletedAt = &t
	}

	if purgeAfterStr.Valid {
		t, err := time.Parse(time.RFC3339, purgeAfterStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse purge_after: %w", err)
		}
		mem.PurgeAfter = &t
	}

	return mem, nil
}
