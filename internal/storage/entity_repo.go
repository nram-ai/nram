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

// EntityRepo provides CRUD operations for the entities table.
type EntityRepo struct {
	db DB
}

// NewEntityRepo creates a new EntityRepo backed by the given DB.
func NewEntityRepo(db DB) *EntityRepo {
	return &EntityRepo{db: db}
}

// Create inserts a new entity. ID is generated if zero-valued.
// Properties defaults to `{}` if nil. Metadata defaults to `{}` if nil.
func (r *EntityRepo) Create(ctx context.Context, entity *model.Entity) error {
	if entity.ID == uuid.Nil {
		entity.ID = uuid.New()
	}
	if entity.Properties == nil {
		entity.Properties = json.RawMessage(`{}`)
	}
	if entity.Metadata == nil {
		entity.Metadata = json.RawMessage(`{}`)
	}

	var embeddingDim interface{}
	if entity.EmbeddingDim != nil {
		embeddingDim = *entity.EmbeddingDim
	}

	query := `INSERT INTO entities (id, namespace_id, name, canonical, entity_type, embedding_dim, properties, mention_count, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO entities (id, namespace_id, name, canonical, entity_type, embedding_dim, properties, mention_count, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	}

	_, err := r.db.Exec(ctx, query,
		entity.ID.String(), entity.NamespaceID.String(), entity.Name, entity.Canonical,
		entity.EntityType, embeddingDim, string(entity.Properties),
		entity.MentionCount, string(entity.Metadata),
	)
	if err != nil {
		return fmt.Errorf("entity create: %w", err)
	}

	return r.reload(ctx, entity)
}

// GetByID returns an entity by its UUID.
func (r *EntityRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Entity, error) {
	query := selectEntityColumns + ` FROM entities WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityColumns + ` FROM entities WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanEntity(row)
}

// Upsert performs canonical dedup: if an entity with the same namespace_id,
// entity_type, and canonical name exists, it updates that entity instead of
// creating a new one. The updated fields are: name, properties, mention_count,
// metadata, embedding_dim, and updated_at.
func (r *EntityRepo) Upsert(ctx context.Context, entity *model.Entity) error {
	if entity.ID == uuid.Nil {
		entity.ID = uuid.New()
	}
	if entity.Properties == nil {
		entity.Properties = json.RawMessage(`{}`)
	}
	if entity.Metadata == nil {
		entity.Metadata = json.RawMessage(`{}`)
	}

	// Promote stub entities: if a "unknown"-typed stub exists for the same
	// (namespace_id, canonical) and we now have a real type, update the stub's
	// type in place so its ID (and any relationships attached to it) are kept.
	if entity.EntityType != "unknown" {
		if err := r.promoteStub(ctx, entity); err != nil {
			return err
		}
	}

	var embeddingDim interface{}
	if entity.EmbeddingDim != nil {
		embeddingDim = *entity.EmbeddingDim
	}

	now := time.Now().UTC().Format(time.RFC3339)

	query := `INSERT INTO entities (id, namespace_id, name, canonical, entity_type, embedding_dim, properties, mention_count, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(namespace_id, canonical, entity_type) DO UPDATE SET
			name = excluded.name,
			embedding_dim = excluded.embedding_dim,
			properties = excluded.properties,
			mention_count = mention_count + 1,
			metadata = excluded.metadata,
			updated_at = ?`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO entities (id, namespace_id, name, canonical, entity_type, embedding_dim, properties, mention_count, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT(namespace_id, canonical, entity_type) DO UPDATE SET
				name = EXCLUDED.name,
				embedding_dim = EXCLUDED.embedding_dim,
				properties = EXCLUDED.properties,
				mention_count = entities.mention_count + 1,
				metadata = EXCLUDED.metadata,
				updated_at = $10`
	}

	_, err := r.db.Exec(ctx, query,
		entity.ID.String(), entity.NamespaceID.String(), entity.Name, entity.Canonical,
		entity.EntityType, embeddingDim, string(entity.Properties),
		entity.MentionCount, string(entity.Metadata), now,
	)
	if err != nil {
		return fmt.Errorf("entity upsert: %w", err)
	}

	// Reload to get the actual row (may have existing ID if conflict).
	return r.reloadByCanonical(ctx, entity)
}

// promoteStub checks whether a stub entity (entity_type = 'unknown') exists
// for the same (namespace_id, canonical). If so, it updates the stub's type
// in place so the original ID and all attached relationships are preserved.
func (r *EntityRepo) promoteStub(ctx context.Context, entity *model.Entity) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE entities SET entity_type = ?, name = ?, updated_at = ?
		WHERE namespace_id = ? AND canonical = ? AND entity_type = 'unknown'`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE entities SET entity_type = $1, name = $2, updated_at = $3
			WHERE namespace_id = $4 AND canonical = $5 AND entity_type = 'unknown'`
	}

	_, err := r.db.Exec(ctx, query,
		entity.EntityType, entity.Name, now,
		entity.NamespaceID.String(), entity.Canonical,
	)
	if err != nil {
		return fmt.Errorf("entity promote stub: %w", err)
	}
	return nil
}

// FindBySimilarity finds entities with similar names in the same namespace.
// If kind is non-empty, results are filtered to that entity type.
// Uses case-insensitive LIKE matching with the name.
func (r *EntityRepo) FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error) {
	pattern := "%" + name + "%"

	var query string
	var args []any

	if kind != "" {
		query = selectEntityColumns + ` FROM entities
			WHERE namespace_id = ? AND entity_type = ? AND name LIKE ? COLLATE NOCASE
			ORDER BY mention_count DESC, created_at DESC LIMIT ?`
		if r.db.Backend() == BackendPostgres {
			query = selectEntityColumns + ` FROM entities
				WHERE namespace_id = $1 AND entity_type = $2 AND name ILIKE $3
				ORDER BY mention_count DESC, created_at DESC LIMIT $4`
		}
		args = []any{namespaceID.String(), kind, pattern, limit}
	} else {
		query = selectEntityColumns + ` FROM entities
			WHERE namespace_id = ? AND name LIKE ? COLLATE NOCASE
			ORDER BY mention_count DESC, created_at DESC LIMIT ?`
		if r.db.Backend() == BackendPostgres {
			query = selectEntityColumns + ` FROM entities
				WHERE namespace_id = $1 AND name ILIKE $2
				ORDER BY mention_count DESC, created_at DESC LIMIT $3`
		}
		args = []any{namespaceID.String(), pattern, limit}
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("entity find by similarity: %w", err)
	}
	defer rows.Close()

	return r.scanEntities(rows)
}

// FindByAlias finds entities that have a matching alias in the entity_aliases
// table. Uses case-insensitive matching.
func (r *EntityRepo) FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.Entity, error) {
	query := selectEntityColumnsAliased + ` FROM entities e
		INNER JOIN entity_aliases ea ON e.id = ea.entity_id
		WHERE e.namespace_id = ? AND ea.alias = ? COLLATE NOCASE`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityColumnsAliased + ` FROM entities e
			INNER JOIN entity_aliases ea ON e.id = ea.entity_id
			WHERE e.namespace_id = $1 AND LOWER(ea.alias) = LOWER($2)`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), alias)
	if err != nil {
		return nil, fmt.Errorf("entity find by alias: %w", err)
	}
	defer rows.Close()

	return r.scanEntities(rows)
}

// ListByNamespace returns all entities for a namespace, ordered by created_at DESC.
func (r *EntityRepo) ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error) {
	query := selectEntityColumns + ` FROM entities
		WHERE namespace_id = ?
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityColumns + ` FROM entities
			WHERE namespace_id = $1
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("entity list by namespace: %w", err)
	}
	defer rows.Close()

	return r.scanEntities(rows)
}

// DeleteByNamespace deletes all entities in a namespace. Entity aliases are removed first
// to satisfy foreign key constraints.
func (r *EntityRepo) DeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error {
	// Delete entity_aliases for entities in this namespace.
	aliasQuery := `DELETE FROM entity_aliases WHERE entity_id IN (SELECT id FROM entities WHERE namespace_id = ?)`
	if r.db.Backend() == BackendPostgres {
		aliasQuery = `DELETE FROM entity_aliases WHERE entity_id IN (SELECT id FROM entities WHERE namespace_id = $1)`
	}
	_, err := r.db.Exec(ctx, aliasQuery, namespaceID.String())
	if err != nil {
		return fmt.Errorf("entity delete aliases by namespace: %w", err)
	}

	// Delete entities.
	query := `DELETE FROM entities WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM entities WHERE namespace_id = $1`
	}
	_, err = r.db.Exec(ctx, query, namespaceID.String())
	if err != nil {
		return fmt.Errorf("entity delete by namespace: %w", err)
	}
	return nil
}

// DeleteOrphaned removes entities that have no relationships (neither as source nor target).
// Returns the number of entities deleted.
func (r *EntityRepo) DeleteOrphaned(ctx context.Context) (int64, error) {
	query := `DELETE FROM entities WHERE id NOT IN (
		SELECT source_id FROM relationships
		UNION
		SELECT target_id FROM relationships
	)`
	result, err := r.db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("entity delete orphaned: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("entity delete orphaned rows: %w", err)
	}
	return rows, nil
}

// reload fetches the entity by ID and populates the struct in place.
func (r *EntityRepo) reload(ctx context.Context, entity *model.Entity) error {
	fetched, err := r.GetByID(ctx, entity.ID)
	if err != nil {
		return fmt.Errorf("entity reload: %w", err)
	}
	*entity = *fetched
	return nil
}

// reloadByCanonical fetches the entity by its unique canonical key and
// populates the struct in place. Used after upsert where the ID may differ
// from the one we attempted to insert.
func (r *EntityRepo) reloadByCanonical(ctx context.Context, entity *model.Entity) error {
	query := selectEntityColumns + ` FROM entities
		WHERE namespace_id = ? AND canonical = ? AND entity_type = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityColumns + ` FROM entities
			WHERE namespace_id = $1 AND canonical = $2 AND entity_type = $3`
	}

	row := r.db.QueryRow(ctx, query,
		entity.NamespaceID.String(), entity.Canonical, entity.EntityType,
	)
	fetched, err := r.scanEntity(row)
	if err != nil {
		return fmt.Errorf("entity reload by canonical: %w", err)
	}
	*entity = *fetched
	return nil
}

const selectEntityColumns = `SELECT id, namespace_id, name, canonical, entity_type,
	embedding_dim, properties, mention_count, metadata, created_at, updated_at`

const selectEntityColumnsAliased = `SELECT e.id, e.namespace_id, e.name, e.canonical, e.entity_type,
	e.embedding_dim, e.properties, e.mention_count, e.metadata, e.created_at, e.updated_at`

func (r *EntityRepo) scanEntity(row *sql.Row) (*model.Entity, error) {
	var entity model.Entity
	var idStr, namespaceIDStr string
	var propertiesStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var embeddingDim sql.NullInt64

	err := row.Scan(
		&idStr, &namespaceIDStr, &entity.Name, &entity.Canonical, &entity.EntityType,
		&embeddingDim, &propertiesStr, &entity.MentionCount, &metadataStr,
		&createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateEntity(&entity, idStr, namespaceIDStr, propertiesStr,
		metadataStr, createdAtStr, updatedAtStr, embeddingDim)
}

func (r *EntityRepo) scanEntityFromRows(rows *sql.Rows) (*model.Entity, error) {
	var entity model.Entity
	var idStr, namespaceIDStr string
	var propertiesStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var embeddingDim sql.NullInt64

	err := rows.Scan(
		&idStr, &namespaceIDStr, &entity.Name, &entity.Canonical, &entity.EntityType,
		&embeddingDim, &propertiesStr, &entity.MentionCount, &metadataStr,
		&createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("entity scan rows: %w", err)
	}

	return r.populateEntity(&entity, idStr, namespaceIDStr, propertiesStr,
		metadataStr, createdAtStr, updatedAtStr, embeddingDim)
}

func (r *EntityRepo) scanEntities(rows *sql.Rows) ([]model.Entity, error) {
	result := []model.Entity{}
	for rows.Next() {
		entity, err := r.scanEntityFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *entity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("entity scan iteration: %w", err)
	}
	return result, nil
}

func (r *EntityRepo) populateEntity(
	entity *model.Entity,
	idStr, namespaceIDStr, propertiesStr, metadataStr, createdAtStr, updatedAtStr string,
	embeddingDim sql.NullInt64,
) (*model.Entity, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("entity parse id: %w", err)
	}
	entity.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("entity parse namespace_id: %w", err)
	}
	entity.NamespaceID = nsID

	entity.Properties = json.RawMessage(propertiesStr)
	entity.Metadata = json.RawMessage(metadataStr)

	if embeddingDim.Valid {
		dim := int(embeddingDim.Int64)
		entity.EmbeddingDim = &dim
	}

	entity.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("entity parse created_at: %w", err)
	}
	entity.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("entity parse updated_at: %w", err)
	}

	return entity, nil
}
