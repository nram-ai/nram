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
// for the same (namespace_id, canonical). If the real type does NOT already
// exist, the stub is promoted in place (type updated). If the real type DOES
// already exist, the stub's relationships and aliases are reassigned to the
// real entity and the stub is deleted.
func (r *EntityRepo) promoteStub(ctx context.Context, entity *model.Entity) error {
	// Find the stub.
	stubQuery := selectEntityColumns + ` FROM entities
		WHERE namespace_id = ? AND canonical = ? AND entity_type = 'unknown'`
	if r.db.Backend() == BackendPostgres {
		stubQuery = selectEntityColumns + ` FROM entities
			WHERE namespace_id = $1 AND canonical = $2 AND entity_type = 'unknown'`
	}
	row := r.db.QueryRow(ctx, stubQuery, entity.NamespaceID.String(), entity.Canonical)
	stub, err := r.scanEntity(row)
	if err != nil {
		// No stub exists — nothing to do.
		return nil
	}

	// Check whether the real-typed entity already exists.
	realQuery := selectEntityColumns + ` FROM entities
		WHERE namespace_id = ? AND canonical = ? AND entity_type = ?`
	if r.db.Backend() == BackendPostgres {
		realQuery = selectEntityColumns + ` FROM entities
			WHERE namespace_id = $1 AND canonical = $2 AND entity_type = $3`
	}
	row = r.db.QueryRow(ctx, realQuery, entity.NamespaceID.String(), entity.Canonical, entity.EntityType)
	real, realErr := r.scanEntity(row)

	if realErr != nil {
		// Real entity doesn't exist — promote the stub in place.
		now := time.Now().UTC().Format(time.RFC3339)
		updateQuery := `UPDATE entities SET entity_type = ?, name = ?, updated_at = ?
			WHERE id = ?`
		if r.db.Backend() == BackendPostgres {
			updateQuery = `UPDATE entities SET entity_type = $1, name = $2, updated_at = $3
				WHERE id = $4`
		}
		_, err := r.db.Exec(ctx, updateQuery,
			entity.EntityType, entity.Name, now, stub.ID.String(),
		)
		if err != nil {
			return fmt.Errorf("entity promote stub: %w", err)
		}
		return nil
	}

	// Both stub and real entity exist — merge stub into the real entity.
	stubID := stub.ID.String()
	realID := real.ID.String()

	if err := r.mergeRelationshipsByEndpoint(ctx, "source_id", stubID, realID); err != nil {
		return fmt.Errorf("entity promote stub: reassign source relationships: %w", err)
	}
	if err := r.mergeRelationshipsByEndpoint(ctx, "target_id", stubID, realID); err != nil {
		return fmt.Errorf("entity promote stub: reassign target relationships: %w", err)
	}
	if err := r.mergeAliasesToEntity(ctx, stubID, realID); err != nil {
		return fmt.Errorf("entity promote stub: reassign aliases: %w", err)
	}

	// Delete the stub.
	delQuery := `DELETE FROM entities WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		delQuery = `DELETE FROM entities WHERE id = $1`
	}
	if _, err := r.db.Exec(ctx, delQuery, stubID); err != nil {
		return fmt.Errorf("entity promote stub: delete stub: %w", err)
	}

	// Bump mention count on the real entity.
	real.MentionCount += stub.MentionCount
	now := time.Now().UTC().Format(time.RFC3339)
	countQuery := `UPDATE entities SET mention_count = ?, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		countQuery = `UPDATE entities SET mention_count = $1, updated_at = $2 WHERE id = $3`
	}
	if _, err := r.db.Exec(ctx, countQuery, real.MentionCount, now, realID); err != nil {
		return fmt.Errorf("entity promote stub: update mention count: %w", err)
	}

	return nil
}

// UpdateEmbeddingDimBatch sets embedding_dim on every id in the same UPDATE,
// grouped by dim. The enrichment worker amortizes per-job entity writes this
// way: K entities at one dim become one round-trip instead of K. Empty input
// is a no-op.
func (r *EntityRepo) UpdateEmbeddingDimBatch(ctx context.Context, ids []uuid.UUID, dim int) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)

	args := make([]interface{}, 0, len(ids)+2)
	args = append(args, dim, now)
	placeholders := make([]string, len(ids))
	if r.db.Backend() == BackendPostgres {
		for i, id := range ids {
			placeholders[i] = fmt.Sprintf("$%d", i+3)
			args = append(args, id.String())
		}
	} else {
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id.String())
		}
	}

	query := `UPDATE entities SET embedding_dim = ?, updated_at = ? WHERE id IN (` + strings.Join(placeholders, ",") + `)`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE entities SET embedding_dim = $1, updated_at = $2 WHERE id IN (` + strings.Join(placeholders, ",") + `)`
	}
	if _, err := r.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("entity update embedding_dim batch: %w", err)
	}
	return nil
}

// ClearAllEmbeddingDims sets embedding_dim = NULL for every row in the
// entities table. Used by the embedding-model switch cascade so the
// re-embed pipeline treats every entity as needing fresh vectors. Returns
// the count of rows affected.
func (r *EntityRepo) ClearAllEmbeddingDims(ctx context.Context) (int64, error) {
	query := `UPDATE entities SET embedding_dim = NULL WHERE embedding_dim IS NOT NULL`
	res, err := r.db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("entity clear all embedding_dim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("entity clear all embedding_dim: rows affected: %w", err)
	}
	return n, nil
}

// UpdateEmbeddingDim sets the entity's embedding_dim column without bumping
// mention_count or otherwise re-running the Upsert merge logic. Used by the
// enrichment worker to record the dim that an entity vector was written at,
// so admin queries that filter `WHERE embedding_dim IS NOT NULL` see entities
// whose vectors actually exist in entity_vectors_<dim>.
func (r *EntityRepo) UpdateEmbeddingDim(ctx context.Context, id uuid.UUID, dim int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE entities SET embedding_dim = ?, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE entities SET embedding_dim = $1, updated_at = $2 WHERE id = $3`
	}
	if _, err := r.db.Exec(ctx, query, dim, now, id.String()); err != nil {
		return fmt.Errorf("entity update embedding_dim: %w", err)
	}
	return nil
}

// mergeRelationshipsByEndpoint repoints stub-owned relationships at one
// endpoint (source_id or target_id) to real. Where the repoint would
// collide with an existing real-owned relationship on the UNIQUE
// (namespace_id, source_id, target_id, relation, valid_from) key, the
// real row's weight is pulled up to max(real, stub) and the stub row is
// deleted. Remaining stub rows are reassigned via UPDATE.
func (r *EntityRepo) mergeRelationshipsByEndpoint(ctx context.Context, endpoint, stubID, realID string) error {
	if endpoint != "source_id" && endpoint != "target_id" {
		return fmt.Errorf("mergeRelationshipsByEndpoint: invalid endpoint %q", endpoint)
	}
	// Sibling endpoint fills out the UNIQUE-key match: when reassigning
	// source_id we're looking for rows that agree on target_id/relation/
	// valid_from with a row already owned by real, and vice versa.
	sibling := "target_id"
	if endpoint == "target_id" {
		sibling = "source_id"
	}

	// Step 1: bring max(weight) onto the real row for each (sibling, relation,
	// valid_from) triple that both stub and real already hold.
	var mergeQuery string
	if r.db.Backend() == BackendPostgres {
		mergeQuery = fmt.Sprintf(`UPDATE relationships realrel
			SET weight = GREATEST(realrel.weight, stubrel.weight)
			FROM relationships stubrel
			WHERE realrel.%[1]s = $1
			  AND stubrel.%[1]s = $2
			  AND realrel.namespace_id = stubrel.namespace_id
			  AND realrel.%[2]s = stubrel.%[2]s
			  AND realrel.relation = stubrel.relation
			  AND realrel.valid_from = stubrel.valid_from`, endpoint, sibling)
	} else {
		mergeQuery = fmt.Sprintf(`UPDATE relationships
			SET weight = MAX(weight, (
				SELECT s.weight FROM relationships s
				WHERE s.%[1]s = ?
				  AND s.namespace_id = relationships.namespace_id
				  AND s.%[2]s = relationships.%[2]s
				  AND s.relation = relationships.relation
				  AND s.valid_from = relationships.valid_from
			))
			WHERE %[1]s = ?
			  AND EXISTS (
				SELECT 1 FROM relationships s
				WHERE s.%[1]s = ?
				  AND s.namespace_id = relationships.namespace_id
				  AND s.%[2]s = relationships.%[2]s
				  AND s.relation = relationships.relation
				  AND s.valid_from = relationships.valid_from
			  )`, endpoint, sibling)
	}
	if r.db.Backend() == BackendPostgres {
		if _, err := r.db.Exec(ctx, mergeQuery, realID, stubID); err != nil {
			return fmt.Errorf("merge weights: %w", err)
		}
	} else {
		if _, err := r.db.Exec(ctx, mergeQuery, stubID, realID, stubID); err != nil {
			return fmt.Errorf("merge weights: %w", err)
		}
	}

	// Step 2: delete the now-redundant stub rows.
	var deleteQuery string
	if r.db.Backend() == BackendPostgres {
		deleteQuery = fmt.Sprintf(`DELETE FROM relationships stubrel
			USING relationships realrel
			WHERE stubrel.%[1]s = $1
			  AND realrel.%[1]s = $2
			  AND realrel.namespace_id = stubrel.namespace_id
			  AND realrel.%[2]s = stubrel.%[2]s
			  AND realrel.relation = stubrel.relation
			  AND realrel.valid_from = stubrel.valid_from`, endpoint, sibling)
	} else {
		deleteQuery = fmt.Sprintf(`DELETE FROM relationships
			WHERE %[1]s = ?
			  AND EXISTS (
				SELECT 1 FROM relationships r
				WHERE r.%[1]s = ?
				  AND r.namespace_id = relationships.namespace_id
				  AND r.%[2]s = relationships.%[2]s
				  AND r.relation = relationships.relation
				  AND r.valid_from = relationships.valid_from
			  )`, endpoint, sibling)
	}
	if r.db.Backend() == BackendPostgres {
		if _, err := r.db.Exec(ctx, deleteQuery, stubID, realID); err != nil {
			return fmt.Errorf("delete conflicting stub rows: %w", err)
		}
	} else {
		if _, err := r.db.Exec(ctx, deleteQuery, stubID, realID); err != nil {
			return fmt.Errorf("delete conflicting stub rows: %w", err)
		}
	}

	// Step 3: reassign the remaining stub rows to real — no conflicts possible now.
	reassignQuery := fmt.Sprintf(`UPDATE relationships SET %s = ? WHERE %s = ?`, endpoint, endpoint)
	if r.db.Backend() == BackendPostgres {
		reassignQuery = fmt.Sprintf(`UPDATE relationships SET %s = $1 WHERE %s = $2`, endpoint, endpoint)
	}
	if _, err := r.db.Exec(ctx, reassignQuery, realID, stubID); err != nil {
		return fmt.Errorf("reassign remaining stub rows: %w", err)
	}
	return nil
}

// mergeAliasesToEntity repoints stub-owned aliases to real, dropping any that
// would duplicate an alias already registered against real.
func (r *EntityRepo) mergeAliasesToEntity(ctx context.Context, stubID, realID string) error {
	// Delete conflicting stub aliases.
	delQuery := `DELETE FROM entity_aliases
		WHERE entity_id = ?
		  AND alias IN (SELECT alias FROM entity_aliases WHERE entity_id = ?)`
	if r.db.Backend() == BackendPostgres {
		delQuery = `DELETE FROM entity_aliases
			WHERE entity_id = $1
			  AND alias IN (SELECT alias FROM entity_aliases WHERE entity_id = $2)`
	}
	if _, err := r.db.Exec(ctx, delQuery, stubID, realID); err != nil {
		return fmt.Errorf("delete conflicting stub aliases: %w", err)
	}

	// Reassign the rest.
	reassignQuery := `UPDATE entity_aliases SET entity_id = ? WHERE entity_id = ?`
	if r.db.Backend() == BackendPostgres {
		reassignQuery = `UPDATE entity_aliases SET entity_id = $1 WHERE entity_id = $2`
	}
	if _, err := r.db.Exec(ctx, reassignQuery, realID, stubID); err != nil {
		return fmt.Errorf("reassign remaining stub aliases: %w", err)
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

// ListAll returns a page of entities across every namespace, ordered by id
// for stable pagination. Used by maintenance tooling that needs to walk the
// entire entities table (re-embed, schema migrations). Pass limit=0 to use
// a sensible default page size (500).
func (r *EntityRepo) ListAll(ctx context.Context, limit, offset int) ([]model.Entity, error) {
	if limit <= 0 {
		limit = 500
	}
	query := selectEntityColumns + ` FROM entities
		ORDER BY id
		LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityColumns + ` FROM entities
			ORDER BY id
			LIMIT $1 OFFSET $2`
	}
	rows, err := r.db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("entity list all: %w", err)
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
