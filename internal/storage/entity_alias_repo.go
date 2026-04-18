package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// EntityAliasRepo provides CRUD operations for the entity_aliases table.
type EntityAliasRepo struct {
	db DB
}

// NewEntityAliasRepo creates a new EntityAliasRepo backed by the given DB.
func NewEntityAliasRepo(db DB) *EntityAliasRepo {
	return &EntityAliasRepo{db: db}
}

// Create registers an (entity_id, alias) mapping. Idempotent: duplicate
// inserts are absorbed via ON CONFLICT DO NOTHING since aliases are
// immutable pointers with no per-row state worth merging.
func (r *EntityAliasRepo) Create(ctx context.Context, alias *model.EntityAlias) error {
	if alias.ID == uuid.Nil {
		alias.ID = uuid.New()
	}
	if alias.CreatedAt.IsZero() {
		alias.CreatedAt = time.Now().UTC()
	}
	createdAtStr := alias.CreatedAt.UTC().Format(time.RFC3339)

	query := `INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(entity_id, alias) DO NOTHING`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT(entity_id, alias) DO NOTHING`
	}

	if _, err := r.db.Exec(ctx, query,
		alias.ID.String(), alias.NamespaceID.String(), alias.EntityID.String(),
		alias.Alias, alias.AliasType, createdAtStr,
	); err != nil {
		return fmt.Errorf("entity alias create: %w", err)
	}
	return nil
}

// FindByAlias finds entity aliases matching the alias text (case-insensitive),
// scoped to namespace via JOIN with entities.
func (r *EntityAliasRepo) FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.EntityAlias, error) {
	query := selectEntityAliasColumns + ` FROM entity_aliases ea
		INNER JOIN entities e ON ea.entity_id = e.id
		WHERE e.namespace_id = ? AND ea.namespace_id = ? AND ea.alias = ? COLLATE NOCASE`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityAliasColumns + ` FROM entity_aliases ea
			INNER JOIN entities e ON ea.entity_id = e.id
			WHERE e.namespace_id = $1 AND ea.namespace_id = $1 AND LOWER(ea.alias) = LOWER($2)`
	}

	var args []any
	if r.db.Backend() == BackendPostgres {
		args = []any{namespaceID.String(), alias}
	} else {
		args = []any{namespaceID.String(), namespaceID.String(), alias}
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("entity alias find by alias: %w", err)
	}
	defer rows.Close()

	return r.scanAliases(rows)
}

// ListByEntity returns all aliases for a given entity, ordered by created_at DESC.
func (r *EntityAliasRepo) ListByEntity(ctx context.Context, entityID uuid.UUID) ([]model.EntityAlias, error) {
	query := selectEntityAliasColumns + ` FROM entity_aliases ea
		WHERE ea.entity_id = ?
		ORDER BY ea.created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityAliasColumns + ` FROM entity_aliases ea
			WHERE ea.entity_id = $1
			ORDER BY ea.created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, entityID.String())
	if err != nil {
		return nil, fmt.Errorf("entity alias list by entity: %w", err)
	}
	defer rows.Close()

	return r.scanAliases(rows)
}

const selectEntityAliasColumns = `SELECT ea.id, ea.namespace_id, ea.entity_id, ea.alias, ea.alias_type, ea.created_at`

func (r *EntityAliasRepo) scanAliasFromRows(rows *sql.Rows) (*model.EntityAlias, error) {
	var alias model.EntityAlias
	var idStr, namespaceIDStr, entityIDStr string
	var createdAtStr string

	err := rows.Scan(&idStr, &namespaceIDStr, &entityIDStr, &alias.Alias, &alias.AliasType, &createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("entity alias scan rows: %w", err)
	}

	return r.populateAlias(&alias, idStr, namespaceIDStr, entityIDStr, createdAtStr)
}

func (r *EntityAliasRepo) scanAliases(rows *sql.Rows) ([]model.EntityAlias, error) {
	result := []model.EntityAlias{}
	for rows.Next() {
		alias, err := r.scanAliasFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *alias)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("entity alias scan iteration: %w", err)
	}
	return result, nil
}

func (r *EntityAliasRepo) populateAlias(
	alias *model.EntityAlias,
	idStr, namespaceIDStr, entityIDStr, createdAtStr string,
) (*model.EntityAlias, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("entity alias parse id: %w", err)
	}
	alias.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("entity alias parse namespace_id: %w", err)
	}
	alias.NamespaceID = nsID

	entityID, err := uuid.Parse(entityIDStr)
	if err != nil {
		return nil, fmt.Errorf("entity alias parse entity_id: %w", err)
	}
	alias.EntityID = entityID

	alias.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("entity alias parse created_at: %w", err)
	}

	return alias, nil
}
