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

// Create inserts a new entity alias. ID is generated if zero-valued.
func (r *EntityAliasRepo) Create(ctx context.Context, alias *model.EntityAlias) error {
	if alias.ID == uuid.Nil {
		alias.ID = uuid.New()
	}

	query := `INSERT INTO entity_aliases (id, entity_id, alias, alias_type)
		VALUES (?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO entity_aliases (id, entity_id, alias, alias_type)
			VALUES ($1, $2, $3, $4)`
	}

	_, err := r.db.Exec(ctx, query,
		alias.ID.String(), alias.EntityID.String(), alias.Alias, alias.AliasType,
	)
	if err != nil {
		return fmt.Errorf("entity alias create: %w", err)
	}

	return r.reload(ctx, alias)
}

// FindByAlias finds entity aliases matching the alias text (case-insensitive),
// scoped to namespace via JOIN with entities.
func (r *EntityAliasRepo) FindByAlias(ctx context.Context, namespaceID uuid.UUID, alias string) ([]model.EntityAlias, error) {
	query := selectEntityAliasColumns + ` FROM entity_aliases ea
		INNER JOIN entities e ON ea.entity_id = e.id
		WHERE e.namespace_id = ? AND ea.alias = ? COLLATE NOCASE`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityAliasColumns + ` FROM entity_aliases ea
			INNER JOIN entities e ON ea.entity_id = e.id
			WHERE e.namespace_id = $1 AND LOWER(ea.alias) = LOWER($2)`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), alias)
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

// reload fetches the alias by ID and populates the struct in place.
func (r *EntityAliasRepo) reload(ctx context.Context, alias *model.EntityAlias) error {
	query := selectEntityAliasColumns + ` FROM entity_aliases ea WHERE ea.id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectEntityAliasColumns + ` FROM entity_aliases ea WHERE ea.id = $1`
	}

	row := r.db.QueryRow(ctx, query, alias.ID.String())
	fetched, err := r.scanAlias(row)
	if err != nil {
		return fmt.Errorf("entity alias reload: %w", err)
	}
	*alias = *fetched
	return nil
}

const selectEntityAliasColumns = `SELECT ea.id, ea.entity_id, ea.alias, ea.alias_type, ea.created_at`

func (r *EntityAliasRepo) scanAlias(row *sql.Row) (*model.EntityAlias, error) {
	var alias model.EntityAlias
	var idStr, entityIDStr string
	var createdAtStr string

	err := row.Scan(&idStr, &entityIDStr, &alias.Alias, &alias.AliasType, &createdAtStr)
	if err != nil {
		return nil, err
	}

	return r.populateAlias(&alias, idStr, entityIDStr, createdAtStr)
}

func (r *EntityAliasRepo) scanAliasFromRows(rows *sql.Rows) (*model.EntityAlias, error) {
	var alias model.EntityAlias
	var idStr, entityIDStr string
	var createdAtStr string

	err := rows.Scan(&idStr, &entityIDStr, &alias.Alias, &alias.AliasType, &createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("entity alias scan rows: %w", err)
	}

	return r.populateAlias(&alias, idStr, entityIDStr, createdAtStr)
}

func (r *EntityAliasRepo) scanAliases(rows *sql.Rows) ([]model.EntityAlias, error) {
	var result []model.EntityAlias
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
	idStr, entityIDStr, createdAtStr string,
) (*model.EntityAlias, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("entity alias parse id: %w", err)
	}
	alias.ID = id

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
