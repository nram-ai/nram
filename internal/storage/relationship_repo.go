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

// RelationshipRepo provides CRUD operations for the relationships table.
type RelationshipRepo struct {
	db DB
}

// NewRelationshipRepo creates a new RelationshipRepo backed by the given DB.
func NewRelationshipRepo(db DB) *RelationshipRepo {
	return &RelationshipRepo{db: db}
}

// Create inserts a new relationship. ID is generated if zero-valued.
// Properties defaults to `{}` if nil.
func (r *RelationshipRepo) Create(ctx context.Context, rel *model.Relationship) error {
	if rel.ID == uuid.Nil {
		rel.ID = uuid.New()
	}
	if rel.Properties == nil {
		rel.Properties = json.RawMessage(`{}`)
	}

	var validUntil interface{}
	if rel.ValidUntil != nil {
		validUntil = rel.ValidUntil.UTC().Format(time.RFC3339)
	}

	var sourceMemory interface{}
	if rel.SourceMemory != nil {
		sourceMemory = rel.SourceMemory.String()
	}

	query := `INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, valid_until, source_memory)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, valid_until, source_memory)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	}

	validFrom := rel.ValidFrom
	if validFrom.IsZero() {
		validFrom = time.Now().UTC()
	}

	_, err := r.db.Exec(ctx, query,
		rel.ID.String(), rel.NamespaceID.String(), rel.SourceID.String(), rel.TargetID.String(),
		rel.Relation, rel.Weight, string(rel.Properties),
		validFrom.UTC().Format(time.RFC3339), validUntil, sourceMemory,
	)
	if err != nil {
		return fmt.Errorf("relationship create: %w", err)
	}

	return r.reload(ctx, rel)
}

// GetByID returns a relationship by its UUID.
func (r *RelationshipRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Relationship, error) {
	query := selectRelationshipColumns + ` FROM relationships WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectRelationshipColumns + ` FROM relationships WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanRelationship(row)
}

// Expire sets valid_until to the current time for the given relationship.
func (r *RelationshipRepo) Expire(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE relationships SET valid_until = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships SET valid_until = $1 WHERE id = $2`
	}

	result, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("relationship expire: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("relationship expire rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// Reinforce increments the weight of a relationship by 1.
func (r *RelationshipRepo) Reinforce(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE relationships SET weight = weight + 1 WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships SET weight = weight + 1 WHERE id = $1`
	}

	result, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("relationship reinforce: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("relationship reinforce rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// TraverseFromEntity performs a BFS traversal from a starting entity, collecting
// all relationships up to maxHops hops. It handles cycles by not revisiting entities.
func (r *RelationshipRepo) TraverseFromEntity(ctx context.Context, entityID uuid.UUID, maxHops int) ([]model.Relationship, error) {
	if maxHops <= 0 {
		return nil, nil
	}

	visitedEntities := map[uuid.UUID]bool{entityID: true}
	visitedRels := map[uuid.UUID]bool{}
	frontier := []uuid.UUID{entityID}
	result := []model.Relationship{}

	for hop := 0; hop < maxHops && len(frontier) > 0; hop++ {
		var nextFrontier []uuid.UUID

		for _, eid := range frontier {
			rels, err := r.ListByEntity(ctx, eid)
			if err != nil {
				return nil, fmt.Errorf("relationship traverse hop %d: %w", hop, err)
			}

			for _, rel := range rels {
				if !visitedRels[rel.ID] {
					visitedRels[rel.ID] = true
					result = append(result, rel)
				}

				// Determine the neighbor entity (the other end of the relationship).
				neighbor := rel.TargetID
				if neighbor == eid {
					neighbor = rel.SourceID
				}

				if !visitedEntities[neighbor] {
					visitedEntities[neighbor] = true
					nextFrontier = append(nextFrontier, neighbor)
				}
			}
		}

		frontier = nextFrontier
	}

	return result, nil
}

// ListByNamespace returns all relationships for a namespace, ordered by created_at DESC.
func (r *RelationshipRepo) ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Relationship, error) {
	query := selectRelationshipColumns + ` FROM relationships
		WHERE namespace_id = ?
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectRelationshipColumns + ` FROM relationships
			WHERE namespace_id = $1
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("relationship list by namespace: %w", err)
	}
	defer rows.Close()

	return r.scanRelationships(rows)
}

// ListByEntity returns all relationships where the given entity is either
// the source or the target, ordered by created_at DESC.
func (r *RelationshipRepo) ListByEntity(ctx context.Context, entityID uuid.UUID) ([]model.Relationship, error) {
	query := selectRelationshipColumns + ` FROM relationships
		WHERE source_id = ? OR target_id = ?
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectRelationshipColumns + ` FROM relationships
			WHERE source_id = $1 OR target_id = $2
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, entityID.String(), entityID.String())
	if err != nil {
		return nil, fmt.Errorf("relationship list by entity: %w", err)
	}
	defer rows.Close()

	return r.scanRelationships(rows)
}

// reload fetches the relationship by ID and populates the struct in place.
func (r *RelationshipRepo) reload(ctx context.Context, rel *model.Relationship) error {
	fetched, err := r.GetByID(ctx, rel.ID)
	if err != nil {
		return fmt.Errorf("relationship reload: %w", err)
	}
	*rel = *fetched
	return nil
}

const selectRelationshipColumns = `SELECT id, namespace_id, source_id, target_id, relation,
	weight, properties, valid_from, valid_until, source_memory, created_at`

func (r *RelationshipRepo) scanRelationship(row *sql.Row) (*model.Relationship, error) {
	var rel model.Relationship
	var idStr, namespaceIDStr, sourceIDStr, targetIDStr string
	var propertiesStr string
	var validFromStr, createdAtStr string
	var validUntilStr sql.NullString
	var sourceMemoryStr sql.NullString

	err := row.Scan(
		&idStr, &namespaceIDStr, &sourceIDStr, &targetIDStr, &rel.Relation,
		&rel.Weight, &propertiesStr, &validFromStr, &validUntilStr,
		&sourceMemoryStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateRelationship(&rel, idStr, namespaceIDStr, sourceIDStr, targetIDStr,
		propertiesStr, validFromStr, validUntilStr, sourceMemoryStr, createdAtStr)
}

func (r *RelationshipRepo) scanRelationshipFromRows(rows *sql.Rows) (*model.Relationship, error) {
	var rel model.Relationship
	var idStr, namespaceIDStr, sourceIDStr, targetIDStr string
	var propertiesStr string
	var validFromStr, createdAtStr string
	var validUntilStr sql.NullString
	var sourceMemoryStr sql.NullString

	err := rows.Scan(
		&idStr, &namespaceIDStr, &sourceIDStr, &targetIDStr, &rel.Relation,
		&rel.Weight, &propertiesStr, &validFromStr, &validUntilStr,
		&sourceMemoryStr, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("relationship scan rows: %w", err)
	}

	return r.populateRelationship(&rel, idStr, namespaceIDStr, sourceIDStr, targetIDStr,
		propertiesStr, validFromStr, validUntilStr, sourceMemoryStr, createdAtStr)
}

func (r *RelationshipRepo) scanRelationships(rows *sql.Rows) ([]model.Relationship, error) {
	result := []model.Relationship{}
	for rows.Next() {
		rel, err := r.scanRelationshipFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *rel)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("relationship scan iteration: %w", err)
	}
	return result, nil
}

func (r *RelationshipRepo) populateRelationship(
	rel *model.Relationship,
	idStr, namespaceIDStr, sourceIDStr, targetIDStr, propertiesStr string,
	validFromStr string,
	validUntilStr, sourceMemoryStr sql.NullString,
	createdAtStr string,
) (*model.Relationship, error) {
	var err error

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("relationship parse id: %w", err)
	}
	rel.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("relationship parse namespace_id: %w", err)
	}
	rel.NamespaceID = nsID

	srcID, err := uuid.Parse(sourceIDStr)
	if err != nil {
		return nil, fmt.Errorf("relationship parse source_id: %w", err)
	}
	rel.SourceID = srcID

	tgtID, err := uuid.Parse(targetIDStr)
	if err != nil {
		return nil, fmt.Errorf("relationship parse target_id: %w", err)
	}
	rel.TargetID = tgtID

	rel.Properties = json.RawMessage(propertiesStr)

	rel.ValidFrom, err = time.Parse(time.RFC3339, validFromStr)
	if err != nil {
		return nil, fmt.Errorf("relationship parse valid_from: %w", err)
	}

	if validUntilStr.Valid {
		t, err := time.Parse(time.RFC3339, validUntilStr.String)
		if err != nil {
			return nil, fmt.Errorf("relationship parse valid_until: %w", err)
		}
		rel.ValidUntil = &t
	}

	if sourceMemoryStr.Valid {
		smID, err := uuid.Parse(sourceMemoryStr.String)
		if err != nil {
			return nil, fmt.Errorf("relationship parse source_memory: %w", err)
		}
		rel.SourceMemory = &smID
	}

	rel.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("relationship parse created_at: %w", err)
	}

	return rel, nil
}
