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

// relationshipBatchChunkSize bounds how many rows go into one multi-row
// statement issued by the BatchCreate / BatchExpire / BatchReinforce /
// BatchUpdateWeight / BatchDeleteByID methods. BatchCreate uses 11
// placeholders per row, so a 500-row chunk emits 5500 placeholders well
// under the SQLite default SQLITE_MAX_VARIABLE_NUMBER (32766) and Postgres
// protocol limit (65535). The same constant covers the update/delete
// methods, which use at most 3 placeholders per row.
const relationshipBatchChunkSize = 500

// RelationshipRepo provides CRUD operations for the relationships table.
type RelationshipRepo struct {
	db DB
}

// NewRelationshipRepo creates a new RelationshipRepo backed by the given DB.
func NewRelationshipRepo(db DB) *RelationshipRepo {
	return &RelationshipRepo{db: db}
}

// Create inserts a new relationship. ID/ValidFrom/CreatedAt default from Go
// when zero. Properties defaults to `{}` if nil. Concurrent calls with the
// same (namespace, src, tgt, relation, valid_from) triple converge on
// max(inputs) for weight and last-writer-wins for properties.
func (r *RelationshipRepo) Create(ctx context.Context, rel *model.Relationship) error {
	if rel.ID == uuid.Nil {
		rel.ID = uuid.New()
	}
	if rel.Properties == nil {
		rel.Properties = json.RawMessage(`{}`)
	}
	now := time.Now().UTC()
	if rel.ValidFrom.IsZero() {
		rel.ValidFrom = now
	}
	if rel.CreatedAt.IsZero() {
		rel.CreatedAt = now
	}
	// Canonicalize the relation label so formatting variants collapse onto one
	// row via the unique key (the ON CONFLICT below then merges weights). The
	// repo applies formatting-only normalization; closed-vocabulary coercion is
	// done at the extraction write path so imports and programmatic edges keep
	// their original labels. Write back onto the struct, like rel.ID above.
	rel.Relation = model.CanonicalRelation(rel.Relation)

	var validUntil any
	if rel.ValidUntil != nil {
		validUntil = rel.ValidUntil.UTC().Format(time.RFC3339)
	}

	var sourceMemory any
	if rel.SourceMemory != nil {
		sourceMemory = rel.SourceMemory.String()
	}

	validFromStr := rel.ValidFrom.UTC().Format(time.RFC3339)
	createdAtStr := rel.CreatedAt.UTC().Format(time.RFC3339)

	var query string
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, valid_until, source_memory, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT(namespace_id, source_id, target_id, relation, valid_from) DO UPDATE SET
				weight = GREATEST(relationships.weight, EXCLUDED.weight),
				properties = EXCLUDED.properties
			RETURNING id`
	} else {
		query = `INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, valid_until, source_memory, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(namespace_id, source_id, target_id, relation, valid_from) DO UPDATE SET
				weight = MAX(weight, excluded.weight),
				properties = excluded.properties
			RETURNING id`
	}

	var actualID string
	err := r.db.WriteQueryRow(ctx, query,
		rel.ID.String(), rel.NamespaceID.String(), rel.SourceID.String(), rel.TargetID.String(),
		rel.Relation, rel.Weight, string(rel.Properties),
		validFromStr, validUntil, sourceMemory, createdAtStr,
	).Scan(&actualID)
	if err != nil {
		return fmt.Errorf("relationship create: %w", err)
	}
	rel.ID, _ = uuid.Parse(actualID)
	return nil
}

// GetByID returns a relationship by its UUID, bounded to namespaceID. A row in
// a different namespace reads as sql.ErrNoRows: existence is never leaked across
// the tenant boundary.
func (r *RelationshipRepo) GetByID(ctx context.Context, id, namespaceID uuid.UUID) (*model.Relationship, error) {
	query := selectRelationshipColumns + ` FROM relationships WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectRelationshipColumns + ` FROM relationships WHERE id = $1 AND namespace_id = $2`
	}

	row := r.db.QueryRow(ctx, query, id.String(), namespaceID.String())
	return r.scanRelationship(row)
}

// Expire sets valid_until to the current time for the given relationship.
func (r *RelationshipRepo) Expire(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE relationships SET valid_until = ? WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships SET valid_until = $1 WHERE id = $2 AND namespace_id = $3`
	}

	result, err := r.db.Exec(ctx, query, now, id.String(), namespaceID.String())
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

// Reinforce adds delta to a relationship's weight, clamped at the 2.0 ceiling
// at the SQL layer (min on SQLite, LEAST on Postgres). Returns sql.ErrNoRows
// if the (id, namespace_id) pair does not match a row, so callers can detect
// and ignore deletions racing with reinforcement.
//
// The cap matches the upper clamp in dreaming/phase_weights.go calculateWeight
// so the recall-side write and the dream-side adjustment cannot diverge from
// the same ceiling.
func (r *RelationshipRepo) Reinforce(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, delta float64) error {
	query := `UPDATE relationships SET weight = min(weight + ?, 2.0) WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships SET weight = LEAST(weight + $1, 2.0) WHERE id = $2 AND namespace_id = $3`
	}

	result, err := r.db.Exec(ctx, query, delta, id.String(), namespaceID.String())
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

// TraversalResult carries the relationship slice and a truncation signal so
// callers can attach a partial-result envelope without having to compare the
// returned slice length against the cap they passed in. Cap echoes the limit
// that was applied (0 when no cap was requested).
type TraversalResult struct {
	Relationships []model.Relationship
	Truncated     bool
	Cap           int
}

// TraverseFromEntity performs a BFS traversal from a starting entity, collecting
// relationships up to maxHops hops. It handles cycles by not revisiting entities.
// When maxEdges > 0 the traversal short-circuits once that many unique
// relationships have been collected, sparing the unbounded ListByEntity loop
// and the downstream marshal/filter work that follows on large neighborhoods.
// maxEdges <= 0 disables the cap.
func (r *RelationshipRepo) TraverseFromEntity(ctx context.Context, entityID uuid.UUID, namespaces []uuid.UUID, maxHops, maxEdges int) (TraversalResult, error) {
	// Fail-closed: with no namespace bound the traversal returns nothing rather
	// than crossing the tenant boundary. ListByEntity below enforces the same.
	if maxHops <= 0 || len(namespaces) == 0 {
		return TraversalResult{Cap: maxEdges}, nil
	}

	visitedEntities := map[uuid.UUID]bool{entityID: true}
	visitedRels := map[uuid.UUID]bool{}
	frontier := []uuid.UUID{entityID}
	result := []model.Relationship{}
	truncated := false

hops:
	for hop := 0; hop < maxHops && len(frontier) > 0; hop++ {
		var nextFrontier []uuid.UUID

		// One query for the whole frontier instead of one per node. The result
		// is ordered created_at DESC, so filtering it per frontier entity (in
		// frontier order) reproduces the exact per-node ListByEntity ordering
		// the previous one-query-per-node loop used, keeping the accumulation
		// order and the maxEdges truncation point identical.
		hopRels, err := r.ListByEntities(ctx, frontier, namespaces)
		if err != nil {
			return TraversalResult{Cap: maxEdges}, fmt.Errorf("relationship traverse hop %d: %w", hop, err)
		}

		// Bucket the hop's relationships by each endpoint entity in a single
		// pass, preserving the created_at DESC order within each bucket. The
		// frontier walk below then indexes directly into each entity's bucket,
		// making the hop O(len(hopRels)) instead of O(len(frontier)*len(hopRels)).
		// A relationship between two distinct entities lands in both buckets, so
		// a rel touching two frontier nodes is still visited once per node (as in
		// the prior per-node scan); a self-loop is bucketed once.
		byEntity := make(map[uuid.UUID][]model.Relationship, len(frontier))
		for _, rel := range hopRels {
			byEntity[rel.SourceID] = append(byEntity[rel.SourceID], rel)
			if rel.TargetID != rel.SourceID {
				byEntity[rel.TargetID] = append(byEntity[rel.TargetID], rel)
			}
		}

		for _, eid := range frontier {
			for _, rel := range byEntity[eid] {
				if !visitedRels[rel.ID] {
					visitedRels[rel.ID] = true
					result = append(result, rel)
					if maxEdges > 0 && len(result) >= maxEdges {
						truncated = true
						break hops
					}
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

	return TraversalResult{Relationships: result, Truncated: truncated, Cap: maxEdges}, nil
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
	defer func() { _ = rows.Close() }()

	return r.scanRelationships(rows)
}

// ListByEntity returns relationships where the given entity is the source or
// the target, bounded to the supplied namespaces and ordered by created_at
// DESC. The namespace bound lives in the query (not in callers): this is the
// single tenant-isolation choke point for graph traversal, so no caller can
// surface a cross-namespace edge by forgetting to re-filter. Fail-closed: an
// empty namespaces slice returns no rows.
func (r *RelationshipRepo) ListByEntity(ctx context.Context, entityID uuid.UUID, namespaces []uuid.UUID) ([]model.Relationship, error) {
	if len(namespaces) == 0 {
		return []model.Relationship{}, nil
	}

	// source_id/target_id occupy $1/$2; namespace placeholders start at $3.
	nsPlaceholders, nsArgs := uuidInPlaceholders(r.db, namespaces, 3)
	nsIn := strings.Join(nsPlaceholders, ", ")

	var query string
	if r.db.Backend() == BackendPostgres {
		query = selectRelationshipColumns + ` FROM relationships
			WHERE (source_id = $1 OR target_id = $2) AND namespace_id IN (` + nsIn + `)
			ORDER BY created_at DESC`
	} else {
		query = selectRelationshipColumns + ` FROM relationships
			WHERE (source_id = ? OR target_id = ?) AND namespace_id IN (` + nsIn + `)
			ORDER BY created_at DESC`
	}

	args := append([]any{entityID.String(), entityID.String()}, nsArgs...)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("relationship list by entity: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return r.scanRelationships(rows)
}

// RelabelRelations coerces every relationship label into the closed vocabulary
// (model.CanonicalRelationVocab) in place, stamping kinship subtype into
// properties.kind before the family_of collapse erases it. When a relabel would
// collide with an existing edge on the unique key (namespace, source, target,
// relation, valid_from), the colliding rows are merged (max weight kept) rather
// than failing the constraint. Idempotent; dryRun returns the counts without
// writing. Returns rows whose label changes, and the distinct-relation
// cardinality before and after.
func (r *RelationshipRepo) RelabelRelations(ctx context.Context, dryRun bool) (rowsChanged int64, distinctBefore, distinctAfter int, err error) {
	rows, err := r.db.Query(ctx, `SELECT relation, COUNT(*) FROM relationships GROUP BY relation`)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("relabel relations: distinct scan: %w", err)
	}
	type relCount struct {
		old   string
		count int64
	}
	var counts []relCount
	afterSet := make(map[string]struct{})
	for rows.Next() {
		var rc relCount
		if scanErr := rows.Scan(&rc.old, &rc.count); scanErr != nil {
			_ = rows.Close()
			return 0, 0, 0, fmt.Errorf("relabel relations: scan: %w", scanErr)
		}
		counts = append(counts, rc)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, 0, fmt.Errorf("relabel relations: rows: %w", err)
	}

	distinctBefore = len(counts)
	for _, rc := range counts {
		newRel := model.CanonicalRelationVocab(rc.old)
		afterSet[newRel] = struct{}{}
		if newRel != rc.old {
			rowsChanged += rc.count
		}
	}
	distinctAfter = len(afterSet)
	if dryRun {
		return rowsChanged, distinctBefore, distinctAfter, nil
	}

	for _, rc := range counts {
		if kind := model.RelationKind(rc.old); kind != "" {
			if err := r.stampRelationKind(ctx, rc.old, kind); err != nil {
				return rowsChanged, distinctBefore, distinctAfter, err
			}
		}
		newRel := model.CanonicalRelationVocab(rc.old)
		if newRel == rc.old {
			continue
		}
		if err := r.relabelRelationOne(ctx, rc.old, newRel); err != nil {
			return rowsChanged, distinctBefore, distinctAfter, err
		}
	}
	return rowsChanged, distinctBefore, distinctAfter, nil
}

// stampRelationKind writes properties.kind for every row with the given relation
// label that does not already carry a kind, using each backend's JSON builder.
func (r *RelationshipRepo) stampRelationKind(ctx context.Context, relation, kind string) error {
	var query string
	var args []any
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships
			SET properties = jsonb_set(COALESCE(properties, '{}'::jsonb), '{kind}', to_jsonb($2::text), true)
			WHERE relation = $1 AND (properties->>'kind') IS NULL`
		args = []any{relation, kind}
	} else {
		query = `UPDATE relationships
			SET properties = json_set(COALESCE(properties, '{}'), '$.kind', ?)
			WHERE relation = ? AND json_extract(COALESCE(properties, '{}'), '$.kind') IS NULL`
		args = []any{kind, relation}
	}
	if _, err := r.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("stamp relation kind: %w", err)
	}
	return nil
}

// relabelRelationOne relabels all rows with relation=old to new, merging any
// that would collide with an existing new-labeled edge on the unique key
// (keeping the higher weight). Mirrors mergeRelationshipsByEndpoint but keys on
// the relation label.
func (r *RelationshipRepo) relabelRelationOne(ctx context.Context, old, newRel string) error {
	pg := r.db.Backend() == BackendPostgres
	// Step 1: lift max(weight) onto existing new-labeled rows that collide.
	var maxQ string
	if pg {
		maxQ = `UPDATE relationships newrel SET weight = GREATEST(newrel.weight, oldrel.weight)
			FROM relationships oldrel
			WHERE newrel.relation = $2 AND oldrel.relation = $1
			  AND newrel.namespace_id = oldrel.namespace_id
			  AND newrel.source_id = oldrel.source_id
			  AND newrel.target_id = oldrel.target_id
			  AND newrel.valid_from = oldrel.valid_from`
	} else {
		maxQ = `UPDATE relationships
			SET weight = MAX(weight, (
				SELECT o.weight FROM relationships o
				WHERE o.relation = ? AND o.namespace_id = relationships.namespace_id
				  AND o.source_id = relationships.source_id AND o.target_id = relationships.target_id
				  AND o.valid_from = relationships.valid_from))
			WHERE relation = ? AND EXISTS (
				SELECT 1 FROM relationships o
				WHERE o.relation = ? AND o.namespace_id = relationships.namespace_id
				  AND o.source_id = relationships.source_id AND o.target_id = relationships.target_id
				  AND o.valid_from = relationships.valid_from)`
	}
	if pg {
		if _, err := r.db.Exec(ctx, maxQ, old, newRel); err != nil {
			return fmt.Errorf("relabel merge weights: %w", err)
		}
	} else {
		if _, err := r.db.Exec(ctx, maxQ, old, newRel, old); err != nil {
			return fmt.Errorf("relabel merge weights: %w", err)
		}
	}
	// Step 2: delete old-labeled rows that collide with a new-labeled row.
	var delQ string
	if pg {
		delQ = `DELETE FROM relationships oldrel USING relationships newrel
			WHERE oldrel.relation = $1 AND newrel.relation = $2
			  AND newrel.namespace_id = oldrel.namespace_id
			  AND newrel.source_id = oldrel.source_id
			  AND newrel.target_id = oldrel.target_id
			  AND newrel.valid_from = oldrel.valid_from`
	} else {
		delQ = `DELETE FROM relationships
			WHERE relation = ? AND EXISTS (
				SELECT 1 FROM relationships n
				WHERE n.relation = ? AND n.namespace_id = relationships.namespace_id
				  AND n.source_id = relationships.source_id AND n.target_id = relationships.target_id
				  AND n.valid_from = relationships.valid_from)`
	}
	if _, err := r.db.Exec(ctx, delQ, old, newRel); err != nil {
		return fmt.Errorf("relabel delete collisions: %w", err)
	}
	// Step 3: relabel the survivors.
	var updQ string
	if pg {
		updQ = `UPDATE relationships SET relation = $2 WHERE relation = $1`
	} else {
		updQ = `UPDATE relationships SET relation = ? WHERE relation = ?`
	}
	if pg {
		if _, err := r.db.Exec(ctx, updQ, old, newRel); err != nil {
			return fmt.Errorf("relabel survivors: %w", err)
		}
	} else {
		if _, err := r.db.Exec(ctx, updQ, newRel, old); err != nil {
			return fmt.Errorf("relabel survivors: %w", err)
		}
	}
	return nil
}

// ListByEntities returns every relationship touching any of the given entities
// (as source or target), bounded to the supplied namespaces and ordered by
// created_at DESC. It is the batched form of ListByEntity used by graph
// traversal to collect a whole BFS frontier in one query instead of one query
// per node. The same created_at DESC order lets a caller reproduce the
// per-entity ListByEntity ordering by filtering this set per entity. Same
// fail-closed tenant bound: an empty entities or namespaces slice returns no
// rows.
func (r *RelationshipRepo) ListByEntities(ctx context.Context, entityIDs []uuid.UUID, namespaces []uuid.UUID) ([]model.Relationship, error) {
	if len(entityIDs) == 0 || len(namespaces) == 0 {
		return []model.Relationship{}, nil
	}

	// source_id IN (...) placeholders start at $1, target_id IN (...) reuse the
	// same entity values at the next block, namespace IN (...) follows.
	srcPh, srcArgs := uuidInPlaceholders(r.db, entityIDs, 1)
	tgtPh, tgtArgs := uuidInPlaceholders(r.db, entityIDs, 1+len(entityIDs))
	nsPh, nsArgs := uuidInPlaceholders(r.db, namespaces, 1+2*len(entityIDs))

	query := selectRelationshipColumns + ` FROM relationships
		WHERE (source_id IN (` + strings.Join(srcPh, ", ") + `) OR target_id IN (` + strings.Join(tgtPh, ", ") + `))
			AND namespace_id IN (` + strings.Join(nsPh, ", ") + `)
		ORDER BY created_at DESC`

	args := make([]any, 0, len(srcArgs)+len(tgtArgs)+len(nsArgs))
	args = append(args, srcArgs...)
	args = append(args, tgtArgs...)
	args = append(args, nsArgs...)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("relationship list by entities: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return r.scanRelationships(rows)
}

// DeleteByNamespaceTx deletes all relationships in a namespace inside the
// caller's transaction. Used by the project-delete cascade so the whole
// teardown is atomic with the project row delete.
func (r *RelationshipRepo) DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error {
	query := `DELETE FROM relationships WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM relationships WHERE namespace_id = $1`
	}
	if _, err := tx.ExecContext(ctx, query, namespaceID.String()); err != nil {
		return fmt.Errorf("relationship delete by namespace: %w", err)
	}
	return nil
}

// UpdateWeight sets the weight of a relationship to a specific value.
func (r *RelationshipRepo) UpdateWeight(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, weight float64) error {
	query := `UPDATE relationships SET weight = ? WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships SET weight = $1 WHERE id = $2 AND namespace_id = $3`
	}
	_, err := r.db.Exec(ctx, query, weight, id.String(), namespaceID.String())
	if err != nil {
		return fmt.Errorf("relationship update weight: %w", err)
	}
	return nil
}

// FindActiveByTriple returns an active (non-expired) relationship matching the
// given (namespace, source, target, relation) triple, or nil if none exists.
func (r *RelationshipRepo) FindActiveByTriple(ctx context.Context, namespaceID, sourceID, targetID uuid.UUID, relation string) (*model.Relationship, error) {
	// Relations are stored canonical, so the lookup key must be canonicalized to
	// match (covers callers that pass a raw extractor/LLM relation string).
	relation = model.CanonicalRelation(relation)
	query := selectRelationshipColumns + ` FROM relationships
		WHERE namespace_id = ? AND source_id = ? AND target_id = ? AND relation = ? AND valid_until IS NULL
		ORDER BY weight DESC LIMIT 1`
	if r.db.Backend() == BackendPostgres {
		query = selectRelationshipColumns + ` FROM relationships
			WHERE namespace_id = $1 AND source_id = $2 AND target_id = $3 AND relation = $4 AND valid_until IS NULL
			ORDER BY weight DESC LIMIT 1`
	}

	row := r.db.QueryRow(ctx, query, namespaceID.String(), sourceID.String(), targetID.String(), relation)
	rel, err := r.scanRelationship(row)
	if err != nil {
		return nil, err
	}
	return rel, nil
}

// CountActiveByNamespace returns the number of active (non-expired) relationships in a namespace.
func (r *RelationshipRepo) CountActiveByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM relationships WHERE namespace_id = ? AND valid_until IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT COUNT(*) FROM relationships WHERE namespace_id = $1 AND valid_until IS NULL`
	}

	var count int
	err := r.db.QueryRow(ctx, query, namespaceID.String()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("relationship count by namespace: %w", err)
	}
	return count, nil
}

// HasBySourceMemory reports whether at least one relationship row exists
// with the given memory recorded as source_memory. Used by the enrichment
// worker to detect that entity extraction has already produced edges for
// this memory in a prior run, so the chat-completion step can be skipped.
func (r *RelationshipRepo) HasBySourceMemory(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) (bool, error) {
	query := `SELECT 1 FROM relationships
		WHERE namespace_id = ? AND source_memory = ?
		LIMIT 1`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT 1 FROM relationships
			WHERE namespace_id = $1 AND source_memory = $2
			LIMIT 1`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), memoryID.String())
	if err != nil {
		return false, fmt.Errorf("relationship has by source memory: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return rows.Next(), rows.Err()
}

// ExpireLowWeight expires all active relationships in a namespace with weight below the threshold.
// Returns the number of relationships expired.
func (r *RelationshipRepo) ExpireLowWeight(ctx context.Context, namespaceID uuid.UUID, threshold float64) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE relationships SET valid_until = ? WHERE namespace_id = ? AND valid_until IS NULL AND weight < ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships SET valid_until = $1 WHERE namespace_id = $2 AND valid_until IS NULL AND weight < $3`
	}

	result, err := r.db.Exec(ctx, query, now, namespaceID.String(), threshold)
	if err != nil {
		return 0, fmt.Errorf("relationship expire low weight: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("relationship expire low weight rows: %w", err)
	}
	return rows, nil
}

// ExpireLowestNTransitive expires the N lowest-weight active transitive
// (inferred) relationships in a namespace. Transitive rows are identified
// by the properties.source = "transitive" marker that the transitive
// discovery phase writes at creation time (source-of-truth constant:
// dreaming.transitivePropertySource, kept in sync with the SQL literal
// below). Ties broken by oldest first. User-asserted relationships are
// never touched. Returns the count expired.
func (r *RelationshipRepo) ExpireLowestNTransitive(ctx context.Context, namespaceID uuid.UUID, n int) (int64, error) {
	if n <= 0 {
		return 0, nil
	}
	now := time.Now().UTC().Format(time.RFC3339)

	// SQLite stores properties as TEXT; reach in via json_extract. Postgres
	// stores as jsonb; reach in via the ->> operator. Both dialects support
	// UPDATE ... WHERE id IN (SELECT ... LIMIT N).
	query := `UPDATE relationships SET valid_until = ?
		WHERE id IN (
			SELECT id FROM relationships
			WHERE namespace_id = ?
			  AND valid_until IS NULL
			  AND json_extract(properties, '$.source') = 'transitive'
			ORDER BY weight ASC, created_at ASC
			LIMIT ?
		)`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE relationships SET valid_until = $1
			WHERE id IN (
				SELECT id FROM relationships
				WHERE namespace_id = $2
				  AND valid_until IS NULL
				  AND properties->>'source' = 'transitive'
				ORDER BY weight ASC, created_at ASC
				LIMIT $3
			)`
	}

	result, err := r.db.Exec(ctx, query, now, namespaceID.String(), n)
	if err != nil {
		return 0, fmt.Errorf("relationship expire lowest n transitive: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("relationship expire lowest n transitive rows: %w", err)
	}
	return rows, nil
}

// DeleteByID removes a single relationship by its ID.
func (r *RelationshipRepo) DeleteByID(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error {
	query := `DELETE FROM relationships WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM relationships WHERE id = $1 AND namespace_id = $2`
	}
	_, err := r.db.Exec(ctx, query, id.String(), namespaceID.String())
	if err != nil {
		return fmt.Errorf("relationship delete by id: %w", err)
	}
	return nil
}

// DeleteDangling removes relationships where the source or target entity no longer exists.
// Returns the number of relationships deleted.
func (r *RelationshipRepo) DeleteDangling(ctx context.Context) (int64, error) {
	query := `DELETE FROM relationships WHERE
		source_id NOT IN (SELECT id FROM entities) OR
		target_id NOT IN (SELECT id FROM entities)`
	result, err := r.db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("relationship delete dangling: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("relationship delete dangling rows: %w", err)
	}
	return rows, nil
}

// lostProvenancePredicate matches relationships whose provenance is gone: the
// source_memory pointer is NULL (the sourcing memory was hard-deleted, firing
// the ON DELETE SET NULL FK action, or a dream phase inherited an
// already-null parent) or it points at a memory that is soft-deleted or
// superseded. No legitimate insert leaves source_memory NULL, and verified on
// live data, every non-null pointer resolves to an existing memory, so the
// predicate never matches a live-sourced edge.
const lostProvenancePredicate = `source_memory IS NULL OR source_memory IN (` +
	`SELECT id FROM memories WHERE deleted_at IS NOT NULL OR superseded_by IS NOT NULL)`

// DeleteBySourceMemory removes every relationship whose provenance points at
// the given memory and returns the distinct entity IDs that were endpoints of
// the deleted edges, so the caller can recompute their mention_count and
// orphan-sweep entities that drop to zero edges. The forget and supersede
// paths call this to reap a memory's exclusively-sourced graph footprint
// before the FK ON DELETE SET NULL would erase the provenance link.
func (r *RelationshipRepo) DeleteBySourceMemory(ctx context.Context, namespaceID, memoryID uuid.UUID) ([]uuid.UUID, error) {
	query := `DELETE FROM relationships WHERE namespace_id = ? AND source_memory = ? RETURNING source_id, target_id`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM relationships WHERE namespace_id = $1 AND source_memory = $2 RETURNING source_id, target_id`
	}
	rows, err := r.db.WriteQuery(ctx, query, namespaceID.String(), memoryID.String())
	if err != nil {
		return nil, fmt.Errorf("relationship delete by source memory: %w", err)
	}
	defer func() { _ = rows.Close() }()
	ids, _, err := scanEndpointIDs(rows, "relationship delete by source memory")
	return ids, err
}

// DeleteByLostProvenance removes relationships matching lostProvenancePredicate
// and returns the distinct entity IDs that were endpoints of the deleted edges
// (so the caller can scope a mention_count recompute to exactly those entities)
// alongside the raw number of rows deleted (so the batch loop knows when a short
// batch ended it). Such an edge can never be tied back to a live memory and every
// read path now drops it; reaping converges the stored graph and stops dream
// phases from breeding more null-provenance edges off it.
//
// limit > 0 bounds one batch via an id subquery (neither backend supports
// DELETE ... LIMIT portably); callers loop until a short batch is returned.
// limit <= 0 deletes all matching rows in one statement.
func (r *RelationshipRepo) DeleteByLostProvenance(ctx context.Context, limit int) ([]uuid.UUID, int64, error) {
	var query string
	var args []any
	if limit > 0 {
		ph := "?"
		if r.db.Backend() == BackendPostgres {
			ph = "$1"
		}
		query = `DELETE FROM relationships WHERE id IN (` +
			`SELECT id FROM relationships WHERE ` + lostProvenancePredicate + ` LIMIT ` + ph + `)` +
			` RETURNING source_id, target_id`
		args = []any{limit}
	} else {
		query = `DELETE FROM relationships WHERE ` + lostProvenancePredicate +
			` RETURNING source_id, target_id`
	}
	rows, err := r.db.WriteQuery(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("relationship delete by lost provenance: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanEndpointIDs(rows, "relationship delete by lost provenance")
}

// CountLostProvenance returns the number of relationships matching
// lostProvenancePredicate. Surfaced in the admin graph-health endpoint so the
// console can show how many orphaned edges a repair would reap.
func (r *RelationshipRepo) CountLostProvenance(ctx context.Context) (int64, error) {
	const query = `SELECT COUNT(*) FROM relationships WHERE ` + lostProvenancePredicate
	var n int64
	if err := r.db.QueryRow(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("relationship count lost provenance: %w", err)
	}
	return n, nil
}

// scanEndpointIDs drains a *sql.Rows of (source_id, target_id) pairs and
// returns the distinct entity IDs across both columns, preserving first-seen
// order, plus the raw number of rows drained (which differs from the deduped
// endpoint count: one deleted edge contributes up to two endpoints, and many
// edges can share endpoints). Callers that batch on the delete count use the
// drained total; callers that recompute use the deduped IDs. Unparseable IDs
// are skipped rather than failing the whole reap.
func scanEndpointIDs(rows *sql.Rows, errPrefix string) ([]uuid.UUID, int64, error) {
	seen := make(map[uuid.UUID]struct{})
	var out []uuid.UUID
	var drained int64
	for rows.Next() {
		var srcStr, tgtStr string
		if err := rows.Scan(&srcStr, &tgtStr); err != nil {
			return nil, 0, fmt.Errorf("%s scan: %w", errPrefix, err)
		}
		drained++
		for _, s := range [2]string{srcStr, tgtStr} {
			id, err := uuid.Parse(s)
			if err != nil {
				continue
			}
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("%s rows: %w", errPrefix, err)
	}
	return out, drained, nil
}

// BatchCreate inserts (or upserts) the given relationships in one
// transaction with multi-row INSERTs chunked at relationshipBatchChunkSize.
// Per-chunk savepoints absorb tolerable per-row constraint failures (FK
// to a vanished entity, unique violation outside the upsert key): on
// such an error the chunk is rolled back to its savepoint and retried
// row-by-row inside per-row savepoints, with each failed row counted as
// Skipped. Non-tolerable errors abort the outer transaction.
//
// Per-row defaults mirror Create: IDs default to uuid.New(), Properties
// defaults to "{}", ValidFrom and CreatedAt default to now(). The ON
// CONFLICT clause is the same MAX/GREATEST weight, last-writer-wins
// properties pattern.
//
// Caller's rel.ID contract:
//   - On a successful insert, rel.ID is the persisted id (matches the
//     client-generated one).
//   - On ON CONFLICT DO UPDATE, rel.ID is overwritten via RETURNING to
//     the existing row's id so the caller can map back to the actual
//     surviving row (important for dream-log target_id values).
//   - On a per-row constraint-violation skip, rel.ID is set to uuid.Nil
//     as a sentinel so callers can filter out non-persisted entries
//     when iterating post-batch.
func (r *RelationshipRepo) BatchCreate(ctx context.Context, rels []*model.Relationship) (model.BatchCreateResult, error) {
	if len(rels) == 0 {
		return model.BatchCreateResult{}, nil
	}
	now := time.Now().UTC()
	for _, rel := range rels {
		if rel.ID == uuid.Nil {
			rel.ID = uuid.New()
		}
		if rel.Properties == nil {
			rel.Properties = json.RawMessage(`{}`)
		}
		if rel.ValidFrom.IsZero() {
			rel.ValidFrom = now
		}
		if rel.CreatedAt.IsZero() {
			rel.CreatedAt = now
		}
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BatchCreateResult{}, fmt.Errorf("relationship batch create begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var result model.BatchCreateResult
	for i := 0; i < len(rels); i += relationshipBatchChunkSize {
		end := min(i+relationshipBatchChunkSize, len(rels))
		chunk := rels[i:end]
		chunkName := fmt.Sprintf("rel_batch_%d", i/relationshipBatchChunkSize)
		outcome, err := withSavepoint(ctx, tx, chunkName, func() error {
			return r.execBatchCreateChunk(ctx, tx, chunk)
		})
		if err != nil {
			return model.BatchCreateResult{}, err
		}
		if outcome == savepointOK {
			result.Affected += int64(len(chunk))
			continue
		}
		affected, skipped, fbErr := r.fallbackPerRowCreate(ctx, tx, chunk, chunkName)
		if fbErr != nil {
			return model.BatchCreateResult{}, fbErr
		}
		result.Affected += affected
		result.Skipped += skipped
	}
	if err := tx.Commit(); err != nil {
		return model.BatchCreateResult{}, fmt.Errorf("relationship batch create commit: %w", err)
	}
	return result, nil
}

// execBatchCreateChunk emits one multi-VALUES INSERT for the given chunk
// using the same ON CONFLICT semantics as the single-row Create. The
// statement appends `RETURNING id` so each input rel can be re-bound to
// the row that actually persists, including ON CONFLICT cases where the
// surviving row keeps its existing id. The returned ids are scanned in
// input order; Postgres and SQLite both preserve VALUES order in
// RETURNING for INSERT ... ON CONFLICT DO UPDATE.
func (r *RelationshipRepo) execBatchCreateChunk(ctx context.Context, tx *sql.Tx, chunk []*model.Relationship) error {
	isPg := r.db.Backend() == BackendPostgres
	var b strings.Builder
	args := make([]any, 0, 11*len(chunk))
	b.WriteString("INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, valid_until, source_memory, created_at) VALUES ")
	for i, rel := range chunk {
		if i > 0 {
			b.WriteString(", ")
		}
		if isPg {
			base := i*11 + 1
			fmt.Fprintf(&b, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10)
		} else {
			b.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		}
		// Formatting-only canonicalization before binding so variants collide on
		// the unique key and the ON CONFLICT merge fires. fallbackPerRowCreate
		// re-enters this method per row, but CanonicalRelation is idempotent so
		// that is harmless. Vocabulary coercion happens at the extraction layer.
		rel.Relation = model.CanonicalRelation(rel.Relation)
		var validUntil any
		if rel.ValidUntil != nil {
			validUntil = rel.ValidUntil.UTC().Format(time.RFC3339)
		}
		var sourceMemory any
		if rel.SourceMemory != nil {
			sourceMemory = rel.SourceMemory.String()
		}
		args = append(args,
			rel.ID.String(), rel.NamespaceID.String(), rel.SourceID.String(), rel.TargetID.String(),
			rel.Relation, rel.Weight, string(rel.Properties),
			rel.ValidFrom.UTC().Format(time.RFC3339), validUntil, sourceMemory,
			rel.CreatedAt.UTC().Format(time.RFC3339),
		)
	}
	if isPg {
		b.WriteString(" ON CONFLICT(namespace_id, source_id, target_id, relation, valid_from) DO UPDATE SET weight = GREATEST(relationships.weight, EXCLUDED.weight), properties = EXCLUDED.properties RETURNING id")
	} else {
		b.WriteString(" ON CONFLICT(namespace_id, source_id, target_id, relation, valid_from) DO UPDATE SET weight = MAX(weight, excluded.weight), properties = excluded.properties RETURNING id")
	}
	rows, err := tx.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return fmt.Errorf("batch create chunk exec: %w", err)
	}
	defer func() { _ = rows.Close() }()
	i := 0
	for rows.Next() {
		if i >= len(chunk) {
			return fmt.Errorf("batch create chunk: RETURNING produced more rows than input (%d)", len(chunk))
		}
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return fmt.Errorf("batch create chunk scan id: %w", err)
		}
		id, parseErr := uuid.Parse(idStr)
		if parseErr != nil {
			return fmt.Errorf("batch create chunk parse id: %w", parseErr)
		}
		chunk[i].ID = id
		i++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("batch create chunk iter: %w", err)
	}
	if i != len(chunk) {
		return fmt.Errorf("batch create chunk: RETURNING produced %d rows, expected %d", i, len(chunk))
	}
	return nil
}

// fallbackPerRowCreate retries a chunk row-by-row after a tolerable
// multi-row failure. Each row gets its own savepoint so a single bad
// row does not poison the rest of the chunk. Successful rows have
// rel.ID updated via the inner execBatchCreateChunk's RETURNING; skipped
// rows have rel.ID set to uuid.Nil as a sentinel so the caller can
// filter them out when iterating post-batch (e.g. when writing dream-log
// entries that would otherwise reference non-existent rows).
func (r *RelationshipRepo) fallbackPerRowCreate(ctx context.Context, tx *sql.Tx, chunk []*model.Relationship, parentName string) (affected, skipped int64, err error) {
	for j, rel := range chunk {
		rowName := fmt.Sprintf("%s_row_%d", parentName, j)
		outcome, sErr := withSavepoint(ctx, tx, rowName, func() error {
			return r.execBatchCreateChunk(ctx, tx, []*model.Relationship{rel})
		})
		if sErr != nil {
			return affected, skipped, sErr
		}
		if outcome == savepointOK {
			affected++
		} else {
			rel.ID = uuid.Nil
			skipped++
		}
	}
	return affected, skipped, nil
}

// BatchExpire sets valid_until = now() for every id in ids that lives in
// namespaceID. Chunked at relationshipBatchChunkSize and wrapped in one
// transaction so partial-state never leaks. ids not matched by a row
// contribute zero to the returned count (matches Expire's sql.ErrNoRows
// semantics that callers already swallow); they are not counted as a
// distinct skipped class.
func (r *RelationshipRepo) BatchExpire(ctx context.Context, namespaceID uuid.UUID, ids []uuid.UUID) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("relationship batch expire begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	isPg := r.db.Backend() == BackendPostgres
	var total int64
	for i := 0; i < len(ids); i += relationshipBatchChunkSize {
		end := min(i+relationshipBatchChunkSize, len(ids))
		chunk := ids[i:end]
		n, execErr := r.execBatchExpireChunk(ctx, tx, namespaceID, chunk, now, isPg)
		if execErr != nil {
			return 0, execErr
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("relationship batch expire commit: %w", err)
	}
	return total, nil
}

func (r *RelationshipRepo) execBatchExpireChunk(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID, chunk []uuid.UUID, nowStr string, isPg bool) (int64, error) {
	var b strings.Builder
	args := make([]any, 0, 2+len(chunk))
	b.WriteString("UPDATE relationships SET valid_until = ")
	if isPg {
		b.WriteString("$1 WHERE namespace_id = $2 AND id IN (")
	} else {
		b.WriteString("? WHERE namespace_id = ? AND id IN (")
	}
	args = append(args, nowStr, namespaceID.String())
	for j, id := range chunk {
		if j > 0 {
			b.WriteString(", ")
		}
		if isPg {
			fmt.Fprintf(&b, "$%d", j+3)
		} else {
			b.WriteString("?")
		}
		args = append(args, id.String())
	}
	b.WriteString(")")
	res, err := tx.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("batch expire chunk exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("batch expire chunk rows affected: %w", err)
	}
	return n, nil
}

// BatchDeleteByID deletes every id in ids that lives in namespaceID.
// Chunked and wrapped in one transaction.
func (r *RelationshipRepo) BatchDeleteByID(ctx context.Context, namespaceID uuid.UUID, ids []uuid.UUID) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("relationship batch delete begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	isPg := r.db.Backend() == BackendPostgres
	var total int64
	for i := 0; i < len(ids); i += relationshipBatchChunkSize {
		end := min(i+relationshipBatchChunkSize, len(ids))
		chunk := ids[i:end]
		n, execErr := r.execBatchDeleteChunk(ctx, tx, namespaceID, chunk, isPg)
		if execErr != nil {
			return 0, execErr
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("relationship batch delete commit: %w", err)
	}
	return total, nil
}

func (r *RelationshipRepo) execBatchDeleteChunk(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID, chunk []uuid.UUID, isPg bool) (int64, error) {
	var b strings.Builder
	args := make([]any, 0, 1+len(chunk))
	if isPg {
		b.WriteString("DELETE FROM relationships WHERE namespace_id = $1 AND id IN (")
	} else {
		b.WriteString("DELETE FROM relationships WHERE namespace_id = ? AND id IN (")
	}
	args = append(args, namespaceID.String())
	for j, id := range chunk {
		if j > 0 {
			b.WriteString(", ")
		}
		if isPg {
			fmt.Fprintf(&b, "$%d", j+2)
		} else {
			b.WriteString("?")
		}
		args = append(args, id.String())
	}
	b.WriteString(")")
	res, err := tx.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("batch delete chunk exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("batch delete chunk rows affected: %w", err)
	}
	return n, nil
}

// BatchReinforce adds each item's Delta to its row's weight, clamped at
// 2.0 to match the single-row Reinforce ceiling (and dreaming's
// calculateWeight upper clamp). Chunked via UPDATE ... FROM (VALUES ...)
// per chunk in one outer transaction. Returns the count of rows actually
// updated; items naming a missing id contribute zero.
//
// Both dialects support UPDATE ... FROM (VALUES ...): Postgres natively,
// SQLite since 3.33.0 (2020-08); the bundled modernc.org/sqlite driver
// satisfies this floor.
func (r *RelationshipRepo) BatchReinforce(ctx context.Context, namespaceID uuid.UUID, items []model.ReinforceItem) (int64, error) {
	if len(items) == 0 {
		return 0, nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("relationship batch reinforce begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	isPg := r.db.Backend() == BackendPostgres
	var total int64
	for i := 0; i < len(items); i += relationshipBatchChunkSize {
		end := min(i+relationshipBatchChunkSize, len(items))
		chunk := items[i:end]
		n, execErr := r.execBatchReinforceChunk(ctx, tx, namespaceID, chunk, isPg)
		if execErr != nil {
			return 0, execErr
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("relationship batch reinforce commit: %w", err)
	}
	return total, nil
}

func (r *RelationshipRepo) execBatchReinforceChunk(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID, chunk []model.ReinforceItem, isPg bool) (int64, error) {
	var b strings.Builder
	args := make([]any, 0, 1+2*len(chunk))
	if isPg {
		// VALUES with parenthesized alias is standard in Postgres. Cast
		// the VALUES side to uuid (not relationships.id::text) so the
		// PK btree on relationships(id) remains usable for the join;
		// wrapping the indexed column in a cast would force a seq scan.
		b.WriteString("UPDATE relationships SET weight = LEAST(relationships.weight + d.delta, 2.0) FROM (VALUES ")
		for j, item := range chunk {
			if j > 0 {
				b.WriteString(", ")
			}
			base := j*2 + 1
			fmt.Fprintf(&b, "($%d::uuid, $%d::float8)", base, base+1)
			args = append(args, item.ID.String(), item.Delta)
		}
		fmt.Fprintf(&b, ") AS d(id, delta) WHERE relationships.id = d.id AND relationships.namespace_id = $%d", 2*len(chunk)+1)
	} else {
		// SQLite versions prior to 3.39 reject AS d(id, delta); use a
		// SELECT ... UNION ALL subquery to name columns portably.
		b.WriteString("UPDATE relationships SET weight = min(weight + d.delta, 2.0) FROM (")
		for j, item := range chunk {
			if j == 0 {
				b.WriteString("SELECT ? AS id, ? AS delta")
			} else {
				b.WriteString(" UNION ALL SELECT ?, ?")
			}
			args = append(args, item.ID.String(), item.Delta)
		}
		b.WriteString(") AS d WHERE relationships.id = d.id AND relationships.namespace_id = ?")
	}
	args = append(args, namespaceID.String())
	res, err := tx.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("batch reinforce chunk exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("batch reinforce chunk rows affected: %w", err)
	}
	return n, nil
}

// BatchUpdateWeight sets each item's row to its absolute Weight value.
// Same chunked UPDATE ... FROM (VALUES ...) envelope as BatchReinforce.
// Returns the count of rows actually updated.
func (r *RelationshipRepo) BatchUpdateWeight(ctx context.Context, namespaceID uuid.UUID, items []model.WeightUpdateItem) (int64, error) {
	if len(items) == 0 {
		return 0, nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("relationship batch update weight begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	isPg := r.db.Backend() == BackendPostgres
	var total int64
	for i := 0; i < len(items); i += relationshipBatchChunkSize {
		end := min(i+relationshipBatchChunkSize, len(items))
		chunk := items[i:end]
		n, execErr := r.execBatchUpdateWeightChunk(ctx, tx, namespaceID, chunk, isPg)
		if execErr != nil {
			return 0, execErr
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("relationship batch update weight commit: %w", err)
	}
	return total, nil
}

func (r *RelationshipRepo) execBatchUpdateWeightChunk(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID, chunk []model.WeightUpdateItem, isPg bool) (int64, error) {
	var b strings.Builder
	args := make([]any, 0, 1+2*len(chunk))
	if isPg {
		// Cast the VALUES side (not the PK column) so relationships(id)
		// remains sargable. See execBatchReinforceChunk for the same
		// reasoning.
		b.WriteString("UPDATE relationships SET weight = d.weight FROM (VALUES ")
		for j, item := range chunk {
			if j > 0 {
				b.WriteString(", ")
			}
			base := j*2 + 1
			fmt.Fprintf(&b, "($%d::uuid, $%d::float8)", base, base+1)
			args = append(args, item.ID.String(), item.Weight)
		}
		fmt.Fprintf(&b, ") AS d(id, weight) WHERE relationships.id = d.id AND relationships.namespace_id = $%d", 2*len(chunk)+1)
	} else {
		// SQLite portable form: SELECT ... UNION ALL to name columns
		// without relying on AS d(col, col).
		b.WriteString("UPDATE relationships SET weight = d.weight FROM (")
		for j, item := range chunk {
			if j == 0 {
				b.WriteString("SELECT ? AS id, ? AS weight")
			} else {
				b.WriteString(" UNION ALL SELECT ?, ?")
			}
			args = append(args, item.ID.String(), item.Weight)
		}
		b.WriteString(") AS d WHERE relationships.id = d.id AND relationships.namespace_id = ?")
	}
	args = append(args, namespaceID.String())
	res, err := tx.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("batch update weight chunk exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("batch update weight chunk rows affected: %w", err)
	}
	return n, nil
}

// savepointOutcome distinguishes a clean exec from one that failed with
// a tolerable per-row constraint error. Non-tolerable errors are
// returned directly by withSavepoint.
type savepointOutcome int

const (
	savepointOK savepointOutcome = iota
	savepointTolerableErr
)

// withSavepoint wraps fn in a SAVEPOINT / RELEASE pair. On a tolerable
// per-row constraint error (FK or unique), the savepoint is rolled back
// and released and the helper reports savepointTolerableErr so the
// caller can retry row-by-row. On any other error the helper returns
// the error and the outer transaction's deferred Rollback cleans up.
func withSavepoint(ctx context.Context, tx *sql.Tx, name string, fn func() error) (savepointOutcome, error) {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+name); err != nil {
		return 0, fmt.Errorf("savepoint %s: %w", name, err)
	}
	execErr := fn()
	if execErr != nil {
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+name); err != nil {
			return 0, fmt.Errorf("rollback to savepoint %s after %v: %w", name, execErr, err)
		}
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+name); err != nil {
			return 0, fmt.Errorf("release savepoint %s after rollback: %w", name, err)
		}
		if isTolerableRowError(execErr) {
			return savepointTolerableErr, nil
		}
		return 0, execErr
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+name); err != nil {
		return 0, fmt.Errorf("release savepoint %s: %w", name, err)
	}
	return savepointOK, nil
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
