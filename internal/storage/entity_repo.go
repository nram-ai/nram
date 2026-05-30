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
	db          DB
	vectorStore VectorStore
}

// NewEntityRepo creates a new EntityRepo backed by the given DB.
func NewEntityRepo(db DB) *EntityRepo {
	return &EntityRepo{db: db}
}

// SetVectorStore wires an optional VectorStore for opportunistic vector
// cleanup on internal entity deletes (currently: stub deletion inside
// promoteStub). Bulk-delete paths return their deleted IDs to the caller and
// do not depend on this field. Safe to call once at construction time before
// the repo is shared across goroutines.
func (r *EntityRepo) SetVectorStore(vs VectorStore) {
	r.vectorStore = vs
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

	var embeddingDim any
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

	var embeddingDim any
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

	// Best-effort cleanup of the stub's vector. SQL-backed stores (pgvector,
	// HNSW) cascade via entity_vectors; the call is a no-op there. Qdrant has
	// no SQL FK to the entities table, so without this the stub's points are
	// leaked indefinitely in the entity-vectors collections.
	if r.vectorStore != nil {
		_ = r.vectorStore.Delete(ctx, VectorKindEntity, stub.ID)
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

	args := make([]any, 0, len(ids)+2)
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

// CountWithEmbeddingDim returns the number of entities that currently
// have a non-NULL embedding_dim.
func (r *EntityRepo) CountWithEmbeddingDim(ctx context.Context) (int64, error) {
	query := `SELECT COUNT(*) FROM entities WHERE embedding_dim IS NOT NULL`
	var n int64
	if err := r.db.QueryRow(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("entity count with embedding_dim: %w", err)
	}
	return n, nil
}

// ClearAllEmbeddingDims sets embedding_dim = NULL for every entity.
// Returns the count of rows affected.
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

// FindBySimilarity finds entities whose name contains the given string as
// a case-insensitive substring within the same namespace. If kind is
// non-empty, results are filtered to that entity type. Ordered by
// mention_count DESC, created_at DESC.
//
// **Semantics are literal substring.** A multi-word query like "John Doe"
// matches only entities whose name literally contains "John Doe" — it
// does NOT split into tokens. This is the contract every internal caller
// (enrichment dedup, recall context build, dreaming) was written against:
// canonical names are pre-normalised (`canonicalize` in
// `internal/enrichment/entity_resolution.go`), and the call asks "does an
// entity with this canonical exist." For agent-supplied free-form
// queries that need tokenized + alias-aware search, use
// `SearchEntities` instead.
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
	defer func() { _ = rows.Close() }()

	return r.scanEntities(rows)
}

// SearchEntities is the free-form-agent-query counterpart to
// FindBySimilarity. It tokenizes the query on whitespace and, for
// multi-token inputs, ORs LIKE clauses across tokens against both
// `entities.name` AND `entity_aliases.alias`, ranking rows by the number
// of distinct name-token matches. Single-token (or empty) queries fall
// through to `FindBySimilarity` so the result set is identical to a
// literal call — there's no token-OR to do.
//
// Use this for agent-supplied queries (e.g. the MCP `graph` tool's
// `entity` argument). Do NOT use it from programmatic / canonical paths
// (enrichment Resolve, dreaming consolidation): the broader match shape
// will pull in unrelated entities sharing a single token, which the
// canonical-dedup logic then treats as fuzzy matches and aliases
// incorrectly. Those callers belong on `FindBySimilarity`.
//
// Scoring rule for the multi-token branch: ORDER BY name-token-match-count
// DESC, mention_count DESC, created_at DESC. Alias matches surface a row
// but do not contribute to the score axis — name matches are higher
// signal than alias matches, so an entity matched only via alias loses
// the score-tier tiebreak to one matched via name and falls back to
// mention_count ordering. This avoids a second per-row EXISTS subquery
// in the ORDER BY (the WHERE clause already runs it once) at the cost
// of one tiebreak rule.
func (r *EntityRepo) SearchEntities(ctx context.Context, namespaceID uuid.UUID, query string, kind string, limit int) ([]model.Entity, error) {
	tokens := splitQueryTokens(query)
	if len(tokens) <= 1 {
		return r.FindBySimilarity(ctx, namespaceID, query, kind, limit)
	}
	return r.searchEntitiesMultiToken(ctx, namespaceID, tokens, kind, limit)
}

// splitQueryTokens trims and whitespace-splits a query into non-empty
// tokens. Returns nil for an empty/whitespace-only input. Used by
// SearchEntities to branch between the single-token fallback (delegating
// to FindBySimilarity) and the multi-token tokenized matcher.
func splitQueryTokens(name string) []string {
	fields := strings.Fields(name)
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != "" {
			out = append(out, f)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// searchEntitiesMultiToken implements the multi-token branch of
// SearchEntities. SQL shape (Postgres positional, SQLite uses ?):
//
//	SELECT <cols> FROM entities e
//	WHERE e.namespace_id = $1
//	  [AND e.entity_type = $K]
//	  AND (
//	    e.name ILIKE $tN1 OR e.name ILIKE $tN2 OR ...
//	    OR EXISTS (SELECT 1 FROM entity_aliases ea
//	               WHERE ea.entity_id = e.id
//	                 AND (ea.alias ILIKE $tA1 OR ea.alias ILIKE $tA2 OR ...))
//	  )
//	ORDER BY
//	  ((CASE WHEN e.name ILIKE $tS1 THEN 1 ELSE 0 END)
//	 + (CASE WHEN e.name ILIKE $tS2 THEN 1 ELSE 0 END) + ...) DESC,
//	  e.mention_count DESC, e.created_at DESC
//	LIMIT $L
//
// Each token's pattern is bound THREE times (name-WHERE, alias-WHERE,
// score-ORDER-BY) so the bind positions need to be carefully tracked.
// The builder emits args in lockstep with placeholder generation so a
// reordering or new token-position would only need updates in one
// place.
func (r *EntityRepo) searchEntitiesMultiToken(ctx context.Context, namespaceID uuid.UUID, tokens []string, kind string, limit int) ([]model.Entity, error) {
	isPg := r.db.Backend() == BackendPostgres

	patterns := make([]string, len(tokens))
	for i, t := range tokens {
		patterns[i] = "%" + t + "%"
	}

	var sb strings.Builder
	sb.WriteString(selectEntityColumns)
	sb.WriteString(` FROM entities e WHERE `)

	args := make([]any, 0, 3*len(patterns)+3)

	ph := func() string {
		if isPg {
			return fmt.Sprintf("$%d", len(args))
		}
		return "?"
	}

	args = append(args, namespaceID.String())
	if isPg {
		sb.WriteString(`e.namespace_id = $1`)
	} else {
		sb.WriteString(`e.namespace_id = ?`)
	}

	if kind != "" {
		args = append(args, kind)
		sb.WriteString(` AND e.entity_type = `)
		sb.WriteString(ph())
	}

	// Name LIKE clauses.
	sb.WriteString(` AND (`)
	nameOp := "LIKE"
	if isPg {
		nameOp = "ILIKE"
	}
	for i, pat := range patterns {
		args = append(args, pat)
		if i > 0 {
			sb.WriteString(" OR ")
		}
		fmt.Fprintf(&sb, "e.name %s %s", nameOp, ph())
		if !isPg {
			sb.WriteString(" COLLATE NOCASE")
		}
	}

	// Alias EXISTS clause.
	sb.WriteString(` OR EXISTS (SELECT 1 FROM entity_aliases ea WHERE ea.entity_id = e.id AND (`)
	for i, pat := range patterns {
		args = append(args, pat)
		if i > 0 {
			sb.WriteString(" OR ")
		}
		fmt.Fprintf(&sb, "ea.alias %s %s", nameOp, ph())
		if !isPg {
			sb.WriteString(" COLLATE NOCASE")
		}
	}
	sb.WriteString(`))`)

	sb.WriteString(`) ORDER BY (`)

	// Score: sum of name-LIKE CASE WHEN per token. Alias matches are NOT
	// scored — they surface via WHERE but tiebreak via mention_count.
	for i, pat := range patterns {
		args = append(args, pat)
		if i > 0 {
			sb.WriteString(" + ")
		}
		fmt.Fprintf(&sb, "(CASE WHEN e.name %s %s", nameOp, ph())
		if !isPg {
			sb.WriteString(" COLLATE NOCASE")
		}
		sb.WriteString(" THEN 1 ELSE 0 END)")
	}
	sb.WriteString(`) DESC, e.mention_count DESC, e.created_at DESC LIMIT `)
	args = append(args, limit)
	sb.WriteString(ph())

	rows, err := r.db.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("entity search (multi-token): %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = rows.Close() }()

	return r.scanEntities(rows)
}

// GetBatch returns multiple entities by their UUIDs in a single query. Missing
// IDs are silently dropped — callers are responsible for diffing the result
// against the input list if they need to detect them. Order is not preserved.
func (r *EntityRepo) GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Entity, error) {
	if len(ids) == 0 {
		return []model.Entity{}, nil
	}

	placeholders, args := uuidInPlaceholders(r.db, ids, 1)
	query := selectEntityColumns + ` FROM entities WHERE id IN (` +
		strings.Join(placeholders, ", ") + `)`

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("entity get batch: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = rows.Close() }()

	return r.scanEntities(rows)
}

// ListAll returns a page of entities across every namespace, id-ordered
// for stable pagination. limit=0 uses 500.
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
	defer func() { _ = rows.Close() }()
	return r.scanEntities(rows)
}

// scanReturnedUUIDs drains a *sql.Rows whose only column is a UUID-shaped
// TEXT/UUID value (the typical RETURNING id payload), parsing each into a
// uuid.UUID. errPrefix is woven into wrapped errors so the caller's site
// stays attributable.
func scanReturnedUUIDs(rows *sql.Rows, errPrefix string) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, fmt.Errorf("%s scan: %w", errPrefix, err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("%s parse id: %w", errPrefix, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s rows: %w", errPrefix, err)
	}
	return ids, nil
}

// DeleteByNamespaceTx deletes all entities (and their cascaded aliases /
// relationships / entity_vectors) in a namespace inside the caller's
// transaction. The schema (post-000032 / 000035) cascades aliases and the
// relationships endpoints into entities; we no longer need to pre-delete
// aliases here. Returns the IDs of deleted entities so the caller can clean
// up out-of-band vector storage (Qdrant) after the transaction commits.
func (r *EntityRepo) DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) ([]uuid.UUID, error) {
	query := `DELETE FROM entities WHERE namespace_id = ? RETURNING id`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM entities WHERE namespace_id = $1 RETURNING id`
	}
	rows, err := tx.QueryContext(ctx, query, namespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("entity delete by namespace: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanReturnedUUIDs(rows, "entity delete by namespace")
}

// DeleteOrphaned removes entities that have no relationships (neither as
// source nor target) AND were created at or before olderThan. The age cutoff
// protects in-flight enrichment: a job that just upserted an entity but has
// not yet written its relationships (or has not yet upserted the entity's
// vector) must not have its row pulled out from under it. Callers pick
// olderThan = now - grace, where grace must exceed the longest plausible
// gap between entity Upsert and vector UpsertBatch (LLM embed round-trip,
// queue dispatch, etc.). Returns the IDs of deleted entities so the caller
// can clean up out-of-band vector storage (Qdrant) atomically with the SQL
// delete - the slice is exactly what the DELETE acted on, no race window.
func (r *EntityRepo) DeleteOrphaned(ctx context.Context, olderThan time.Time) ([]uuid.UUID, error) {
	cutoff := olderThan.UTC().Format(time.RFC3339)
	query := `DELETE FROM entities WHERE created_at <= ? AND id NOT IN (
		SELECT source_id FROM relationships
		UNION
		SELECT target_id FROM relationships
	) RETURNING id`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM entities WHERE created_at <= $1 AND id NOT IN (
			SELECT source_id FROM relationships
			UNION
			SELECT target_id FROM relationships
		) RETURNING id`
	}
	rows, err := r.db.WriteQuery(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("entity delete orphaned: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanReturnedUUIDs(rows, "entity delete orphaned")
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
