package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// HashContent returns the canonical sha256 hex digest used for exact-content
// dedup at ingest. Callers that want to look up potential duplicates without
// going through Create can use this directly.
func HashContent(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// MemoryRepo provides CRUD operations for the memories table.
type MemoryRepo struct {
	db          DB
	vectorStore VectorStore
}

// NewMemoryRepo creates a new MemoryRepo backed by the given DB.
func NewMemoryRepo(db DB) *MemoryRepo {
	return &MemoryRepo{db: db}
}

// AttachVectorStore wires a VectorStore into the repo so soft-delete and
// hard-delete also purge the associated vector. Passing nil disables the
// hook. Best-effort: Delete errors are swallowed because the row-level
// state change is the load-bearing invariant; a stale vector will cost
// some HNSW/pgvector search cycles until the next retention sweep at
// worst.
func (r *MemoryRepo) AttachVectorStore(vs VectorStore) {
	r.vectorStore = vs
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
	// Fill zero timestamps from Go so the caller's struct matches the DB row
	// without a reload SELECT after INSERT.
	now := time.Now().UTC()
	if mem.CreatedAt.IsZero() {
		mem.CreatedAt = now
	}
	if mem.UpdatedAt.IsZero() {
		mem.UpdatedAt = now
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

	var supersededAt interface{}
	if mem.SupersededAt != nil {
		supersededAt = mem.SupersededAt.UTC().Format(time.RFC3339)
	}

	if mem.ContentHash == "" {
		mem.ContentHash = HashContent(mem.Content)
	}

	var purgeAfter interface{}
	if mem.PurgeAfter != nil {
		purgeAfter = mem.PurgeAfter.UTC().Format(time.RFC3339)
	}

	enrichedVal := encodeBool(r.db.Backend(), mem.Enriched)
	createdAtStr := mem.CreatedAt.UTC().Format(time.RFC3339)
	updatedAtStr := mem.UpdatedAt.UTC().Format(time.RFC3339)

	query := `INSERT INTO memories (id, namespace_id, content, content_hash, embedding_dim, source, tags,
		confidence, importance, access_count, last_accessed, expires_at, superseded_by, superseded_at,
		enriched, metadata, purge_after, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO memories (id, namespace_id, content, content_hash, embedding_dim, source, tags,
			confidence, importance, access_count, last_accessed, expires_at, superseded_by, superseded_at,
			enriched, metadata, purge_after, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`
	}

	_, err := r.db.Exec(ctx, query,
		mem.ID.String(), mem.NamespaceID.String(), mem.Content, mem.ContentHash,
		embeddingDim, source, tagsVal, mem.Confidence, mem.Importance, mem.AccessCount,
		lastAccessed, expiresAt, supersededBy, supersededAt, enrichedVal, string(mem.Metadata), purgeAfter,
		createdAtStr, updatedAtStr,
	)
	if err != nil {
		return fmt.Errorf("memory create: %w", err)
	}

	return nil
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

// LookupByContentHash returns the live memory in the namespace that matches the
// given sha256 content hash, or sql.ErrNoRows if none exists. The index is
// non-unique (legacy duplicates exist), so LIMIT 1 keeps behavior deterministic.
func (r *MemoryRepo) LookupByContentHash(ctx context.Context, namespaceID uuid.UUID, hash string) (*model.Memory, error) {
	query := selectMemoryColumns + ` FROM memories
		WHERE namespace_id = ? AND content_hash = ? AND deleted_at IS NULL
		ORDER BY created_at ASC LIMIT 1`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories
			WHERE namespace_id = $1 AND content_hash = $2 AND deleted_at IS NULL
			ORDER BY created_at ASC LIMIT 1`
	}

	row := r.db.QueryRow(ctx, query, namespaceID.String(), hash)
	return r.scanMemory(row)
}

// BackfillContentHashes populates content_hash for up to batchSize live rows
// where the column is NULL. Returns the number of rows updated. Callers loop
// until 0 to drain. Idempotent: rows that already have a hash are skipped by
// the WHERE clause.
func (r *MemoryRepo) BackfillContentHashes(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	selectQuery := `SELECT id, content FROM memories
		WHERE content_hash IS NULL AND deleted_at IS NULL
		LIMIT ?`
	if r.db.Backend() == BackendPostgres {
		selectQuery = `SELECT id, content FROM memories
			WHERE content_hash IS NULL AND deleted_at IS NULL
			LIMIT $1`
	}

	rows, err := r.db.Query(ctx, selectQuery, batchSize)
	if err != nil {
		return 0, fmt.Errorf("backfill select: %w", err)
	}

	type pending struct {
		id      string
		content string
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.content); err != nil {
			rows.Close()
			return 0, fmt.Errorf("backfill scan: %w", err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("backfill iter: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	updateQuery := `UPDATE memories SET content_hash = ?
		WHERE id = ? AND content_hash IS NULL`
	if r.db.Backend() == BackendPostgres {
		updateQuery = `UPDATE memories SET content_hash = $1
			WHERE id = $2 AND content_hash IS NULL`
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("backfill begin: %w", err)
	}
	processed := 0
	for _, p := range batch {
		if _, err := tx.ExecContext(ctx, updateQuery, HashContent(p.content), p.id); err != nil {
			_ = tx.Rollback()
			return processed, fmt.Errorf("backfill update %s: %w", p.id, err)
		}
		processed++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("backfill commit: %w", err)
	}
	return processed, nil
}

// uuidPlaceholders returns N placeholders and stringified UUIDs for an IN-list.
// startIndex is the first Postgres placeholder number ($N); it is ignored for
// SQLite (which always uses "?"). Returned ids are stringified so callers can
// append directly to the Exec/Query args slice.
func (r *MemoryRepo) uuidPlaceholders(ids []uuid.UUID, startIndex int) ([]string, []interface{}) {
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		if r.db.Backend() == BackendPostgres {
			placeholders[i] = fmt.Sprintf("$%d", startIndex+i)
		} else {
			placeholders[i] = "?"
		}
		args[i] = id.String()
	}
	return placeholders, args
}

// GetBatch returns multiple memories by their UUIDs. Soft-deleted records are excluded.
func (r *MemoryRepo) GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	if len(ids) == 0 {
		return []model.Memory{}, nil
	}

	placeholders, args := r.uuidPlaceholders(ids, 1)

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

// MemoryListFilters narrows a memory listing. All fields are optional; the
// zero value means "no filter on this dimension". Tag matching uses AND
// semantics — a memory must contain ALL listed tags. Source and Search are
// case-insensitive substring matches against the source and content columns
// respectively. Tag SQL is backend-specific because SQLite stores tags as a
// JSON-encoded TEXT column and Postgres stores them as TEXT[].
type MemoryListFilters struct {
	Tags     []string
	DateFrom *time.Time
	DateTo   *time.Time
	// Enriched is a tri-state filter: nil = no filter, *true = enriched only,
	// *false = not-enriched only.
	Enriched *bool
	Source   string
	Search   string
}

// IsZero reports whether no filter dimensions are active.
func (f MemoryListFilters) IsZero() bool {
	return len(f.Tags) == 0 && f.DateFrom == nil && f.DateTo == nil &&
		f.Enriched == nil && f.Source == "" && f.Search == ""
}

// whereBuilder accumulates WHERE clause fragments and their bind values while
// generating backend-appropriate placeholders ($N for Postgres, ? for SQLite).
type whereBuilder struct {
	postgres bool
	clauses  []string
	args     []interface{}
}

func (w *whereBuilder) placeholder() string {
	if w.postgres {
		return fmt.Sprintf("$%d", len(w.args)+1)
	}
	return "?"
}

func (w *whereBuilder) add(clauseFmt string, value interface{}) {
	w.clauses = append(w.clauses, fmt.Sprintf(clauseFmt, w.placeholder()))
	w.args = append(w.args, value)
}

func (w *whereBuilder) where() string {
	return strings.Join(w.clauses, " AND ")
}

// buildFilterWhere produces a WHERE clause + arg slice for the given filters,
// always anchored on namespace_id and deleted_at IS NULL. The returned args
// can be appended with limit/offset/etc. as needed.
func (r *MemoryRepo) buildFilterWhere(namespaceID uuid.UUID, filters MemoryListFilters) (string, []interface{}) {
	wb := &whereBuilder{postgres: r.db.Backend() == BackendPostgres}
	wb.add("namespace_id = %s", namespaceID.String())
	wb.clauses = append(wb.clauses, "deleted_at IS NULL")

	// Tag filter — AND semantics.
	if len(filters.Tags) > 0 {
		if wb.postgres {
			// Build ARRAY[$n, $n+1, ...]::text[] and use the @> contains operator.
			placeholders := make([]string, len(filters.Tags))
			for i, t := range filters.Tags {
				placeholders[i] = wb.placeholder()
				wb.args = append(wb.args, t)
			}
			wb.clauses = append(wb.clauses,
				fmt.Sprintf("tags @> ARRAY[%s]::text[]", strings.Join(placeholders, ",")))
		} else {
			// SQLite: tags is a JSON-encoded TEXT column. Match each tag with a
			// LIKE against the JSON string-quoted form.
			for _, t := range filters.Tags {
				wb.add(`tags LIKE %s ESCAPE '\'`, `%"`+escapeLike(t)+`"%`)
			}
		}
	}

	if filters.DateFrom != nil {
		wb.add("created_at >= %s", filters.DateFrom.UTC().Format(time.RFC3339))
	}
	if filters.DateTo != nil {
		wb.add("created_at < %s", filters.DateTo.UTC().Format(time.RFC3339))
	}

	if filters.Enriched != nil {
		wb.add("enriched = %s", encodeBool(r.db.Backend(), *filters.Enriched))
	}

	if filters.Source != "" {
		wb.add(`LOWER(COALESCE(source, '')) LIKE %s ESCAPE '\'`, "%"+strings.ToLower(escapeLike(filters.Source))+"%")
	}

	if filters.Search != "" {
		wb.add(`LOWER(content) LIKE %s ESCAPE '\'`, "%"+strings.ToLower(escapeLike(filters.Search))+"%")
	}

	return wb.where(), wb.args
}

// ListByNamespace returns memories in a namespace, paginated, ordered by created_at DESC.
// Soft-deleted records are excluded. This is a thin wrapper around
// ListByNamespaceFiltered with no filters set.
func (r *MemoryRepo) ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error) {
	return r.ListByNamespaceFiltered(ctx, namespaceID, MemoryListFilters{}, limit, offset)
}

// ListByNamespaceFiltered returns memories in a namespace narrowed by the
// given filters, paginated and ordered by created_at DESC. Soft-deleted
// records are always excluded.
func (r *MemoryRepo) ListByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters MemoryListFilters, limit, offset int) ([]model.Memory, error) {
	where, args := r.buildFilterWhere(namespaceID, filters)

	limitPH := "?"
	offsetPH := "?"
	if r.db.Backend() == BackendPostgres {
		limitPH = fmt.Sprintf("$%d", len(args)+1)
		offsetPH = fmt.Sprintf("$%d", len(args)+2)
	}
	args = append(args, limit, offset)

	query := selectMemoryColumns + ` FROM memories WHERE ` + where +
		` ORDER BY created_at DESC LIMIT ` + limitPH + ` OFFSET ` + offsetPH

	rows, err := r.db.Query(ctx, query, args...)
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
// Thin wrapper around CountByNamespaceFiltered with no filters set.
func (r *MemoryRepo) CountByNamespace(ctx context.Context, namespaceID uuid.UUID) (int, error) {
	return r.CountByNamespaceFiltered(ctx, namespaceID, MemoryListFilters{})
}

// CountByNamespaceFiltered returns the count of non-deleted memories in a
// namespace that match the given filters.
func (r *MemoryRepo) CountByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters MemoryListFilters) (int, error) {
	where, args := r.buildFilterWhere(namespaceID, filters)
	query := `SELECT COUNT(*) FROM memories WHERE ` + where
	row := r.db.QueryRow(ctx, query, args...)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("memory count by namespace: %w", err)
	}
	return count, nil
}

// ListIDsByNamespaceFiltered returns up to maxIDs memory IDs matching the
// given filters, ordered by created_at DESC. Used by the admin UI to power
// "select all matching" affordances. The cap exists to bound response size;
// callers can detect truncation by comparing the returned length against the
// total count from CountByNamespaceFiltered.
func (r *MemoryRepo) ListIDsByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters MemoryListFilters, maxIDs int) ([]uuid.UUID, error) {
	if maxIDs <= 0 {
		return []uuid.UUID{}, nil
	}
	where, args := r.buildFilterWhere(namespaceID, filters)

	limitPH := "?"
	if r.db.Backend() == BackendPostgres {
		limitPH = fmt.Sprintf("$%d", len(args)+1)
	}
	args = append(args, maxIDs)

	query := `SELECT id FROM memories WHERE ` + where +
		` ORDER BY created_at DESC LIMIT ` + limitPH

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory list ids by namespace: %w", err)
	}
	defer rows.Close()

	result := []uuid.UUID{}
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, fmt.Errorf("memory list ids scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("memory list ids parse: %w", err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory list ids iteration: %w", err)
	}
	return result, nil
}

// ClearAllEmbeddingDims sets embedding_dim = NULL for every live row in
// the memories table. Used by the embedding-model switch cascade so the
// enrichment worker treats every memory as needing fresh vectors. Returns
// the count of rows affected.
func (r *MemoryRepo) ClearAllEmbeddingDims(ctx context.Context) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories SET embedding_dim = NULL, updated_at = ? WHERE embedding_dim IS NOT NULL AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET embedding_dim = NULL, updated_at = $1 WHERE embedding_dim IS NOT NULL AND deleted_at IS NULL`
	}
	res, err := r.db.Exec(ctx, query, now)
	if err != nil {
		return 0, fmt.Errorf("memory clear all embedding_dim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("memory clear all embedding_dim: rows affected: %w", err)
	}
	return n, nil
}

// UpdateEmbeddingDim sets a memory's embedding_dim without rewriting every
// other column. Used by the enrichment worker to record the dim that a
// child memory's vector was written at.
func (r *MemoryRepo) UpdateEmbeddingDim(ctx context.Context, id uuid.UUID, dim int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories SET embedding_dim = ?, updated_at = ? WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET embedding_dim = $1, updated_at = $2 WHERE id = $3`
	}
	if _, err := r.db.Exec(ctx, query, dim, now, id.String()); err != nil {
		return fmt.Errorf("memory update embedding_dim: %w", err)
	}
	return nil
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

	var supersededAt interface{}
	if mem.SupersededAt != nil {
		supersededAt = mem.SupersededAt.UTC().Format(time.RFC3339)
	}

	// Recompute content hash on update so in-place content edits stay truthful.
	mem.ContentHash = HashContent(mem.Content)

	var purgeAfter interface{}
	if mem.PurgeAfter != nil {
		purgeAfter = mem.PurgeAfter.UTC().Format(time.RFC3339)
	}

	enrichedVal := encodeBool(r.db.Backend(), mem.Enriched)

	query := `UPDATE memories SET content = ?, content_hash = ?, embedding_dim = ?, source = ?, tags = ?,
		confidence = ?, importance = ?, access_count = ?, last_accessed = ?,
		expires_at = ?, superseded_by = ?, superseded_at = ?, enriched = ?, metadata = ?,
		purge_after = ?, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET content = $1, content_hash = $2, embedding_dim = $3, source = $4, tags = $5,
			confidence = $6, importance = $7, access_count = $8, last_accessed = $9,
			expires_at = $10, superseded_by = $11, superseded_at = $12, enriched = $13, metadata = $14,
			purge_after = $15, updated_at = $16
			WHERE id = $17 AND namespace_id = $18 AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query,
		mem.Content, mem.ContentHash, embeddingDim, source, tagsVal,
		mem.Confidence, mem.Importance, mem.AccessCount, lastAccessed,
		expiresAt, supersededBy, supersededAt, enrichedVal, string(mem.Metadata),
		purgeAfter, now, mem.ID.String(), mem.NamespaceID.String(),
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

// BumpReinforcement atomically bumps access_count, last_accessed, and
// multiplicatively nudges confidence (capped at 1.0) for the given memory IDs
// that are not soft-deleted. factor is the multiplicative reinforcement term:
// confidence becomes MIN(1.0, confidence * (1.0 + factor)). Unknown IDs and
// soft-deleted rows are silently skipped. Returns the number of rows updated.
//
// This is the read-path write used by reconsolidation: every recall that
// surfaces a memory nudges these three fields asynchronously so memories the
// system actually uses accumulate real signal, and memories it does not use
// fade under the complementary decay performed by the pruning phase.
func (r *MemoryRepo) BumpReinforcement(ctx context.Context, ids []uuid.UUID, now time.Time, factor float64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	nowStr := now.UTC().Format(time.RFC3339)

	// Two fixed args come first (last_accessed, factor). IDs follow starting at $3.
	placeholders, idArgs := r.uuidPlaceholders(ids, 3)
	args := make([]interface{}, 0, 2+len(ids))
	args = append(args, nowStr, factor)
	args = append(args, idArgs...)

	var query string
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories
			SET access_count = access_count + 1,
			    last_accessed = $1,
			    confidence = LEAST(1.0, confidence * (1.0 + $2))
			WHERE id IN (` + strings.Join(placeholders, ", ") + `) AND deleted_at IS NULL`
	} else {
		query = `UPDATE memories
			SET access_count = access_count + 1,
			    last_accessed = ?,
			    confidence = MIN(1.0, confidence * (1.0 + ?))
			WHERE id IN (` + strings.Join(placeholders, ", ") + `) AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("memory bump reinforcement: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("memory bump reinforcement rows affected: %w", err)
	}
	return rows, nil
}

// DecayConfidence multiplicatively scales confidence for the given memory IDs,
// clamped to the given floor. multiplier should be in (0, 1] — values less
// than 1 shrink confidence, 1 is a no-op. Soft-deleted rows are skipped.
// Returns rows updated.
//
// Used by the dreaming pruning phase to make idle memories fade, complementing
// the read-path reinforcement performed by BumpReinforcement.
func (r *MemoryRepo) DecayConfidence(ctx context.Context, ids []uuid.UUID, multiplier, floor float64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	// Arg layout (both backends): (floor, multiplier, ...ids). Postgres's
	// GREATEST and SQLite's MAX are both variadic scalar functions returning
	// the largest argument, so the SQL reads the same way with matching
	// placeholder positions.
	placeholders, idArgs := r.uuidPlaceholders(ids, 3)
	args := make([]interface{}, 0, 2+len(ids))
	args = append(args, floor, multiplier)
	args = append(args, idArgs...)

	var query string
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories
			SET confidence = GREATEST($1, confidence * $2)
			WHERE id IN (` + strings.Join(placeholders, ", ") + `) AND deleted_at IS NULL`
	} else {
		query = `UPDATE memories
			SET confidence = MAX(?, confidence * ?)
			WHERE id IN (` + strings.Join(placeholders, ", ") + `) AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("memory decay confidence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("memory decay confidence rows affected: %w", err)
	}
	return rows, nil
}

// SoftDelete sets the deleted_at timestamp on a memory and purges the
// associated vector from the attached vector store (if any). The SQL row
// is retained so rollback and retention windows remain intact, but it is
// excluded from recall via the deleted_at IS NULL filter everywhere. Vector
// purge errors are not propagated — the row-level state change is the
// load-bearing invariant; a stale vector will cost some HNSW/pgvector
// search cycles until the next retention sweep at worst.
func (r *MemoryRepo) SoftDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE memories SET deleted_at = ?, updated_at = ? WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET deleted_at = $1, updated_at = $2 WHERE id = $3 AND namespace_id = $4 AND deleted_at IS NULL`
	}

	result, err := r.db.Exec(ctx, query, now, now, id.String(), namespaceID.String())
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
	r.purgeVector(ctx, id)
	return nil
}

// HardDelete permanently removes a memory from the table. The memory_vectors_*
// FK cascades the persisted vector row; this call also fires the attached
// vector store's Delete so in-memory indexes (HNSW) drop the node.
func (r *MemoryRepo) HardDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error {
	query := `DELETE FROM memories WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM memories WHERE id = $1 AND namespace_id = $2`
	}

	_, err := r.db.Exec(ctx, query, id.String(), namespaceID.String())
	if err != nil {
		return fmt.Errorf("memory hard delete: %w", err)
	}
	r.purgeVector(ctx, id)
	return nil
}

// purgeVector asks the attached vector store to drop the row-level vector
// and the in-memory HNSW graph node (if any). No-op when no vector store
// is attached. Errors are swallowed so they cannot stall the row-level
// lifecycle — the vector store is an index, not the source of truth.
func (r *MemoryRepo) purgeVector(ctx context.Context, id uuid.UUID) {
	if r.vectorStore == nil {
		return
	}
	_ = r.vectorStore.Delete(ctx, VectorKindMemory, id)
}

// HardDeleteSoftDeletedBefore hard-deletes rows whose deleted_at is older
// than cutoff, up to limit rows per call. Vector rows cascade via the
// memory_vectors_* ON DELETE CASCADE constraint. Returns the number of
// rows removed. A non-positive limit means "no cap" (caller-bounded).
func (r *MemoryRepo) HardDeleteSoftDeletedBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	cutoffStr := cutoff.UTC().Format(time.RFC3339)
	pg := r.db.Backend() == BackendPostgres

	cutoffPh, limitPh := "?", "?"
	if pg {
		cutoffPh, limitPh = "$1", "$2"
	}

	args := []interface{}{cutoffStr}
	inner := `SELECT id FROM memories WHERE deleted_at IS NOT NULL AND deleted_at < ` + cutoffPh
	if limit > 0 {
		inner += ` LIMIT ` + limitPh
		args = append(args, limit)
	}
	query := `DELETE FROM memories WHERE id IN (` + inner + `)`

	result, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("memory hard delete soft-deleted before: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("memory hard delete soft-deleted before rows affected: %w", err)
	}
	return rows, nil
}

// ListIDsByNamespace returns all non-deleted memory IDs in a namespace.
func (r *MemoryRepo) ListIDsByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]uuid.UUID, error) {
	query := `SELECT id FROM memories WHERE namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id FROM memories WHERE namespace_id = $1 AND deleted_at IS NULL`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("memory list ids by namespace: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, fmt.Errorf("memory list ids by namespace scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("memory list ids by namespace parse: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory list ids by namespace iteration: %w", err)
	}
	return ids, nil
}

// HardDeleteByNamespace permanently deletes all memories in a namespace.
func (r *MemoryRepo) HardDeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error {
	query := `DELETE FROM memories WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM memories WHERE namespace_id = $1`
	}

	_, err := r.db.Exec(ctx, query, namespaceID.String())
	if err != nil {
		return fmt.Errorf("memory hard delete by namespace: %w", err)
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
	superseded_at, enriched, metadata, content_hash, created_at, updated_at, deleted_at, purge_after`

func (r *MemoryRepo) scanMemory(row *sql.Row) (*model.Memory, error) {
	var mem model.Memory
	var idStr, namespaceIDStr string
	var tagsStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var embeddingDim sql.NullInt64
	var source sql.NullString
	var lastAccessedStr, expiresAtStr, deletedAtStr, purgeAfterStr sql.NullString
	var supersededByStr, supersededAtStr, contentHashStr sql.NullString
	var enrichedBool bool

	err := row.Scan(
		&idStr, &namespaceIDStr, &mem.Content, &embeddingDim, &source, &tagsStr,
		&mem.Confidence, &mem.Importance, &mem.AccessCount, &lastAccessedStr,
		&expiresAtStr, &supersededByStr, &supersededAtStr, &enrichedBool, &metadataStr,
		&contentHashStr, &createdAtStr, &updatedAtStr, &deletedAtStr, &purgeAfterStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateMemory(&mem, idStr, namespaceIDStr, tagsStr, metadataStr,
		createdAtStr, updatedAtStr, embeddingDim, source, lastAccessedStr,
		expiresAtStr, supersededByStr, supersededAtStr, contentHashStr,
		enrichedBool, deletedAtStr, purgeAfterStr)
}

func (r *MemoryRepo) scanMemoryFromRows(rows *sql.Rows) (*model.Memory, error) {
	var mem model.Memory
	var idStr, namespaceIDStr string
	var tagsStr, metadataStr string
	var createdAtStr, updatedAtStr string
	var embeddingDim sql.NullInt64
	var source sql.NullString
	var lastAccessedStr, expiresAtStr, deletedAtStr, purgeAfterStr sql.NullString
	var supersededByStr, supersededAtStr, contentHashStr sql.NullString
	var enrichedBool bool

	err := rows.Scan(
		&idStr, &namespaceIDStr, &mem.Content, &embeddingDim, &source, &tagsStr,
		&mem.Confidence, &mem.Importance, &mem.AccessCount, &lastAccessedStr,
		&expiresAtStr, &supersededByStr, &supersededAtStr, &enrichedBool, &metadataStr,
		&contentHashStr, &createdAtStr, &updatedAtStr, &deletedAtStr, &purgeAfterStr,
	)
	if err != nil {
		return nil, fmt.Errorf("memory scan rows: %w", err)
	}

	return r.populateMemory(&mem, idStr, namespaceIDStr, tagsStr, metadataStr,
		createdAtStr, updatedAtStr, embeddingDim, source, lastAccessedStr,
		expiresAtStr, supersededByStr, supersededAtStr, contentHashStr,
		enrichedBool, deletedAtStr, purgeAfterStr)
}

func (r *MemoryRepo) populateMemory(
	mem *model.Memory,
	idStr, namespaceIDStr, tagsStr, metadataStr, createdAtStr, updatedAtStr string,
	embeddingDim sql.NullInt64,
	source, lastAccessedStr, expiresAtStr, supersededByStr, supersededAtStr, contentHashStr sql.NullString,
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

	if supersededAtStr.Valid {
		t, err := time.Parse(time.RFC3339, supersededAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse superseded_at: %w", err)
		}
		mem.SupersededAt = &t
	}

	if contentHashStr.Valid {
		mem.ContentHash = contentHashStr.String
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
