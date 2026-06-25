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
	"github.com/nram-ai/nram/internal/tags"
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

// memoryInsertArgs normalizes a *model.Memory's optional fields and returns
// the 19-arg slice used by the memories INSERT statement, plus the
// backend-specific INSERT query. Mutates mem to fill in defaults
// (ID, Tags, Metadata, ContentHash, CreatedAt, UpdatedAt) so callers
// can reuse the populated struct without an extra reload SELECT.
func (r *MemoryRepo) memoryInsertArgs(mem *model.Memory) (string, []any) {
	if mem.ID == uuid.Nil {
		mem.ID = uuid.New()
	}
	mem.Tags = tags.Normalize(mem.Tags)
	if mem.Tags == nil {
		mem.Tags = []string{}
	}
	if mem.Metadata == nil {
		mem.Metadata = json.RawMessage(`{}`)
	}
	now := time.Now().UTC()
	if mem.CreatedAt.IsZero() {
		mem.CreatedAt = now
	}
	if mem.UpdatedAt.IsZero() {
		mem.UpdatedAt = now
	}
	if mem.ContentHash == "" {
		mem.ContentHash = HashContent(mem.Content)
	}

	var source, embeddingDim, lastAccessed, expiresAt, supersededBy, supersededAt, purgeAfter any
	var augmentedQueries, augmentedEmbeddingAt any
	if mem.Source != nil {
		source = *mem.Source
	}
	origin := string(mem.Origin.OrDefault())
	if mem.EmbeddingDim != nil {
		embeddingDim = *mem.EmbeddingDim
	}
	if mem.LastAccessed != nil {
		lastAccessed = mem.LastAccessed.UTC().Format(time.RFC3339)
	}
	if mem.ExpiresAt != nil {
		expiresAt = mem.ExpiresAt.UTC().Format(time.RFC3339)
	}
	if mem.SupersededBy != nil {
		supersededBy = mem.SupersededBy.String()
	}
	if mem.SupersededAt != nil {
		supersededAt = mem.SupersededAt.UTC().Format(time.RFC3339)
	}
	if mem.PurgeAfter != nil {
		purgeAfter = mem.PurgeAfter.UTC().Format(time.RFC3339)
	}
	if len(mem.AugmentedQueries) > 0 {
		raw, _ := json.Marshal(mem.AugmentedQueries)
		augmentedQueries = string(raw)
	}
	if mem.AugmentedEmbeddingAt != nil {
		augmentedEmbeddingAt = mem.AugmentedEmbeddingAt.UTC().Format(time.RFC3339)
	}

	query := `INSERT INTO memories (id, namespace_id, content, content_hash, embedding_dim, source, tags,
		confidence, importance, access_count, last_accessed, expires_at, superseded_by, superseded_at,
		enriched, metadata, purge_after, created_at, updated_at, augmented_queries, augmented_embedding_at, origin)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO memories (id, namespace_id, content, content_hash, embedding_dim, source, tags,
			confidence, importance, access_count, last_accessed, expires_at, superseded_by, superseded_at,
			enriched, metadata, purge_after, created_at, updated_at, augmented_queries, augmented_embedding_at, origin)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`
	}

	return query, []any{
		mem.ID.String(), mem.NamespaceID.String(), mem.Content, mem.ContentHash,
		embeddingDim, source, encodeStringArray(r.db.Backend(), mem.Tags),
		mem.Confidence, mem.Importance, mem.AccessCount,
		lastAccessed, expiresAt, supersededBy, supersededAt,
		EncodeBool(r.db.Backend(), mem.Enriched), string(mem.Metadata), purgeAfter,
		mem.CreatedAt.UTC().Format(time.RFC3339), mem.UpdatedAt.UTC().Format(time.RFC3339),
		augmentedQueries, augmentedEmbeddingAt, origin,
	}
}

// Create inserts a new memory. ID is generated if zero-valued.
// Tags defaults to `[]` if nil. Metadata defaults to `{}` if nil.
func (r *MemoryRepo) Create(ctx context.Context, mem *model.Memory) error {
	query, args := r.memoryInsertArgs(mem)
	if _, err := r.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("memory create: %w", err)
	}
	return nil
}

// GetByID returns a memory by its UUID, bounded to namespaceID. Soft-deleted
// records are excluded. A row in a different namespace reads as sql.ErrNoRows;
// existence is never leaked across the tenant boundary.
func (r *MemoryRepo) GetByID(ctx context.Context, id, namespaceID uuid.UUID) (*model.Memory, error) {
	return r.getByIDExec(ctx, dbExec{r.db}, id, namespaceID)
}

// GetByIDTx is the transactional variant of GetByID. Used by callers that
// pair Get with a subsequent Update inside a WithMemoryLock body so the
// read sees the locked row's committed state.
func (r *MemoryRepo) GetByIDTx(ctx context.Context, tx *sql.Tx, id, namespaceID uuid.UUID) (*model.Memory, error) {
	return r.getByIDExec(ctx, tx, id, namespaceID)
}

func (r *MemoryRepo) getByIDExec(ctx context.Context, exec sqlExecer, id, namespaceID uuid.UUID) (*model.Memory, error) {
	query := selectMemoryColumns + ` FROM memories WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories WHERE id = $1 AND namespace_id = $2 AND deleted_at IS NULL`
	}

	row := exec.QueryRowContext(ctx, query, id.String(), namespaceID.String())
	return r.scanMemory(row)
}

// getByIDIncludeDeleted returns a memory by its UUID (bounded to namespaceID)
// including soft-deleted records. Used internally for reload after create.
func (r *MemoryRepo) getByIDIncludeDeleted(ctx context.Context, id, namespaceID uuid.UUID) (*model.Memory, error) {
	return r.getByIDIncludeDeletedExec(ctx, dbExec{r.db}, id, namespaceID)
}

func (r *MemoryRepo) getByIDIncludeDeletedExec(ctx context.Context, exec sqlExecer, id, namespaceID uuid.UUID) (*model.Memory, error) {
	query := selectMemoryColumns + ` FROM memories WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories WHERE id = $1 AND namespace_id = $2`
	}

	row := exec.QueryRowContext(ctx, query, id.String(), namespaceID.String())
	return r.scanMemory(row)
}

// LookupByContentHash returns the live memory in the namespace that matches the
// given sha256 content hash, or sql.ErrNoRows if none exists. The index is
// non-unique (legacy duplicates exist), so LIMIT 1 keeps behavior deterministic.
// Superseded rows are excluded so ingest dedup never returns a paraphrase loser.
func (r *MemoryRepo) LookupByContentHash(ctx context.Context, namespaceID uuid.UUID, hash string) (*model.Memory, error) {
	query := selectMemoryColumns + ` FROM memories
		WHERE namespace_id = ? AND content_hash = ? AND deleted_at IS NULL AND superseded_by IS NULL
		ORDER BY created_at ASC LIMIT 1`
	if r.db.Backend() == BackendPostgres {
		query = selectMemoryColumns + ` FROM memories
			WHERE namespace_id = $1 AND content_hash = $2 AND deleted_at IS NULL AND superseded_by IS NULL
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
			_ = rows.Close()
			return 0, fmt.Errorf("backfill scan: %w", err)
		}
		batch = append(batch, p)
	}
	_ = rows.Close()
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

// uuidInPlaceholders returns N placeholders and stringified UUIDs for an
// IN-list. startIndex is the first Postgres placeholder number ($N); it is
// ignored for SQLite (which always uses "?"). Returned ids are stringified so
// callers can append directly to the Exec/Query args slice.
func uuidInPlaceholders(db DB, ids []uuid.UUID, startIndex int) ([]string, []any) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	pg := db.Backend() == BackendPostgres
	for i, id := range ids {
		if pg {
			placeholders[i] = fmt.Sprintf("$%d", startIndex+i)
		} else {
			placeholders[i] = "?"
		}
		args[i] = id.String()
	}
	return placeholders, args
}

// GetBatch returns multiple memories by their UUIDs, bounded to the supplied
// namespaces. Soft-deleted records are excluded. Fail-closed: an empty
// namespaces slice returns no rows, so a caller can never hydrate a
// cross-namespace memory by id.
func (r *MemoryRepo) GetBatch(ctx context.Context, ids, namespaces []uuid.UUID) ([]model.Memory, error) {
	if len(ids) == 0 || len(namespaces) == 0 {
		return []model.Memory{}, nil
	}

	idPlaceholders, idArgs := uuidInPlaceholders(r.db, ids, 1)
	nsPlaceholders, nsArgs := uuidInPlaceholders(r.db, namespaces, len(ids)+1)

	query := selectMemoryColumns + ` FROM memories WHERE id IN (` +
		strings.Join(idPlaceholders, ", ") + `) AND namespace_id IN (` +
		strings.Join(nsPlaceholders, ", ") + `) AND deleted_at IS NULL`

	args := append(idArgs, nsArgs...)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory get batch: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
// semantics: a memory must contain ALL listed tags. Source and Search are
// case-insensitive substring matches against the source and content columns
// respectively. Tag SQL is backend-specific because SQLite stores tags as a
// JSON-encoded TEXT column and Postgres stores them as TEXT[].
//
// In parent-anchored listing (buildParentListWhere) the dimensions split into
// two semantic classes, and a new field MUST be slotted into the right one:
//   - Content-discovery (Tags, DateFrom, DateTo, Source, Search): surface a
//     family when the parent OR any enrichment descendant matches. Emitted by
//     contentFilterClauses.
//   - Per-row status (Enriched, Origin, Augmented): a property of the displayed
//     row itself, matched against the parent only, never via a descendant, or
//     a non-matching parent leaks in through a matching child. Emitted by
//     statusFilterClauses.
//
// HideSuperseded and StaleStampKey get bespoke handling and belong to neither
// helper. The flat-list path (buildFilterWhere) applies all dimensions inline
// as ANDs; keep it in sync when adding a field.
type MemoryListFilters struct {
	Tags     []string
	DateFrom *time.Time
	DateTo   *time.Time
	// Enriched is a tri-state filter: nil = no filter, *true = enriched only,
	// *false = not-enriched only.
	Enriched *bool
	// Origin, when non-empty, restricts results to rows with a matching
	// origin column value ("user" | "dream" | "import"). Validation of the
	// value belongs to the caller (the REST handler rejects unknown values).
	Origin string
	// Augmented is a tri-state filter over augmented_embedding_at: nil = no
	// filter, *true = augmented only (timestamp present), *false = not-augmented
	// only (timestamp NULL).
	Augmented *bool
	Source    string
	Search    string
	// HideSuperseded excludes rows with superseded_by set. Mirrors the
	// always-on deleted_at filter but is opt-in so dreaming phases that walk
	// the full set with zero-value filters still see superseded rows.
	HideSuperseded bool
	// StaleStampKey, when non-empty, restricts results to rows whose
	// metadata stamp at that key is missing OR whose stamp predates
	// updated_at. Drives the dreaming phase staleness predicate at the
	// SQL layer so phases never accumulate the full namespace in memory.
	// Used by ListByNamespaceStale; ignored by other listing methods'
	// default ORDER BY (callers requiring oldest-stale-first should use
	// ListByNamespaceStale, which forces ORDER BY updated_at ASC).
	StaleStampKey string
}

// IsZero reports whether no filter dimensions are active.
func (f MemoryListFilters) IsZero() bool {
	return len(f.Tags) == 0 && f.DateFrom == nil && f.DateTo == nil &&
		f.Enriched == nil && f.Origin == "" && f.Augmented == nil &&
		f.Source == "" && f.Search == "" && !f.HideSuperseded &&
		f.StaleStampKey == ""
}

// whereBuilder accumulates WHERE clause fragments and their bind values while
// generating backend-appropriate placeholders ($N for Postgres, ? for SQLite).
type whereBuilder struct {
	postgres bool
	clauses  []string
	args     []any
}

func (w *whereBuilder) placeholder() string {
	if w.postgres {
		return fmt.Sprintf("$%d", len(w.args)+1)
	}
	return "?"
}

func (w *whereBuilder) add(clauseFmt string, value any) {
	w.clauses = append(w.clauses, fmt.Sprintf(clauseFmt, w.placeholder()))
	w.args = append(w.args, value)
}

func (w *whereBuilder) where() string {
	return strings.Join(w.clauses, " AND ")
}

// buildFilterWhere produces a WHERE clause + arg slice for the given filters,
// always anchored on namespace_id and deleted_at IS NULL. The returned args
// can be appended with limit/offset/etc. as needed.
func (r *MemoryRepo) buildFilterWhere(namespaceID uuid.UUID, filters MemoryListFilters) (string, []any) {
	wb := &whereBuilder{postgres: r.db.Backend() == BackendPostgres}
	wb.add("namespace_id = %s", namespaceID.String())
	wb.clauses = append(wb.clauses, "deleted_at IS NULL")
	if filters.HideSuperseded {
		wb.clauses = append(wb.clauses, "superseded_by IS NULL")
	}

	// Tag filter: AND semantics.
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
		wb.add("enriched = %s", EncodeBool(r.db.Backend(), *filters.Enriched))
	}

	if filters.Origin != "" {
		wb.add("origin = %s", filters.Origin)
	}

	if filters.Augmented != nil {
		// augmented_embedding_at is a nullable timestamp stamped when a
		// memory's query-augmented embedding is computed. Presence/absence is
		// the filter; no bind value needed.
		if *filters.Augmented {
			wb.clauses = append(wb.clauses, "augmented_embedding_at IS NOT NULL")
		} else {
			wb.clauses = append(wb.clauses, "augmented_embedding_at IS NULL")
		}
	}

	if filters.Source != "" {
		wb.add(`LOWER(COALESCE(source, '')) LIKE %s ESCAPE '\'`, "%"+strings.ToLower(escapeLike(filters.Source))+"%")
	}

	if filters.Search != "" {
		wb.add(`LOWER(content) LIKE %s ESCAPE '\'`, "%"+strings.ToLower(escapeLike(filters.Search))+"%")
	}

	if filters.StaleStampKey != "" {
		// Row is stale when the stamp is absent OR strictly predates
		// updated_at. The same staleness rule the in-memory isStale helpers
		// implement (phase_paraphrase_dedup.go isParaphraseStale, etc.) are
		// pushed into SQL so phases load only stale candidates.
		//
		// Postgres: metadata is JSONB. metadata->>$key returns text; cast to
		// timestamptz for comparison. If a value is malformed the cast will
		// abort the query; phases write stamps via time.RFC3339Nano so this
		// requires manual metadata corruption to trigger; phases retain
		// in-memory collectStale as belt-and-suspenders for that case.
		//
		// SQLite: metadata is TEXT. json_extract returns NULL for missing
		// keys; datetime() returns NULL on unparseable strings, so a
		// malformed stamp falls through the comparison as NULL (treated as
		// false by < and OR), which would mark the row as fresh; defensive
		// in-memory collectStale catches that edge case.
		if wb.postgres {
			ph1 := wb.bindOnly(filters.StaleStampKey)
			ph2 := wb.bindOnly(filters.StaleStampKey)
			wb.clauses = append(wb.clauses,
				fmt.Sprintf("((metadata->>%s) IS NULL OR (metadata->>%s)::timestamptz < updated_at)", ph1, ph2))
		} else {
			path := "$." + filters.StaleStampKey
			ph1 := wb.bindOnly(path)
			ph2 := wb.bindOnly(path)
			wb.clauses = append(wb.clauses,
				fmt.Sprintf("(json_extract(metadata, %s) IS NULL OR datetime(json_extract(metadata, %s)) < datetime(updated_at))", ph1, ph2))
		}
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
	defer func() { _ = rows.Close() }()

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

// ListByNamespaceFramingOrder returns live (non-deleted, non-superseded)
// memories in a namespace ordered for the about_me persona "framing" load:
//  1. identity-centrality: the MAX mention_count over the entities a memory's
//     relationships link to (memories anchoring the most-mentioned entities in
//     the namespace come first; a memory with no linked entities ranks at 0),
//  2. recall-count: access_count (how often the memory has surfaced),
//  3. recency: created_at.
//
// Implemented as an ordered-id query (the join/aggregate) followed by a batch
// hydrate, so it reuses the canonical memory scan without re-listing columns.
func (r *MemoryRepo) ListByNamespaceFramingOrder(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error) {
	query := `SELECT m.id
		FROM memories m
		LEFT JOIN relationships rel ON rel.source_memory = m.id
		LEFT JOIN entities e ON e.id = rel.source_id OR e.id = rel.target_id
		WHERE m.namespace_id = ? AND m.deleted_at IS NULL AND m.superseded_by IS NULL
		GROUP BY m.id, m.access_count, m.created_at
		ORDER BY COALESCE(MAX(e.mention_count), 0) DESC, m.access_count DESC, m.created_at DESC
		LIMIT ? OFFSET ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT m.id
		FROM memories m
		LEFT JOIN relationships rel ON rel.source_memory = m.id
		LEFT JOIN entities e ON e.id = rel.source_id OR e.id = rel.target_id
		WHERE m.namespace_id = $1 AND m.deleted_at IS NULL AND m.superseded_by IS NULL
		GROUP BY m.id, m.access_count, m.created_at
		ORDER BY COALESCE(MAX(e.mention_count), 0) DESC, m.access_count DESC, m.created_at DESC
		LIMIT $2 OFFSET $3`
	}

	rows, err := r.db.Query(ctx, query, namespaceID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("memory framing-order list: %w", err)
	}
	orderedIDs := []uuid.UUID{}
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("memory framing-order scan: %w", err)
		}
		id, perr := uuid.Parse(idStr)
		if perr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("memory framing-order parse id: %w", perr)
		}
		orderedIDs = append(orderedIDs, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("memory framing-order iteration: %w", err)
	}
	_ = rows.Close()

	if len(orderedIDs) == 0 {
		return []model.Memory{}, nil
	}

	batch, err := r.GetBatch(ctx, orderedIDs, []uuid.UUID{namespaceID})
	if err != nil {
		return nil, err
	}
	byID := make(map[uuid.UUID]model.Memory, len(batch))
	for i := range batch {
		byID[batch[i].ID] = batch[i]
	}
	result := make([]model.Memory, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if mem, ok := byID[id]; ok {
			result = append(result, mem)
		}
	}
	return result, nil
}

// ListByNamespaceStale returns up to limit non-deleted memories whose
// metadata stamp at stampKey is missing or strictly predates updated_at,
// ordered oldest-updated_at first so the older tail drains before fresher
// rows. Used by dreaming phases to bound per-cycle working-set memory by
// only loading rows that need work; the in-memory collectStale checks
// remain in each phase as defensive belt-and-suspenders for malformed
// stamps that survive the SQL predicate.
func (r *MemoryRepo) ListByNamespaceStale(ctx context.Context, namespaceID uuid.UUID, stampKey string, limit int) ([]model.Memory, error) {
	if stampKey == "" {
		return nil, fmt.Errorf("memory list stale: empty stamp key")
	}
	if limit <= 0 {
		limit = 1000
	}
	where, args := r.buildFilterWhere(namespaceID, MemoryListFilters{StaleStampKey: stampKey})

	limitPH := "?"
	if r.db.Backend() == BackendPostgres {
		limitPH = fmt.Sprintf("$%d", len(args)+1)
	}
	args = append(args, limit)

	query := selectMemoryColumns + ` FROM memories WHERE ` + where +
		` ORDER BY updated_at ASC LIMIT ` + limitPH

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory list stale: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory list stale iteration: %w", err)
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
	defer func() { _ = rows.Close() }()

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

// ExtractedChildRelations is the canonical set of memory_lineage relations
// that represent enrichment-derived child memories. Used by the parent-
// anchored list endpoint to identify rows that should roll up under their
// source memory rather than appear as standalone entries.
var ExtractedChildRelations = []string{
	model.LineageExtractedFact,
	model.LineageSynthesizedFrom,
	model.LineageExtractedFrom,
}

// FactExtractionRelations is the strict subset of ExtractedChildRelations
// that the paraphrase-guard backfill sweep is allowed to act on. Excludes
// LineageSynthesizedFrom (dreaming-consolidation outputs whose embedding
// naturally tracks their source) and LineageExtractedFrom (older
// extraction paths) so a backfill cannot destroy synthesized or
// non-fact-extraction child memories whose high cosine to the parent is
// expected and intentional.
var FactExtractionRelations = []string{
	model.LineageExtractedFact,
}

// bindOnly claims a placeholder against the builder and binds the value,
// returning the placeholder string. Unlike add(), it does not append to
// clauses; the caller is responsible for placing the placeholder inside a
// larger SQL fragment.
func (w *whereBuilder) bindOnly(value any) string {
	ph := w.placeholder()
	w.args = append(w.args, value)
	return ph
}

// aliasQualifier returns a function that prefixes a column with the given
// table alias (e.g. "m.tags"), or returns the bare column when alias is empty.
// Shared by the filter-clause helpers so each can qualify columns against the
// outer table or an EXISTS-subquery alias without re-deriving the rule.
func aliasQualifier(alias string) func(string) string {
	return func(col string) string {
		if alias == "" {
			return col
		}
		return alias + "." + col
	}
}

// contentFilterClauses returns predicate strings for the content-discovery
// filter dimensions (tags, dates, source, search), qualified by the given
// table alias. These are the filters that, in parent-anchored listing, may
// match either the parent or any enrichment descendant: a family is surfaced
// when any member matches. Args are claimed against wb so placeholders are
// unique within the surrounding query. Anchors that always apply (namespace,
// deleted_at, hide-superseded) are NOT emitted by this helper; callers add
// them separately so the filter can be reused inside an OR-EXISTS subquery.
func (r *MemoryRepo) contentFilterClauses(wb *whereBuilder, filters MemoryListFilters, alias string) []string {
	qualify := aliasQualifier(alias)
	out := []string{}

	if len(filters.Tags) > 0 {
		if wb.postgres {
			phs := make([]string, len(filters.Tags))
			for i, t := range filters.Tags {
				phs[i] = wb.bindOnly(t)
			}
			out = append(out, fmt.Sprintf("%s @> ARRAY[%s]::text[]", qualify("tags"), strings.Join(phs, ",")))
		} else {
			for _, t := range filters.Tags {
				ph := wb.bindOnly(`%"` + escapeLike(t) + `"%`)
				out = append(out, fmt.Sprintf(`%s LIKE %s ESCAPE '\'`, qualify("tags"), ph))
			}
		}
	}
	if filters.DateFrom != nil {
		ph := wb.bindOnly(filters.DateFrom.UTC().Format(time.RFC3339))
		out = append(out, fmt.Sprintf("%s >= %s", qualify("created_at"), ph))
	}
	if filters.DateTo != nil {
		ph := wb.bindOnly(filters.DateTo.UTC().Format(time.RFC3339))
		out = append(out, fmt.Sprintf("%s < %s", qualify("created_at"), ph))
	}
	if filters.Source != "" {
		ph := wb.bindOnly("%" + strings.ToLower(escapeLike(filters.Source)) + "%")
		out = append(out, fmt.Sprintf(`LOWER(COALESCE(%s, '')) LIKE %s ESCAPE '\'`, qualify("source"), ph))
	}
	if filters.Search != "" {
		ph := wb.bindOnly("%" + strings.ToLower(escapeLike(filters.Search)) + "%")
		out = append(out, fmt.Sprintf(`LOWER(%s) LIKE %s ESCAPE '\'`, qualify("content"), ph))
	}
	return out
}

// statusFilterClauses returns predicate strings for the per-row status filter
// dimensions (enriched, origin, augmented), qualified by the given table
// alias. Unlike contentFilterClauses these describe a property of an
// individual row, so in parent-anchored listing they must constrain the
// displayed parent directly, never a descendant. Matching them family-wide
// would surface a parent that does not itself satisfy the filter (e.g. an
// augmented parent leaking into a "not augmented" view via a not-augmented
// child). Args are claimed against wb so placeholders stay unique.
func (r *MemoryRepo) statusFilterClauses(wb *whereBuilder, filters MemoryListFilters, alias string) []string {
	qualify := aliasQualifier(alias)
	out := []string{}

	if filters.Enriched != nil {
		ph := wb.bindOnly(EncodeBool(r.db.Backend(), *filters.Enriched))
		out = append(out, fmt.Sprintf("%s = %s", qualify("enriched"), ph))
	}
	if filters.Origin != "" {
		ph := wb.bindOnly(filters.Origin)
		out = append(out, fmt.Sprintf("%s = %s", qualify("origin"), ph))
	}
	if filters.Augmented != nil {
		if *filters.Augmented {
			out = append(out, fmt.Sprintf("%s IS NOT NULL", qualify("augmented_embedding_at")))
		} else {
			out = append(out, fmt.Sprintf("%s IS NULL", qualify("augmented_embedding_at")))
		}
	}
	return out
}

// memoryColumnsAliased returns the SELECT column list with each column
// prefixed by the given alias. Mirrors selectMemoryColumns but for joined
// queries where bare names would be ambiguous.
func memoryColumnsAliased(alias string) string {
	cols := []string{
		"id", "namespace_id", "content", "embedding_dim", "source", "tags",
		"confidence", "importance", "access_count", "last_accessed", "expires_at",
		"superseded_by", "superseded_at", "enriched", "metadata", "content_hash",
		"created_at", "updated_at", "deleted_at", "purge_after",
		"augmented_queries", "augmented_embedding_at", "origin", "faceted_at", "facet_count",
		"entity_extracted_at",
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = alias + "." + c
	}
	return "SELECT " + strings.Join(parts, ", ")
}

// buildParentListWhere assembles the WHERE clause for parent-anchored
// listing. The result selects memories that are NOT themselves enrichment-
// derived children of another memory in the same namespace, with user
// filters matching either the parent itself or any of its enrichment
// descendants. The returned WHERE references alias `m` for the outer
// memories table.
func (r *MemoryRepo) buildParentListWhere(namespaceID uuid.UUID, filters MemoryListFilters) (string, []any) {
	wb := &whereBuilder{postgres: r.db.Backend() == BackendPostgres}

	wb.add("m.namespace_id = %s", namespaceID.String())
	wb.clauses = append(wb.clauses, "m.deleted_at IS NULL")
	if filters.HideSuperseded {
		wb.clauses = append(wb.clauses, "m.superseded_by IS NULL")
	}

	relPHs := make([]string, len(ExtractedChildRelations))
	for i, rel := range ExtractedChildRelations {
		relPHs[i] = wb.bindOnly(rel)
	}
	antiJoin := fmt.Sprintf(
		`NOT EXISTS (SELECT 1 FROM memory_lineage ls WHERE ls.namespace_id = m.namespace_id AND ls.memory_id = m.id AND ls.parent_id IS NOT NULL AND ls.relation IN (%s))`,
		strings.Join(relPHs, ", "),
	)
	wb.clauses = append(wb.clauses, antiJoin)

	// Per-row status filters (enriched, origin, augmented) describe the
	// displayed parent row itself, so they constrain m directly and are never
	// routed through the descendant subquery; otherwise a parent that does
	// not match (e.g. an augmented parent) would leak in via a matching child.
	wb.clauses = append(wb.clauses, r.statusFilterClauses(wb, filters, "m")...)

	// Content-discovery filters (tags, dates, source, search) surface a family
	// when the parent OR any enrichment descendant matches.
	parentClauses := r.contentFilterClauses(wb, filters, "m")
	if len(parentClauses) > 0 {
		descRelPHs := make([]string, len(ExtractedChildRelations))
		for i, rel := range ExtractedChildRelations {
			descRelPHs[i] = wb.bindOnly(rel)
		}
		childClauses := r.contentFilterClauses(wb, filters, "c")
		descSub := fmt.Sprintf(
			`EXISTS (SELECT 1 FROM memory_lineage ld JOIN memories c ON c.id = ld.memory_id WHERE ld.namespace_id = m.namespace_id AND ld.parent_id = m.id AND ld.relation IN (%s) AND c.deleted_at IS NULL AND %s)`,
			strings.Join(descRelPHs, ", "),
			strings.Join(childClauses, " AND "),
		)
		combined := fmt.Sprintf("(%s OR %s)", strings.Join(parentClauses, " AND "), descSub)
		wb.clauses = append(wb.clauses, combined)
	}

	return wb.where(), wb.args
}

// ListParentsByNamespaceFiltered returns memories that are not themselves
// enrichment-derived children of another memory in the namespace, narrowed
// by filters that match the parent or any descendant. Paginated, ordered by
// created_at DESC. Drives the parent-anchored Memory Browser view so a
// parent and its extracted facts always appear together.
func (r *MemoryRepo) ListParentsByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters MemoryListFilters, limit, offset int) ([]model.Memory, error) {
	where, args := r.buildParentListWhere(namespaceID, filters)

	limitPH := "?"
	offsetPH := "?"
	if r.db.Backend() == BackendPostgres {
		limitPH = fmt.Sprintf("$%d", len(args)+1)
		offsetPH = fmt.Sprintf("$%d", len(args)+2)
	}
	args = append(args, limit, offset)

	query := memoryColumnsAliased("m") + ` FROM memories m WHERE ` + where +
		` ORDER BY m.created_at DESC LIMIT ` + limitPH + ` OFFSET ` + offsetPH

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory list parents by namespace: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory list parents iteration: %w", err)
	}
	return result, nil
}

// CountParentsByNamespaceFiltered returns the total parent-anchored count
// matching the same predicate as ListParentsByNamespaceFiltered. Used to
// drive infinite-scroll has-more detection.
func (r *MemoryRepo) CountParentsByNamespaceFiltered(ctx context.Context, namespaceID uuid.UUID, filters MemoryListFilters) (int, error) {
	where, args := r.buildParentListWhere(namespaceID, filters)
	query := `SELECT COUNT(*) FROM memories m WHERE ` + where
	row := r.db.QueryRow(ctx, query, args...)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("memory count parents by namespace: %w", err)
	}
	return count, nil
}

// FindChildrenByParents returns the enrichment-derived child memories for
// each given parent ID, bucketed in a single batched query. Soft-deleted
// and superseded children are excluded so the response shape matches what
// the list endpoint already filters out for top-level rows. Children are
// ordered by created_at DESC within each bucket.
func (r *MemoryRepo) FindChildrenByParents(ctx context.Context, namespaceID uuid.UUID, parentIDs []uuid.UUID, relations []string) (map[uuid.UUID][]model.Memory, error) {
	if len(parentIDs) == 0 || len(relations) == 0 {
		return map[uuid.UUID][]model.Memory{}, nil
	}

	postgres := r.db.Backend() == BackendPostgres
	args := make([]any, 0, 1+len(parentIDs)+len(relations))
	args = append(args, namespaceID.String())

	parentPHs := make([]string, len(parentIDs))
	for i, pid := range parentIDs {
		if postgres {
			parentPHs[i] = fmt.Sprintf("$%d", len(args)+1)
		} else {
			parentPHs[i] = "?"
		}
		args = append(args, pid.String())
	}

	relPHs := make([]string, len(relations))
	for i, rel := range relations {
		if postgres {
			relPHs[i] = fmt.Sprintf("$%d", len(args)+1)
		} else {
			relPHs[i] = "?"
		}
		args = append(args, rel)
	}

	nsPH := "?"
	if postgres {
		nsPH = "$1"
	}

	query := memoryColumnsAliased("m") + `, l.parent_id FROM memories m JOIN memory_lineage l ON l.memory_id = m.id` +
		` WHERE l.namespace_id = ` + nsPH +
		` AND l.parent_id IN (` + strings.Join(parentPHs, ", ") + `)` +
		` AND l.relation IN (` + strings.Join(relPHs, ", ") + `)` +
		` AND m.deleted_at IS NULL AND m.superseded_by IS NULL` +
		` ORDER BY m.created_at DESC`

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory find children by parents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[uuid.UUID][]model.Memory)
	for rows.Next() {
		var mem model.Memory
		var idStr, namespaceIDStr string
		var tagsStr, metadataStr string
		var createdAtStr, updatedAtStr string
		var embeddingDim sql.NullInt64
		var source sql.NullString
		var lastAccessedStr, expiresAtStr, deletedAtStr, purgeAfterStr sql.NullString
		var supersededByStr, supersededAtStr, contentHashStr sql.NullString
		var enrichedBool bool
		var augmentedQueriesStr, augmentedEmbeddingAtStr, originStr, facetedAtStr, entityExtractedAtStr sql.NullString
		var facetCountVal sql.NullInt64
		var parentIDStr string

		err := rows.Scan(
			&idStr, &namespaceIDStr, &mem.Content, &embeddingDim, &source, &tagsStr,
			&mem.Confidence, &mem.Importance, &mem.AccessCount, &lastAccessedStr,
			&expiresAtStr, &supersededByStr, &supersededAtStr, &enrichedBool, &metadataStr,
			&contentHashStr, &createdAtStr, &updatedAtStr, &deletedAtStr, &purgeAfterStr,
			&augmentedQueriesStr, &augmentedEmbeddingAtStr, &originStr, &facetedAtStr, &facetCountVal,
			&entityExtractedAtStr,
			&parentIDStr,
		)
		if err != nil {
			return nil, fmt.Errorf("memory find children scan: %w", err)
		}

		populated, err := r.populateMemory(&mem, idStr, namespaceIDStr, tagsStr, metadataStr,
			createdAtStr, updatedAtStr, embeddingDim, source, lastAccessedStr,
			expiresAtStr, supersededByStr, supersededAtStr, contentHashStr,
			enrichedBool, deletedAtStr, purgeAfterStr,
			augmentedQueriesStr, augmentedEmbeddingAtStr, originStr, facetedAtStr, facetCountVal,
			entityExtractedAtStr)
		if err != nil {
			return nil, err
		}

		parentID, err := uuid.Parse(parentIDStr)
		if err != nil {
			return nil, fmt.Errorf("memory find children parse parent_id: %w", err)
		}

		// Set ParentID so callers/JSON responses match the detail handler shape.
		pidCopy := parentID
		populated.ParentID = &pidCopy

		result[parentID] = append(result[parentID], *populated)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory find children iteration: %w", err)
	}
	return result, nil
}

// CountWithEmbeddingDim returns the number of live memories that
// currently have a non-NULL embedding_dim.
func (r *MemoryRepo) CountWithEmbeddingDim(ctx context.Context) (int64, error) {
	query := `SELECT COUNT(*) FROM memories WHERE embedding_dim IS NOT NULL AND deleted_at IS NULL`
	var n int64
	if err := r.db.QueryRow(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("memory count with embedding_dim: %w", err)
	}
	return n, nil
}

// ClearAllEmbeddingDims sets embedding_dim = NULL for every live memory.
// Returns the count of rows affected. Used by the embedding-model switch
// cascade so re-embed treats every row as fresh.
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

// BackfillCandidate is a memory id paired with its namespace, returned by the
// admin backfill candidate listers. Carrying the namespace lets the caller
// enqueue jobs for candidates spanning many namespaces (the whole-deployment
// admin sweep) without an unbounded per-id read to learn each one's namespace.
type BackfillCandidate struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
}

// scanBackfillCandidates drains a (id, namespace_id) result set into
// BackfillCandidate values.
func scanBackfillCandidates(rows *sql.Rows) ([]BackfillCandidate, error) {
	out := []BackfillCandidate{}
	for rows.Next() {
		var idStr, nsStr string
		if err := rows.Scan(&idStr, &nsStr); err != nil {
			return nil, fmt.Errorf("scan backfill candidate: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("parse backfill candidate id %s: %w", idStr, err)
		}
		ns, err := uuid.Parse(nsStr)
		if err != nil {
			return nil, fmt.Errorf("parse backfill candidate namespace %s: %w", nsStr, err)
		}
		out = append(out, BackfillCandidate{ID: id, NamespaceID: ns})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter backfill candidates: %w", err)
	}
	return out, nil
}

// ListAugmentationBackfillCandidates returns memories (id + namespace) that
// still need an augmented embedding (augmented_embedding_at IS NULL) within the
// given namespace scope. namespaceIDs == nil scans the whole deployment.
// Soft-deleted and superseded rows are excluded so the backfill never re-embeds
// rows the runtime ignores anyway. limit == 0 returns all matches.
//
// Empty/whitespace-only-content rows are also excluded: the worker skips query
// augmentation for them (query_augment_skip_reason=content_empty) and therefore
// never stamps augmented_embedding_at, so without this guard they would be
// re-selected every cycle in an unbreakable loop. They have nothing to augment,
// and a later content edit re-enqueues enrichment via the write path. Transient
// skip reasons (provider_unavailable, llm_error, parse_error) intentionally stay
// eligible so a later cycle retries them.
// listBackfillCandidates is the shared query builder for the backfill candidate
// listers. It selects live, non-superseded memories with non-empty content,
// scoped to namespaceIDs (empty = whole deployment) and bounded by limit, plus
// any caller-supplied static extra predicates (no bound params, so they do not
// shift placeholder numbering). Returns id+namespace pairs so callers enqueue
// without a per-id read on the whole-deployment sweep.
func (r *MemoryRepo) listBackfillCandidates(ctx context.Context, extraWhere []string, namespaceIDs []uuid.UUID, limit int, label, orderBy string) ([]BackfillCandidate, error) {
	pg := r.db.Backend() == BackendPostgres
	args := []any{}
	where := append([]string{
		"deleted_at IS NULL",
		"superseded_by IS NULL",
		"content IS NOT NULL AND trim(content) <> ''",
	}, extraWhere...)
	if len(namespaceIDs) > 0 {
		placeholders := make([]string, len(namespaceIDs))
		for i, ns := range namespaceIDs {
			if pg {
				placeholders[i] = fmt.Sprintf("$%d", len(args)+1)
			} else {
				placeholders[i] = "?"
			}
			args = append(args, ns.String())
		}
		where = append(where, "namespace_id IN ("+strings.Join(placeholders, ", ")+")")
	}
	query := "SELECT id, namespace_id FROM memories WHERE " + strings.Join(where, " AND ") + " ORDER BY " + orderBy
	if limit > 0 {
		if pg {
			query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		} else {
			query += " LIMIT ?"
		}
		args = append(args, limit)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	defer func() { _ = rows.Close() }()

	return scanBackfillCandidates(rows)
}

// ListMultiVectorBackfillCandidates lists live in-scope memories that have not
// yet been faceted (faceted_at IS NULL) AND carry a stored whole-memory vector
// (embedding_dim IS NOT NULL). The facet pass stamps faceted_at on every memory
// it processes (single-topic ones included), so the candidate count drops to
// zero once a backfill completes and a re-run only picks up genuinely new
// memories, rather than re-faceting the whole corpus. Dream syntheses
// are enqueued first because they are the population most prone to multi-topic
// dilution (lexical consolidation fused unrelated sources) and many have already
// purged their sources on supersession, so faceting is the only recall recovery
// left for them; ordering them ahead of plain memories means a limited or
// interrupted backfill repairs the highest-value memories first.
//
// The embedding_dim IS NOT NULL predicate is load-bearing: runMultiVectorFacetSweep
// reuses the stored facet-0 vector and keys the fetch off memories.embedding_dim,
// so a NULL-dim row (vector never written, or write soft-failed) can never be
// faceted and the sweep would skip it WITHOUT stamping faceted_at, re-selecting it
// on every run forever. Excluding NULL-dim rows here (matching what the worker can
// actually act on, like ListEnrichedParentsWithExtractedChildren) stops that churn;
// the embedding-backfill phase restores embedding_dim for such rows, after which
// they become facet candidates again on the next cycle.
func (r *MemoryRepo) ListMultiVectorBackfillCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]BackfillCandidate, error) {
	return r.listBackfillCandidates(ctx, []string{"faceted_at IS NULL", "embedding_dim IS NOT NULL"}, namespaceIDs, limit, "list multi-vector backfill candidates", "(origin = 'dream') DESC, created_at ASC")
}

// ListAugmentationBackfillCandidates lists in-scope memories whose vector
// pre-dates query augmentation (augmented_embedding_at IS NULL).
func (r *MemoryRepo) ListAugmentationBackfillCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]BackfillCandidate, error) {
	return r.listBackfillCandidates(ctx, []string{"augmented_embedding_at IS NULL"}, namespaceIDs, limit, "list augmentation backfill candidates", "created_at ASC")
}

// ListEnrichedParentsWithExtractedChildren returns the IDs of enriched,
// non-deleted, non-superseded parent memories that have at least one live
// extracted-fact lineage child. Used by the BackfillExtractedFactParaphrase
// service method to enumerate candidate parents whose children should be
// swept by the paraphrase-guard backfill job. Children are filtered by the
// ExtractedChildRelations set and exclude soft-deleted and superseded rows
// so the candidate count matches what the worker can actually act on.
func (r *MemoryRepo) ListEnrichedParentsWithExtractedChildren(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]BackfillCandidate, error) {
	pg := r.db.Backend() == BackendPostgres
	args := []any{}
	args = append(args, EncodeBool(r.db.Backend(), true))
	enrichedPH := "?"
	if pg {
		enrichedPH = "$1"
	}

	relPHs := make([]string, len(ExtractedChildRelations))
	for i, rel := range ExtractedChildRelations {
		if pg {
			relPHs[i] = fmt.Sprintf("$%d", len(args)+1)
		} else {
			relPHs[i] = "?"
		}
		args = append(args, rel)
	}

	nsClause := ""
	if len(namespaceIDs) > 0 {
		placeholders := make([]string, len(namespaceIDs))
		for i, ns := range namespaceIDs {
			if pg {
				placeholders[i] = fmt.Sprintf("$%d", len(args)+1)
			} else {
				placeholders[i] = "?"
			}
			args = append(args, ns.String())
		}
		nsClause = " AND m.namespace_id IN (" + strings.Join(placeholders, ", ") + ")"
	}

	query := `SELECT DISTINCT m.id, m.namespace_id
FROM memories m
JOIN memory_lineage l ON l.parent_id = m.id AND l.namespace_id = m.namespace_id
JOIN memories c ON c.id = l.memory_id
WHERE m.enriched = ` + enrichedPH + `
  AND m.deleted_at IS NULL
  AND m.superseded_by IS NULL
  AND c.deleted_at IS NULL
  AND c.superseded_by IS NULL
  AND l.relation IN (` + strings.Join(relPHs, ", ") + `)` + nsClause + `
ORDER BY m.id ASC`
	if limit > 0 {
		if pg {
			query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		} else {
			query += " LIMIT ?"
		}
		args = append(args, limit)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list paraphrase backfill candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanBackfillCandidates(rows)
}

// ListReExtractCandidates returns memories eligible for full re-extraction:
// enriched, live (not deleted/superseded), user/import origin (never dream
// syntheses, which the worker would skip anyway), and not themselves a derived
// extracted-fact child (re-extracting a parent regenerates those). Scoped to the
// given namespaces when non-empty; ordered by id for stable paging.
func (r *MemoryRepo) ListReExtractCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]BackfillCandidate, error) {
	pg := r.db.Backend() == BackendPostgres
	args := []any{}
	ph := func() string {
		if pg {
			return fmt.Sprintf("$%d", len(args)+1)
		}
		return "?"
	}

	enrichedPH := ph()
	args = append(args, EncodeBool(r.db.Backend(), true))
	dreamPH := ph()
	args = append(args, string(model.OriginDream))

	relPHs := make([]string, len(ExtractedChildRelations))
	for i, rel := range ExtractedChildRelations {
		relPHs[i] = ph()
		args = append(args, rel)
	}

	nsClause := ""
	if len(namespaceIDs) > 0 {
		placeholders := make([]string, len(namespaceIDs))
		for i, ns := range namespaceIDs {
			placeholders[i] = ph()
			args = append(args, ns.String())
		}
		nsClause = " AND m.namespace_id IN (" + strings.Join(placeholders, ", ") + ")"
	}

	query := `SELECT m.id, m.namespace_id
FROM memories m
WHERE m.enriched = ` + enrichedPH + `
  AND m.deleted_at IS NULL
  AND m.superseded_by IS NULL
  AND m.origin <> ` + dreamPH + `
  AND NOT EXISTS (
    SELECT 1 FROM memory_lineage l
    WHERE l.memory_id = m.id AND l.namespace_id = m.namespace_id
      AND l.relation IN (` + strings.Join(relPHs, ", ") + `)
  )` + nsClause + `
ORDER BY m.id ASC`
	if limit > 0 {
		query += " LIMIT " + ph()
		args = append(args, limit)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list re-extract candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanBackfillCandidates(rows)
}

// ListReExtractCandidatesByIDs returns the subset of the given memory IDs that
// are eligible for re-extraction, applying the same eligibility filter as
// ListReExtractCandidates (enriched, live, non-dream, not a derived
// extracted-fact child). When namespacePrefix is non-empty the result is
// restricted to memories whose namespace path equals or descends from it,
// mirroring the path-prefix scoping the retry path uses (an empty prefix means
// global, the admin path). IDs outside scope or ineligible are silently
// dropped, so the caller can compare the returned count to the requested count.
func (r *MemoryRepo) ListReExtractCandidatesByIDs(ctx context.Context, namespacePrefix string, memoryIDs []uuid.UUID) ([]BackfillCandidate, error) {
	if len(memoryIDs) == 0 {
		return nil, nil
	}
	pg := r.db.Backend() == BackendPostgres
	args := []any{}
	ph := func() string {
		if pg {
			return fmt.Sprintf("$%d", len(args)+1)
		}
		return "?"
	}

	enrichedPH := ph()
	args = append(args, EncodeBool(r.db.Backend(), true))
	dreamPH := ph()
	args = append(args, string(model.OriginDream))

	relPHs := make([]string, len(ExtractedChildRelations))
	for i, rel := range ExtractedChildRelations {
		relPHs[i] = ph()
		args = append(args, rel)
	}

	idPHs := make([]string, len(memoryIDs))
	for i, id := range memoryIDs {
		idPHs[i] = ph()
		args = append(args, id.String())
	}

	nsClause := ""
	if namespacePrefix != "" {
		exactPH := ph()
		args = append(args, namespacePrefix)
		prefixPH := ph()
		args = append(args, namespacePrefix+"/%")
		nsClause = " AND (n.path = " + exactPH + " OR n.path LIKE " + prefixPH + ")"
	}

	query := `SELECT m.id, m.namespace_id
FROM memories m
JOIN namespaces n ON n.id = m.namespace_id
WHERE m.enriched = ` + enrichedPH + `
  AND m.deleted_at IS NULL
  AND m.superseded_by IS NULL
  AND m.origin <> ` + dreamPH + `
  AND NOT EXISTS (
    SELECT 1 FROM memory_lineage l
    WHERE l.memory_id = m.id AND l.namespace_id = m.namespace_id
      AND l.relation IN (` + strings.Join(relPHs, ", ") + `)
  )
  AND m.id IN (` + strings.Join(idPHs, ", ") + `)` + nsClause + `
ORDER BY m.id ASC`

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list re-extract candidates by ids: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanBackfillCandidates(rows)
}

// ListDreamEntityBackfillCandidates returns active CONSOLIDATION dreams that
// still lack entity-graph coverage: origin=dream, live (not deleted/superseded),
// carrying a non-empty source_memory_ids metadata array (the discriminator that
// excludes project-description and other dream types — see
// model.Memory.IsConsolidationDream). A dream is a candidate only if it has
// NEITHER an entity_extracted_at stamp NOR a relationship sourced by it:
//   - entity_extracted_at IS NULL: entity extraction has never run for this
//     memory. The worker stamps it at finalize whenever extraction is performed,
//     even when it produced zero entities/relationships, so an entity-only
//     synthesis (entities but no relationships) drops out after one extraction
//     instead of re-extracting every cycle. This is the convergence gate.
//   - NOT EXISTS (relationship sourced by m): excludes dreams whose coverage
//     predates the stamp column (e.g. the manual-runbook recoveries), which have
//     relationships but a NULL stamp; without this they would be re-extracted
//     once after the column is added.
//
// Drives both the on-demand admin backfill and the ConsolidationEntityBackfill
// dream phase. Scoped to namespaces when non-empty; ordered by id for stable
// paging.
func (r *MemoryRepo) ListDreamEntityBackfillCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]BackfillCandidate, error) {
	pg := r.db.Backend() == BackendPostgres
	args := []any{}
	ph := func() string {
		if pg {
			return fmt.Sprintf("$%d", len(args)+1)
		}
		return "?"
	}

	dreamPH := ph()
	args = append(args, string(model.OriginDream))

	// Non-empty source_memory_ids array. Postgres metadata is JSONB, SQLite is
	// TEXT; the key name is a fixed literal, so it is inlined safely. The
	// Postgres jsonb_typeof guard makes jsonb_array_length non-erroring even if
	// the value were ever a non-array.
	sourceIDsClause := "json_array_length(m.metadata, '$.source_memory_ids') > 0"
	if pg {
		sourceIDsClause = "jsonb_typeof(m.metadata -> 'source_memory_ids') = 'array' " +
			"AND jsonb_array_length(m.metadata -> 'source_memory_ids') > 0"
	}

	nsClause := ""
	if len(namespaceIDs) > 0 {
		placeholders := make([]string, len(namespaceIDs))
		for i, ns := range namespaceIDs {
			placeholders[i] = ph()
			args = append(args, ns.String())
		}
		nsClause = " AND m.namespace_id IN (" + strings.Join(placeholders, ", ") + ")"
	}

	query := `SELECT m.id, m.namespace_id
FROM memories m
WHERE m.origin = ` + dreamPH + `
  AND m.deleted_at IS NULL
  AND m.superseded_by IS NULL
  AND m.entity_extracted_at IS NULL
  AND ` + sourceIDsClause + `
  AND NOT EXISTS (
    SELECT 1 FROM relationships rel WHERE rel.source_memory = m.id
  )` + nsClause + `
ORDER BY m.id ASC`
	if limit > 0 {
		query += " LIMIT " + ph()
		args = append(args, limit)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list dream entity backfill candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanBackfillCandidates(rows)
}

// ListMissingEmbeddingCandidates returns live, embeddable memories that have no
// stored vector (embedding_dim IS NULL) across the given namespaces, or the whole
// deployment when namespaceIDs is empty. These are the "embedding-stranded"
// memories — enriched or not — that no longer surface in vector recall; the
// missing-embeddings backfill re-enqueues them so the worker re-embeds and
// finalizes. Mirrors ListReExtractCandidates' shape; the predicate matches
// FindMemoriesNullEmbeddingDim so the on-demand backfill and the dreaming
// embedding-backfill phase select the same rows.
func (r *MemoryRepo) ListMissingEmbeddingCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]BackfillCandidate, error) {
	return r.listBackfillCandidates(ctx,
		[]string{"confidence > 0", "embedding_dim IS NULL"},
		namespaceIDs, limit, "list missing-embedding candidates", "id ASC")
}

// CountMissingEmbeddings returns how many live, embeddable memories have no
// stored vector across the whole deployment. Drives the enrichment health
// surface so an operator can see the embedding-stranded count and watch the
// backfill drain it.
func (r *MemoryRepo) CountMissingEmbeddings(ctx context.Context) (int64, error) {
	query := `SELECT count(*) FROM memories m
WHERE m.deleted_at IS NULL
  AND m.superseded_by IS NULL
  AND m.confidence > 0
  AND m.embedding_dim IS NULL
  AND m.content IS NOT NULL AND trim(m.content) <> ''`
	var n int64
	if err := r.db.QueryRow(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("count missing embeddings: %w", err)
	}
	return n, nil
}

// ResetEnriched clears the enriched flag so the worker re-runs fact and entity
// extraction on the next enqueue (the skip guard gates on enriched). Used by the
// re-extraction path after the memory's prior graph footprint is tombstoned.
func (r *MemoryRepo) ResetEnriched(ctx context.Context, id, namespaceID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories SET enriched = ?, updated_at = ? WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET enriched = $1, updated_at = $2 WHERE id = $3 AND namespace_id = $4`
	}
	if _, err := r.db.Exec(ctx, query, EncodeBool(r.db.Backend(), false), now, id.String(), namespaceID.String()); err != nil {
		return fmt.Errorf("reset enriched: %w", err)
	}
	return nil
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

// UpdateFacetState records that the multi-vector facet pass processed this
// memory: faceted_at is stamped now and facet_count is set to the number of
// facets produced (1 = single topic, i.e. facet 0 only; N = facet 0 plus N-1
// topic facets). Scoped by namespace as a tenancy guard. updated_at is left
// untouched: faceting is additive metadata layered on the already-written
// vector, not a content change, so it must not shift recency ordering or look
// like a user edit. The faceted_at stamp is what removes the memory from the
// ListMultiVectorBackfillCandidates set.
func (r *MemoryRepo) UpdateFacetState(ctx context.Context, id, namespaceID uuid.UUID, facetCount int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories SET faceted_at = ?, facet_count = ? WHERE id = ? AND namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET faceted_at = $1, facet_count = $2 WHERE id = $3 AND namespace_id = $4`
	}
	if _, err := r.db.Exec(ctx, query, now, facetCount, id.String(), namespaceID.String()); err != nil {
		return fmt.Errorf("memory update facet state: %w", err)
	}
	return nil
}

// Update updates all mutable fields of a memory and bumps updated_at.
func (r *MemoryRepo) Update(ctx context.Context, mem *model.Memory) error {
	return r.updateExec(ctx, dbExec{r.db}, mem)
}

// UpdateTx is the transactional variant of Update. Used by callers that
// pair Get with Update inside a WithMemoryLock body so the read-modify-write
// happens atomically under the row's advisory lock.
func (r *MemoryRepo) UpdateTx(ctx context.Context, tx *sql.Tx, mem *model.Memory) error {
	return r.updateExec(ctx, tx, mem)
}

// MutateInLock acquires the cross-process memory row lock, re-reads the row
// inside the lock, invokes mutate to apply changes in place, and writes the
// result back under the same lock when mutate signals a write is needed.
// Returns the post-write (or post-read, on no-write) memory, or nil with
// the error if any step fails.
//
// Use this for any read-modify-write on a memory row; the alternative,
// GetByID + mutate in Go + Update, has a lost-update window between
// concurrent workers (or processes) that this helper closes via
// pg_advisory_xact_lock on Postgres and an in-process mutex on SQLite.
func (r *MemoryRepo) MutateInLock(
	ctx context.Context,
	id, namespaceID uuid.UUID,
	mutate func(*model.Memory) (write bool, err error),
) (*model.Memory, error) {
	var result *model.Memory
	err := r.db.WithMemoryLock(ctx, id, func(ctx context.Context, tx *sql.Tx) error {
		// Bounded read: a row outside namespaceID reads as sql.ErrNoRows, so a
		// caller cannot lock-and-mutate another tenant's memory by id.
		fresh, err := r.getByIDExec(ctx, tx, id, namespaceID)
		if err != nil {
			return err
		}
		write, err := mutate(fresh)
		if err != nil {
			return err
		}
		if !write {
			result = fresh
			return nil
		}
		if err := r.updateExec(ctx, tx, fresh); err != nil {
			return err
		}
		result = fresh
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *MemoryRepo) updateExec(ctx context.Context, exec sqlExecer, mem *model.Memory) error {
	now := time.Now().UTC().Format(time.RFC3339)

	mem.Tags = tags.Normalize(mem.Tags)
	if mem.Tags == nil {
		mem.Tags = []string{}
	}
	if mem.Metadata == nil {
		mem.Metadata = json.RawMessage(`{}`)
	}

	tagsVal := encodeStringArray(r.db.Backend(), mem.Tags)

	var source any
	if mem.Source != nil {
		source = *mem.Source
	}

	origin := string(mem.Origin.OrDefault())

	var embeddingDim any
	if mem.EmbeddingDim != nil {
		embeddingDim = *mem.EmbeddingDim
	}

	var lastAccessed any
	if mem.LastAccessed != nil {
		lastAccessed = mem.LastAccessed.UTC().Format(time.RFC3339)
	}

	var expiresAt any
	if mem.ExpiresAt != nil {
		expiresAt = mem.ExpiresAt.UTC().Format(time.RFC3339)
	}

	var supersededBy any
	if mem.SupersededBy != nil {
		supersededBy = mem.SupersededBy.String()
	}

	var supersededAt any
	if mem.SupersededAt != nil {
		supersededAt = mem.SupersededAt.UTC().Format(time.RFC3339)
	}

	// Recompute content hash on update so in-place content edits stay truthful.
	mem.ContentHash = HashContent(mem.Content)

	var purgeAfter any
	if mem.PurgeAfter != nil {
		purgeAfter = mem.PurgeAfter.UTC().Format(time.RFC3339)
	}

	var augmentedQueries, augmentedEmbeddingAt any
	if len(mem.AugmentedQueries) > 0 {
		raw, _ := json.Marshal(mem.AugmentedQueries)
		augmentedQueries = string(raw)
	}
	if mem.AugmentedEmbeddingAt != nil {
		augmentedEmbeddingAt = mem.AugmentedEmbeddingAt.UTC().Format(time.RFC3339)
	}

	enrichedVal := EncodeBool(r.db.Backend(), mem.Enriched)

	query := `UPDATE memories SET content = ?, content_hash = ?, embedding_dim = ?, source = ?, tags = ?,
		confidence = ?, importance = ?, access_count = ?, last_accessed = ?,
		expires_at = ?, superseded_by = ?, superseded_at = ?, enriched = ?, metadata = ?,
		purge_after = ?, augmented_queries = ?, augmented_embedding_at = ?, origin = ?, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET content = $1, content_hash = $2, embedding_dim = $3, source = $4, tags = $5,
			confidence = $6, importance = $7, access_count = $8, last_accessed = $9,
			expires_at = $10, superseded_by = $11, superseded_at = $12, enriched = $13, metadata = $14,
			purge_after = $15, augmented_queries = $16, augmented_embedding_at = $17, origin = $18, updated_at = $19
			WHERE id = $20 AND namespace_id = $21 AND deleted_at IS NULL`
	}

	result, err := exec.ExecContext(ctx, query,
		mem.Content, mem.ContentHash, embeddingDim, source, tagsVal,
		mem.Confidence, mem.Importance, mem.AccessCount, lastAccessed,
		expiresAt, supersededBy, supersededAt, enrichedVal, string(mem.Metadata),
		purgeAfter, augmentedQueries, augmentedEmbeddingAt, origin, now, mem.ID.String(), mem.NamespaceID.String(),
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

	// Reload through the same exec so a transactional Update sees its own
	// uncommitted write.
	fetched, err := r.getByIDIncludeDeletedExec(ctx, exec, mem.ID, mem.NamespaceID)
	if err != nil {
		return fmt.Errorf("memory reload: %w", err)
	}
	*mem = *fetched
	return nil
}

// UpdateMetadata writes only the metadata column. It deliberately does
// NOT bump updated_at, so phases that stamp a "checked-by-X" sentinel
// can record the visit without invalidating their own staleness check
// (stamp < updated_at) on the next cycle. The caller is responsible for
// keeping any in-memory representation of metadata in sync; no reload
// is performed.
func (r *MemoryRepo) UpdateMetadata(ctx context.Context, id, namespaceID uuid.UUID, metadata json.RawMessage) error {
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	query := `UPDATE memories SET metadata = ? WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET metadata = $1 WHERE id = $2 AND namespace_id = $3 AND deleted_at IS NULL`
	}
	result, err := r.db.Exec(ctx, query, string(metadata), id.String(), namespaceID.String())
	if err != nil {
		return fmt.Errorf("memory update metadata: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("memory update metadata rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// ClearEmbeddingDim drops embedding_dim to NULL and bumps updated_at.
// Used by phases that detect a vector mismatch (e.g. embedding-backfill
// after a vector store eviction) so the row stops claiming a vector
// that no longer exists. Partial-column write so a concurrent
// memory_update that supersedes the row cannot be clobbered.
func (r *MemoryRepo) ClearEmbeddingDim(ctx context.Context, id, namespaceID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories SET embedding_dim = NULL, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET embedding_dim = NULL, updated_at = $1
			WHERE id = $2 AND namespace_id = $3 AND deleted_at IS NULL`
	}
	if _, err := r.db.Exec(ctx, query, now, id.String(), namespaceID.String()); err != nil {
		return fmt.Errorf("memory clear embedding_dim: %w", err)
	}
	return nil
}

// UpdateConfidence sets confidence and bumps updated_at. Used by the
// consolidation phase's confidence adjustment. Partial-column write so
// a concurrent memory_update that supersedes the row cannot have its
// supersede pointer clobbered.
func (r *MemoryRepo) UpdateConfidence(ctx context.Context, id, namespaceID uuid.UUID, confidence float64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories SET confidence = ?, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET confidence = $1, updated_at = $2
			WHERE id = $3 AND namespace_id = $4 AND deleted_at IS NULL`
	}
	if _, err := r.db.Exec(ctx, query, confidence, now, id.String(), namespaceID.String()); err != nil {
		return fmt.Errorf("memory update confidence: %w", err)
	}
	return nil
}

// Demote zeroes confidence, drops embedding_dim, replaces metadata, and
// bumps updated_at in a single statement. Used by the novelty audit
// when it demotes a low-novelty dream-source memory. Composite partial
// write, atomic at the row level so the demote can never partially
// land. Other columns (including superseded_by) are not touched, so a
// concurrent memory_update that supersedes the row keeps its chain
// pointer intact.
func (r *MemoryRepo) Demote(ctx context.Context, id, namespaceID uuid.UUID, metadata json.RawMessage) error {
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories SET confidence = 0, embedding_dim = NULL, metadata = ?, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories SET confidence = 0, embedding_dim = NULL, metadata = $1, updated_at = $2
			WHERE id = $3 AND namespace_id = $4 AND deleted_at IS NULL`
	}
	if _, err := r.db.Exec(ctx, query, string(metadata), now, id.String(), namespaceID.String()); err != nil {
		return fmt.Errorf("memory demote: %w", err)
	}
	return nil
}

// MarkSupersededBy points an existing memory's superseded_by at newID,
// stamps superseded_at and updated_at, and clears embedding_dim because
// the caller is about to purge the vector. The WHERE clause includes
// "AND superseded_by IS NULL" so two concurrent supersede writers
// cannot both win; the loser gets ErrConcurrentSupersede and rolls
// back. Used by the consolidation phase's supersedeOriginals and the
// worker's ingestion-decision UPDATE path; SupersedeReplacing is the
// fork that also creates the new memory atomically.
func (r *MemoryRepo) MarkSupersededBy(ctx context.Context, oldID, namespaceID, newID uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)
	query := `UPDATE memories
		SET superseded_by = ?, superseded_at = ?, embedding_dim = NULL, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL AND superseded_by IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE memories
			SET superseded_by = $1, superseded_at = $2, embedding_dim = NULL, updated_at = $3
			WHERE id = $4 AND namespace_id = $5 AND deleted_at IS NULL AND superseded_by IS NULL`
	}
	result, err := r.db.Exec(ctx, query,
		newID.String(), now, now, oldID.String(), namespaceID.String())
	if err != nil {
		return fmt.Errorf("memory mark superseded_by: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("memory mark superseded_by rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrConcurrentSupersede
	}
	return nil
}

// MarkEnriched flips the enriched flag to true, optionally sets
// embedding_dim, metadata, augmented_queries, and augmented_embedding_at,
// and bumps updated_at, all in one statement. Other columns are not
// touched. The enrichment worker uses this in finalize so a concurrent
// memory_update that supersedes the row mid-flight cannot have its
// supersede pointer clobbered by a stale full-row Update. Folding the
// augmentation marker into the same partial UPDATE makes the enriched
// flag and the augmentation marker land atomically per row, so a
// transient DB error cannot leave the row in the (enriched=true,
// augmented_embedding_at=NULL) state the backfill query targets.
//
// nil augmentedQueries or nil augmentedEmbeddingAt mean leave that
// column alone, matching how nil embeddingDim and empty metadata behave.
// This method does NOT provide a way to NULL the augmented columns
// once set; callers needing to reset a stale marker must add a
// dedicated method or issue a targeted UPDATE. The two augmented
// params are intended to be passed as a coupled pair (both nil or
// both non-nil); callers that mix nil and non-nil will produce a row
// with one column written and the other untouched.
//
// Soft-deleted rows (deleted_at IS NOT NULL) cannot be marked
// enriched: the WHERE clause excludes tombstones, so MarkEnriched
// returns sql.ErrNoRows for any row that was soft-deleted between
// claim and finalize. This also means the augmented columns will not
// be written on a tombstone, which differs from the prior dedicated
// UpdateAugmentedEmbedding method (which had no soft-delete filter).
func (r *MemoryRepo) MarkEnriched(
	ctx context.Context,
	id, namespaceID uuid.UUID,
	embeddingDim *int,
	metadata json.RawMessage,
	augmentedQueries []string,
	augmentedEmbeddingAt *time.Time,
	entityExtractedAt *time.Time,
) error {
	now := time.Now().UTC().Format(time.RFC3339)
	pg := r.db.Backend() == BackendPostgres

	setClauses := []string{"enriched = "}
	args := []any{EncodeBool(r.db.Backend(), true)}
	if embeddingDim != nil {
		setClauses = append(setClauses, "embedding_dim = ")
		args = append(args, *embeddingDim)
	}
	if len(metadata) > 0 {
		setClauses = append(setClauses, "metadata = ")
		args = append(args, string(metadata))
	}
	if augmentedQueries != nil {
		raw, err := json.Marshal(augmentedQueries)
		if err != nil {
			return fmt.Errorf("memory marshal augmented_queries: %w", err)
		}
		setClauses = append(setClauses, "augmented_queries = ")
		args = append(args, string(raw))
	}
	if augmentedEmbeddingAt != nil {
		setClauses = append(setClauses, "augmented_embedding_at = ")
		args = append(args, augmentedEmbeddingAt.UTC().Format(time.RFC3339))
	}
	if entityExtractedAt != nil {
		setClauses = append(setClauses, "entity_extracted_at = ")
		args = append(args, entityExtractedAt.UTC().Format(time.RFC3339))
	}
	setClauses = append(setClauses, "updated_at = ")
	args = append(args, now)
	args = append(args, id.String(), namespaceID.String())

	var setSQL strings.Builder
	for i, clause := range setClauses {
		if i > 0 {
			setSQL.WriteString(", ")
		}
		setSQL.WriteString(clause)
		if pg {
			fmt.Fprintf(&setSQL, "$%d", i+1)
		} else {
			setSQL.WriteString("?")
		}
	}
	var whereSQL string
	if pg {
		whereSQL = fmt.Sprintf("WHERE id = $%d AND namespace_id = $%d AND deleted_at IS NULL",
			len(setClauses)+1, len(setClauses)+2)
	} else {
		whereSQL = "WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL"
	}

	query := "UPDATE memories SET " + setSQL.String() + " " + whereSQL
	result, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("memory mark enriched: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("memory mark enriched rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}
	return nil
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
	placeholders, idArgs := uuidInPlaceholders(r.db, ids, 3)
	args := make([]any, 0, 2+len(ids))
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
// clamped to the given floor. multiplier should be in (0, 1]; values less
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
	placeholders, idArgs := uuidInPlaceholders(r.db, ids, 3)
	args := make([]any, 0, 2+len(ids))
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
// purge errors are not propagated; the row-level state change is the
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
// lifecycle; the vector store is an index, not the source of truth.
func (r *MemoryRepo) purgeVector(ctx context.Context, id uuid.UUID) {
	if r.vectorStore == nil {
		return
	}
	_ = r.vectorStore.Delete(ctx, VectorKindMemory, id)
}

// FindMemoriesMissingVector returns memories that record an embedding_dim
// equal to dim but have no corresponding row in the dim's vector table.
// Used by the embedding-backfill phase to repair drift between
// memories.embedding_dim and memory_vectors_<dim>. Soft-deleted,
// superseded, and demoted (confidence = 0) rows are excluded so the
// repair targets only memories that should currently be searchable.
func (r *MemoryRepo) FindMemoriesMissingVector(ctx context.Context, namespaceID uuid.UUID, dim, limit int) ([]model.Memory, error) {
	if !SupportedVectorDimensions[dim] {
		return nil, fmt.Errorf("memory find missing vector: unsupported dim %d", dim)
	}
	if limit <= 0 {
		return nil, nil
	}

	selectCols := memoryColumnsAliased("m")

	var query string
	var args []any
	if r.db.Backend() == BackendPostgres {
		table := fmt.Sprintf("memory_vectors_%d", dim)
		query = selectCols + ` FROM memories m
			LEFT JOIN ` + table + ` v ON v.memory_id = m.id
			WHERE m.namespace_id = $1
			  AND m.deleted_at IS NULL
			  AND m.superseded_by IS NULL
			  AND m.confidence > 0
			  AND m.embedding_dim = $2
			  AND v.memory_id IS NULL
			ORDER BY m.created_at DESC
			LIMIT $3`
		args = []any{namespaceID.String(), dim, limit}
	} else {
		query = selectCols + ` FROM memories m
			LEFT JOIN memory_vectors v ON v.memory_id = m.id AND v.dimension = ?
			WHERE m.namespace_id = ?
			  AND m.deleted_at IS NULL
			  AND m.superseded_by IS NULL
			  AND m.confidence > 0
			  AND m.embedding_dim = ?
			  AND v.memory_id IS NULL
			ORDER BY m.created_at DESC
			LIMIT ?`
		args = []any{dim, namespaceID.String(), dim, limit}
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory find missing vector: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, fmt.Errorf("memory find missing vector scan: %w", err)
		}
		out = append(out, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory find missing vector iteration: %w", err)
	}
	return out, nil
}

// FindMemoriesNullEmbeddingDim returns live, searchable (confidence > 0)
// memories with non-empty content whose embedding_dim is NULL — rows the
// enrichment embed never recorded a dim for. These are invisible to
// FindMemoriesMissingVector (which matches embedding_dim = dim), so the
// embedding-backfill phase uses this finder to heal them: re-embedding the
// genuinely vectorless ones and restamping any whose stored vector survived
// while the dim was lost (the desync the enrichment write path could leave
// before the worker.finalizeJob guard). Demoted rows (confidence = 0) are
// excluded so the repair targets only memories that should currently be
// searchable, matching FindMemoriesMissingVector. Pure memories-table query
// (no vector-table join) so it is identical across postgres and sqlite and
// independent of which vector store backs the deployment.
func (r *MemoryRepo) FindMemoriesNullEmbeddingDim(ctx context.Context, namespaceID uuid.UUID, limit int) ([]model.Memory, error) {
	if limit <= 0 {
		return nil, nil
	}

	selectCols := memoryColumnsAliased("m")

	var query string
	var args []any
	if r.db.Backend() == BackendPostgres {
		query = selectCols + ` FROM memories m
			WHERE m.namespace_id = $1
			  AND m.deleted_at IS NULL
			  AND m.superseded_by IS NULL
			  AND m.confidence > 0
			  AND m.embedding_dim IS NULL
			  AND m.content IS NOT NULL AND trim(m.content) <> ''
			ORDER BY m.created_at DESC
			LIMIT $2`
		args = []any{namespaceID.String(), limit}
	} else {
		query = selectCols + ` FROM memories m
			WHERE m.namespace_id = ?
			  AND m.deleted_at IS NULL
			  AND m.superseded_by IS NULL
			  AND m.confidence > 0
			  AND m.embedding_dim IS NULL
			  AND m.content IS NOT NULL AND trim(m.content) <> ''
			ORDER BY m.created_at DESC
			LIMIT ?`
		args = []any{namespaceID.String(), limit}
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("memory find null embedding_dim: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []model.Memory{}
	for rows.Next() {
		mem, err := r.scanMemoryFromRows(rows)
		if err != nil {
			return nil, fmt.Errorf("memory find null embedding_dim scan: %w", err)
		}
		out = append(out, *mem)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory find null embedding_dim iteration: %w", err)
	}
	return out, nil
}

// FindBySupersededBy returns the IDs of live memories whose superseded_by
// column equals id, scoped to namespaceID. Used by the forget service to
// walk supersede chains so forgetting the active head also forgets the
// older versions. Soft-deleted rows are excluded so an already-pruned
// ancestor is not re-visited.
func (r *MemoryRepo) FindBySupersededBy(ctx context.Context, namespaceID uuid.UUID, id uuid.UUID) ([]uuid.UUID, error) {
	query := `SELECT id FROM memories WHERE superseded_by = ? AND namespace_id = ? AND deleted_at IS NULL`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id FROM memories WHERE superseded_by = $1 AND namespace_id = $2 AND deleted_at IS NULL`
	}

	rows, err := r.db.Query(ctx, query, id.String(), namespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("memory find by superseded_by: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []uuid.UUID
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, fmt.Errorf("memory find by superseded_by scan: %w", err)
		}
		parsed, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("memory find by superseded_by parse: %w", err)
		}
		ids = append(ids, parsed)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory find by superseded_by iterate: %w", err)
	}
	return ids, nil
}

// ErrConcurrentSupersede is returned by SupersedeReplacing when the old
// memory row has already been superseded or soft-deleted by another
// writer. Callers should refresh the active head and retry against it.
var ErrConcurrentSupersede = fmt.Errorf("memory was concurrently superseded or deleted")

// SupersedeReplacing atomically performs the three writes that move a
// memory thread forward when content changes:
//  1. INSERT the new memory row (newMem) with a fresh UUID.
//  2. UPDATE the old memory row, setting superseded_by, superseded_at,
//     and updated_at. The old row's embedding_dim is left intact because
//     the old vector survives in the store; phase pruning eventually
//     soft-deletes the row and purges the vector together.
//  3. INSERT the lineage row marking new -> old as a supersedes edge.
//
// The UPDATE in step 2 includes "AND superseded_by IS NULL AND deleted_at
// IS NULL" so that two concurrent writers do not both append to the same
// chain. The losing writer gets ErrConcurrentSupersede and the entire
// transaction rolls back; no new memory or lineage row is left behind.
func (r *MemoryRepo) SupersedeReplacing(
	ctx context.Context,
	oldID uuid.UUID,
	newMem *model.Memory,
	lineage *model.MemoryLineage,
) error {
	if newMem == nil {
		return fmt.Errorf("supersede replacing: newMem is required")
	}
	if lineage == nil {
		return fmt.Errorf("supersede replacing: lineage is required")
	}

	insertMemQuery, insertMemArgs := r.memoryInsertArgs(newMem)

	if lineage.ID == uuid.Nil {
		lineage.ID = uuid.New()
	}
	if lineage.MemoryID == uuid.Nil {
		// The lineage row points new -> old; the caller cannot pre-set
		// memory_id because newMem.ID is assigned by memoryInsertArgs.
		lineage.MemoryID = newMem.ID
	}
	if lineage.Context == nil {
		lineage.Context = json.RawMessage(`{}`)
	}
	if lineage.CreatedAt.IsZero() {
		lineage.CreatedAt = newMem.CreatedAt
	}

	supersedeQuery := `UPDATE memories
		SET superseded_by = ?, superseded_at = ?, updated_at = ?
		WHERE id = ? AND namespace_id = ? AND deleted_at IS NULL AND superseded_by IS NULL`
	if r.db.Backend() == BackendPostgres {
		supersedeQuery = `UPDATE memories
			SET superseded_by = $1, superseded_at = $2, updated_at = $3
			WHERE id = $4 AND namespace_id = $5 AND deleted_at IS NULL AND superseded_by IS NULL`
	}

	var lineageParentID any
	if lineage.ParentID != nil {
		lineageParentID = lineage.ParentID.String()
	}

	insertLineageQuery := `INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context)
		VALUES (?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		insertLineageQuery = `INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context)
			VALUES ($1, $2, $3, $4, $5, $6)`
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("supersede replacing begin: %w", err)
	}

	if _, err := tx.ExecContext(ctx, insertMemQuery, insertMemArgs...); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("supersede replacing insert new memory: %w", err)
	}

	supersededAtStr := newMem.CreatedAt.UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx, supersedeQuery,
		newMem.ID.String(), supersededAtStr, supersededAtStr,
		oldID.String(), newMem.NamespaceID.String(),
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("supersede replacing update old memory: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("supersede replacing rows affected: %w", err)
	}
	if rowsAffected == 0 {
		_ = tx.Rollback()
		return ErrConcurrentSupersede
	}

	if _, err := tx.ExecContext(ctx, insertLineageQuery,
		lineage.ID.String(), lineage.NamespaceID.String(), lineage.MemoryID.String(), lineageParentID,
		lineage.Relation, string(lineage.Context),
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("supersede replacing insert lineage: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("supersede replacing commit: %w", err)
	}
	return nil
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

	args := []any{cutoffStr}
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
	defer func() { _ = rows.Close() }()

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

// HardDeleteByNamespaceTx permanently deletes all memories in a namespace
// inside the caller's transaction. Schema-level ON DELETE actions reap the
// memory's FK children (memory_lineage CASCADE, enrichment_queue CASCADE,
// relationships.source_memory SET NULL, token_usage.memory_id SET NULL).
func (r *MemoryRepo) HardDeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error {
	query := `DELETE FROM memories WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM memories WHERE namespace_id = $1`
	}
	if _, err := tx.ExecContext(ctx, query, namespaceID.String()); err != nil {
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
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = rows.Close() }()

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

// MemoryRank is one (id, score) pair from a lexical search. The Rank value
// is raw and unitless; sign and magnitude differ between backends (BM25 is
// lower-is-better, ts_rank_cd is higher-is-better). RRF only consumes the
// ordinal position of each row in the returned slice, so callers should
// treat Rank as opaque.
type MemoryRank struct {
	ID   uuid.UUID
	Rank float64
}

// SearchByText runs a backend-native lexical search over memories.content
// scoped to a single namespace and live (non-soft-deleted) rows. Rows are
// returned in best-first order, capped at limit.
//
// SQLite uses the FTS5 virtual table memories_fts (created in migration
// 000005) joined to memories for the namespace + soft-delete filter, with
// bm25() as the rank.
//
// Postgres uses the content_tsv generated tsvector column (added in
// migration 000018) with ts_rank_cd against plainto_tsquery('english', ?).
//
// An empty/whitespace query returns (nil, nil) without touching the DB.
// Backend-level query parse failures (rare) are returned as an empty slice
// rather than an error so the recall path can degrade gracefully when
// lexical input cannot be parsed.
func (r *MemoryRepo) SearchByText(ctx context.Context, namespaceID uuid.UUID, query string, limit int) ([]MemoryRank, error) {
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}
	if limit <= 0 {
		return nil, nil
	}

	var sql string
	var args []any
	if r.db.Backend() == BackendPostgres {
		// Compute plainto_tsquery once via CTE so it is reused in both
		// the rank expression and the @@ filter.
		sql = `WITH q AS (SELECT plainto_tsquery('english', $1) AS tsq)
			SELECT m.id, ts_rank_cd(m.content_tsv, q.tsq) AS rank
			FROM memories m, q
			WHERE m.namespace_id = $2
			  AND m.deleted_at IS NULL
			  AND m.content_tsv @@ q.tsq
			ORDER BY rank DESC
			LIMIT $3`
		args = []any{query, namespaceID.String(), limit}
	} else {
		sql = `SELECT m.id, bm25(memories_fts) AS rank
			FROM memories_fts
			JOIN memories m ON m.id = memories_fts.memory_id
			WHERE memories_fts MATCH ?
			  AND m.namespace_id = ?
			  AND m.deleted_at IS NULL
			ORDER BY rank ASC
			LIMIT ?`
		args = []any{query, namespaceID.String(), limit}
	}

	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		// Treat malformed queries (FTS5 syntax errors, plainto_tsquery
		// failures) as empty result. Recall must not error just because
		// a user typed `:` or `(` into the query box.
		return nil, nil
	}
	defer func() { _ = rows.Close() }()

	result := []MemoryRank{}
	for rows.Next() {
		var idStr string
		var rank float64
		if err := rows.Scan(&idStr, &rank); err != nil {
			return nil, fmt.Errorf("memory search by text scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("memory search by text parse: %w", err)
		}
		result = append(result, MemoryRank{ID: id, Rank: rank})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory search by text iteration: %w", err)
	}
	return result, nil
}

const selectMemoryColumns = `SELECT id, namespace_id, content, embedding_dim, source, tags,
	confidence, importance, access_count, last_accessed, expires_at, superseded_by,
	superseded_at, enriched, metadata, content_hash, created_at, updated_at, deleted_at, purge_after,
	augmented_queries, augmented_embedding_at, origin, faceted_at, facet_count, entity_extracted_at`

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
	var augmentedQueriesStr, augmentedEmbeddingAtStr, originStr, facetedAtStr, entityExtractedAtStr sql.NullString
	var facetCountVal sql.NullInt64

	err := row.Scan(
		&idStr, &namespaceIDStr, &mem.Content, &embeddingDim, &source, &tagsStr,
		&mem.Confidence, &mem.Importance, &mem.AccessCount, &lastAccessedStr,
		&expiresAtStr, &supersededByStr, &supersededAtStr, &enrichedBool, &metadataStr,
		&contentHashStr, &createdAtStr, &updatedAtStr, &deletedAtStr, &purgeAfterStr,
		&augmentedQueriesStr, &augmentedEmbeddingAtStr, &originStr, &facetedAtStr, &facetCountVal,
		&entityExtractedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateMemory(&mem, idStr, namespaceIDStr, tagsStr, metadataStr,
		createdAtStr, updatedAtStr, embeddingDim, source, lastAccessedStr,
		expiresAtStr, supersededByStr, supersededAtStr, contentHashStr,
		enrichedBool, deletedAtStr, purgeAfterStr,
		augmentedQueriesStr, augmentedEmbeddingAtStr, originStr, facetedAtStr, facetCountVal,
		entityExtractedAtStr)
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
	var augmentedQueriesStr, augmentedEmbeddingAtStr, originStr, facetedAtStr, entityExtractedAtStr sql.NullString
	var facetCountVal sql.NullInt64

	err := rows.Scan(
		&idStr, &namespaceIDStr, &mem.Content, &embeddingDim, &source, &tagsStr,
		&mem.Confidence, &mem.Importance, &mem.AccessCount, &lastAccessedStr,
		&expiresAtStr, &supersededByStr, &supersededAtStr, &enrichedBool, &metadataStr,
		&contentHashStr, &createdAtStr, &updatedAtStr, &deletedAtStr, &purgeAfterStr,
		&augmentedQueriesStr, &augmentedEmbeddingAtStr, &originStr, &facetedAtStr, &facetCountVal,
		&entityExtractedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("memory scan rows: %w", err)
	}

	return r.populateMemory(&mem, idStr, namespaceIDStr, tagsStr, metadataStr,
		createdAtStr, updatedAtStr, embeddingDim, source, lastAccessedStr,
		expiresAtStr, supersededByStr, supersededAtStr, contentHashStr,
		enrichedBool, deletedAtStr, purgeAfterStr,
		augmentedQueriesStr, augmentedEmbeddingAtStr, originStr, facetedAtStr, facetCountVal,
		entityExtractedAtStr)
}

func (r *MemoryRepo) populateMemory(
	mem *model.Memory,
	idStr, namespaceIDStr, tagsStr, metadataStr, createdAtStr, updatedAtStr string,
	embeddingDim sql.NullInt64,
	source, lastAccessedStr, expiresAtStr, supersededByStr, supersededAtStr, contentHashStr sql.NullString,
	enrichedBool bool,
	deletedAtStr, purgeAfterStr sql.NullString,
	augmentedQueriesStr, augmentedEmbeddingAtStr, originStr, facetedAtStr sql.NullString,
	facetCountVal sql.NullInt64,
	entityExtractedAtStr sql.NullString,
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

	// Empty/NULL origin (legacy rows, a NULL slipping through) reads back as
	// OriginUser rather than an empty (invalid) enum value.
	mem.Origin = model.MemoryOrigin(originStr.String).OrDefault()

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

	if augmentedQueriesStr.Valid && augmentedQueriesStr.String != "" && augmentedQueriesStr.String != "null" {
		var qs []string
		if err := json.Unmarshal([]byte(augmentedQueriesStr.String), &qs); err != nil {
			return nil, fmt.Errorf("memory parse augmented_queries: %w", err)
		}
		mem.AugmentedQueries = qs
	}

	if augmentedEmbeddingAtStr.Valid {
		t, err := time.Parse(time.RFC3339, augmentedEmbeddingAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse augmented_embedding_at: %w", err)
		}
		mem.AugmentedEmbeddingAt = &t
	}

	if facetedAtStr.Valid && facetedAtStr.String != "" {
		t, err := time.Parse(time.RFC3339, facetedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse faceted_at: %w", err)
		}
		mem.FacetedAt = &t
	}

	if facetCountVal.Valid {
		n := int(facetCountVal.Int64)
		mem.FacetCount = &n
	}

	if entityExtractedAtStr.Valid && entityExtractedAtStr.String != "" {
		t, err := time.Parse(time.RFC3339, entityExtractedAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("memory parse entity_extracted_at: %w", err)
		}
		mem.EntityExtractedAt = &t
	}

	return mem, nil
}
