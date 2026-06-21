package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvector "github.com/pgvector/pgvector-go"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
)

// supportedMemoryDimensions enumerates the vector dimensions backed by a
// memory_vectors_<dim> table.
var supportedMemoryDimensions = map[int]string{
	384:  "memory_vectors_384",
	512:  "memory_vectors_512",
	768:  "memory_vectors_768",
	1024: "memory_vectors_1024",
	1536: "memory_vectors_1536",
	3072: "memory_vectors_3072",
}

// supportedEntityDimensions enumerates the vector dimensions backed by an
// entity_vectors_<dim> table (created in migration 000007_graph_tables).
var supportedEntityDimensions = map[int]string{
	384:  "entity_vectors_384",
	512:  "entity_vectors_512",
	768:  "entity_vectors_768",
	1024: "entity_vectors_1024",
	1536: "entity_vectors_1536",
	3072: "entity_vectors_3072",
}

// PgVectorStore implements VectorStore using PostgreSQL with pgvector.
type PgVectorStore struct {
	pool *pgxpool.Pool
	// maxFacetsFn resolves enrichment.multi_vector.max_facets so the faceted
	// Search over-fetch tracks the configured cap (see overFetchFor). Nil until
	// SetMaxFacetsResolver is called; nil falls back to the over-fetch floor.
	maxFacetsFn func() int
}

// SetMaxFacetsResolver injects the resolver for enrichment.multi_vector.max_facets
// so the faceted Search candidate window scales with the configured facet cap
// instead of a fixed multiplier. Wired at boot; safe to leave unset in tests.
func (s *PgVectorStore) SetMaxFacetsResolver(fn func() int) { s.maxFacetsFn = fn }

// Compile-time interface checks.
var (
	_ VectorStore      = (*PgVectorStore)(nil)
	_ FacetVectorStore = (*PgVectorStore)(nil)
)

// NewPgVectorStore creates a new PgVectorStore from the given DSN.
// It creates a pgxpool.Pool with AfterConnect that registers pgvector types.
func NewPgVectorStore(dsn string) (*PgVectorStore, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgvector: failed to parse config: %w", err)
	}

	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("pgvector: failed to create pool: %w", err)
	}

	return &PgVectorStore{pool: pool}, nil
}

// NewPgVectorStoreFromPool creates a new PgVectorStore from an existing pgxpool.Pool.
// The caller is responsible for ensuring pgvector types are registered via AfterConnect.
func NewPgVectorStoreFromPool(pool *pgxpool.Pool) *PgVectorStore {
	return &PgVectorStore{pool: pool}
}

// pgvectorTableSpec captures everything routing needs to know to dispatch a
// query to the correct family of tables.
type pgvectorTableSpec struct {
	table       string         // dimension-specific vector table
	parent      string         // parent row table for namespace JOINs
	idColumn    string         // foreign-key column (memory_id / entity_id)
	softDeletes bool           // parent table has a deleted_at column
	faceted     bool           // table carries facet_id (memory tables only)
	dimTables   map[int]string // every dim table for this kind, used for delete-all-dim
}

func dimTablesForKind(kind VectorKind) map[int]string {
	if kind == VectorKindEntity {
		return supportedEntityDimensions
	}
	return supportedMemoryDimensions
}

// resolveTableSpec maps (Kind, dimension) to the routing spec. Returns an error
// when the dimension is not supported by the chosen kind.
func resolveTableSpec(kind VectorKind, dimension int) (pgvectorTableSpec, error) {
	switch kind {
	case "", VectorKindMemory:
		name, ok := supportedMemoryDimensions[dimension]
		if !ok {
			return pgvectorTableSpec{}, fmt.Errorf("pgvector: unsupported memory dimension %d; supported: 384, 512, 768, 1024, 1536, 3072", dimension)
		}
		return pgvectorTableSpec{
			table:       name,
			parent:      "memories",
			idColumn:    "memory_id",
			softDeletes: true,
			faceted:     true,
			dimTables:   supportedMemoryDimensions,
		}, nil
	case VectorKindEntity:
		name, ok := supportedEntityDimensions[dimension]
		if !ok {
			return pgvectorTableSpec{}, fmt.Errorf("pgvector: unsupported entity dimension %d; supported: 384, 512, 768, 1024, 1536, 3072", dimension)
		}
		return pgvectorTableSpec{
			table:     name,
			parent:    "entities",
			idColumn:  "entity_id",
			dimTables: supportedEntityDimensions,
		}, nil
	default:
		return pgvectorTableSpec{}, fmt.Errorf("pgvector: unknown vector kind %q", kind)
	}
}

// Upsert inserts or updates a single vector in the appropriate dimension table.
func (s *PgVectorStore) Upsert(ctx context.Context, kind VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	spec, err := resolveTableSpec(kind, dimension)
	if err != nil {
		return err
	}

	query := upsertQuery(spec)
	_, err = s.pool.Exec(ctx, query, id, pgvector.NewVector(embedding))
	if err != nil {
		return fmt.Errorf("pgvector: upsert failed for table %s: %w", spec.table, err)
	}
	return nil
}

// upsertQuery builds the single-vector upsert for a spec. For faceted (memory)
// tables it targets facet 0 with the composite-key conflict target so it never
// clobbers topic facets; for entity tables it conflicts on the bare id column.
func upsertQuery(spec pgvectorTableSpec) string {
	if spec.faceted {
		return fmt.Sprintf(
			`INSERT INTO %s (%s, facet_id, embedding) VALUES ($1, 0, $2)
			 ON CONFLICT (%s, facet_id) DO UPDATE SET embedding = EXCLUDED.embedding`,
			spec.table, spec.idColumn, spec.idColumn,
		)
	}
	return fmt.Sprintf(
		`INSERT INTO %s (%s, embedding) VALUES ($1, $2)
		 ON CONFLICT (%s) DO UPDATE SET embedding = EXCLUDED.embedding`,
		spec.table, spec.idColumn, spec.idColumn,
	)
}

// UpsertBatch inserts or updates multiple vectors, grouping by (kind, dimension)
// for efficiency.
func (s *PgVectorStore) UpsertBatch(ctx context.Context, items []VectorUpsertItem) error {
	if len(items) == 0 {
		return nil
	}

	type batchKey struct {
		kind VectorKind
		dim  int
	}

	// Validate every item up front and group by (kind, dimension).
	groups := make(map[batchKey][]VectorUpsertItem)
	for _, item := range items {
		k := item.EffectiveKind()
		if _, err := resolveTableSpec(k, item.Dimension); err != nil {
			return err
		}
		key := batchKey{kind: k, dim: item.Dimension}
		groups[key] = append(groups[key], item)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgvector: failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for key, group := range groups {
		spec, _ := resolveTableSpec(key.kind, key.dim) // already validated above

		batch := &pgx.Batch{}
		query := upsertQuery(spec)
		for _, item := range group {
			batch.Queue(query, item.ID, pgvector.NewVector(item.Embedding))
		}

		br := tx.SendBatch(ctx, batch)
		for range group {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return fmt.Errorf("pgvector: batch upsert failed for table %s: %w", spec.table, err)
			}
		}
		if err := br.Close(); err != nil {
			return fmt.Errorf("pgvector: batch close failed for table %s: %w", spec.table, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgvector: failed to commit batch upsert: %w", err)
	}
	return nil
}

// UpsertFacets atomically replaces a memory's entire facet set at the given
// dimension. facets[0] is facet 0 (the pooled whole-memory vector); facets[1:]
// are topic facets written at facet_id = their slice index. The delete-then-
// insert runs in one transaction so a concurrent reader never sees a partial
// set. An empty slice removes all facets for the memory at that dimension.
func (s *PgVectorStore) UpsertFacets(ctx context.Context, memoryID uuid.UUID, _ uuid.UUID, dimension int, facets [][]float32) error {
	spec, err := resolveTableSpec(VectorKindMemory, dimension)
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgvector: upsert-facets begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = $1", spec.table, spec.idColumn), memoryID); err != nil {
		return fmt.Errorf("pgvector: upsert-facets clear %s: %w", spec.table, err)
	}

	if len(facets) > 0 {
		insert := fmt.Sprintf("INSERT INTO %s (%s, facet_id, embedding) VALUES ($1, $2, $3)", spec.table, spec.idColumn)
		batch := &pgx.Batch{}
		for i, vec := range facets {
			batch.Queue(insert, memoryID, i, pgvector.NewVector(vec))
		}
		br := tx.SendBatch(ctx, batch)
		for range facets {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return fmt.Errorf("pgvector: upsert-facets insert %s: %w", spec.table, err)
			}
		}
		if err := br.Close(); err != nil {
			return fmt.Errorf("pgvector: upsert-facets batch close %s: %w", spec.table, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgvector: upsert-facets commit: %w", err)
	}
	return nil
}

// Search finds the nearest vectors within a namespace using cosine distance.
// It joins the dimension table with the parent (memories or entities) for
// namespace scoping. Memory searches additionally exclude soft-deleted rows;
// entity rows are not soft-deleted.
func (s *PgVectorStore) Search(ctx context.Context, kind VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error) {
	spec, err := resolveTableSpec(kind, dimension)
	if err != nil {
		return nil, err
	}

	whereExtra := ""
	if spec.softDeletes {
		whereExtra = " AND p.deleted_at IS NULL"
	}

	var query string
	var args []any
	if spec.faceted {
		// Multiple facet rows can share a memory_id. Scan the index-ordered top
		// (topK * overFetch) candidate facet rows, keep the best (nearest) facet
		// per memory via ROW_NUMBER, then take topK distinct memories. The inner
		// ORDER BY ... LIMIT uses the hnsw index; the window runs over only that
		// bounded candidate set. Score = 1 - min(distance) = best facet.
		query = fmt.Sprintf(
			`SELECT %s, score, namespace_id FROM (
			   SELECT %s, score, namespace_id,
			          ROW_NUMBER() OVER (PARTITION BY %s ORDER BY score DESC) AS rn
			   FROM (
			     SELECT v.%s, 1 - (v.embedding <=> $1) AS score, p.namespace_id
			     FROM %s v
			     JOIN %s p ON v.%s = p.id
			     WHERE p.namespace_id = $2%s
			     ORDER BY v.embedding <=> $1
			     LIMIT $3
			   ) cand
			 ) ranked
			 WHERE rn = 1
			 ORDER BY score DESC
			 LIMIT $4`,
			spec.idColumn, spec.idColumn, spec.idColumn, spec.idColumn,
			spec.table, spec.parent, spec.idColumn, whereExtra,
		)
		args = []any{pgvector.NewVector(embedding), namespaceID, topK * overFetchFor(s.maxFacetsFn), topK}
	} else {
		query = fmt.Sprintf(
			`SELECT v.%s, 1 - (v.embedding <=> $1) AS score, p.namespace_id
			 FROM %s v
			 JOIN %s p ON v.%s = p.id
			 WHERE p.namespace_id = $2%s
			 ORDER BY v.embedding <=> $1
			 LIMIT $3`,
			spec.idColumn, spec.table, spec.parent, spec.idColumn, whereExtra,
		)
		args = []any{pgvector.NewVector(embedding), namespaceID, topK}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgvector: search query failed: %w", err)
	}
	defer rows.Close()

	var results []VectorSearchResult
	for rows.Next() {
		var r VectorSearchResult
		if err := rows.Scan(&r.ID, &r.Score, &r.NamespaceID); err != nil {
			return nil, fmt.Errorf("pgvector: search scan failed: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgvector: search rows error: %w", err)
	}

	return results, nil
}

func (s *PgVectorStore) GetByIDs(ctx context.Context, kind VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error) {
	if len(ids) == 0 {
		return map[uuid.UUID][]float32{}, nil
	}
	spec, err := resolveTableSpec(kind, dimension)
	if err != nil {
		return nil, err
	}

	// Faceted (memory) tables hold multiple rows per id; callers of GetByIDs want
	// the pooled whole-memory vector, which is facet 0.
	facetFilter := ""
	if spec.faceted {
		facetFilter = " AND facet_id = 0"
	}
	query := fmt.Sprintf(`SELECT %s, embedding FROM %s WHERE %s = ANY($1)%s`, spec.idColumn, spec.table, spec.idColumn, facetFilter)
	rows, err := s.pool.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("pgvector: get-by-ids query failed for table %s: %w", spec.table, err)
	}
	defer rows.Close()

	out := make(map[uuid.UUID][]float32, len(ids))
	for rows.Next() {
		var id uuid.UUID
		var vec pgvector.Vector
		if err := rows.Scan(&id, &vec); err != nil {
			return nil, fmt.Errorf("pgvector: get-by-ids scan failed: %w", err)
		}
		out[id] = vec.Slice()
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgvector: get-by-ids rows error: %w", err)
	}

	return out, nil
}

// Delete removes a vector from every dimension table for the given kind, since
// the dimension is unknown at delete time.
func (s *PgVectorStore) Delete(ctx context.Context, kind VectorKind, id uuid.UUID) error {
	idCol := "memory_id"
	if kind == VectorKindEntity {
		idCol = "entity_id"
	} else if kind != "" && kind != VectorKindMemory {
		return fmt.Errorf("pgvector: unknown vector kind %q", kind)
	}
	tables := dimTablesForKind(kind)

	batch := &pgx.Batch{}
	for _, table := range tables {
		batch.Queue(fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, idCol), id)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for range tables {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("pgvector: delete kind=%s id=%s failed: %w", kind, id, err)
		}
	}
	return nil
}

// TruncateAllVectors clears every memory_vectors_<dim> and
// entity_vectors_<dim> table in one statement. Schema is preserved
// (TRUNCATE, not DROP). Multi-table TRUNCATE is atomic and acquires the
// table locks in one phase, so no explicit transaction wrapper is needed.
func (s *PgVectorStore) TruncateAllVectors(ctx context.Context) error {
	tables := make([]string, 0, len(supportedMemoryDimensions)+len(supportedEntityDimensions))
	for _, t := range supportedMemoryDimensions {
		tables = append(tables, t)
	}
	for _, t := range supportedEntityDimensions {
		tables = append(tables, t)
	}
	if _, err := s.pool.Exec(ctx, "TRUNCATE TABLE "+strings.Join(tables, ", ")); err != nil {
		return fmt.Errorf("pgvector: truncate all vector tables: %w", err)
	}
	return nil
}

// Ping verifies connectivity to the PostgreSQL database.
func (s *PgVectorStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases the connection pool resources.
func (s *PgVectorStore) Close() {
	s.pool.Close()
}
