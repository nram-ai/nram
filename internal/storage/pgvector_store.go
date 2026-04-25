package storage

import (
	"context"
	"fmt"

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
}

// Compile-time interface check.
var _ VectorStore = (*PgVectorStore)(nil)

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
	table       string          // dimension-specific vector table
	parent      string          // parent row table for namespace JOINs
	idColumn    string          // foreign-key column (memory_id / entity_id)
	softDeletes bool            // parent table has a deleted_at column
	dimTables   map[int]string  // every dim table for this kind, used for delete-all-dim
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

	query := fmt.Sprintf(
		`INSERT INTO %s (%s, embedding) VALUES ($1, $2)
		 ON CONFLICT (%s) DO UPDATE SET embedding = EXCLUDED.embedding`,
		spec.table, spec.idColumn, spec.idColumn,
	)

	_, err = s.pool.Exec(ctx, query, id, pgvector.NewVector(embedding))
	if err != nil {
		return fmt.Errorf("pgvector: upsert failed for table %s: %w", spec.table, err)
	}
	return nil
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
		query := fmt.Sprintf(
			`INSERT INTO %s (%s, embedding) VALUES ($1, $2)
			 ON CONFLICT (%s) DO UPDATE SET embedding = EXCLUDED.embedding`,
			spec.table, spec.idColumn, spec.idColumn,
		)
		for _, item := range group {
			batch.Queue(query, item.ID, pgvector.NewVector(item.Embedding))
		}

		br := tx.SendBatch(ctx, batch)
		for range group {
			if _, err := br.Exec(); err != nil {
				br.Close()
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

	query := fmt.Sprintf(
		`SELECT v.%s, 1 - (v.embedding <=> $1) AS score, p.namespace_id
		 FROM %s v
		 JOIN %s p ON v.%s = p.id
		 WHERE p.namespace_id = $2%s
		 ORDER BY v.embedding <=> $1
		 LIMIT $3`,
		spec.idColumn, spec.table, spec.parent, spec.idColumn, whereExtra,
	)

	rows, err := s.pool.Query(ctx, query, pgvector.NewVector(embedding), namespaceID, topK)
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

	query := fmt.Sprintf(`SELECT %s, embedding FROM %s WHERE %s = ANY($1)`, spec.idColumn, spec.table, spec.idColumn)
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
	defer br.Close()

	for range tables {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("pgvector: delete kind=%s id=%s failed: %w", kind, id, err)
		}
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
