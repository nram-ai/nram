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

// supportedDimensions enumerates the vector dimensions with corresponding tables.
var supportedDimensions = map[int]string{
	384:  "memory_vectors_384",
	512:  "memory_vectors_512",
	768:  "memory_vectors_768",
	1024: "memory_vectors_1024",
	1536: "memory_vectors_1536",
	3072: "memory_vectors_3072",
}

// allDimensionTables is the list of all dimension table names for delete operations.
var allDimensionTables = []string{
	"memory_vectors_384",
	"memory_vectors_512",
	"memory_vectors_768",
	"memory_vectors_1024",
	"memory_vectors_1536",
	"memory_vectors_3072",
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

// tableName maps a dimension to its memory_vectors table name.
// Returns an error if the dimension is not one of the supported sizes.
func tableName(dimension int) (string, error) {
	name, ok := supportedDimensions[dimension]
	if !ok {
		return "", fmt.Errorf("pgvector: unsupported dimension %d; supported: 384, 512, 768, 1024, 1536, 3072", dimension)
	}
	return name, nil
}

// Upsert inserts or updates a single vector in the appropriate dimension table.
func (s *PgVectorStore) Upsert(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	table, err := tableName(dimension)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (memory_id, embedding) VALUES ($1, $2)
		 ON CONFLICT (memory_id) DO UPDATE SET embedding = EXCLUDED.embedding`,
		table,
	)

	_, err = s.pool.Exec(ctx, query, id, pgvector.NewVector(embedding))
	if err != nil {
		return fmt.Errorf("pgvector: upsert failed for table %s: %w", table, err)
	}
	return nil
}

// UpsertBatch inserts or updates multiple vectors, grouping by dimension for efficiency.
func (s *PgVectorStore) UpsertBatch(ctx context.Context, items []VectorUpsertItem) error {
	if len(items) == 0 {
		return nil
	}

	// Group items by dimension.
	groups := make(map[int][]VectorUpsertItem)
	for _, item := range items {
		if _, err := tableName(item.Dimension); err != nil {
			return err
		}
		groups[item.Dimension] = append(groups[item.Dimension], item)
	}

	// Process each dimension group in a transaction.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgvector: failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for dim, group := range groups {
		table, _ := tableName(dim) // already validated above

		// Build a batch of upsert statements.
		batch := &pgx.Batch{}
		query := fmt.Sprintf(
			`INSERT INTO %s (memory_id, embedding) VALUES ($1, $2)
			 ON CONFLICT (memory_id) DO UPDATE SET embedding = EXCLUDED.embedding`,
			table,
		)
		for _, item := range group {
			batch.Queue(query, item.ID, pgvector.NewVector(item.Embedding))
		}

		br := tx.SendBatch(ctx, batch)
		for range group {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return fmt.Errorf("pgvector: batch upsert failed for table %s: %w", table, err)
			}
		}
		if err := br.Close(); err != nil {
			return fmt.Errorf("pgvector: batch close failed for table %s: %w", table, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgvector: failed to commit batch upsert: %w", err)
	}
	return nil
}

// Search finds the nearest vectors within a namespace using cosine distance.
// It joins the dimension table with the memories table for namespace scoping.
func (s *PgVectorStore) Search(ctx context.Context, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error) {
	table, err := tableName(dimension)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(
		`SELECT v.memory_id, 1 - (v.embedding <=> $1) AS score, m.namespace_id
		 FROM %s v
		 JOIN memories m ON v.memory_id = m.id
		 WHERE m.namespace_id = $2
		   AND m.deleted_at IS NULL
		 ORDER BY v.embedding <=> $1
		 LIMIT $3`,
		table,
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

func (s *PgVectorStore) GetByIDs(ctx context.Context, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error) {
	if len(ids) == 0 {
		return map[uuid.UUID][]float32{}, nil
	}
	table, err := tableName(dimension)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT memory_id, embedding FROM %s WHERE memory_id = ANY($1)`, table)
	rows, err := s.pool.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("pgvector: get-by-ids query failed for table %s: %w", table, err)
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

// Delete removes a vector from all dimension tables since the dimension is unknown.
func (s *PgVectorStore) Delete(ctx context.Context, id uuid.UUID) error {
	batch := &pgx.Batch{}
	for _, table := range allDimensionTables {
		batch.Queue(fmt.Sprintf("DELETE FROM %s WHERE memory_id = $1", table), id)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for _, table := range allDimensionTables {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("pgvector: delete from %s failed: %w", table, err)
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
