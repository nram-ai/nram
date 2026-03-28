package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// HNSWConfig holds configuration for the HNSW vector store.
type HNSWConfig struct {
	M                int
	EfConstruction   int
	EfSearch         int
	MaxLoadedIndexes int
	SnapshotInterval time.Duration
}

// DefaultHNSWConfig returns sensible defaults for HNSW configuration.
func DefaultHNSWConfig() HNSWConfig {
	return HNSWConfig{
		M:                16,
		EfConstruction:   200,
		EfSearch:         50,
		MaxLoadedIndexes: 64,
		SnapshotInterval: 60 * time.Second,
	}
}

// HNSWStore implements VectorStore using a pure-Go HNSW index backed by SQLite.
type HNSWStore struct {
	readDB  *sql.DB
	writeDB *sql.DB
	cache   *hnsw.IndexCache
}

// Compile-time interface check.
var _ VectorStore = (*HNSWStore)(nil)

// NewHNSWStore creates a new HNSWStore backed by the given SQLite database.
// readDB is used for loading vectors; writeDB is used for upserts, deletes,
// and snapshot persistence.
func NewHNSWStore(readDB, writeDB *sql.DB, cfg HNSWConfig) *HNSWStore {
	// Apply defaults for zero-valued config fields.
	if cfg.M <= 0 {
		cfg.M = 16
	}
	if cfg.EfConstruction <= 0 {
		cfg.EfConstruction = 200
	}
	if cfg.EfSearch <= 0 {
		cfg.EfSearch = 50
	}
	if cfg.MaxLoadedIndexes <= 0 {
		cfg.MaxLoadedIndexes = 64
	}
	if cfg.SnapshotInterval <= 0 {
		cfg.SnapshotInterval = 60 * time.Second
	}

	cacheCfg := hnsw.CacheConfig{
		MaxIndexes:       cfg.MaxLoadedIndexes,
		SnapshotInterval: cfg.SnapshotInterval,
		GraphOpts: []hnsw.Option{
			hnsw.WithM(cfg.M),
			hnsw.WithEfConstruction(cfg.EfConstruction),
			hnsw.WithEfSearch(cfg.EfSearch),
		},
	}

	return &HNSWStore{
		readDB:  readDB,
		writeDB: writeDB,
		cache:   hnsw.NewIndexCache(readDB, writeDB, cacheCfg),
	}
}

// Upsert inserts or updates a single vector associated with a memory.
func (s *HNSWStore) Upsert(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	if !SupportedVectorDimensions[dimension] {
		return fmt.Errorf("hnsw: unsupported dimension %d", dimension)
	}

	encoded := hnsw.EncodeVector(embedding)
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	_, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO memory_vectors (memory_id, namespace_id, dimension, embedding, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
		  namespace_id = excluded.namespace_id,
		  dimension = excluded.dimension,
		  embedding = excluded.embedding,
		  updated_at = excluded.updated_at`,
		id.String(), namespaceID.String(), dimension, encoded, now, now,
	)
	if err != nil {
		return fmt.Errorf("hnsw: upsert memory_vectors: %w", err)
	}

	graph, err := s.cache.GetOrCreate(ctx, namespaceID, dimension)
	if err != nil {
		return fmt.Errorf("hnsw: get or create index: %w", err)
	}

	// HNSW Add handles update semantics internally (removes existing node first).
	if err := graph.Add(hnsw.Node{ID: id, Vector: embedding}); err != nil {
		return fmt.Errorf("hnsw: add node: %w", err)
	}

	s.cache.MarkDirty(namespaceID, dimension)
	return nil
}

// UpsertBatch inserts or updates multiple vectors in a single operation.
func (s *HNSWStore) UpsertBatch(ctx context.Context, items []VectorUpsertItem) error {
	if len(items) == 0 {
		return nil
	}

	// Validate all dimensions first.
	for _, item := range items {
		if !SupportedVectorDimensions[item.Dimension] {
			return fmt.Errorf("hnsw: unsupported dimension %d", item.Dimension)
		}
	}

	// Group items by (namespaceID, dimension).
	type groupKey struct {
		NamespaceID uuid.UUID
		Dimension   int
	}
	groups := make(map[groupKey][]VectorUpsertItem)
	for _, item := range items {
		key := groupKey{NamespaceID: item.NamespaceID, Dimension: item.Dimension}
		groups[key] = append(groups[key], item)
	}

	for gk, group := range groups {
		// Insert all SQLite rows in a transaction.
		tx, err := s.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("hnsw: begin transaction: %w", err)
		}

		now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO memory_vectors (memory_id, namespace_id, dimension, embedding, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(memory_id) DO UPDATE SET
			  namespace_id = excluded.namespace_id,
			  dimension = excluded.dimension,
			  embedding = excluded.embedding,
			  updated_at = excluded.updated_at`)
		if err != nil {
			tx.Rollback() //nolint:errcheck
			return fmt.Errorf("hnsw: prepare batch insert: %w", err)
		}

		for _, item := range group {
			encoded := hnsw.EncodeVector(item.Embedding)
			_, err := stmt.ExecContext(ctx, item.ID.String(), item.NamespaceID.String(), item.Dimension, encoded, now, now)
			if err != nil {
				stmt.Close() //nolint:errcheck
				tx.Rollback() //nolint:errcheck
				return fmt.Errorf("hnsw: batch insert item %s: %w", item.ID, err)
			}
		}
		stmt.Close() //nolint:errcheck

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("hnsw: commit batch insert: %w", err)
		}

		// Load the HNSW index and add all nodes.
		graph, err := s.cache.GetOrCreate(ctx, gk.NamespaceID, gk.Dimension)
		if err != nil {
			return fmt.Errorf("hnsw: get or create index for batch: %w", err)
		}

		for _, item := range group {
			if err := graph.Add(hnsw.Node{ID: item.ID, Vector: item.Embedding}); err != nil {
				return fmt.Errorf("hnsw: batch add node %s: %w", item.ID, err)
			}
		}

		s.cache.MarkDirty(gk.NamespaceID, gk.Dimension)
	}

	return nil
}

// Search finds the nearest neighbor vectors within a namespace, returning up to topK results.
func (s *HNSWStore) Search(ctx context.Context, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error) {
	if !SupportedVectorDimensions[dimension] {
		return nil, fmt.Errorf("hnsw: unsupported dimension %d", dimension)
	}

	graph, err := s.cache.GetOrCreate(ctx, namespaceID, dimension)
	if err != nil {
		return nil, fmt.Errorf("hnsw: get or create index for search: %w", err)
	}

	if graph.Len() == 0 {
		return []VectorSearchResult{}, nil
	}

	results, err := graph.Search(embedding, topK)
	if err != nil {
		return nil, fmt.Errorf("hnsw: search: %w", err)
	}

	out := make([]VectorSearchResult, len(results))
	for i, r := range results {
		out[i] = VectorSearchResult{
			ID:          r.ID,
			Score:       r.Score,
			NamespaceID: namespaceID,
		}
	}
	return out, nil
}

// Delete removes a vector by its associated memory ID.
func (s *HNSWStore) Delete(ctx context.Context, id uuid.UUID) error {
	// Look up the memory_vectors row to get namespace_id and dimension.
	var nsIDStr string
	var dimension int
	err := s.readDB.QueryRowContext(ctx,
		"SELECT namespace_id, dimension FROM memory_vectors WHERE memory_id = ?",
		id.String(),
	).Scan(&nsIDStr, &dimension)
	if err == sql.ErrNoRows {
		return nil // Already deleted.
	}
	if err != nil {
		return fmt.Errorf("hnsw: lookup memory_vectors for delete: %w", err)
	}

	nsID, err := uuid.Parse(nsIDStr)
	if err != nil {
		return fmt.Errorf("hnsw: parse namespace_id %q: %w", nsIDStr, err)
	}

	// Delete from SQLite.
	_, err = s.writeDB.ExecContext(ctx, "DELETE FROM memory_vectors WHERE memory_id = ?", id.String())
	if err != nil {
		return fmt.Errorf("hnsw: delete from memory_vectors: %w", err)
	}

	// Remove from the HNSW index if it's loaded in cache.
	// We use GetOrCreate to check — if the graph is loaded it's a fast cache hit.
	// If it's not loaded, we load it (which will reflect the deletion from SQLite).
	graph, err := s.cache.GetOrCreate(ctx, nsID, dimension)
	if err != nil {
		// The deletion is persisted in SQLite; the graph will be correct on next load.
		return nil
	}

	graph.Delete(id)
	s.cache.MarkDirty(nsID, dimension)

	return nil
}

// DeleteByNamespace removes all HNSW snapshots for a given namespace.
func (s *HNSWStore) DeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error {
	_, err := s.writeDB.ExecContext(ctx,
		"DELETE FROM hnsw_snapshots WHERE namespace_id = ?",
		namespaceID.String(),
	)
	if err != nil {
		return fmt.Errorf("hnsw: delete snapshots by namespace: %w", err)
	}
	return nil
}

// Ping checks vector store connectivity by pinging the underlying SQLite database.
func (s *HNSWStore) Ping(ctx context.Context) error {
	return s.readDB.PingContext(ctx)
}

// Close stops the background snapshot goroutine and flushes all dirty snapshots.
func (s *HNSWStore) Close() error {
	return s.cache.Close()
}
