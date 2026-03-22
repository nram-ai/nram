package storage_test

import (
	"context"
	"database/sql"
	"math"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
	_ "modernc.org/sqlite"
)

// setupHNSWTestDB creates an in-memory SQLite database with the required tables.
func setupHNSWTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	// Enable WAL mode for concurrent reads.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("set WAL mode: %v", err)
	}

	// Create memory_vectors table.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS memory_vectors (
			memory_id TEXT PRIMARY KEY,
			namespace_id TEXT NOT NULL,
			dimension INTEGER NOT NULL,
			embedding BLOB NOT NULL,
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
		)`)
	if err != nil {
		t.Fatalf("create memory_vectors: %v", err)
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_memory_vectors_ns_dim ON memory_vectors(namespace_id, dimension)`)
	if err != nil {
		t.Fatalf("create index: %v", err)
	}

	// Create hnsw_snapshots table.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS hnsw_snapshots (
			namespace_id TEXT NOT NULL,
			dimension INTEGER NOT NULL,
			graph_data BLOB NOT NULL,
			node_count INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			PRIMARY KEY (namespace_id, dimension)
		)`)
	if err != nil {
		t.Fatalf("create hnsw_snapshots: %v", err)
	}

	t.Cleanup(func() { db.Close() })
	return db
}

// randomVector generates a random float32 vector of the given dimension.
func randomVector(dim int, seed int64) []float32 {
	rng := rand.New(rand.NewSource(seed))
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

// normalizeVector returns a unit-length copy of v.
func normalizeVector(v []float32) []float32 {
	var sum float64
	for _, f := range v {
		sum += float64(f) * float64(f)
	}
	norm := float32(math.Sqrt(sum))
	out := make([]float32, len(v))
	for i, f := range v {
		out[i] = f / norm
	}
	return out
}

func TestHNSWStoreUpsertAndSearch(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1 // effectively disable background snapshots for tests
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384

	// Upsert 10 vectors.
	vectors := make([][]float32, 10)
	ids := make([]uuid.UUID, 10)
	for i := 0; i < 10; i++ {
		ids[i] = uuid.New()
		vectors[i] = normalizeVector(randomVector(dim, int64(i+1)))
		err := store.Upsert(ctx, ids[i], nsID, vectors[i], dim)
		if err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	// Search for the nearest neighbor of vectors[0] — it should be vectors[0] itself.
	results, err := store.Search(ctx, vectors[0], nsID, dim, 3)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected at least one search result")
	}

	// The top result should be vectors[0] with a score very close to 1.0.
	if results[0].ID != ids[0] {
		t.Errorf("expected top result ID %s, got %s", ids[0], results[0].ID)
	}
	if results[0].Score < 0.99 {
		t.Errorf("expected top result score >= 0.99, got %f", results[0].Score)
	}
	if results[0].NamespaceID != nsID {
		t.Errorf("expected namespace_id %s, got %s", nsID, results[0].NamespaceID)
	}
}

func TestHNSWStoreUpsertBatch(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384

	items := make([]storage.VectorUpsertItem, 50)
	for i := range items {
		items[i] = storage.VectorUpsertItem{
			ID:          uuid.New(),
			NamespaceID: nsID,
			Embedding:   normalizeVector(randomVector(dim, int64(i+100))),
			Dimension:   dim,
		}
	}

	err := store.UpsertBatch(ctx, items)
	if err != nil {
		t.Fatalf("upsert batch: %v", err)
	}

	// Search for the first item.
	results, err := store.Search(ctx, items[0].Embedding, nsID, dim, 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected at least one result")
	}
	if results[0].ID != items[0].ID {
		t.Errorf("expected top result ID %s, got %s", items[0].ID, results[0].ID)
	}
}

func TestHNSWStoreDelete(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384
	id := uuid.New()
	vec := normalizeVector(randomVector(dim, 42))

	// Upsert a vector.
	if err := store.Upsert(ctx, id, nsID, vec, dim); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Verify it's found.
	results, err := store.Search(ctx, vec, nsID, dim, 1)
	if err != nil {
		t.Fatalf("search before delete: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result before delete, got %d", len(results))
	}

	// Delete.
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Search again — should be empty.
	results, err = store.Search(ctx, vec, nsID, dim, 1)
	if err != nil {
		t.Fatalf("search after delete: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results after delete, got %d", len(results))
	}

	// Delete again — should be a no-op.
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("double delete: %v", err)
	}
}

func TestHNSWStoreNamespaceIsolation(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	ns1 := uuid.New()
	ns2 := uuid.New()
	dim := 384

	// Use a known vector for ns1 and a different one for ns2.
	vec1 := normalizeVector(randomVector(dim, 1))
	vec2 := normalizeVector(randomVector(dim, 2))
	id1 := uuid.New()
	id2 := uuid.New()

	if err := store.Upsert(ctx, id1, ns1, vec1, dim); err != nil {
		t.Fatalf("upsert ns1: %v", err)
	}
	if err := store.Upsert(ctx, id2, ns2, vec2, dim); err != nil {
		t.Fatalf("upsert ns2: %v", err)
	}

	// Search in ns1 — should only find id1.
	results, err := store.Search(ctx, vec1, ns1, dim, 10)
	if err != nil {
		t.Fatalf("search ns1: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result in ns1, got %d", len(results))
	}
	if results[0].ID != id1 {
		t.Errorf("expected ns1 result ID %s, got %s", id1, results[0].ID)
	}

	// Search in ns2 — should only find id2.
	results, err = store.Search(ctx, vec2, ns2, dim, 10)
	if err != nil {
		t.Fatalf("search ns2: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result in ns2, got %d", len(results))
	}
	if results[0].ID != id2 {
		t.Errorf("expected ns2 result ID %s, got %s", id2, results[0].ID)
	}
}

func TestHNSWStoreUpsertUpdate(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384
	id := uuid.New()

	vec1 := normalizeVector(randomVector(dim, 10))
	vec2 := normalizeVector(randomVector(dim, 20))

	// Upsert with vec1.
	if err := store.Upsert(ctx, id, nsID, vec1, dim); err != nil {
		t.Fatalf("upsert vec1: %v", err)
	}

	// Upsert same ID with vec2.
	if err := store.Upsert(ctx, id, nsID, vec2, dim); err != nil {
		t.Fatalf("upsert vec2: %v", err)
	}

	// Search for vec2 — should find id with high score.
	results, err := store.Search(ctx, vec2, nsID, dim, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != id {
		t.Errorf("expected ID %s, got %s", id, results[0].ID)
	}
	if results[0].Score < 0.99 {
		t.Errorf("expected score >= 0.99, got %f", results[0].Score)
	}
}

func TestHNSWStoreSearchEmptyNamespace(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384
	query := normalizeVector(randomVector(dim, 99))

	results, err := store.Search(ctx, query, nsID, dim, 5)
	if err != nil {
		t.Fatalf("search empty namespace: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestHNSWStoreUnsupportedDimension(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()

	// Dimension 999 is not in SupportedVectorDimensions.
	vec := randomVector(999, 1)
	err := store.Upsert(ctx, uuid.New(), nsID, vec, 999)
	if err == nil {
		t.Fatal("expected error for unsupported dimension, got nil")
	}

	// Search with unsupported dimension should also fail.
	_, err = store.Search(ctx, vec, nsID, 999, 5)
	if err == nil {
		t.Fatal("expected error for unsupported search dimension, got nil")
	}

	// UpsertBatch with unsupported dimension.
	err = store.UpsertBatch(ctx, []storage.VectorUpsertItem{
		{ID: uuid.New(), NamespaceID: nsID, Embedding: vec, Dimension: 999},
	})
	if err == nil {
		t.Fatal("expected error for unsupported batch dimension, got nil")
	}
}

func TestHNSWStorePing(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestHNSWStoreClose(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384
	vec := normalizeVector(randomVector(dim, 42))

	// Upsert a vector so there's dirty state to flush.
	if err := store.Upsert(ctx, uuid.New(), nsID, vec, dim); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Close should flush and not panic.
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Verify the snapshot was written.
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM hnsw_snapshots").Scan(&count)
	if err != nil {
		t.Fatalf("query hnsw_snapshots: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 snapshot after close, got %d", count)
	}
}
