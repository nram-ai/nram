package hnsw

import (
	"context"
	"database/sql"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := `
	CREATE TABLE IF NOT EXISTS memories (
		id TEXT PRIMARY KEY
	);
	CREATE TABLE IF NOT EXISTS memory_vectors (
		memory_id TEXT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
		namespace_id TEXT NOT NULL,
		dimension INTEGER NOT NULL,
		embedding BLOB NOT NULL,
		created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%%H:%M:%fZ', 'now')),
		updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%%H:%M:%fZ', 'now'))
	);
	CREATE INDEX IF NOT EXISTS idx_memory_vectors_ns_dim ON memory_vectors(namespace_id, dimension);
	CREATE TABLE IF NOT EXISTS hnsw_snapshots (
		namespace_id TEXT NOT NULL,
		dimension INTEGER NOT NULL,
		graph_data BLOB NOT NULL,
		node_count INTEGER NOT NULL DEFAULT 0,
		updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%%H:%M:%fZ', 'now')),
		PRIMARY KEY (namespace_id, dimension)
	);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func insertTestVector(t *testing.T, db *sql.DB, nsID uuid.UUID, dim int, memID uuid.UUID, vec []float32) {
	t.Helper()
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	_, err := db.Exec("INSERT INTO memories (id) VALUES (?)", memID.String())
	if err != nil {
		t.Fatalf("insert memory: %v", err)
	}
	_, err = db.Exec(
		"INSERT INTO memory_vectors (memory_id, namespace_id, dimension, embedding, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		memID.String(), nsID.String(), dim, EncodeVector(vec), now, now,
	)
	if err != nil {
		t.Fatalf("insert memory_vector: %v", err)
	}
}

func TestCacheGetOrCreateEmpty(t *testing.T) {
	db := setupTestDB(t)
	cache := NewIndexCache(db, CacheConfig{
		MaxIndexes:       8,
		SnapshotInterval: time.Hour, // no background flush during test
	})
	defer cache.Close()

	ctx := context.Background()
	nsID := uuid.New()

	g, err := cache.GetOrCreate(ctx, nsID, 128)
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if g == nil {
		t.Fatal("GetOrCreate returned nil graph")
	}
	if g.Dimension() != 128 {
		t.Errorf("Dimension() = %d, want 128", g.Dimension())
	}
	if g.Len() != 0 {
		t.Errorf("Len() = %d, want 0", g.Len())
	}
}

func TestCacheGetOrCreateCached(t *testing.T) {
	db := setupTestDB(t)
	cache := NewIndexCache(db, CacheConfig{
		MaxIndexes:       8,
		SnapshotInterval: time.Hour,
	})
	defer cache.Close()

	ctx := context.Background()
	nsID := uuid.New()

	g1, err := cache.GetOrCreate(ctx, nsID, 64)
	if err != nil {
		t.Fatalf("first GetOrCreate: %v", err)
	}

	g2, err := cache.GetOrCreate(ctx, nsID, 64)
	if err != nil {
		t.Fatalf("second GetOrCreate: %v", err)
	}

	if g1 != g2 {
		t.Error("second GetOrCreate returned different graph pointer; expected same cached instance")
	}
}

func TestCacheLRUEviction(t *testing.T) {
	db := setupTestDB(t)
	cache := NewIndexCache(db, CacheConfig{
		MaxIndexes:       2,
		SnapshotInterval: time.Hour,
	})
	defer cache.Close()

	ctx := context.Background()
	ns1 := uuid.New()
	ns2 := uuid.New()
	ns3 := uuid.New()

	g1, err := cache.GetOrCreate(ctx, ns1, 32)
	if err != nil {
		t.Fatalf("GetOrCreate ns1: %v", err)
	}

	_, err = cache.GetOrCreate(ctx, ns2, 32)
	if err != nil {
		t.Fatalf("GetOrCreate ns2: %v", err)
	}

	// Loading ns3 should evict ns1 (LRU).
	_, err = cache.GetOrCreate(ctx, ns3, 32)
	if err != nil {
		t.Fatalf("GetOrCreate ns3: %v", err)
	}

	// Verify ns1 was evicted from cache.
	cache.mu.Lock()
	_, ns1Cached := cache.indexes[indexKey{NamespaceID: ns1, Dimension: 32}]
	cacheSize := len(cache.indexes)
	cache.mu.Unlock()

	if ns1Cached {
		t.Error("ns1 should have been evicted but was still in cache")
	}
	if cacheSize != 2 {
		t.Errorf("cache size = %d, want 2", cacheSize)
	}

	// Re-loading ns1 should create a new graph (not same pointer).
	g1Again, err := cache.GetOrCreate(ctx, ns1, 32)
	if err != nil {
		t.Fatalf("GetOrCreate ns1 again: %v", err)
	}
	if g1Again == g1 {
		t.Error("re-loaded graph should be a new instance after eviction")
	}
}

func TestCacheMarkDirtyAndFlush(t *testing.T) {
	db := setupTestDB(t)
	cache := NewIndexCache(db, CacheConfig{
		MaxIndexes:       8,
		SnapshotInterval: time.Hour,
	})
	defer cache.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 16

	g, err := cache.GetOrCreate(ctx, nsID, dim)
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Add a node and mark dirty.
	rng := rand.New(rand.NewSource(99))
	vec := randomVector(rng, dim)
	if err := g.Add(Node{ID: uuid.New(), Vector: vec}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	cache.MarkDirty(nsID, dim)

	// Flush.
	if err := cache.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	// Verify snapshot exists in DB.
	var nodeCount int
	err = db.QueryRowContext(ctx,
		"SELECT node_count FROM hnsw_snapshots WHERE namespace_id = ? AND dimension = ?",
		nsID.String(), dim,
	).Scan(&nodeCount)
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if nodeCount != 1 {
		t.Errorf("node_count = %d, want 1", nodeCount)
	}

	// Verify dirty flag cleared.
	cache.mu.Lock()
	entry := cache.indexes[indexKey{NamespaceID: nsID, Dimension: dim}]
	dirty := entry.dirty
	cache.mu.Unlock()
	if dirty {
		t.Error("entry should not be dirty after flush")
	}
}

func TestCacheSnapshotReload(t *testing.T) {
	db := setupTestDB(t)
	nsID := uuid.New()
	dim := 8
	rng := rand.New(rand.NewSource(42))

	// Insert vectors into DB.
	nVectors := 5
	ids := make([]uuid.UUID, nVectors)
	for i := 0; i < nVectors; i++ {
		ids[i] = uuid.New()
		insertTestVector(t, db, nsID, dim, ids[i], randomVector(rng, dim))
	}

	// Create cache and load — should rebuild from memory_vectors.
	cache := NewIndexCache(db, CacheConfig{
		MaxIndexes:       8,
		SnapshotInterval: time.Hour,
	})
	defer cache.Close()

	ctx := context.Background()
	g, err := cache.GetOrCreate(ctx, nsID, dim)
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if g.Len() != nVectors {
		t.Errorf("Len() = %d, want %d", g.Len(), nVectors)
	}

	// Verify all IDs are present.
	for _, id := range ids {
		if !g.Has(id) {
			t.Errorf("graph missing node %s", id)
		}
	}

	// Mark dirty and flush to create a snapshot.
	cache.MarkDirty(nsID, dim)
	if err := cache.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	// Create a new cache — should load from snapshot this time.
	cache2 := NewIndexCache(db, CacheConfig{
		MaxIndexes:       8,
		SnapshotInterval: time.Hour,
	})
	defer cache2.Close()

	g2, err := cache2.GetOrCreate(ctx, nsID, dim)
	if err != nil {
		t.Fatalf("second GetOrCreate: %v", err)
	}
	if g2.Len() != nVectors {
		t.Errorf("snapshot reload: Len() = %d, want %d", g2.Len(), nVectors)
	}
	for _, id := range ids {
		if !g2.Has(id) {
			t.Errorf("snapshot reload: graph missing node %s", id)
		}
	}
}

func TestCacheClose(t *testing.T) {
	db := setupTestDB(t)
	cache := NewIndexCache(db, CacheConfig{
		MaxIndexes:       8,
		SnapshotInterval: time.Hour,
	})

	ctx := context.Background()
	nsID := uuid.New()
	dim := 16

	g, err := cache.GetOrCreate(ctx, nsID, dim)
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Add data and mark dirty.
	rng := rand.New(rand.NewSource(7))
	if err := g.Add(Node{ID: uuid.New(), Vector: randomVector(rng, dim)}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	cache.MarkDirty(nsID, dim)

	// Close should flush.
	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify snapshot was written.
	var nodeCount int
	err = db.QueryRowContext(ctx,
		"SELECT node_count FROM hnsw_snapshots WHERE namespace_id = ? AND dimension = ?",
		nsID.String(), dim,
	).Scan(&nodeCount)
	if err != nil {
		t.Fatalf("query snapshot after Close: %v", err)
	}
	if nodeCount != 1 {
		t.Errorf("node_count = %d, want 1", nodeCount)
	}
}

func TestCacheRemove(t *testing.T) {
	db := setupTestDB(t)
	cache := NewIndexCache(db, CacheConfig{
		MaxIndexes:       8,
		SnapshotInterval: time.Hour,
	})
	defer cache.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 32

	_, err := cache.GetOrCreate(ctx, nsID, dim)
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Verify it's cached.
	cache.mu.Lock()
	_, ok := cache.indexes[indexKey{NamespaceID: nsID, Dimension: dim}]
	cache.mu.Unlock()
	if !ok {
		t.Fatal("entry should be in cache before Remove")
	}

	cache.Remove(nsID, dim)

	// Verify it's gone.
	cache.mu.Lock()
	_, ok = cache.indexes[indexKey{NamespaceID: nsID, Dimension: dim}]
	size := len(cache.indexes)
	cache.mu.Unlock()
	if ok {
		t.Error("entry should not be in cache after Remove")
	}
	if size != 0 {
		t.Errorf("cache size = %d, want 0", size)
	}
}
