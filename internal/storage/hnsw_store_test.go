package storage_test

import (
	"context"
	"database/sql"
	"math"
	"math/rand"
	"sort"
	"sync"
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

// makeVector creates a deterministic vector of the given dimension seeded by a float value.
// The seed value significantly influences the vector direction, making vectors with
// different seeds distinguishable in cosine similarity searches.
func makeVector(dim int, seed float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = seed*float32(i+1) + float32(i)*0.01
	}
	return v
}

// setupHNSWTestDBNoCleanup creates an in-memory SQLite database without auto-closing.
// The caller is responsible for closing the DB.
func setupHNSWTestDBNoCleanup(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("set WAL mode: %v", err)
	}

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

	return db
}

func TestHNSWStoreSnapshotPersistence(t *testing.T) {
	db := setupHNSWTestDBNoCleanup(t)
	defer db.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384

	// Phase 1: Create store, upsert vectors, close to flush snapshots.
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store1 := storage.NewHNSWStore(db, cfg)

	ids := make([]uuid.UUID, 10)
	vecs := make([][]float32, 10)
	for i := 0; i < 10; i++ {
		ids[i] = uuid.New()
		vecs[i] = normalizeVector(randomVector(dim, int64(i+1)))
		if err := store1.Upsert(ctx, ids[i], nsID, vecs[i], dim); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("close store1: %v", err)
	}

	// Verify snapshot exists.
	var snapCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM hnsw_snapshots").Scan(&snapCount); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if snapCount != 1 {
		t.Fatalf("expected 1 snapshot, got %d", snapCount)
	}

	// Phase 2: Create a NEW store with the same DB (simulates restart).
	store2 := storage.NewHNSWStore(db, cfg)
	defer store2.Close()

	// Search should return correct results loaded from the snapshot.
	results, err := store2.Search(ctx, vecs[0], nsID, dim, 3)
	if err != nil {
		t.Fatalf("search after restart: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results after restart, got 0")
	}
	if results[0].ID != ids[0] {
		t.Errorf("expected top result ID %s, got %s", ids[0], results[0].ID)
	}
	if results[0].Score < 0.99 {
		t.Errorf("expected top result score >= 0.99, got %f", results[0].Score)
	}

	// Verify all 10 vectors are searchable.
	allResults, err := store2.Search(ctx, vecs[5], nsID, dim, 10)
	if err != nil {
		t.Fatalf("search all after restart: %v", err)
	}
	if len(allResults) != 10 {
		t.Errorf("expected 10 results after restart, got %d", len(allResults))
	}
}

func TestHNSWStoreLRUEviction(t *testing.T) {
	db := setupHNSWTestDB(t)

	ctx := context.Background()
	dim := 384

	// Create store with MaxLoadedIndexes=2 so 3 namespaces forces eviction.
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	cfg.MaxLoadedIndexes = 2
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ns1 := uuid.New()
	ns2 := uuid.New()
	ns3 := uuid.New()
	namespaces := []uuid.UUID{ns1, ns2, ns3}

	// Upsert 5 vectors per namespace using randomVector for good separation.
	type nsData struct {
		ids  []uuid.UUID
		vecs [][]float32
	}
	data := make([]nsData, 3)
	for ni, ns := range namespaces {
		data[ni].ids = make([]uuid.UUID, 5)
		data[ni].vecs = make([][]float32, 5)
		for i := 0; i < 5; i++ {
			data[ni].ids[i] = uuid.New()
			data[ni].vecs[i] = normalizeVector(randomVector(dim, int64(ni*1000+i+1)))
			if err := store.Upsert(ctx, data[ni].ids[i], ns, data[ni].vecs[i], dim); err != nil {
				t.Fatalf("upsert ns%d vec%d: %v", ni, i, err)
			}
		}
	}

	// At this point ns1 was loaded first, ns3 was loaded last.
	// With MaxLoadedIndexes=2, ns1 should have been evicted when ns3 was loaded.
	// But search should still work because it reloads from DB.
	for ni, ns := range namespaces {
		results, err := store.Search(ctx, data[ni].vecs[0], ns, dim, 3)
		if err != nil {
			t.Fatalf("search ns%d: %v", ni, err)
		}
		if len(results) == 0 {
			t.Fatalf("expected results for ns%d, got 0", ni)
		}
		if results[0].ID != data[ni].ids[0] {
			t.Errorf("ns%d: expected top result ID %s, got %s", ni, data[ni].ids[0], results[0].ID)
		}
		if results[0].Score < 0.99 {
			t.Errorf("ns%d: expected top result score >= 0.99, got %f", ni, results[0].Score)
		}
	}
}

func TestHNSWStoreConcurrentReadWrite(t *testing.T) {
	db := setupHNSWTestDBNoCleanup(t)
	// Pin to a single connection so SQLite :memory: doesn't open separate databases.
	db.SetMaxOpenConns(1)
	defer db.Close()
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384

	// Seed some initial vectors so searches have data.
	for i := 0; i < 5; i++ {
		vec := normalizeVector(randomVector(dim, int64(i+1000)))
		if err := store.Upsert(ctx, uuid.New(), nsID, vec, dim); err != nil {
			t.Fatalf("seed upsert %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// 10 writer goroutines.
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			vec := normalizeVector(randomVector(dim, int64(seed+2000)))
			if err := store.Upsert(ctx, uuid.New(), nsID, vec, dim); err != nil {
				errCh <- err
			}
		}(w)
	}

	// 10 reader goroutines.
	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			query := normalizeVector(randomVector(dim, int64(seed+3000)))
			_, err := store.Search(ctx, query, nsID, dim, 3)
			if err != nil {
				errCh <- err
			}
		}(r)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent operation error: %v", err)
	}
}

func TestHNSWStoreDeleteAndSearchConsistency(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384

	// Create a query vector and 20 vectors all close to it.
	queryBase := makeVector(dim, 1.0)
	query := normalizeVector(queryBase)

	ids := make([]uuid.UUID, 20)
	for i := 0; i < 20; i++ {
		ids[i] = uuid.New()
		// Create vectors very close to query but with slight offsets.
		v := make([]float32, dim)
		copy(v, queryBase)
		// Add a small increasing offset so vectors are ordered by distance.
		for j := range v {
			v[j] += float32(i) * 0.0001
		}
		vec := normalizeVector(v)
		if err := store.Upsert(ctx, ids[i], nsID, vec, dim); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	// Search for top 20 to get the rank order.
	allResults, err := store.Search(ctx, query, nsID, dim, 20)
	if err != nil {
		t.Fatalf("search all: %v", err)
	}
	if len(allResults) != 20 {
		t.Fatalf("expected 20 results, got %d", len(allResults))
	}

	// Identify the top-5 IDs.
	top5 := make(map[uuid.UUID]bool)
	for _, r := range allResults[:5] {
		top5[r.ID] = true
	}

	// Delete the top 5.
	for id := range top5 {
		if err := store.Delete(ctx, id); err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}
	}

	// Search again — none of the deleted vectors should appear.
	afterResults, err := store.Search(ctx, query, nsID, dim, 20)
	if err != nil {
		t.Fatalf("search after delete: %v", err)
	}
	if len(afterResults) != 15 {
		t.Fatalf("expected 15 results after deleting 5, got %d", len(afterResults))
	}

	for _, r := range afterResults {
		if top5[r.ID] {
			t.Errorf("deleted ID %s still appears in search results", r.ID)
		}
	}
}

func TestHNSWStoreBatchUpsertMultiNamespace(t *testing.T) {
	db := setupHNSWTestDB(t)
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store := storage.NewHNSWStore(db, cfg)
	defer store.Close()

	ctx := context.Background()

	ns1 := uuid.New()
	ns2 := uuid.New()
	ns3 := uuid.New()
	dim384 := 384
	dim768 := 768

	// Build a batch spanning 3 namespaces and 2 dimensions.
	type idVec struct {
		id  uuid.UUID
		vec []float32
	}
	groups := map[string][]idVec{
		"ns1_384": {},
		"ns2_384": {},
		"ns3_768": {},
	}

	var items []storage.VectorUpsertItem

	// ns1, dim=384: 5 vectors
	for i := 0; i < 5; i++ {
		id := uuid.New()
		vec := normalizeVector(randomVector(dim384, int64(i+100)))
		items = append(items, storage.VectorUpsertItem{
			ID: id, NamespaceID: ns1, Embedding: vec, Dimension: dim384,
		})
		groups["ns1_384"] = append(groups["ns1_384"], idVec{id, vec})
	}

	// ns2, dim=384: 5 vectors
	for i := 0; i < 5; i++ {
		id := uuid.New()
		vec := normalizeVector(randomVector(dim384, int64(i+200)))
		items = append(items, storage.VectorUpsertItem{
			ID: id, NamespaceID: ns2, Embedding: vec, Dimension: dim384,
		})
		groups["ns2_384"] = append(groups["ns2_384"], idVec{id, vec})
	}

	// ns3, dim=768: 5 vectors
	for i := 0; i < 5; i++ {
		id := uuid.New()
		vec := normalizeVector(randomVector(dim768, int64(i+300)))
		items = append(items, storage.VectorUpsertItem{
			ID: id, NamespaceID: ns3, Embedding: vec, Dimension: dim768,
		})
		groups["ns3_768"] = append(groups["ns3_768"], idVec{id, vec})
	}

	if err := store.UpsertBatch(ctx, items); err != nil {
		t.Fatalf("upsert batch: %v", err)
	}

	// Verify namespace + dimension isolation.
	testCases := []struct {
		name string
		ns   uuid.UUID
		dim  int
		key  string
	}{
		{"ns1_dim384", ns1, dim384, "ns1_384"},
		{"ns2_dim384", ns2, dim384, "ns2_384"},
		{"ns3_dim768", ns3, dim768, "ns3_768"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := groups[tc.key]
			results, err := store.Search(ctx, g[0].vec, tc.ns, tc.dim, 10)
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if len(results) != 5 {
				t.Fatalf("expected 5 results, got %d", len(results))
			}

			// Top result should be the query vector itself.
			if results[0].ID != g[0].id {
				t.Errorf("expected top result ID %s, got %s", g[0].id, results[0].ID)
			}
			if results[0].Score < 0.99 {
				t.Errorf("expected top result score >= 0.99, got %f", results[0].Score)
			}

			// All results should belong to the correct namespace.
			expectedIDs := make(map[uuid.UUID]bool)
			for _, iv := range g {
				expectedIDs[iv.id] = true
			}
			for _, r := range results {
				if !expectedIDs[r.ID] {
					t.Errorf("unexpected ID %s in results for %s", r.ID, tc.name)
				}
				if r.NamespaceID != tc.ns {
					t.Errorf("expected namespace %s, got %s", tc.ns, r.NamespaceID)
				}
			}
		})
	}

	// Cross-check: searching ns1 with dim=768 should return nothing.
	crossResults, err := store.Search(ctx, normalizeVector(makeVector(dim768, 10.0)), ns1, dim768, 10)
	if err != nil {
		t.Fatalf("cross search: %v", err)
	}
	if len(crossResults) != 0 {
		t.Errorf("expected 0 cross-namespace/dim results, got %d", len(crossResults))
	}
}

func TestHNSWStoreRebuildFromVectors(t *testing.T) {
	db := setupHNSWTestDBNoCleanup(t)
	defer db.Close()

	ctx := context.Background()
	nsID := uuid.New()
	dim := 384

	// Phase 1: Create store, upsert vectors, close to flush snapshots.
	cfg := storage.DefaultHNSWConfig()
	cfg.SnapshotInterval = 1<<63 - 1
	store1 := storage.NewHNSWStore(db, cfg)

	ids := make([]uuid.UUID, 10)
	vecs := make([][]float32, 10)
	for i := 0; i < 10; i++ {
		ids[i] = uuid.New()
		vecs[i] = normalizeVector(randomVector(dim, int64(i+500)))
		if err := store1.Upsert(ctx, ids[i], nsID, vecs[i], dim); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("close store1: %v", err)
	}

	// Verify snapshot exists.
	var snapCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM hnsw_snapshots").Scan(&snapCount); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if snapCount != 1 {
		t.Fatalf("expected 1 snapshot, got %d", snapCount)
	}

	// Phase 2: Delete the snapshot to force a rebuild from memory_vectors.
	_, err := db.Exec("DELETE FROM hnsw_snapshots")
	if err != nil {
		t.Fatalf("delete snapshots: %v", err)
	}

	// Verify memory_vectors still has data.
	var vecCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM memory_vectors").Scan(&vecCount); err != nil {
		t.Fatalf("count vectors: %v", err)
	}
	if vecCount != 10 {
		t.Fatalf("expected 10 vectors in memory_vectors, got %d", vecCount)
	}

	// Phase 3: Create a new store — it should rebuild from memory_vectors.
	store2 := storage.NewHNSWStore(db, cfg)
	defer store2.Close()

	// Search should return correct results rebuilt from raw vectors.
	results, err := store2.Search(ctx, vecs[0], nsID, dim, 3)
	if err != nil {
		t.Fatalf("search after rebuild: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results after rebuild, got 0")
	}
	if results[0].ID != ids[0] {
		t.Errorf("expected top result ID %s, got %s", ids[0], results[0].ID)
	}
	if results[0].Score < 0.99 {
		t.Errorf("expected top result score >= 0.99, got %f", results[0].Score)
	}

	// Verify all 10 vectors are searchable.
	allResults, err := store2.Search(ctx, vecs[5], nsID, dim, 10)
	if err != nil {
		t.Fatalf("search all after rebuild: %v", err)
	}
	if len(allResults) != 10 {
		t.Errorf("expected 10 results after rebuild, got %d", len(allResults))
	}

	// Verify the IDs we get back are the IDs we inserted.
	resultIDs := make(map[uuid.UUID]bool)
	for _, r := range allResults {
		resultIDs[r.ID] = true
	}
	// Sort ids for deterministic error output.
	sortedIDs := make([]uuid.UUID, len(ids))
	copy(sortedIDs, ids)
	sort.Slice(sortedIDs, func(i, j int) bool {
		return sortedIDs[i].String() < sortedIDs[j].String()
	})
	for _, id := range sortedIDs {
		if !resultIDs[id] {
			t.Errorf("inserted ID %s not found in rebuild search results", id)
		}
	}
}
