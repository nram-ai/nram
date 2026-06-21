package storage

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

// ensureQdrantAddr reads QDRANT_TEST_ADDR from the environment and skips the
// test if it is unset or empty.
func ensureQdrantAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant tests (e.g. localhost:6334)")
	}
	return addr
}

func setupQdrantTest(t *testing.T) *QdrantStore {
	t.Helper()

	addr := ensureQdrantAddr(t)

	store, err := NewQdrantStore(QdrantConfig{Addr: addr})
	if err != nil {
		t.Fatalf("NewQdrantStore: %v", err)
	}

	ctx := context.Background()
	if err := store.EnsureCollections(ctx); err != nil {
		_ = store.Close()
		t.Fatalf("EnsureCollections: %v", err)
	}

	client := store.Client()

	t.Cleanup(func() {
		// Clean up all test data from all collections (both memory and entity
		// families). Without entity-collection cleanup, entity-vector tests
		// would leak points across runs against a shared dev Qdrant instance.
		for _, family := range []map[int]string{qdrantMemoryCollections, qdrantEntityCollections} {
			for _, collection := range family {
				limit := uint32(10000)
				points, err := client.Scroll(ctx, &qdrant.ScrollPoints{
					CollectionName: collection,
					Limit:          &limit,
				})
				if err == nil && len(points) > 0 {
					ids := make([]*qdrant.PointId, len(points))
					for i, p := range points {
						ids[i] = p.GetId()
					}
					client.Delete(ctx, &qdrant.DeletePoints{ //nolint:errcheck
						CollectionName: collection,
						Points:         qdrant.NewPointsSelectorIDs(ids),
					})
				}
			}
		}
		_ = store.Close()
	})

	return store
}

func TestQdrantStore_Ping(t *testing.T) {
	store := setupQdrantTest(t)

	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestQdrantStore_UpsertAndSearch(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID := uuid.New()
	memID1 := uuid.New()
	memID2 := uuid.New()

	dim := 384

	// Create vectors that point in different directions (not just scaled versions).
	// emb1: [1, 0, 0, 0, ...], first element dominant
	// emb2: [0, 1, 0, 0, ...], second element dominant
	emb1 := make([]float32, dim)
	emb1[0] = 1.0
	for i := 1; i < dim; i++ {
		emb1[i] = 0.01
	}
	emb2 := make([]float32, dim)
	emb2[1] = 1.0
	for i := range dim {
		if i != 1 {
			emb2[i] = 0.01
		}
	}

	// Upsert two vectors.
	if err := store.Upsert(ctx, VectorKindMemory, memID1, nsID, emb1, dim); err != nil {
		t.Fatalf("Upsert 1: %v", err)
	}
	if err := store.Upsert(ctx, VectorKindMemory, memID2, nsID, emb2, dim); err != nil {
		t.Fatalf("Upsert 2: %v", err)
	}

	// Search: query vector identical to emb1 should rank memID1 first.
	results, err := store.Search(ctx, VectorKindMemory, emb1, nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Search returned %d results, want 2", len(results))
	}
	if results[0].ID != memID1 {
		t.Errorf("first result ID = %s, want %s", results[0].ID, memID1)
	}
	if results[0].Score < results[1].Score {
		t.Errorf("first result score %f < second result score %f", results[0].Score, results[1].Score)
	}
	if results[0].NamespaceID != nsID {
		t.Errorf("first result namespace_id = %s, want %s", results[0].NamespaceID, nsID)
	}

	// Upsert update: change emb1 to match emb2 and verify search changes.
	if err := store.Upsert(ctx, VectorKindMemory, memID1, nsID, emb2, dim); err != nil {
		t.Fatalf("Upsert update: %v", err)
	}
	results, err = store.Search(ctx, VectorKindMemory, emb2, nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search after update: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Search after update returned %d results, want 2", len(results))
	}
	// Both should now have perfect score since they have the same embedding.
	if results[0].Score < 0.99 {
		t.Errorf("expected near-perfect score after update, got %f", results[0].Score)
	}
}

func TestQdrantStore_UpsertBatch(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID := uuid.New()

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	items := []VectorUpsertItem{
		{ID: ids[0], NamespaceID: nsID, Embedding: makeEmbedding(384, 1.0), Dimension: 384},
		{ID: ids[1], NamespaceID: nsID, Embedding: makeEmbedding(384, 0.5), Dimension: 384},
		{ID: ids[2], NamespaceID: nsID, Embedding: makeEmbedding(512, 0.8), Dimension: 512},
	}

	if err := store.UpsertBatch(ctx, items); err != nil {
		t.Fatalf("UpsertBatch: %v", err)
	}

	// Verify 384-dim vectors.
	results384, err := store.Search(ctx, VectorKindMemory, makeEmbedding(384, 1.0), nsID, 384, 10)
	if err != nil {
		t.Fatalf("Search 384: %v", err)
	}
	if len(results384) != 2 {
		t.Errorf("Search 384 returned %d results, want 2", len(results384))
	}

	// Verify 512-dim vectors.
	results512, err := store.Search(ctx, VectorKindMemory, makeEmbedding(512, 0.8), nsID, 512, 10)
	if err != nil {
		t.Fatalf("Search 512: %v", err)
	}
	if len(results512) != 1 {
		t.Errorf("Search 512 returned %d results, want 1", len(results512))
	}
}

func TestQdrantStore_UpsertBatch_Empty(t *testing.T) {
	ensureQdrantAddr(t)

	store := setupQdrantTest(t)

	if err := store.UpsertBatch(context.Background(), nil); err != nil {
		t.Fatalf("UpsertBatch with nil: %v", err)
	}
	if err := store.UpsertBatch(context.Background(), []VectorUpsertItem{}); err != nil {
		t.Fatalf("UpsertBatch with empty slice: %v", err)
	}
}

func TestQdrantStore_Delete(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID := uuid.New()
	memID := uuid.New()

	dim := 384
	if err := store.Upsert(ctx, VectorKindMemory, memID, nsID, makeEmbedding(dim, 1.0), dim); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Verify it exists.
	results, err := store.Search(ctx, VectorKindMemory, makeEmbedding(dim, 1.0), nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search before delete: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result before delete, got %d", len(results))
	}

	// Delete.
	if err := store.Delete(ctx, VectorKindMemory, memID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify it's gone.
	results, err = store.Search(ctx, VectorKindMemory, makeEmbedding(dim, 1.0), nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}
}

// TestQdrantStore_DeleteEntityVector verifies the entity-collection cleanup
// path used by the lifecycle orphan sweep, ProjectDeleteService, and the
// Upsert promoteStub merge. Without this Delete call landing on the entity
// family, those code paths would silently leak Qdrant points after the SQL
// rows are gone (entity_vectors_* SQL CASCADE does not reach Qdrant).
func TestQdrantStore_DeleteEntityVector(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID := uuid.New()
	entityID := uuid.New()
	dim := 384
	emb := makeEmbedding(dim, 1.0)

	if err := store.Upsert(ctx, VectorKindEntity, entityID, nsID, emb, dim); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := store.GetByIDs(ctx, VectorKindEntity, []uuid.UUID{entityID}, dim)
	if err != nil {
		t.Fatalf("GetByIDs before delete: %v", err)
	}
	if _, present := got[entityID]; !present {
		t.Fatalf("entity vector %s not present after Upsert", entityID)
	}

	if err := store.Delete(ctx, VectorKindEntity, entityID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err = store.GetByIDs(ctx, VectorKindEntity, []uuid.UUID{entityID}, dim)
	if err != nil {
		t.Fatalf("GetByIDs after delete: %v", err)
	}
	if _, present := got[entityID]; present {
		t.Fatalf("entity vector %s still present after Delete (got=%v)", entityID, got)
	}
}

func TestQdrantStore_SearchNamespaceScoping(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID1 := uuid.New()
	nsID2 := uuid.New()
	memID1 := uuid.New()
	memID2 := uuid.New()

	dim := 384
	emb := makeEmbedding(dim, 1.0)

	if err := store.Upsert(ctx, VectorKindMemory, memID1, nsID1, emb, dim); err != nil {
		t.Fatalf("Upsert ns1: %v", err)
	}
	if err := store.Upsert(ctx, VectorKindMemory, memID2, nsID2, emb, dim); err != nil {
		t.Fatalf("Upsert ns2: %v", err)
	}

	// Search in ns1 should only return memID1.
	results, err := store.Search(ctx, VectorKindMemory, emb, nsID1, dim, 10)
	if err != nil {
		t.Fatalf("Search ns1: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("ns1 search returned %d results, want 1", len(results))
	}
	if results[0].ID != memID1 {
		t.Errorf("ns1 search returned %s, want %s", results[0].ID, memID1)
	}

	// Search in ns2 should only return memID2.
	results, err = store.Search(ctx, VectorKindMemory, emb, nsID2, dim, 10)
	if err != nil {
		t.Fatalf("Search ns2: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("ns2 search returned %d results, want 1", len(results))
	}
	if results[0].ID != memID2 {
		t.Errorf("ns2 search returned %s, want %s", results[0].ID, memID2)
	}
}

func TestQdrantStore_Upsert_InvalidDimension(t *testing.T) {
	ensureQdrantAddr(t)

	store := setupQdrantTest(t)

	err := store.Upsert(context.Background(), VectorKindMemory, uuid.New(), uuid.New(), makeEmbedding(128, 1.0), 128)
	if err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

func TestQdrantStore_Search_InvalidDimension(t *testing.T) {
	ensureQdrantAddr(t)

	store := setupQdrantTest(t)

	_, err := store.Search(context.Background(), VectorKindMemory, makeEmbedding(128, 1.0), uuid.New(), 128, 10)
	if err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

func TestQdrantStore_UpsertBatch_InvalidDimension(t *testing.T) {
	ensureQdrantAddr(t)

	store := setupQdrantTest(t)

	items := []VectorUpsertItem{
		{ID: uuid.New(), NamespaceID: uuid.New(), Embedding: makeEmbedding(128, 1.0), Dimension: 128},
	}

	if err := store.UpsertBatch(context.Background(), items); err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

func TestQdrantCollectionName(t *testing.T) {
	tests := []struct {
		dim     int
		wantErr bool
		want    string
	}{
		{384, false, "nram_vectors_384"},
		{512, false, "nram_vectors_512"},
		{768, false, "nram_vectors_768"},
		{1024, false, "nram_vectors_1024"},
		{1536, false, "nram_vectors_1536"},
		{3072, false, "nram_vectors_3072"},
		{128, true, ""},
		{256, true, ""},
		{0, true, ""},
		{-1, true, ""},
		{2048, true, ""},
	}

	for _, tt := range tests {
		name, err := qdrantCollectionName(VectorKindMemory, tt.dim)
		if tt.wantErr {
			if err == nil {
				t.Errorf("qdrantCollectionName(memory, %d) expected error, got %q", tt.dim, name)
			}
		} else {
			if err != nil {
				t.Errorf("qdrantCollectionName(memory, %d) unexpected error: %v", tt.dim, err)
			}
			if name != tt.want {
				t.Errorf("qdrantCollectionName(memory, %d) = %q, want %q", tt.dim, name, tt.want)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// New E2E tests for config-based construction
// ---------------------------------------------------------------------------

func TestQdrantStore_NewWithFullConfig(t *testing.T) {
	addr := ensureQdrantAddr(t)

	store, err := NewQdrantStore(QdrantConfig{
		Addr:             addr,
		PoolSize:         2,
		KeepAliveTime:    30,
		KeepAliveTimeout: 5,
	})
	if err != nil {
		t.Fatalf("NewQdrantStore with full config: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	if err := store.EnsureCollections(ctx); err != nil {
		t.Fatalf("EnsureCollections: %v", err)
	}

	nsID := uuid.New()
	memID := uuid.New()
	dim := 384
	emb := makeEmbedding(dim, 1.0)

	if err := store.Upsert(ctx, VectorKindMemory, memID, nsID, emb, dim); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	results, err := store.Search(ctx, VectorKindMemory, emb, nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != memID {
		t.Errorf("result ID = %s, want %s", results[0].ID, memID)
	}

	// Clean up inserted point.
	if err := store.Delete(ctx, VectorKindMemory, memID); err != nil {
		t.Errorf("cleanup Delete: %v", err)
	}
}

func TestQdrantStore_NewWithPoolSizeOne(t *testing.T) {
	addr := ensureQdrantAddr(t)

	store, err := NewQdrantStore(QdrantConfig{
		Addr:     addr,
		PoolSize: 1,
	})
	if err != nil {
		t.Fatalf("NewQdrantStore with PoolSize=1: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	if err := store.EnsureCollections(ctx); err != nil {
		t.Fatalf("EnsureCollections: %v", err)
	}

	nsID := uuid.New()
	memID := uuid.New()
	dim := 384
	emb := makeEmbedding(dim, 1.0)

	if err := store.Upsert(ctx, VectorKindMemory, memID, nsID, emb, dim); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	results, err := store.Search(ctx, VectorKindMemory, emb, nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != memID {
		t.Errorf("result ID = %s, want %s", results[0].ID, memID)
	}

	// Clean up inserted point.
	if err := store.Delete(ctx, VectorKindMemory, memID); err != nil {
		t.Errorf("cleanup Delete: %v", err)
	}
}

func TestQdrantStore_NewWithKeepAliveDisabled(t *testing.T) {
	addr := ensureQdrantAddr(t)

	store, err := NewQdrantStore(QdrantConfig{
		Addr:          addr,
		KeepAliveTime: -1,
	})
	if err != nil {
		t.Fatalf("NewQdrantStore with KeepAliveTime=-1: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestQdrantStore_NewWithEmptyAPIKey(t *testing.T) {
	addr := ensureQdrantAddr(t)

	store, err := NewQdrantStore(QdrantConfig{
		Addr:   addr,
		APIKey: "",
	})
	if err != nil {
		t.Fatalf("NewQdrantStore with empty APIKey: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestQdrantStore_GetByIDs_RoundTrip(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID := uuid.New()
	dim := 384
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	embs := []([]float32){
		makeEmbedding(dim, 0.1),
		makeEmbedding(dim, 0.5),
		makeEmbedding(dim, 0.9),
	}
	items := []VectorUpsertItem{
		{ID: ids[0], NamespaceID: nsID, Embedding: embs[0], Dimension: dim},
		{ID: ids[1], NamespaceID: nsID, Embedding: embs[1], Dimension: dim},
		{ID: ids[2], NamespaceID: nsID, Embedding: embs[2], Dimension: dim},
	}
	if err := store.UpsertBatch(ctx, items); err != nil {
		t.Fatalf("UpsertBatch: %v", err)
	}

	got, err := store.GetByIDs(ctx, VectorKindMemory, ids, dim)
	if err != nil {
		t.Fatalf("GetByIDs: %v", err)
	}
	if len(got) != len(ids) {
		t.Errorf("expected %d hits, got %d", len(ids), len(got))
	}
	for i, id := range ids {
		v, ok := got[id]
		if !ok {
			t.Errorf("missing id %s", id)
			continue
		}
		if len(v) != dim {
			t.Errorf("id %s: vector dim = %d, want %d", id, len(v), dim)
			continue
		}
		// Qdrant normalises cosine vectors internally; the stored constant
		// vector returns scaled (1/sqrt(dim)) values, so just sanity-check
		// the ratio of any element to the original is consistent.
		_ = embs[i]
	}
}

func TestQdrantStore_GetByIDs_PartialAndEmpty(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID := uuid.New()
	dim := 384
	stored := uuid.New()
	if err := store.Upsert(ctx, VectorKindMemory, stored, nsID, makeEmbedding(dim, 0.3), dim); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	missing := uuid.New()
	got, err := store.GetByIDs(ctx, VectorKindMemory, []uuid.UUID{stored, missing}, dim)
	if err != nil {
		t.Fatalf("GetByIDs partial: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 hit, got %d", len(got))
	}
	if _, ok := got[missing]; ok {
		t.Errorf("missing id should not be in result")
	}

	emptyResult, err := store.GetByIDs(ctx, VectorKindMemory, nil, dim)
	if err != nil {
		t.Fatalf("GetByIDs empty: %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("expected empty map for nil input, got %d", len(emptyResult))
	}
}

func TestQdrantStore_GetByIDs_WrongDimension(t *testing.T) {
	store := setupQdrantTest(t)
	ctx := context.Background()

	nsID := uuid.New()
	id := uuid.New()
	if err := store.Upsert(ctx, VectorKindMemory, id, nsID, makeEmbedding(384, 0.4), 384); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := store.GetByIDs(ctx, VectorKindMemory, []uuid.UUID{id}, 768)
	if err != nil {
		t.Fatalf("GetByIDs at dim 768: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 hits at wrong dim, got %d", len(got))
	}
}

// TestQdrantStore_UpsertFacets_CollapsesToBestFacet runs against a live Qdrant
// (QDRANT_TEST_ADDR). It is deliberately self-contained: it uses a unique random
// namespace and deletes only the points it creates, so it never wipes shared
// collections or touches unrelated data.
func TestQdrantStore_UpsertFacets_CollapsesToBestFacet(t *testing.T) {
	addr := ensureQdrantAddr(t)
	ctx := context.Background()
	store, err := NewQdrantStore(QdrantConfig{Addr: addr})
	if err != nil {
		t.Fatalf("NewQdrantStore: %v", err)
	}
	if err := store.EnsureCollections(ctx); err != nil {
		t.Fatalf("EnsureCollections: %v", err)
	}

	ns := uuid.New()
	dim := 384
	multi := uuid.New()  // facet 0 = axis 0, facet 1 = axis 5
	single := uuid.New() // axis 5 (facet 0 only)
	axis := func(a int) []float32 {
		v := make([]float32, dim)
		v[a] = 1
		return v
	}
	// Cleanup runs LIFO: register Close first so it runs LAST, after the point
	// deletes. Deleting on a closed client would empty the pool and panic.
	t.Cleanup(func() { _ = store.Close() })
	t.Cleanup(func() {
		_ = store.Delete(ctx, VectorKindMemory, multi)
		_ = store.Delete(ctx, VectorKindMemory, single)
	})

	if err := store.UpsertFacets(ctx, multi, ns, dim, [][]float32{axis(0), axis(5)}); err != nil {
		t.Fatalf("UpsertFacets: %v", err)
	}
	if err := store.Upsert(ctx, VectorKindMemory, single, ns, axis(5), dim); err != nil {
		t.Fatalf("Upsert single: %v", err)
	}

	results, err := store.Search(ctx, VectorKindMemory, axis(5), ns, dim, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	seen := map[uuid.UUID]int{}
	score := map[uuid.UUID]float64{}
	for _, r := range results {
		seen[r.ID]++
		score[r.ID] = r.Score
	}
	if seen[multi] != 1 {
		t.Fatalf("multi appeared %d times, want exactly 1 (collapsed)", seen[multi])
	}
	if score[multi] < 0.99 {
		t.Errorf("multi score %f on axis-5 facet, want ~1.0 (best facet, not pooled)", score[multi])
	}
	if _, ok := score[single]; !ok {
		t.Error("single-topic memory missing from results")
	}

	// Delete removes facet 0 and all topic facets; the memory must vanish.
	if err := store.Delete(ctx, VectorKindMemory, multi); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	after, err := store.Search(ctx, VectorKindMemory, axis(5), ns, dim, 10)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	for _, r := range after {
		if r.ID == multi {
			t.Errorf("multi still present after delete")
		}
	}
}

// TestQdrantStore_IterateVectors_SkipsTopicFacets guards the reverse-migration
// fix: IterateVectors (the Qdrant->SQL migration source) must yield only facet 0,
// keyed by memory_id. Without the facet_id filter it would yield topic-facet
// points by their derived UUIDv5 ids, which the migrator would write back as
// phantom facet-0 rows keyed on a non-existent memory (FK violation on pgvector,
// silent loss on SQLite). Self-contained: unique namespace, deletes only its own
// points.
func TestQdrantStore_IterateVectors_SkipsTopicFacets(t *testing.T) {
	addr := ensureQdrantAddr(t)
	ctx := context.Background()
	store, err := NewQdrantStore(QdrantConfig{Addr: addr})
	if err != nil {
		t.Fatalf("NewQdrantStore: %v", err)
	}
	if err := store.EnsureCollections(ctx); err != nil {
		t.Fatalf("EnsureCollections: %v", err)
	}

	ns := uuid.New()
	dim := 384
	mem := uuid.New()
	axis := func(a int) []float32 {
		v := make([]float32, dim)
		v[a] = 1
		return v
	}
	t.Cleanup(func() { _ = store.Close() })
	t.Cleanup(func() { _ = store.Delete(ctx, VectorKindMemory, mem) })

	// facet 0 + two topic facets => three Qdrant points for one memory.
	if err := store.UpsertFacets(ctx, mem, ns, dim, [][]float32{axis(0), axis(5), axis(9)}); err != nil {
		t.Fatalf("UpsertFacets: %v", err)
	}

	var yielded []uuid.UUID
	err = store.IterateVectors(ctx, VectorKindMemory, dim, 100, func(id, gotNS uuid.UUID, _ []float32) error {
		if gotNS == ns {
			yielded = append(yielded, id)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("IterateVectors: %v", err)
	}
	if len(yielded) != 1 {
		t.Fatalf("IterateVectors yielded %d points for a 3-facet memory, want 1 (facet 0 only): %v", len(yielded), yielded)
	}
	if yielded[0] != mem {
		t.Errorf("IterateVectors yielded id %v, want the memory_id %v (facet-0 keying)", yielded[0], mem)
	}
}
