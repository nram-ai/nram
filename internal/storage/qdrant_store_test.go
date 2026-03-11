package storage

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

func setupQdrantTest(t *testing.T) *QdrantStore {
	t.Helper()

	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant tests (e.g. localhost:6334)")
	}

	host, port, err := parseQdrantAddr(addr)
	if err != nil {
		t.Fatalf("parseQdrantAddr: %v", err)
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("qdrant.NewClient: %v", err)
	}

	store := NewQdrantStoreFromClient(client)

	ctx := context.Background()
	if err := store.EnsureCollections(ctx); err != nil {
		store.Close()
		t.Fatalf("EnsureCollections: %v", err)
	}

	t.Cleanup(func() {
		// Clean up all test data from all collections.
		for _, collection := range allQdrantCollections {
			// Delete all points by using a filter that matches everything.
			// Use scroll to get all point IDs, then delete them.
			points, err := client.Scroll(ctx, &qdrant.ScrollPoints{
				CollectionName: collection,
				Limit:          qdrant.PtrOf(uint32(10000)),
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
		store.Close()
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
	// emb1: [1, 0, 0, 0, ...] — first element dominant
	// emb2: [0, 1, 0, 0, ...] — second element dominant
	emb1 := make([]float32, dim)
	emb1[0] = 1.0
	for i := 1; i < dim; i++ {
		emb1[i] = 0.01
	}
	emb2 := make([]float32, dim)
	emb2[1] = 1.0
	for i := 0; i < dim; i++ {
		if i != 1 {
			emb2[i] = 0.01
		}
	}

	// Upsert two vectors.
	if err := store.Upsert(ctx, memID1, nsID, emb1, dim); err != nil {
		t.Fatalf("Upsert 1: %v", err)
	}
	if err := store.Upsert(ctx, memID2, nsID, emb2, dim); err != nil {
		t.Fatalf("Upsert 2: %v", err)
	}

	// Search — query vector identical to emb1 should rank memID1 first.
	results, err := store.Search(ctx, emb1, nsID, dim, 10)
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

	// Upsert update — change emb1 to match emb2 and verify search changes.
	if err := store.Upsert(ctx, memID1, nsID, emb2, dim); err != nil {
		t.Fatalf("Upsert update: %v", err)
	}
	results, err = store.Search(ctx, emb2, nsID, dim, 10)
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
	results384, err := store.Search(ctx, makeEmbedding(384, 1.0), nsID, 384, 10)
	if err != nil {
		t.Fatalf("Search 384: %v", err)
	}
	if len(results384) != 2 {
		t.Errorf("Search 384 returned %d results, want 2", len(results384))
	}

	// Verify 512-dim vectors.
	results512, err := store.Search(ctx, makeEmbedding(512, 0.8), nsID, 512, 10)
	if err != nil {
		t.Fatalf("Search 512: %v", err)
	}
	if len(results512) != 1 {
		t.Errorf("Search 512 returned %d results, want 1", len(results512))
	}
}

func TestQdrantStore_UpsertBatch_Empty(t *testing.T) {
	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant tests")
	}

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
	if err := store.Upsert(ctx, memID, nsID, makeEmbedding(dim, 1.0), dim); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Verify it exists.
	results, err := store.Search(ctx, makeEmbedding(dim, 1.0), nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search before delete: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result before delete, got %d", len(results))
	}

	// Delete.
	if err := store.Delete(ctx, memID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify it's gone.
	results, err = store.Search(ctx, makeEmbedding(dim, 1.0), nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
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

	if err := store.Upsert(ctx, memID1, nsID1, emb, dim); err != nil {
		t.Fatalf("Upsert ns1: %v", err)
	}
	if err := store.Upsert(ctx, memID2, nsID2, emb, dim); err != nil {
		t.Fatalf("Upsert ns2: %v", err)
	}

	// Search in ns1 should only return memID1.
	results, err := store.Search(ctx, emb, nsID1, dim, 10)
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
	results, err = store.Search(ctx, emb, nsID2, dim, 10)
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
	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant tests")
	}

	store := setupQdrantTest(t)

	err := store.Upsert(context.Background(), uuid.New(), uuid.New(), makeEmbedding(128, 1.0), 128)
	if err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

func TestQdrantStore_Search_InvalidDimension(t *testing.T) {
	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant tests")
	}

	store := setupQdrantTest(t)

	_, err := store.Search(context.Background(), makeEmbedding(128, 1.0), uuid.New(), 128, 10)
	if err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

func TestQdrantStore_UpsertBatch_InvalidDimension(t *testing.T) {
	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant tests")
	}

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
		name, err := qdrantCollectionName(tt.dim)
		if tt.wantErr {
			if err == nil {
				t.Errorf("qdrantCollectionName(%d) expected error, got %q", tt.dim, name)
			}
		} else {
			if err != nil {
				t.Errorf("qdrantCollectionName(%d) unexpected error: %v", tt.dim, err)
			}
			if name != tt.want {
				t.Errorf("qdrantCollectionName(%d) = %q, want %q", tt.dim, name, tt.want)
			}
		}
	}
}
