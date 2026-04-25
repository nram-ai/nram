package storage

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
)

func setupPgVectorTest(t *testing.T) *PgVectorStore {
	t.Helper()

	dsn := os.Getenv("PGVECTOR_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGVECTOR_TEST_DSN to run pgvector tests")
	}

	store, err := NewPgVectorStore(dsn)
	if err != nil {
		t.Fatalf("NewPgVectorStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	return store
}

func setupPgVectorTestWithSchema(t *testing.T) *PgVectorStore {
	t.Helper()

	dsn := os.Getenv("PGVECTOR_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGVECTOR_TEST_DSN to run pgvector tests")
	}

	ctx := context.Background()

	// Create a raw pool for schema setup.
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	// Ensure pgvector extension and test tables exist.
	setupSQL := `
		CREATE EXTENSION IF NOT EXISTS vector;

		CREATE TABLE IF NOT EXISTS namespaces (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS memories (
			id UUID PRIMARY KEY,
			namespace_id UUID NOT NULL REFERENCES namespaces(id),
			content TEXT NOT NULL DEFAULT '',
			deleted_at TIMESTAMPTZ
		);

		CREATE TABLE IF NOT EXISTS memory_vectors_384 (
			memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
			embedding vector(384) NOT NULL
		);
		CREATE TABLE IF NOT EXISTS memory_vectors_512 (
			memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
			embedding vector(512) NOT NULL
		);
		CREATE TABLE IF NOT EXISTS memory_vectors_768 (
			memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
			embedding vector(768) NOT NULL
		);
		CREATE TABLE IF NOT EXISTS memory_vectors_1024 (
			memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
			embedding vector(1024) NOT NULL
		);
		CREATE TABLE IF NOT EXISTS memory_vectors_1536 (
			memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
			embedding vector(1536) NOT NULL
		);
		CREATE TABLE IF NOT EXISTS memory_vectors_3072 (
			memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
			embedding vector(3072) NOT NULL
		);
	`
	if _, err := pool.Exec(ctx, setupSQL); err != nil {
		pool.Close()
		t.Fatalf("schema setup: %v", err)
	}

	store := NewPgVectorStoreFromPool(pool)
	t.Cleanup(func() {
		// Clean up test data.
		cleanupSQL := `
			DELETE FROM memory_vectors_384;
			DELETE FROM memory_vectors_512;
			DELETE FROM memory_vectors_768;
			DELETE FROM memory_vectors_1024;
			DELETE FROM memory_vectors_1536;
			DELETE FROM memory_vectors_3072;
			DELETE FROM memories;
			DELETE FROM namespaces;
		`
		pool.Exec(ctx, cleanupSQL) //nolint:errcheck
		store.Close()
	})

	return store
}

func makeEmbedding(dim int, val float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = val
	}
	return v
}

func TestTableName(t *testing.T) {
	tests := []struct {
		dim     int
		wantErr bool
		want    string
	}{
		{384, false, "memory_vectors_384"},
		{512, false, "memory_vectors_512"},
		{768, false, "memory_vectors_768"},
		{1024, false, "memory_vectors_1024"},
		{1536, false, "memory_vectors_1536"},
		{3072, false, "memory_vectors_3072"},
		{128, true, ""},
		{256, true, ""},
		{0, true, ""},
		{-1, true, ""},
		{2048, true, ""},
	}

	for _, tt := range tests {
		spec, err := resolveTableSpec(VectorKindMemory, tt.dim)
		if tt.wantErr {
			if err == nil {
				t.Errorf("resolveTableSpec(memory, %d) expected error, got %q", tt.dim, spec.table)
			}
		} else {
			if err != nil {
				t.Errorf("resolveTableSpec(memory, %d) unexpected error: %v", tt.dim, err)
			}
			if spec.table != tt.want {
				t.Errorf("resolveTableSpec(memory, %d).table = %q, want %q", tt.dim, spec.table, tt.want)
			}
		}
	}
}

func TestPgVectorStore_Ping(t *testing.T) {
	store := setupPgVectorTest(t)

	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestPgVectorStore_UpsertAndSearch(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID := uuid.New()
	memID1 := uuid.New()
	memID2 := uuid.New()

	// Create namespace and memories for the foreign key references.
	pool := store.pool
	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID, "test-ns"); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", memID1, nsID, "memory 1"); err != nil {
		t.Fatalf("insert memory 1: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", memID2, nsID, "memory 2"); err != nil {
		t.Fatalf("insert memory 2: %v", err)
	}

	dim := 384
	emb1 := makeEmbedding(dim, 1.0)
	emb2 := makeEmbedding(dim, 0.5)

	// Upsert two vectors.
	if err := store.Upsert(ctx, VectorKindMemory, memID1, nsID, emb1, dim); err != nil {
		t.Fatalf("Upsert 1: %v", err)
	}
	if err := store.Upsert(ctx, VectorKindMemory, memID2, nsID, emb2, dim); err != nil {
		t.Fatalf("Upsert 2: %v", err)
	}

	// Search — query vector identical to emb1 should rank memID1 first.
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

	// Upsert update — change emb1 to match emb2 and verify search changes.
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

func TestPgVectorStore_UpsertBatch(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID := uuid.New()
	pool := store.pool
	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID, "batch-ns"); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}

	// Create 3 memories across 2 dimensions.
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for i, id := range ids {
		if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", id, nsID, "batch memory "+string(rune('A'+i))); err != nil {
			t.Fatalf("insert memory %d: %v", i, err)
		}
	}

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

func TestPgVectorStore_UpsertBatch_Empty(t *testing.T) {
	store := setupPgVectorTest(t)

	if err := store.UpsertBatch(context.Background(), nil); err != nil {
		t.Fatalf("UpsertBatch with nil: %v", err)
	}
	if err := store.UpsertBatch(context.Background(), []VectorUpsertItem{}); err != nil {
		t.Fatalf("UpsertBatch with empty slice: %v", err)
	}
}

func TestPgVectorStore_UpsertBatch_InvalidDimension(t *testing.T) {
	store := setupPgVectorTest(t)

	items := []VectorUpsertItem{
		{ID: uuid.New(), NamespaceID: uuid.New(), Embedding: makeEmbedding(128, 1.0), Dimension: 128},
	}

	if err := store.UpsertBatch(context.Background(), items); err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

func TestPgVectorStore_Delete(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID := uuid.New()
	memID := uuid.New()
	pool := store.pool

	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID, "del-ns"); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", memID, nsID, "to delete"); err != nil {
		t.Fatalf("insert memory: %v", err)
	}

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

func TestPgVectorStore_SearchNamespaceScoping(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID1 := uuid.New()
	nsID2 := uuid.New()
	memID1 := uuid.New()
	memID2 := uuid.New()
	pool := store.pool

	// Create two namespaces.
	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID1, "ns1"); err != nil {
		t.Fatalf("insert namespace 1: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID2, "ns2"); err != nil {
		t.Fatalf("insert namespace 2: %v", err)
	}

	// Memories in different namespaces.
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", memID1, nsID1, "ns1 memory"); err != nil {
		t.Fatalf("insert memory 1: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", memID2, nsID2, "ns2 memory"); err != nil {
		t.Fatalf("insert memory 2: %v", err)
	}

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

func TestPgVectorStore_SearchExcludesSoftDeleted(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID := uuid.New()
	memID := uuid.New()
	pool := store.pool

	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID, "soft-del-ns"); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", memID, nsID, "soft deleted"); err != nil {
		t.Fatalf("insert memory: %v", err)
	}

	dim := 384
	emb := makeEmbedding(dim, 1.0)
	if err := store.Upsert(ctx, VectorKindMemory, memID, nsID, emb, dim); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Soft-delete the memory.
	if _, err := pool.Exec(ctx, "UPDATE memories SET deleted_at = NOW() WHERE id = $1", memID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	// Search should return no results.
	results, err := store.Search(ctx, VectorKindMemory, emb, nsID, dim, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for soft-deleted memory, got %d", len(results))
	}
}

func TestPgVectorStore_Upsert_InvalidDimension(t *testing.T) {
	store := setupPgVectorTest(t)

	err := store.Upsert(context.Background(), VectorKindMemory, uuid.New(), uuid.New(), makeEmbedding(128, 1.0), 128)
	if err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

func TestPgVectorStore_Search_InvalidDimension(t *testing.T) {
	store := setupPgVectorTest(t)

	_, err := store.Search(context.Background(), VectorKindMemory, makeEmbedding(128, 1.0), uuid.New(), 128, 10)
	if err == nil {
		t.Fatal("expected error for unsupported dimension 128, got nil")
	}
}

// approxEqualVec compares float32 slices within a tolerance large enough to
// absorb the pgvector wire-encoding round-trip.
func approxEqualVec(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		d := a[i] - b[i]
		if d < 0 {
			d = -d
		}
		if d > 1e-5 {
			return false
		}
	}
	return true
}

func TestPgVectorStore_GetByIDs_RoundTrip(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID := uuid.New()
	pool := store.pool
	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID, "getbyids-ns"); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for i, id := range ids {
		if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", id, nsID, "m"+string(rune('A'+i))); err != nil {
			t.Fatalf("insert memory %d: %v", i, err)
		}
	}

	dim := 384
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
		if !approxEqualVec(v, embs[i]) {
			t.Errorf("vector mismatch for id %s", id)
		}
	}
}

func TestPgVectorStore_GetByIDs_PartialAndEmpty(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID := uuid.New()
	pool := store.pool
	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID, "partial-ns"); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	stored := uuid.New()
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", stored, nsID, "stored"); err != nil {
		t.Fatalf("insert memory: %v", err)
	}
	dim := 384
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
		t.Errorf("missing id should not appear in result")
	}

	emptyResult, err := store.GetByIDs(ctx, VectorKindMemory, nil, dim)
	if err != nil {
		t.Fatalf("GetByIDs empty: %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("expected empty map for nil input, got %d", len(emptyResult))
	}
}

func TestPgVectorStore_GetByIDs_WrongDimension(t *testing.T) {
	store := setupPgVectorTestWithSchema(t)
	ctx := context.Background()

	nsID := uuid.New()
	pool := store.pool
	if _, err := pool.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, $2)", nsID, "dim-ns"); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	id := uuid.New()
	if _, err := pool.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, $3)", id, nsID, "x"); err != nil {
		t.Fatalf("insert memory: %v", err)
	}
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
