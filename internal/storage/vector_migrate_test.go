package storage

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// --- test doubles for the Qdrant side, so the core is exercised without a
// live server ---

type fakeQdrantTarget struct {
	items   map[string]VectorUpsertItem
	ensured bool
}

func newFakeQdrantTarget() *fakeQdrantTarget {
	return &fakeQdrantTarget{items: map[string]VectorUpsertItem{}}
}

func fakeKey(kind VectorKind, dim int, id uuid.UUID) string {
	return fmt.Sprintf("%s|%d|%s", kind, dim, id)
}

func (f *fakeQdrantTarget) EnsureCollections(_ context.Context) error {
	f.ensured = true
	return nil
}

func (f *fakeQdrantTarget) UpsertBatch(_ context.Context, items []VectorUpsertItem) error {
	for _, it := range items {
		f.items[fakeKey(it.EffectiveKind(), it.Dimension, it.ID)] = it
	}
	return nil
}

func (f *fakeQdrantTarget) CountVectors(_ context.Context, kind VectorKind, dim int) (int, error) {
	n := 0
	for _, it := range f.items {
		if it.EffectiveKind() == kind && it.Dimension == dim {
			n++
		}
	}
	return n, nil
}

type fakePoint struct {
	kind VectorKind
	dim  int
	id   uuid.UUID
	ns   uuid.UUID
	vec  []float32
}

type fakeQdrantSource struct {
	points []fakePoint
}

func (f *fakeQdrantSource) IterateVectors(_ context.Context, kind VectorKind, dim int, _ int, fn func(id, ns uuid.UUID, vec []float32) error) error {
	for _, p := range f.points {
		if p.kind == kind && p.dim == dim {
			if err := fn(p.id, p.ns, p.vec); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- seeding helpers (backend-aware raw SQL) ---

func testVec(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		// values exactly representable in float32 so they round-trip through
		// both the pgvector text cast and the SQLite BLOB encoding.
		v[i] = float32(i%7) * 0.125
	}
	return v
}

func formatVec(v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = strconv.FormatFloat(float64(f), 'f', -1, 32)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func seedMemory(t *testing.T, ctx context.Context, db DB, nsID, id uuid.UUID, deleted bool) {
	t.Helper()
	var deletedAt any
	if deleted {
		deletedAt = "2026-01-01T00:00:00Z"
	}
	q := "INSERT INTO memories (id, namespace_id, content, deleted_at) VALUES (?, ?, ?, ?)"
	if db.Backend() == BackendPostgres {
		q = "INSERT INTO memories (id, namespace_id, content, deleted_at) VALUES ($1, $2, $3, $4)"
	}
	if _, err := db.Exec(ctx, q, id.String(), nsID.String(), "test content", deletedAt); err != nil {
		t.Fatalf("seed memory: %v", err)
	}
}

func seedEntity(t *testing.T, ctx context.Context, db DB, nsID, id uuid.UUID) {
	t.Helper()
	q := "INSERT INTO entities (id, namespace_id, name, canonical, entity_type) VALUES (?, ?, ?, ?, ?)"
	if db.Backend() == BackendPostgres {
		q = "INSERT INTO entities (id, namespace_id, name, canonical, entity_type) VALUES ($1, $2, $3, $4, $5)"
	}
	canonical := "canon-" + id.String()[:8]
	if _, err := db.Exec(ctx, q, id.String(), nsID.String(), "name", canonical, "concept"); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
}

func seedVector(t *testing.T, ctx context.Context, db DB, kind VectorKind, id, nsID uuid.UUID, vec []float32, dim int) {
	t.Helper()
	if db.Backend() == BackendPostgres {
		table := fmt.Sprintf("memory_vectors_%d", dim)
		idCol := "memory_id"
		if kind == VectorKindEntity {
			table = fmt.Sprintf("entity_vectors_%d", dim)
			idCol = "entity_id"
		}
		q := fmt.Sprintf("INSERT INTO %s (%s, embedding) VALUES ($1, $2::vector)", table, idCol)
		if _, err := db.Exec(ctx, q, id.String(), formatVec(vec)); err != nil {
			t.Fatalf("seed pg vector: %v", err)
		}
		return
	}
	table := "memory_vectors"
	idCol := "memory_id"
	if kind == VectorKindEntity {
		table = "entity_vectors"
		idCol = "entity_id"
	}
	q := fmt.Sprintf("INSERT INTO %s (%s, namespace_id, dimension, embedding) VALUES (?, ?, ?, ?)", table, idCol)
	if _, err := db.Exec(ctx, q, id.String(), nsID.String(), dim, hnsw.EncodeVector(vec)); err != nil {
		t.Fatalf("seed sqlite vector: %v", err)
	}
}

func vecsEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if math.Abs(float64(a[i]-b[i])) > 1e-6 {
			return false
		}
	}
	return true
}

// --- tests ---

func TestParsePgVectorText(t *testing.T) {
	cases := []struct {
		in      string
		want    []float32
		wantErr bool
	}{
		{"[]", []float32{}, false},
		{"[0.5]", []float32{0.5}, false},
		{"[0.1,0.2,0.3]", []float32{0.1, 0.2, 0.3}, false},
		{"  [1,2]  ", []float32{1, 2}, false},
		{"0.1,0.2", nil, true},
		{"", nil, true},
		{"[a]", nil, true},
	}
	for _, c := range cases {
		got, err := parsePgVectorText(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parsePgVectorText(%q): expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parsePgVectorText(%q): unexpected error %v", c.in, err)
			continue
		}
		if !vecsEqual(got, c.want) {
			t.Errorf("parsePgVectorText(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestMigrateVectorsToQdrant(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)

		// 768-dim memories: 2 live, 1 soft-deleted (must be excluded).
		live768 := map[uuid.UUID][]float32{}
		for range 2 {
			id := uuid.New()
			vec := testVec(768)
			seedMemory(t, ctx, db, nsID, id, false)
			seedVector(t, ctx, db, VectorKindMemory, id, nsID, vec, 768)
			live768[id] = vec
		}
		deletedID := uuid.New()
		seedMemory(t, ctx, db, nsID, deletedID, true)
		seedVector(t, ctx, db, VectorKindMemory, deletedID, nsID, testVec(768), 768)

		// 384-dim memory.
		mem384 := uuid.New()
		vec384 := testVec(384)
		seedMemory(t, ctx, db, nsID, mem384, false)
		seedVector(t, ctx, db, VectorKindMemory, mem384, nsID, vec384, 384)

		// 768-dim entities.
		ent := map[uuid.UUID][]float32{}
		for range 2 {
			id := uuid.New()
			vec := testVec(768)
			seedEntity(t, ctx, db, nsID, id)
			seedVector(t, ctx, db, VectorKindEntity, id, nsID, vec, 768)
			ent[id] = vec
		}

		// Dry run: counts but no writes.
		dryTarget := newFakeQdrantTarget()
		dryStats, err := MigrateVectorsToQdrant(ctx, db, dryTarget, 2, true, nil)
		if err != nil {
			t.Fatalf("dry run: %v", err)
		}
		if !dryStats.DryRun || dryStats.MemoryCount != 3 || dryStats.EntityCount != 2 {
			t.Fatalf("dry run stats: memory=%d entity=%d dry=%v (want 3/2/true)", dryStats.MemoryCount, dryStats.EntityCount, dryStats.DryRun)
		}
		if len(dryTarget.items) != 0 {
			t.Fatalf("dry run wrote %d items, expected 0", len(dryTarget.items))
		}

		// Real run.
		target := newFakeQdrantTarget()
		stats, err := MigrateVectorsToQdrant(ctx, db, target, 2, false, nil)
		if err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if !target.ensured {
			t.Error("EnsureCollections not called on real run")
		}
		if stats.MemoryCount != 3 {
			t.Errorf("MemoryCount = %d, want 3 (2 live 768 + 1 live 384, soft-deleted excluded)", stats.MemoryCount)
		}
		if stats.EntityCount != 2 {
			t.Errorf("EntityCount = %d, want 2", stats.EntityCount)
		}
		if stats.Mismatch() {
			t.Error("unexpected mismatch on full copy")
		}

		// Soft-deleted memory must be absent.
		if _, ok := target.items[fakeKey(VectorKindMemory, 768, deletedID)]; ok {
			t.Error("soft-deleted memory vector was migrated")
		}
		// Live 768 memories present with correct vectors.
		for id, vec := range live768 {
			it, ok := target.items[fakeKey(VectorKindMemory, 768, id)]
			if !ok {
				t.Errorf("missing memory %s", id)
				continue
			}
			if it.NamespaceID != nsID || !vecsEqual(it.Embedding, vec) {
				t.Errorf("memory %s: ns or vector mismatch", id)
			}
		}
		// 384 memory present.
		if it, ok := target.items[fakeKey(VectorKindMemory, 384, mem384)]; !ok || !vecsEqual(it.Embedding, vec384) {
			t.Error("384-dim memory not migrated correctly")
		}
		// Entities present.
		for id, vec := range ent {
			it, ok := target.items[fakeKey(VectorKindEntity, 768, id)]
			if !ok || !vecsEqual(it.Embedding, vec) {
				t.Errorf("entity %s not migrated correctly", id)
			}
		}

		// Idempotent: a second run yields the same counts and no duplication.
		stats2, err := MigrateVectorsToQdrant(ctx, db, target, 2, false, nil)
		if err != nil {
			t.Fatalf("second migrate: %v", err)
		}
		if stats2.MemoryCount != 3 || stats2.EntityCount != 2 {
			t.Errorf("second run stats memory=%d entity=%d, want 3/2", stats2.MemoryCount, stats2.EntityCount)
		}
		// 2 live 768 + 1 384 memory + 2 entity = 5 distinct points.
		if len(target.items) != 5 {
			t.Errorf("after re-run target holds %d items, want 5 (no duplication)", len(target.items))
		}
	})
}

// TestMigrateVectorsFromQdrantToHNSW proves the reverse path writes rows into
// the SQLite store AND leaves the HNSW index queryable (the failure mode of
// writing raw rows behind the index's back). SQLite-only because it exercises
// the HNSW writer.
func TestMigrateVectorsFromQdrantToHNSW(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsID := createTestNamespace(t, ctx, db)

	// Parent rows must exist for the vector FKs.
	memIDs := []uuid.UUID{uuid.New(), uuid.New()}
	entIDs := []uuid.UUID{uuid.New(), uuid.New()}
	src := &fakeQdrantSource{}
	for _, id := range memIDs {
		seedMemory(t, ctx, db, nsID, id, false)
		src.points = append(src.points, fakePoint{kind: VectorKindMemory, dim: 768, id: id, ns: nsID, vec: testVec(768)})
	}
	for _, id := range entIDs {
		seedEntity(t, ctx, db, nsID, id)
		src.points = append(src.points, fakePoint{kind: VectorKindEntity, dim: 768, id: id, ns: nsID, vec: testVec(768)})
	}

	dst := NewHNSWStore(db.DB(), db.WriteDB(), DefaultHNSWConfig())
	defer func() { _ = dst.Close() }()

	stats, err := MigrateVectorsFromQdrant(ctx, db, src, dst, 10, false, nil)
	if err != nil {
		t.Fatalf("reverse migrate: %v", err)
	}
	if stats.MemoryCount != 2 || stats.EntityCount != 2 {
		t.Fatalf("reverse stats memory=%d entity=%d, want 2/2", stats.MemoryCount, stats.EntityCount)
	}

	// Rows landed in the SQL table.
	if n, err := countSQLVectors(ctx, db, VectorKindMemory, 768); err != nil || n != 2 {
		t.Fatalf("memory_vectors count = %d (err %v), want 2", n, err)
	}

	// Index is queryable and returns the migrated memories.
	results, err := dst.Search(ctx, VectorKindMemory, testVec(768), nsID, 768, 10)
	if err != nil {
		t.Fatalf("search after reverse migrate: %v", err)
	}
	found := map[uuid.UUID]bool{}
	for _, r := range results {
		found[r.ID] = true
	}
	for _, id := range memIDs {
		if !found[id] {
			t.Errorf("memory %s not returned by HNSW search after reverse migration", id)
		}
	}
}

// TestMigrateVectorsToQdrantLive is an opt-in end-to-end test against a real
// Qdrant. It is non-destructive: it writes points under random UUIDs and a
// random namespace and deletes exactly those points on cleanup, never touching
// unrelated collections. Set NRAM_TEST_QDRANT_ADDR (e.g. 192.168.2.63:6333) to
// run it.
func TestMigrateVectorsToQdrantLive(t *testing.T) {
	addr := os.Getenv("NRAM_TEST_QDRANT_ADDR")
	if addr == "" {
		t.Skip("NRAM_TEST_QDRANT_ADDR not set; skipping live Qdrant test")
	}
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsID := createTestNamespace(t, ctx, db)

	qs, err := NewQdrantStore(QdrantConfig{Addr: addr})
	if err != nil {
		t.Fatalf("connect qdrant %s: %v", addr, err)
	}
	// Registered first so it runs LAST (t.Cleanup is LIFO): the point-delete
	// cleanup below must run while the client is still open.
	t.Cleanup(func() { _ = qs.Close() })
	if err := qs.Ping(ctx); err != nil {
		t.Skipf("qdrant %s not reachable: %v", addr, err)
	}

	seeded := map[uuid.UUID][]float32{}
	for range 3 {
		id := uuid.New()
		vec := testVec(768)
		seedMemory(t, ctx, db, nsID, id, false)
		seedVector(t, ctx, db, VectorKindMemory, id, nsID, vec, 768)
		seeded[id] = vec
	}
	// Clean up our own points across all dim collections, even on failure.
	t.Cleanup(func() {
		for id := range seeded {
			_ = qs.Delete(context.Background(), VectorKindMemory, id)
		}
	})

	if _, err := MigrateVectorsToQdrant(ctx, db, qs, 100, false, nil); err != nil {
		t.Fatalf("live migrate: %v", err)
	}

	ids := make([]uuid.UUID, 0, len(seeded))
	for id := range seeded {
		ids = append(ids, id)
	}
	got, err := qs.GetByIDs(ctx, VectorKindMemory, ids, 768)
	if err != nil {
		t.Fatalf("get-by-ids: %v", err)
	}
	for id, vec := range seeded {
		gv, ok := got[id]
		if !ok {
			t.Errorf("point %s not found in qdrant after migration", id)
			continue
		}
		// Qdrant normalizes vectors for Cosine collections, so compare by
		// direction (cosine similarity ~ 1.0) rather than exact bytes.
		if sim := cosineSim(gv, vec); sim < 0.9999 {
			t.Errorf("point %s direction mismatch after round trip: cosine=%f", id, sim)
		}
	}
}

func cosineSim(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
