package hnsw

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

func TestExportImportEmpty(t *testing.T) {
	g := NewGraph(64, WithM(8), WithEfConstruction(100), WithEfSearch(30))

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export empty graph: %v", err)
	}

	imported, err := Import(&buf)
	if err != nil {
		t.Fatalf("Import empty graph: %v", err)
	}

	if imported.Dimension() != 64 {
		t.Errorf("dimension: got %d, want 64", imported.Dimension())
	}
	if imported.Len() != 0 {
		t.Errorf("node count: got %d, want 0", imported.Len())
	}
	if imported.m != 8 {
		t.Errorf("M: got %d, want 8", imported.m)
	}
	if imported.mMax0 != 16 {
		t.Errorf("MMax0: got %d, want 16", imported.mMax0)
	}
	if imported.efConstruction != 100 {
		t.Errorf("EfConstruction: got %d, want 100", imported.efConstruction)
	}
	if imported.efSearch != 30 {
		t.Errorf("EfSearch: got %d, want 30", imported.efSearch)
	}
}

func TestExportImportSingle(t *testing.T) {
	g := NewGraph(8, WithSeed(99))
	id := uuid.New()
	vec := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	if err := g.Add(Node{ID: id, Vector: vec}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	imported, err := Import(&buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}

	if imported.Len() != 1 {
		t.Fatalf("node count: got %d, want 1", imported.Len())
	}
	if !imported.Has(id) {
		t.Fatalf("imported graph missing node %s", id)
	}

	// Search should return the single node.
	results, err := imported.Search(vec, 1)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 || results[0].ID != id {
		t.Fatalf("search results: got %v, want [{%s ~1.0}]", results, id)
	}
	if results[0].Score < 0.999 {
		t.Errorf("score: got %f, want ~1.0", results[0].Score)
	}
}

func TestExportImportMultiple(t *testing.T) {
	const (
		dim   = 64
		count = 100
	)
	rng := rand.New(rand.NewSource(12345))
	g := NewGraph(dim, WithM(16), WithEfConstruction(200), WithEfSearch(50), WithSeed(42))

	ids := make([]uuid.UUID, count)
	for i := 0; i < count; i++ {
		ids[i] = uuid.New()
		if err := g.Add(Node{ID: ids[i], Vector: randomVector(rng, dim)}); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
	}

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	imported, err := Import(&buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}

	// Verify counts and params.
	if imported.Len() != count {
		t.Errorf("node count: got %d, want %d", imported.Len(), count)
	}
	if imported.Dimension() != dim {
		t.Errorf("dimension: got %d, want %d", imported.Dimension(), dim)
	}
	if imported.m != g.m {
		t.Errorf("M: got %d, want %d", imported.m, g.m)
	}
	if imported.efConstruction != g.efConstruction {
		t.Errorf("EfConstruction: got %d, want %d", imported.efConstruction, g.efConstruction)
	}
	if imported.efSearch != g.efSearch {
		t.Errorf("EfSearch: got %d, want %d", imported.efSearch, g.efSearch)
	}

	// Verify all nodes present.
	for _, id := range ids {
		if !imported.Has(id) {
			t.Errorf("missing node %s", id)
		}
	}

	// Verify search results match.
	queryRng := rand.New(rand.NewSource(9999))
	for q := 0; q < 5; q++ {
		query := randomVector(queryRng, dim)
		origResults, err := g.Search(query, 10)
		if err != nil {
			t.Fatalf("original Search: %v", err)
		}
		impResults, err := imported.Search(query, 10)
		if err != nil {
			t.Fatalf("imported Search: %v", err)
		}
		if len(origResults) != len(impResults) {
			t.Errorf("query %d: result count %d vs %d", q, len(origResults), len(impResults))
			continue
		}
		for i := range origResults {
			if origResults[i].ID != impResults[i].ID {
				t.Errorf("query %d result %d: ID %s vs %s", q, i, origResults[i].ID, impResults[i].ID)
			}
		}
	}
}

func TestExportImportWithDeletes(t *testing.T) {
	const dim = 32
	rng := rand.New(rand.NewSource(777))
	g := NewGraph(dim, WithSeed(42))

	ids := make([]uuid.UUID, 50)
	for i := range ids {
		ids[i] = uuid.New()
		if err := g.Add(Node{ID: ids[i], Vector: randomVector(rng, dim)}); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
	}

	// Delete first 10.
	deleted := make(map[uuid.UUID]bool)
	for i := 0; i < 10; i++ {
		g.Delete(ids[i])
		deleted[ids[i]] = true
	}

	if g.Len() != 40 {
		t.Fatalf("after delete: got %d nodes, want 40", g.Len())
	}

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	imported, err := Import(&buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}

	if imported.Len() != 40 {
		t.Errorf("imported node count: got %d, want 40", imported.Len())
	}

	for id := range deleted {
		if imported.Has(id) {
			t.Errorf("deleted node %s still present in imported graph", id)
		}
	}
	for _, id := range ids[10:] {
		if !imported.Has(id) {
			t.Errorf("surviving node %s missing from imported graph", id)
		}
	}
}

func TestImportInvalidMagic(t *testing.T) {
	data := []byte("JUNK and some more garbage bytes for good measure")
	_, err := Import(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error for invalid magic bytes")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("invalid magic")) {
		t.Errorf("error should mention invalid magic, got: %v", err)
	}
}

func TestImportTruncated(t *testing.T) {
	// Write a valid header but truncate it partway through.
	g := NewGraph(16, WithSeed(1))
	id := uuid.New()
	vec := make([]float32, 16)
	for i := range vec {
		vec[i] = float32(i)
	}
	if err := g.Add(Node{ID: id, Vector: vec}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	var full bytes.Buffer
	if err := g.Export(&full); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Try importing at various truncation points.
	data := full.Bytes()
	truncPoints := []int{0, 2, 4, 10, 20, len(data) / 2, len(data) - 1}
	for _, n := range truncPoints {
		if n > len(data) {
			continue
		}
		_, err := Import(bytes.NewReader(data[:n]))
		if err == nil {
			t.Errorf("expected error for truncated data at %d bytes (total %d)", n, len(data))
		}
	}
}

func TestExportImportPreservesSearchResults(t *testing.T) {
	const (
		dim       = 128
		count     = 500
		numQueries = 10
		k         = 10
	)
	rng := rand.New(rand.NewSource(54321))
	g := NewGraph(dim, WithM(16), WithEfConstruction(200), WithEfSearch(100), WithSeed(42))

	for i := 0; i < count; i++ {
		if err := g.Add(Node{ID: uuid.New(), Vector: randomVector(rng, dim)}); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
	}

	// Generate queries and collect original results.
	queryRng := rand.New(rand.NewSource(11111))
	queries := make([][]float32, numQueries)
	origResults := make([][]SearchResult, numQueries)
	for q := 0; q < numQueries; q++ {
		queries[q] = randomVector(queryRng, dim)
		var err error
		origResults[q], err = g.Search(queries[q], k)
		if err != nil {
			t.Fatalf("original Search[%d]: %v", q, err)
		}
	}

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	imported, err := Import(&buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}

	for q := 0; q < numQueries; q++ {
		impResults, err := imported.Search(queries[q], k)
		if err != nil {
			t.Fatalf("imported Search[%d]: %v", q, err)
		}
		if len(origResults[q]) != len(impResults) {
			t.Errorf("query %d: result count %d vs %d", q, len(origResults[q]), len(impResults))
			continue
		}
		for i := range origResults[q] {
			if origResults[q][i].ID != impResults[i].ID {
				t.Errorf("query %d result %d: ID mismatch %s vs %s", q, i, origResults[q][i].ID, impResults[i].ID)
			}
			if origResults[q][i].Score != impResults[i].Score {
				t.Errorf("query %d result %d: score mismatch %f vs %f", q, i, origResults[q][i].Score, impResults[i].Score)
			}
		}
	}
}

func TestImportedGraphIsFullyFunctional(t *testing.T) {
	const dim = 16
	rng := rand.New(rand.NewSource(333))
	g := NewGraph(dim, WithSeed(42))

	// Add initial nodes.
	for i := 0; i < 20; i++ {
		if err := g.Add(Node{ID: uuid.New(), Vector: randomVector(rng, dim)}); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
	}

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	imported, err := Import(&buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}

	// Add new nodes to the imported graph.
	newIDs := make([]uuid.UUID, 10)
	for i := range newIDs {
		newIDs[i] = uuid.New()
		if err := imported.Add(Node{ID: newIDs[i], Vector: randomVector(rng, dim)}); err != nil {
			t.Fatalf("Add to imported[%d]: %v", i, err)
		}
	}

	if imported.Len() != 30 {
		t.Errorf("after adding: got %d nodes, want 30", imported.Len())
	}

	// Search should work.
	query := randomVector(rng, dim)
	results, err := imported.Search(query, 5)
	if err != nil {
		t.Fatalf("Search on imported: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("search result count: got %d, want 5", len(results))
	}

	// Delete should work.
	if !imported.Delete(newIDs[0]) {
		t.Error("Delete returned false for existing node")
	}
	if imported.Len() != 29 {
		t.Errorf("after delete: got %d nodes, want 29", imported.Len())
	}
	if imported.Has(newIDs[0]) {
		t.Error("deleted node still present")
	}
}
