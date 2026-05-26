package hnsw

import (
	"bytes"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// assertFriendsInvariant fails the test if any node has an entry in friends[L]
// whose level is below L. Violating this invariant causes addSingle's
// back-connection to panic with "index out of range".
func assertFriendsInvariant(t *testing.T, g *Graph) {
	t.Helper()
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, gn := range g.nodes {
		for layer, friends := range gn.friends {
			for _, f := range friends {
				if f.level < layer {
					t.Errorf("invariant violated: node %s level=%d holds friend %s level=%d at layer=%d",
						gn.id, gn.level, f.id, f.level, layer)
				}
			}
		}
	}
}

func TestNewGraph(t *testing.T) {
	g := NewGraph(128)
	if g.Dimension() != 128 {
		t.Errorf("Dimension() = %d, want 128", g.Dimension())
	}
	if g.Len() != 0 {
		t.Errorf("Len() = %d, want 0", g.Len())
	}

	// With options.
	g2 := NewGraph(64, WithM(32), WithEfConstruction(400), WithEfSearch(100))
	if g2.m != 32 {
		t.Errorf("m = %d, want 32", g2.m)
	}
	if g2.mMax0 != 64 {
		t.Errorf("mMax0 = %d, want 64", g2.mMax0)
	}
	if g2.efConstruction != 400 {
		t.Errorf("efConstruction = %d, want 400", g2.efConstruction)
	}
	if g2.efSearch != 100 {
		t.Errorf("efSearch = %d, want 100", g2.efSearch)
	}
}

func TestAddSingle(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	id := uuid.New()
	err := g.Add(Node{ID: id, Vector: []float32{1, 2, 3}})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if g.Len() != 1 {
		t.Errorf("Len() = %d, want 1", g.Len())
	}
	if !g.Has(id) {
		t.Error("Has() = false, want true")
	}
}

func TestAddMultiple(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	nodes := make([]Node, 100)
	for i := range nodes {
		nodes[i] = Node{
			ID:     uuid.New(),
			Vector: []float32{float32(i), float32(i + 1), float32(i + 2)},
		}
	}
	err := g.Add(nodes...)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if g.Len() != 100 {
		t.Errorf("Len() = %d, want 100", g.Len())
	}
}

func TestAddDimensionMismatch(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	err := g.Add(Node{ID: uuid.New(), Vector: []float32{1, 2}})
	if err != ErrDimensionMismatch {
		t.Errorf("Add() error = %v, want ErrDimensionMismatch", err)
	}
}

func TestAddEmptyVector(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	err := g.Add(Node{ID: uuid.New(), Vector: []float32{}})
	if err != ErrEmptyVector {
		t.Errorf("Add() error = %v, want ErrEmptyVector", err)
	}
	err = g.Add(Node{ID: uuid.New(), Vector: nil})
	if err != ErrEmptyVector {
		t.Errorf("Add() error = %v, want ErrEmptyVector", err)
	}
}

func TestSearchBasic(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	target := Node{ID: uuid.New(), Vector: []float32{1, 0, 0}}
	near := Node{ID: uuid.New(), Vector: []float32{0.9, 0.1, 0}}
	far := Node{ID: uuid.New(), Vector: []float32{0, 0, 1}}

	err := g.Add(target, near, far)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	results, err := g.Search([]float32{1, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Search() returned %d results, want 2", len(results))
	}

	// The first result should be the target (exact match).
	if results[0].ID != target.ID {
		t.Errorf("Search() first result = %v, want %v", results[0].ID, target.ID)
	}
	if math.Abs(results[0].Score-1.0) > 1e-6 {
		t.Errorf("Search() first score = %f, want 1.0", results[0].Score)
	}
}

func TestSearchEmptyGraph(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	_, err := g.Search([]float32{1, 2, 3}, 5)
	if err != ErrEmptyGraph {
		t.Errorf("Search() error = %v, want ErrEmptyGraph", err)
	}
}

func TestSearchDimensionMismatch(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	_ = g.Add(Node{ID: uuid.New(), Vector: []float32{1, 2, 3}})
	_, err := g.Search([]float32{1, 2}, 1)
	if err != ErrDimensionMismatch {
		t.Errorf("Search() error = %v, want ErrDimensionMismatch", err)
	}
}

func TestSearchEmptyQuery(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	_ = g.Add(Node{ID: uuid.New(), Vector: []float32{1, 2, 3}})
	_, err := g.Search(nil, 1)
	if err != ErrEmptyVector {
		t.Errorf("Search() error = %v, want ErrEmptyVector", err)
	}
}

func TestDelete(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	id1 := uuid.New()
	id2 := uuid.New()
	_ = g.Add(
		Node{ID: id1, Vector: []float32{1, 0, 0}},
		Node{ID: id2, Vector: []float32{0, 1, 0}},
	)

	ok := g.Delete(id1)
	if !ok {
		t.Error("Delete() = false, want true")
	}
	if g.Len() != 1 {
		t.Errorf("Len() = %d, want 1", g.Len())
	}
	if g.Has(id1) {
		t.Error("Has(deleted) = true, want false")
	}

	// Search should still work.
	results, err := g.Search([]float32{0, 1, 0}, 1)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Search() returned %d results, want 1", len(results))
	}
	if results[0].ID != id2 {
		t.Errorf("Search() result = %v, want %v", results[0].ID, id2)
	}
}

func TestDeleteNonExistent(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	_ = g.Add(Node{ID: uuid.New(), Vector: []float32{1, 2, 3}})
	ok := g.Delete(uuid.New())
	if ok {
		t.Error("Delete(nonexistent) = true, want false")
	}
}

func TestDeleteEntryPoint(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	id1 := uuid.New()
	id2 := uuid.New()
	_ = g.Add(
		Node{ID: id1, Vector: []float32{1, 0, 0}},
		Node{ID: id2, Vector: []float32{0, 1, 0}},
	)

	// Delete the entry point.
	epID := g.entryPoint.id
	g.Delete(epID)

	if g.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", g.Len())
	}

	// Search should still work with the remaining node.
	results, err := g.Search([]float32{0, 1, 0}, 1)
	if err != nil {
		t.Fatalf("Search() after entry point deletion: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
}

func TestDeleteAll(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	id1 := uuid.New()
	id2 := uuid.New()
	_ = g.Add(
		Node{ID: id1, Vector: []float32{1, 0, 0}},
		Node{ID: id2, Vector: []float32{0, 1, 0}},
	)
	g.Delete(id1)
	g.Delete(id2)

	if g.Len() != 0 {
		t.Errorf("Len() = %d, want 0", g.Len())
	}

	_, err := g.Search([]float32{1, 0, 0}, 1)
	if err != ErrEmptyGraph {
		t.Errorf("Search on empty graph: got %v, want ErrEmptyGraph", err)
	}
}

func TestConcurrent(t *testing.T) {
	g := NewGraph(16, WithSeed(42))
	rng := rand.New(rand.NewSource(99))

	// Pre-populate with some data.
	for range 50 {
		v := make([]float32, 16)
		for j := range v {
			v[j] = rng.Float32()
		}
		_ = g.Add(Node{ID: uuid.New(), Vector: v})
	}

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Concurrent writers.
	for i := range 10 {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for range 10 {
				v := make([]float32, 16)
				for k := range v {
					v[k] = r.Float32()
				}
				if err := g.Add(Node{ID: uuid.New(), Vector: v}); err != nil {
					errs <- err
				}
			}
		}(int64(i * 1000))
	}

	// Concurrent readers.
	for i := range 10 {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for range 10 {
				q := make([]float32, 16)
				for k := range q {
					q[k] = r.Float32()
				}
				_, err := g.Search(q, 5)
				if err != nil {
					errs <- err
				}
			}
		}(int64(i*1000 + 500))
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent operation error: %v", err)
	}
}

func TestRecall(t *testing.T) {
	const (
		n          = 1000
		dim        = 128
		k          = 10
		numQueries = 20
	)

	g := NewGraph(dim, WithSeed(42), WithEfConstruction(200), WithEfSearch(100))
	rng := rand.New(rand.NewSource(42))

	// Generate random vectors.
	vectors := make([][]float32, n)
	ids := make([]uuid.UUID, n)
	for i := range n {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		vectors[i] = v
		ids[i] = uuid.New()
	}

	// Insert all vectors.
	for i := range n {
		err := g.Add(Node{ID: ids[i], Vector: vectors[i]})
		if err != nil {
			t.Fatalf("Add() error at %d: %v", i, err)
		}
	}

	// For several query vectors, compute brute-force top-k and compare with HNSW results.
	totalRecall := 0.0
	for range numQueries {
		query := make([]float32, dim)
		for j := range query {
			query[j] = rng.Float32()*2 - 1
		}

		// Brute-force top-k.
		type bruteResult struct {
			id    uuid.UUID
			score float64
		}
		brute := make([]bruteResult, n)
		for i := range n {
			brute[i] = bruteResult{
				id:    ids[i],
				score: CosineSimilarity(query, vectors[i]),
			}
		}
		// Sort descending.
		for i := range brute {
			for j := i + 1; j < len(brute); j++ {
				if brute[j].score > brute[i].score {
					brute[i], brute[j] = brute[j], brute[i]
				}
			}
		}
		trueTopK := make(map[uuid.UUID]bool, k)
		for i := range k {
			trueTopK[brute[i].id] = true
		}

		// HNSW search.
		results, err := g.Search(query, k)
		if err != nil {
			t.Fatalf("Search() error: %v", err)
		}

		// Count recall.
		hits := 0
		for _, r := range results {
			if trueTopK[r.ID] {
				hits++
			}
		}
		totalRecall += float64(hits) / float64(k)
	}

	avgRecall := totalRecall / float64(numQueries)
	t.Logf("Average recall@%d: %.2f%% (%d queries, %d vectors, %dd)", k, avgRecall*100, numQueries, n, dim)
	if avgRecall < 0.90 {
		t.Errorf("Recall = %.2f%%, want >= 90%%", avgRecall*100)
	}
}

func TestDuplicateAdd(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	id := uuid.New()

	_ = g.Add(Node{ID: id, Vector: []float32{1, 0, 0}})
	if g.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", g.Len())
	}

	// Add the same ID with a different vector.
	_ = g.Add(Node{ID: id, Vector: []float32{0, 1, 0}})
	if g.Len() != 1 {
		t.Errorf("Len() after duplicate = %d, want 1", g.Len())
	}

	// Search should find the updated vector.
	results, _ := g.Search([]float32{0, 1, 0}, 1)
	if len(results) != 1 {
		t.Fatalf("Search() returned %d results, want 1", len(results))
	}
	if results[0].ID != id {
		t.Errorf("Search() returned wrong ID")
	}
	if math.Abs(results[0].Score-1.0) > 1e-6 {
		t.Errorf("Search() score = %f, want 1.0 (vector should be updated)", results[0].Score)
	}
}

func TestSearchMoreThanExists(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	id1 := uuid.New()
	id2 := uuid.New()
	_ = g.Add(
		Node{ID: id1, Vector: []float32{1, 0, 0}},
		Node{ID: id2, Vector: []float32{0, 1, 0}},
	)

	results, err := g.Search([]float32{1, 0, 0}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Search(k=10) returned %d results, want 2 (all nodes)", len(results))
	}
}

func TestSearchResultOrder(t *testing.T) {
	g := NewGraph(3, WithSeed(1))
	_ = g.Add(
		Node{ID: uuid.New(), Vector: []float32{1, 0, 0}},
		Node{ID: uuid.New(), Vector: []float32{0.9, 0.1, 0}},
		Node{ID: uuid.New(), Vector: []float32{0.5, 0.5, 0}},
		Node{ID: uuid.New(), Vector: []float32{0, 0, 1}},
	)

	results, err := g.Search([]float32{1, 0, 0}, 4)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	// Verify results are in descending score order.
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("Results not sorted: [%d].Score=%f > [%d].Score=%f",
				i, results[i].Score, i-1, results[i-1].Score)
		}
	}
}

// TestSnapshotRoundTripThenPerCallAdd reproduces the contradiction-phase
// self-heal path that panicked in production: the graph is loaded from a
// snapshot (Export -> Import) and then a batch of fresh ids is inserted via
// individual Add calls (one lock acquisition per node, matching
// HNSWStore.UpsertBatch). The test asserts no panic and that the friends-list
// invariant holds throughout — every entry in any node's friends[L] has
// level >= L.
func TestSnapshotRoundTripThenPerCallAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test; skipped under -short")
	}

	const (
		dim      = 32
		preCount = 500
		newCount = 75
	)

	for trial := range 40 {
		seed := int64(20260430 + trial)
		t.Run("seed="+strconv.FormatInt(seed, 10), func(t *testing.T) {
			g := NewGraph(dim, WithSeed(seed), WithEfConstruction(200), WithEfSearch(50))
			rng := rand.New(rand.NewSource(seed + 1))

			for i := range preCount {
				v := make([]float32, dim)
				for j := range v {
					v[j] = rng.Float32()*2 - 1
				}
				if err := g.Add(Node{ID: uuid.New(), Vector: v}); err != nil {
					t.Fatalf("pre-populate Add %d: %v", i, err)
				}
			}
			assertFriendsInvariant(t, g)

			var buf bytes.Buffer
			if err := g.Export(&buf); err != nil {
				t.Fatalf("Export: %v", err)
			}
			imported, err := Import(&buf)
			if err != nil {
				t.Fatalf("Import: %v", err)
			}
			assertFriendsInvariant(t, imported)

			for i := range newCount {
				v := make([]float32, dim)
				for j := range v {
					v[j] = rng.Float32()*2 - 1
				}
				if err := imported.Add(Node{ID: uuid.New(), Vector: v}); err != nil {
					t.Fatalf("post-snapshot Add %d: %v", i, err)
				}
			}
			assertFriendsInvariant(t, imported)

			if imported.Len() != preCount+newCount {
				t.Errorf("Len: got %d, want %d", imported.Len(), preCount+newCount)
			}
		})
	}
}
