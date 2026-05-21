package hnsw

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

// findNodesByLevelLocked returns one node with level >= 1 and one node with
// level == 0 from the graph. Caller must hold g.mu (read or write).
func findNodesByLevelLocked(t *testing.T, g *Graph) (high, low *graphNode) {
	t.Helper()
	for _, n := range g.nodes {
		if high == nil && n.level >= 1 {
			high = n
		}
		if low == nil && n.level == 0 {
			low = n
		}
		if high != nil && low != nil && high != low {
			return high, low
		}
	}
	t.Fatalf("graph lacks both a level>=1 and a level==0 node (have high=%v low=%v)", high, low)
	return nil, nil
}

// populateGraph adds n deterministic random vectors to g.
func populateGraph(t *testing.T, g *Graph, n int, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < n; i++ {
		vec := make([]float32, g.Dimension())
		for d := range vec {
			vec[d] = float32(rng.NormFloat64())
		}
		if err := g.Add(Node{ID: uuid.New(), Vector: vec}); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
	}
}

// newTestGraph builds a Graph with reduced efConstruction so test populations
// finish quickly; recall quality is irrelevant to these tests.
func newTestGraph(seed int64) *Graph {
	return NewGraph(8, WithSeed(seed), WithEfConstruction(20))
}

// exportRawForTest writes a snapshot using the same wire format as Export but
// without the strict filter, so the produced bytes look like one a pre-fix
// binary would have written.
func exportRawForTest(w io.Writer, g *Graph) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return exportLocked(w, g, false)
}

func TestImportRepairsForwardEdgeInvariant(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(t *testing.T, g *Graph)
	}{
		{
			name: "forward edge to lower-level neighbor",
			corrupt: func(t *testing.T, g *Graph) {
				g.mu.Lock()
				defer g.mu.Unlock()
				high, low := findNodesByLevelLocked(t, g)
				high.friends[1] = append(high.friends[1], low)
			},
		},
		{
			name: "self loop",
			corrupt: func(t *testing.T, g *Graph) {
				g.mu.Lock()
				defer g.mu.Unlock()
				for _, n := range g.nodes {
					n.friends[0] = append(n.friends[0], n)
					return
				}
				t.Fatalf("empty graph")
			},
		},
		{
			name: "duplicate neighbor at same layer",
			corrupt: func(t *testing.T, g *Graph) {
				g.mu.Lock()
				defer g.mu.Unlock()
				for _, n := range g.nodes {
					if len(n.friends[0]) == 0 {
						continue
					}
					n.friends[0] = append(n.friends[0], n.friends[0][0])
					return
				}
				t.Fatalf("no node had any layer-0 friends")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestGraph(7)
			populateGraph(t, g, 120, 11)
			tc.corrupt(t, g)

			var buf bytes.Buffer
			if err := g.Export(&buf); err != nil {
				t.Fatalf("Export: %v", err)
			}
			imported, err := Import(&buf)
			if err != nil {
				t.Fatalf("Import: %v", err)
			}
			assertFriendsInvariant(t, imported)
			if imported.Len() != g.Len() {
				t.Errorf("imported node count = %d, want %d", imported.Len(), g.Len())
			}
		})
	}
}

func TestImportRepairCountsCorruption(t *testing.T) {
	g := newTestGraph(7)
	populateGraph(t, g, 120, 13)

	g.mu.Lock()
	high, low := findNodesByLevelLocked(t, g)
	high.friends[1] = append(high.friends[1], low)
	g.mu.Unlock()

	var buf bytes.Buffer
	if err := exportRawForTest(&buf, g); err != nil {
		t.Fatalf("exportRawForTest: %v", err)
	}

	imported, stats, err := ImportWithStats(&buf)
	if err != nil {
		t.Fatalf("ImportWithStats: %v", err)
	}
	if stats.ForwardDropped != 1 {
		t.Errorf("ForwardDropped = %d, want 1 (stats=%+v)", stats.ForwardDropped, stats)
	}
	if !stats.AnyDropped() {
		t.Errorf("AnyDropped = false, want true (stats=%+v)", stats)
	}
	assertFriendsInvariant(t, imported)
}

func TestRemoveLockedSurvivesCorruptedFriends(t *testing.T) {
	g := newTestGraph(7)
	populateGraph(t, g, 80, 17)

	beforeLen := g.Len()

	g.mu.Lock()
	high, low := findNodesByLevelLocked(t, g)
	highID := high.id
	high.friends[1] = append(high.friends[1], low)
	g.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Delete panicked on corrupted graph: %v", r)
		}
	}()

	if !g.Delete(highID) {
		t.Fatalf("Delete(%s) returned false", highID)
	}
	if g.Len() != beforeLen-1 {
		t.Errorf("Len() = %d, want %d", g.Len(), beforeLen-1)
	}
}

func TestExportStripsInvariantViolatingEdges(t *testing.T) {
	g := newTestGraph(7)
	populateGraph(t, g, 100, 19)

	g.mu.Lock()
	high, low := findNodesByLevelLocked(t, g)
	high.friends[1] = append(high.friends[1], low)
	high.friends[0] = append(high.friends[0], high)
	if len(low.friends[0]) > 0 {
		low.friends[0] = append(low.friends[0], low.friends[0][0])
	}
	g.mu.Unlock()

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	imported, stats, err := ImportWithStats(&buf)
	if err != nil {
		t.Fatalf("ImportWithStats: %v", err)
	}
	if stats.AnyDropped() {
		t.Errorf("repair after Export should be a no-op, got stats=%+v", stats)
	}
	assertFriendsInvariant(t, imported)
}

func TestRepairIsNoOpOnCleanGraph(t *testing.T) {
	g := newTestGraph(7)
	populateGraph(t, g, 150, 23)

	g.mu.RLock()
	ids := make([]uuid.UUID, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	g.mu.RUnlock()
	rng := rand.New(rand.NewSource(29))
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	for _, id := range ids[:len(ids)/3] {
		g.Delete(id)
	}

	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	imported, stats, err := ImportWithStats(&buf)
	if err != nil {
		t.Fatalf("ImportWithStats: %v", err)
	}
	if stats.AnyDropped() {
		t.Errorf("clean graph round-trip should not require repair, got stats=%+v", stats)
	}
	assertFriendsInvariant(t, imported)
	if imported.Len() != g.Len() {
		t.Errorf("imported.Len()=%d, want %d", imported.Len(), g.Len())
	}
}

func TestSnapshotRoundTripThenDeleteCorrupted(t *testing.T) {
	g := newTestGraph(31)
	populateGraph(t, g, 200, 37)

	g.mu.Lock()
	high, low := findNodesByLevelLocked(t, g)
	highID := high.id
	high.friends[1] = append(high.friends[1], low)
	g.mu.Unlock()

	var buf bytes.Buffer
	if err := exportRawForTest(&buf, g); err != nil {
		t.Fatalf("exportRawForTest: %v", err)
	}

	imported, stats, err := ImportWithStats(&buf)
	if err != nil {
		t.Fatalf("ImportWithStats: %v", err)
	}
	if !stats.AnyDropped() {
		t.Fatalf("expected import to repair the seeded corruption, got %+v", stats)
	}
	assertFriendsInvariant(t, imported)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Delete sequence panicked on repaired graph: %v", r)
		}
	}()
	if !imported.Delete(highID) {
		t.Fatalf("Delete(%s) returned false", highID)
	}

	imported.mu.RLock()
	survivors := make([]uuid.UUID, 0, len(imported.nodes))
	for id := range imported.nodes {
		survivors = append(survivors, id)
	}
	imported.mu.RUnlock()
	rng := rand.New(rand.NewSource(41))
	rng.Shuffle(len(survivors), func(i, j int) { survivors[i], survivors[j] = survivors[j], survivors[i] })
	for _, id := range survivors[:len(survivors)/4] {
		imported.Delete(id)
	}
	assertFriendsInvariant(t, imported)
}
