package cluster

import (
	"reflect"
	"testing"
)

func TestAnchorClusters_Empty(t *testing.T) {
	if got := AnchorClusters(nil, 0.5); got != nil {
		t.Fatalf("expected nil for empty input, got %v", got)
	}
}

func TestAnchorClusters_GroupsSimilarNonTransitively(t *testing.T) {
	// Two clear groups along orthogonal axes, given in interleaved order so the
	// test also proves grouping is by cosine, not adjacency.
	vecs := [][]float32{
		{1, 0},       // 0 -> group A
		{0, 1},       // 1 -> group B
		{0.99, 0.01}, // 2 -> group A (near vec 0)
		{0.02, 0.98}, // 3 -> group B (near vec 1)
	}
	got := AnchorClusters(vecs, 0.9)
	want := [][]int{{0, 2}, {1, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clusters = %v, want %v", got, want)
	}
}

func TestAnchorClusters_HighThresholdSingletons(t *testing.T) {
	vecs := [][]float32{{1, 0}, {0, 1}, {0.7, 0.7}}
	got := AnchorClusters(vecs, 0.999)
	want := [][]int{{0}, {1}, {2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clusters = %v, want %v", got, want)
	}
}

func TestAnchorClusters_NonTransitive(t *testing.T) {
	// b is close to both a and c, but a and c are far apart. With a as anchor,
	// only b joins (cosine a~b high); c starts its own cluster. This proves the
	// rule does not chain a->b->c.
	a := []float32{1, 0}
	b := []float32{0.8, 0.6}
	c := []float32{0, 1}
	got := AnchorClusters([][]float32{a, b, c}, 0.75)
	// a~b cosine = 0.8 (>=0.75, joins); a~c = 0 (<0.75). c is its own cluster.
	want := [][]int{{0, 1}, {2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clusters = %v, want %v", got, want)
	}
}

func TestAnchorClusters_ZeroNormAndMismatchAreSingletons(t *testing.T) {
	vecs := [][]float32{
		{1, 0},    // 0
		{0, 0},    // 1 zero norm -> only matches itself
		{1, 0, 0}, // 2 dimension mismatch vs anchor -> singleton
	}
	got := AnchorClusters(vecs, 0.5)
	want := [][]int{{0}, {1}, {2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clusters = %v, want %v", got, want)
	}
}

func TestPool_Mean(t *testing.T) {
	got := Pool([][]float32{{2, 4}, {4, 8}})
	want := []float32{3, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pool = %v, want %v", got, want)
	}
}

func TestPool_SkipsDimensionMismatch(t *testing.T) {
	// The 3-dim member is skipped; mean is over the two 2-dim members.
	got := Pool([][]float32{{2, 4}, {1, 2, 3}, {4, 8}})
	want := []float32{3, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pool = %v, want %v", got, want)
	}
}

func TestPool_EmptyAndDegenerate(t *testing.T) {
	if got := Pool(nil); got != nil {
		t.Fatalf("expected nil for empty input, got %v", got)
	}
	if got := Pool([][]float32{{}}); got != nil {
		t.Fatalf("expected nil for zero-dim first vector, got %v", got)
	}
	// First vector sets dim=2 but every subsequent member mismatches and the
	// first is counted, so this still pools the single valid member.
	if got := Pool([][]float32{{1, 1}, {9, 9, 9}}); !reflect.DeepEqual(got, []float32{1, 1}) {
		t.Fatalf("pool = %v, want [1 1]", got)
	}
}

func TestPool_NoMatchingMembers(t *testing.T) {
	// First vector is zero-dim-checked separately; here the first sets dim and is
	// counted. To hit the count==0 path, the first must be non-empty but all
	// (including itself) skipped, which cannot happen since the first always
	// matches its own dim. So count==0 is only reachable via an all-empty slice,
	// covered above. This guards the inv computation stays correct for one member.
	if got := Pool([][]float32{{5, 5}}); !reflect.DeepEqual(got, []float32{5, 5}) {
		t.Fatalf("pool = %v, want [5 5]", got)
	}
}
