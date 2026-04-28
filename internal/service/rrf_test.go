package service

import (
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
)

func mustUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("parse uuid %q: %v", s, err)
	}
	return id
}

func ranking(ids ...uuid.UUID) []storage.MemoryRank {
	out := make([]storage.MemoryRank, len(ids))
	for i, id := range ids {
		out[i] = storage.MemoryRank{ID: id, Rank: float64(len(ids) - i)}
	}
	return out
}

func TestReciprocalRankFusion_EmptyInput(t *testing.T) {
	got := ReciprocalRankFusion(nil, 60, nil)
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(got))
	}

	got = ReciprocalRankFusion([][]storage.MemoryRank{}, 60, []float64{})
	if len(got) != 0 {
		t.Fatalf("expected empty map for zero rankings, got %d entries", len(got))
	}
}

func TestReciprocalRankFusion_SingleRankingWeights(t *testing.T) {
	a := uuid.New()
	b := uuid.New()
	c := uuid.New()

	got := ReciprocalRankFusion(
		[][]storage.MemoryRank{ranking(a, b, c)},
		60,
		[]float64{1.0},
	)

	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	// Head should outscore tail for a single uniform-weight ranking.
	if got[a] <= got[b] || got[b] <= got[c] {
		t.Fatalf("expected a > b > c; got a=%v b=%v c=%v", got[a], got[b], got[c])
	}

	// Exact value at rank 1 with k=60: 1/61.
	want := 1.0 / 61.0
	if math.Abs(got[a]-want) > 1e-12 {
		t.Fatalf("expected score[a]=%.12f, got %.12f", want, got[a])
	}
}

func TestReciprocalRankFusion_DocInOnlyOneRanking(t *testing.T) {
	a := uuid.New()
	b := uuid.New()

	got := ReciprocalRankFusion(
		[][]storage.MemoryRank{
			ranking(a),         // a in list 1 only
			ranking(b),         // b in list 2 only
		},
		60,
		[]float64{0.7, 0.3},
	)

	wantA := 0.7 / 61.0
	wantB := 0.3 / 61.0
	if math.Abs(got[a]-wantA) > 1e-12 {
		t.Fatalf("score[a]=%.12f, want %.12f", got[a], wantA)
	}
	if math.Abs(got[b]-wantB) > 1e-12 {
		t.Fatalf("score[b]=%.12f, want %.12f", got[b], wantB)
	}
}

func TestReciprocalRankFusion_DocInBothAtSamePosition(t *testing.T) {
	a := uuid.New()
	b := uuid.New()

	// a is rank 1 in both lists; b is rank 2 in only list 1.
	got := ReciprocalRankFusion(
		[][]storage.MemoryRank{
			ranking(a, b),
			ranking(a),
		},
		60,
		[]float64{0.7, 0.3},
	)

	// a appears in both at rank 1 → 0.7/61 + 0.3/61 = 1.0/61.
	wantA := 1.0 / 61.0
	if math.Abs(got[a]-wantA) > 1e-12 {
		t.Fatalf("score[a]=%.12f, want %.12f", got[a], wantA)
	}
	// b only in list 1 at rank 2 → 0.7/62.
	wantB := 0.7 / 62.0
	if math.Abs(got[b]-wantB) > 1e-12 {
		t.Fatalf("score[b]=%.12f, want %.12f", got[b], wantB)
	}
	// Doc-in-both must outrank doc-in-one.
	if got[a] <= got[b] {
		t.Fatalf("expected a > b; got a=%v b=%v", got[a], got[b])
	}
}

func TestReciprocalRankFusion_KCoercion(t *testing.T) {
	a := uuid.New()
	got := ReciprocalRankFusion(
		[][]storage.MemoryRank{ranking(a)},
		0, // coerced to 60
		[]float64{1.0},
	)
	want := 1.0 / 61.0
	if math.Abs(got[a]-want) > 1e-12 {
		t.Fatalf("k=0 coercion: score[a]=%.12f, want %.12f", got[a], want)
	}

	got = ReciprocalRankFusion(
		[][]storage.MemoryRank{ranking(a)},
		-5,
		[]float64{1.0},
	)
	if math.Abs(got[a]-want) > 1e-12 {
		t.Fatalf("k=-5 coercion: score[a]=%.12f, want %.12f", got[a], want)
	}
}

func TestReciprocalRankFusion_WeightLengthCoercion(t *testing.T) {
	a := uuid.New()
	got := ReciprocalRankFusion(
		[][]storage.MemoryRank{ranking(a), ranking(a)},
		60,
		[]float64{0.7}, // wrong length → coerced to uniform
	)
	// Uniform 1.0 weights across both rankings, both at rank 1: 1/61 + 1/61.
	want := 2.0 / 61.0
	if math.Abs(got[a]-want) > 1e-12 {
		t.Fatalf("weight coercion: score[a]=%.12f, want %.12f", got[a], want)
	}
}
