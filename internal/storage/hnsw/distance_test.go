package hnsw

import (
	"math"
	"testing"
)

func TestCosineSimilarity(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	sim := CosineSimilarity(a, b)
	// Expected: (4+10+18) / (sqrt(14) * sqrt(77)) = 32 / sqrt(1078) ≈ 0.9746
	expected := 32.0 / math.Sqrt(1078.0)
	if math.Abs(sim-expected) > 1e-6 {
		t.Errorf("CosineSimilarity(%v, %v) = %f, want %f", a, b, sim, expected)
	}
}

func TestCosineSimilarityIdentical(t *testing.T) {
	v := []float32{3, 4, 0}
	sim := CosineSimilarity(v, v)
	if math.Abs(sim-1.0) > 1e-6 {
		t.Errorf("CosineSimilarity of identical vectors = %f, want 1.0", sim)
	}
}

func TestCosineSimilarityOrthogonal(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	sim := CosineSimilarity(a, b)
	if math.Abs(sim) > 1e-6 {
		t.Errorf("CosineSimilarity of orthogonal vectors = %f, want 0.0", sim)
	}
}

func TestCosineSimilarityOpposite(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{-1, 0, 0}
	sim := CosineSimilarity(a, b)
	if math.Abs(sim-(-1.0)) > 1e-6 {
		t.Errorf("CosineSimilarity of opposite vectors = %f, want -1.0", sim)
	}
}

func TestCosineSimilarityWithNorms(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	normA := Norm(a)
	normB := Norm(b)

	sim1 := CosineSimilarity(a, b)
	sim2 := CosineSimilarityWithNorms(a, b, normA, normB)

	if math.Abs(sim1-sim2) > 1e-10 {
		t.Errorf("CosineSimilarityWithNorms = %f, CosineSimilarity = %f, should match", sim2, sim1)
	}
}

func TestZeroVector(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{1, 2, 3}

	sim := CosineSimilarity(a, b)
	if sim != 0.0 {
		t.Errorf("CosineSimilarity with zero vector = %f, want 0.0", sim)
	}

	sim2 := CosineSimilarity(a, a)
	if sim2 != 0.0 {
		t.Errorf("CosineSimilarity of two zero vectors = %f, want 0.0", sim2)
	}
}

func TestCosineSimilarityDimensionMismatch(t *testing.T) {
	a := []float32{1, 2}
	b := []float32{1, 2, 3}
	sim := CosineSimilarity(a, b)
	if sim != 0.0 {
		t.Errorf("CosineSimilarity with dimension mismatch = %f, want 0.0", sim)
	}
}

func TestCosineSimilarityEmpty(t *testing.T) {
	sim := CosineSimilarity(nil, nil)
	if sim != 0.0 {
		t.Errorf("CosineSimilarity of nil vectors = %f, want 0.0", sim)
	}
	sim = CosineSimilarity([]float32{}, []float32{})
	if sim != 0.0 {
		t.Errorf("CosineSimilarity of empty vectors = %f, want 0.0", sim)
	}
}

func TestNorm(t *testing.T) {
	v := []float32{3, 4}
	n := Norm(v)
	if math.Abs(float64(n)-5.0) > 1e-6 {
		t.Errorf("Norm(%v) = %f, want 5.0", v, n)
	}
}
