package hnsw

import "math"

// Norm computes the L2 norm of a vector.
func Norm(v []float32) float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	return float32(math.Sqrt(sum))
}

// CosineSimilarity computes the cosine similarity between two vectors.
// Returns 1.0 for identical directions, 0.0 for orthogonal, -1.0 for opposite.
// Returns 0.0 if either vector is zero or dimensions mismatch.
func CosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0.0
	}
	na := Norm(a)
	nb := Norm(b)
	return CosineSimilarityWithNorms(a, b, na, nb)
}

// CosineSimilarityWithNorms computes cosine similarity using precomputed norms.
// This is the fast path when norms are already cached.
// Returns 0.0 if either norm is zero or dimensions mismatch.
func CosineSimilarityWithNorms(a, b []float32, normA, normB float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0.0
	}
	if normA == 0 || normB == 0 {
		return 0.0
	}
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	sim := dot / (float64(normA) * float64(normB))
	// Clamp to [-1, 1] to handle floating point imprecision.
	if sim > 1.0 {
		sim = 1.0
	} else if sim < -1.0 {
		sim = -1.0
	}
	return sim
}
