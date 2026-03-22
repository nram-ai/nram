package hnsw

import "math"

// Norm computes the L2 norm of a vector.
func Norm(v []float32) float32 {
	var sum float32
	n := len(v)
	i := 0
	for ; i <= n-8; i += 8 {
		sum += v[i]*v[i] + v[i+1]*v[i+1] + v[i+2]*v[i+2] + v[i+3]*v[i+3] +
			v[i+4]*v[i+4] + v[i+5]*v[i+5] + v[i+6]*v[i+6] + v[i+7]*v[i+7]
	}
	for ; i < n; i++ {
		sum += v[i] * v[i]
	}
	return float32(math.Sqrt(float64(sum)))
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
	var dot float32
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		dot += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3] +
			a[i+4]*b[i+4] + a[i+5]*b[i+5] + a[i+6]*b[i+6] + a[i+7]*b[i+7]
	}
	for ; i < n; i++ {
		dot += a[i] * b[i]
	}
	sim := float64(dot) / (float64(normA) * float64(normB))
	// Clamp to [-1, 1] to handle floating point imprecision.
	if sim > 1.0 {
		sim = 1.0
	} else if sim < -1.0 {
		sim = -1.0
	}
	return sim
}
