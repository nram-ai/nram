package hnsw

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

func randomVector(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

func buildGraph(b *testing.B, n, dim int) *Graph {
	b.Helper()
	g := NewGraph(dim, WithSeed(42))
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < n; i++ {
		_ = g.Add(Node{ID: uuid.New(), Vector: randomVector(rng, dim)})
	}
	return g
}

func BenchmarkAdd_384d(b *testing.B) {
	g := NewGraph(384, WithSeed(42))
	rng := rand.New(rand.NewSource(99))
	nodes := make([]Node, b.N)
	for i := range nodes {
		nodes[i] = Node{ID: uuid.New(), Vector: randomVector(rng, 384)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Add(nodes[i])
	}
}

func BenchmarkAdd_1536d(b *testing.B) {
	g := NewGraph(1536, WithSeed(42))
	rng := rand.New(rand.NewSource(99))
	nodes := make([]Node, b.N)
	for i := range nodes {
		nodes[i] = Node{ID: uuid.New(), Vector: randomVector(rng, 1536)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Add(nodes[i])
	}
}

func BenchmarkSearch_1K_384d(b *testing.B) {
	g := buildGraph(b, 1000, 384)
	rng := rand.New(rand.NewSource(99))
	queries := make([][]float32, b.N)
	for i := range queries {
		queries[i] = randomVector(rng, 384)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = g.Search(queries[i], 10)
	}
}

func BenchmarkSearch_1K_1536d(b *testing.B) {
	g := buildGraph(b, 1000, 1536)
	rng := rand.New(rand.NewSource(99))
	queries := make([][]float32, b.N)
	for i := range queries {
		queries[i] = randomVector(rng, 1536)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = g.Search(queries[i], 10)
	}
}

func BenchmarkSearch_10K_384d(b *testing.B) {
	g := buildGraph(b, 10000, 384)
	rng := rand.New(rand.NewSource(99))
	queries := make([][]float32, b.N)
	for i := range queries {
		queries[i] = randomVector(rng, 384)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = g.Search(queries[i], 10)
	}
}

func BenchmarkSearch_10K_1536d(b *testing.B) {
	g := buildGraph(b, 10000, 1536)
	rng := rand.New(rand.NewSource(99))
	queries := make([][]float32, b.N)
	for i := range queries {
		queries[i] = randomVector(rng, 1536)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = g.Search(queries[i], 10)
	}
}

func BenchmarkCosineSimilarity_384d(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	a := randomVector(rng, 384)
	vec := randomVector(rng, 384)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineSimilarity(a, vec)
	}
}

func BenchmarkCosineSimilarity_1536d(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	a := randomVector(rng, 1536)
	vec := randomVector(rng, 1536)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineSimilarity(a, vec)
	}
}
