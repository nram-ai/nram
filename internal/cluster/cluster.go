// Package cluster provides a small, deterministic cosine-clustering primitive
// shared across the codebase. It is used at two granularities: grouping related
// memories during dream consolidation (internal/dreaming) and grouping a single
// memory's sentence embeddings into facets during multi-vector extraction
// (internal/enrichment). Keeping one implementation means both call sites share
// the same anchor-based, non-transitive semantics.
package cluster

import "github.com/nram-ai/nram/internal/storage/hnsw"

// AnchorClusters groups vectors by cosine similarity using a single-pass,
// anchor-based, non-transitive rule that mirrors the original lexical
// consolidation clusterer: walking the input in order, each not-yet-assigned
// vector becomes an anchor, and every remaining not-yet-assigned vector whose
// cosine to that anchor is >= threshold joins the anchor's cluster. Membership
// is decided against the anchor only, never transitively, so a cluster cannot
// chain-drift across the space.
//
// Returns clusters as slices of original indices, in anchor order, each
// cluster's members in ascending index order. Every input index appears in
// exactly one cluster (a vector matching no other forms a singleton). A
// zero-length input returns nil. Vectors with a zero L2 norm or a dimension
// mismatch against the anchor only ever match themselves, so they fall into
// singletons (CosineSimilarityWithNorms returns 0 for those, and a threshold
// above 0 excludes them).
func AnchorClusters(vectors [][]float32, threshold float64) [][]int {
	if len(vectors) == 0 {
		return nil
	}
	norms := make([]float32, len(vectors))
	for i, v := range vectors {
		norms[i] = hnsw.Norm(v)
	}
	assigned := make([]bool, len(vectors))
	var clusters [][]int
	for i := range vectors {
		if assigned[i] {
			continue
		}
		assigned[i] = true
		members := []int{i}
		for j := i + 1; j < len(vectors); j++ {
			if assigned[j] {
				continue
			}
			if hnsw.CosineSimilarityWithNorms(vectors[i], vectors[j], norms[i], norms[j]) >= threshold {
				members = append(members, j)
				assigned[j] = true
			}
		}
		clusters = append(clusters, members)
	}
	return clusters
}

// Pool returns the element-wise mean (centroid) of the given vectors. Vectors
// whose dimension does not match the first vector are skipped so a stray
// malformed member cannot corrupt the result. Empty input, a zero-dimension
// first vector, or no dimension-matching members all return nil. The result is
// not L2-normalized; cosine comparison is scale-invariant, so callers that only
// use the centroid for cosine search do not need normalization.
func Pool(vectors [][]float32) []float32 {
	if len(vectors) == 0 || len(vectors[0]) == 0 {
		return nil
	}
	dim := len(vectors[0])
	sum := make([]float64, dim)
	count := 0
	for _, v := range vectors {
		if len(v) != dim {
			continue
		}
		for i, x := range v {
			sum[i] += float64(x)
		}
		count++
	}
	if count == 0 {
		return nil
	}
	inv := 1.0 / float64(count)
	out := make([]float32, dim)
	for i, s := range sum {
		out[i] = float32(s * inv)
	}
	return out
}
