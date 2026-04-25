package dreaming

import (
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// TestShouldMerge_VectorSimilarityFallback exercises the A3 dedup change: when
// canonical text matching cannot tie two entities together, the vector
// similarity branch should still merge them if their stored vectors agree
// at or above entityMergeCosineThreshold.
func TestShouldMerge_VectorSimilarityFallback(t *testing.T) {
	dim := 4
	idA := uuid.New()
	idB := uuid.New()
	dimP := dim

	entA := &model.Entity{
		ID:           idA,
		NamespaceID:  uuid.New(),
		Name:         "Acme Corporation",
		Canonical:    "acme corporation",
		EntityType:   "organization",
		EmbeddingDim: &dimP,
	}
	entB := &model.Entity{
		ID:           idB,
		NamespaceID:  entA.NamespaceID,
		Name:         "Acme Inc",
		Canonical:    "acme inc",
		EntityType:   "organization",
		EmbeddingDim: &dimP,
	}

	// Identical vectors → cosine 1.0, well above the 0.92 threshold.
	vec := []float32{1, 0, 0, 0}
	vectorsByID := map[uuid.UUID][]float32{
		idA: vec,
		idB: vec,
	}

	p := &EntityDedupPhase{}
	if !p.shouldMerge(entA, entB, vectorsByID, normsByID(vectorsByID)) {
		t.Fatal("expected vector-similarity fallback to merge entities with cosine 1.0")
	}
}

// TestShouldMerge_VectorSimilarityBelowThreshold guards against false-positive
// merges: orthogonal vectors must not satisfy the fallback even when text
// matching also failed.
func TestShouldMerge_VectorSimilarityBelowThreshold(t *testing.T) {
	dim := 4
	idA := uuid.New()
	idB := uuid.New()
	dimP := dim

	entA := &model.Entity{
		ID: idA, NamespaceID: uuid.New(), Name: "Apple", Canonical: "apple",
		EntityType: "organization", EmbeddingDim: &dimP,
	}
	entB := &model.Entity{
		ID: idB, NamespaceID: entA.NamespaceID, Name: "Microsoft", Canonical: "microsoft",
		EntityType: "organization", EmbeddingDim: &dimP,
	}

	vectorsByID := map[uuid.UUID][]float32{
		idA: {1, 0, 0, 0},
		idB: {0, 1, 0, 0}, // orthogonal → cosine 0
	}

	p := &EntityDedupPhase{}
	if p.shouldMerge(entA, entB, vectorsByID, normsByID(vectorsByID)) {
		t.Fatal("expected orthogonal vectors to fall below entityMergeCosineThreshold")
	}
}

// TestShouldMerge_DimMismatchReturnsFalse — a deployment in the middle of an
// embed-provider switch may have entities at different dims. The fallback
// must refuse to compare across dims rather than panic on length-mismatched
// dot products.
func TestShouldMerge_DimMismatchReturnsFalse(t *testing.T) {
	idA := uuid.New()
	idB := uuid.New()
	dimA := 4
	dimB := 8

	entA := &model.Entity{
		ID: idA, Canonical: "alpha", EntityType: "x", EmbeddingDim: &dimA,
	}
	entB := &model.Entity{
		ID: idB, Canonical: "beta", EntityType: "x", EmbeddingDim: &dimB,
	}

	vectorsByID := map[uuid.UUID][]float32{
		idA: {1, 0, 0, 0},
		idB: {1, 0, 0, 0, 0, 0, 0, 0},
	}

	p := &EntityDedupPhase{}
	if p.shouldMerge(entA, entB, vectorsByID, normsByID(vectorsByID)) {
		t.Fatal("expected dim mismatch to short-circuit before cosine comparison")
	}
}

// TestShouldMerge_TextMatchStillFiresFirst sanity-checks that the cosine
// fallback didn't break the cheap canonical-equality path.
func TestShouldMerge_TextMatchStillFiresFirst(t *testing.T) {
	entA := &model.Entity{Canonical: "react", EntityType: "library"}
	entB := &model.Entity{Canonical: "react", EntityType: "library"}

	p := &EntityDedupPhase{}
	if !p.shouldMerge(entA, entB, nil, nil) {
		t.Fatal("expected canonical-equality merge even with nil vectorsByID")
	}
}

func normsByID(vecs map[uuid.UUID][]float32) map[uuid.UUID]float32 {
	out := make(map[uuid.UUID]float32, len(vecs))
	for k, v := range vecs {
		out[k] = hnsw.Norm(v)
	}
	return out
}
