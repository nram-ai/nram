package dreaming

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// noopAliasWriter satisfies EntityAliasWriter for merge tests.
type noopAliasWriter struct{}

func (noopAliasWriter) Create(_ context.Context, _ *model.EntityAlias) error { return nil }

// TestMergeEntities_DeletesAbsorbedCandidate verifies the merge deletes the
// absorbed candidate inline (via DeleteByIDs) rather than leaving it as a
// zero-edge orphan, and folds its mention count into the primary.
func TestMergeEntities_DeletesAbsorbedCandidate(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()

	w := &recordingEntityWriter{}
	p := &EntityDedupPhase{
		entityWriter:  w,
		aliases:       noopAliasWriter{},
		relationships: &fakeRelationshipReader{},
		relWriter:     noopRelWriter{},
	}
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.UUID{})

	primary := &model.Entity{ID: uuid.New(), NamespaceID: ns, Name: "Primary", MentionCount: 3}
	candidate := &model.Entity{ID: uuid.New(), NamespaceID: ns, Name: "Candidate", MentionCount: 2}

	if len(w.deleted) != 0 {
		t.Fatalf("precondition: no deletes expected before merge, got %v", w.deleted)
	}

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	if err := p.mergeEntities(ctx, cycle, primary, candidate, logger); err != nil {
		t.Fatalf("mergeEntities: %v", err)
	}

	if len(w.deleted) != 1 || w.deleted[0] != candidate.ID {
		t.Fatalf("expected absorbed candidate %s deleted inline, got %v", candidate.ID, w.deleted)
	}
	if primary.MentionCount != 5 {
		t.Fatalf("primary mention count = %d, want 5 (3+2)", primary.MentionCount)
	}
}

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
	if !p.shouldMerge(entA, entB, vectorsByID, normsByID(vectorsByID), 0.92) {
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
	if p.shouldMerge(entA, entB, vectorsByID, normsByID(vectorsByID), 0.92) {
		t.Fatal("expected orthogonal vectors to fall below entityMergeCosineThreshold")
	}
}

// TestShouldMerge_DimMismatchReturnsFalse: a deployment in the middle of an
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
	if p.shouldMerge(entA, entB, vectorsByID, normsByID(vectorsByID), 0.92) {
		t.Fatal("expected dim mismatch to short-circuit before cosine comparison")
	}
}

// TestShouldMerge_TextMatchStillFiresFirst sanity-checks that the cosine
// fallback didn't break the cheap canonical-equality path.
func TestShouldMerge_TextMatchStillFiresFirst(t *testing.T) {
	entA := &model.Entity{Canonical: "react", EntityType: "library"}
	entB := &model.Entity{Canonical: "react", EntityType: "library"}

	p := &EntityDedupPhase{}
	if !p.shouldMerge(entA, entB, nil, nil, 0.92) {
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
