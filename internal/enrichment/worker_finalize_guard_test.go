package enrichment

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// When an embedder is configured but no vector was produced (EmbeddingDim nil)
// for an embeddable memory, finalizeJob must fail the job for bounded retry and
// must NOT mark it enriched. Otherwise the row strands as
// enriched=true/embedding_dim=NULL/no-vector: invisible to the embedding
// backfill and a perpetual multi-vector facet candidate.
func TestFinalizeJob_FailsWhenEmbedExpectedButNoVector(t *testing.T) {
	queue := newMockQueueClaimer()
	updater := &mockMemoryUpdater{}
	emb := &fakeFacetEmbedder{dim: 8} // non-nil embedder; never actually called here
	wp := &WorkerPool{
		queue:         queue,
		memUpdater:    updater,
		embedProvider: func() provider.EmbeddingProvider { return emb },
	}
	job := &model.EnrichmentJob{ID: uuid.New()}
	p := &pendingJob{
		job:      job,
		workerID: "w1",
		mem:      &model.Memory{ID: uuid.New(), NamespaceID: uuid.New(), Content: "has content but no vector"},
	}

	err := wp.finalizeJob(context.Background(), p)
	if err == nil {
		t.Fatal("expected finalizeJob to return an error when embed produced no vector")
	}
	if _, failed := queue.failed[job.ID]; !failed {
		t.Errorf("expected job %s to be failed for retry", job.ID)
	}
	if len(updater.enrichedMarks) != 0 {
		t.Errorf("memory must NOT be marked enriched without a vector; got %d marks", len(updater.enrichedMarks))
	}
}

// With no embedder configured, enriched-without-vector is the intended degraded
// mode: finalizeJob proceeds to MarkEnriched and completes the job rather than
// failing it forever.
func TestFinalizeJob_NoEmbedderMarksEnrichedWithoutVector(t *testing.T) {
	queue := newMockQueueClaimer()
	updater := &mockMemoryUpdater{}
	wp := &WorkerPool{
		queue:         queue,
		memUpdater:    updater,
		embedProvider: func() provider.EmbeddingProvider { return nil }, // embedding disabled
	}
	job := &model.EnrichmentJob{ID: uuid.New()}
	p := &pendingJob{
		job:      job,
		workerID: "w1",
		mem:      &model.Memory{ID: uuid.New(), NamespaceID: uuid.New(), Content: "content, embedding disabled"},
	}

	if err := wp.finalizeJob(context.Background(), p); err != nil {
		t.Fatalf("finalizeJob: %v", err)
	}
	if _, failed := queue.failed[job.ID]; failed {
		t.Errorf("job must not be failed when no embedder is configured")
	}
	if len(updater.enrichedMarks) != 1 {
		t.Fatalf("expected MarkEnriched to run in degraded mode; got %d marks", len(updater.enrichedMarks))
	}
	if updater.enrichedMarks[0].embeddingDim != nil {
		t.Errorf("embeddingDim should be nil in degraded mode")
	}
	if len(queue.completed) != 1 {
		t.Errorf("expected the job to be completed once; got %d", len(queue.completed))
	}
}
