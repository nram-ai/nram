package enrichment

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
)

// countingAugmentLLM returns an LLM stub that yields a valid query array and
// increments *calls on every invocation. newIngestionHarness wires no dedicated
// query-augment provider, so the augmentation phase falls back to the fact
// provider; for an already-enriched memory fact extraction is skipped, so a
// call to this stub can only be the augmentation phase.
func countingAugmentLLM(calls *int, mu *sync.Mutex) *mockLLMProvider {
	return &mockLLMProvider{name: "augment-counter", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		mu.Lock()
		*calls++
		mu.Unlock()
		return &provider.CompletionResponse{Content: `["q1","q2"]`, Model: "augment-model"}, nil
	}}
}

// countingEmbedder returns an embedding stub that yields a fixed 3-dim vector
// per input and increments *calls on every batch embed request.
func countingEmbedder(calls *int, mu *sync.Mutex) *mockEmbeddingProvider {
	return &mockEmbeddingProvider{name: "embed-counter", respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
		mu.Lock()
		*calls++
		mu.Unlock()
		out := make([][]float32, len(req.Input))
		for i := range req.Input {
			out[i] = []float32{0.1, 0.2, 0.3}
		}
		return &provider.EmbeddingResponse{Embeddings: out, Model: "embed-model"}, nil
	}}
}

// augmentedMemory returns a memory in the finalized+augmented state the durable
// skip gate keys on (Enriched, augmented_embedding_at stamped, augmented_queries
// present). embeddingDim is passed through so a caller can model the model-switch
// cascade (embedding_dim NULL) by passing nil.
func augmentedMemory(embeddingDim *int) *model.Memory {
	at := time.Now().UTC().Add(-time.Hour)
	m := testMemory()
	m.Enriched = true
	m.EmbeddingDim = embeddingDim
	m.AugmentedEmbeddingAt = &at
	m.AugmentedQueries = []string{"prior one", "prior two"}
	return m
}

// TestReAugment_SkipsWhenAlreadyAugmented pins the durable idempotency gate: a
// re-enqueued memory that already carries a finalized augmented vector
// (Enriched, embedding_dim set, augmented_embedding_at stamped, augmented_queries
// present) must spend NO augment LLM call and NO re-embed, and must leave the
// stored vector and augmented columns untouched. Without the gate the augment
// call and a raw re-embed both fire (the latter overwriting the augmented
// vector with a raw one).
func TestReAugment_SkipsWhenAlreadyAugmented(t *testing.T) {
	var mu sync.Mutex
	var augmentCalls, embedCalls int

	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		countingAugmentLLM(&augmentCalls, &mu), // fact slot == augment fallback
		minimalEntityLLM(),
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		countingEmbedder(&embedCalls, &mu),
	)

	d := 3
	mem := augmentedMemory(&d)
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if augmentCalls != 0 {
		t.Errorf("expected 0 augment LLM calls for already-augmented memory, got %d", augmentCalls)
	}
	if embedCalls != 0 {
		t.Errorf("expected 0 re-embed calls (stored vector reused), got %d", embedCalls)
	}
	if len(h.vectors.vectors) != 0 {
		t.Errorf("expected 0 vector upserts (existing vector preserved), got %d", len(h.vectors.vectors))
	}
	if got := h.queue.queryAugmentSkips[job.ID]; got != model.QueryAugmentSkipAlreadyDone {
		t.Errorf("query_augment_skip_reason = %q, want %q", got, model.QueryAugmentSkipAlreadyDone)
	}
	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected 1 MarkEnriched call, got %d", len(h.updater.enrichedMarks))
	}
	mark := h.updater.enrichedMarks[0]
	if mark.augmentedQueries != nil || mark.augmentedEmbeddingAt != nil {
		t.Errorf("MarkEnriched must leave augmented columns untouched (nil args); got queries=%v at=%v",
			mark.augmentedQueries, mark.augmentedEmbeddingAt)
	}
}

// TestReAugment_CascadeReAugmentsWhenEmbeddingCleared pins the load-bearing
// embedding_dim clause. The model-switch cascade (ClearAllEmbeddingDims) NULLs
// embedding_dim while leaving augmented_embedding_at set. The gate MUST NOT fire
// there: augmentation must re-run and the parent must be re-embedded, or the row
// would end with a raw vector while augmented_embedding_at falsely claims it is
// augmented.
func TestReAugment_CascadeReAugmentsWhenEmbeddingCleared(t *testing.T) {
	var mu sync.Mutex
	var augmentCalls, embedCalls int

	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		countingAugmentLLM(&augmentCalls, &mu),
		minimalEntityLLM(),
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		countingEmbedder(&embedCalls, &mu),
	)

	mem := augmentedMemory(nil) // cascade cleared embedding_dim
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if augmentCalls != 1 {
		t.Errorf("cascade (embedding_dim NULL) MUST re-augment; got %d augment calls", augmentCalls)
	}
	if embedCalls != 1 {
		t.Errorf("cascade MUST re-embed the augmented content; got %d embed calls", embedCalls)
	}
	if len(h.vectors.vectors) != 1 || h.vectors.vectors[0].ID != mem.ID {
		t.Errorf("expected the parent vector re-upserted; got %+v", h.vectors.vectors)
	}
	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected 1 MarkEnriched call, got %d", len(h.updater.enrichedMarks))
	}
	if h.updater.enrichedMarks[0].augmentedEmbeddingAt == nil {
		t.Errorf("re-augment MUST re-stamp augmented_embedding_at; got nil")
	}
}

// TestReAugment_EntityExtractionStillRunsOnConsolidationBackfill guards the
// consolidation-entity backfill: it enqueues a plain job on an
// enriched+embedded+augmented consolidation dream specifically to run entity
// extraction. The augment/embed reuse must not suppress that: entity extraction
// still runs (entity_extracted_at stamped) while the parent vector is reused.
func TestReAugment_EntityExtractionStillRunsOnConsolidationBackfill(t *testing.T) {
	var mu sync.Mutex
	var augmentCalls, embedCalls, entityCalls int

	entityLLM := &mockLLMProvider{name: "entity", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		mu.Lock()
		entityCalls++
		mu.Unlock()
		return &provider.CompletionResponse{Content: `{"entities":[],"relationships":[]}`, Model: "entity-model"}, nil
	}}

	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		countingAugmentLLM(&augmentCalls, &mu),
		entityLLM,
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		countingEmbedder(&embedCalls, &mu),
	)

	d := 3
	mem := augmentedMemory(&d)
	// Consolidation dream: Origin=dream + source_memory_ids metadata makes
	// IsConsolidationDream() true, the one dream type eligible for entity
	// extraction on re-enqueue.
	mem.Origin = model.OriginDream
	mem.Metadata = []byte(`{"source_memory_ids":["11111111-1111-1111-1111-111111111111"]}`)
	h.reader.byID[mem.ID] = mem

	job := testJob(mem.ID, mem.NamespaceID)
	job.StepsCompleted = []byte(`["fact_extraction"]`) // shape the backfill enqueues

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if augmentCalls != 0 {
		t.Errorf("augment must be skipped for the already-augmented dream, got %d", augmentCalls)
	}
	if entityCalls != 1 {
		t.Errorf("entity extraction MUST still run for the consolidation backfill, got %d calls", entityCalls)
	}
	if embedCalls != 0 {
		t.Errorf("no entities and parent reused: expected 0 embed calls, got %d", embedCalls)
	}
	if len(h.vectors.vectors) != 0 {
		t.Errorf("parent reused and no entities: expected 0 vector upserts, got %d", len(h.vectors.vectors))
	}
	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected 1 MarkEnriched call, got %d", len(h.updater.enrichedMarks))
	}
	mark := h.updater.enrichedMarks[0]
	if mark.entityExtractedAt == nil {
		t.Errorf("entity_extracted_at MUST be stamped when entity extraction ran; got nil")
	}
	if mark.augmentedEmbeddingAt != nil {
		t.Errorf("augmented columns must be left untouched on reuse; got at=%v", mark.augmentedEmbeddingAt)
	}
}

// TestReAugment_MixedBatchOffsetsStayCorrect drives runEmbedBatch directly with
// two pendings in one batch: A reuses its stored vector (parent NOT in the
// shared inputs slice) and B embeds fresh. It pins the offset arithmetic in a
// mixed batch — a reuse pending must not consume an inputs slot, so B's
// embedStart must still index B's own vector and only B's parent may be
// upserted. If the reuse guard were dropped, A would read B's embedding at
// offset 0 and upsert it under A's ID (a cross-contaminated second upsert).
func TestReAugment_MixedBatchOffsetsStayCorrect(t *testing.T) {
	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		constEmbedder(),
	)

	d := 3
	memA := augmentedMemory(&d)
	// A already-augmented + no new augmented blob → reuseStoredParentVector.
	pA := &pendingJob{job: testJob(memA.ID, memA.NamespaceID), mem: memA, workerID: "w-0"}

	memB := testMemory()
	// B carries fresh augmented content, so it must embed (reuse guard false).
	pB := &pendingJob{
		job:              testJob(memB.ID, memB.NamespaceID),
		mem:              memB,
		workerID:         "w-0",
		augmentedContent: "augmented blob for B",
		augmentedQueries: []string{"b one", "b two"},
	}

	if !pA.reuseStoredParentVector() {
		t.Fatalf("precondition: pA must qualify for vector reuse")
	}
	if pB.reuseStoredParentVector() {
		t.Fatalf("precondition: pB has augmented content and must NOT reuse")
	}

	if err := h.pool.runEmbedBatch(context.Background(), []*pendingJob{pA, pB}); err != nil {
		t.Fatalf("runEmbedBatch: %v", err)
	}

	if len(h.vectors.vectors) != 1 {
		t.Fatalf("expected exactly 1 vector upsert (B only; A reused), got %d: %+v",
			len(h.vectors.vectors), h.vectors.vectors)
	}
	if h.vectors.vectors[0].ID != memB.ID {
		t.Errorf("the single upsert must be B's parent vector; got ID %v want %v",
			h.vectors.vectors[0].ID, memB.ID)
	}
	if got := h.vectors.vectors[0].Dimension; got != 3 {
		t.Errorf("B's embedding must be a valid 3-dim vector; got dim %d", got)
	}
	if pB.mem.EmbeddingDim == nil || *pB.mem.EmbeddingDim != 3 {
		t.Errorf("B must have embedding_dim stamped to 3; got %v", pB.mem.EmbeddingDim)
	}
	if pB.embedUsedAugmented != true {
		t.Errorf("B embedded its augmented content, so embedUsedAugmented must be true")
	}
}
