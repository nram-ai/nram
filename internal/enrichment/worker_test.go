package enrichment

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

type mockMemoryReader struct {
	mu      sync.Mutex
	byID    map[uuid.UUID]*model.Memory
	err     error
}

func newMockMemoryReader() *mockMemoryReader {
	return &mockMemoryReader{byID: make(map[uuid.UUID]*model.Memory)}
}

func (m *mockMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	mem, ok := m.byID[id]
	if !ok {
		return nil, fmt.Errorf("memory %s not found", id)
	}
	cp := *mem
	return &cp, nil
}

type mockMemoryUpdater struct {
	mu          sync.Mutex
	updated     []*model.Memory
	dimUpdates  []dimUpdate
	err         error
}

type dimUpdate struct {
	id  uuid.UUID
	dim int
}

func (m *mockMemoryUpdater) Update(_ context.Context, mem *model.Memory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	cp := *mem
	m.updated = append(m.updated, &cp)
	return nil
}

func (m *mockMemoryUpdater) UpdateEmbeddingDim(_ context.Context, id uuid.UUID, dim int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.dimUpdates = append(m.dimUpdates, dimUpdate{id: id, dim: dim})
	return nil
}

type mockMemoryCreator struct {
	mu      sync.Mutex
	created []*model.Memory
	err     error
}

func (m *mockMemoryCreator) Create(_ context.Context, mem *model.Memory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	cp := *mem
	m.created = append(m.created, &cp)
	return nil
}

type mockQueueClaimer struct {
	mu        sync.Mutex
	jobs      []*model.EnrichmentJob
	completed []uuid.UUID
	failed    map[uuid.UUID]string
	claimErr  error
}

func newMockQueueClaimer() *mockQueueClaimer {
	return &mockQueueClaimer{failed: make(map[uuid.UUID]string)}
}

func (m *mockQueueClaimer) ClaimNext(_ context.Context, _ string) (*model.EnrichmentJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimErr != nil {
		return nil, m.claimErr
	}
	if len(m.jobs) == 0 {
		return nil, nil
	}
	j := m.jobs[0]
	m.jobs = m.jobs[1:]
	return j, nil
}

func (m *mockQueueClaimer) ClaimNextBatch(_ context.Context, _ string, max int) ([]*model.EnrichmentJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimErr != nil {
		return nil, m.claimErr
	}
	if len(m.jobs) == 0 {
		return nil, sql.ErrNoRows
	}
	n := max
	if n > len(m.jobs) {
		n = len(m.jobs)
	}
	batch := make([]*model.EnrichmentJob, n)
	copy(batch, m.jobs[:n])
	m.jobs = m.jobs[n:]
	return batch, nil
}

func (m *mockQueueClaimer) Complete(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed = append(m.completed, id)
	return nil
}

func (m *mockQueueClaimer) Fail(_ context.Context, id uuid.UUID, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed[id] = errMsg
	return nil
}

type mockEntityUpserter struct {
	mu       sync.Mutex
	upserted []*model.Entity
	err      error
}

func (m *mockEntityUpserter) Upsert(_ context.Context, entity *model.Entity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	cp := *entity
	m.upserted = append(m.upserted, &cp)
	return nil
}

func (m *mockEntityUpserter) FindBySimilarity(_ context.Context, _ uuid.UUID, _ string, _ string, _ int) ([]model.Entity, error) {
	return nil, nil
}

func (m *mockEntityUpserter) UpdateEmbeddingDimBatch(_ context.Context, _ []uuid.UUID, _ int) error {
	return nil
}

type mockRelationshipCreator struct {
	mu      sync.Mutex
	created []*model.Relationship
	err     error
}

func (m *mockRelationshipCreator) Create(_ context.Context, rel *model.Relationship) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	cp := *rel
	m.created = append(m.created, &cp)
	return nil
}

func (m *mockRelationshipCreator) FindActiveByTriple(_ context.Context, _, _, _ uuid.UUID, _ string) (*model.Relationship, error) {
	return nil, nil
}

func (m *mockRelationshipCreator) UpdateWeight(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ float64) error {
	return nil
}

type mockLineageCreator struct {
	mu      sync.Mutex
	created []*model.MemoryLineage
	err     error
}

func (m *mockLineageCreator) Create(_ context.Context, lin *model.MemoryLineage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	cp := *lin
	m.created = append(m.created, &cp)
	return nil
}

type mockTokenRecorder struct {
	mu      sync.Mutex
	records []*model.TokenUsage
	err     error
}

func (m *mockTokenRecorder) Record(_ context.Context, usage *model.TokenUsage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	cp := *usage
	m.records = append(m.records, &cp)
	return nil
}

type mockVectorWriter struct {
	mu      sync.Mutex
	vectors []vectorEntry
	err     error
}

type vectorEntry struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
	Embedding   []float32
	Dimension   int
}

func (m *mockVectorWriter) Upsert(_ context.Context, _ storage.VectorKind, id, nsID uuid.UUID, emb []float32, dim int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.vectors = append(m.vectors, vectorEntry{ID: id, NamespaceID: nsID, Embedding: emb, Dimension: dim})
	return nil
}

func (m *mockVectorWriter) UpsertBatch(_ context.Context, items []storage.VectorUpsertItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	for _, it := range items {
		m.vectors = append(m.vectors, vectorEntry{ID: it.ID, NamespaceID: it.NamespaceID, Embedding: it.Embedding, Dimension: it.Dimension})
	}
	return nil
}

// mockLLMProvider simulates an LLM provider.
type mockLLMProvider struct {
	name    string
	respond func(req *provider.CompletionRequest) (*provider.CompletionResponse, error)
}

func (m *mockLLMProvider) Complete(_ context.Context, req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	return m.respond(req)
}
func (m *mockLLMProvider) Name() string     { return m.name }
func (m *mockLLMProvider) Models() []string { return []string{"test-model"} }

// mockEmbeddingProvider simulates an embedding provider.
type mockEmbeddingProvider struct {
	name    string
	respond func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error)
}

func (m *mockEmbeddingProvider) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return m.respond(req)
}
func (m *mockEmbeddingProvider) Name() string    { return m.name }
func (m *mockEmbeddingProvider) Dimensions() []int { return []int{3} }

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func testMemory() *model.Memory {
	src := "test-source"
	return &model.Memory{
		ID:          uuid.New(),
		NamespaceID: uuid.New(),
		Content:     "Alice works at Acme Corp. She is 30 years old.",
		Source:      &src,
		Tags:        []string{"parent-tag", "important"},
		Confidence:  1.0,
		Importance:  0.5,
		Enriched:    false,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
}

func testJob(memID, nsID uuid.UUID) *model.EnrichmentJob {
	return &model.EnrichmentJob{
		ID:          uuid.New(),
		MemoryID:    memID,
		NamespaceID: nsID,
		Status:      "pending",
		Priority:    0,
		Attempts:    0,
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
}

func factJSON() string {
	facts := []extractedFact{
		{Content: "Alice works at Acme Corp", Confidence: 0.95, Tags: []string{"employment"}},
		{Content: "Alice is 30 years old", Confidence: 0.9, Tags: []string{"age"}},
	}
	b, _ := json.Marshal(facts)
	return string(b)
}

func entityJSON() string {
	result := entityExtractionResult{
		Entities: []extractedEntity{
			{Name: "Alice", Type: "person", Properties: map[string]interface{}{"age": 30}},
			{Name: "Acme Corp", Type: "organization", Properties: nil},
		},
		Relationships: []extractedRelationship{
			{Source: "Alice", Target: "Acme Corp", Relation: "works_at", Weight: 0.95},
		},
	}
	b, _ := json.Marshal(result)
	return string(b)
}

type testHarness struct {
	pool   *WorkerPool
	reader *mockMemoryReader
	updater *mockMemoryUpdater
	creator *mockMemoryCreator
	queue  *mockQueueClaimer
	entities *mockEntityUpserter
	rels   *mockRelationshipCreator
	lineage *mockLineageCreator
	tokens *mockTokenRecorder
	vectors *mockVectorWriter
}

func newTestHarness(
	factLLM provider.LLMProvider,
	entityLLM provider.LLMProvider,
	embedProv provider.EmbeddingProvider,
) *testHarness {
	h := &testHarness{
		reader:   newMockMemoryReader(),
		updater:  &mockMemoryUpdater{},
		creator:  &mockMemoryCreator{},
		queue:    newMockQueueClaimer(),
		entities: &mockEntityUpserter{},
		rels:     &mockRelationshipCreator{},
		lineage:  &mockLineageCreator{},
		tokens:   &mockTokenRecorder{},
		vectors:  &mockVectorWriter{},
	}

	h.pool = NewWorkerPool(
		WorkerConfig{Workers: 1, PollInterval: 10 * time.Millisecond},
		h.reader, h.updater, h.creator, h.queue,
		h.entities, h.rels, h.lineage, h.tokens, nil, h.vectors,
		func() provider.LLMProvider { return factLLM },
		func() provider.LLMProvider { return entityLLM },
		func() provider.EmbeddingProvider { return embedProv },
	)
	return h
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestProcessJob_FullPipeline(t *testing.T) {
	factLLM := &mockLLMProvider{name: "test-fact", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: factJSON(),
			Model:   "fact-model",
			Usage:   provider.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
		}, nil
	}}
	entityLLM := &mockLLMProvider{name: "test-entity", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: entityJSON(),
			Model:   "entity-model",
			Usage:   provider.TokenUsage{PromptTokens: 80, CompletionTokens: 60, TotalTokens: 140},
		}, nil
	}}
	embedProv := &mockEmbeddingProvider{name: "test-embed", respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
		embs := make([][]float32, len(req.Input))
		for i := range req.Input {
			embs[i] = []float32{0.1, 0.2, 0.3}
		}
		return &provider.EmbeddingResponse{
			Embeddings: embs,
			Model:      "embed-model",
			Usage:      provider.TokenUsage{PromptTokens: 20, TotalTokens: 20},
		}, nil
	}}

	h := newTestHarness(factLLM, entityLLM, embedProv)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	err := h.pool.processJob(context.Background(), "w-0", job)
	if err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	// Job should be completed.
	if len(h.queue.completed) != 1 || h.queue.completed[0] != job.ID {
		t.Errorf("expected job completed, got %v", h.queue.completed)
	}

	// Parent gets a full Update (Enriched=true plus its dim); children take
	// the focused UpdateEmbeddingDim path so finalize doesn't rewrite every
	// column for a brand-new row.
	if len(h.updater.updated) != 1 || !h.updater.updated[0].Enriched {
		t.Errorf("expected 1 parent update with Enriched=true, got %d updates", len(h.updater.updated))
	}
	if h.updater.updated[0].EmbeddingDim == nil || *h.updater.updated[0].EmbeddingDim != 3 {
		t.Errorf("parent EmbeddingDim = %v, want 3", h.updater.updated[0].EmbeddingDim)
	}
	if len(h.updater.dimUpdates) != 2 {
		t.Errorf("expected 2 child dim updates, got %d", len(h.updater.dimUpdates))
	}
	for i, du := range h.updater.dimUpdates {
		if du.dim != 3 {
			t.Errorf("child dim update %d: dim = %d, want 3", i, du.dim)
		}
	}

	// Two child memories (facts).
	if len(h.creator.created) != 2 {
		t.Errorf("expected 2 child memories, got %d", len(h.creator.created))
	}

	// Child memories must inherit parent source and tags.
	for i, child := range h.creator.created {
		if child.Source == nil || *child.Source != "test-source" {
			t.Errorf("child %d: expected source 'test-source', got %v", i, child.Source)
		}
		// Must contain parent tags
		hasParentTag := false
		for _, tag := range child.Tags {
			if tag == "parent-tag" {
				hasParentTag = true
				break
			}
		}
		if !hasParentTag {
			t.Errorf("child %d: expected parent tag 'parent-tag' in tags %v", i, child.Tags)
		}
		if child.Importance != 0.5 {
			t.Errorf("child %d: expected importance 0.5, got %f", i, child.Importance)
		}
	}

	// Two lineage records.
	if len(h.lineage.created) != 2 {
		t.Errorf("expected 2 lineage records, got %d", len(h.lineage.created))
	}
	for _, lin := range h.lineage.created {
		if lin.Relation != "extracted_fact" {
			t.Errorf("expected relation 'extracted_fact', got %q", lin.Relation)
		}
		if lin.ParentID == nil || *lin.ParentID != mem.ID {
			t.Error("lineage parent should be original memory")
		}
	}

	// Two entities upserted.
	if len(h.entities.upserted) != 2 {
		t.Errorf("expected 2 entities, got %d", len(h.entities.upserted))
	}
	for _, ent := range h.entities.upserted {
		if ent.NamespaceID != mem.NamespaceID {
			t.Error("entity namespace should match memory namespace")
		}
	}

	// One relationship.
	if len(h.rels.created) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(h.rels.created))
	} else {
		rel := h.rels.created[0]
		if rel.Relation != "works_at" {
			t.Errorf("expected relation 'works_at', got %q", rel.Relation)
		}
	}

	// Vectors upserted for the parent memory, each extracted-fact child, and
	// each upserted entity. Fixture produces 2 facts and 2 entities, so we
	// expect 5 total upserts (parent + 2 facts + 2 entities).
	if len(h.vectors.vectors) != 5 {
		t.Errorf("expected 5 vector upserts (parent + 2 facts + 2 entities), got %d", len(h.vectors.vectors))
	}
	// First upsert must be the parent memory itself, not a fact — guards
	// against the old bug where the first fact's embedding was stored under
	// the parent's ID.
	if len(h.vectors.vectors) > 0 && h.vectors.vectors[0].ID != mem.ID {
		t.Errorf("first vector upsert should target parent memory %s, got %s", mem.ID, h.vectors.vectors[0].ID)
	}

	// Token usage: fact_extraction + entity_extraction + embedding = 3 records.
	if len(h.tokens.records) != 3 {
		t.Errorf("expected 3 token usage records, got %d", len(h.tokens.records))
	}
}

func TestProcessJob_SkipsLLMWhenAlreadyEnriched(t *testing.T) {
	var factCalls, entityCalls, embedCalls int
	var mu sync.Mutex

	factLLM := &mockLLMProvider{name: "test-fact", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		mu.Lock()
		factCalls++
		mu.Unlock()
		return &provider.CompletionResponse{Content: factJSON(), Model: "fact-model"}, nil
	}}
	entityLLM := &mockLLMProvider{name: "test-entity", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		mu.Lock()
		entityCalls++
		mu.Unlock()
		return &provider.CompletionResponse{Content: entityJSON(), Model: "entity-model"}, nil
	}}
	embedProv := &mockEmbeddingProvider{name: "test-embed", respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
		mu.Lock()
		embedCalls++
		mu.Unlock()
		embs := make([][]float32, len(req.Input))
		for i := range req.Input {
			embs[i] = []float32{0.1, 0.2, 0.3}
		}
		return &provider.EmbeddingResponse{Embeddings: embs, Model: "embed-model"}, nil
	}}

	h := newTestHarness(factLLM, entityLLM, embedProv)
	mem := testMemory()
	mem.Enriched = true
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	if factCalls != 0 {
		t.Errorf("expected 0 fact extraction calls for already-enriched memory, got %d", factCalls)
	}
	if entityCalls != 0 {
		t.Errorf("expected 0 entity extraction calls for already-enriched memory, got %d", entityCalls)
	}
	if embedCalls != 1 {
		t.Errorf("expected 1 embed call, got %d", embedCalls)
	}
	if len(h.creator.created) != 0 {
		t.Errorf("expected 0 child memories for enriched re-run, got %d", len(h.creator.created))
	}
	if len(h.lineage.created) != 0 {
		t.Errorf("expected 0 new lineage rows for enriched re-run, got %d", len(h.lineage.created))
	}
	if len(h.entities.upserted) != 0 {
		t.Errorf("expected 0 new entity upserts for enriched re-run, got %d", len(h.entities.upserted))
	}
	if len(h.queue.completed) != 1 || h.queue.completed[0] != job.ID {
		t.Errorf("expected job completed, got %v", h.queue.completed)
	}
	if len(h.vectors.vectors) != 1 || h.vectors.vectors[0].ID != mem.ID {
		t.Errorf("expected 1 vector upsert for parent memory, got %+v", h.vectors.vectors)
	}
}

func TestProcessJob_FactExtractionOnly(t *testing.T) {
	factLLM := &mockLLMProvider{name: "fact", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: factJSON(),
			Model:   "m",
			Usage:   provider.TokenUsage{PromptTokens: 10, CompletionTokens: 5},
		}, nil
	}}

	h := newTestHarness(factLLM, nil, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	err := h.pool.processJob(context.Background(), "w-0", job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(h.queue.completed) != 1 {
		t.Error("job should be completed")
	}
	if len(h.creator.created) != 2 {
		t.Errorf("expected 2 child memories, got %d", len(h.creator.created))
	}
	if len(h.entities.upserted) != 0 {
		t.Error("no entities should be upserted without entity provider")
	}
}

func TestProcessJob_EntityExtractionOnly(t *testing.T) {
	entityLLM := &mockLLMProvider{name: "entity", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: entityJSON(),
			Model:   "m",
			Usage:   provider.TokenUsage{PromptTokens: 10, CompletionTokens: 5},
		}, nil
	}}

	h := newTestHarness(nil, entityLLM, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	err := h.pool.processJob(context.Background(), "w-0", job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(h.queue.completed) != 1 {
		t.Error("job should be completed")
	}
	if len(h.creator.created) != 0 {
		t.Error("no child memories without fact provider")
	}
	if len(h.entities.upserted) != 2 {
		t.Errorf("expected 2 entities, got %d", len(h.entities.upserted))
	}
	if len(h.rels.created) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(h.rels.created))
	}
}

func TestProcessJob_NoProviders(t *testing.T) {
	h := newTestHarness(nil, nil, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	err := h.pool.processJob(context.Background(), "w-0", job)
	if err == nil {
		t.Fatal("expected error when no providers configured")
	}

	// Job should be failed, not completed.
	if len(h.queue.completed) != 0 {
		t.Error("job should not be completed with no providers")
	}
	if len(h.queue.failed) != 1 {
		t.Error("job should be marked failed when no providers configured")
	}
	if len(h.updater.updated) != 0 {
		t.Error("memory should not be marked enriched when no providers ran")
	}
}

func TestProcessJob_FactLLMError(t *testing.T) {
	factLLM := &mockLLMProvider{name: "fact", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return nil, errors.New("LLM unavailable")
	}}

	h := newTestHarness(factLLM, nil, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	err := h.pool.processJob(context.Background(), "w-0", job)
	if err == nil {
		t.Fatal("expected error from fact LLM failure")
	}

	if _, ok := h.queue.failed[job.ID]; !ok {
		t.Error("job should be marked as failed")
	}
}

func TestProcessJob_EntityLLMError_FactsSucceed(t *testing.T) {
	factLLM := &mockLLMProvider{name: "fact", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: factJSON(),
			Model:   "m",
			Usage:   provider.TokenUsage{PromptTokens: 10, CompletionTokens: 5},
		}, nil
	}}
	entityLLM := &mockLLMProvider{name: "entity", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return nil, errors.New("entity LLM down")
	}}

	h := newTestHarness(factLLM, entityLLM, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	err := h.pool.processJob(context.Background(), "w-0", job)
	if err != nil {
		t.Fatalf("expected partial success (facts ok, entities failed), got error: %v", err)
	}

	// Job completed (partial success).
	if len(h.queue.completed) != 1 {
		t.Error("job should be completed on partial success")
	}
	// Facts were processed.
	if len(h.creator.created) != 2 {
		t.Errorf("expected 2 child memories, got %d", len(h.creator.created))
	}
	// No entities.
	if len(h.entities.upserted) != 0 {
		t.Error("no entities should be upserted when entity LLM fails")
	}
}

func TestProcessJob_MemoryNotFound(t *testing.T) {
	h := newTestHarness(nil, nil, nil)
	job := testJob(uuid.New(), uuid.New()) // memory doesn't exist

	err := h.pool.processJob(context.Background(), "w-0", job)
	if err == nil {
		t.Fatal("expected error for missing memory")
	}

	if _, ok := h.queue.failed[job.ID]; !ok {
		t.Error("job should be marked as failed")
	}
}

func TestProcessJob_TokenUsageRecorded(t *testing.T) {
	factLLM := &mockLLMProvider{name: "fact-prov", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: factJSON(),
			Model:   "fact-m",
			Usage:   provider.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
		}, nil
	}}

	h := newTestHarness(factLLM, nil, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	_ = h.pool.processJob(context.Background(), "w-0", job)

	if len(h.tokens.records) != 1 {
		t.Fatalf("expected 1 token usage record, got %d", len(h.tokens.records))
	}
	rec := h.tokens.records[0]
	if rec.Operation != "fact_extraction" {
		t.Errorf("expected operation 'fact_extraction', got %q", rec.Operation)
	}
	if rec.Provider != "fact-prov" {
		t.Errorf("expected provider 'fact-prov', got %q", rec.Provider)
	}
	if rec.Model != "fact-m" {
		t.Errorf("expected model 'fact-m', got %q", rec.Model)
	}
	if rec.TokensInput != 100 || rec.TokensOutput != 50 {
		t.Errorf("token counts mismatch: input=%d output=%d", rec.TokensInput, rec.TokensOutput)
	}
	if rec.MemoryID == nil || *rec.MemoryID != mem.ID {
		t.Error("token usage should reference the memory")
	}
}

func TestProcessJob_LineageRecordsCreated(t *testing.T) {
	factLLM := &mockLLMProvider{name: "fact", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: factJSON(),
			Model:   "m",
			Usage:   provider.TokenUsage{},
		}, nil
	}}

	h := newTestHarness(factLLM, nil, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	_ = h.pool.processJob(context.Background(), "w-0", job)

	if len(h.lineage.created) != 2 {
		t.Fatalf("expected 2 lineage records, got %d", len(h.lineage.created))
	}

	// Each lineage record's MemoryID should match a created child memory ID.
	childIDs := make(map[uuid.UUID]bool)
	for _, c := range h.creator.created {
		childIDs[c.ID] = true
	}
	for _, lin := range h.lineage.created {
		if !childIDs[lin.MemoryID] {
			t.Errorf("lineage MemoryID %s does not match any child memory", lin.MemoryID)
		}
	}
}

func TestProcessJob_EntitiesNamespaceMatch(t *testing.T) {
	entityLLM := &mockLLMProvider{name: "entity", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{
			Content: entityJSON(),
			Model:   "m",
			Usage:   provider.TokenUsage{},
		}, nil
	}}

	h := newTestHarness(nil, entityLLM, nil)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	_ = h.pool.processJob(context.Background(), "w-0", job)

	for _, ent := range h.entities.upserted {
		if ent.NamespaceID != mem.NamespaceID {
			t.Errorf("entity %q namespace %s != memory namespace %s",
				ent.Name, ent.NamespaceID, mem.NamespaceID)
		}
	}
}

func TestWorkerPool_StartStop(t *testing.T) {
	h := newTestHarness(nil, nil, nil)
	h.pool.Start()
	// Allow a brief moment for goroutines to spin up.
	time.Sleep(20 * time.Millisecond)
	h.pool.Stop()
	// If we reach here without panic or hang, the test passes.
}

// TestProcessBatch_SingleSharedEmbed verifies the batched path runs ONE embed
// call for N jobs and distributes the returned vectors back to the right
// parent + child IDs. This is the throughput-preservation guarantee from the
// 60s-bug fix: removing sync embed from the write path must not turn a
// 100-item batch_store into 100 embed API calls.
func TestProcessBatch_SingleSharedEmbed(t *testing.T) {
	// Count embed invocations and capture the batched input size.
	var embedCallCount int
	var lastInputSize int
	embedProv := &mockEmbeddingProvider{name: "test-embed", respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
		embedCallCount++
		lastInputSize = len(req.Input)
		embs := make([][]float32, len(req.Input))
		for i := range req.Input {
			embs[i] = []float32{float32(i), float32(i) + 0.1, float32(i) + 0.2}
		}
		return &provider.EmbeddingResponse{
			Embeddings: embs,
			Model:      "embed-model",
			Usage:      provider.TokenUsage{PromptTokens: 40, TotalTokens: 40},
		}, nil
	}}

	// Embed-only worker (no fact/entity providers) so each job produces
	// exactly one input (the parent's own content).
	h := newTestHarness(nil, nil, embedProv)

	jobs := make([]*model.EnrichmentJob, 0, 3)
	wantParentIDs := make(map[uuid.UUID]bool, 3)
	for i := 0; i < 3; i++ {
		mem := testMemory()
		mem.ID = uuid.New()
		mem.Content = fmt.Sprintf("memory-content-%d", i)
		h.reader.byID[mem.ID] = mem
		wantParentIDs[mem.ID] = true
		jobs = append(jobs, testJob(mem.ID, mem.NamespaceID))
	}

	h.pool.processBatch(context.Background(), "w-0", jobs)

	if embedCallCount != 1 {
		t.Fatalf("expected 1 shared embed call for %d jobs, got %d", len(jobs), embedCallCount)
	}
	if lastInputSize != len(jobs) {
		t.Fatalf("expected batched embed input size %d (one per parent), got %d", len(jobs), lastInputSize)
	}

	if len(h.vectors.vectors) != len(jobs) {
		t.Fatalf("expected %d vector upserts, got %d", len(jobs), len(h.vectors.vectors))
	}
	for _, v := range h.vectors.vectors {
		if !wantParentIDs[v.ID] {
			t.Errorf("unexpected vector upsert for id %s (not in parent set)", v.ID)
		}
	}

	// Every job must be marked complete.
	if len(h.queue.completed) != len(jobs) {
		t.Errorf("expected %d completed jobs, got %d", len(jobs), len(h.queue.completed))
	}

	// Embedding usage records are split per-job (one record each) so
	// downstream billing can attribute by parent memory.
	var embedRecords int
	for _, r := range h.tokens.records {
		if r.Operation == "embedding" {
			embedRecords++
		}
	}
	if embedRecords != len(jobs) {
		t.Errorf("expected %d per-job embedding usage records, got %d", len(jobs), embedRecords)
	}
}

// ---------------------------------------------------------------------------
// Parse response tests (three-tier recovery)
// ---------------------------------------------------------------------------

func TestParseFactResponse_Direct(t *testing.T) {
	facts, err := parseFactResponse(factJSON())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(facts) != 2 {
		t.Errorf("expected 2 facts, got %d", len(facts))
	}
}

func TestParseFactResponse_WithFences(t *testing.T) {
	// With JSON mode enabled, LLM output should never contain markdown fences.
	// The parser no longer strips fences, so fenced input is treated as invalid.
	input := "```json\n" + factJSON() + "\n```"
	_, err := parseFactResponse(input)
	if err == nil {
		t.Error("expected error for markdown-fenced input (JSON mode makes fence stripping unnecessary)")
	}
}

func TestParseFactResponse_RegexFallback(t *testing.T) {
	// With JSON mode enabled, LLM output should be pure JSON.
	// The parser no longer extracts JSON from surrounding text.
	input := "Here are the facts:\n" + factJSON() + "\nHope that helps!"
	_, err := parseFactResponse(input)
	if err == nil {
		t.Error("expected error for text-wrapped input (JSON mode makes extraction unnecessary)")
	}
}

func TestParseEntityResponse_Direct(t *testing.T) {
	result, err := parseEntityResponse(entityJSON())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Entities) != 2 {
		t.Errorf("expected 2 entities, got %d", len(result.Entities))
	}
	if len(result.Relationships) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(result.Relationships))
	}
}

func TestParseEntityResponse_WithFences(t *testing.T) {
	// With JSON mode enabled, LLM output should never contain markdown fences.
	// The parser no longer strips fences, so fenced input is treated as invalid.
	input := "```\n" + entityJSON() + "\n```"
	_, err := parseEntityResponse(input)
	if err == nil {
		t.Error("expected error for markdown-fenced input (JSON mode makes fence stripping unnecessary)")
	}
}

func TestParseFactResponse_Invalid(t *testing.T) {
	_, err := parseFactResponse("not json at all")
	if err == nil {
		t.Error("expected error for invalid input")
	}
}

func TestParseEntityResponse_Invalid(t *testing.T) {
	_, err := parseEntityResponse("not json at all")
	if err == nil {
		t.Error("expected error for invalid input")
	}
}
