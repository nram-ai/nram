package enrichment

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

func (m *mockMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	out := make([]model.Memory, 0, len(ids))
	for _, id := range ids {
		if mem, ok := m.byID[id]; ok {
			out = append(out, *mem)
		}
	}
	return out, nil
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
	mu             sync.Mutex
	jobs           []*model.EnrichmentJob
	completed      []uuid.UUID
	failed         map[uuid.UUID]string
	released       []uuid.UUID
	stepsCompleted map[uuid.UUID][]string
	claimErr       error
}

func newMockQueueClaimer() *mockQueueClaimer {
	return &mockQueueClaimer{
		failed:         make(map[uuid.UUID]string),
		stepsCompleted: make(map[uuid.UUID][]string),
	}
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

// CompleteWithWarning mirrors EnrichmentQueueRepo.CompleteWithWarning: the
// job is added to completed AND the warning payload is JSON-encoded into
// the failed map alongside it, so tests asserting on either dimension see
// the same wire-form admin views would render.
func (m *mockQueueClaimer) CompleteWithWarning(_ context.Context, id uuid.UUID, payload any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed = append(m.completed, id)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	m.failed[id] = string(encoded)
	return nil
}

// Fail mirrors EnrichmentQueueRepo.Fail's contract: payload is JSON-marshalled
// before being stored, so tests that string-match against the failed map see
// the same encoded form admin views would render on the queue row.
func (m *mockQueueClaimer) Fail(_ context.Context, id uuid.UUID, payload any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	m.failed[id] = string(encoded)
	return nil
}

func (m *mockQueueClaimer) Release(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = append(m.released, id)
	return nil
}

func (m *mockQueueClaimer) MarkStepCompleted(_ context.Context, id uuid.UUID, step string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.stepsCompleted[id] {
		if existing == step {
			return nil
		}
	}
	m.stepsCompleted[id] = append(m.stepsCompleted[id], step)
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
	mu                sync.Mutex
	created           []*model.Relationship
	err               error
	hasBySourceMemory bool
	hasBySourceErr    error
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

func (m *mockRelationshipCreator) HasBySourceMemory(_ context.Context, _, _ uuid.UUID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hasBySourceErr != nil {
		return false, m.hasBySourceErr
	}
	return m.hasBySourceMemory, nil
}

type mockLineageCreator struct {
	mu                       sync.Mutex
	created                  []*model.MemoryLineage
	err                      error
	hasExtractedFactChildren bool
	hasExtractedFactErr      error
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

func (m *mockLineageCreator) HasExtractedFactChildren(_ context.Context, _, _ uuid.UUID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hasExtractedFactErr != nil {
		return false, m.hasExtractedFactErr
	}
	return m.hasExtractedFactChildren, nil
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
	deleted []uuid.UUID
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

func (m *mockVectorWriter) Delete(_ context.Context, _ storage.VectorKind, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, id)
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

// noopFactLLM returns an LLM stub that emits an empty fact array. Used by
// tests that focus on a single slot (e.g., entity extraction) but need the
// other slots configured to satisfy the worker's all-three gate.
func noopFactLLM() *mockLLMProvider {
	return &mockLLMProvider{
		name: "fact-noop",
		respond: func(*provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{
				Content: `[]`,
				Model:   "noop",
				Usage:   provider.TokenUsage{},
			}, nil
		},
	}
}

// noopEntityLLM returns an LLM stub that emits an empty entity payload.
func noopEntityLLM() *mockLLMProvider {
	return &mockLLMProvider{
		name: "entity-noop",
		respond: func(*provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{
				Content: `{"entities":[],"relationships":[]}`,
				Model:   "noop",
				Usage:   provider.TokenUsage{},
			}, nil
		},
	}
}

// noopEmbed returns an embedding stub that emits a 3-dim zero vector for
// each input.
func noopEmbed() *mockEmbeddingProvider {
	return &mockEmbeddingProvider{
		name: "embed-noop",
		respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			embs := make([][]float32, len(req.Input))
			for i := range req.Input {
				embs[i] = []float32{0, 0, 0}
			}
			return &provider.EmbeddingResponse{
				Embeddings: embs,
				Model:      "noop",
				Usage:      provider.TokenUsage{},
			}, nil
		},
	}
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

	// Wrap test provider stubs so the middleware writes token_usage rows
	// to h.tokens on every wrapped call — matches production wiring
	// (registry wrap) without spinning up a registry in unit tests.
	factFn := provider.WrapLLMForTest(constLLM(factLLM), h.tokens)
	entityFn := provider.WrapLLMForTest(constLLM(entityLLM), h.tokens)
	embedFn := provider.WrapEmbeddingForTest(constEmbed(embedProv), h.tokens)

	h.pool = NewWorkerPool(
		WorkerConfig{Workers: 1, PollInterval: 10 * time.Millisecond},
		h.reader, h.updater, h.creator, nil, h.queue,
		h.entities, h.rels, h.lineage, h.vectors,
		factFn, entityFn, embedFn,
		nil, nil, nil, nil,
	)
	return h
}

// constLLM returns a closure that always yields p (which may be nil).
// Always returns a non-nil function so worker code can call it without
// nil-checking; the closure body returns whatever p is.
func constLLM(p provider.LLMProvider) func() provider.LLMProvider {
	return func() provider.LLMProvider { return p }
}

// constEmbed mirrors constLLM for embedding providers.
func constEmbed(p provider.EmbeddingProvider) func() provider.EmbeddingProvider {
	return func() provider.EmbeddingProvider { return p }
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

// backfillProbeProviders builds counting fact/entity/embed providers used
// across the per-step gating tests. Returns the providers and pointers to
// per-call counters so individual tests can assert which LLM steps fired.
func backfillProbeProviders() (factLLM, entityLLM provider.LLMProvider, embedProv provider.EmbeddingProvider, factCalls, entityCalls, embedCalls *int) {
	var fc, ec, embC int
	var mu sync.Mutex
	factLLM = &mockLLMProvider{name: "test-fact", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		mu.Lock()
		fc++
		mu.Unlock()
		return &provider.CompletionResponse{Content: factJSON(), Model: "fact-model"}, nil
	}}
	entityLLM = &mockLLMProvider{name: "test-entity", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		mu.Lock()
		ec++
		mu.Unlock()
		return &provider.CompletionResponse{Content: entityJSON(), Model: "entity-model"}, nil
	}}
	embedProv = &mockEmbeddingProvider{name: "test-embed", respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
		mu.Lock()
		embC++
		mu.Unlock()
		embs := make([][]float32, len(req.Input))
		for i := range req.Input {
			embs[i] = []float32{0.1, 0.2, 0.3}
		}
		return &provider.EmbeddingResponse{Embeddings: embs, Model: "embed-model"}, nil
	}}
	return factLLM, entityLLM, embedProv, &fc, &ec, &embC
}

// TestProcessJob_BackfillSkipsFactsWhenLineagePresent verifies the
// historical-memory case for embed backfill: a memory with mem.Enriched
// false but extracted_fact lineage rows already in the DB triggers the
// lineage probe, which short-circuits fact extraction and avoids burning
// a chat completion.
func TestProcessJob_BackfillSkipsFactsWhenLineagePresent(t *testing.T) {
	factLLM, entityLLM, embedProv, factCalls, entityCalls, embedCalls := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	h.lineage.hasExtractedFactChildren = true // simulate prior fact extraction
	mem := testMemory()
	mem.Enriched = false
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	if *factCalls != 0 {
		t.Errorf("expected 0 fact extraction calls when lineage present, got %d", *factCalls)
	}
	if *entityCalls != 1 {
		t.Errorf("expected 1 entity extraction call (only fact gated), got %d", *entityCalls)
	}
	if *embedCalls != 1 {
		t.Errorf("expected 1 embed call, got %d", *embedCalls)
	}
}

// TestProcessJob_BackfillSkipsEntitiesWhenRelationshipsPresent — symmetric
// case for entity extraction. A memory with relationship rows already
// present (source_memory = mem.ID) triggers the relationship probe and
// skips the entity LLM call.
func TestProcessJob_BackfillSkipsEntitiesWhenRelationshipsPresent(t *testing.T) {
	factLLM, entityLLM, embedProv, factCalls, entityCalls, embedCalls := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	h.rels.hasBySourceMemory = true
	mem := testMemory()
	mem.Enriched = false
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	if *entityCalls != 0 {
		t.Errorf("expected 0 entity extraction calls when relationships present, got %d", *entityCalls)
	}
	if *factCalls != 1 {
		t.Errorf("expected 1 fact extraction call (only entity gated), got %d", *factCalls)
	}
	if *embedCalls != 1 {
		t.Errorf("expected 1 embed call, got %d", *embedCalls)
	}
}

// TestProcessJob_BackfillRunsBothWhenNothingPresent — control case. A
// historical memory with mem.Enriched=false, empty steps_completed, and
// no lineage/relationship rows should run both extractions normally.
func TestProcessJob_BackfillRunsBothWhenNothingPresent(t *testing.T) {
	factLLM, entityLLM, embedProv, factCalls, entityCalls, embedCalls := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	mem := testMemory()
	mem.Enriched = false
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	if *factCalls != 1 {
		t.Errorf("expected 1 fact extraction call, got %d", *factCalls)
	}
	if *entityCalls != 1 {
		t.Errorf("expected 1 entity extraction call, got %d", *entityCalls)
	}
	if *embedCalls != 1 {
		t.Errorf("expected 1 embed call, got %d", *embedCalls)
	}
}

// TestProcessJob_BackfillSkipsByStepsCompleted — covers the in-flight
// retry case. job.StepsCompleted carries "fact_extraction" from a
// partially-successful prior run; the gate skips fact extraction without
// consulting the lineage probe.
func TestProcessJob_BackfillSkipsByStepsCompleted(t *testing.T) {
	factLLM, entityLLM, embedProv, factCalls, entityCalls, _ := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	mem := testMemory()
	mem.Enriched = false
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)
	job.StepsCompleted = json.RawMessage(`["` + model.StepFactExtraction + `"]`)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	if *factCalls != 0 {
		t.Errorf("expected 0 fact calls when steps_completed names fact_extraction, got %d", *factCalls)
	}
	if *entityCalls != 1 {
		t.Errorf("expected 1 entity call, got %d", *entityCalls)
	}
}

// TestProcessJob_StampsStepsCompletedOnSuccess verifies that finalize
// records each successful step into the queue's steps_completed marker so
// retries skip work that has already run.
func TestProcessJob_StampsStepsCompletedOnSuccess(t *testing.T) {
	factLLM, entityLLM, embedProv, _, _, _ := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	mem := testMemory()
	mem.Enriched = false
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	steps := h.queue.stepsCompleted[job.ID]
	want := map[string]bool{
		model.StepFactExtraction:   false,
		model.StepEntityExtraction: false,
		model.StepEmbedding:        false,
	}
	for _, s := range steps {
		if _, ok := want[s]; ok {
			want[s] = true
		}
	}
	for s, seen := range want {
		if !seen {
			t.Errorf("expected step %q to be marked completed; got %v", s, steps)
		}
	}
}

// TestStepDoneSet covers the parser used by runPreEmbed to read prior
// step markers off the job. Tolerant of malformed inputs.
func TestStepDoneSet(t *testing.T) {
	cases := []struct {
		name string
		in   json.RawMessage
		want map[string]bool
	}{
		{"nil", nil, map[string]bool{}},
		{"empty", json.RawMessage(`[]`), map[string]bool{}},
		{"single", json.RawMessage(`["fact_extraction"]`), map[string]bool{"fact_extraction": true}},
		{"multi", json.RawMessage(`["fact_extraction","embedding"]`), map[string]bool{"fact_extraction": true, "embedding": true}},
		{"malformed-string", json.RawMessage(`"not-an-array"`), map[string]bool{}},
		{"malformed-json", json.RawMessage(`{`), map[string]bool{}},
		{"empty-string-entries", json.RawMessage(`["",""]`), map[string]bool{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stepDoneSet(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len mismatch: got %v, want %v", got, tc.want)
			}
			for k := range tc.want {
				if !got[k] {
					t.Errorf("missing key %q in %v", k, got)
				}
			}
		})
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

	// Entity + embed are no-op stubs to keep the all-three gate open while
	// the test focuses on the fact-extraction path.
	h := newTestHarness(factLLM, noopEntityLLM(), noopEmbed())
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
		t.Error("no entities should be upserted when entity stub returns empty payload")
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

	// Fact + embed are no-op stubs to keep the all-three gate open while
	// the test focuses on the entity-extraction path.
	h := newTestHarness(noopFactLLM(), entityLLM, noopEmbed())
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
		t.Error("no child memories when fact stub returns empty payload")
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
		t.Fatal("expected error when gate is closed (no providers configured)")
	}

	// Gate-closed mid-batch must release the job (status=pending, attempts
	// unchanged) so the backlog drains automatically once the missing slot
	// is configured. Failing the job would force admins to manually retry.
	if len(h.queue.completed) != 0 {
		t.Error("job should not be completed with no providers")
	}
	if len(h.queue.failed) != 0 {
		t.Errorf("job should not be failed when gate is closed; got failed=%d", len(h.queue.failed))
	}
	if len(h.queue.released) != 1 {
		t.Errorf("job should be released to pending when gate is closed; got released=%d", len(h.queue.released))
	}
	if len(h.queue.released) == 1 && h.queue.released[0] != job.ID {
		t.Errorf("released job ID = %s, want %s", h.queue.released[0], job.ID)
	}
	if len(h.updater.updated) != 0 {
		t.Error("memory should not be marked enriched when no providers ran")
	}
}

// TestProcessJob_PartialProviders verifies the gate is closed when ANY of
// embedding, fact, or entity is unconfigured — not just all three. This is
// the new behavior; the old worker only failed when all three were nil.
func TestProcessJob_PartialProviders(t *testing.T) {
	cases := []struct {
		name         string
		fact, entity bool
		embed        bool
	}{
		{"only-fact-missing", false, true, true},
		{"only-entity-missing", true, false, true},
		{"only-embed-missing", true, true, false},
		{"fact-and-entity-set", true, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Declare as interface types so unset slots stay as
			// nil-interface (not typed-nil pointer wrapped in a
			// non-nil interface, which would defeat the gate check).
			var fact, entity provider.LLMProvider
			var embed provider.EmbeddingProvider
			if tc.fact {
				fact = &mockLLMProvider{name: "fact"}
			}
			if tc.entity {
				entity = &mockLLMProvider{name: "entity"}
			}
			if tc.embed {
				embed = &mockEmbeddingProvider{}
			}
			h := newTestHarness(fact, entity, embed)
			mem := testMemory()
			h.reader.byID[mem.ID] = mem
			job := testJob(mem.ID, mem.NamespaceID)

			err := h.pool.processJob(context.Background(), "w-0", job)
			if err == nil {
				t.Fatalf("expected error when gate is closed (case %s)", tc.name)
			}
			if len(h.queue.failed) != 0 {
				t.Errorf("job should not be failed when gate is partially closed (case %s)", tc.name)
			}
			if len(h.queue.released) != 1 {
				t.Errorf("job should be released when gate is partially closed (case %s); got released=%d", tc.name, len(h.queue.released))
			}
		})
	}
}

func TestProcessJob_FactLLMError(t *testing.T) {
	factLLM := &mockLLMProvider{name: "fact", respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return nil, errors.New("LLM unavailable")
	}}

	// Entity + embed are no-op stubs so the all-three gate stays open and
	// the test reaches the fact-LLM-error path under runPreEmbed.
	h := newTestHarness(factLLM, noopEntityLLM(), noopEmbed())
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

	// Embed is a no-op stub to keep the all-three gate open; the test
	// exercises partial success of fact + entity extraction.
	h := newTestHarness(factLLM, entityLLM, noopEmbed())
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

	// Entity + embed are no-op stubs to keep the all-three gate open
	// while the test focuses on the fact extraction's token-usage row.
	h := newTestHarness(factLLM, noopEntityLLM(), noopEmbed())
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	_ = h.pool.processJob(context.Background(), "w-0", job)

	// Three records: fact (real), entity (noop), embedding (noop).
	if len(h.tokens.records) < 1 {
		t.Fatalf("expected at least 1 token usage record, got %d", len(h.tokens.records))
	}
	var rec *model.TokenUsage
	for _, r := range h.tokens.records {
		if r.Operation == "fact_extraction" {
			rec = r
			break
		}
	}
	if rec == nil {
		t.Fatalf("expected a fact_extraction token usage record")
	}
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

	// Entity + embed are no-op stubs to keep the all-three gate open
	// while the test focuses on lineage records produced by fact extraction.
	h := newTestHarness(factLLM, noopEntityLLM(), noopEmbed())
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

	// Fact + entity stubs return empty payloads so each job produces
	// exactly one input (the parent's own content) for the shared embed
	// call. The all-three gate stays open with the stubs in place.
	h := newTestHarness(noopFactLLM(), noopEntityLLM(), embedProv)

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

	// One batched embed call → one aggregate token_usage row. Per-job
	// attribution was removed when recording centralized in the
	// UsageRecordingProvider middleware (see plan: "aggregate-only is
	// the correct trade"). Per-job attribution can be recovered via
	// request_id correlation when needed.
	var embedRecords int
	for _, r := range h.tokens.records {
		if r.Operation == "embedding" {
			embedRecords++
		}
	}
	if embedRecords != 1 {
		t.Errorf("expected 1 aggregate embedding usage record, got %d", embedRecords)
	}
}

// TestProcessBatch_VectorUpsertFailure_FailsJobs drives the
// UpsertBatch failure path. Before the fix, runEmbedBatch logged the
// error and let processBatch's finalize loop run, persisting
// embedding_dim on memories whose vectors had not landed. Now the
// pending jobs in the failed batch are marked failed and finalize is
// skipped, so the memory rows do not persist a stale embedding_dim.
func TestProcessBatch_VectorUpsertFailure_FailsJobs(t *testing.T) {
	embedProv := &mockEmbeddingProvider{
		name: "test-embed",
		respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			embs := make([][]float32, len(req.Input))
			for i := range req.Input {
				embs[i] = []float32{float32(i), float32(i) + 0.1, float32(i) + 0.2}
			}
			return &provider.EmbeddingResponse{
				Embeddings: embs,
				Model:      "embed-model",
				Usage:      provider.TokenUsage{PromptTokens: 40, TotalTokens: 40},
			}, nil
		},
	}

	h := newTestHarness(noopFactLLM(), noopEntityLLM(), embedProv)
	// Force every UpsertBatch to fail so the worker's failure path runs.
	h.vectors.err = errors.New("vector store offline")

	jobs := make([]*model.EnrichmentJob, 0, 2)
	memIDs := make([]uuid.UUID, 0, 2)
	for i := 0; i < 2; i++ {
		mem := testMemory()
		mem.ID = uuid.New()
		mem.Content = fmt.Sprintf("memory-content-%d", i)
		h.reader.byID[mem.ID] = mem
		memIDs = append(memIDs, mem.ID)
		jobs = append(jobs, testJob(mem.ID, mem.NamespaceID))
	}

	h.pool.processBatch(context.Background(), "w-0", jobs)

	// All jobs must be in the queue's failed map; none completed.
	if len(h.queue.completed) != 0 {
		t.Errorf("no jobs should be completed when vector batch failed; got %d", len(h.queue.completed))
	}
	if len(h.queue.failed) != len(jobs) {
		t.Errorf("expected %d failed jobs; got %d", len(jobs), len(h.queue.failed))
	}
	for _, j := range jobs {
		if msg, ok := h.queue.failed[j.ID]; !ok {
			t.Errorf("job %s not marked failed", j.ID)
		} else if !strings.Contains(msg, "vector upsert batch") {
			t.Errorf("expected failure message to mention vector upsert; got %q", msg)
		}
	}

	// No memory Update should have happened — finalizeJob is the only
	// thing that persists embedding_dim, and it must be skipped on the
	// vectorWriteFailed flag.
	for _, mem := range h.updater.updated {
		if mem.EmbeddingDim != nil {
			t.Errorf("memory %s should not persist embedding_dim after vector batch failure; got %d",
				mem.ID, *mem.EmbeddingDim)
		}
	}
	_ = memIDs
}

// Parser tests for the unified extraction parsers live in
// internal/service/extract_test.go (TestParseFacts_*, TestParseEntities_*).
// The worker's local copies were removed as part of the
// extract.go/worker.go duplication collapse.
