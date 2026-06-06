package enrichment

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

type mockMemoryReader struct {
	mu   sync.Mutex
	byID map[uuid.UUID]*model.Memory
	err  error
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
	mu             sync.Mutex
	updated        []*model.Memory
	dimUpdates     []dimUpdate
	enrichedMarks  []enrichedMark
	supersedeMarks []supersedeMark
	err            error
	// reader, when wired, lets MutateInLock re-read the freshest stored
	// memory before invoking the mutator. Tests that exercise the merge
	// paths (paraphrase guard, ingestion stamp) need this; tests that
	// only exercise partial-column writes can leave it nil.
	reader *mockMemoryReader
}

type supersedeMark struct {
	oldID       uuid.UUID
	namespaceID uuid.UUID
	newID       uuid.UUID
}

type dimUpdate struct {
	id  uuid.UUID
	dim int
}

type enrichedMark struct {
	id                   uuid.UUID
	namespaceID          uuid.UUID
	embeddingDim         *int
	metadata             json.RawMessage
	augmentedQueries     []string
	augmentedEmbeddingAt *time.Time
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

func (m *mockMemoryUpdater) MarkEnriched(_ context.Context, id, namespaceID uuid.UUID, embeddingDim *int, metadata json.RawMessage, augmentedQueries []string, augmentedEmbeddingAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	var queriesCopy []string
	if augmentedQueries != nil {
		// Preserve the empty-vs-nil distinction the production code
		// branches on. append([]string(nil), empty...) returns nil and
		// would collapse []string{} back to nil here.
		queriesCopy = make([]string, len(augmentedQueries))
		copy(queriesCopy, augmentedQueries)
	}
	var atCopy *time.Time
	if augmentedEmbeddingAt != nil {
		t := *augmentedEmbeddingAt
		atCopy = &t
	}
	m.enrichedMarks = append(m.enrichedMarks, enrichedMark{
		id:                   id,
		namespaceID:          namespaceID,
		embeddingDim:         embeddingDim,
		metadata:             metadata,
		augmentedQueries:     queriesCopy,
		augmentedEmbeddingAt: atCopy,
	})
	return nil
}

func (m *mockMemoryUpdater) MarkSupersededBy(_ context.Context, oldID, namespaceID, newID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.supersedeMarks = append(m.supersedeMarks, supersedeMark{oldID: oldID, namespaceID: namespaceID, newID: newID})
	return nil
}

func (m *mockMemoryUpdater) MutateInLock(ctx context.Context, id uuid.UUID, mutate func(*model.Memory) (bool, error)) (*model.Memory, error) {
	if m.reader == nil {
		return nil, fmt.Errorf("mockMemoryUpdater.MutateInLock: no reader wired (set updater.reader in the test harness)")
	}
	// Hold m.mu across the entire lookup-mutate-write so two concurrent
	// MutateInLock callers on the same id cannot both read the same
	// baseline and clobber each other, the same invariant the production
	// WithMemoryLock provides. Mutators in this codebase never re-enter
	// the stub, so the long hold cannot deadlock.
	m.mu.Lock()
	defer m.mu.Unlock()

	var current *model.Memory
	for i := len(m.updated) - 1; i >= 0; i-- {
		if m.updated[i].ID == id {
			cp := *m.updated[i]
			cp.Tags = append([]string(nil), m.updated[i].Tags...)
			cp.Metadata = append(json.RawMessage(nil), m.updated[i].Metadata...)
			current = &cp
			break
		}
	}
	if current == nil {
		fresh, err := m.reader.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		current = fresh
	}
	write, err := mutate(current)
	if err != nil {
		return nil, err
	}
	if write {
		if m.err != nil {
			return nil, m.err
		}
		cp := *current
		m.updated = append(m.updated, &cp)
	}
	return current, nil
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
	mu                sync.Mutex
	jobs              []*model.EnrichmentJob
	completed         []uuid.UUID
	failed            map[uuid.UUID]string
	released          []uuid.UUID
	stepsCompleted    map[uuid.UUID][]string
	queryAugmentSkips map[uuid.UUID]string
	heartbeats        map[string]int
	claimErr          error
	enqueued          []*model.EnrichmentJob
	enqueueErr        error
}

func newMockQueueClaimer() *mockQueueClaimer {
	return &mockQueueClaimer{
		failed:            make(map[uuid.UUID]string),
		stepsCompleted:    make(map[uuid.UUID][]string),
		queryAugmentSkips: make(map[uuid.UUID]string),
	}
}

func (m *mockQueueClaimer) Enqueue(_ context.Context, item *model.EnrichmentJob) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.enqueueErr != nil {
		return false, m.enqueueErr
	}
	m.enqueued = append(m.enqueued, item)
	return true, nil
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
	n := min(max, len(m.jobs))
	batch := make([]*model.EnrichmentJob, n)
	copy(batch, m.jobs[:n])
	m.jobs = m.jobs[n:]
	return batch, nil
}

// The mock has no concept of claim ownership, so workerID is ignored. The
// stale-write guard is exercised in the storage repo tests (where the real
// SQL lives).
func (m *mockQueueClaimer) Complete(_ context.Context, id uuid.UUID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed = append(m.completed, id)
	return nil
}

// CompleteWithWarning encodes the payload to JSON before storing (same
// wire form admin views render) so tests can string-match against
// m.failed regardless of whether the job took the clean or partial path.
func (m *mockQueueClaimer) CompleteWithWarning(_ context.Context, id uuid.UUID, _ string, payload any) error {
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

func (m *mockQueueClaimer) Fail(_ context.Context, id uuid.UUID, _ string, payload any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	m.failed[id] = string(encoded)
	return nil
}

func (m *mockQueueClaimer) Release(_ context.Context, id uuid.UUID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = append(m.released, id)
	return nil
}

func (m *mockQueueClaimer) MarkStepCompleted(_ context.Context, id uuid.UUID, step string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slices.Contains(m.stepsCompleted[id], step) {
		return nil
	}
	m.stepsCompleted[id] = append(m.stepsCompleted[id], step)
	return nil
}

func (m *mockQueueClaimer) SetQueryAugmentSkipReason(_ context.Context, id uuid.UUID, _ string, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reason == "" {
		delete(m.queryAugmentSkips, id)
		return nil
	}
	m.queryAugmentSkips[id] = reason
	return nil
}

func (m *mockQueueClaimer) TickHeartbeat(_ context.Context, workerID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.heartbeats == nil {
		m.heartbeats = make(map[string]int)
	}
	m.heartbeats[workerID]++
	return 0, nil
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

func (m *mockRelationshipCreator) BatchCreate(_ context.Context, rels []*model.Relationship) (model.BatchCreateResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return model.BatchCreateResult{}, m.err
	}
	for _, rel := range rels {
		cp := *rel
		m.created = append(m.created, &cp)
	}
	return model.BatchCreateResult{Affected: int64(len(rels))}, nil
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
	childIDsByParent         map[uuid.UUID][]uuid.UUID
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

func (m *mockLineageCreator) FindChildIDsByRelation(_ context.Context, _ uuid.UUID, parentID uuid.UUID, _ []string) ([]uuid.UUID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	if m.childIDsByParent == nil {
		return nil, nil
	}
	return append([]uuid.UUID(nil), m.childIDsByParent[parentID]...), nil
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

func (m *mockVectorWriter) GetByIDs(_ context.Context, _ storage.VectorKind, ids []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	out := make(map[uuid.UUID][]float32, len(ids))
	want := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	// Return the most recently upserted embedding per id when present.
	for _, v := range m.vectors {
		if _, ok := want[v.ID]; ok {
			out[v.ID] = v.Embedding
		}
	}
	return out, nil
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
func (m *mockEmbeddingProvider) Name() string      { return m.name }
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
			{Name: "Alice", Type: "person", Properties: map[string]any{"age": 30}},
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
	pool     *WorkerPool
	reader   *mockMemoryReader
	updater  *mockMemoryUpdater
	creator  *mockMemoryCreator
	queue    *mockQueueClaimer
	entities *mockEntityUpserter
	rels     *mockRelationshipCreator
	lineage  *mockLineageCreator
	tokens   *mockTokenRecorder
	vectors  *mockVectorWriter
	settings *service.SettingsService
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
	// Wire reader into updater so MutateInLock can re-read fresh memory
	// state before invoking the mutator (mirrors production WithMemoryLock
	// + GetByIDTx flow).
	h.updater.reader = h.reader

	// Wrap test provider stubs so the middleware writes token_usage rows
	// to h.tokens on every wrapped call, matches production wiring
	// (registry wrap) without spinning up a registry in unit tests.
	factFn := provider.WrapLLMForTest(constLLM(factLLM), h.tokens)
	entityFn := provider.WrapLLMForTest(constLLM(entityLLM), h.tokens)
	embedFn := provider.WrapEmbeddingForTest(constEmbed(embedProv), h.tokens)

	// Dedicated query-augmentation provider. Query augmentation defaults on,
	// so the worker calls it for every non-empty memory (including dream and
	// already-enriched ones; only fact/entity extraction skip those). Giving
	// it its own provider keeps augment calls off the fact counter so the
	// skip-extraction tests measure fact extraction alone. Returns a valid
	// JSON query array so the phase succeeds rather than failing soft.
	augmentFn := provider.WrapLLMForTest(constLLM(constStringLLM("test-augment", `["query one","query two"]`)), h.tokens)

	// Use an in-memory settings repo so SettingsService.Set actually
	// persists overrides. Settings default to production values; tests that
	// rely on fixed-vector embedders (which would otherwise trip the
	// paraphrase guard on every extracted fact) must opt out explicitly
	// via disableParaphraseGuard(h).
	settingsRepo := newTestSettingsRepo()
	settingsSvc := service.NewSettingsService(settingsRepo)
	h.settings = settingsSvc

	h.pool = NewWorkerPool(
		WorkerConfig{Workers: 1, PollInterval: 10 * time.Millisecond},
		h.reader, h.updater, h.creator, nil, h.queue,
		h.entities, h.rels, h.lineage, h.vectors,
		factFn, entityFn, embedFn,
		nil, augmentFn, nil, settingsSvc, nil, nil,
	)
	return h
}

// testSettingsRepo is a tiny in-memory SettingsRepository used by
// newTestHarness so SettingsService.Set actually persists values into a
// readable store. Required for any test that needs to override built-in
// defaults (e.g., toggling the extracted-fact paraphrase guard).
type testSettingsRepo struct {
	mu   sync.Mutex
	data map[string]*model.Setting
}

func newTestSettingsRepo() *testSettingsRepo {
	return &testSettingsRepo{data: make(map[string]*model.Setting)}
}

func testSettingsKey(key, scope string) string { return scope + "::" + key }

func (r *testSettingsRepo) Get(_ context.Context, key, scope string) (*model.Setting, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.data[testSettingsKey(key, scope)]; ok {
		return s, nil
	}
	return nil, sql.ErrNoRows
}

func (r *testSettingsRepo) Set(_ context.Context, s *model.Setting) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *s
	r.data[testSettingsKey(s.Key, s.Scope)] = &cp
	return nil
}

func (r *testSettingsRepo) Delete(_ context.Context, key, scope string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.data, testSettingsKey(key, scope))
	return nil
}

func (r *testSettingsRepo) ListByScope(_ context.Context, scope string) ([]model.Setting, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]model.Setting, 0)
	for _, v := range r.data {
		if v.Scope == scope {
			out = append(out, *v)
		}
	}
	return out, nil
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

// TestWorkerPool_HeartbeatTicksClaimedJobs drives one heartbeat tick
// directly via tickHeartbeats and asserts TickHeartbeat was called for
// every worker ID. The production ticker loop is exercised in integration
// tests; the unit-test surface is the fan-out logic itself.
func TestWorkerPool_HeartbeatTicksClaimedJobs(t *testing.T) {
	h := newTestHarness(nil, nil, nil)
	h.pool.config.Workers = 3

	workerIDs := []string{"abc-worker-0", "abc-worker-1", "abc-worker-2"}
	h.pool.tickHeartbeats(context.Background(), workerIDs)

	h.queue.mu.Lock()
	defer h.queue.mu.Unlock()
	for _, w := range workerIDs {
		if h.queue.heartbeats[w] < 1 {
			t.Fatalf("expected heartbeat for %s, got %d (full map: %v)", w, h.queue.heartbeats[w], h.queue.heartbeats)
		}
	}
}

// TestWorkerPool_HeartbeatSurvivesPanic verifies the defer recover() in
// tickHeartbeats keeps the goroutine alive when TickHeartbeat panics. The
// next tick (driven manually here) should run normally.
func TestWorkerPool_HeartbeatSurvivesPanic(t *testing.T) {
	h := newTestHarness(nil, nil, nil)
	h.pool.config.Workers = 1

	panicQueue := &panickingHeartbeatQueue{
		mockQueueClaimer: h.queue,
		panicOnce:        true,
	}
	h.pool.queue = panicQueue

	// First tick panics inside the per-worker loop; recover catches it so
	// tickHeartbeats returns normally.
	h.pool.tickHeartbeats(context.Background(), []string{"abc-worker-0"})

	// Second tick must run without panicking.
	h.pool.tickHeartbeats(context.Background(), []string{"abc-worker-0"})

	h.queue.mu.Lock()
	defer h.queue.mu.Unlock()
	if h.queue.heartbeats["abc-worker-0"] < 1 {
		t.Fatalf("expected at least one heartbeat after panic recovery, got %d", h.queue.heartbeats["abc-worker-0"])
	}
}

// TestWorkerPool_EphemeralWorkerIDs asserts that two successive Start
// invocations on independent pools produce non-overlapping worker IDs and
// that each ID matches the "<nonce>-worker-N" format. This is the
// load-bearing correctness property: a restart or sibling instance must
// not be able to inherit another process's claims via heartbeat.
//
// IDs are read via WorkerIDsSnapshot, which Start writes synchronously
// before launching any goroutine. The test does not depend on goroutine
// scheduling and cannot be flaked by CI load.
func TestWorkerPool_EphemeralWorkerIDs(t *testing.T) {
	mint := func() []string {
		h := newTestHarness(nil, nil, nil)
		h.pool.config.Workers = 2
		h.pool.Start()
		defer h.pool.Stop()
		return h.pool.WorkerIDsSnapshot()
	}

	idsA := mint()
	idsB := mint()

	if len(idsA) != 2 {
		t.Fatalf("pool A: expected 2 worker IDs, got %d (%v)", len(idsA), idsA)
	}
	if len(idsB) != 2 {
		t.Fatalf("pool B: expected 2 worker IDs, got %d (%v)", len(idsB), idsB)
	}

	// Format check: every ID must end in "-worker-N" with N in {0,1}.
	checkFormat := func(t *testing.T, label string, ids []string) {
		t.Helper()
		seenSuffix := make(map[string]bool)
		for _, id := range ids {
			if !strings.Contains(id, "-worker-") {
				t.Errorf("%s: id %q lacks -worker- separator (ephemeral prefix missing?)", label, id)
				continue
			}
			parts := strings.Split(id, "-worker-")
			if len(parts) != 2 || parts[0] == "" {
				t.Errorf("%s: id %q does not match <nonce>-worker-N format", label, id)
				continue
			}
			seenSuffix["-worker-"+parts[1]] = true
		}
		if !seenSuffix["-worker-0"] || !seenSuffix["-worker-1"] {
			t.Errorf("%s: expected suffixes -worker-0 and -worker-1, got %v", label, seenSuffix)
		}
	}
	checkFormat(t, "pool A", idsA)
	checkFormat(t, "pool B", idsB)

	// Disjoint check: no ID from pool A may equal any ID from pool B.
	setA := map[string]struct{}{}
	for _, id := range idsA {
		setA[id] = struct{}{}
	}
	for _, id := range idsB {
		if _, hit := setA[id]; hit {
			t.Errorf("ephemeral worker IDs overlapped across pools: %q present in both A=%v and B=%v", id, idsA, idsB)
		}
	}
}

// panickingHeartbeatQueue wraps mockQueueClaimer so TickHeartbeat panics
// on the first call and behaves normally afterwards. Used to verify the
// runHeartbeat goroutine's defer recover() keeps the loop alive.
type panickingHeartbeatQueue struct {
	*mockQueueClaimer
	mu        sync.Mutex
	panicOnce bool
}

func (p *panickingHeartbeatQueue) TickHeartbeat(ctx context.Context, workerID string) (int, error) {
	p.mu.Lock()
	if p.panicOnce {
		p.panicOnce = false
		p.mu.Unlock()
		panic("simulated tick panic")
	}
	p.mu.Unlock()
	return p.mockQueueClaimer.TickHeartbeat(ctx, workerID)
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
	// Fixed-vector embedder would cosine-match every fact to the parent
	// and trigger the paraphrase guard; this test is about the full
	// extraction pipeline, not the guard.
	disableParaphraseGuard(t, h)
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

	// Parent's enriched flag, dim, and metadata land in one MarkEnriched
	// call. Children are no longer embedded inline, so the parent job issues
	// no child UpdateEmbeddingDim calls; each child's own enrichment job
	// stamps its dim via MarkEnriched.
	if len(h.updater.enrichedMarks) != 1 {
		t.Errorf("expected 1 parent MarkEnriched, got %d", len(h.updater.enrichedMarks))
	}
	if mark := h.updater.enrichedMarks[0]; mark.embeddingDim == nil || *mark.embeddingDim != 3 {
		t.Errorf("MarkEnriched embedding_dim = %v, want 3", mark.embeddingDim)
	}
	if len(h.updater.dimUpdates) != 0 {
		t.Errorf("expected 0 child dim updates (children embed via their own jobs), got %d", len(h.updater.dimUpdates))
	}

	// Two child memories (facts).
	if len(h.creator.created) != 2 {
		t.Errorf("expected 2 child memories, got %d", len(h.creator.created))
	}

	// Each child is enqueued for its own augmentation + embedding job.
	if len(h.queue.enqueued) != 2 {
		t.Fatalf("expected 2 child enrichment jobs enqueued, got %d", len(h.queue.enqueued))
	}
	childIDs := make(map[uuid.UUID]bool, len(h.creator.created))
	for _, child := range h.creator.created {
		childIDs[child.ID] = true
	}
	for i, cj := range h.queue.enqueued {
		if !childIDs[cj.MemoryID] {
			t.Errorf("enqueued job %d: MemoryID %s is not one of the created children", i, cj.MemoryID)
		}
		if cj.Status != model.EnrichmentStatusPending {
			t.Errorf("enqueued job %d: status = %q, want %q", i, cj.Status, model.EnrichmentStatusPending)
		}
		if cj.MaxAttempts != 3 {
			t.Errorf("enqueued job %d: max_attempts = %d, want 3", i, cj.MaxAttempts)
		}
	}

	// Child memories must inherit parent source and tags.
	for i, child := range h.creator.created {
		if child.Source == nil || *child.Source != "test-source" {
			t.Errorf("child %d: expected source 'test-source', got %v", i, child.Source)
		}
		// Must contain parent tags
		hasParentTag := slices.Contains(child.Tags, "parent-tag")
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

	// Vectors upserted for the parent memory and each upserted entity only.
	// Extracted-fact children are vectored by their own enrichment jobs, not
	// inline here. Fixture produces 2 entities, so we expect 3 total upserts
	// (parent + 2 entities) and no child memory vectors.
	if len(h.vectors.vectors) != 3 {
		t.Errorf("expected 3 vector upserts (parent + 2 entities), got %d", len(h.vectors.vectors))
	}
	// First upsert must be the parent memory itself, not a fact, guards
	// against the old bug where the first fact's embedding was stored under
	// the parent's ID.
	if len(h.vectors.vectors) > 0 && h.vectors.vectors[0].ID != mem.ID {
		t.Errorf("first vector upsert should target parent memory %s, got %s", mem.ID, h.vectors.vectors[0].ID)
	}
	// No child memory should be embedded inline by the parent job.
	for _, v := range h.vectors.vectors {
		if childIDs[v.ID] {
			t.Errorf("child memory %s was embedded inline; it should be vectored by its own job", v.ID)
		}
	}

	// Token usage: fact_extraction + entity_extraction + query_augment +
	// embedding = 4 records (query augmentation defaults on and runs on the
	// parent before embedding).
	if len(h.tokens.records) != 4 {
		t.Errorf("expected 4 token usage records, got %d", len(h.tokens.records))
	}
}

// TestProcessJob_ChildJobAugmentsAndEmbeds is the direct regression test for
// the fix: an extracted-fact child (Enriched=true) processed through its own
// enrichment job must receive query augmentation and an augmented embedding,
// exactly like a parent or a dream synthesis. With augmentation enabled and
// the fact provider returning a query array (fact extraction is skipped, so
// the only completion call is augmentation), finalizeJob must stamp
// augmented_queries + augmented_embedding_at and mark the query_augmentation
// and embedding steps.
func TestProcessJob_ChildJobAugmentsAndEmbeds(t *testing.T) {
	factLLM := &mockLLMProvider{name: "test-fact", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		// Enriched=true skips fact extraction, so this provider is reached
		// only for query augmentation. Return a JSON query array.
		return &provider.CompletionResponse{Content: `["how does the child fact work","child fact summary"]`, Model: "augment-model"}, nil
	}}
	entityLLM := &mockLLMProvider{name: "test-entity", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{Content: entityJSON(), Model: "entity-model"}, nil
	}}
	embedProv := constEmbedder()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	if err := h.settings.Set(context.Background(), service.SettingQueryAugmentEnabled, "true", "global", nil); err != nil {
		t.Fatalf("enable query augment: %v", err)
	}

	// A child memory: Enriched=true, so ingestion/fact/entity all skip and
	// only augmentation + embedding run.
	mem := testMemory()
	mem.Enriched = true
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected 1 MarkEnriched, got %d", len(h.updater.enrichedMarks))
	}
	mark := h.updater.enrichedMarks[0]
	if len(mark.augmentedQueries) != 2 {
		t.Errorf("expected 2 augmented queries stamped, got %v", mark.augmentedQueries)
	}
	if mark.augmentedEmbeddingAt == nil {
		t.Error("expected augmented_embedding_at to be stamped, got nil")
	}
	if mark.embeddingDim == nil || *mark.embeddingDim != 3 {
		t.Errorf("expected embedding_dim 3, got %v", mark.embeddingDim)
	}
	// The query-augmentation skip reason must be cleared (augmentation ran).
	if reason, ok := h.queue.queryAugmentSkips[job.ID]; ok && reason != "" {
		t.Errorf("expected no query_augment skip reason, got %q", reason)
	}
	steps := h.queue.stepsCompleted[job.ID]
	if !slices.Contains(steps, model.StepQueryAugmentation) {
		t.Errorf("expected query_augmentation step marked, got %v", steps)
	}
	if !slices.Contains(steps, model.StepEmbedding) {
		t.Errorf("expected embedding step marked, got %v", steps)
	}
	// A child is a leaf: it must not itself create children or new lineage.
	if len(h.creator.created) != 0 {
		t.Errorf("child job must not create further children, got %d", len(h.creator.created))
	}
	if len(h.queue.enqueued) != 0 {
		t.Errorf("child job must not enqueue further jobs, got %d", len(h.queue.enqueued))
	}
}

// TestProcessJob_EnqueueChildFailureIsNonFatal verifies that a failure to
// enqueue a child's augmentation job does not fail the parent job: the child
// memory and its lineage are still created, and the parent completes. The
// stranded child is recoverable via the BackfillAugmentation admin path,
// the same guarantee the dream path relies on.
func TestProcessJob_EnqueueChildFailureIsNonFatal(t *testing.T) {
	h := newTestHarness(
		&mockLLMProvider{name: "test-fact", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{Content: factJSON(), Model: "fact-model"}, nil
		}},
		&mockLLMProvider{name: "test-entity", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{Content: entityJSON(), Model: "entity-model"}, nil
		}},
		constEmbedder(),
	)
	disableParaphraseGuard(t, h)
	h.queue.enqueueErr = errors.New("queue insert boom")

	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("enqueue failure must not fail the parent job, got: %v", err)
	}
	if len(h.queue.completed) != 1 || h.queue.completed[0] != job.ID {
		t.Errorf("expected parent job completed despite enqueue failure, got %v", h.queue.completed)
	}
	if len(h.creator.created) != 2 {
		t.Errorf("expected 2 child memories still created, got %d", len(h.creator.created))
	}
	if len(h.lineage.created) != 2 {
		t.Errorf("expected 2 lineage rows still created, got %d", len(h.lineage.created))
	}
	if len(h.queue.enqueued) != 0 {
		t.Errorf("enqueue errored, so nothing should be recorded enqueued, got %d", len(h.queue.enqueued))
	}
}

// TestProcessJob_SuppressedFactNotEnqueued verifies the paraphrase guard's
// interaction with the enqueue path: a fact suppressed as a near-duplicate of
// its parent produces no child memory and therefore no enqueued augmentation
// job. The fixed-vector embedder makes every fact cosine=1 to the parent, so
// with the guard enabled the single extracted fact is suppressed.
func TestProcessJob_SuppressedFactNotEnqueued(t *testing.T) {
	factLLM := &mockLLMProvider{name: "test-fact", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{Content: factJSON(), Model: "fact-model"}, nil
	}}
	entityLLM := &mockLLMProvider{name: "test-entity", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		return &provider.CompletionResponse{Content: `{"entities":[],"relationships":[]}`, Model: "entity-model"}, nil
	}}
	embedProv := constEmbedder()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	// Guard ON (production default, but set explicitly so the test is robust
	// against default changes). Fixed-vector embedder => every fact cosine=1
	// to the parent => suppressed.
	if err := h.settings.Set(context.Background(), service.SettingExtractedFactGuardEnabled, "true", "global", nil); err != nil {
		t.Fatalf("enable guard: %v", err)
	}

	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}
	if len(h.creator.created) != 0 {
		t.Errorf("expected all facts suppressed (0 children), got %d", len(h.creator.created))
	}
	if len(h.queue.enqueued) != 0 {
		t.Errorf("suppressed facts must not be enqueued, got %d", len(h.queue.enqueued))
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

// TestProcessJob_SkipsLLMWhenSourceIsDream pins one half of the dream-
// recursion guard at the worker's skipFact / skipEntity gates. A memory
// with Source=DreamSource MUST skip fact and entity extraction even when
// Enriched=false, because the source check (not the Enriched flag) is the
// readable expression of the recursion-prevention contract. Removing the
// Source==DreamSource clause from worker.go skipFact/skipEntity makes this test fail;
// that is the entire point. Sibling site: phase_ingestion.go (the
// runIngestionDecision source-check early-return covered by
// TestIngestion_SkipsWhenSourceIsDream).
func TestProcessJob_SkipsLLMWhenSourceIsDream(t *testing.T) {
	factLLM, entityLLM, embedProv, factCalls, entityCalls, embedCalls := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	mem := testMemory()
	// Deliberately Enriched=false so the source check is the only signal
	// gating fact/entity extraction. If the worker dropped the origin
	// check from its predicate, this test would see factCalls>0.
	mem.Enriched = false
	mem.Origin = model.OriginDream
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob returned error: %v", err)
	}

	if *factCalls != 0 {
		t.Errorf("dream-source memory must skip fact extraction; got %d fact calls", *factCalls)
	}
	if *entityCalls != 0 {
		t.Errorf("dream-source memory must skip entity extraction; got %d entity calls", *entityCalls)
	}
	if *embedCalls != 1 {
		t.Errorf("dream-source memory must still embed (augmentation runs unchanged); got %d embed calls", *embedCalls)
	}
	if len(h.creator.created) != 0 {
		t.Errorf("dream-source memory must not produce extracted-fact children; got %d created", len(h.creator.created))
	}
	if len(h.lineage.created) != 0 {
		t.Errorf("dream-source memory must not produce new lineage rows; got %d", len(h.lineage.created))
	}
	if len(h.entities.upserted) != 0 {
		t.Errorf("dream-source memory must not upsert entities; got %d", len(h.entities.upserted))
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

// TestProcessJob_BackfillSkipsEntitiesWhenRelationshipsPresent: symmetric
// case for entity extraction. A memory with relationship rows already
// present (source_memory = mem.ID) triggers the relationship probe and
// skips the entity LLM call.
func TestProcessJob_BackfillSkipsEntitiesWhenRelationshipsPresent(t *testing.T) {
	factLLM, entityLLM, embedProv, factCalls, entityCalls, embedCalls := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	// backfillProbeProviders uses fixed embeddings; suppress the
	// paraphrase guard so this test exercises the relationship-probe
	// short-circuit, not the guard.
	disableParaphraseGuard(t, h)
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

// TestProcessJob_BackfillRunsBothWhenNothingPresent: control case. A
// historical memory with mem.Enriched=false, empty steps_completed, and
// no lineage/relationship rows should run both extractions normally.
func TestProcessJob_BackfillRunsBothWhenNothingPresent(t *testing.T) {
	factLLM, entityLLM, embedProv, factCalls, entityCalls, embedCalls := backfillProbeProviders()

	h := newTestHarness(factLLM, entityLLM, embedProv)
	// backfillProbeProviders uses fixed embeddings; suppress the
	// paraphrase guard so this test exercises the absence-probe path
	// and not the guard.
	disableParaphraseGuard(t, h)
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

// TestProcessJob_BackfillSkipsByStepsCompleted: covers the in-flight
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

// guardedTestHarness is now a no-op alias for newTestHarness; the default
// harness already inherits the production-on default for the paraphrase
// guard. Kept for callers that want the explicit, self-documenting name
// when their test exercises the guard. Removing the explicit Set call also
// avoids hiding regressions where a future change accidentally disables
// the guard at the settings layer.
func guardedTestHarness(
	t *testing.T,
	factLLM, entityLLM provider.LLMProvider,
	embedProv provider.EmbeddingProvider,
) *testHarness {
	t.Helper()
	return newTestHarness(factLLM, entityLLM, embedProv)
}

// disableParaphraseGuard turns the extracted-fact paraphrase guard OFF on
// an existing harness. Required for tests that use fixed-vector embedders
// (e.g., the same [0.1, 0.2, 0.3] vector for every input) where every
// extracted fact would otherwise be cosine=1 to the parent and suppressed.
// The opt-out is explicit so a future test author cannot accidentally
// silence the guard.
func disableParaphraseGuard(t *testing.T, h *testHarness) {
	t.Helper()
	if err := h.settings.Set(context.Background(), service.SettingExtractedFactGuardEnabled, "false", "global", nil); err != nil {
		t.Fatalf("disable guard: %v", err)
	}
}

// keyedEmbedder responds with different fixed vectors depending on which
// input string is being embedded. Inputs not in the map fall back to a zero
// vector (cosine 0 against everything).
func keyedEmbedder(name string, byContent map[string][]float32) *mockEmbeddingProvider {
	return &mockEmbeddingProvider{
		name: name,
		respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			out := make([][]float32, len(req.Input))
			for i, in := range req.Input {
				if v, ok := byContent[in]; ok {
					out[i] = v
				} else {
					out[i] = []float32{0, 0, 0}
				}
			}
			return &provider.EmbeddingResponse{Embeddings: out, Model: name + "-model"}, nil
		},
	}
}

func TestProcessJob_FactGuard_SuppressesHighCosineFact(t *testing.T) {
	// LLM emits a single fact whose content is treated as a near-paraphrase
	// of the parent (identical embedding vector). The guard must skip the
	// memCreator.Create and instead absorb the fact's tags into the parent
	// with a LineageExtractedFactSuppressed audit row.
	factBody := `[{"content":"Alice works at Acme","confidence":0.9,"tags":["employment"]}]`
	factLLM := constStringLLM("fact", factBody)

	mem := testMemory()
	embedProv := keyedEmbedder("embed", map[string][]float32{
		mem.Content:           {1, 0, 0},
		"Alice works at Acme": {1, 0, 0},
	})

	h := guardedTestHarness(t, factLLM, noopEntityLLM(), embedProv)
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if got := len(h.creator.created); got != 0 {
		t.Errorf("expected no child memory created (suppressed), got %d", got)
	}

	// Parent must be updated with the suppressed fact's tag merged in.
	if len(h.updater.updated) == 0 {
		t.Fatalf("expected parent memUpdater.Update call to merge tags")
	}
	last := h.updater.updated[len(h.updater.updated)-1]
	foundEmployment := false
	for _, tg := range last.Tags {
		if tg == "employment" {
			foundEmployment = true
		}
	}
	if !foundEmployment {
		t.Errorf("parent tags after merge missing 'employment': %v", last.Tags)
	}

	// Lineage row with the new relation must be written, with memory_id ==
	// parent_id == parent.ID (memory_lineage.memory_id is NOT NULL with FK
	// to memories.id, so the suppression row references the parent on
	// both sides).
	var foundLin *model.MemoryLineage
	for _, lin := range h.lineage.created {
		if lin.Relation == model.LineageExtractedFactSuppressed {
			foundLin = lin
			break
		}
	}
	if foundLin == nil {
		t.Fatalf("expected lineage row with relation %q; got %+v",
			model.LineageExtractedFactSuppressed, h.lineage.created)
	}
	if foundLin.MemoryID != mem.ID {
		t.Errorf("lineage.MemoryID = %s, want parent.ID %s", foundLin.MemoryID, mem.ID)
	}
	if foundLin.ParentID == nil || *foundLin.ParentID != mem.ID {
		t.Errorf("lineage.ParentID should reference parent; got %v", foundLin.ParentID)
	}
}

func TestProcessJob_FactGuard_PassesThroughLowCosineFact(t *testing.T) {
	// LLM emits a fact whose embedding is orthogonal to the parent. The
	// guard must NOT suppress; the child memory is created normally with
	// a LineageExtractedFact row (not the suppression relation).
	factBody := `[{"content":"Acme is headquartered in Cleveland","confidence":0.9,"tags":["location"]}]`
	factLLM := constStringLLM("fact", factBody)

	mem := testMemory()
	embedProv := keyedEmbedder("embed", map[string][]float32{
		mem.Content:                          {1, 0, 0},
		"Acme is headquartered in Cleveland": {0, 1, 0},
	})

	h := guardedTestHarness(t, factLLM, noopEntityLLM(), embedProv)
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if got := len(h.creator.created); got != 1 {
		t.Errorf("expected 1 child memory created, got %d", got)
	}
	for _, lin := range h.lineage.created {
		if lin.Relation == model.LineageExtractedFactSuppressed {
			t.Errorf("unexpected suppression lineage row for low-cosine fact: %+v", lin)
		}
	}
}

func TestProcessJob_FactGuard_LazyEmbedsMissingParent(t *testing.T) {
	// Ingestion-decision is disabled in the harness, so the guard runs
	// without a pre-computed parent embedding. The guard must lazy-embed
	// the parent on the first candidate and reuse the cached value for
	// subsequent candidates in the same job.
	factBody := `[
		{"content":"Alice works at Acme","confidence":0.9,"tags":["employment"]},
		{"content":"Alice works at Acme","confidence":0.9,"tags":["employer"]}
	]`
	factLLM := constStringLLM("fact", factBody)

	mem := testMemory()
	embedProv := keyedEmbedder("embed", map[string][]float32{
		mem.Content:           {1, 0, 0},
		"Alice works at Acme": {1, 0, 0},
	})

	h := guardedTestHarness(t, factLLM, noopEntityLLM(), embedProv)
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// Both facts paraphrase the parent → both suppressed → no children.
	if got := len(h.creator.created); got != 0 {
		t.Errorf("expected no children (both suppressed), got %d", got)
	}
	// Two suppression lineage rows expected: one per suppressed fact.
	suppressed := 0
	for _, lin := range h.lineage.created {
		if lin.Relation == model.LineageExtractedFactSuppressed {
			suppressed++
		}
	}
	if suppressed != 2 {
		t.Errorf("expected 2 suppression lineage rows, got %d", suppressed)
	}
	// Parent must have been re-read at least twice (once per merge call).
	// The mock reader's call count is the easiest probe; just ensure the
	// memUpdater.Update count covers both merges (with the no-delta skip
	// guard absorbing duplicate-tag-set writes).
	if len(h.updater.updated) < 1 {
		t.Errorf("expected parent updates from tag merges; got %d", len(h.updater.updated))
	}
}

func TestProcessJob_FactGuard_FailsOpenOnEmptyEmbedResponse(t *testing.T) {
	// Some providers return a non-error EmbeddingResponse with an empty
	// Embeddings slice on rate-limit fallback or partial failure. The guard
	// must treat that the same as an error: fall through and let the child
	// be created. Otherwise extracted facts vanish silently with no log
	// distinguishing 'paraphrase guard suppressed' from 'guard could not run'.
	factBody := `[{"content":"Alice works at Acme","confidence":0.9,"tags":["employment"]}]`
	factLLM := constStringLLM("fact", factBody)

	mem := testMemory()
	embedProv := &mockEmbeddingProvider{
		name: "embed-empty",
		respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			return &provider.EmbeddingResponse{Embeddings: nil, Model: "empty"}, nil
		},
	}

	h := guardedTestHarness(t, factLLM, noopEntityLLM(), embedProv)
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Logf("processJob surfaced error (expected; downstream embed batch hits the same empty response): %v", err)
	}
	if got := len(h.creator.created); got != 1 {
		t.Errorf("fail-open on empty-embed-response: expected child memory created, got %d", got)
	}
	for _, lin := range h.lineage.created {
		if lin.Relation == model.LineageExtractedFactSuppressed {
			t.Errorf("unexpected suppression lineage row with no candidate embedding: %+v", lin)
		}
	}
}

func TestProcessJob_FactGuard_FailsOpenOnEmbedError(t *testing.T) {
	// Embedder returns an error for the candidate fact's content. The
	// guard must fall through and let the child memory be created so the
	// extracted fact is not silently lost.
	factBody := `[{"content":"Alice works at Acme","confidence":0.9,"tags":["employment"]}]`
	factLLM := constStringLLM("fact", factBody)

	mem := testMemory()
	embedProv := &mockEmbeddingProvider{
		name: "embed-err",
		respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			return nil, fmt.Errorf("embed offline")
		},
	}

	h := guardedTestHarness(t, factLLM, noopEntityLLM(), embedProv)
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		// processJob may still surface the downstream embed batch error
		// (vectors fail to write). The point of THIS test is that
		// child memory creation was attempted despite the embed error
		// in the guard.
		t.Logf("processJob surfaced error (expected): %v", err)
	}
	if got := len(h.creator.created); got != 1 {
		t.Errorf("fail-open: expected child memory created despite embed error, got %d", got)
	}
}

// statefulMemoryStore is a tiny shared-state mock that implements BOTH
// MemoryReader and MemoryUpdater against one map under one mutex, so an
// Update is visible to subsequent GetByID calls. Required for tests that
// exercise the read-modify-write cycle of mergeTagsIntoParent; the
// default mockMemoryReader returns a detached copy from GetByID and so
// would hide lost-update bugs by always serving the original state.
type statefulMemoryStore struct {
	mu      sync.Mutex
	byID    map[uuid.UUID]*model.Memory
	writes  int
	idLocks sync.Map // map[uuid.UUID]*sync.Mutex, mirrors WithMemoryLock per-id serialization
}

func newStatefulMemoryStore() *statefulMemoryStore {
	return &statefulMemoryStore{byID: make(map[uuid.UUID]*model.Memory)}
}

func (s *statefulMemoryStore) put(mem *model.Memory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *mem
	s.byID[mem.ID] = &cp
}

func (s *statefulMemoryStore) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mem, ok := s.byID[id]
	if !ok {
		return nil, fmt.Errorf("memory %s not found", id)
	}
	cp := *mem
	cp.Tags = append([]string(nil), mem.Tags...)
	cp.Metadata = append(json.RawMessage(nil), mem.Metadata...)
	return &cp, nil
}

func (s *statefulMemoryStore) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Memory, 0, len(ids))
	for _, id := range ids {
		if mem, ok := s.byID[id]; ok {
			out = append(out, *mem)
		}
	}
	return out, nil
}

func (s *statefulMemoryStore) Update(_ context.Context, mem *model.Memory) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *mem
	cp.Tags = append([]string(nil), mem.Tags...)
	cp.Metadata = append(json.RawMessage(nil), mem.Metadata...)
	s.byID[mem.ID] = &cp
	s.writes++
	return nil
}

func (s *statefulMemoryStore) UpdateEmbeddingDim(_ context.Context, _ uuid.UUID, _ int) error {
	return nil
}

func (s *statefulMemoryStore) MarkEnriched(_ context.Context, _, _ uuid.UUID, _ *int, _ json.RawMessage, _ []string, _ *time.Time) error {
	return nil
}

func (s *statefulMemoryStore) MarkSupersededBy(_ context.Context, _, _, _ uuid.UUID) error {
	return nil
}

func (s *statefulMemoryStore) MutateInLock(ctx context.Context, id uuid.UUID, mutate func(*model.Memory) (bool, error)) (*model.Memory, error) {
	mxAny, _ := s.idLocks.LoadOrStore(id, &sync.Mutex{})
	mx := mxAny.(*sync.Mutex)
	mx.Lock()
	defer mx.Unlock()

	fresh, err := s.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	write, err := mutate(fresh)
	if err != nil {
		return nil, err
	}
	if !write {
		return fresh, nil
	}
	if err := s.Update(ctx, fresh); err != nil {
		return nil, err
	}
	return fresh, nil
}

func TestMergeTagsIntoParent_ConcurrentMergesAreSerialized(t *testing.T) {
	// Two goroutines call mergeTagsIntoParent on the same parent with
	// disjoint tag sets. With the per-parent lock the resulting parent
	// tags must contain BOTH sets (no lost write); without the lock the
	// last write would clobber the first because Update is a full-row
	// write of the metadata + tags columns.
	store := newStatefulMemoryStore()
	parent := testMemory()
	parent.Tags = []string{"original"}
	parent.Metadata = nil
	store.put(parent)

	// Wire a minimal pool: only the helper's dependencies need real
	// implementations. memCreator, factProvider, etc. stay nil because
	// mergeTagsIntoParent does not reach them.
	lineage := &mockLineageCreator{}
	pool := NewWorkerPool(
		WorkerConfig{Workers: 1, PollInterval: 10 * time.Millisecond},
		store, store, &mockMemoryCreator{}, nil, newMockQueueClaimer(),
		&mockEntityUpserter{}, &mockRelationshipCreator{}, lineage,
		&mockVectorWriter{},
		provider.WrapLLMForTest(constLLM(nil), &mockTokenRecorder{}),
		provider.WrapLLMForTest(constLLM(nil), &mockTokenRecorder{}),
		provider.WrapEmbeddingForTest(constEmbed(nil), &mockTokenRecorder{}),
		nil, nil, nil, service.NewNoopSettingsService(), nil, nil,
	)

	const goroutines = 8
	tagsPerGoroutine := []string{
		"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
	}
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		tag := tagsPerGoroutine[i]
		go func() {
			defer wg.Done()
			if err := pool.mergeTagsIntoParent(
				context.Background(),
				parent, nil,
				[]string{tag}, "suppressed-"+tag, 0.99, "parent", "test",
			); err != nil {
				t.Errorf("merge for tag %q failed: %v", tag, err)
			}
		}()
	}
	wg.Wait()

	got, err := store.GetByID(context.Background(), parent.ID)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	tagSet := map[string]struct{}{}
	for _, tg := range got.Tags {
		tagSet[tg] = struct{}{}
	}
	if _, ok := tagSet["original"]; !ok {
		t.Errorf("original tag lost from parent: %v", got.Tags)
	}
	for _, want := range tagsPerGoroutine {
		if _, ok := tagSet[want]; !ok {
			t.Errorf("lost-update: tag %q missing from final parent tags %v", want, got.Tags)
		}
	}
	if store.writes < goroutines {
		t.Errorf("expected at least %d Update calls (one per goroutine), got %d",
			goroutines, store.writes)
	}
	suppressionRows := 0
	for _, lin := range lineage.created {
		if lin.Relation == model.LineageExtractedFactSuppressed {
			suppressionRows++
		}
	}
	if suppressionRows != goroutines {
		t.Errorf("expected %d suppression lineage rows (one per goroutine), got %d",
			goroutines, suppressionRows)
	}
}

func TestProcessJob_ParaphraseBackfill_SuppressesHighCosineChild(t *testing.T) {
	// A backfill job is enqueued with the JobMarkerOnlyParaphraseGuard
	// sentinel pre-populated in StepsCompleted. The worker must route ONLY
	// to the sweep handler (skip fact/entity/embed), iterate the parent's
	// extracted-fact children, suppress those whose stored embedding is
	// at-or-above threshold to the parent's, and leave the rest untouched.
	h := guardedTestHarness(t, noopFactLLM(), noopEntityLLM(), noopEmbed())

	parent := testMemory()
	dim := 3
	parent.EmbeddingDim = &dim
	parent.Enriched = true
	h.reader.byID[parent.ID] = parent

	highChild := *parent
	highChild.ID = uuid.New()
	highChild.Content = "Alice works at Acme"
	highChild.Tags = []string{"employment"}
	hc := highChild
	h.reader.byID[hc.ID] = &hc

	lowChild := *parent
	lowChild.ID = uuid.New()
	lowChild.Content = "Acme is in Cleveland"
	lowChild.Tags = []string{"location"}
	lc := lowChild
	h.reader.byID[lc.ID] = &lc

	h.lineage.childIDsByParent = map[uuid.UUID][]uuid.UUID{
		parent.ID: {hc.ID, lc.ID},
	}

	// Seed embeddings via the vector writer's existing Upsert pathway so
	// GetByIDs returns them.
	ctx := context.Background()
	if err := h.vectors.Upsert(ctx, storage.VectorKindMemory, parent.ID, parent.NamespaceID, []float32{1, 0, 0}, 3); err != nil {
		t.Fatalf("seed parent embed: %v", err)
	}
	if err := h.vectors.Upsert(ctx, storage.VectorKindMemory, hc.ID, parent.NamespaceID, []float32{1, 0, 0}, 3); err != nil {
		t.Fatalf("seed high embed: %v", err)
	}
	if err := h.vectors.Upsert(ctx, storage.VectorKindMemory, lc.ID, parent.NamespaceID, []float32{0, 1, 0}, 3); err != nil {
		t.Fatalf("seed low embed: %v", err)
	}

	marker, _ := json.Marshal([]string{model.JobMarkerOnlyParaphraseGuard})
	job := testJob(parent.ID, parent.NamespaceID)
	job.StepsCompleted = marker

	if err := h.pool.processJob(ctx, "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// Backfill jobs must NOT create new child memories or run extraction.
	if len(h.creator.created) != 0 {
		t.Errorf("backfill should not create children; got %d", len(h.creator.created))
	}
	// The high-cosine child must be superseded by the parent.
	supersededHigh := false
	for _, m := range h.updater.supersedeMarks {
		if m.oldID == hc.ID && m.newID == parent.ID {
			supersededHigh = true
		}
		if m.oldID == lc.ID {
			t.Errorf("low-cosine child %s was unexpectedly superseded", lc.ID)
		}
	}
	if !supersededHigh {
		t.Errorf("high-cosine child %s was not superseded", hc.ID)
	}
	// The high-cosine child's vector must be purged.
	purgedHigh := false
	for _, id := range h.vectors.deleted {
		if id == hc.ID {
			purgedHigh = true
		}
		if id == lc.ID {
			t.Errorf("low-cosine child's vector was unexpectedly purged")
		}
	}
	if !purgedHigh {
		t.Errorf("high-cosine child's vector was not purged")
	}
	// One LineageExtractedFactSuppressed row must be written.
	suppCount := 0
	for _, lin := range h.lineage.created {
		if lin.Relation == model.LineageExtractedFactSuppressed {
			suppCount++
		}
	}
	if suppCount != 1 {
		t.Errorf("expected 1 suppression lineage row, got %d", suppCount)
	}
	// The parent must have been updated with the suppressed child's tags.
	if len(h.updater.updated) == 0 {
		t.Fatalf("expected parent update from tag merge")
	}
	parentUpdate := h.updater.updated[len(h.updater.updated)-1]
	gotEmployment := false
	for _, tg := range parentUpdate.Tags {
		if tg == "employment" {
			gotEmployment = true
		}
	}
	if !gotEmployment {
		t.Errorf("parent tags after backfill missing 'employment': %v", parentUpdate.Tags)
	}
	// Job must be marked complete.
	if len(h.queue.completed) != 1 || h.queue.completed[0] != job.ID {
		t.Errorf("expected job marked complete, got %v", h.queue.completed)
	}
}

func TestProcessJob_ParaphraseBackfill_SkipsParentWithUnknownDim(t *testing.T) {
	// A parent whose EmbeddingDim was never recorded (legacy row, or a row
	// whose embedding write failed) must NOT cause the sweep to error out.
	// Production vector stores reject dim=0 with a hard error; fetchSingleVector
	// short-circuits to (nil, nil) so the sweep treats the parent as "no
	// embedding available, skip" and the job completes cleanly.
	h := guardedTestHarness(t, noopFactLLM(), noopEntityLLM(), noopEmbed())

	parent := testMemory()
	parent.EmbeddingDim = nil // legacy: dim never recorded
	parent.Enriched = true
	h.reader.byID[parent.ID] = parent

	child := *parent
	child.ID = uuid.New()
	child.Content = "child fact"
	ch := child
	h.reader.byID[ch.ID] = &ch
	h.lineage.childIDsByParent = map[uuid.UUID][]uuid.UUID{
		parent.ID: {ch.ID},
	}

	marker, _ := json.Marshal([]string{model.JobMarkerOnlyParaphraseGuard})
	job := testJob(parent.ID, parent.NamespaceID)
	job.StepsCompleted = marker

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob with nil-dim parent must not error: %v", err)
	}
	if len(h.updater.supersedeMarks) != 0 {
		t.Errorf("expected no supersedes when parent has no embedding, got %v", h.updater.supersedeMarks)
	}
	for _, lin := range h.lineage.created {
		if lin.Relation == model.LineageExtractedFactSuppressed {
			t.Errorf("unexpected suppression lineage row when parent has no embedding: %+v", lin)
		}
	}
	if len(h.queue.completed) != 1 || h.queue.completed[0] != job.ID {
		t.Errorf("expected job marked complete, got %v", h.queue.completed)
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
	if len(h.updater.updated) != 0 || len(h.updater.enrichedMarks) != 0 {
		t.Errorf("memory should not be marked enriched when no providers ran; got %d full Updates and %d MarkEnriched calls",
			len(h.updater.updated), len(h.updater.enrichedMarks))
	}
}

// TestProcessJob_PartialProviders verifies the gate is closed when ANY of
// embedding, fact, or entity is unconfigured, not just all three. This is
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
	for i := range 3 {
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
	for i := range 2 {
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

	// No memory persistence should have happened; finalizeJob is the
	// only thing that persists enriched/embedding_dim, and it must be
	// skipped on the vectorWriteFailed flag. That covers MarkEnriched
	// (enriched flag), UpdateEmbeddingDim (parent + child dims), and the
	// full-row Update used by the ingestion-decision UPDATE path.
	if len(h.updater.enrichedMarks) != 0 {
		t.Errorf("expected 0 MarkEnriched calls after vector batch failure; got %d", len(h.updater.enrichedMarks))
	}
	if len(h.updater.dimUpdates) != 0 {
		t.Errorf("expected 0 dim updates after vector batch failure; got %d", len(h.updater.dimUpdates))
	}
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
