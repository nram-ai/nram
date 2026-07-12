package enrichment

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// recordingFacetStore satisfies VectorWriter and storage.FacetVectorStore,
// recording UpsertFacets calls so writeMemoryFacets can be asserted directly.
// stored backs GetByIDs so the facet-only sweep (runMultiVectorFacetSweep),
// which reuses the stored facet-0 vector instead of re-embedding, can be tested
// against a known vector. A nil stored map means GetByIDs returns nothing,
// matching the "no stored vector" path.
type recordingFacetStore struct {
	facetCalls map[uuid.UUID][][]float32
	stored     map[uuid.UUID][]float32
}

func newRecordingFacetStore() *recordingFacetStore {
	return &recordingFacetStore{facetCalls: map[uuid.UUID][][]float32{}}
}

func (s *recordingFacetStore) Upsert(context.Context, storage.VectorKind, uuid.UUID, uuid.UUID, []float32, int) error {
	return nil
}
func (s *recordingFacetStore) UpsertBatch(context.Context, []storage.VectorUpsertItem) error {
	return nil
}
func (s *recordingFacetStore) Delete(context.Context, storage.VectorKind, uuid.UUID) error {
	return nil
}
func (s *recordingFacetStore) GetByIDs(_ context.Context, _ storage.VectorKind, ids []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	out := map[uuid.UUID][]float32{}
	for _, id := range ids {
		if v, ok := s.stored[id]; ok {
			out[id] = v
		}
	}
	return out, nil
}
func (s *recordingFacetStore) Search(context.Context, storage.VectorKind, []float32, uuid.UUID, int, int) ([]storage.VectorSearchResult, error) {
	return nil, nil
}
func (s *recordingFacetStore) UpsertFacets(_ context.Context, memoryID, _ uuid.UUID, _ int, facets [][]float32) error {
	s.facetCalls[memoryID] = facets
	return nil
}

func newMultiVectorTestPool(t *testing.T, store *recordingFacetStore, enabled bool) *WorkerPool {
	t.Helper()
	svc := service.NewSettingsService(newTestSettingsRepo())
	// Set the flag explicitly (not relying on the registered default, which is
	// now true) so the disabled-path tests stay meaningful regardless of the
	// default.
	val := "false"
	if enabled {
		val = "true"
	}
	if err := svc.Set(context.Background(), service.SettingMultiVectorEnabled, val, "global", nil); err != nil {
		t.Fatalf("set multi_vector.enabled: %v", err)
	}
	emb := &fakeFacetEmbedder{dim: 8, axisFor: func(s string) int {
		if strings.Contains(s, "PRICE") {
			return 1
		}
		return 5
	}}
	return &WorkerPool{
		settings:      svc,
		vectorStore:   store,
		embedProvider: func() provider.EmbeddingProvider { return emb },
		memUpdater:    &mockMemoryUpdater{},
	}
}

// facetStateMarksOf returns the facet-state stamps recorded by a pool built via
// newMultiVectorTestPool (whose memUpdater is a *mockMemoryUpdater).
func facetStateMarksOf(t *testing.T, pool *WorkerPool) []facetStateMark {
	t.Helper()
	mu, ok := pool.memUpdater.(*mockMemoryUpdater)
	if !ok {
		t.Fatalf("pool.memUpdater is %T, want *mockMemoryUpdater", pool.memUpdater)
	}
	return mu.facetStateMarks
}

func TestWriteMemoryFacets_WritesFacetsForMultiTopicMemory(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newMultiVectorTestPool(t, store, true)

	memID := uuid.New()
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	pending := &pendingJob{
		job:        &model.EnrichmentJob{ID: uuid.New()},
		mem:        &model.Memory{ID: memID, NamespaceID: uuid.New(), Content: "PRICE one. PRICE two. DEPLOY one."},
		embedStart: 0,
	}

	pool.writeMemoryFacets(context.Background(), []*pendingJob{pending}, [][]float32{whole})

	facets, ok := store.facetCalls[memID]
	if !ok {
		t.Fatal("UpsertFacets was not called for a multi-topic memory with multi_vector enabled")
	}
	if len(facets) < 3 {
		t.Fatalf("expected facet 0 + 2 topic facets, got %d", len(facets))
	}
}

// ctxCapturingEmbedder records the context of its last Embed call so a test can
// assert what attribution the facet sentence-embed carries. It returns one
// distinct-axis vector per input so clustering yields multiple facets.
type ctxCapturingEmbedder struct {
	dim         int
	gotMemory   *uuid.UUID
	gotNS       uuid.UUID
	gotOp       provider.Operation
	gotOpExists bool
}

func (e *ctxCapturingEmbedder) Name() string      { return "ctx-capture" }
func (e *ctxCapturingEmbedder) Dimensions() []int { return []int{e.dim} }
func (e *ctxCapturingEmbedder) Embed(ctx context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	e.gotMemory = provider.MemoryIDFromContext(ctx)
	e.gotNS = provider.NamespaceIDFromContext(ctx)
	e.gotOp, e.gotOpExists = provider.OperationFromContext(ctx)
	out := make([][]float32, len(req.Input))
	for i := range req.Input {
		v := make([]float32, e.dim)
		v[i%e.dim] = 1
		out[i] = v
	}
	return &provider.EmbeddingResponse{Embeddings: out, Model: "ctx-capture"}, nil
}

// TestWriteMemoryFacets_AttributesEmbedToMemory guards token-usage attribution:
// the facet sentence-embed must carry the memory's namespace + id and the
// embedding operation, so the UsageRecordingEmbedding middleware writes the
// token_usage row against the right memory rather than with null ownership.
func TestWriteMemoryFacets_AttributesEmbedToMemory(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newMultiVectorTestPool(t, store, true)
	emb := &ctxCapturingEmbedder{dim: 8}
	pool.embedProvider = func() provider.EmbeddingProvider { return emb }

	memID, nsID := uuid.New(), uuid.New()
	whole := make([]float32, 8)
	whole[0] = 1
	pending := &pendingJob{
		job:        &model.EnrichmentJob{ID: uuid.New()},
		mem:        &model.Memory{ID: memID, NamespaceID: nsID, Content: "Topic alpha sentence. Different beta sentence. Third gamma sentence."},
		embedStart: 0,
	}

	pool.writeMemoryFacets(context.Background(), []*pendingJob{pending}, [][]float32{whole})

	if emb.gotMemory == nil || *emb.gotMemory != memID {
		t.Errorf("facet embed memory attribution = %v, want %v", emb.gotMemory, memID)
	}
	if emb.gotNS != nsID {
		t.Errorf("facet embed namespace attribution = %v, want %v", emb.gotNS, nsID)
	}
	if !emb.gotOpExists || emb.gotOp != provider.OperationFacetEmbedding {
		t.Errorf("facet embed operation = %q (exists=%v), want %q", emb.gotOp, emb.gotOpExists, provider.OperationFacetEmbedding)
	}
}

func TestFacetEmbedSemaphore_SizesFromSetting(t *testing.T) {
	store := newRecordingFacetStore()
	ctx := context.Background()

	// Default: capacity from the registered default (4).
	def := newMultiVectorTestPool(t, store, true)
	if got := cap(def.facetEmbedSemaphore(ctx)); got != 4 {
		t.Errorf("default facet embed concurrency = %d, want 4", got)
	}

	// Operator override is honoured.
	custom := newMultiVectorTestPool(t, store, true)
	if err := custom.settings.Set(ctx, service.SettingMultiVectorEmbedConcurrency, "2", "global", nil); err != nil {
		t.Fatalf("set embed_concurrency: %v", err)
	}
	if got := cap(custom.facetEmbedSemaphore(ctx)); got != 2 {
		t.Errorf("overridden facet embed concurrency = %d, want 2", got)
	}

	// The limiter is sized once: a later setting change does not resize it.
	if err := custom.settings.Set(ctx, service.SettingMultiVectorEmbedConcurrency, "9", "global", nil); err != nil {
		t.Fatalf("set embed_concurrency again: %v", err)
	}
	if got := cap(custom.facetEmbedSemaphore(ctx)); got != 2 {
		t.Errorf("semaphore resized after first use = %d, want stable 2", got)
	}
}

func TestWriteMemoryFacets_DisabledIsNoOp(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newMultiVectorTestPool(t, store, false)

	memID := uuid.New()
	whole := make([]float32, 8)
	whole[0] = 1
	pending := &pendingJob{
		job:        &model.EnrichmentJob{ID: uuid.New()},
		mem:        &model.Memory{ID: memID, NamespaceID: uuid.New(), Content: "PRICE one. DEPLOY two."},
		embedStart: 0,
	}

	pool.writeMemoryFacets(context.Background(), []*pendingJob{pending}, [][]float32{whole})

	if len(store.facetCalls) != 0 {
		t.Fatalf("expected no facet writes when multi_vector is disabled, got %d", len(store.facetCalls))
	}
}

func TestWriteMemoryFacets_SingleTopicWritesNoFacets(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newMultiVectorTestPool(t, store, true)

	memID := uuid.New()
	whole := make([]float32, 8)
	whole[0] = 1
	// All sentences share the PRICE topic -> one cluster -> facet 0 only, so
	// UpsertFacets is skipped (the batch already wrote facet 0).
	pending := &pendingJob{
		job:        &model.EnrichmentJob{ID: uuid.New()},
		mem:        &model.Memory{ID: memID, NamespaceID: uuid.New(), Content: "PRICE one. PRICE two. PRICE three."},
		embedStart: 0,
	}

	pool.writeMemoryFacets(context.Background(), []*pendingJob{pending}, [][]float32{whole})

	if _, ok := store.facetCalls[memID]; ok {
		t.Fatal("UpsertFacets should be skipped for a single-topic memory")
	}
}

// TestWriteMemoryFacets_SingleTopicClearsStalePriorFacets guards the stale-facet
// leak fix: a memory that PREVIOUSLY produced topic facets (FacetCount > 1) but
// now re-clusters to a single topic must still call UpsertFacets — which deletes
// the whole facet set and rewrites just facet 0 — so its old facet_id>0 rows
// cannot linger in a different vector space (e.g. after a same-dimension
// embedding-model switch) and pollute recall. The base whole-memory Upsert only
// rewrites facet 0 and never clears topic rows, so without this the stale topics
// would survive while facet_count is stamped back to 1.
func TestWriteMemoryFacets_SingleTopicClearsStalePriorFacets(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newMultiVectorTestPool(t, store, true)

	memID := uuid.New()
	whole := make([]float32, 8)
	whole[0] = 1
	priorFacetCount := 3
	pending := &pendingJob{
		job: &model.EnrichmentJob{ID: uuid.New()},
		mem: &model.Memory{
			ID:          memID,
			NamespaceID: uuid.New(),
			Content:     "PRICE one. PRICE two. PRICE three.", // single topic now
			FacetCount:  &priorFacetCount,                     // was multi-topic before
		},
		embedStart: 0,
	}

	pool.writeMemoryFacets(context.Background(), []*pendingJob{pending}, [][]float32{whole})

	facets, ok := store.facetCalls[memID]
	if !ok {
		t.Fatal("UpsertFacets must be called to clear stale topic facets when a previously multi-topic memory re-clusters to a single topic")
	}
	if len(facets) != 1 {
		t.Fatalf("clear-on-shrink must rewrite exactly facet 0 (len 1), got %d facets", len(facets))
	}
	// facet_count must be re-stamped to 1 to match the now-single facet set.
	marks := facetStateMarksOf(t, pool)
	if len(marks) != 1 || marks[0].facetCount != 1 {
		t.Fatalf("facet state stamps = %+v, want exactly one stamp with count 1", marks)
	}
}

// failIfCalledLLM is a provider.LLMProvider that fails the test if Complete is
// invoked. Wired into the pool's fact/entity slots so the facet-only sweep can
// prove it makes no LLM (SGLang) calls, only facet sentence-embeds.
type failIfCalledLLM struct{ t *testing.T }

func (l *failIfCalledLLM) Complete(context.Context, *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	l.t.Helper()
	l.t.Fatal("facet-only backfill must not call any LLM provider")
	return nil, nil
}
func (l *failIfCalledLLM) Name() string     { return "fail-if-called" }
func (l *failIfCalledLLM) Models() []string { return nil }

// newSweepTestPool builds a pool for runMultiVectorFacetSweep with LLM slots
// that fail the test if touched, so any SGLang call surfaces as a failure.
func newSweepTestPool(t *testing.T, store *recordingFacetStore, enabled bool) *WorkerPool {
	t.Helper()
	pool := newMultiVectorTestPool(t, store, enabled)
	llm := &failIfCalledLLM{t: t}
	pool.factProvider = func() provider.LLMProvider { return llm }
	pool.entityProvider = func() provider.LLMProvider { return llm }
	return pool
}

func TestRunMultiVectorFacetSweep_WritesFacetsFromStoredVector(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newSweepTestPool(t, store, true)

	memID, nsID := uuid.New(), uuid.New()
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	store.stored = map[uuid.UUID][]float32{memID: whole}

	mem := &model.Memory{ID: memID, NamespaceID: nsID, EmbeddingDim: &dim,
		Content: "PRICE one. PRICE two. DEPLOY one."}
	job := &model.EnrichmentJob{ID: uuid.New(), MemoryID: memID, NamespaceID: nsID}

	if err := pool.runMultiVectorFacetSweep(context.Background(), job, mem); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	facets, ok := store.facetCalls[memID]
	if !ok {
		t.Fatal("UpsertFacets was not called for a multi-topic memory with a stored vector")
	}
	if len(facets) < 3 {
		t.Fatalf("expected facet 0 + 2 topic facets, got %d", len(facets))
	}
	// Facet 0 must be the reused stored whole-memory vector, proving the sweep
	// did not re-embed the whole memory.
	if len(facets[0]) != dim || facets[0][0] != 1 {
		t.Errorf("facet 0 = %v, want the stored whole-memory vector", facets[0])
	}
	// Facet state stamped so the memory drops out of the candidate set.
	marks := facetStateMarksOf(t, pool)
	if len(marks) != 1 || marks[0].id != memID || marks[0].facetCount != len(facets) {
		t.Errorf("facet-state marks = %+v, want one mark for %s with count %d", marks, memID, len(facets))
	}
}

func TestRunMultiVectorFacetSweep_SkipsWhenNoStoredVector(t *testing.T) {
	store := newRecordingFacetStore() // stored is nil -> GetByIDs returns nothing
	pool := newSweepTestPool(t, store, true)

	memID, nsID := uuid.New(), uuid.New()
	dim := 8
	mem := &model.Memory{ID: memID, NamespaceID: nsID, EmbeddingDim: &dim,
		Content: "PRICE one. DEPLOY two. AUDIT three."}
	job := &model.EnrichmentJob{ID: uuid.New(), MemoryID: memID, NamespaceID: nsID}

	if err := pool.runMultiVectorFacetSweep(context.Background(), job, mem); err != nil {
		t.Fatalf("sweep should not error when the stored vector is absent: %v", err)
	}
	if len(store.facetCalls) != 0 {
		t.Fatalf("expected no facet writes when the stored vector is absent, got %d", len(store.facetCalls))
	}
	// Not stamped: a vector-less memory stays a candidate (it belongs to the
	// embedding/augmentation backfill, not this one).
	if marks := facetStateMarksOf(t, pool); len(marks) != 0 {
		t.Errorf("expected no facet-state stamp when the stored vector is absent, got %+v", marks)
	}
}

func TestRunMultiVectorFacetSweep_SingleTopicStampsCountOne(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newSweepTestPool(t, store, true)

	memID, nsID := uuid.New(), uuid.New()
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	store.stored = map[uuid.UUID][]float32{memID: whole}
	// All sentences share the PRICE topic -> one cluster -> facet 0 only, so no
	// topic facets are upserted but the memory is still stamped (count 1) so it
	// drops out of the candidate set.
	mem := &model.Memory{ID: memID, NamespaceID: nsID, EmbeddingDim: &dim,
		Content: "PRICE one. PRICE two. PRICE three."}
	job := &model.EnrichmentJob{ID: uuid.New(), MemoryID: memID, NamespaceID: nsID}

	if err := pool.runMultiVectorFacetSweep(context.Background(), job, mem); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, ok := store.facetCalls[memID]; ok {
		t.Fatal("single-topic memory must not upsert topic facets")
	}
	marks := facetStateMarksOf(t, pool)
	if len(marks) != 1 || marks[0].facetCount != 1 {
		t.Errorf("facet-state marks = %+v, want one mark with count 1", marks)
	}
}

func TestRunMultiVectorFacetSweep_SkipsWhenDimUnknown(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newSweepTestPool(t, store, true)

	memID, nsID := uuid.New(), uuid.New()
	whole := make([]float32, 8)
	whole[0] = 1
	store.stored = map[uuid.UUID][]float32{memID: whole}
	// EmbeddingDim nil: fetchSingleVector returns (nil, nil) rather than
	// forwarding dim=0 to the store, so the sweep is a clean skip.
	mem := &model.Memory{ID: memID, NamespaceID: nsID, Content: "PRICE one. DEPLOY two."}
	job := &model.EnrichmentJob{ID: uuid.New(), MemoryID: memID, NamespaceID: nsID}

	if err := pool.runMultiVectorFacetSweep(context.Background(), job, mem); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(store.facetCalls) != 0 {
		t.Fatalf("expected no facet writes when EmbeddingDim is unknown, got %d", len(store.facetCalls))
	}
}

func TestRunMultiVectorFacetSweep_Idempotent(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newSweepTestPool(t, store, true)

	memID, nsID := uuid.New(), uuid.New()
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	store.stored = map[uuid.UUID][]float32{memID: whole}
	mem := &model.Memory{ID: memID, NamespaceID: nsID, EmbeddingDim: &dim,
		Content: "PRICE one. PRICE two. DEPLOY one."}
	job := &model.EnrichmentJob{ID: uuid.New(), MemoryID: memID, NamespaceID: nsID}

	if err := pool.runMultiVectorFacetSweep(context.Background(), job, mem); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	first := len(store.facetCalls[memID])
	if err := pool.runMultiVectorFacetSweep(context.Background(), job, mem); err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	// UpsertFacets replaces (the recording store overwrites the entry), so the
	// facet set is the same shape, never doubled.
	if got := len(store.facetCalls[memID]); got != first {
		t.Fatalf("re-run facet count = %d, want stable %d (UpsertFacets must replace)", got, first)
	}
}

// TestProcessJob_MultiVectorMarkerRoutesToSweepNoLLM is the integration-level
// guard for the 0.7.1 behavior change: a backfill job carrying the
// JobMarkerOnlyMultiVector sentinel must route through runPreEmbed straight to
// the lean facet sweep, completing the job and writing facets WITHOUT running
// the SGLang pipeline (ingestion-decision, fact/entity extraction, query
// augmentation) and WITHOUT re-embedding the whole memory.
func TestProcessJob_MultiVectorMarkerRoutesToSweepNoLLM(t *testing.T) {
	// Fact and entity providers fail the test if the worker calls them, so any
	// SGLang call on the marker path surfaces as a failure.
	failLLM := &mockLLMProvider{name: "fail", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		t.Error("multi-vector marker job must not call any LLM provider")
		return &provider.CompletionResponse{Content: "[]", Model: "fail"}, nil
	}}
	h := newTestHarness(failLLM, failLLM, constEmbedder())
	if err := h.settings.Set(context.Background(), service.SettingMultiVectorEnabled, "true", "global", nil); err != nil {
		t.Fatalf("enable multi_vector: %v", err)
	}

	memID, nsID := uuid.New(), uuid.New()
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	store := newRecordingFacetStore()
	store.stored = map[uuid.UUID][]float32{memID: whole}
	// Inject a facet-capable store + multi-topic embedder so the routed sweep
	// can actually write facets.
	h.pool.vectorStore = store
	h.pool.embedProvider = func() provider.EmbeddingProvider {
		return &fakeFacetEmbedder{dim: dim, axisFor: func(s string) int {
			if strings.Contains(s, "PRICE") {
				return 1
			}
			return 5
		}}
	}

	mem := testMemory()
	mem.ID = memID
	mem.NamespaceID = nsID
	mem.Enriched = true
	mem.EmbeddingDim = &dim
	mem.Content = "PRICE one. PRICE two. DEPLOY one."
	h.reader.byID[mem.ID] = mem

	job := testJob(mem.ID, mem.NamespaceID)
	marker, err := json.Marshal([]string{model.JobMarkerOnlyMultiVector})
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}
	job.StepsCompleted = marker

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// Job completed via the sweep path.
	if len(h.queue.completed) != 1 || h.queue.completed[0] != job.ID {
		t.Fatalf("expected marker job completed, got %v", h.queue.completed)
	}
	// StepMultiVectorFacets marked so a retry is a no-op.
	steps := h.queue.stepsCompleted[job.ID]
	if !slices.Contains(steps, model.StepMultiVectorFacets) {
		t.Errorf("expected %q step marked, got %v", model.StepMultiVectorFacets, steps)
	}
	// Facets written, reusing the stored facet-0 vector.
	facets, ok := store.facetCalls[memID]
	if !ok || len(facets) < 3 {
		t.Fatalf("expected facets written from the stored vector, got %v (ok=%v)", facets, ok)
	}
	if len(facets[0]) != dim || facets[0][0] != 1 {
		t.Errorf("facet 0 = %v, want the reused stored whole-memory vector", facets[0])
	}
	// The SGLang pipeline never ran: finalizeJob (and thus MarkEnriched) is
	// unreached, and none of the full-pipeline steps are marked.
	if len(h.updater.enrichedMarks) != 0 {
		t.Errorf("marker job must not run finalizeJob/MarkEnriched, got %d", len(h.updater.enrichedMarks))
	}
	if slices.Contains(steps, model.StepQueryAugmentation) || slices.Contains(steps, model.StepEmbedding) ||
		slices.Contains(steps, model.StepFactExtraction) || slices.Contains(steps, model.StepEntityExtraction) {
		t.Errorf("marker job must not run any full-pipeline step, got %v", steps)
	}
	// Facet state stamped through the routed sweep so the memory leaves the
	// backfill candidate set.
	if len(h.updater.facetStateMarks) != 1 || h.updater.facetStateMarks[0].id != memID {
		t.Errorf("expected one facet-state stamp for %s, got %+v", memID, h.updater.facetStateMarks)
	}
}

func TestRunMultiVectorFacetSweep_DisabledIsNoOp(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newSweepTestPool(t, store, false)

	memID, nsID := uuid.New(), uuid.New()
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	store.stored = map[uuid.UUID][]float32{memID: whole}
	mem := &model.Memory{ID: memID, NamespaceID: nsID, EmbeddingDim: &dim,
		Content: "PRICE one. PRICE two. DEPLOY one."}
	job := &model.EnrichmentJob{ID: uuid.New(), MemoryID: memID, NamespaceID: nsID}

	if err := pool.runMultiVectorFacetSweep(context.Background(), job, mem); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(store.facetCalls) != 0 {
		t.Fatalf("expected no facet writes when multi_vector is disabled, got %d", len(store.facetCalls))
	}
}
