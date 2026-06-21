package enrichment

import (
	"context"
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
type recordingFacetStore struct {
	facetCalls map[uuid.UUID][][]float32
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
func (s *recordingFacetStore) GetByIDs(context.Context, storage.VectorKind, []uuid.UUID, int) (map[uuid.UUID][]float32, error) {
	return map[uuid.UUID][]float32{}, nil
}
func (s *recordingFacetStore) UpsertFacets(_ context.Context, memoryID, _ uuid.UUID, _ int, facets [][]float32) error {
	s.facetCalls[memoryID] = facets
	return nil
}

func newMultiVectorTestPool(t *testing.T, store *recordingFacetStore, enabled bool) *WorkerPool {
	t.Helper()
	svc := service.NewSettingsService(newTestSettingsRepo())
	if enabled {
		if err := svc.Set(context.Background(), service.SettingMultiVectorEnabled, "true", "global", nil); err != nil {
			t.Fatalf("set multi_vector.enabled: %v", err)
		}
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
	}
}

func TestWriteMemoryFacets_WritesFacetsForMultiTopicMemory(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newMultiVectorTestPool(t, store, true)

	memID := uuid.New()
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	pending := &pendingJob{
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

func TestWriteMemoryFacets_DisabledByDefault(t *testing.T) {
	store := newRecordingFacetStore()
	pool := newMultiVectorTestPool(t, store, false)

	memID := uuid.New()
	whole := make([]float32, 8)
	whole[0] = 1
	pending := &pendingJob{
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
		mem:        &model.Memory{ID: memID, NamespaceID: uuid.New(), Content: "PRICE one. PRICE two. PRICE three."},
		embedStart: 0,
	}

	pool.writeMemoryFacets(context.Background(), []*pendingJob{pending}, [][]float32{whole})

	if _, ok := store.facetCalls[memID]; ok {
		t.Fatal("UpsertFacets should be skipped for a single-topic memory")
	}
}
