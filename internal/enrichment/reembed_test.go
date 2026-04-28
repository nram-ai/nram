package enrichment

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// reembedFakeRepo is a stub EntityReembedRepo backed by an in-memory list,
// returning entities once and then an empty page (single-pass behavior).
type reembedFakeRepo struct {
	entities    []model.Entity
	called      bool
	dimUpdates  []reembedDimUpdate
}

type reembedDimUpdate struct {
	ids []uuid.UUID
	dim int
}

func (r *reembedFakeRepo) ListAll(_ context.Context, _, _ int) ([]model.Entity, error) {
	if r.called {
		return nil, nil
	}
	r.called = true
	return r.entities, nil
}

func (r *reembedFakeRepo) UpdateEmbeddingDimBatch(_ context.Context, ids []uuid.UUID, dim int) error {
	r.dimUpdates = append(r.dimUpdates, reembedDimUpdate{ids: ids, dim: dim})
	return nil
}

// reembedFakeVectorStore captures vector upsert batches.
type reembedFakeVectorStore struct {
	batches [][]storage.VectorUpsertItem
}

func (s *reembedFakeVectorStore) Upsert(context.Context, storage.VectorKind, uuid.UUID, uuid.UUID, []float32, int) error {
	return nil
}
func (s *reembedFakeVectorStore) UpsertBatch(_ context.Context, items []storage.VectorUpsertItem) error {
	s.batches = append(s.batches, items)
	return nil
}
func (s *reembedFakeVectorStore) Search(context.Context, storage.VectorKind, []float32, uuid.UUID, int, int) ([]storage.VectorSearchResult, error) {
	return nil, nil
}
func (s *reembedFakeVectorStore) GetByIDs(context.Context, storage.VectorKind, []uuid.UUID, int) (map[uuid.UUID][]float32, error) {
	return nil, nil
}
func (s *reembedFakeVectorStore) Delete(context.Context, storage.VectorKind, uuid.UUID) error {
	return nil
}
func (s *reembedFakeVectorStore) TruncateAllVectors(context.Context) error { return nil }
func (s *reembedFakeVectorStore) Ping(context.Context) error               { return nil }

// reembedCapturingEmbedder produces deterministic 4-dim vectors and records
// every ctx it receives so the test can assert that operation/namespace_id
// were stamped before the wrapped middleware runs.
type reembedCapturingEmbedder struct {
	mu        sync.Mutex
	calls     []capturedCall
}

type capturedCall struct {
	op           provider.Operation
	hasOp        bool
	namespaceID  uuid.UUID
	inputCount   int
}

func (e *reembedCapturingEmbedder) Embed(ctx context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	op, has := provider.OperationFromContext(ctx)
	e.mu.Lock()
	e.calls = append(e.calls, capturedCall{
		op:          op,
		hasOp:       has,
		namespaceID: provider.NamespaceIDFromContext(ctx),
		inputCount:  len(req.Input),
	})
	e.mu.Unlock()

	embeddings := make([][]float32, len(req.Input))
	for i := range req.Input {
		embeddings[i] = []float32{0.1, 0.2, 0.3, 0.4}
	}
	return &provider.EmbeddingResponse{
		Embeddings: embeddings,
		Model:      "stub-embed",
		Usage:      provider.TokenUsage{PromptTokens: 5 * len(req.Input), TotalTokens: 5 * len(req.Input)},
	}, nil
}
func (e *reembedCapturingEmbedder) Name() string      { return "stub-embedder" }
func (e *reembedCapturingEmbedder) Dimensions() []int { return []int{4} }

// reembedRecorder collects every TokenUsage row written by the middleware
// so the test can assert on what landed in token_usage.
type reembedRecorder struct {
	mu   sync.Mutex
	rows []*model.TokenUsage
}

func (r *reembedRecorder) Record(_ context.Context, u *model.TokenUsage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows = append(r.rows, u)
	return nil
}

// TestReembedAllEntities_RecordsTokenUsage verifies the contract added in
// step 8 of the audit plan: ReembedAllEntities, when invoked with an
// embedder wrapped in UsageRecordingProvider middleware, lands a token_usage
// row per chunked Embed call with operation=embedding and namespace_id set
// to the first entity's namespace in the batch.
func TestReembedAllEntities_RecordsTokenUsage(t *testing.T) {
	nsA := uuid.New()
	repo := &reembedFakeRepo{
		entities: []model.Entity{
			{ID: uuid.New(), NamespaceID: nsA, Canonical: "alice"},
			{ID: uuid.New(), NamespaceID: nsA, Canonical: "bob"},
			{ID: uuid.New(), NamespaceID: nsA, Canonical: "carol"},
		},
	}
	vectorStore := &reembedFakeVectorStore{}
	inner := &reembedCapturingEmbedder{}
	recorder := &reembedRecorder{}
	wrapped := provider.NewUsageRecordingEmbedding(inner, recorder, nil)

	result, err := ReembedAllEntities(context.Background(), repo, vectorStore, wrapped)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Reembedded != 3 {
		t.Fatalf("expected 3 reembedded, got %d", result.Reembedded)
	}

	// Embedder saw the stamped operation and namespace_id.
	if len(inner.calls) != 1 {
		t.Fatalf("expected 1 embed call (single chunk), got %d", len(inner.calls))
	}
	c := inner.calls[0]
	if !c.hasOp || c.op != provider.OperationEmbedding {
		t.Errorf("embedder ctx missing OperationEmbedding (hasOp=%v op=%q)", c.hasOp, c.op)
	}
	if c.namespaceID != nsA {
		t.Errorf("embedder ctx namespace: got %v want %v", c.namespaceID, nsA)
	}
	if c.inputCount != 3 {
		t.Errorf("embed input count: got %d want 3", c.inputCount)
	}

	// Recorder got exactly one token_usage row, attributed to nsA, with the
	// embedding operation and provider tokens propagated.
	if len(recorder.rows) != 1 {
		t.Fatalf("expected 1 token_usage row, got %d", len(recorder.rows))
	}
	row := recorder.rows[0]
	if row.Operation != string(provider.OperationEmbedding) {
		t.Errorf("row.Operation: got %q want %q", row.Operation, provider.OperationEmbedding)
	}
	if row.NamespaceID != nsA {
		t.Errorf("row.NamespaceID: got %v want %v", row.NamespaceID, nsA)
	}
	if row.Provider != "stub-embedder" {
		t.Errorf("row.Provider: got %q", row.Provider)
	}
	if row.Model != "stub-embed" {
		t.Errorf("row.Model: got %q", row.Model)
	}
	if row.TokensInput != 15 { // 5 tokens × 3 inputs
		t.Errorf("row.TokensInput: got %d want 15", row.TokensInput)
	}
	if !row.Success {
		t.Error("expected Success=true")
	}
	if row.LatencyMs == nil {
		t.Error("expected LatencyMs to be populated by middleware")
	}
}
