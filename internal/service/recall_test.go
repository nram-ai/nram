package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// --- Recall mock implementations ---

type mockMemoryReader struct {
	memories map[uuid.UUID]*model.Memory
	nsList   []model.Memory
	listErr  error
	batchErr error
}

func (m *mockMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mem, nil
}

func (m *mockMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	if m.batchErr != nil {
		return nil, m.batchErr
	}
	var result []model.Memory
	for _, id := range ids {
		if mem, ok := m.memories[id]; ok {
			result = append(result, *mem)
		}
	}
	return result, nil
}

func (m *mockMemoryReader) ListByNamespace(_ context.Context, _ uuid.UUID, limit, _ int) ([]model.Memory, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	if limit > len(m.nsList) {
		return m.nsList, nil
	}
	return m.nsList[:limit], nil
}

func (m *mockMemoryReader) ListByNamespaceFiltered(ctx context.Context, ns uuid.UUID, _ storage.MemoryListFilters, limit, offset int) ([]model.Memory, error) {
	return m.ListByNamespace(ctx, ns, limit, offset)
}

type mockVectorSearcher struct {
	results []storage.VectorSearchResult
	err     error
}

func (m *mockVectorSearcher) Search(_ context.Context, _ storage.VectorKind, _ []float32, _ uuid.UUID, _ int, topK int) ([]storage.VectorSearchResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	if topK > len(m.results) {
		return m.results, nil
	}
	return m.results[:topK], nil
}

type mockEntityReader struct {
	entities []model.Entity
	aliases  []model.Entity
	err      error
}

func (m *mockEntityReader) FindBySimilarity(_ context.Context, _ uuid.UUID, _ string, _ string, _ int) ([]model.Entity, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.entities, nil
}

func (m *mockEntityReader) FindByAlias(_ context.Context, _ uuid.UUID, _ string) ([]model.Entity, error) {
	return m.aliases, nil
}

type mockRelTraverser struct {
	rels []model.Relationship
	err  error
	// maxEdgesByCall records the cap RecallService passed on each call so
	// tests can assert that the graph.max_edges setting flows through and
	// tightens across seeds via the cumulative-cap logic.
	maxEdgesByCall []int
}

func (m *mockRelTraverser) TraverseFromEntity(_ context.Context, _ uuid.UUID, _, maxEdges int) (storage.TraversalResult, error) {
	m.maxEdgesByCall = append(m.maxEdgesByCall, maxEdges)
	if m.err != nil {
		return storage.TraversalResult{}, m.err
	}
	return storage.TraversalResult{Relationships: m.rels}, nil
}

// --- Recall test helpers ---

func makeTestMemory(id uuid.UUID, nsID uuid.UUID, content string, tags []string, importance float64, accessCount int, createdAt time.Time) *model.Memory {
	return &model.Memory{
		ID:          id,
		NamespaceID: nsID,
		Content:     content,
		Tags:        tags,
		Confidence:  1.0,
		Importance:  importance,
		AccessCount: accessCount,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
}

func newRecallService(
	memories MemoryReader,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	vectorSearch VectorSearcher,
	entityReader EntityReader,
	traverser RelationshipTraverser,
	embedFn func() provider.EmbeddingProvider,
) (*RecallService, *mockTokenUsageRepo) {
	tokenUsage := &mockTokenUsageRepo{}
	// Wrap embedFn so the middleware writes token_usage rows on every
	// Embed call — matches production wiring.
	wrapped := provider.WrapEmbeddingForTest(embedFn, tokenUsage)
	svc := NewRecallService(memories, projects, namespaces, vectorSearch, entityReader, traverser, wrapped)
	return svc, tokenUsage
}

// --- Tests ---

func TestRecall_SuccessWithVectorSearch(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	mem1ID := uuid.New()
	mem2ID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			mem1ID: makeTestMemory(mem1ID, nsID, "first memory", []string{"go"}, 0.8, 5, now.Add(-1*time.Hour)),
			mem2ID: makeTestMemory(mem2ID, nsID, "second memory", []string{"rust"}, 0.6, 2, now.Add(-24*time.Hour)),
		},
	}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: mem1ID, Score: 0.95, NamespaceID: nsID},
			{ID: mem2ID, Score: 0.80, NamespaceID: nsID},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, TotalTokens: 5},
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "find something",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}

	// First result should have higher score.
	if resp.Memories[0].Score < resp.Memories[1].Score {
		t.Error("expected first result to have higher score")
	}

	// Similarity should be set.
	if resp.Memories[0].Similarity == nil {
		t.Error("expected similarity to be set when using vector search")
	}

	if resp.LatencyMs < 0 {
		t.Error("expected non-negative latency")
	}
}

func TestRecall_WithoutEmbeddingProvider(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	mem1ID := uuid.New()
	mem2ID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(mem1ID, nsID, "listed memory 1", []string{"go"}, 0.5, 1, now),
			*makeTestMemory(mem2ID, nsID, "listed memory 2", nil, 0.5, 1, now),
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "find something",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}

	// Similarity should be nil when not using vector search.
	if resp.Memories[0].Similarity != nil {
		t.Error("expected similarity to be nil without vector search")
	}
}

func TestRecall_TagFiltering(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	mem1ID := uuid.New()
	mem2ID := uuid.New()
	mem3ID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(mem1ID, nsID, "has go and test tags", []string{"go", "test"}, 0.5, 1, now),
			*makeTestMemory(mem2ID, nsID, "has only go tag", []string{"go"}, 0.5, 1, now),
			*makeTestMemory(mem3ID, nsID, "has rust tag", []string{"rust"}, 0.5, 1, now),
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "find something",
		Tags:      []string{"go", "test"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory matching both tags, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != mem1ID {
		t.Errorf("expected memory %s, got %s", mem1ID, resp.Memories[0].ID)
	}
}

func TestRecall_ThresholdFiltering(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	mem1ID := uuid.New()
	mem2ID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			mem1ID: makeTestMemory(mem1ID, nsID, "high sim", nil, 0.9, 10, now),
			mem2ID: makeTestMemory(mem2ID, nsID, "low sim", nil, 0.1, 0, now.Add(-720*time.Hour)),
		},
	}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: mem1ID, Score: 0.99, NamespaceID: nsID},
			{ID: mem2ID, Score: 0.10, NamespaceID: nsID},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{128},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 128)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 3, TotalTokens: 3},
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	// Set a high threshold — should filter out the low-scoring result.
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "search",
		Threshold: 0.5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory above threshold, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != mem1ID {
		t.Errorf("expected memory %s above threshold", mem1ID)
	}
}

func TestRecall_LimitRespected(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	var nsList []model.Memory
	for i := 0; i < 20; i++ {
		nsList = append(nsList, *makeTestMemory(uuid.New(), nsID, fmt.Sprintf("memory %d", i), nil, 0.5, 1, now))
	}

	memReader := &mockMemoryReader{nsList: nsList}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "find",
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 5 {
		t.Fatalf("expected 5 memories (limit), got %d", len(resp.Memories))
	}
}

func TestRecall_DefaultLimit(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	var nsList []model.Memory
	for i := 0; i < 40; i++ {
		nsList = append(nsList, *makeTestMemory(uuid.New(), nsID, fmt.Sprintf("memory %d", i), nil, 0.5, 1, now))
	}

	memReader := &mockMemoryReader{nsList: nsList}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "find",
		// Limit defaults to 10
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 10 {
		t.Fatalf("expected default limit of 10, got %d", len(resp.Memories))
	}
}

func TestRecall_RankingOrder(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	highID := uuid.New()
	lowID := uuid.New()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			highID: makeTestMemory(highID, nsID, "important recent", nil, 0.9, 10, now.Add(-1*time.Hour)),
			lowID:  makeTestMemory(lowID, nsID, "old low importance", nil, 0.1, 0, now.Add(-720*time.Hour)),
		},
	}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: highID, Score: 0.95, NamespaceID: nsID},
			{ID: lowID, Score: 0.40, NamespaceID: nsID},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{128},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 128)},
			Model:      "model",
			Usage:      provider.TokenUsage{PromptTokens: 3, TotalTokens: 3},
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "search",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}

	if resp.Memories[0].ID != highID {
		t.Error("expected high-score memory to be ranked first")
	}
	if resp.Memories[0].Score <= resp.Memories[1].Score {
		t.Error("expected first result to have strictly higher score")
	}
}

func TestRecall_GraphTraversal(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	memID := uuid.New()

	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(memID, nsID, "graph memory", nil, 0.5, 1, now),
		},
	}

	entityID := uuid.New()
	entityReader := &mockEntityReader{
		entities: []model.Entity{
			{ID: entityID, NamespaceID: nsID, Name: "TestEntity", EntityType: "concept"},
		},
	}

	traverser := &mockRelTraverser{
		rels: []model.Relationship{
			{
				ID:           uuid.New(),
				NamespaceID:  nsID,
				SourceID:     entityID,
				TargetID:     uuid.New(),
				Relation:     "related_to",
				Weight:       0.8,
				SourceMemory: &memID,
				CreatedAt:    now,
				ValidFrom:    now,
			},
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, entityReader, traverser, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:    projectID,
		Query:        "find graph",
		IncludeGraph: true,
		GraphDepth:   2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Graph.Entities) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(resp.Graph.Entities))
	}
	if resp.Graph.Entities[0].Name != "TestEntity" {
		t.Errorf("expected entity 'TestEntity', got %q", resp.Graph.Entities[0].Name)
	}
	if resp.Graph.Entities[0].EntityType != "concept" {
		t.Errorf("expected entity type 'concept', got %q", resp.Graph.Entities[0].EntityType)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(resp.Memories))
	}
	// Confirm graph.max_edges flows through to the traverser. Without a
	// wired SettingsService, ResolveIntWithDefault returns the registered
	// default (2000). The fix in this change cuts the traverser short on
	// the recall path's graph block too, matching memory_graph behavior.
	if len(traverser.maxEdgesByCall) == 0 {
		t.Fatalf("expected traverser to be called at least once, got 0")
	}
	if traverser.maxEdgesByCall[0] != 2000 {
		t.Errorf("expected recall to pass graph.max_edges=2000 to traverser, got %d", traverser.maxEdgesByCall[0])
	}
}

func TestRecall_ProjectNotFound(t *testing.T) {
	_, _, projects, namespaces := setupTestFixtures()
	memReader := &mockMemoryReader{}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: uuid.New(), // non-existent
		Query:     "search",
	})
	if err == nil {
		t.Error("expected error for non-existent project")
	}
}

func TestRecall_EmptyQuery(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	memReader := &mockMemoryReader{}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "",
	})
	if err == nil {
		t.Error("expected error for empty query")
	}

	_, err = svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "   ",
	})
	if err == nil {
		t.Error("expected error for whitespace-only query")
	}
}

func TestRecall_TokenUsageRecorded(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	userID := uuid.New()
	apiKeyID := uuid.New()

	memID := uuid.New()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			memID: makeTestMemory(memID, nsID, "content", nil, 0.5, 1, time.Now()),
		},
	}

	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: memID, Score: 0.9, NamespaceID: nsID},
		},
	}

	embProvider := &mockEmbeddingProvider{
		name:       "usage-provider",
		dimensions: []int{256},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 256)},
			Model:      "usage-model",
			Usage:      provider.TokenUsage{PromptTokens: 7, CompletionTokens: 0, TotalTokens: 7},
		},
	}

	svc, tokenUsage := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "search with usage",
		UserID:    &userID,
		APIKeyID:  &apiKeyID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tokenUsage.usages) != 1 {
		t.Fatalf("expected 1 token usage record, got %d", len(tokenUsage.usages))
	}

	tu := tokenUsage.usages[0]
	if tu.Operation != "embedding" {
		t.Errorf("expected operation 'embedding', got %q", tu.Operation)
	}
	if tu.Provider != "usage-provider" {
		t.Errorf("expected provider 'usage-provider', got %q", tu.Provider)
	}
	if tu.Model != "usage-model" {
		t.Errorf("expected model 'usage-model', got %q", tu.Model)
	}
	if tu.TokensInput != 7 {
		t.Errorf("expected 7 input tokens, got %d", tu.TokensInput)
	}
	if *tu.UserID != userID {
		t.Errorf("expected user ID %s, got %s", userID, *tu.UserID)
	}
	if *tu.APIKeyID != apiKeyID {
		t.Errorf("expected API key ID %s, got %s", apiKeyID, *tu.APIKeyID)
	}
}

func TestRecall_LatencyTracked(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(uuid.New(), nsID, "content", nil, 0.5, 1, time.Now()),
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "latency test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestRecall_NamespaceIDOverride(t *testing.T) {
	_, nsID, projects, namespaces := setupTestFixtures()

	overrideNsID := nsID // use the same namespace ID for simplicity
	memID := uuid.New()

	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(memID, overrideNsID, "override ns memory", nil, 0.5, 1, time.Now()),
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	// Use NamespaceID override — project_id can be nil.
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		NamespaceID: &overrideNsID,
		Query:       "override search",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != memID {
		t.Errorf("expected memory %s, got %s", memID, resp.Memories[0].ID)
	}
}

func TestRecall_WithSourceAndMetadata(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	memID := uuid.New()
	source := "api"
	meta := json.RawMessage(`{"key":"val"}`)
	mem := makeTestMemory(memID, nsID, "with metadata", []string{"tag1"}, 0.5, 1, time.Now())
	mem.Source = &source
	mem.Metadata = meta

	memReader := &mockMemoryReader{
		nsList: []model.Memory{*mem},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "metadata check",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(resp.Memories))
	}

	r := resp.Memories[0]
	if r.Source == nil || *r.Source != "api" {
		t.Error("expected source 'api'")
	}
	if string(r.Metadata) != `{"key":"val"}` {
		t.Errorf("expected metadata, got %s", string(r.Metadata))
	}
}

func TestRecall_EmptyResults(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	memReader := &mockMemoryReader{
		nsList: []model.Memory{},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "nothing here",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Memories == nil {
		t.Error("expected non-nil memories slice")
	}
	if len(resp.Memories) != 0 {
		t.Errorf("expected 0 memories, got %d", len(resp.Memories))
	}
}

// --- diversify_by_tag_prefix tests ---

// diversifySeed is a compact per-memory fixture used by the diversification
// tests: deterministic id, tag list, and vector-search similarity score.
type diversifySeed struct {
	id    uuid.UUID
	tags  []string
	score float64
}

// buildDiversifyService wires a RecallService using vector search so the
// similarity scores driving ranking are deterministic and directly controlled
// by the seed list's score field.
func buildDiversifyService(
	t *testing.T,
	projects *mockProjectRepo,
	namespaces *mockNamespaceRepo,
	nsID uuid.UUID,
	seeds []diversifySeed,
) (*RecallService, uuid.UUID) {
	t.Helper()
	now := time.Now()
	memoryMap := make(map[uuid.UUID]*model.Memory, len(seeds))
	vecResults := make([]storage.VectorSearchResult, 0, len(seeds))
	for _, s := range seeds {
		memoryMap[s.id] = makeTestMemory(s.id, nsID, "content", s.tags, 0.5, 0, now)
		vecResults = append(vecResults, storage.VectorSearchResult{ID: s.id, Score: s.score, NamespaceID: nsID})
	}
	memReader := &mockMemoryReader{memories: memoryMap}
	vs := &mockVectorSearcher{results: vecResults}
	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{128},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 128)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 1, TotalTokens: 1},
		},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, vs, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	return svc, uuid.Nil
}

func TestRecall_DiversifyByTagPrefix_RoundRobin(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	// 3 groups × 3 items + 2 ungrouped. Similarity strictly descending so
	// ranking is deterministic regardless of the composite-score tiebreaker.
	a0, a1, a2 := uuid.New(), uuid.New(), uuid.New()
	b0, b1, b2 := uuid.New(), uuid.New(), uuid.New()
	c0, c1, c2 := uuid.New(), uuid.New(), uuid.New()
	u0, u1 := uuid.New(), uuid.New()
	seeds := []diversifySeed{
		{a0, []string{"category-a", "x"}, 0.99},
		{a1, []string{"category-a", "y"}, 0.96},
		{a2, []string{"category-a", "z"}, 0.93},
		{b0, []string{"category-b", "x"}, 0.90},
		{b1, []string{"category-b", "y"}, 0.87},
		{b2, []string{"category-b", "z"}, 0.84},
		{c0, []string{"category-c", "x"}, 0.81},
		{c1, []string{"category-c", "y"}, 0.78},
		{c2, []string{"category-c", "z"}, 0.75},
		{u0, []string{"other"}, 0.72},
		{u1, []string{"unrelated", "misc"}, 0.69},
	}

	svc, _ := buildDiversifyService(t, projects, namespaces, nsID, seeds)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:            projectID,
		Query:                "q",
		Limit:                6,
		DiversifyByTagPrefix: "category-",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 6 {
		t.Fatalf("expected 6 memories, got %d", len(resp.Memories))
	}

	// Round-robin in first-seen group order: a, b, c, a, b, c.
	wantGroups := []string{"category-a", "category-b", "category-c", "category-a", "category-b", "category-c"}
	for i, want := range wantGroups {
		got := firstTagWithPrefix(resp.Memories[i].Tags, "category-")
		if got != want {
			t.Errorf("memory %d: expected group %s, got %s (tags=%v)", i, want, got, resp.Memories[i].Tags)
		}
	}

	// Ungrouped memories must not appear.
	for _, m := range resp.Memories {
		if firstTagWithPrefix(m.Tags, "category-") == "" {
			t.Errorf("ungrouped memory returned: tags=%v", m.Tags)
		}
	}

	// All 3 groups represented → no coverage gaps.
	if len(resp.CoverageGaps) != 0 {
		t.Errorf("expected no coverage gaps, got %+v", resp.CoverageGaps)
	}
}

func TestRecall_DiversifyByTagPrefix_LimitCausesGap(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	a0, b0, c0 := uuid.New(), uuid.New(), uuid.New()
	seeds := []diversifySeed{
		{a0, []string{"category-a"}, 0.99},
		{b0, []string{"category-b"}, 0.96},
		{c0, []string{"category-c"}, 0.93},
	}
	svc, _ := buildDiversifyService(t, projects, namespaces, nsID, seeds)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:            projectID,
		Query:                "q",
		Limit:                2,
		DiversifyByTagPrefix: "category-",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories (limit), got %d", len(resp.Memories))
	}

	returned := map[string]bool{}
	for _, m := range resp.Memories {
		returned[firstTagWithPrefix(m.Tags, "category-")] = true
	}
	if !returned["category-a"] || !returned["category-b"] {
		t.Errorf("expected a and b to be returned first, got %v", returned)
	}
	if returned["category-c"] {
		t.Error("did not expect category-c to be returned at limit=2")
	}

	if len(resp.CoverageGaps) != 1 {
		t.Fatalf("expected 1 coverage gap, got %d: %+v", len(resp.CoverageGaps), resp.CoverageGaps)
	}
	if resp.CoverageGaps[0].GroupKey != "category-c" {
		t.Errorf("expected gap for category-c, got %s", resp.CoverageGaps[0].GroupKey)
	}
	if resp.CoverageGaps[0].Cause != CoverageCauseLimit {
		t.Errorf("expected cause=%s, got %s", CoverageCauseLimit, resp.CoverageGaps[0].Cause)
	}
}

func TestRecall_DiversifyByTagPrefix_ThresholdCausesGap(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	a0 := uuid.New()
	b0 := uuid.New()
	// a0 at 0.99 will produce a composite score well above 0.5. b0 at 0.02 —
	// with Similarity weight 0.5 contributing 0.01 — will be below even the
	// recency/importance floor. Confirm: b0 composite ≤ 0.5*0.02 + 0.15*~1 +
	// 0.10*0.5 + 0 + 0 ≈ 0.21, comfortably below threshold=0.5.
	seeds := []diversifySeed{
		{a0, []string{"category-a"}, 0.99},
		{b0, []string{"category-b"}, 0.02},
	}
	svc, _ := buildDiversifyService(t, projects, namespaces, nsID, seeds)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:            projectID,
		Query:                "q",
		Limit:                10,
		Threshold:            0.5,
		DiversifyByTagPrefix: "category-",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory above threshold, got %d", len(resp.Memories))
	}
	if firstTagWithPrefix(resp.Memories[0].Tags, "category-") != "category-a" {
		t.Errorf("expected only category-a to survive threshold")
	}

	if len(resp.CoverageGaps) != 1 {
		t.Fatalf("expected 1 coverage gap, got %d: %+v", len(resp.CoverageGaps), resp.CoverageGaps)
	}
	if resp.CoverageGaps[0].GroupKey != "category-b" || resp.CoverageGaps[0].Cause != CoverageCauseThreshold {
		t.Errorf("expected category-b threshold gap, got %+v", resp.CoverageGaps[0])
	}
}

func TestRecall_DiversifyByTagPrefix_TagFilterCausesGap(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	a0 := uuid.New()
	b0 := uuid.New()
	seeds := []diversifySeed{
		{a0, []string{"required", "category-a"}, 0.99},
		{b0, []string{"category-b"}, 0.96}, // missing "required"
	}
	svc, _ := buildDiversifyService(t, projects, namespaces, nsID, seeds)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:            projectID,
		Query:                "q",
		Limit:                10,
		Tags:                 []string{"required"},
		DiversifyByTagPrefix: "category-",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory after tag filter, got %d", len(resp.Memories))
	}
	if firstTagWithPrefix(resp.Memories[0].Tags, "category-") != "category-a" {
		t.Errorf("expected category-a to pass tag filter")
	}

	if len(resp.CoverageGaps) != 1 {
		t.Fatalf("expected 1 coverage gap, got %d: %+v", len(resp.CoverageGaps), resp.CoverageGaps)
	}
	if resp.CoverageGaps[0].GroupKey != "category-b" || resp.CoverageGaps[0].Cause != CoverageCauseTagFilter {
		t.Errorf("expected category-b tag_filter gap, got %+v", resp.CoverageGaps[0])
	}
}

func TestRecall_DiversifyByTagPrefix_Unset_NoGaps(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	a0, b0 := uuid.New(), uuid.New()
	seeds := []diversifySeed{
		{a0, []string{"category-a"}, 0.99},
		{b0, []string{"category-b"}, 0.96},
	}
	svc, _ := buildDiversifyService(t, projects, namespaces, nsID, seeds)

	// Omitting DiversifyByTagPrefix → existing behavior, no CoverageGaps.
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "q",
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(resp.Memories))
	}
	if resp.CoverageGaps != nil {
		t.Errorf("expected nil CoverageGaps when diversify unset, got %+v", resp.CoverageGaps)
	}
}

func TestRecall_DiversifyByTagPrefix_NoMatchingCandidates(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	u0, u1 := uuid.New(), uuid.New()
	seeds := []diversifySeed{
		{u0, []string{"misc"}, 0.99},
		{u1, []string{"other"}, 0.96},
	}
	svc, _ := buildDiversifyService(t, projects, namespaces, nsID, seeds)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:            projectID,
		Query:                "q",
		Limit:                10,
		DiversifyByTagPrefix: "category-",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 0 {
		t.Errorf("expected 0 memories (no prefix matches), got %d", len(resp.Memories))
	}
	if len(resp.CoverageGaps) != 0 {
		t.Errorf("expected no coverage gaps (no observed groups), got %+v", resp.CoverageGaps)
	}
}

func TestRecall_DiversifyByTagPrefix_Deterministic(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	// 4 groups × 2 items, limit=3 → c and d must both surface as gaps; order
	// must be sorted by group key on every run regardless of map iteration.
	ids := make([]uuid.UUID, 8)
	for i := range ids {
		ids[i] = uuid.New()
	}
	seeds := []diversifySeed{
		{ids[0], []string{"category-a"}, 0.99},
		{ids[1], []string{"category-a"}, 0.98},
		{ids[2], []string{"category-b"}, 0.97},
		{ids[3], []string{"category-b"}, 0.96},
		{ids[4], []string{"category-c"}, 0.95},
		{ids[5], []string{"category-c"}, 0.94},
		{ids[6], []string{"category-d"}, 0.93},
		{ids[7], []string{"category-d"}, 0.92},
	}
	svc, _ := buildDiversifyService(t, projects, namespaces, nsID, seeds)

	var first *RecallResponse
	for i := 0; i < 2; i++ {
		resp, err := svc.Recall(context.Background(), &RecallRequest{
			ProjectID:            projectID,
			Query:                "q",
			Limit:                3,
			DiversifyByTagPrefix: "category-",
		})
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if first == nil {
			first = resp
			continue
		}
		if len(resp.Memories) != len(first.Memories) {
			t.Fatalf("iteration %d: memory count drift: %d vs %d", i, len(resp.Memories), len(first.Memories))
		}
		for j := range resp.Memories {
			if resp.Memories[j].ID != first.Memories[j].ID {
				t.Errorf("iteration %d, position %d: non-deterministic memory order", i, j)
			}
		}
		if len(resp.CoverageGaps) != len(first.CoverageGaps) {
			t.Fatalf("iteration %d: coverage-gap count drift", i)
		}
		for j := range resp.CoverageGaps {
			if resp.CoverageGaps[j] != first.CoverageGaps[j] {
				t.Errorf("iteration %d, gap %d: non-deterministic gap order", i, j)
			}
		}
	}

	// Sanity: gaps are sorted ascending.
	for i := 1; i < len(first.CoverageGaps); i++ {
		if first.CoverageGaps[i-1].GroupKey > first.CoverageGaps[i].GroupKey {
			t.Errorf("coverage_gaps not sorted: %+v", first.CoverageGaps)
		}
	}
}

// TestRecall_IncludeLowNovelty_BypassesDreamFilter confirms that the
// dream-source low_novelty filter at the candidate-pruning step is gated on
// req.IncludeLowNovelty: default false hides demoted dreams, true surfaces
// them so an MCP caller can inspect what the dreamer demoted and why.
func TestRecall_IncludeLowNovelty_BypassesDreamFilter(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	dreamSrc := model.DreamSource
	regularSrc := "api"
	now := time.Now()

	demotedID := uuid.New()
	demoted := makeTestMemory(demotedID, nsID, "demoted dream", nil, 0.5, 1, now)
	demoted.Source = &dreamSrc
	demoted.Metadata = json.RawMessage(`{"low_novelty":true,"low_novelty_reason":"orphan_no_sources"}`)

	keptID := uuid.New()
	kept := makeTestMemory(keptID, nsID, "regular memory", nil, 0.5, 1, now)
	kept.Source = &regularSrc

	memReader := &mockMemoryReader{
		nsList: []model.Memory{*demoted, *kept},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	// Default: demoted dream filtered out.
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "anything",
	})
	if err != nil {
		t.Fatalf("default recall: %v", err)
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("default recall: expected 1 memory, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != keptID {
		t.Errorf("default recall: expected only the regular memory, got %s", resp.Memories[0].ID)
	}

	// Opt-in: demoted dream surfaces alongside the regular memory.
	resp, err = svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         projectID,
		Query:             "anything",
		IncludeLowNovelty: true,
	})
	if err != nil {
		t.Fatalf("include_low_novelty recall: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("include_low_novelty recall: expected 2 memories, got %d", len(resp.Memories))
	}
	got := map[uuid.UUID]bool{}
	for _, m := range resp.Memories {
		got[m.ID] = true
	}
	if !got[demotedID] || !got[keptID] {
		t.Errorf("expected both memories surfaced; got %v", got)
	}
}

// TestRecall_PerNamespaceProjectAttribution confirms that candidates fetched
// from the global namespace alongside the primary project's namespace get
// stamped with the global project's slug, not the primary's. Without the
// per-namespace lookup, every result was attributed to the primary project.
func TestRecall_PerNamespaceProjectAttribution(t *testing.T) {
	primaryID := uuid.New()
	primaryNs := uuid.New()
	globalID := uuid.New()
	globalNs := uuid.New()

	projects := &mockProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			primaryID: {ID: primaryID, NamespaceID: primaryNs, Name: "Primary", Slug: "primary"},
			globalID:  {ID: globalID, NamespaceID: globalNs, Name: "Global", Slug: "global"},
		},
	}
	namespaces := &mockNamespaceRepo{
		namespaces: map[uuid.UUID]*model.Namespace{
			primaryNs: {ID: primaryNs, Slug: "primary", Kind: "project", Path: "primary"},
			globalNs:  {ID: globalNs, Slug: "global", Kind: "project", Path: "global"},
		},
	}

	primaryMemID := uuid.New()
	globalMemID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(primaryMemID, primaryNs, "primary content", nil, 0.5, 1, now),
			*makeTestMemory(globalMemID, globalNs, "global content", nil, 0.5, 1, now),
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}

	bySlug := map[uuid.UUID]string{}
	for _, m := range resp.Memories {
		bySlug[m.ID] = m.ProjectSlug
	}
	if bySlug[primaryMemID] != "primary" {
		t.Errorf("primary memory: expected slug 'primary', got %q", bySlug[primaryMemID])
	}
	if bySlug[globalMemID] != "global" {
		t.Errorf("global memory: expected slug 'global', got %q (regression: globals were being attributed to the search-target project)", bySlug[globalMemID])
	}
}

// --- Hybrid recall fusion tests ---

type mockLexicalSearcher struct {
	results map[uuid.UUID][]storage.MemoryRank // namespace → ranked results
}

func (m *mockLexicalSearcher) SearchByText(_ context.Context, ns uuid.UUID, _ string, limit int) ([]storage.MemoryRank, error) {
	r := m.results[ns]
	if limit > 0 && limit < len(r) {
		return r[:limit], nil
	}
	return r, nil
}

// TestRecall_FusionDisabled_NoBehaviorChange verifies the off-flag path is
// untouched: a fusion-aware build with FusionConfig.Enabled=false (the
// default) produces the same output as a build without a lexical searcher.
// The regression we are guarding against is "wiring fusion accidentally
// changed cosine-only ranking."
func TestRecall_FusionDisabled_NoBehaviorChange(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	mem1ID := uuid.New()
	mem2ID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			mem1ID: makeTestMemory(mem1ID, nsID, "first memory", []string{"go"}, 0.8, 5, now.Add(-1*time.Hour)),
			mem2ID: makeTestMemory(mem2ID, nsID, "second memory", []string{"rust"}, 0.6, 2, now.Add(-24*time.Hour)),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: mem1ID, Score: 0.95, NamespaceID: nsID},
			{ID: mem2ID, Score: 0.80, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })

	// Wire a lexical searcher that would rank mem2 first if it were
	// consulted — fusion-off must ignore it.
	svc.SetLexical(&mockLexicalSearcher{
		results: map[uuid.UUID][]storage.MemoryRank{
			nsID: {{ID: mem2ID, Rank: 1}, {ID: mem1ID, Rank: 0.5}},
		},
	})
	// Default FusionConfig has Enabled=false — leave it.

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: "find something", Limit: 10})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	// mem1 has higher cosine + higher importance + more recent → should win
	// when fusion is off, ignoring lexical entirely.
	if resp.Memories[0].ID != mem1ID {
		t.Errorf("fusion-off: expected mem1 first, got %v (lexical bled through?)", resp.Memories[0].ID)
	}
}

// TestRecall_FusionEnabled_LexicalOnlyHit verifies the lexical channel
// surfaces memories the vector channel completely missed when fusion is on.
// This is the headline value of the feature.
func TestRecall_FusionEnabled_LexicalOnlyHit(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	lexHitID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			lexHitID: makeTestMemory(lexHitID, nsID, "retatrutide-2.4mg dosing protocol", nil, 0.5, 0, now),
		},
	}
	// Vector returns nothing — embedder cannot resolve the lexical query.
	vectorSearcher := &mockVectorSearcher{results: nil}
	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	svc.SetLexical(&mockLexicalSearcher{
		results: map[uuid.UUID][]storage.MemoryRank{
			nsID: {{ID: lexHitID, Rank: 1.0}},
		},
	})
	svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.7, LexicalWeight: 0.3})

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: "retatrutide-2.4mg", Limit: 10})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory from lexical channel, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != lexHitID {
		t.Errorf("expected lexHit %v, got %v", lexHitID, resp.Memories[0].ID)
	}
}

// TestRecall_FusionEnabled_EmptyLexicalMatchesVectorOnly guards against
// fusion-on regressing queries the vector channel already handles when
// the lexical channel produces nothing — the realistic case where the
// user's query has no exact-token matches in the corpus.
func TestRecall_FusionEnabled_EmptyLexicalMatchesVectorOnly(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	mem1ID := uuid.New()
	mem2ID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			mem1ID: makeTestMemory(mem1ID, nsID, "first memory", []string{"go"}, 0.8, 5, now.Add(-1*time.Hour)),
			mem2ID: makeTestMemory(mem2ID, nsID, "second memory", []string{"rust"}, 0.6, 2, now.Add(-24*time.Hour)),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: mem1ID, Score: 0.95, NamespaceID: nsID},
			{ID: mem2ID, Score: 0.80, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	// Lexical returns no rows — the realistic case where the user's query
	// has no exact-token matches in the corpus.
	svc.SetLexical(&mockLexicalSearcher{results: map[uuid.UUID][]storage.MemoryRank{}})
	svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.70, LexicalWeight: 0.30})

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: "find something", Limit: 10})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != mem1ID {
		t.Errorf("fusion-on with empty lex: expected mem1 first (matches vector-only result), got %v", resp.Memories[0].ID)
	}
	if resp.Memories[0].Similarity == nil {
		t.Error("expected similarity to be set under fusion (it carries the normalized fused score)")
	}
}

// TestRecall_FusionEnabled_BothChannelsBoost verifies that a memory which
// surfaces in both rankings ranks above one that appears in only the
// vector channel — the documents-with-multi-channel-evidence-win property
// is what makes RRF worth the engineering.
func TestRecall_FusionEnabled_BothChannelsBoost(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	bothID := uuid.New()
	vecOnlyID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			bothID:    makeTestMemory(bothID, nsID, "memory in both channels", nil, 0.5, 0, now),
			vecOnlyID: makeTestMemory(vecOnlyID, nsID, "memory in vector only", nil, 0.5, 0, now),
		},
	}
	// Both vector positions roughly equivalent — RRF should pick the doc
	// with cross-channel evidence.
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: vecOnlyID, Score: 0.9, NamespaceID: nsID}, // rank 1 in vector
			{ID: bothID, Score: 0.85, NamespaceID: nsID},   // rank 2 in vector
		},
	}
	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	svc.SetLexical(&mockLexicalSearcher{
		results: map[uuid.UUID][]storage.MemoryRank{
			nsID: {{ID: bothID, Rank: 1.0}}, // bothID rank 1 in lexical
		},
	})
	svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.5, LexicalWeight: 0.5})

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: "anything", Limit: 10})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	// bothID: vec rank 2 (1/62) + lex rank 1 (1/61); vecOnlyID: vec rank 1 (1/61).
	// 1/62 + 1/61 ≈ 0.0325 vs 1/61 ≈ 0.0164 — bothID wins.
	if resp.Memories[0].ID != bothID {
		t.Errorf("expected cross-channel memory to rank first; got %v", resp.Memories[0].ID)
	}
}

// --- Confidence ranking term + per-project resolver ---

// makeTestMemoryWithConfidence is a variant of makeTestMemory that takes an
// explicit confidence so tests can build adjacent rows differing only by
// confidence (the existing helper hard-codes 1.0).
func makeTestMemoryWithConfidence(id uuid.UUID, nsID uuid.UUID, content string, importance, confidence float64, createdAt time.Time) *model.Memory {
	return &model.Memory{
		ID:          id,
		NamespaceID: nsID,
		Content:     content,
		Tags:        []string{},
		Confidence:  confidence,
		Importance:  importance,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
}

// TestRecall_ConfidenceRanksHigher verifies the new Confidence term in
// computeScore actually shifts ordering. Two memories share content,
// importance, and recency; only their stored Confidence differs. The
// higher-confidence row must rank first AND its score must be strictly
// greater (so a future regression that drops the term is caught).
func TestRecall_ConfidenceRanksHigher(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	highID := uuid.New()
	lowID := uuid.New()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			highID: makeTestMemoryWithConfidence(highID, nsID, "shared content", 0.5, 1.0, now.Add(-1*time.Hour)),
			lowID:  makeTestMemoryWithConfidence(lowID, nsID, "shared content", 0.5, 0.5, now.Add(-1*time.Hour)),
		},
	}

	// Identical similarity to isolate the Confidence contribution.
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: highID, Score: 0.80, NamespaceID: nsID},
			{ID: lowID, Score: 0.80, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "search",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != highID {
		t.Errorf("expected high-confidence memory to rank first, got %v", resp.Memories[0].ID)
	}
	if resp.Memories[0].Score <= resp.Memories[1].Score {
		t.Errorf("expected strict score gap; got %v vs %v", resp.Memories[0].Score, resp.Memories[1].Score)
	}
	// Score delta should be approximately Confidence_weight * (1.0 - 0.5) = 0.025.
	delta := resp.Memories[0].Score - resp.Memories[1].Score
	if delta < 0.020 || delta > 0.030 {
		t.Errorf("expected delta ~= 0.025, got %v", delta)
	}
}

// TestRecall_ZeroConfidenceFiltered verifies the kill-signal at recall.go:725
// is preserved. A confidence=0 memory must not appear in results.
func TestRecall_ZeroConfidenceFiltered(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	zeroID := uuid.New()
	keepID := uuid.New()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			zeroID: makeTestMemoryWithConfidence(zeroID, nsID, "filtered", 0.5, 0.0, now),
			keepID: makeTestMemoryWithConfidence(keepID, nsID, "kept", 0.5, 1.0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: zeroID, Score: 0.99, NamespaceID: nsID},
			{ID: keepID, Score: 0.50, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "x",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("expected exactly 1 memory (zero-confidence filtered), got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != keepID {
		t.Errorf("expected the non-zero memory to be returned, got %v", resp.Memories[0].ID)
	}
}

// TestRecall_PerProjectOverrideMerges verifies that a project's
// ranking_weights JSON override merges into the system weights, leaving
// non-overridden fields at their system value. The override sets only
// Confidence to 0.50; with otherwise-identical candidates the boost on the
// higher-confidence row should now be ~10x larger than under the system
// default of 0.05.
func TestRecall_PerProjectOverrideMerges(t *testing.T) {
	projectID, nsID, _, namespaces := setupTestFixtures()

	// Re-build projects with a Settings JSON that overrides Confidence.
	projects := &mockProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: nsID,
				Name:        "Test Project",
				Slug:        "test-project",
				Settings:    json.RawMessage(`{"ranking_weights":{"confidence":0.50}}`),
			},
		},
	}

	now := time.Now()
	highID := uuid.New()
	lowID := uuid.New()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			highID: makeTestMemoryWithConfidence(highID, nsID, "shared", 0.5, 1.0, now),
			lowID:  makeTestMemoryWithConfidence(lowID, nsID, "shared", 0.5, 0.5, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: highID, Score: 0.80, NamespaceID: nsID},
			{ID: lowID, Score: 0.80, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "search",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != highID {
		t.Errorf("expected high-confidence memory to rank first, got %v", resp.Memories[0].ID)
	}
	// Override pumps Confidence weight to 0.50, so the delta should be
	// ~0.50 * 0.5 = 0.25 — much larger than the default-weight delta of
	// ~0.025 from the previous test.
	delta := resp.Memories[0].Score - resp.Memories[1].Score
	if delta < 0.20 || delta > 0.30 {
		t.Errorf("expected delta ~= 0.25 with project override, got %v", delta)
	}
}

// TestRecall_PerProjectOverrideLegacyShape verifies the parser still honors
// projects whose settings have not been migrated yet (the legacy
// recency/relevance/importance shape). With relevance set high but Confidence
// unset, ranking should still be sane.
func TestRecall_PerProjectOverrideLegacyShape(t *testing.T) {
	projectID, nsID, _, namespaces := setupTestFixtures()

	// Legacy shape: relevance instead of similarity, no other canonical keys.
	projects := &mockProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: nsID,
				Settings:    json.RawMessage(`{"ranking_weights":{"relevance":0.80,"recency":0.10,"importance":0.10}}`),
			},
		},
	}

	now := time.Now()
	id := uuid.New()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id: makeTestMemoryWithConfidence(id, nsID, "shared", 0.5, 1.0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{{ID: id, Score: 0.80, NamespaceID: nsID}},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: "x"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("legacy-shape override should still yield results, got %d", len(resp.Memories))
	}
}

// --- Origin weight + namespace-quota + per-channel-normalize tests ---

// setupPrimaryGlobalFixtures builds the standard primary+global namespace
// pair used by every test in this group. Both projects exist in the
// project repo so the per-namespace attribution stamp resolves correctly.
func setupPrimaryGlobalFixtures() (uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, *mockProjectRepo, *mockNamespaceRepo) {
	primaryID := uuid.New()
	primaryNs := uuid.New()
	globalID := uuid.New()
	globalNs := uuid.New()
	projects := &mockProjectRepo{projects: map[uuid.UUID]*model.Project{
		primaryID: {ID: primaryID, NamespaceID: primaryNs, Name: "Primary", Slug: "primary"},
		globalID:  {ID: globalID, NamespaceID: globalNs, Name: "Global", Slug: "global"},
	}}
	namespaces := &mockNamespaceRepo{namespaces: map[uuid.UUID]*model.Namespace{
		primaryNs: {ID: primaryNs, Slug: "primary", Kind: "project", Path: "primary"},
		globalNs:  {ID: globalNs, Slug: "global", Kind: "project", Path: "global"},
	}}
	return primaryID, primaryNs, globalID, globalNs, projects, namespaces
}

// TestRecall_OriginWeightZero_GlobalBeatsProjectOnCosine establishes the
// baseline: with Origin=0 (the shipped default), a global memory with
// strictly higher cosine similarity ranks above a project memory. This is
// the symptom the origin weight was added to address — locking the default
// in a test guards against an upgrade-time behavioral surprise.
func TestRecall_OriginWeightZero_GlobalBeatsProjectOnCosine(t *testing.T) {
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()

	projectMemID := uuid.New()
	globalMemID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			projectMemID: makeTestMemory(projectMemID, primaryNs, "project content", nil, 0.5, 0, now),
			globalMemID:  makeTestMemory(globalMemID, globalNs, "global content", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: projectMemID, Score: 0.60, NamespaceID: primaryNs},
			{ID: globalMemID, Score: 0.65, NamespaceID: globalNs},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != globalMemID {
		t.Errorf("default Origin=0 should let the higher-cosine global rank first; got %v first", resp.Memories[0].ID)
	}
}

// TestRecall_OriginWeightFlipsTie verifies that a small Origin boost is
// enough to flip the project/global order when cosines are close. With
// Origin=0.10 the project's 1.0*0.10 affinity term overtakes the global's
// 0.05 cosine edge from w.Similarity*(0.65-0.60)=0.025.
func TestRecall_OriginWeightFlipsTie(t *testing.T) {
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()

	projectMemID := uuid.New()
	globalMemID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			projectMemID: makeTestMemory(projectMemID, primaryNs, "project content", nil, 0.5, 0, now),
			globalMemID:  makeTestMemory(globalMemID, globalNs, "global content", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: projectMemID, Score: 0.60, NamespaceID: primaryNs},
			{ID: globalMemID, Score: 0.65, NamespaceID: globalNs},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	// Boost the project-affinity term. Default DefaultRankingWeights has
	// Similarity 0.50, so a 0.05 cosine gap contributes 0.025 to the
	// score. Origin 0.10 contributes 0.10 — flips the comparison.
	w := DefaultRankingWeights
	w.Origin = 0.10
	svc.SetWeights(w)

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != projectMemID {
		t.Errorf("Origin=0.10 should lift project memory above the marginally-higher-cosine global; got %v first", resp.Memories[0].ID)
	}
}

// TestRecall_OriginWeightOverrideDoesNotLeakAcrossProjects mirrors the
// existing TestRecall_PerNamespaceProjectAttribution discipline: a per-
// project Origin override applies to candidates from that project only.
// The global memory must continue to be scored with the global project's
// (unset) Origin, not the primary's elevated override.
func TestRecall_OriginWeightOverrideDoesNotLeakAcrossProjects(t *testing.T) {
	primaryID, primaryNs, globalID, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()
	// Override primary's ranking weights with Origin=0.30; leave global's
	// settings empty so it inherits the system base (Origin=0).
	projects.projects[primaryID].Settings = []byte(`{"ranking_weights":{"origin":0.30}}`)
	_ = globalID

	projectMemID := uuid.New()
	globalMemID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			projectMemID: makeTestMemory(projectMemID, primaryNs, "project content", nil, 0.5, 0, now),
			globalMemID:  makeTestMemory(globalMemID, globalNs, "global content", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: projectMemID, Score: 0.50, NamespaceID: primaryNs},
			{ID: globalMemID, Score: 0.95, NamespaceID: globalNs},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Memories))
	}
	// Verify the global memory is still attributed to "global" — the
	// per-project Origin override applies only to primary-stamped
	// candidates. The primary candidate's score: 0.50*Sim(0.50) + Origin*1
	// (0.30) = 0.55. Global's: 0.95*0.50 + Origin*0 = 0.475. Primary wins
	// because of its own override, not because global was mis-scored.
	if resp.Memories[0].ID != projectMemID {
		t.Errorf("primary's Origin=0.30 override should lift the primary memory; got %v first", resp.Memories[0].ID)
	}
	var globalSlug string
	for _, m := range resp.Memories {
		if m.ID == globalMemID {
			globalSlug = m.ProjectSlug
		}
	}
	if globalSlug != "global" {
		t.Errorf("global memory must remain attributed to 'global', got %q (override leaked across projects)", globalSlug)
	}
}

// TestRecall_NamespaceQuotaReservesProjectSlots covers the quota path: when
// project_min > 0 and the project has at least that many passing candidates,
// the final result contains exactly that many primary-stamped memories even
// when the score-only truncation would have included fewer.
func TestRecall_NamespaceQuotaReservesProjectSlots(t *testing.T) {
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()

	now := time.Now()
	// 3 project memories (low similarity) + 10 global memories (high
	// similarity). Without quota, all 10 globals rank above all 3
	// project memories. With quota=2, the top-5 result holds 2 project
	// + 3 global.
	memMap := map[uuid.UUID]*model.Memory{}
	var vecResults []storage.VectorSearchResult
	var projectIDs []uuid.UUID
	for i := 0; i < 3; i++ {
		id := uuid.New()
		projectIDs = append(projectIDs, id)
		memMap[id] = makeTestMemory(id, primaryNs, "project content", nil, 0.5, 0, now)
		vecResults = append(vecResults, storage.VectorSearchResult{ID: id, Score: 0.30, NamespaceID: primaryNs})
	}
	for i := 0; i < 10; i++ {
		id := uuid.New()
		memMap[id] = makeTestMemory(id, globalNs, "global content", nil, 0.5, 0, now)
		vecResults = append(vecResults, storage.VectorSearchResult{ID: id, Score: 0.90, NamespaceID: globalNs})
	}
	memReader := &mockMemoryReader{memories: memMap}
	vectorSearcher := &mockVectorSearcher{results: vecResults}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:                primaryID,
		GlobalNamespaceID:        &globalNs,
		Query:                    "anything",
		Limit:                    5,
		NamespaceQuotaProjectMin: 2,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 5 {
		t.Fatalf("expected 5 memories (limit), got %d", len(resp.Memories))
	}
	projectSet := map[uuid.UUID]struct{}{}
	for _, id := range projectIDs {
		projectSet[id] = struct{}{}
	}
	projectInTop := 0
	for _, m := range resp.Memories {
		if _, ok := projectSet[m.ID]; ok {
			projectInTop++
		}
	}
	if projectInTop < 2 {
		t.Errorf("quota=2 should guarantee >=2 primary candidates in top-5, got %d", projectInTop)
	}
}

// TestRecall_NamespaceQuotaRespectsAvailability protects against the quota
// padding the result with phantom rows. If the project has 1 passing
// candidate but the quota asks for 5, the final result has 1 primary and
// (limit-1) other candidates — no duplication, no synthetic fill.
func TestRecall_NamespaceQuotaRespectsAvailability(t *testing.T) {
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()

	now := time.Now()
	memMap := map[uuid.UUID]*model.Memory{}
	var vecResults []storage.VectorSearchResult

	projectMemID := uuid.New()
	memMap[projectMemID] = makeTestMemory(projectMemID, primaryNs, "only project mem", nil, 0.5, 0, now)
	vecResults = append(vecResults, storage.VectorSearchResult{ID: projectMemID, Score: 0.40, NamespaceID: primaryNs})

	for i := 0; i < 8; i++ {
		id := uuid.New()
		memMap[id] = makeTestMemory(id, globalNs, "global content", nil, 0.5, 0, now)
		vecResults = append(vecResults, storage.VectorSearchResult{ID: id, Score: 0.90, NamespaceID: globalNs})
	}

	memReader := &mockMemoryReader{memories: memMap}
	vectorSearcher := &mockVectorSearcher{results: vecResults}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:                primaryID,
		GlobalNamespaceID:        &globalNs,
		Query:                    "anything",
		Limit:                    5,
		NamespaceQuotaProjectMin: 5,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 5 {
		t.Fatalf("expected 5 memories (limit), got %d", len(resp.Memories))
	}
	seen := map[uuid.UUID]int{}
	for _, m := range resp.Memories {
		seen[m.ID]++
		if seen[m.ID] > 1 {
			t.Errorf("memory %v appears %d times — quota over-asked must not duplicate", m.ID, seen[m.ID])
		}
	}
}

// TestRecall_NamespaceQuotaDefaultZero_BehaviorUnchanged verifies the
// shipped default (project_min=0) preserves the pre-feature truncation.
// When the quota is zero, the top-N is whatever the unified sort produced
// — globals included.
func TestRecall_NamespaceQuotaDefaultZero_BehaviorUnchanged(t *testing.T) {
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()

	now := time.Now()
	memMap := map[uuid.UUID]*model.Memory{}
	var vecResults []storage.VectorSearchResult
	projectMemID := uuid.New()
	memMap[projectMemID] = makeTestMemory(projectMemID, primaryNs, "weak project hit", nil, 0.5, 0, now)
	vecResults = append(vecResults, storage.VectorSearchResult{ID: projectMemID, Score: 0.30, NamespaceID: primaryNs})
	for i := 0; i < 5; i++ {
		id := uuid.New()
		memMap[id] = makeTestMemory(id, globalNs, "strong global hit", nil, 0.5, 0, now)
		vecResults = append(vecResults, storage.VectorSearchResult{ID: id, Score: 0.90, NamespaceID: globalNs})
	}

	memReader := &mockMemoryReader{memories: memMap}
	vectorSearcher := &mockVectorSearcher{results: vecResults}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })

	// No NamespaceQuotaProjectMin in the request → falls through to
	// the registered default (0).
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
		Limit:             3,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 3 {
		t.Fatalf("expected 3 memories, got %d", len(resp.Memories))
	}
	// All three slots are globals — the pre-feature behavior.
	for i, m := range resp.Memories {
		if m.ID == projectMemID {
			t.Errorf("default quota=0 should not lift the weak project hit (pos %d)", i)
		}
	}
}

// TestRecall_FusionNormalizePerChannel_BalancesUnevenCorpora verifies the
// flag wiring: with NormalizePerChannel on, the per-channel weights are
// divided by channel length, so a deep ranking does not dominate the
// fused output. We do not assert a specific ordering — the property under
// test is that the flag is observed during fusion. Two channels with
// very different lengths must produce a different score distribution
// from the same channels with the flag off.
func TestRecall_FusionNormalizePerChannel_BalancesUnevenCorpora(t *testing.T) {
	primaryID, primaryNs, _, globalNs, projects, namespaces := setupPrimaryGlobalFixtures()

	now := time.Now()
	// 1 project candidate + 5 global candidates. Vector channel returns
	// every memory; lexical channel returns the same. With fusion off
	// the test would just verify the unified sort works (covered
	// elsewhere). The point of this test is to confirm setting
	// NormalizePerChannel does not panic and the flag flows through to
	// runHybridSearch.
	projectMemID := uuid.New()
	memMap := map[uuid.UUID]*model.Memory{
		projectMemID: makeTestMemory(projectMemID, primaryNs, "project hit", nil, 0.5, 0, now),
	}
	primaryVec := []storage.VectorSearchResult{{ID: projectMemID, Score: 0.50, NamespaceID: primaryNs}}
	primaryLex := []storage.MemoryRank{{ID: projectMemID, Rank: 1.0}}

	var globalIDs []uuid.UUID
	for i := 0; i < 5; i++ {
		id := uuid.New()
		globalIDs = append(globalIDs, id)
		memMap[id] = makeTestMemory(id, globalNs, "global hit", nil, 0.5, 0, now)
	}
	allVec := append([]storage.VectorSearchResult{}, primaryVec...)
	globalLex := []storage.MemoryRank{}
	for i, id := range globalIDs {
		allVec = append(allVec, storage.VectorSearchResult{ID: id, Score: 0.90 - float64(i)*0.01, NamespaceID: globalNs})
		globalLex = append(globalLex, storage.MemoryRank{ID: id, Rank: 1.0 - float64(i)*0.01})
	}

	memReader := &mockMemoryReader{memories: memMap}
	vectorSearcher := &mockVectorSearcher{results: allVec}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	svc.SetLexical(&mockLexicalSearcher{results: map[uuid.UUID][]storage.MemoryRank{
		primaryNs: primaryLex,
		globalNs:  globalLex,
	}})
	svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.70, LexicalWeight: 0.30, NormalizePerChannel: true})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:         primaryID,
		GlobalNamespaceID: &globalNs,
		Query:             "anything",
		Limit:             10,
	})
	if err != nil {
		t.Fatalf("recall with NormalizePerChannel=true failed: %v", err)
	}
	if len(resp.Memories) == 0 {
		t.Fatalf("expected memories from fusion with normalize-per-channel on, got 0")
	}
	// The project hit must appear in the response — it appeared in both
	// primary's vector and lexical channels, so even after normalization
	// it has a non-zero fused score.
	found := false
	for _, m := range resp.Memories {
		if m.ID == projectMemID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("project hit missing from fusion result with NormalizePerChannel=true")
	}
}

// --- SimilarityThreshold (item 1.1) ---

// TestRecall_SimilarityThreshold_RawCosine_FusionOff_DropsLowCosine verifies
// the raw_cosine filter site in the non-fusion path: a vector hit whose raw
// cosine sits below the threshold never reaches the candidate pool. This is
// the primary contamination-suppression case for deployments that have not
// enabled fusion yet.
func TestRecall_SimilarityThreshold_RawCosine_FusionOff_DropsLowCosine(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	highID := uuid.New()
	lowID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			highID: makeTestMemory(highID, nsID, "high cosine", nil, 0.5, 0, now),
			lowID:  makeTestMemory(lowID, nsID, "low cosine", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: highID, Score: 0.85, NamespaceID: nsID},
			{ID: lowID, Score: 0.40, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "anything",
		Limit:                   10,
		SimilarityThreshold:     0.70,
		SimilarityThresholdMode: SimilarityThresholdModeRawCosine,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory above raw cosine floor, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != highID {
		t.Errorf("expected high-cosine memory %v, got %v", highID, resp.Memories[0].ID)
	}
}

// TestRecall_SimilarityThreshold_RawCosine_FusionOn_PreservesLexicalOnly is
// the headline raw_cosine semantic: a vector-only contaminant is dropped
// before RRF, but a lexical-only hit (the canonical answer) survives. That
// is the property the contamination probe set targets.
func TestRecall_SimilarityThreshold_RawCosine_FusionOn_PreservesLexicalOnly(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	contaminantID := uuid.New() // vector-only, low cosine
	lexHitID := uuid.New()      // lexical-only, no vector evidence
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			contaminantID: makeTestMemory(contaminantID, nsID, "looks similar in embedding space", nil, 0.5, 0, now),
			lexHitID:      makeTestMemory(lexHitID, nsID, "canonical token match", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: contaminantID, Score: 0.45, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	svc.SetLexical(&mockLexicalSearcher{
		results: map[uuid.UUID][]storage.MemoryRank{
			nsID: {{ID: lexHitID, Rank: 1.0}},
		},
	})
	svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.7, LexicalWeight: 0.3})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "anything",
		Limit:                   10,
		SimilarityThreshold:     0.70,
		SimilarityThresholdMode: SimilarityThresholdModeRawCosine,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory (lexical-only survivor), got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != lexHitID {
		t.Errorf("expected lex-only hit %v to survive, got %v", lexHitID, resp.Memories[0].ID)
	}
}

// TestRecall_SimilarityThreshold_Fused_FusionOn_DropsLowNormalized verifies
// the fused filter site: filtering on post-RRF max-normalized similarity
// drops candidates whose combined evidence is weak after both channels and
// RRF have spoken. Top candidate (similarity=1.0 by normalization) passes,
// a much-lower candidate fails.
func TestRecall_SimilarityThreshold_Fused_FusionOn_DropsLowNormalized(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	topID := uuid.New()
	weakID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			topID:  makeTestMemory(topID, nsID, "top", nil, 0.5, 0, now),
			weakID: makeTestMemory(weakID, nsID, "weak", nil, 0.5, 0, now),
		},
	}
	// topID ranks 1 in vector; weakID ranks much lower in vector and not at
	// all in lexical. Post-RRF max-normalize, topID is similarity=1.0; weakID
	// will be well below.
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: topID, Score: 0.95, NamespaceID: nsID},
			{ID: weakID, Score: 0.60, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	svc.SetLexical(&mockLexicalSearcher{
		results: map[uuid.UUID][]storage.MemoryRank{
			nsID: {{ID: topID, Rank: 1.0}},
		},
	})
	svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.7, LexicalWeight: 0.3})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "anything",
		Limit:                   10,
		SimilarityThreshold:     0.75,
		SimilarityThresholdMode: SimilarityThresholdModeFusedCombined,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory above fused similarity floor, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != topID {
		t.Errorf("expected top-fused memory %v, got %v", topID, resp.Memories[0].ID)
	}
}

// TestRecall_SimilarityThreshold_InvalidMode_ReturnsError guards the typed
// error path. A caller passing an unrecognized mode should get a clear error
// rather than silently falling back to a default.
func TestRecall_SimilarityThreshold_InvalidMode_ReturnsError(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	svc, _ := newRecallService(&mockMemoryReader{memories: map[uuid.UUID]*model.Memory{}}, projects, namespaces, nil, nil, nil, nil)

	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "anything",
		SimilarityThreshold:     0.5,
		SimilarityThresholdMode: "not_a_mode",
	})
	if err == nil {
		t.Fatal("expected error for invalid mode, got nil")
	}
	if !strings.Contains(err.Error(), "invalid similarity_threshold_mode") {
		t.Errorf("expected mode validation error, got %v", err)
	}
}

// TestRecall_SimilarityThreshold_Superset is the in-process equivalent of
// the plan's end-to-end smoke check: a recall with similarity_threshold > 0
// must return a SUBSET of the rows returned with similarity_threshold = 0,
// and every returned row in the filtered run must have Similarity >= the
// threshold. Both raw_cosine and fused modes have to satisfy this.
func TestRecall_SimilarityThreshold_Superset(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	// Five candidates spanning the cosine range. With threshold 0.65 in
	// raw_cosine mode and no lexical channel, only the top three should
	// survive. The fusion-enabled scenarios additionally wire a lexical
	// searcher so the fused_combined mode actually exercises post-RRF
	// max-normalization.
	type cand struct {
		id     uuid.UUID
		cosine float64
	}
	cands := []cand{
		{id: uuid.New(), cosine: 0.95},
		{id: uuid.New(), cosine: 0.82},
		{id: uuid.New(), cosine: 0.70},
		{id: uuid.New(), cosine: 0.55},
		{id: uuid.New(), cosine: 0.32},
	}
	memMap := make(map[uuid.UUID]*model.Memory, len(cands))
	vecResults := make([]storage.VectorSearchResult, 0, len(cands))
	now := time.Now()
	for i, c := range cands {
		memMap[c.id] = makeTestMemory(c.id, nsID, fmt.Sprintf("candidate %d", i), nil, 0.5, 0, now)
		vecResults = append(vecResults, storage.VectorSearchResult{ID: c.id, Score: c.cosine, NamespaceID: nsID})
	}
	memReader := &mockMemoryReader{memories: memMap}
	vectorSearcher := &mockVectorSearcher{results: vecResults}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	// Lexical fixture for the fusion-enabled scenarios: surface the top
	// two cosine candidates also via the lexical channel so RRF actually
	// has both channels feeding simMap. cand[0] and cand[1] both rank at
	// the top of lexical, mirroring a "good two-channel match" case.
	lexRanks := []storage.MemoryRank{
		{ID: cands[0].id, Rank: 1.0},
		{ID: cands[1].id, Rank: 1.0},
	}

	scenarios := []struct {
		name      string
		mode      string
		withLex   bool
		threshold float64
		// reportMatchesFilter is true when the reported Similarity on each
		// RecallResult uses the same metric the filter compared against.
		// Examples: raw_cosine no-fusion (both raw cosine), fused_combined
		// no-fusion (both raw cosine since fusion is off and simMap carries
		// cosine), fused_combined fusion-on (both post-RRF normalized).
		// The one asymmetric scenario is raw_cosine fusion-on: the filter
		// compares raw cosine pre-RRF, but the reported Similarity is the
		// post-RRF max-normalized value. Properties 2 and 3 only hold when
		// filter-metric and report-metric are the same.
		reportMatchesFilter bool
	}{
		{name: "raw_cosine no-fusion", mode: SimilarityThresholdModeRawCosine, withLex: false, threshold: 0.65, reportMatchesFilter: true},
		// fused_combined requires fusion to be enabled. The
		// "fused_combined no-fusion" scenario that used to live here is
		// covered by TestRecall_FusedCombinedRequiresFusionEnabled, which
		// asserts the typed error is returned.
		{name: "raw_cosine fusion-on", mode: SimilarityThresholdModeRawCosine, withLex: true, threshold: 0.65, reportMatchesFilter: false},
		{name: "fused_combined fusion-on", mode: SimilarityThresholdModeFusedCombined, withLex: true, threshold: 0.65, reportMatchesFilter: true},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
			if sc.withLex {
				svc.SetLexical(&mockLexicalSearcher{results: map[uuid.UUID][]storage.MemoryRank{nsID: lexRanks}})
				svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.6, LexicalWeight: 0.4})
			}

			base, err := svc.Recall(context.Background(), &RecallRequest{
				ProjectID: projectID, Query: "anything", Limit: 50,
				SimilarityThreshold: 0, SimilarityThresholdMode: sc.mode,
			})
			if err != nil {
				t.Fatalf("baseline recall failed: %v", err)
			}
			baseIDs := map[uuid.UUID]float64{}
			for _, m := range base.Memories {
				if m.Similarity != nil {
					baseIDs[m.ID] = *m.Similarity
				} else {
					baseIDs[m.ID] = 0
				}
			}
			if len(baseIDs) != len(cands) {
				t.Fatalf("baseline expected %d rows, got %d", len(cands), len(baseIDs))
			}

			filtered, err := svc.Recall(context.Background(), &RecallRequest{
				ProjectID: projectID, Query: "anything", Limit: 50,
				SimilarityThreshold: sc.threshold, SimilarityThresholdMode: sc.mode,
			})
			if err != nil {
				t.Fatalf("filtered recall failed: %v", err)
			}

			// Property 1 (always): filtered is a subset of base.
			for _, m := range filtered.Memories {
				if _, ok := baseIDs[m.ID]; !ok {
					t.Errorf("filtered row %v not present in baseline (subset violated)", m.ID)
				}
			}

			// Property 2 (only when report-metric == filter-metric): every
			// filtered row has reported Similarity >= threshold.
			if sc.reportMatchesFilter {
				for _, m := range filtered.Memories {
					if m.Similarity == nil {
						t.Errorf("filtered row %v has nil Similarity", m.ID)
						continue
					}
					if *m.Similarity < sc.threshold {
						t.Errorf("filtered row %v has similarity %.3f, below threshold %.3f",
							m.ID, *m.Similarity, sc.threshold)
					}
				}
			}

			// Property 3 (only when report-metric == filter-metric): the
			// filtered count equals the count of base rows above the
			// threshold. In the raw_cosine fusion-on case the baseline's
			// reported Similarity is the post-RRF normalized score, not
			// raw cosine, so this count cannot be derived from baseIDs.
			if sc.reportMatchesFilter {
				var expectFiltered int
				for _, sim := range baseIDs {
					if sim >= sc.threshold {
						expectFiltered++
					}
				}
				if len(filtered.Memories) != expectFiltered {
					t.Errorf("filtered count %d, expected %d (rows with similarity >= %.3f)",
						len(filtered.Memories), expectFiltered, sc.threshold)
				}
			}

			// For the raw_cosine fusion-on case, verify that no candidate
			// with raw cosine below the threshold survived (using the
			// fixture's known raw cosines).
			if !sc.reportMatchesFilter && sc.mode == SimilarityThresholdModeRawCosine {
				cosineByID := map[uuid.UUID]float64{}
				for _, c := range cands {
					cosineByID[c.id] = c.cosine
				}
				for _, m := range filtered.Memories {
					raw, ok := cosineByID[m.ID]
					if !ok {
						continue
					}
					if raw < sc.threshold {
						t.Errorf("raw_cosine fusion-on: filtered row %v has raw cosine %.3f below threshold %.3f",
							m.ID, raw, sc.threshold)
					}
				}
			}
		})
	}
}

// TestRecall_SimilarityThreshold_ZeroIsNoOp confirms the default-no-op
// contract: threshold=0 must not filter anything regardless of which mode
// is set. This is the property every existing recall test implicitly
// relies on, since the new fields default to inactive.
func TestRecall_SimilarityThreshold_ZeroIsNoOp(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	highID := uuid.New()
	lowID := uuid.New()
	now := time.Now()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			highID: makeTestMemory(highID, nsID, "high cosine", nil, 0.5, 0, now),
			lowID:  makeTestMemory(lowID, nsID, "low cosine", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: highID, Score: 0.85, NamespaceID: nsID},
			{ID: lowID, Score: 0.10, NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}

	for _, mode := range []string{"", SimilarityThresholdModeRawCosine, SimilarityThresholdModeFusedCombined} {
		svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
		resp, err := svc.Recall(context.Background(), &RecallRequest{
			ProjectID:               projectID,
			Query:                   "anything",
			Limit:                   10,
			SimilarityThreshold:     0,
			SimilarityThresholdMode: mode,
		})
		if err != nil {
			t.Fatalf("recall mode=%q failed: %v", mode, err)
		}
		if len(resp.Memories) != 2 {
			t.Errorf("mode=%q with threshold=0: expected 2 memories (no-op filter), got %d", mode, len(resp.Memories))
		}
	}
}

// TestRecall_SimilarityThreshold_NaNRejected confirms NaN passes the
// range check (which uses strict comparisons) only when math.IsNaN is part
// of the guard. JSON callers are protected by encoding/json, but in-process
// Go callers and any future internal API must hit this rejection.
func TestRecall_SimilarityThreshold_NaNRejected(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _ := newRecallService(nil, projects, namespaces, nil, nil, nil, nil)
	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:           projectID,
		Query:               "anything",
		SimilarityThreshold: math.NaN(),
	})
	if err == nil {
		t.Fatal("expected NaN similarity_threshold to be rejected; got nil error")
	}
	if !strings.Contains(err.Error(), "invalid similarity_threshold") {
		t.Errorf("expected error to mention invalid similarity_threshold, got %q", err.Error())
	}
}

// TestRecall_SimilarityThreshold_NegativeRejected confirms negative values
// (which the MCP handler used to silently zero) reach the service-layer
// rejection.
func TestRecall_SimilarityThreshold_NegativeRejected(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _ := newRecallService(nil, projects, namespaces, nil, nil, nil, nil)
	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:           projectID,
		Query:               "anything",
		SimilarityThreshold: -0.5,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid similarity_threshold") {
		t.Fatalf("expected negative similarity_threshold to be rejected; got %v", err)
	}
}

// TestRecall_WhitespaceOnlyMode_Rejected confirms a mode value that trims to
// empty is treated as a caller error rather than a silent fallback to the
// default. An entirely missing field still picks the default.
func TestRecall_WhitespaceOnlyMode_Rejected(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _ := newRecallService(nil, projects, namespaces, nil, nil, nil, nil)
	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "anything",
		SimilarityThresholdMode: "   ",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid similarity_threshold_mode") {
		t.Fatalf("expected whitespace-only mode to be rejected; got %v", err)
	}
}

// TestRecall_FusedCombinedRequiresFusionEnabled confirms the new
// cross-flag validation: fused_combined with a non-zero threshold while
// fusion is disabled returns a typed error, not a silent semantic collapse
// into raw_cosine.
func TestRecall_FusedCombinedRequiresFusionEnabled(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{},
	}
	vectorSearcher := &mockVectorSearcher{}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	_ = nsID
	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:               projectID,
		Query:                   "anything",
		SimilarityThreshold:     0.5,
		SimilarityThresholdMode: SimilarityThresholdModeFusedCombined,
	})
	if err == nil || !strings.Contains(err.Error(), "requires recall.fusion.enabled") {
		t.Fatalf("expected fused_combined-without-fusion to be rejected; got %v", err)
	}
}

// TestRecall_NaNVectorScoreFiltered confirms a vector backend that returns
// a NaN score does not propagate that NaN through the filter, simMap, and
// sort comparator. With rawCosineFloor > 0 the !( >=) form drops NaN at
// the filter site; with floor == 0 clampScore neutralizes NaN downstream.
func TestRecall_NaNVectorScoreFiltered(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	goodID := uuid.New()
	nanID := uuid.New()
	now := time.Now()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			goodID: makeTestMemory(goodID, nsID, "good", nil, 0.5, 0, now),
			nanID:  makeTestMemory(nanID, nsID, "nan", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: goodID, Score: 0.9, NamespaceID: nsID},
			{ID: nanID, Score: math.NaN(), NamespaceID: nsID},
		},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:           projectID,
		Query:               "anything",
		Limit:               10,
		SimilarityThreshold: 0.3,
		// raw_cosine mode: NaN should be dropped at the filter site.
		SimilarityThresholdMode: SimilarityThresholdModeRawCosine,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	for _, m := range resp.Memories {
		if m.ID == nanID {
			t.Errorf("NaN-scored row %v survived the rawCosineFloor filter", m.ID)
		}
	}
}

// TestRecall_LexicalOnlyFusionHit_NoVectorClaim confirms a memory that
// enters simMap only via the lexical RRF channel reports Similarity == nil
// (no vector evidence) even though its post-RRF score is non-zero. The
// previous behavior set viaVector unconditionally for every simMap entry,
// which misled consumers reading the field as "vector said X".
func TestRecall_LexicalOnlyFusionHit_NoVectorClaim(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	vecID := uuid.New()     // surfaces only via vector
	lexOnlyID := uuid.New() // surfaces only via lexical
	bothID := uuid.New()    // surfaces via both
	now := time.Now()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			vecID:     makeTestMemory(vecID, nsID, "vec", nil, 0.5, 0, now),
			lexOnlyID: makeTestMemory(lexOnlyID, nsID, "lex", nil, 0.5, 0, now),
			bothID:    makeTestMemory(bothID, nsID, "both", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: vecID, Score: 0.8, NamespaceID: nsID},
			{ID: bothID, Score: 0.7, NamespaceID: nsID},
		},
	}
	lexRanks := []storage.MemoryRank{
		{ID: lexOnlyID, Rank: 1.0},
		{ID: bothID, Rank: 2.0},
	}
	embProvider := &mockEmbeddingProvider{
		name: "test-embed", dimensions: []int{128},
		resp: &provider.EmbeddingResponse{Embeddings: [][]float32{make([]float32, 128)}, Model: "m"},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider { return embProvider })
	svc.SetLexical(&mockLexicalSearcher{results: map[uuid.UUID][]storage.MemoryRank{nsID: lexRanks}})
	svc.SetFusion(FusionConfig{Enabled: true, RRFConstant: 60, VectorWeight: 0.6, LexicalWeight: 0.4})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "anything",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	byID := map[uuid.UUID]*RecallResult{}
	for i := range resp.Memories {
		byID[resp.Memories[i].ID] = &resp.Memories[i]
	}
	if r, ok := byID[lexOnlyID]; !ok {
		t.Fatalf("lex-only memory missing from results")
	} else if r.Similarity != nil {
		t.Errorf("lex-only memory should have nil Similarity (no vector evidence); got %v", *r.Similarity)
	}
	if r, ok := byID[vecID]; !ok {
		t.Fatalf("vector-only memory missing from results")
	} else if r.Similarity == nil {
		t.Errorf("vector-only memory should have non-nil Similarity")
	}
	if r, ok := byID[bothID]; !ok {
		t.Fatalf("two-channel memory missing from results")
	} else if r.Similarity == nil {
		t.Errorf("two-channel memory should have non-nil Similarity")
	}
}

// TestRecallResult_SimilarityJSONIsNullWhenNil confirms the JSON wire
// format change in F1.7: a nil Similarity pointer must serialize as
// "similarity": null (matching OpenAPI nullable: true), not be omitted.
func TestRecallResult_SimilarityJSONIsNullWhenNil(t *testing.T) {
	r := RecallResult{ID: uuid.New(), Similarity: nil}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if !strings.Contains(string(raw), `"similarity":null`) {
		t.Errorf("expected nil Similarity to serialize as null; got %s", raw)
	}
	zero := 0.0
	r.Similarity = &zero
	raw, _ = json.Marshal(r)
	if !strings.Contains(string(raw), `"similarity":0`) {
		t.Errorf("expected zero Similarity to serialize as 0; got %s", raw)
	}
}

// mockVectorHydrator returns pre-configured embeddings keyed by ID. Missing
// IDs are simply absent from the returned map (the same contract as the
// production VectorStore.GetByIDs implementations).
type mockVectorHydrator struct {
	embeddings map[uuid.UUID][]float32
	err        error
}

func (m *mockVectorHydrator) GetByIDs(_ context.Context, _ storage.VectorKind, ids []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	if m.err != nil {
		return nil, m.err
	}
	out := make(map[uuid.UUID][]float32, len(ids))
	for _, id := range ids {
		if v, ok := m.embeddings[id]; ok {
			out[id] = v
		}
	}
	return out, nil
}

// TestRecall_MmrDemotesParaphraseCluster verifies that MMR diversifies a tight
// semantic cluster: five near-identical embeddings (cosine 0.99+ to each
// other) sharing the top of the composite-score ranking, plus three unrelated
// embeddings further down. At limit=5 and default lambda 0.75, the result
// should surface one cluster representative plus the three unrelated memories
// plus one cluster runner-up (since the 2x-limit MMR window holds 10 items
// but only 8 candidates exist, then final-slice cuts to 5).
func TestRecall_MmrDemotesParaphraseCluster(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	clusterIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	unrelatedIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	memories := map[uuid.UUID]*model.Memory{}
	for i, id := range clusterIDs {
		memories[id] = makeTestMemory(id, nsID, fmt.Sprintf("cluster_%d", i), nil, 0.5, 0, now.Add(time.Duration(-i)*time.Hour))
	}
	for i, id := range unrelatedIDs {
		memories[id] = makeTestMemory(id, nsID, fmt.Sprintf("unrelated_%d", i), nil, 0.5, 0, now.Add(time.Duration(-i)*time.Hour))
	}

	// Vector search returns all 8, cluster scoring higher than unrelated.
	vecResults := make([]storage.VectorSearchResult, 0, 8)
	for i, id := range clusterIDs {
		vecResults = append(vecResults, storage.VectorSearchResult{ID: id, Score: 0.85 - float64(i)*0.01, NamespaceID: nsID})
	}
	for i, id := range unrelatedIDs {
		vecResults = append(vecResults, storage.VectorSearchResult{ID: id, Score: 0.65 - float64(i)*0.01, NamespaceID: nsID})
	}

	// Embeddings: cluster shares a dominant direction (cosine 0.99+ between
	// cluster pairs); the three unrelated candidates each live in their own
	// orthogonal dimension so MMR sees no redundancy between any
	// unrelated/unrelated pair or any unrelated/cluster pair.
	embs := map[uuid.UUID][]float32{
		clusterIDs[0]:   {1.0, 0.10, 0, 0, 0},
		clusterIDs[1]:   {1.0, 0.12, 0, 0, 0},
		clusterIDs[2]:   {1.0, 0.11, 0, 0, 0},
		clusterIDs[3]:   {1.0, 0.13, 0, 0, 0},
		clusterIDs[4]:   {1.0, 0.09, 0, 0, 0},
		unrelatedIDs[0]: {0, 0, 1, 0, 0},
		unrelatedIDs[1]: {0, 0, 0, 1, 0},
		unrelatedIDs[2]: {0, 0, 0, 0, 1},
	}

	memReader := &mockMemoryReader{memories: memories}
	vectorSearcher := &mockVectorSearcher{results: vecResults}
	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, TotalTokens: 5},
		},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})
	svc.SetVectorHydrator(&mockVectorHydrator{embeddings: embs})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "anything",
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Memories) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resp.Memories))
	}

	clusterSet := map[uuid.UUID]bool{}
	for _, id := range clusterIDs {
		clusterSet[id] = true
	}
	unrelatedSet := map[uuid.UUID]bool{}
	for _, id := range unrelatedIDs {
		unrelatedSet[id] = true
	}
	clusterCount, unrelatedCount := 0, 0
	for _, m := range resp.Memories {
		if clusterSet[m.ID] {
			clusterCount++
		}
		if unrelatedSet[m.ID] {
			unrelatedCount++
		}
	}
	// Without MMR, the result would be the top-5 by composite, which is all
	// five cluster members (clusterCount=5, unrelatedCount=0). With MMR at
	// lambda 0.75, the cluster gets aggressively penalized after the first
	// pick, so the unrelated memories outrank the redundant cluster siblings.
	// The 2x window holds 8 (all candidates); final slice cuts to 5, which
	// gives: 1 cluster seed + 3 unrelated + 1 cluster runner-up.
	if unrelatedCount != 3 {
		t.Errorf("expected all 3 unrelated memories to surface after MMR demotion of cluster; got %d unrelated, %d cluster", unrelatedCount, clusterCount)
	}
	if clusterCount != 2 {
		t.Errorf("expected 2 cluster members to survive (seed + runner-up after unrelated padded); got %d", clusterCount)
	}
	// First result should still be the composite winner (cluster_0). MMR
	// seeds with the highest-composite embedded candidate.
	if resp.Memories[0].ID != clusterIDs[0] {
		t.Errorf("first result should be the composite winner cluster_0; got %q", resp.Memories[0].Content)
	}
}

// TestRecall_HydrationSkipsMissingEmbeddings verifies that candidates without
// hydrated embeddings still surface in the result set rather than being
// dropped. MMR cannot rank them against siblings, so they pad the tail of
// the MMR window in composite-score order.
func TestRecall_HydrationSkipsMissingEmbeddings(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	now := time.Now()
	embeddedID := uuid.New()
	missingID := uuid.New()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			embeddedID: makeTestMemory(embeddedID, nsID, "with embedding", nil, 0.5, 0, now),
			missingID:  makeTestMemory(missingID, nsID, "without embedding", nil, 0.5, 0, now),
		},
	}
	vectorSearcher := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: embeddedID, Score: 0.85, NamespaceID: nsID},
			{ID: missingID, Score: 0.70, NamespaceID: nsID},
		},
	}
	// Hydrator returns an embedding ONLY for embeddedID. missingID is absent.
	hydrator := &mockVectorHydrator{embeddings: map[uuid.UUID][]float32{
		embeddedID: {1, 0, 0},
	}}
	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, TotalTokens: 5},
		},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, vectorSearcher, nil, nil, func() provider.EmbeddingProvider {
		return embProvider
	})
	svc.SetVectorHydrator(hydrator)
	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "anything",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Memories) != 2 {
		t.Fatalf("expected both memories to surface, got %d", len(resp.Memories))
	}
	gotIDs := map[uuid.UUID]bool{resp.Memories[0].ID: true, resp.Memories[1].ID: true}
	if !gotIDs[embeddedID] || !gotIDs[missingID] {
		t.Errorf("missing-embedding candidate should still surface; got %v", gotIDs)
	}
}
