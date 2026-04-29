package service

import (
	"context"
	"encoding/json"
	"fmt"
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
}

func (m *mockRelTraverser) TraverseFromEntity(_ context.Context, _ uuid.UUID, _ int) ([]model.Relationship, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.rels, nil
}

type mockMemoryShareReader struct {
	shares []model.MemoryShare
}

func (m *mockMemoryShareReader) ListSharedToNamespace(_ context.Context, _ uuid.UUID) ([]model.MemoryShare, error) {
	return m.shares, nil
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
	svc := NewRecallService(memories, projects, namespaces, vectorSearch, entityReader, traverser, &mockMemoryShareReader{}, wrapped)
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
