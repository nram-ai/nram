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

type mockVectorSearcher struct {
	results []storage.VectorSearchResult
	err     error
}

func (m *mockVectorSearcher) Search(_ context.Context, _ []float32, _ uuid.UUID, _ int, topK int) ([]storage.VectorSearchResult, error) {
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
	svc := NewRecallService(memories, projects, namespaces, tokenUsage, vectorSearch, entityReader, traverser, &mockMemoryShareReader{}, embedFn)
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
