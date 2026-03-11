package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// --- Batch-specific mock helpers ---

// mockMemoryRepoWithFailures lets us fail on specific Create calls by index.
type mockMemoryRepoWithFailures struct {
	created    []*model.Memory
	failOnCall int // 0-indexed call number to fail on; -1 means never fail
	callCount  int
}

func (m *mockMemoryRepoWithFailures) Create(_ context.Context, mem *model.Memory) error {
	idx := m.callCount
	m.callCount++
	if m.failOnCall >= 0 && idx == m.failOnCall {
		return fmt.Errorf("simulated create failure at index %d", idx)
	}
	m.created = append(m.created, mem)
	return nil
}

func (m *mockMemoryRepoWithFailures) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for _, mem := range m.created {
		if mem.ID == id {
			return mem, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func newBatchTestService(
	projects *mockProjectRepo,
	namespaces *mockNamespaceRepo,
	embedFn func() provider.EmbeddingProvider,
) (*BatchStoreService, *mockMemoryRepo, *mockIngestionLogRepo, *mockTokenUsageRepo, *mockEnrichmentQueueRepo, *mockVectorStore) {
	memories := &mockMemoryRepo{}
	ingestion := &mockIngestionLogRepo{}
	tokenUsage := &mockTokenUsageRepo{}
	enrichment := &mockEnrichmentQueueRepo{}
	vectors := &mockVectorStore{}

	svc := NewBatchStoreService(memories, projects, namespaces, ingestion, tokenUsage, enrichment, vectors, embedFn)
	return svc, memories, ingestion, tokenUsage, enrichment, vectors
}

func newBatchTestServiceWithFailingMemory(
	projects *mockProjectRepo,
	namespaces *mockNamespaceRepo,
	embedFn func() provider.EmbeddingProvider,
	failOnCall int,
) (*BatchStoreService, *mockMemoryRepoWithFailures, *mockIngestionLogRepo, *mockTokenUsageRepo, *mockEnrichmentQueueRepo, *mockVectorStore) {
	memories := &mockMemoryRepoWithFailures{failOnCall: failOnCall}
	ingestion := &mockIngestionLogRepo{}
	tokenUsage := &mockTokenUsageRepo{}
	enrichment := &mockEnrichmentQueueRepo{}
	vectors := &mockVectorStore{}

	svc := NewBatchStoreService(memories, projects, namespaces, ingestion, tokenUsage, enrichment, vectors, embedFn)
	return svc, memories, ingestion, tokenUsage, enrichment, vectors
}

// --- Tests ---

func TestBatchStore_SuccessThreeItems(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	svc, memories, ingestion, _, _, _ := newBatchTestService(projects, namespaces, nil)

	resp, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "item one", Source: "test", Tags: []string{"a"}},
			{Content: "item two", Source: "test", Tags: []string{"b"}},
			{Content: "item three", Source: "test"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Processed != 3 {
		t.Errorf("expected processed=3, got %d", resp.Processed)
	}
	if resp.MemoriesCreated != 3 {
		t.Errorf("expected memories_created=3, got %d", resp.MemoriesCreated)
	}
	if len(resp.Errors) != 0 {
		t.Errorf("expected 0 errors, got %d", len(resp.Errors))
	}

	if len(memories.created) != 3 {
		t.Fatalf("expected 3 memories created, got %d", len(memories.created))
	}

	// Verify namespace assignment.
	for i, mem := range memories.created {
		if mem.NamespaceID != nsID {
			t.Errorf("memory %d: expected namespace %s, got %s", i, nsID, mem.NamespaceID)
		}
	}

	// Verify ingestion logs created for each item.
	if len(ingestion.logs) != 3 {
		t.Errorf("expected 3 ingestion logs, got %d", len(ingestion.logs))
	}
}

func TestBatchStore_WithEmbedding(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{
				make([]float32, 384),
				make([]float32, 384),
			},
			Model: "test-model",
			Usage: provider.TokenUsage{PromptTokens: 20, TotalTokens: 20},
		},
	}

	svc, memories, _, tokenUsage, _, vectors := newBatchTestService(projects, namespaces, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "first", Source: "test"},
			{Content: "second", Source: "test"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.MemoriesCreated != 2 {
		t.Errorf("expected 2 memories created, got %d", resp.MemoriesCreated)
	}

	// Verify embeddings applied to memories.
	for i, mem := range memories.created {
		if mem.EmbeddingDim == nil {
			t.Errorf("memory %d: expected EmbeddingDim set", i)
		} else if *mem.EmbeddingDim != 384 {
			t.Errorf("memory %d: expected EmbeddingDim 384, got %d", i, *mem.EmbeddingDim)
		}
	}

	// Verify vector upserts.
	if len(vectors.upserted) != 2 {
		t.Fatalf("expected 2 vector upserts, got %d", len(vectors.upserted))
	}

	// Verify single token usage record for the batch.
	if len(tokenUsage.usages) != 1 {
		t.Fatalf("expected 1 token usage record, got %d", len(tokenUsage.usages))
	}
	tu := tokenUsage.usages[0]
	if tu.TokensInput != 20 {
		t.Errorf("expected 20 input tokens, got %d", tu.TokensInput)
	}
	if tu.Provider != "test-provider" {
		t.Errorf("expected provider 'test-provider', got %q", tu.Provider)
	}
	if tu.Model != "test-model" {
		t.Errorf("expected model 'test-model', got %q", tu.Model)
	}
}

func TestBatchStore_PerItemErrors(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, ingestion, _, _, _ := newBatchTestServiceWithFailingMemory(projects, namespaces, nil, 1)

	resp, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "item one", Source: "test"},
			{Content: "item two (will fail)", Source: "test"},
			{Content: "item three", Source: "test"},
		},
	})
	if err != nil {
		t.Fatalf("batch store should not return top-level error for per-item failures: %v", err)
	}

	if resp.Processed != 3 {
		t.Errorf("expected processed=3, got %d", resp.Processed)
	}
	if resp.MemoriesCreated != 2 {
		t.Errorf("expected memories_created=2, got %d", resp.MemoriesCreated)
	}
	if len(resp.Errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(resp.Errors))
	}
	if resp.Errors[0].Index != 1 {
		t.Errorf("expected error at index 1, got %d", resp.Errors[0].Index)
	}

	// Only 2 memories should have been created.
	if len(memories.created) != 2 {
		t.Errorf("expected 2 memories created, got %d", len(memories.created))
	}

	// Only 2 ingestion logs for the successful items.
	if len(ingestion.logs) != 2 {
		t.Errorf("expected 2 ingestion logs, got %d", len(ingestion.logs))
	}
}

func TestBatchStore_EmptyItems(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, _, _ := newBatchTestService(projects, namespaces, nil)

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items:     []BatchStoreItem{},
	})
	if err == nil {
		t.Error("expected error for empty items list")
	}
}

func TestBatchStore_TooManyItems(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, _, _ := newBatchTestService(projects, namespaces, nil)

	items := make([]BatchStoreItem, 101)
	for i := range items {
		items[i] = BatchStoreItem{Content: fmt.Sprintf("item %d", i), Source: "test"}
	}

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items:     items,
	})
	if err == nil {
		t.Error("expected error for too many items")
	}
}

func TestBatchStore_ProjectNotFound(t *testing.T) {
	_, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, _, _ := newBatchTestService(projects, namespaces, nil)

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: uuid.New(), // non-existent
		Items: []BatchStoreItem{
			{Content: "test", Source: "test"},
		},
	})
	if err == nil {
		t.Error("expected error for non-existent project")
	}
}

func TestBatchStore_EmbeddingFailure(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "failing-provider",
		dimensions: []int{384},
		err:        fmt.Errorf("embedding service unavailable"),
	}

	svc, memories, _, tokenUsage, _, vectors := newBatchTestService(projects, namespaces, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "still stored", Source: "test"},
			{Content: "also stored", Source: "test"},
		},
	})
	if err != nil {
		t.Fatalf("batch store should succeed even when embedding fails: %v", err)
	}

	if resp.MemoriesCreated != 2 {
		t.Errorf("expected 2 memories created, got %d", resp.MemoriesCreated)
	}

	// Memories created without embeddings.
	for i, mem := range memories.created {
		if mem.EmbeddingDim != nil {
			t.Errorf("memory %d: expected nil EmbeddingDim on embedding failure", i)
		}
	}

	// No vectors upserted.
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}

	// No token usage recorded.
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestBatchStore_EnrichmentQueueing(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, enrichment, _ := newBatchTestService(projects, namespaces, nil)

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "enrich me", Source: "test"},
			{Content: "enrich me too", Source: "test"},
		},
		Options: StoreOptions{Enrich: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(enrichment.jobs) != 2 {
		t.Fatalf("expected 2 enrichment jobs, got %d", len(enrichment.jobs))
	}

	for i, job := range enrichment.jobs {
		if job.Status != "pending" {
			t.Errorf("job %d: expected status 'pending', got %q", i, job.Status)
		}
		if job.MaxAttempts != 3 {
			t.Errorf("job %d: expected max_attempts 3, got %d", i, job.MaxAttempts)
		}
	}
}

func TestBatchStore_LatencyTracked(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, _, _ := newBatchTestService(projects, namespaces, nil)

	resp, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "latency test", Source: "test"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestBatchStore_TokenUsageRecorded(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	userID := uuid.New()
	orgID := uuid.New()
	apiKeyID := uuid.New()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{256},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{
				make([]float32, 256),
				make([]float32, 256),
				make([]float32, 256),
			},
			Model: "embed-model",
			Usage: provider.TokenUsage{PromptTokens: 30, CompletionTokens: 0, TotalTokens: 30},
		},
	}

	svc, _, _, tokenUsage, _, _ := newBatchTestService(projects, namespaces, func() provider.EmbeddingProvider {
		return embProvider
	})

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "one", Source: "test"},
			{Content: "two", Source: "test"},
			{Content: "three", Source: "test"},
		},
		UserID:   &userID,
		OrgID:    &orgID,
		APIKeyID: &apiKeyID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Single token usage record for the entire batch.
	if len(tokenUsage.usages) != 1 {
		t.Fatalf("expected 1 token usage record, got %d", len(tokenUsage.usages))
	}

	tu := tokenUsage.usages[0]
	if tu.Operation != "embedding" {
		t.Errorf("expected operation 'embedding', got %q", tu.Operation)
	}
	if tu.Provider != "test-provider" {
		t.Errorf("expected provider 'test-provider', got %q", tu.Provider)
	}
	if tu.Model != "embed-model" {
		t.Errorf("expected model 'embed-model', got %q", tu.Model)
	}
	if tu.TokensInput != 30 {
		t.Errorf("expected 30 input tokens, got %d", tu.TokensInput)
	}
	if *tu.UserID != userID {
		t.Errorf("expected user ID %s, got %s", userID, *tu.UserID)
	}
	if *tu.OrgID != orgID {
		t.Errorf("expected org ID %s, got %s", orgID, *tu.OrgID)
	}
	if *tu.APIKeyID != apiKeyID {
		t.Errorf("expected API key ID %s, got %s", apiKeyID, *tu.APIKeyID)
	}
	// MemoryID should be nil for batch-level token usage.
	if tu.MemoryID != nil {
		t.Errorf("expected nil MemoryID for batch token usage, got %s", tu.MemoryID)
	}
}

func TestBatchStore_NilProjectID(t *testing.T) {
	_, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, _, _ := newBatchTestService(projects, namespaces, nil)

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: uuid.Nil,
		Items: []BatchStoreItem{
			{Content: "test", Source: "test"},
		},
	})
	if err == nil {
		t.Error("expected error for nil project ID")
	}
}

func TestBatchStore_MetadataPreserved(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, _, _, _, _ := newBatchTestService(projects, namespaces, nil)

	meta := json.RawMessage(`{"key":"value"}`)

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "with meta", Source: "test", Metadata: meta, Tags: []string{"t1", "t2"}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(memories.created) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(memories.created))
	}
	mem := memories.created[0]
	if string(mem.Metadata) != `{"key":"value"}` {
		t.Errorf("expected metadata preserved, got %s", string(mem.Metadata))
	}
	if len(mem.Tags) != 2 || mem.Tags[0] != "t1" || mem.Tags[1] != "t2" {
		t.Errorf("expected tags [t1 t2], got %v", mem.Tags)
	}
}
