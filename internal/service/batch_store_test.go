package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
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
) (*BatchStoreService, *mockMemoryRepo, *mockIngestionLogRepo, *mockEnrichmentQueueRepo) {
	memories := &mockMemoryRepo{}
	ingestion := &mockIngestionLogRepo{}
	enrichment := &mockEnrichmentQueueRepo{}

	svc := NewBatchStoreService(memories, projects, namespaces, ingestion, enrichment)
	return svc, memories, ingestion, enrichment
}

func newBatchTestServiceWithFailingMemory(
	projects *mockProjectRepo,
	namespaces *mockNamespaceRepo,
	failOnCall int,
) (*BatchStoreService, *mockMemoryRepoWithFailures, *mockIngestionLogRepo, *mockEnrichmentQueueRepo) {
	memories := &mockMemoryRepoWithFailures{failOnCall: failOnCall}
	ingestion := &mockIngestionLogRepo{}
	enrichment := &mockEnrichmentQueueRepo{}

	svc := NewBatchStoreService(memories, projects, namespaces, ingestion, enrichment)
	return svc, memories, ingestion, enrichment
}

// --- Tests ---

func TestBatchStore_SuccessThreeItems(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	svc, memories, ingestion, enrichment := newBatchTestService(projects, namespaces)

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
		if mem.EmbeddingDim != nil {
			t.Errorf("memory %d: service layer should not set EmbeddingDim", i)
		}
	}

	// Verify ingestion logs created for each item.
	if len(ingestion.logs) != 3 {
		t.Errorf("expected 3 ingestion logs, got %d", len(ingestion.logs))
	}

	// Every memory must produce an enrichment job regardless of the Enrich
	// flag — embedding, vector upsert, and token usage are the worker's job.
	if len(enrichment.jobs) != 3 {
		t.Errorf("expected 3 enrichment jobs, got %d", len(enrichment.jobs))
	}
}

func TestBatchStore_PerItemErrors(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, ingestion, enrichment := newBatchTestServiceWithFailingMemory(projects, namespaces, 1)

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

	if len(memories.created) != 2 {
		t.Errorf("expected 2 memories created, got %d", len(memories.created))
	}
	if len(ingestion.logs) != 2 {
		t.Errorf("expected 2 ingestion logs, got %d", len(ingestion.logs))
	}
	// Failed item should not produce a job.
	if len(enrichment.jobs) != 2 {
		t.Errorf("expected 2 enrichment jobs (one per successful memory), got %d", len(enrichment.jobs))
	}
}

func TestBatchStore_EmptyItems(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newBatchTestService(projects, namespaces)

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
	svc, _, _, _ := newBatchTestService(projects, namespaces)

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
	svc, _, _, _ := newBatchTestService(projects, namespaces)

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

func TestBatchStore_EnrichmentQueueingUnconditional(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, enrichment := newBatchTestService(projects, namespaces)

	// Enrich=false should still enqueue jobs — the flag is on a deprecation
	// path. Every memory goes through the worker for embedding regardless.
	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "a", Source: "test"},
			{Content: "b", Source: "test"},
		},
		Options: StoreOptions{Enrich: false},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(enrichment.jobs) != 2 {
		t.Fatalf("expected 2 enrichment jobs even with Enrich=false, got %d", len(enrichment.jobs))
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
	svc, _, _, _ := newBatchTestService(projects, namespaces)

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

func TestBatchStore_NilProjectID(t *testing.T) {
	_, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newBatchTestService(projects, namespaces)

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
	svc, memories, _, _ := newBatchTestService(projects, namespaces)

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

func TestBatchStore_ExtractRejected(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newBatchTestService(projects, namespaces)

	_, err := svc.BatchStore(context.Background(), &BatchStoreRequest{
		ProjectID: projectID,
		Items: []BatchStoreItem{
			{Content: "test", Source: "test"},
		},
		Options: StoreOptions{Extract: true},
	})
	if err == nil {
		t.Error("expected error when Extract=true (not yet implemented)")
	}
}
