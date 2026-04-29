package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// mockEmbeddingProvider is shared by extract, recall, and update service
// tests; StoreService does not use it.
type mockEmbeddingProvider struct {
	name       string
	dimensions []int
	resp       *provider.EmbeddingResponse
	err        error
}

func (m *mockEmbeddingProvider) Embed(_ context.Context, _ *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

func (m *mockEmbeddingProvider) Name() string      { return m.name }
func (m *mockEmbeddingProvider) Dimensions() []int { return m.dimensions }

// --- Mock implementations ---

type mockMemoryRepo struct {
	created []*model.Memory
	getErr  error
}

func (m *mockMemoryRepo) Create(_ context.Context, mem *model.Memory) error {
	m.created = append(m.created, mem)
	return nil
}

func (m *mockMemoryRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	for _, mem := range m.created {
		if mem.ID == id {
			return mem, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockMemoryRepo) LookupByContentHash(_ context.Context, namespaceID uuid.UUID, hash string) (*model.Memory, error) {
	for _, mem := range m.created {
		if mem.NamespaceID != namespaceID {
			continue
		}
		memHash := mem.ContentHash
		if memHash == "" {
			memHash = storage.HashContent(mem.Content)
		}
		if memHash == hash {
			return mem, nil
		}
	}
	return nil, sql.ErrNoRows
}

type mockProjectRepo struct {
	projects map[uuid.UUID]*model.Project
}

func (m *mockProjectRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	p, ok := m.projects[id]
	if !ok {
		return nil, fmt.Errorf("project not found")
	}
	return p, nil
}

func (m *mockProjectRepo) GetByNamespaceID(_ context.Context, namespaceID uuid.UUID) (*model.Project, error) {
	for _, p := range m.projects {
		if p.NamespaceID == namespaceID {
			return p, nil
		}
	}
	return nil, fmt.Errorf("project not found for namespace")
}

type mockNamespaceRepo struct {
	namespaces map[uuid.UUID]*model.Namespace
}

func (m *mockNamespaceRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Namespace, error) {
	ns, ok := m.namespaces[id]
	if !ok {
		return nil, fmt.Errorf("namespace not found")
	}
	return ns, nil
}

type mockIngestionLogRepo struct {
	logs []*model.IngestionLog
}

func (m *mockIngestionLogRepo) Create(_ context.Context, log *model.IngestionLog) error {
	m.logs = append(m.logs, log)
	return nil
}

type mockTokenUsageRepo struct {
	usages []*model.TokenUsage
}

func (m *mockTokenUsageRepo) Record(_ context.Context, usage *model.TokenUsage) error {
	m.usages = append(m.usages, usage)
	return nil
}

type mockEnrichmentQueueRepo struct {
	jobs []*model.EnrichmentJob
}

func (m *mockEnrichmentQueueRepo) Enqueue(_ context.Context, item *model.EnrichmentJob) error {
	m.jobs = append(m.jobs, item)
	return nil
}

type mockVectorStore struct {
	upserted []struct {
		ID          uuid.UUID
		NamespaceID uuid.UUID
		Embedding   []float32
		Dimension   int
	}
	// upsertErr, when non-nil, is returned by every Upsert call. Tests
	// that drive the write-path failure branch set this.
	upsertErr error
}

func (m *mockVectorStore) Upsert(_ context.Context, _ storage.VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	m.upserted = append(m.upserted, struct {
		ID          uuid.UUID
		NamespaceID uuid.UUID
		Embedding   []float32
		Dimension   int
	}{id, namespaceID, embedding, dimension})
	return nil
}

// --- Test helpers ---

func setupTestFixtures() (uuid.UUID, uuid.UUID, *mockProjectRepo, *mockNamespaceRepo) {
	projectID := uuid.New()
	nsID := uuid.New()

	projects := &mockProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: nsID,
				Name:        "Test Project",
				Slug:        "test-project",
			},
		},
	}

	namespaces := &mockNamespaceRepo{
		namespaces: map[uuid.UUID]*model.Namespace{
			nsID: {
				ID:   nsID,
				Name: "Test NS",
				Slug: "test-ns",
				Kind: "project",
				Path: "test-ns",
			},
		},
	}

	return projectID, nsID, projects, namespaces
}

func newTestService(
	projects *mockProjectRepo,
	namespaces *mockNamespaceRepo,
) (*StoreService, *mockMemoryRepo, *mockIngestionLogRepo, *mockEnrichmentQueueRepo) {
	memories := &mockMemoryRepo{}
	ingestion := &mockIngestionLogRepo{}
	enrichment := &mockEnrichmentQueueRepo{}

	svc := NewStoreService(memories, projects, namespaces, ingestion, enrichment)
	return svc, memories, ingestion, enrichment
}

// --- Tests ---

func TestStore_Success(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, ingestion, enrichment := newTestService(projects, namespaces)

	resp, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Hello world",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.ID == uuid.Nil {
		t.Error("expected non-nil ID")
	}
	if resp.ProjectID != projectID {
		t.Errorf("expected project ID %s, got %s", projectID, resp.ProjectID)
	}
	if resp.ProjectSlug != "test-project" {
		t.Errorf("expected slug 'test-project', got %q", resp.ProjectSlug)
	}
	if resp.Path != "test-ns" {
		t.Errorf("expected path 'test-ns', got %q", resp.Path)
	}
	if resp.Content != "Hello world" {
		t.Errorf("expected content 'Hello world', got %q", resp.Content)
	}
	if resp.Enriched {
		t.Error("expected enriched=false")
	}
	if resp.LatencyMs < 0 {
		t.Error("expected non-negative latency")
	}

	if len(memories.created) != 1 {
		t.Fatalf("expected 1 memory created, got %d", len(memories.created))
	}
	mem := memories.created[0]
	if mem.Confidence != 1.0 {
		t.Errorf("expected confidence 1.0, got %f", mem.Confidence)
	}
	if mem.Importance != 0.5 {
		t.Errorf("expected importance 0.5, got %f", mem.Importance)
	}
	if mem.EmbeddingDim != nil {
		t.Error("expected nil EmbeddingDim — service layer no longer embeds")
	}

	if len(ingestion.logs) != 1 {
		t.Fatalf("expected 1 ingestion log, got %d", len(ingestion.logs))
	}

	// Every store must enqueue exactly one enrichment job, regardless of the
	// Options.Enrich flag. The worker is responsible for embedding, fact/entity
	// extraction, and token-usage recording.
	if len(enrichment.jobs) != 1 {
		t.Fatalf("expected 1 enrichment job enqueued, got %d", len(enrichment.jobs))
	}
	if !resp.EnrichmentQueued {
		t.Error("expected EnrichmentQueued=true")
	}
	job := enrichment.jobs[0]
	if job.MemoryID != resp.ID {
		t.Errorf("expected job memory ID %s, got %s", resp.ID, job.MemoryID)
	}
	if job.Status != "pending" {
		t.Errorf("expected status 'pending', got %q", job.Status)
	}
	if job.MaxAttempts != 3 {
		t.Errorf("expected max_attempts 3, got %d", job.MaxAttempts)
	}
}

func TestStore_EnqueuesRegardlessOfEnrichFlag(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	// Enrich=false should still produce a job — the flag is on a deprecation
	// path and must not gate the async embedding/enrichment work.
	svc, _, _, enrichment := newTestService(projects, namespaces)
	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "no-flag",
		Source:    "test",
		Options:   StoreOptions{Enrich: false},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(enrichment.jobs) != 1 {
		t.Fatalf("expected 1 job even when Enrich=false, got %d", len(enrichment.jobs))
	}
}

func TestStore_WithTagsAndMetadata(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, _, _ := newTestService(projects, namespaces)

	meta := json.RawMessage(`{"key":"value"}`)
	tags := []string{"tag1", "tag2"}

	resp, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Tagged content",
		Source:    "test",
		Tags:     tags,
		Metadata: meta,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Tags) != 2 || resp.Tags[0] != "tag1" || resp.Tags[1] != "tag2" {
		t.Errorf("expected tags [tag1 tag2], got %v", resp.Tags)
	}

	mem := memories.created[0]
	if len(mem.Tags) != 2 {
		t.Errorf("expected 2 tags on memory, got %d", len(mem.Tags))
	}
	if string(mem.Metadata) != `{"key":"value"}` {
		t.Errorf("expected metadata {\"key\":\"value\"}, got %s", string(mem.Metadata))
	}
}

func TestStore_WithTTL(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, _, _ := newTestService(projects, namespaces)

	before := time.Now()

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "TTL content",
		Source:    "test",
		Options:   StoreOptions{TTL: "7d"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mem := memories.created[0]
	if mem.ExpiresAt == nil {
		t.Fatal("expected ExpiresAt to be set")
	}

	expectedMin := before.Add(7 * 24 * time.Hour)
	expectedMax := time.Now().Add(7*24*time.Hour + time.Second)

	if mem.ExpiresAt.Before(expectedMin) || mem.ExpiresAt.After(expectedMax) {
		t.Errorf("ExpiresAt %v outside expected range [%v, %v]", mem.ExpiresAt, expectedMin, expectedMax)
	}
}

func TestStore_InvalidTTL(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newTestService(projects, namespaces)

	cases := []struct {
		name string
		ttl  string
	}{
		{"no unit", "30"},
		{"no number", "d"},
		{"invalid unit", "30x"},
		{"empty after trim", "   "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Store(context.Background(), &StoreRequest{
				ProjectID: projectID,
				Content:   "test",
				Source:    "test",
				Options:   StoreOptions{TTL: tc.ttl},
			})
			if err == nil {
				t.Error("expected error for invalid TTL")
			}
		})
	}
}

func TestStore_EmptyContent(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newTestService(projects, namespaces)

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for empty content")
	}

	_, err = svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "   ",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for whitespace-only content")
	}
}

func TestStore_ProjectNotFound(t *testing.T) {
	_, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newTestService(projects, namespaces)

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: uuid.New(), // non-existent
		Content:   "test",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for non-existent project")
	}
}

func TestStore_IngestionLogCreated(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	svc, _, ingestion, _ := newTestService(projects, namespaces)

	resp, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "log this",
		Source:    "api",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(ingestion.logs) != 1 {
		t.Fatalf("expected 1 ingestion log, got %d", len(ingestion.logs))
	}

	log := ingestion.logs[0]
	if log.NamespaceID != nsID {
		t.Errorf("expected namespace ID %s, got %s", nsID, log.NamespaceID)
	}
	if log.Source != "api" {
		t.Errorf("expected source 'api', got %q", log.Source)
	}
	if log.RawContent != "log this" {
		t.Errorf("expected raw content 'log this', got %q", log.RawContent)
	}
	if log.Status != "completed" {
		t.Errorf("expected status 'completed', got %q", log.Status)
	}
	if len(log.MemoryIDs) != 1 || log.MemoryIDs[0] != resp.ID {
		t.Errorf("expected memory ID %s in log, got %v", resp.ID, log.MemoryIDs)
	}
}

func TestStore_LatencyTracking(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newTestService(projects, namespaces)

	resp, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "latency test",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestParseTTL(t *testing.T) {
	cases := []struct {
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{"30d", 30 * 24 * time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},
		{"24h", 24 * time.Hour, false},
		{"1h", time.Hour, false},
		{"60m", 60 * time.Minute, false},
		{"30s", 30 * time.Second, false},
		{"", 0, true},
		{"d", 0, true},
		{"30", 0, true},
		{"30x", 0, true},
		{"abc", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseTTL(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("parseTTL(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestStore_NilProjectID(t *testing.T) {
	_, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newTestService(projects, namespaces)

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: uuid.Nil,
		Content:   "test",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for nil project ID")
	}
}

func TestStore_ExtractRejected(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _ := newTestService(projects, namespaces)

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "test",
		Source:    "test",
		Options:   StoreOptions{Extract: true},
	})
	if err == nil {
		t.Error("expected error when Extract=true (not yet implemented)")
	}
}
