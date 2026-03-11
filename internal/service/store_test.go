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
)

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
}

func (m *mockVectorStore) Upsert(_ context.Context, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	m.upserted = append(m.upserted, struct {
		ID          uuid.UUID
		NamespaceID uuid.UUID
		Embedding   []float32
		Dimension   int
	}{id, namespaceID, embedding, dimension})
	return nil
}

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
func (m *mockEmbeddingProvider) Dimensions() []int  { return m.dimensions }

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
	embedFn func() provider.EmbeddingProvider,
) (*StoreService, *mockMemoryRepo, *mockIngestionLogRepo, *mockTokenUsageRepo, *mockEnrichmentQueueRepo, *mockVectorStore) {
	memories := &mockMemoryRepo{}
	ingestion := &mockIngestionLogRepo{}
	tokenUsage := &mockTokenUsageRepo{}
	enrichment := &mockEnrichmentQueueRepo{}
	vectors := &mockVectorStore{}

	svc := NewStoreService(memories, projects, namespaces, ingestion, tokenUsage, enrichment, vectors, embedFn)
	return svc, memories, ingestion, tokenUsage, enrichment, vectors
}

// --- Tests ---

func TestStore_SuccessWithoutEmbedding(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, ingestion, _, _, _ := newTestService(projects, namespaces, nil)

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
		t.Error("expected nil EmbeddingDim without provider")
	}

	if len(ingestion.logs) != 1 {
		t.Fatalf("expected 1 ingestion log, got %d", len(ingestion.logs))
	}
}

func TestStore_SuccessWithEmbedding(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "test-model",
			Usage:      provider.TokenUsage{PromptTokens: 10, TotalTokens: 10},
		},
	}

	svc, memories, _, tokenUsage, _, vectors := newTestService(projects, namespaces, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Embedded content",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.ID == uuid.Nil {
		t.Error("expected non-nil ID")
	}

	mem := memories.created[0]
	if mem.EmbeddingDim == nil {
		t.Fatal("expected EmbeddingDim to be set")
	}
	if *mem.EmbeddingDim != 384 {
		t.Errorf("expected EmbeddingDim 384, got %d", *mem.EmbeddingDim)
	}

	if len(vectors.upserted) != 1 {
		t.Fatalf("expected 1 vector upsert, got %d", len(vectors.upserted))
	}
	if vectors.upserted[0].Dimension != 384 {
		t.Errorf("expected dimension 384, got %d", vectors.upserted[0].Dimension)
	}

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
	if tu.Model != "test-model" {
		t.Errorf("expected model 'test-model', got %q", tu.Model)
	}
	if tu.TokensInput != 10 {
		t.Errorf("expected 10 input tokens, got %d", tu.TokensInput)
	}
}

func TestStore_WithTagsAndMetadata(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, memories, _, _, _, _ := newTestService(projects, namespaces, nil)

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
	svc, memories, _, _, _, _ := newTestService(projects, namespaces, nil)

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

func TestStore_WithEnrich(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, enrichment, _ := newTestService(projects, namespaces, nil)

	resp, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Enrich me",
		Source:    "test",
		Options:   StoreOptions{Enrich: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.EnrichmentQueued {
		t.Error("expected EnrichmentQueued=true")
	}

	if len(enrichment.jobs) != 1 {
		t.Fatalf("expected 1 enrichment job, got %d", len(enrichment.jobs))
	}

	job := enrichment.jobs[0]
	if job.Status != "pending" {
		t.Errorf("expected status 'pending', got %q", job.Status)
	}
	if job.MaxAttempts != 3 {
		t.Errorf("expected max_attempts 3, got %d", job.MaxAttempts)
	}
	if job.MemoryID != resp.ID {
		t.Errorf("expected job memory ID %s, got %s", resp.ID, job.MemoryID)
	}
}

func TestStore_InvalidTTL(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, _, _ := newTestService(projects, namespaces, nil)

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
	svc, _, _, _, _, _ := newTestService(projects, namespaces, nil)

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
	svc, _, _, _, _, _ := newTestService(projects, namespaces, nil)

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: uuid.New(), // non-existent
		Content:   "test",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for non-existent project")
	}
}

func TestStore_EmbeddingProviderError(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	embProvider := &mockEmbeddingProvider{
		name:       "failing-provider",
		dimensions: []int{384},
		err:        fmt.Errorf("embedding service unavailable"),
	}

	svc, memories, _, tokenUsage, _, vectors := newTestService(projects, namespaces, func() provider.EmbeddingProvider {
		return embProvider
	})

	resp, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Content despite embedding failure",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("store should succeed even when embedding fails, got: %v", err)
	}

	if resp.ID == uuid.Nil {
		t.Error("expected non-nil ID")
	}

	// Memory should be created without embedding.
	if len(memories.created) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(memories.created))
	}
	if memories.created[0].EmbeddingDim != nil {
		t.Error("expected nil EmbeddingDim on embedding failure")
	}

	// No vector should be upserted.
	if len(vectors.upserted) != 0 {
		t.Errorf("expected 0 vector upserts, got %d", len(vectors.upserted))
	}

	// No token usage should be recorded.
	if len(tokenUsage.usages) != 0 {
		t.Errorf("expected 0 token usage records, got %d", len(tokenUsage.usages))
	}
}

func TestStore_TokenUsageRecorded(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	userID := uuid.New()
	orgID := uuid.New()
	apiKeyID := uuid.New()

	embProvider := &mockEmbeddingProvider{
		name:       "test-provider",
		dimensions: []int{256},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 256)},
			Model:      "embed-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, CompletionTokens: 0, TotalTokens: 5},
		},
	}

	svc, _, _, tokenUsage, _, _ := newTestService(projects, namespaces, func() provider.EmbeddingProvider {
		return embProvider
	})

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "track usage",
		Source:    "test",
		UserID:    &userID,
		OrgID:     &orgID,
		APIKeyID:  &apiKeyID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tokenUsage.usages) != 1 {
		t.Fatalf("expected 1 usage record, got %d", len(tokenUsage.usages))
	}

	tu := tokenUsage.usages[0]
	if *tu.UserID != userID {
		t.Errorf("expected user ID %s, got %s", userID, *tu.UserID)
	}
	if *tu.OrgID != orgID {
		t.Errorf("expected org ID %s, got %s", orgID, *tu.OrgID)
	}
	if *tu.APIKeyID != apiKeyID {
		t.Errorf("expected API key ID %s, got %s", apiKeyID, *tu.APIKeyID)
	}
	if tu.MemoryID == nil {
		t.Error("expected memory ID to be set")
	}
}

func TestStore_IngestionLogCreated(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	svc, _, ingestion, _, _, _ := newTestService(projects, namespaces, nil)

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
	svc, _, _, _, _, _ := newTestService(projects, namespaces, nil)

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
	svc, _, _, _, _, _ := newTestService(projects, namespaces, nil)

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: uuid.Nil,
		Content:   "test",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for nil project ID")
	}
}

func TestStore_EnrichAndExtractRejected(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()
	svc, _, _, _, _, _ := newTestService(projects, namespaces, nil)

	_, err := svc.Store(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "test",
		Source:    "test",
		Options:   StoreOptions{Enrich: true, Extract: true},
	})
	if err == nil {
		t.Error("expected error when both enrich and extract are true")
	}
}
