package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock dependencies ---

type mockDatabasePinger struct {
	backend string
	pingErr error
}

func (m *mockDatabasePinger) Backend() string              { return m.backend }
func (m *mockDatabasePinger) Ping(_ context.Context) error { return m.pingErr }

type mockProviderRegistry struct {
	embedding provider.EmbeddingProvider
	fact      provider.LLMProvider
	entity    provider.LLMProvider
}

func (m *mockProviderRegistry) GetEmbedding() provider.EmbeddingProvider { return m.embedding }

// GetLLM resolves an LLM slot by name, mirroring the real registry's
// fallback: the optional slots resolve to the fact provider when no dedicated
// provider is set.
func (m *mockProviderRegistry) GetLLM(name string) provider.LLMProvider {
	switch name {
	case provider.SlotFact:
		return m.fact
	case provider.SlotEntity:
		return m.entity
	case provider.SlotQueryAugment, provider.SlotIngestionDecision:
		return m.fact // fallback to fact, per the slot definitions
	default:
		return nil
	}
}

type mockQueueStatter struct {
	stats *storage.QueueStats
	err   error
}

func (m *mockQueueStatter) CountByStatus(_ context.Context) (*storage.QueueStats, error) {
	return m.stats, m.err
}

type mockEmbeddingProvider struct {
	name       string
	dimensions []int
	pingErr    error
}

func (m *mockEmbeddingProvider) Embed(_ context.Context, _ *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, nil
}
func (m *mockEmbeddingProvider) Name() string                   { return m.name }
func (m *mockEmbeddingProvider) Dimensions() []int              { return m.dimensions }
func (m *mockEmbeddingProvider) Ping(ctx context.Context) error { return m.pingErr }

type mockLLMProvider struct {
	name   string
	models []string
}

func (m *mockLLMProvider) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	return nil, nil
}
func (m *mockLLMProvider) Name() string     { return m.name }
func (m *mockLLMProvider) Models() []string { return m.models }

// mockLLMProviderWithPing wraps mockLLMProvider and adds ProviderHealth. It
// counts Ping calls so tests can assert a shared provider is pinged once.
type mockLLMProviderWithPing struct {
	mockLLMProvider
	pingErr   error
	pingCalls atomic.Int32
}

func (m *mockLLMProviderWithPing) Ping(_ context.Context) error {
	m.pingCalls.Add(1)
	return m.pingErr
}

// --- tests ---

func TestHealthSQLiteBackend(t *testing.T) {
	h := NewHealthHandler(HealthConfig{
		DB:        &mockDatabasePinger{backend: "sqlite"},
		Providers: nil,
		Queue:     nil,
		Version:   "0.1.0",
		StartTime: time.Now().Add(-10 * time.Second),
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %q", resp.Status)
	}
	if resp.Backend != "sqlite" {
		t.Errorf("expected backend sqlite, got %q", resp.Backend)
	}
	if resp.Providers["embedding"].Status != "not_configured" {
		t.Errorf("expected embedding not_configured, got %q", resp.Providers["embedding"].Status)
	}
	if resp.Providers["fact"].Status != "not_configured" {
		t.Errorf("expected fact not_configured, got %q", resp.Providers["fact"].Status)
	}
	if resp.Providers["entity"].Status != "not_configured" {
		t.Errorf("expected entity not_configured, got %q", resp.Providers["entity"].Status)
	}
}

func TestHealthPostgresConfiguredProviders(t *testing.T) {
	h := NewHealthHandler(HealthConfig{
		DB: &mockDatabasePinger{backend: "postgres"},
		Providers: &mockProviderRegistry{
			embedding: &mockEmbeddingProvider{name: "ollama", dimensions: []int{768}},
			fact:      &mockLLMProviderWithPing{mockLLMProvider: mockLLMProvider{name: "openai", models: []string{"gpt-4.1-nano"}}},
			entity:    nil,
		},
		Queue: &mockQueueStatter{
			stats: &storage.QueueStats{Pending: 3, Processing: 1, Failed: 0},
		},
		Version:   "0.1.0",
		StartTime: time.Now().Add(-100 * time.Second),
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("expected ok, got %q", resp.Status)
	}
	if resp.Backend != "postgres" {
		t.Errorf("expected postgres, got %q", resp.Backend)
	}

	// Embedding provider should be ok with ping latency.
	if resp.Providers["embedding"].Status != "ok" {
		t.Errorf("expected embedding ok, got %q", resp.Providers["embedding"].Status)
	}
	if resp.Providers["embedding"].Provider != "ollama" {
		t.Errorf("expected embedding provider ollama, got %q", resp.Providers["embedding"].Provider)
	}
	if resp.Providers["embedding"].LatencyMs == nil {
		t.Error("expected embedding latency_ms to be set")
	}

	// Fact provider should be ok.
	if resp.Providers["fact"].Status != "ok" {
		t.Errorf("expected fact ok, got %q", resp.Providers["fact"].Status)
	}
	if resp.Providers["fact"].Provider != "openai" {
		t.Errorf("expected fact provider openai, got %q", resp.Providers["fact"].Provider)
	}
	if resp.Providers["fact"].Model != "gpt-4.1-nano" {
		t.Errorf("expected fact model gpt-4.1-nano, got %q", resp.Providers["fact"].Model)
	}

	// Entity not configured.
	if resp.Providers["entity"].Status != "not_configured" {
		t.Errorf("expected entity not_configured, got %q", resp.Providers["entity"].Status)
	}

	// Optional slots fall back to the configured fact provider, so they report
	// operational with the fact provider/model that serves them.
	if resp.Providers["query_augment"].Status != "ok" {
		t.Errorf("expected query_augment ok via fallback, got %q", resp.Providers["query_augment"].Status)
	}
	if resp.Providers["query_augment"].Provider != "openai" {
		t.Errorf("expected query_augment provider openai via fallback, got %q", resp.Providers["query_augment"].Provider)
	}
	if resp.Providers["ingestion_decision"].Status != "ok" {
		t.Errorf("expected ingestion_decision ok via fallback, got %q", resp.Providers["ingestion_decision"].Status)
	}

	// Enrichment queue.
	if resp.EnrichmentQueue == nil {
		t.Fatal("expected enrichment_queue to be present on Postgres")
	}
	if resp.EnrichmentQueue.Pending != 3 {
		t.Errorf("expected pending 3, got %d", resp.EnrichmentQueue.Pending)
	}
	if resp.EnrichmentQueue.Processing != 1 {
		t.Errorf("expected processing 1, got %d", resp.EnrichmentQueue.Processing)
	}
}

func TestHealthPostgresNoProviders(t *testing.T) {
	h := NewHealthHandler(HealthConfig{
		DB:        &mockDatabasePinger{backend: "postgres"},
		Providers: &mockProviderRegistry{},
		Queue:     &mockQueueStatter{stats: &storage.QueueStats{}},
		Version:   "0.1.0",
		StartTime: time.Now(),
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Providers["embedding"].Status != "not_configured" {
		t.Errorf("expected embedding not_configured, got %q", resp.Providers["embedding"].Status)
	}
	if resp.Providers["fact"].Status != "not_configured" {
		t.Errorf("expected fact not_configured, got %q", resp.Providers["fact"].Status)
	}
	if resp.Providers["entity"].Status != "not_configured" {
		t.Errorf("expected entity not_configured, got %q", resp.Providers["entity"].Status)
	}
}

func TestHealthDatabaseFailure(t *testing.T) {
	h := NewHealthHandler(HealthConfig{
		DB:        &mockDatabasePinger{backend: "postgres", pingErr: errors.New("connection refused")},
		Providers: &mockProviderRegistry{},
		Queue:     &mockQueueStatter{stats: &storage.QueueStats{}},
		Version:   "0.1.0",
		StartTime: time.Now(),
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Status != "degraded" {
		t.Errorf("expected degraded, got %q", resp.Status)
	}
	if resp.Database.Status != "error" {
		t.Errorf("expected database error, got %q", resp.Database.Status)
	}
}

func TestHealthUptimeAndVersion(t *testing.T) {
	startTime := time.Now().Add(-84392 * time.Second)
	h := NewHealthHandler(HealthConfig{
		DB:        &mockDatabasePinger{backend: "sqlite"},
		Version:   "1.2.3",
		StartTime: startTime,
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Version != "1.2.3" {
		t.Errorf("expected version 1.2.3, got %q", resp.Version)
	}

	// Uptime should be approximately 84392 seconds (allow small delta for test execution).
	if resp.UptimeSeconds < 84390 || resp.UptimeSeconds > 84400 {
		t.Errorf("expected uptime ~84392, got %d", resp.UptimeSeconds)
	}
}

func TestHealthEnrichmentQueuePostgres(t *testing.T) {
	h := NewHealthHandler(HealthConfig{
		DB:        &mockDatabasePinger{backend: "postgres"},
		Providers: &mockProviderRegistry{},
		Queue: &mockQueueStatter{
			stats: &storage.QueueStats{Pending: 5, Processing: 2, Failed: 1},
		},
		Version:   "0.1.0",
		StartTime: time.Now(),
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.EnrichmentQueue == nil {
		t.Fatal("expected enrichment_queue on Postgres")
	}
	if resp.EnrichmentQueue.Pending != 5 {
		t.Errorf("expected pending 5, got %d", resp.EnrichmentQueue.Pending)
	}
	if resp.EnrichmentQueue.Processing != 2 {
		t.Errorf("expected processing 2, got %d", resp.EnrichmentQueue.Processing)
	}
	if resp.EnrichmentQueue.Failed != 1 {
		t.Errorf("expected failed 1, got %d", resp.EnrichmentQueue.Failed)
	}
}

// TestHealthReportsEverySlot guards against the health response drifting from
// the canonical slot set. Every slot in provider.Slots must appear in the
// providers map exactly once, with no extra keys. A slot added to slots.go
// without the health handler following will fail here.
func TestHealthReportsEverySlot(t *testing.T) {
	h := NewHealthHandler(HealthConfig{
		DB:        &mockDatabasePinger{backend: "postgres"},
		Providers: &mockProviderRegistry{},
		Version:   "0.1.0",
		StartTime: time.Now(),
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	want := make(map[string]bool, len(provider.Slots))
	for _, slot := range provider.Slots {
		want[slot.Name] = true
		if _, ok := resp.Providers[slot.Name]; !ok {
			t.Errorf("slot %q missing from /v1/health providers", slot.Name)
		}
	}
	for key := range resp.Providers {
		if !want[key] {
			t.Errorf("/v1/health providers has unknown slot key %q (not in provider.Slots)", key)
		}
	}
	if len(resp.Providers) != len(provider.Slots) {
		t.Errorf("expected %d provider slots, got %d", len(provider.Slots), len(resp.Providers))
	}
}

// TestHealthPingsSharedProviderOnce verifies that a provider serving several
// slots (fact, plus query_augment and ingestion_decision via fallback) is
// pinged once per health check, not once per slot.
func TestHealthPingsSharedProviderOnce(t *testing.T) {
	fact := &mockLLMProviderWithPing{mockLLMProvider: mockLLMProvider{name: "openai", models: []string{"gpt-4.1-nano"}}}
	h := NewHealthHandler(HealthConfig{
		DB:        &mockDatabasePinger{backend: "postgres"},
		Providers: &mockProviderRegistry{fact: fact},
		Version:   "0.1.0",
		StartTime: time.Now(),
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// fact serves three slots (fact, query_augment, ingestion_decision via the
	// mock's fallback), but the shared instance must be pinged exactly once.
	if got := fact.pingCalls.Load(); got != 1 {
		t.Errorf("expected shared fact provider pinged once, got %d pings", got)
	}
}
