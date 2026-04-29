package provider

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRegistryAllSlots(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1", Model: "text-embedding-3-small"},
		Fact:      SlotConfig{Type: ProviderTypeGemini, APIKey: "k2", Model: "gemini-2.0-flash"},
		Entity:    SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k3", Model: "claude-sonnet-4-20250514"},
	}

	r, err := NewRegistry(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}

	if r.GetEmbedding() == nil {
		t.Error("expected embedding provider to be non-nil")
	}
	if r.GetFact() == nil {
		t.Error("expected fact provider to be non-nil")
	}
	if r.GetEntity() == nil {
		t.Error("expected entity provider to be non-nil")
	}

	// Verify circuit breaker wrapping via type assertion.
	if _, ok := r.GetEmbedding().(*CircuitBreakerEmbedding); !ok {
		t.Error("embedding provider should be wrapped in CircuitBreakerEmbedding")
	}
	if _, ok := r.GetFact().(*CircuitBreakerLLM); !ok {
		t.Error("fact provider should be wrapped in CircuitBreakerLLM")
	}
	if _, ok := r.GetEntity().(*CircuitBreakerLLM); !ok {
		t.Error("entity provider should be wrapped in CircuitBreakerLLM")
	}
}

func TestRegistryEmptySlots(t *testing.T) {
	r, err := NewRegistry(RegistryConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}

	if r.GetEmbedding() != nil {
		t.Error("expected embedding provider to be nil")
	}
	if r.GetFact() != nil {
		t.Error("expected fact provider to be nil")
	}
	if r.GetEntity() != nil {
		t.Error("expected entity provider to be nil")
	}
}

func TestRegistryOnlyEmbedding(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1", Model: "text-embedding-3-small"},
	}

	r, err := NewRegistry(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}

	if r.GetEmbedding() == nil {
		t.Error("expected embedding provider to be non-nil")
	}
	if r.GetFact() != nil {
		t.Error("expected fact provider to be nil")
	}
	if r.GetEntity() != nil {
		t.Error("expected entity provider to be nil")
	}
}

func TestRegistryEnrichmentAvailable(t *testing.T) {
	embed := SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1", Model: "text-embedding-3-small"}
	fact := SlotConfig{Type: ProviderTypeGemini, APIKey: "k2", Model: "gemini-2.0-flash"}
	entity := SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k3", Model: "claude-sonnet-4-20250514"}

	cases := []struct {
		name string
		cfg  RegistryConfig
		want bool
	}{
		{"all-three", RegistryConfig{Embedding: embed, Fact: fact, Entity: entity}, true},
		{"missing-embedding", RegistryConfig{Fact: fact, Entity: entity}, false},
		{"missing-fact", RegistryConfig{Embedding: embed, Entity: entity}, false},
		{"missing-entity", RegistryConfig{Embedding: embed, Fact: fact}, false},
		{"only-embedding", RegistryConfig{Embedding: embed}, false},
		{"only-fact", RegistryConfig{Fact: fact}, false},
		{"only-entity", RegistryConfig{Entity: entity}, false},
		{"none", RegistryConfig{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewRegistry(tc.cfg, nil, nil)
			if err != nil {
				t.Fatalf("NewRegistry() error: %v", err)
			}
			if got := r.EnrichmentAvailable(); got != tc.want {
				t.Errorf("EnrichmentAvailable() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRegistryEnrichmentAvailableLiveReload(t *testing.T) {
	embed := SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1", Model: "text-embedding-3-small"}
	fact := SlotConfig{Type: ProviderTypeGemini, APIKey: "k2", Model: "gemini-2.0-flash"}
	entity := SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k3", Model: "claude-sonnet-4-20250514"}

	r, err := NewRegistry(RegistryConfig{Embedding: embed}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}
	if r.EnrichmentAvailable() {
		t.Fatal("EnrichmentAvailable should be false with only embedding configured")
	}

	if err := r.Reload(RegistryConfig{Embedding: embed, Fact: fact, Entity: entity}); err != nil {
		t.Fatalf("Reload() error: %v", err)
	}
	if !r.EnrichmentAvailable() {
		t.Fatal("EnrichmentAvailable should be true after Reload with all three slots")
	}

	if err := r.Reload(RegistryConfig{Embedding: embed, Fact: fact}); err != nil {
		t.Fatalf("Reload() removing entity error: %v", err)
	}
	if r.EnrichmentAvailable() {
		t.Fatal("EnrichmentAvailable should be false after entity slot removed")
	}
}

func TestRegistryAnthropicEmbeddingError(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k1"},
	}

	_, err := NewRegistry(cfg, nil, nil)
	if err == nil {
		t.Fatal("expected error for anthropic embedding slot, got nil")
	}
}

func TestRegistryInvalidProviderType(t *testing.T) {
	tests := []struct {
		name string
		cfg  RegistryConfig
	}{
		{
			name: "invalid embedding type",
			cfg:  RegistryConfig{Embedding: SlotConfig{Type: "invalid"}},
		},
		{
			name: "invalid fact type",
			cfg:  RegistryConfig{Fact: SlotConfig{Type: "bogus"}},
		},
		{
			name: "invalid entity type",
			cfg:  RegistryConfig{Entity: SlotConfig{Type: "unknown"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRegistry(tt.cfg, nil, nil)
			if err == nil {
				t.Fatal("expected error for invalid provider type, got nil")
			}
		})
	}
}

func TestRegistryReload(t *testing.T) {
	// Start with only embedding.
	cfg1 := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1", Model: "m1"},
	}

	r, err := NewRegistry(cfg1, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}

	if r.GetFact() != nil {
		t.Error("fact should be nil before reload")
	}

	// Reload with fact and entity added.
	cfg2 := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeGemini, APIKey: "k2", Model: "m2"},
		Fact:      SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k3", Model: "m3"},
		Entity:    SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k4", Model: "m4"},
	}

	if err := r.Reload(cfg2); err != nil {
		t.Fatalf("Reload() error: %v", err)
	}

	if r.GetEmbedding() == nil {
		t.Error("expected embedding provider after reload")
	}
	if r.GetFact() == nil {
		t.Error("expected fact provider after reload")
	}
	if r.GetEntity() == nil {
		t.Error("expected entity provider after reload")
	}

	// Reload to empty should clear everything.
	if err := r.Reload(RegistryConfig{}); err != nil {
		t.Fatalf("Reload() error: %v", err)
	}

	if r.GetEmbedding() != nil {
		t.Error("embedding should be nil after empty reload")
	}
	if r.GetFact() != nil {
		t.Error("fact should be nil after empty reload")
	}
}

func TestRegistryReloadError(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1"},
	}
	r, err := NewRegistry(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}

	// Attempt reload with invalid config — original state should be preserved.
	badCfg := RegistryConfig{
		Embedding: SlotConfig{Type: "invalid"},
	}
	if err := r.Reload(badCfg); err == nil {
		t.Fatal("expected error from Reload with invalid config")
	}

	// Original embedding should still be present.
	if r.GetEmbedding() == nil {
		t.Error("embedding should be preserved after failed reload")
	}
}

func TestRegistryIsConfigured(t *testing.T) {
	// No embedding = not configured.
	r, err := NewRegistry(RegistryConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}
	if r.IsConfigured() {
		t.Error("expected IsConfigured() = false with no providers")
	}

	// With embedding = configured.
	r2, err := NewRegistry(RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1"},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}
	if !r2.IsConfigured() {
		t.Error("expected IsConfigured() = true with embedding provider")
	}

	// Only fact/entity without embedding = not configured.
	r3, err := NewRegistry(RegistryConfig{
		Fact:   SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1"},
		Entity: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1"},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}
	if r3.IsConfigured() {
		t.Error("expected IsConfigured() = false without embedding provider")
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1", Model: "m1"},
		Fact:      SlotConfig{Type: ProviderTypeGemini, APIKey: "k2", Model: "m2"},
		Entity:    SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k3", Model: "m3"},
	}

	r, err := NewRegistry(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}

	var wg sync.WaitGroup
	const goroutines = 50

	// Concurrent readers.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.GetEmbedding()
			_ = r.GetFact()
			_ = r.GetEntity()
			_ = r.IsConfigured()
		}()
	}

	// Concurrent reloads interleaved with reads.
	for i := 0; i < goroutines/5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Reload(cfg)
		}()
	}

	wg.Wait()
}

func TestCreateLLMProviderTypes(t *testing.T) {
	tests := []struct {
		providerType string
		expectType   string
	}{
		{ProviderTypeOpenAI, "*provider.OpenAIProvider"},
		{ProviderTypeGemini, "*provider.GeminiProvider"},
		{ProviderTypeAnthropic, "*provider.AnthropicProvider"},
	}

	for _, tt := range tests {
		t.Run(tt.providerType, func(t *testing.T) {
			p, err := createLLMProvider(SlotConfig{Type: tt.providerType, APIKey: "test"})
			if err != nil {
				t.Fatalf("createLLMProvider(%q) error: %v", tt.providerType, err)
			}
			if p == nil {
				t.Fatal("expected non-nil provider")
			}
		})
	}
}

func TestCreateEmbeddingProviderTypes(t *testing.T) {
	tests := []struct {
		providerType string
		expectErr    bool
	}{
		{ProviderTypeOpenAI, false},
		{ProviderTypeGemini, false},
		{ProviderTypeAnthropic, true},
	}

	for _, tt := range tests {
		t.Run(tt.providerType, func(t *testing.T) {
			p, err := createEmbeddingProvider(SlotConfig{Type: tt.providerType, APIKey: "test"})
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("createEmbeddingProvider(%q) error: %v", tt.providerType, err)
			}
			if p == nil {
				t.Fatal("expected non-nil provider")
			}
		})
	}
}

// probeEmbedder is a configurable EmbeddingProvider for the EmbeddingDim
// tests. It returns a vector of `dim` zeros (or `err` if set), and counts
// how many times Embed has been called so the cache behavior is testable.
type probeEmbedder struct {
	dim   int
	err   error
	calls atomic.Int32
}

func (p *probeEmbedder) Embed(_ context.Context, _ *EmbeddingRequest) (*EmbeddingResponse, error) {
	p.calls.Add(1)
	if p.err != nil {
		return nil, p.err
	}
	return &EmbeddingResponse{Embeddings: [][]float32{make([]float32, p.dim)}}, nil
}
func (p *probeEmbedder) Name() string      { return "probe" }
func (p *probeEmbedder) Dimensions() []int { return []int{p.dim} }

func TestRegistryEmbeddingDim_ProbesAndCaches(t *testing.T) {
	emb := &probeEmbedder{dim: 768}
	r := &Registry{embedding: emb}

	d1, err := r.EmbeddingDim(context.Background())
	if err != nil {
		t.Fatalf("first probe failed: %v", err)
	}
	if d1 != 768 {
		t.Errorf("first probe dim = %d, want 768", d1)
	}
	if got := emb.calls.Load(); got != 1 {
		t.Fatalf("first call should produce 1 probe, got %d", got)
	}

	d2, err := r.EmbeddingDim(context.Background())
	if err != nil {
		t.Fatalf("cached call failed: %v", err)
	}
	if d2 != 768 {
		t.Errorf("cached dim = %d, want 768", d2)
	}
	if got := emb.calls.Load(); got != 1 {
		t.Fatalf("second call must use cache, got %d probes total", got)
	}
}

func TestRegistryEmbeddingDim_NotConfigured(t *testing.T) {
	r := &Registry{}
	if _, err := r.EmbeddingDim(context.Background()); err == nil {
		t.Fatal("expected error when embedding provider not configured")
	}
}

func TestRegistryEmbeddingDim_ProbeErrorNotCached(t *testing.T) {
	emb := &probeEmbedder{err: errors.New("network blip")}
	r := &Registry{embedding: emb}

	if _, err := r.EmbeddingDim(context.Background()); err == nil {
		t.Fatal("expected error from failing probe")
	}
	_, _ = r.EmbeddingDim(context.Background())
	if got := emb.calls.Load(); got != 2 {
		t.Fatalf("probe error must not be cached; expected 2 probes, got %d", got)
	}

	emb.err = nil
	emb.dim = 1024
	d, err := r.EmbeddingDim(context.Background())
	if err != nil {
		t.Fatalf("recovered probe failed: %v", err)
	}
	if d != 1024 {
		t.Errorf("recovered dim = %d, want 1024", d)
	}
	if got := emb.calls.Load(); got != 3 {
		t.Errorf("expected 3 total probes after recovery, got %d", got)
	}
	_, _ = r.EmbeddingDim(context.Background())
	if got := emb.calls.Load(); got != 3 {
		t.Errorf("post-recovery cache must hold; got %d probes", got)
	}
}

func TestRegistryEmbeddingDim_ReloadInvalidatesCache(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k", Model: "m"},
	}
	r, err := NewRegistry(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry error: %v", err)
	}

	r.mu.Lock()
	emb1 := &probeEmbedder{dim: 768}
	r.embedding = emb1
	r.embDim = 0
	r.mu.Unlock()

	if d, err := r.EmbeddingDim(context.Background()); err != nil || d != 768 {
		t.Fatalf("first probe: dim=%d err=%v", d, err)
	}
	if got := emb1.calls.Load(); got != 1 {
		t.Fatalf("expected 1 probe on first call, got %d", got)
	}

	if err := r.Reload(cfg); err != nil {
		t.Fatalf("Reload error: %v", err)
	}
	r.mu.Lock()
	emb2 := &probeEmbedder{dim: 1024}
	r.embedding = emb2
	r.mu.Unlock()

	d, err := r.EmbeddingDim(context.Background())
	if err != nil {
		t.Fatalf("post-reload probe failed: %v", err)
	}
	if d != 1024 {
		t.Errorf("post-reload dim = %d, want 1024 (cache should have been invalidated)", d)
	}
	if got := emb2.calls.Load(); got != 1 {
		t.Errorf("expected exactly 1 probe of new embedder, got %d", got)
	}
}
