package provider

import (
	"sync"
	"testing"
)

func TestRegistryAllSlots(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1", Model: "text-embedding-3-small"},
		Fact:      SlotConfig{Type: ProviderTypeGemini, APIKey: "k2", Model: "gemini-2.0-flash"},
		Entity:    SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k3", Model: "claude-sonnet-4-20250514"},
	}

	r, err := NewRegistry(cfg)
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
	r, err := NewRegistry(RegistryConfig{})
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

	r, err := NewRegistry(cfg)
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

func TestRegistryAnthropicEmbeddingError(t *testing.T) {
	cfg := RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeAnthropic, APIKey: "k1"},
	}

	_, err := NewRegistry(cfg)
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
			_, err := NewRegistry(tt.cfg)
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

	r, err := NewRegistry(cfg1)
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
	r, err := NewRegistry(cfg)
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
	r, err := NewRegistry(RegistryConfig{})
	if err != nil {
		t.Fatalf("NewRegistry() error: %v", err)
	}
	if r.IsConfigured() {
		t.Error("expected IsConfigured() = false with no providers")
	}

	// With embedding = configured.
	r2, err := NewRegistry(RegistryConfig{
		Embedding: SlotConfig{Type: ProviderTypeOpenAI, APIKey: "k1"},
	})
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
	})
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

	r, err := NewRegistry(cfg)
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
