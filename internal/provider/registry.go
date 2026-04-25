package provider

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Provider type constants identify the backend provider implementation.
const (
	ProviderTypeOpenAI     = "openai"
	ProviderTypeGemini     = "gemini"
	ProviderTypeAnthropic  = "anthropic"
	ProviderTypeOllama     = "ollama"
	ProviderTypeOpenRouter = "openrouter"
	ProviderTypeCustom     = "custom"
)

// SlotConfig represents the configuration for a single provider slot as stored
// in settings.
type SlotConfig struct {
	Type    string `json:"type"`     // "openai", "gemini", "anthropic"
	BaseURL string `json:"base_url"`
	APIKey  string `json:"api_key"`
	Model   string `json:"model"`
	Timeout int    `json:"timeout"` // seconds, 0 = default
}

// RegistryConfig holds the configuration for all provider slots and the shared
// circuit breaker parameters.
type RegistryConfig struct {
	Embedding      SlotConfig           `json:"embedding"`
	Fact           SlotConfig           `json:"fact"`
	Entity         SlotConfig           `json:"entity"`
	CircuitBreaker CircuitBreakerConfig `json:"circuit_breaker"`
}

// Registry manages the lifecycle of provider slots (embedding, fact extraction,
// entity extraction). It instantiates the appropriate provider for each slot,
// wraps them in circuit breakers, and provides thread-safe accessors.
type Registry struct {
	mu        sync.RWMutex
	embedding EmbeddingProvider
	fact      LLMProvider
	entity    LLMProvider
	config    RegistryConfig

	// Cached result of probing the embedding provider for its native output
	// dimension. The probe sends a tiny "probe" string through Embed and
	// reads len(resp.Embeddings[0]). Cached on first successful probe;
	// invalidated on Reload. Probe errors are NOT cached so a transient
	// failure does not pin the dim to 0 forever.
	embDim int
}

// NewRegistry instantiates providers from config, wraps each in a circuit
// breaker, and returns the populated Registry. It returns an error if a
// configured slot has an invalid type or an unsupported type/slot combination
// (e.g., anthropic for embedding).
func NewRegistry(config RegistryConfig) (*Registry, error) {
	r := &Registry{}
	if err := r.load(config); err != nil {
		return nil, err
	}
	return r, nil
}

// GetEmbedding returns the embedding provider, or nil if unconfigured.
func (r *Registry) GetEmbedding() EmbeddingProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.embedding
}

// GetFact returns the fact extraction LLM provider, or nil if unconfigured.
func (r *Registry) GetFact() LLMProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fact
}

// GetEntity returns the entity extraction LLM provider, or nil if unconfigured.
func (r *Registry) GetEntity() LLMProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.entity
}

// Reload recreates all providers from a new configuration, swapping them
// atomically under the write lock. Invalidates the cached embedding
// dimension; the next EmbeddingDim call re-probes against the new
// embedder.
func (r *Registry) Reload(config RegistryConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Build everything into temporaries first so that a partial failure does
	// not leave the registry in a half-updated state.
	tmp := &Registry{}
	if err := tmp.load(config); err != nil {
		return err
	}

	r.embedding = tmp.embedding
	r.fact = tmp.fact
	r.entity = tmp.entity
	r.config = tmp.config
	r.embDim = 0
	return nil
}

// EmbeddingDim returns the embedding provider's native output dimension,
// discovered via a one-shot probe (Embed("probe")) and cached. Reload
// invalidates the cache. Returns 0 if the embedder is not configured;
// returns an error if the probe fails (caller may retry on a later call).
//
// Cross-provider design: the probe sends a request and measures the
// response length. It does not ask the provider what dimensions it
// supports. This works identically for OpenAI, Gemini, Ollama (via the
// OpenAI adapter), OpenRouter, and any custom OpenAI-compatible endpoint.
//
// Concurrency: the probe Embed call happens OUTSIDE the registry lock so
// a slow upstream does not block other registry operations. A racing
// concurrent caller may also probe; the first writer wins and the second
// discards its result.
func (r *Registry) EmbeddingDim(ctx context.Context) (int, error) {
	r.mu.RLock()
	if r.embDim > 0 {
		d := r.embDim
		r.mu.RUnlock()
		return d, nil
	}
	embedder := r.embedding
	r.mu.RUnlock()

	if embedder == nil {
		return 0, fmt.Errorf("registry: embedding provider not configured")
	}

	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := embedder.Embed(probeCtx, &EmbeddingRequest{Input: []string{"probe"}})
	if err != nil {
		return 0, fmt.Errorf("registry: embedding probe failed: %w", err)
	}
	if len(resp.Embeddings) == 0 || len(resp.Embeddings[0]) == 0 {
		return 0, fmt.Errorf("registry: embedding probe returned no vector")
	}
	probedDim := len(resp.Embeddings[0])

	r.mu.Lock()
	defer r.mu.Unlock()
	// Discard if a Reload swapped the embedder under us — the dim we
	// measured belongs to the old provider.
	if r.embedding != embedder {
		return 0, fmt.Errorf("registry: provider changed during probe; retry")
	}
	// First writer wins. If another caller already populated, use their value.
	if r.embDim > 0 {
		return r.embDim, nil
	}
	r.embDim = probedDim
	return probedDim, nil
}

// GetConfig returns the current registry configuration (read-locked).
func (r *Registry) GetConfig() RegistryConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

// IsConfigured returns true if at least the embedding provider is configured.
func (r *Registry) IsConfigured() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.embedding != nil
}

// load does the actual construction work without locking. It is called from
// both NewRegistry and Reload.
func (r *Registry) load(config RegistryConfig) error {
	cbConfig := config.CircuitBreaker
	if cbConfig.MaxFailures == 0 {
		cbConfig = DefaultCircuitBreakerConfig()
	}

	// --- Embedding slot ---
	if config.Embedding.Type != "" {
		ep, err := createEmbeddingProvider(config.Embedding)
		if err != nil {
			return fmt.Errorf("embedding slot: %w", err)
		}
		r.embedding = NewCircuitBreakerEmbedding(ep, cbConfig)
	} else {
		r.embedding = nil
	}

	// --- Fact extraction slot ---
	if config.Fact.Type != "" {
		lp, err := createLLMProvider(config.Fact)
		if err != nil {
			return fmt.Errorf("fact slot: %w", err)
		}
		r.fact = NewCircuitBreakerLLM(lp, cbConfig)
	} else {
		r.fact = nil
	}

	// --- Entity extraction slot ---
	if config.Entity.Type != "" {
		lp, err := createLLMProvider(config.Entity)
		if err != nil {
			return fmt.Errorf("entity slot: %w", err)
		}
		r.entity = NewCircuitBreakerLLM(lp, cbConfig)
	} else {
		r.entity = nil
	}

	r.config = config
	return nil
}

// slotTimeout converts a SlotConfig timeout (seconds) to a time.Duration,
// returning 0 (provider default) when the value is unset.
func slotTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// createLLMProvider is a factory that creates the right LLMProvider based on
// the slot configuration's Type field.
func createLLMProvider(config SlotConfig) (LLMProvider, error) {
	switch config.Type {
	case ProviderTypeOpenAI, ProviderTypeOllama, ProviderTypeOpenRouter, ProviderTypeCustom:
		return NewOpenAIProvider(OpenAIConfig{
			BaseURL:      config.BaseURL,
			APIKey:       config.APIKey,
			DefaultModel: config.Model,
			Timeout:      slotTimeout(config.Timeout),
		}), nil

	case ProviderTypeGemini:
		return NewGeminiProvider(GeminiConfig{
			APIKey:       config.APIKey,
			DefaultModel: config.Model,
			BaseURL:      config.BaseURL,
			Timeout:      slotTimeout(config.Timeout),
		}), nil

	case ProviderTypeAnthropic:
		return NewAnthropicProvider(AnthropicConfig{
			APIKey:       config.APIKey,
			DefaultModel: config.Model,
			BaseURL:      config.BaseURL,
			Timeout:      slotTimeout(config.Timeout),
		}), nil

	default:
		return nil, fmt.Errorf("unsupported provider type: %q", config.Type)
	}
}

// createEmbeddingProvider is a factory that creates the right EmbeddingProvider
// based on the slot configuration's Type field. Anthropic does not support
// embeddings, so requesting it returns an error.
func createEmbeddingProvider(config SlotConfig) (EmbeddingProvider, error) {
	switch config.Type {
	case ProviderTypeOpenAI, ProviderTypeOllama, ProviderTypeOpenRouter, ProviderTypeCustom:
		return NewOpenAIProvider(OpenAIConfig{
			BaseURL:               config.BaseURL,
			APIKey:                config.APIKey,
			DefaultEmbeddingModel: config.Model,
			Timeout:               slotTimeout(config.Timeout),
		}), nil

	case ProviderTypeGemini:
		return NewGeminiProvider(GeminiConfig{
			APIKey:                config.APIKey,
			DefaultEmbeddingModel: config.Model,
			BaseURL:               config.BaseURL,
			Timeout:               slotTimeout(config.Timeout),
		}), nil

	case ProviderTypeAnthropic:
		return nil, fmt.Errorf("anthropic does not support embeddings")

	default:
		return nil, fmt.Errorf("unsupported provider type: %q", config.Type)
	}
}
