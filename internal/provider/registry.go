package provider

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// EmbeddingProviderWrapper is an optional middleware applied to every
// embedding provider built by the Registry, sitting INSIDE the
// usage-recording wrapper so a metrics decorator (see
// internal/observability/metrics.WrapEmbeddingProvider) measures only the
// actual provider call latency, not the synchronous token_usage DB write.
type EmbeddingProviderWrapper func(EmbeddingProvider) EmbeddingProvider

// probeInput is the dummy text fed to the embedder when measuring its
// native output dim. probeSlotEmbedding is the singleflight key that
// coalesces concurrent embedding-slot probes.
const (
	probeInput         = "probe"
	probeSlotEmbedding = "embedding"
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
// wraps them in circuit breakers and the usage-recording middleware, and
// provides thread-safe accessors.
type Registry struct {
	mu        sync.RWMutex
	embedding EmbeddingProvider
	fact      LLMProvider
	entity    LLMProvider
	config    RegistryConfig

	// Wrapping infrastructure. Both may be nil — when nil, providers are
	// returned without the usage-recording middleware (e.g., in tests that
	// don't care about token_usage rows). Captured at construction time and
	// reused across Reload.
	recorder UsageRecorder
	resolver UsageContextResolver
	// tokenCounter is forwarded into every usage-recording wrapper so
	// Prometheus sees a counter increment alongside each token_usage row.
	// nil pointer disables the metric emission while keeping DB recording
	// intact. Atomic so WithTokenCounter is safe to call from any
	// goroutine, and so the indirect closures inside wrapLLM/wrapEmbedding
	// always read the latest value installed on this Registry — including
	// when Reload re-wraps providers under the existing receiver.
	tokenCounter atomic.Pointer[TokenCounter]
	// embedWrapper applies a caller-provided middleware (typically the
	// Prometheus latency/count instrumentation) to every embedding
	// provider built by the Registry, sitting INSIDE the usage-recording
	// wrapper so the metric measures only the upstream provider call.
	// Atomic for the same reasons as tokenCounter.
	embedWrapper atomic.Pointer[EmbeddingProviderWrapper]

	// Cached result of probing the embedding provider for its native output
	// dimension. The probe sends a tiny "probe" string through Embed and
	// reads len(resp.Embeddings[0]). Cached on first successful probe;
	// invalidated on Reload. Probe errors are NOT cached so a transient
	// failure does not pin the dim to 0 forever.
	embDim int

	// Coalesces concurrent probes so the eager prewarm and a racing lazy
	// EmbeddingDim caller share one network round-trip instead of doubling
	// up. Held by pointer so Reload can swap a fresh group atomically
	// without disturbing in-flight goroutines that still reference the
	// old one.
	probeGroup *singleflight.Group
}

// NewRegistry instantiates providers from config, wraps each in a circuit
// breaker and the usage-recording middleware, and returns the populated
// Registry. recorder and resolver may both be nil to skip usage recording
// (e.g., for unit tests). It returns an error if a configured slot has an
// invalid type or an unsupported type/slot combination (e.g., anthropic
// for embedding).
func NewRegistry(config RegistryConfig, recorder UsageRecorder, resolver UsageContextResolver) (*Registry, error) {
	r := &Registry{recorder: recorder, resolver: resolver, probeGroup: &singleflight.Group{}}
	if err := r.load(config); err != nil {
		return nil, err
	}
	// Pre-warm the tokenizer fallback encodings so the first provider
	// call that hits the zero-token path does not block on a remote BPE
	// download.
	PrewarmTokenizers()
	// Eagerly probe the embedder dim so the first downstream caller does
	// not pay the round-trip latency. Failures are non-fatal — the cache
	// stays empty and EmbeddingDim retries on demand. Operation stamping
	// is done inside probeAndCache so every probe call site is covered.
	if r.embedding != nil {
		_, _ = r.probeAndCache(context.Background(), r.embedding, r.probeGroup)
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
// dimension and immediately re-probes the new embedder so the first
// downstream EmbeddingDim caller does not pay the round-trip latency.
// Probe failures are non-fatal — the cache stays empty and a later
// EmbeddingDim call will retry.
//
// Wrapping uses the live receiver r so the indirect tokenCounter /
// embedWrapper closures inside the new wrappers reach the values
// installed on this Registry instance (via WithTokenCounter /
// WithEmbeddingWrapper). Building wrappers through a temporary Registry
// would orphan the closures against fields that were never set.
func (r *Registry) Reload(config RegistryConfig) error {
	r.mu.Lock()
	emb, fact, entity, err := r.buildProviders(config)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	r.embedding = emb
	r.fact = fact
	r.entity = entity
	r.config = config
	r.embDim = 0
	r.probeGroup = &singleflight.Group{}
	embedder := r.embedding
	group := r.probeGroup
	r.mu.Unlock()

	if embedder != nil {
		_, _ = r.probeAndCache(context.Background(), embedder, group)
	}
	return nil
}

// EmbeddingDim returns the embedding provider's native output dimension.
// Discovered by sending Embed("probe") and reading len(resp.Embeddings[0])
// — works identically across every provider because it measures the
// response rather than asking the provider what it supports. Cached;
// Reload invalidates and re-probes eagerly. Probe errors are not cached.
func (r *Registry) EmbeddingDim(ctx context.Context) (int, error) {
	r.mu.RLock()
	if r.embDim > 0 {
		d := r.embDim
		r.mu.RUnlock()
		return d, nil
	}
	embedder := r.embedding
	group := r.probeGroup
	r.mu.RUnlock()

	if embedder == nil {
		return 0, fmt.Errorf("registry: embedding provider not configured")
	}
	return r.probeAndCache(ctx, embedder, group)
}

// probeAndCache coalesces concurrent probes through singleflight so the
// eager prewarm and a racing lazy hit share one network round-trip. The
// probe runs on a bg-derived context with its own 10s budget so a caller
// bailing early does not cancel the work — the result still populates
// the cache for the next caller. If Reload swapped the embedder
// mid-probe, the measured dim is discarded by the identity check.
func (r *Registry) probeAndCache(ctx context.Context, embedder EmbeddingProvider, group *singleflight.Group) (int, error) {
	ch := group.DoChan(probeSlotEmbedding, func() (any, error) {
		probeCtx, cancel := context.WithTimeout(
			WithOperation(context.Background(), OperationProbe),
			10*time.Second,
		)
		defer cancel()
		resp, err := embedder.Embed(probeCtx, &EmbeddingRequest{Input: []string{probeInput}})
		if err != nil {
			return 0, fmt.Errorf("registry: embedding probe failed: %w", err)
		}
		if len(resp.Embeddings) == 0 || len(resp.Embeddings[0]) == 0 {
			return 0, fmt.Errorf("registry: embedding probe returned no vector")
		}
		probedDim := len(resp.Embeddings[0])

		r.mu.Lock()
		defer r.mu.Unlock()
		if r.embedding != embedder {
			return 0, fmt.Errorf("registry: provider changed during probe; retry")
		}
		if r.embDim > 0 {
			return r.embDim, nil
		}
		r.embDim = probedDim
		return probedDim, nil
	})

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return 0, res.Err
		}
		return res.Val.(int), nil
	}
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

// EnrichmentAvailable returns true iff embedding, fact, and entity providers
// are all configured. The gate behind every enrichment + dreaming surface.
func (r *Registry) EnrichmentAvailable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.embedding != nil && r.fact != nil && r.entity != nil
}

// load is called from NewRegistry only. It writes the constructed
// providers directly onto r and is non-transactional. Reload uses
// buildProviders so a mid-build failure does not leave r partially
// mutated.
func (r *Registry) load(config RegistryConfig) error {
	emb, fact, entity, err := r.buildProviders(config)
	if err != nil {
		return err
	}
	r.embedding = emb
	r.fact = fact
	r.entity = entity
	r.config = config
	return nil
}

// buildProviders constructs each configured slot, wraps it via r's wrap
// methods (so closures capture the live receiver), and returns the staged
// values without writing to r. Callers install them atomically under
// r.mu. A nil return for any slot means that slot was unconfigured.
func (r *Registry) buildProviders(config RegistryConfig) (EmbeddingProvider, LLMProvider, LLMProvider, error) {
	cbConfig := config.CircuitBreaker
	if cbConfig.MaxFailures == 0 {
		cbConfig = DefaultCircuitBreakerConfig()
	}

	// breakerCfgFor labels the breaker with "<provider>-<slot>" so
	// CircuitOpenError messages identify both the upstream service and which
	// pipeline stage tripped.
	breakerCfgFor := func(slotType, slotLabel string) CircuitBreakerConfig {
		c := cbConfig
		c.Name = slotType + "-" + slotLabel
		return c
	}

	var emb EmbeddingProvider
	if config.Embedding.Type != "" {
		ep, err := createEmbeddingProvider(config.Embedding)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("embedding slot: %w", err)
		}
		emb = r.wrapEmbedding(NewCircuitBreakerEmbedding(ep, breakerCfgFor(config.Embedding.Type, "embed")))
	}

	var fact LLMProvider
	if config.Fact.Type != "" {
		lp, err := createLLMProvider(config.Fact)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("fact slot: %w", err)
		}
		fact = r.wrapLLM(NewCircuitBreakerLLM(lp, breakerCfgFor(config.Fact.Type, "fact")))
	}

	var entity LLMProvider
	if config.Entity.Type != "" {
		lp, err := createLLMProvider(config.Entity)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("entity slot: %w", err)
		}
		entity = r.wrapLLM(NewCircuitBreakerLLM(lp, breakerCfgFor(config.Entity.Type, "entity")))
	}

	return emb, fact, entity, nil
}

// wrapLLM wraps a circuit-breaker-protected LLM provider in the
// usage-recording middleware so every Complete call lands a token_usage
// row. When no recorder is configured (e.g., in unit tests) the inner
// provider is returned as-is.
// indirectTokenCounter returns a TokenCounter closure that derefs r's
// atomic counter on every call, so wrappers built before WithTokenCounter
// (or before Reload) still pick up the value installed on this live
// receiver. Returns nil-firing semantics when no counter is installed.
func (r *Registry) indirectTokenCounter() TokenCounter {
	return func(p, op string, n float64) {
		if ptr := r.tokenCounter.Load(); ptr != nil {
			(*ptr)(p, op, n)
		}
	}
}

func (r *Registry) wrapLLM(inner LLMProvider) LLMProvider {
	if r.recorder == nil {
		return inner
	}
	return NewUsageRecordingLLM(inner, r.recorder, r.resolver).WithTokenCounter(r.indirectTokenCounter())
}

// wrapEmbedding wraps a circuit-breaker-protected embedding provider in
// optional caller-provided middleware (typically the Prometheus metrics
// decorator) and then in the usage-recording middleware so every Embed
// call lands a token_usage row. The order is important: the metrics
// wrapper sits INSIDE the usage recorder so nram_embedding_duration_seconds
// measures upstream provider latency only, not the synchronous DB write.
// When no recorder is configured the inner provider is returned as-is
// (still passed through the optional wrapper).
func (r *Registry) wrapEmbedding(inner EmbeddingProvider) EmbeddingProvider {
	if wp := r.embedWrapper.Load(); wp != nil && *wp != nil {
		inner = (*wp)(inner)
	}
	if r.recorder == nil {
		return inner
	}
	return NewUsageRecordingEmbedding(inner, r.recorder, r.resolver).WithTokenCounter(r.indirectTokenCounter())
}

// WithTokenCounter installs the callback fired for every recorded
// completion or embedding call. Safe to call concurrently with provider
// calls and Reload; the indirect closures inside wrap{LLM,Embedding}
// pick up the new value atomically.
func (r *Registry) WithTokenCounter(c TokenCounter) *Registry {
	if c == nil {
		r.tokenCounter.Store(nil)
	} else {
		r.tokenCounter.Store(&c)
	}
	return r
}

// WithEmbeddingWrapper installs a middleware applied to every embedding
// provider built by the Registry. Used by the server to inject the
// Prometheus latency/count instrumentation inside the usage recorder.
// Safe to call concurrently with provider calls; Reload picks up the
// current value when re-wrapping.
func (r *Registry) WithEmbeddingWrapper(w EmbeddingProviderWrapper) *Registry {
	if w == nil {
		r.embedWrapper.Store(nil)
	} else {
		r.embedWrapper.Store(&w)
	}
	return r
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
			ProviderType: config.Type,
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
