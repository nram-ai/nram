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
	ProviderTypeOpenAI           = "openai"
	ProviderTypeGemini           = "gemini"
	ProviderTypeAnthropic        = "anthropic"
	ProviderTypeOllama           = "ollama"
	ProviderTypeOpenRouter       = "openrouter"
	ProviderTypeOpenAICompatible = "openai-compatible"
	ProviderTypeVLLM             = "vllm"
	ProviderTypeSGLang           = "sglang"
	ProviderTypeLlamaServer      = "llama-server"

	// ProviderTypeCustomLegacy is the pre-0.5.4 name for the generic
	// OpenAI-compatible passthrough. It is accepted on read and normalized to
	// ProviderTypeOpenAICompatible (see NormalizeProviderType); current code
	// never writes it. The name was renamed because "custom" read like an
	// open-ended inference plugin rather than the plain OpenAI-compatible
	// endpoint it always was.
	ProviderTypeCustomLegacy = "custom"
)

// NormalizeProviderType maps deprecated type aliases to their canonical form.
// "custom" (pre-0.5.4) becomes "openai-compatible"; every other value passes
// through unchanged. Applied wherever a stored type is read so legacy configs
// keep working without a destructive rewrite, while the boot-time migration
// (see adminstore.MigrateProviderTypes) persists the rename.
func NormalizeProviderType(t string) string {
	if t == ProviderTypeCustomLegacy {
		return ProviderTypeOpenAICompatible
	}
	return t
}

// isOpenAICompatibleType reports whether a (normalized) provider type is served
// by the shared OpenAI-compatible adapter (NewOpenAIProvider). vLLM, SGLang, and
// llama.cpp's llama-server speak the same /v1 wire format as
// OpenAI/Ollama/OpenRouter and differ only in the per-type request extensions
// applied in openai.go.
func isOpenAICompatibleType(t string) bool {
	switch t {
	case ProviderTypeOpenAI, ProviderTypeOllama, ProviderTypeOpenRouter,
		ProviderTypeOpenAICompatible, ProviderTypeVLLM, ProviderTypeSGLang,
		ProviderTypeLlamaServer:
		return true
	default:
		return false
	}
}

// IsOpenAICompatibleType reports whether a raw (un-normalized) provider type
// string routes through the OpenAI-compatible adapter and therefore serves a
// GET /v1/models list. Exported for the admin store's served-model auto-detection.
func IsOpenAICompatibleType(t string) bool {
	return isOpenAICompatibleType(NormalizeProviderType(t))
}

// thinkingDisabled resolves the per-slot DisableThinking pointer, defaulting to
// true (thinking off) when unset. The reasoning pass is dead weight on nram's
// extraction/decision/synthesis calls, so a slot that has never set the toggle
// — including every config that predates the field — keeps thinking disabled.
func thinkingDisabled(p *bool) bool {
	return p == nil || *p
}

// SlotConfig represents the configuration for a single provider slot as stored
// in settings.
type SlotConfig struct {
	Type    string `json:"type"` // "openai", "gemini", "anthropic"
	BaseURL string `json:"base_url"`
	APIKey  string `json:"api_key"`
	Model   string `json:"model"`
	Timeout int    `json:"timeout"` // seconds, 0 = default
	// Dimension is the embedding slot's opt-in output dimension. 0 (the default)
	// means use the model's native size and omit the OpenAI "dimensions" request
	// field; a positive value is sent verbatim and requires a Matryoshka-capable
	// model. Applied uniformly to the probe and production embeds so the per-dim
	// vector table matches what the model returns. Inert for non-embedding slots.
	Dimension int `json:"dimension,omitempty"`
	// PromptCacheEnabled marks the system prefix as cacheable on providers that
	// accept an explicit hint (Anthropic). Sourced from the global
	// provider.prompt_cache.enabled setting.
	PromptCacheEnabled bool `json:"prompt_cache_enabled,omitempty"`
	// JSONModeToolUse coerces Anthropic JSONMode requests into a forced
	// tool_use call. Sourced from the global
	// provider.anthropic.json_tool_use.enabled setting; off by default.
	JSONModeToolUse bool `json:"json_tool_use,omitempty"`
	// CustomHeaders are arbitrary HTTP headers attached to every outbound
	// request to this slot's provider host. Intended for proxies/gateways
	// between nram and the provider. Content-Type (and, for Anthropic,
	// anthropic-version) are reserved and cannot be overridden; all other
	// headers, including auth, may be set or overridden here.
	CustomHeaders map[string]string `json:"custom_headers,omitempty"`
	// ExtraBody is merged verbatim onto the top level of every OpenAI-compatible
	// request body (chat completions and embeddings), mirroring the OpenAI SDK's
	// extra_body. User keys win, so a configured chat_template_kwargs overrides
	// the enable_thinking=false default the vllm/sglang types set. Ignored by the
	// Gemini and Anthropic adapters, whose bodies are not OpenAI-shaped.
	ExtraBody map[string]any `json:"extra_body,omitempty"`
	// DisableThinking controls whether nram sends the provider-appropriate
	// "thinking off" knob on completion requests (Ollama reasoning_effort:none,
	// OpenRouter reasoning.enabled:false, vLLM/SGLang/llama-server
	// chat_template_kwargs.enable_thinking:false, Gemini thinkingConfig.thinkingBudget:0).
	// A nil pointer means unset and resolves to disabled (see thinkingDisabled),
	// so existing slots keep skipping the reasoning pass. OpenAI, Anthropic, and
	// the generic openai-compatible type never receive a knob (an explicit disable
	// 400s on current models), so the toggle is inert for them.
	DisableThinking *bool `json:"disable_thinking,omitempty"`
	// RerankMethod selects the reranker-slot implementation: "cross_encoder"
	// (deterministic /v1/rerank) or "judge" (generative chat model). Set by the
	// admin save path from ProbeRerankMethod; empty defaults to cross_encoder at
	// build time. Inert for non-reranker slots.
	RerankMethod string `json:"rerank_method,omitempty"`
}

// RegistryConfig holds the configuration for all provider slots and the shared
// circuit breaker parameters.
type RegistryConfig struct {
	// Slots holds each provider slot's config keyed by slot name (see slots.go).
	// A missing or zero-value entry means "unconfigured": the read path returns
	// the zero SlotConfig (empty Type) and buildProviders skips that slot. Keyed
	// by name so adding a slot is one SlotDef entry in slots.go with no edits here.
	Slots          map[string]SlotConfig
	CircuitBreaker CircuitBreakerConfig
}

// slotConfig returns the SlotConfig for the named slot, or the zero SlotConfig
// (treated as unconfigured) when the slot has no entry. Everything else iterates
// the canonical Slots list (see slots.go).
func (c RegistryConfig) slotConfig(name string) SlotConfig {
	return c.Slots[name]
}

// SetSlotConfig sets the SlotConfig for the named slot, lazily allocating the
// map so it is safe to call on a zero-value RegistryConfig. Used by the admin
// store to build a single-slot config for a connection test and to assemble the
// full config from the persisted per-slot settings rows.
func (c *RegistryConfig) SetSlotConfig(name string, sc SlotConfig) {
	if c.Slots == nil {
		c.Slots = make(map[string]SlotConfig)
	}
	c.Slots[name] = sc
}

// Registry manages the lifecycle of provider slots (embedding, fact extraction,
// entity extraction). It instantiates the appropriate provider for each slot,
// wraps them in circuit breakers and the usage-recording middleware, and
// provides thread-safe accessors.
type Registry struct {
	mu        sync.RWMutex
	embedding EmbeddingProvider
	// llm holds the built LLM-kind providers keyed by slot name (fact, entity,
	// query_augment, ingestion_decision). A missing key means that slot is
	// unconfigured; GetLLM applies the slot's FallbackTo in that case. Keyed by
	// name so build/reload/accessors stay slot-agnostic (see slots.go).
	llm map[string]LLMProvider
	// reranker holds the built KindReranker provider, or nil when the reranker
	// slot is unconfigured. No fallback (like ask), so GetReranker returns nil
	// and the recall/ask rerank stages stay inert.
	reranker RerankProvider
	config   RegistryConfig

	// Host-keyed concurrency gates shared across all slots and subsystems that
	// target the same host, so an upstream host sees an aggregate in-flight cap
	// rather than a per-slot one. Keyed by hostKey(slot); separate maps give LLM
	// and embedding traffic independent limits even when they share a host.
	// Rebuilt on every Reload, reusing the existing gate for a host (keeping its
	// in-flight count) and only resizing when the limit changed.
	llmGates   map[string]*hostGate
	embedGates map[string]*hostGate
	// hostConcurrencyCfg resolves the live per-host limits at build time.
	// Injected (not read directly) so the provider package stays free of a
	// dependency on the settings service. nil → gating disabled (unlimited).
	hostConcurrencyCfg atomic.Pointer[func(context.Context) HostConcurrency]

	// circuitBreakerCfg resolves the live breaker thresholds (max failures,
	// base and max reset windows). Injected like hostConcurrencyCfg so the
	// provider package stays free of a settings dependency. Read live on each
	// breaker state evaluation (via the BoundsResolver closure installed in
	// buildProviders), so an operator changing the settings takes effect within
	// the settings-cache TTL with no registry reload. nil → static defaults.
	circuitBreakerCfg atomic.Pointer[func() CircuitBreakerBounds]

	// Wrapping infrastructure. Both may be nil; when nil, providers are
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
	// always read the latest value installed on this Registry, including
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

	// embedStore is the process-wide exact-match embedding cache, installed
	// via WithEmbeddingCache. nil disables caching. Atomic so it can be set
	// once at startup and read lock-free inside buildProviders on every
	// Reload, mirroring embedWrapper. The wrapper around each rebuilt inner
	// chain is recreated per Reload, but the store persists.
	embedStore atomic.Pointer[embedCacheStore]
	// embedCacheCounter observes embedding-cache hit/miss results for
	// Prometheus. nil disables the metric while leaving caching intact.
	// Atomic for the same reasons as tokenCounter, so the indirect closure
	// handed to the persistent embedStore reads the latest value regardless
	// of the order WithEmbedCacheCounter and WithEmbeddingCache are called.
	embedCacheCounter atomic.Pointer[EmbedCacheRecorder]
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
	// not pay the round-trip latency. Failures are non-fatal; the cache
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

// GetLLM returns the provider for an LLM slot. When the dedicated slot is
// unconfigured it applies the slot's FallbackTo (e.g. query_augment and
// ingestion_decision fall back to fact, a small task well within the fact
// model's capability). Returns nil only when neither the slot nor its fallback
// is configured.
func (r *Registry) GetLLM(name string) LLMProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.resolveLLM(name)
}

// resolveLLM resolves an LLM slot with fallback. Caller must hold r.mu.
func (r *Registry) resolveLLM(name string) LLMProvider {
	if p := r.llm[name]; p != nil {
		return p
	}
	if def, ok := SlotByName(name); ok && def.FallbackTo != "" {
		return r.llm[def.FallbackTo]
	}
	return nil
}

// GetFact returns the fact extraction LLM provider, or nil if unconfigured.
func (r *Registry) GetFact() LLMProvider { return r.GetLLM(SlotFact) }

// GetEntity returns the entity extraction LLM provider, or nil if unconfigured.
func (r *Registry) GetEntity() LLMProvider { return r.GetLLM(SlotEntity) }

// GetQueryAugment returns the query-augmentation provider (dedicated slot, else
// the fact fallback per its SlotDef).
func (r *Registry) GetQueryAugment() LLMProvider { return r.GetLLM(SlotQueryAugment) }

// GetIngestionDecision returns the ingestion-decision provider (dedicated slot,
// else the fact fallback per its SlotDef).
func (r *Registry) GetIngestionDecision() LLMProvider { return r.GetLLM(SlotIngestionDecision) }

// GetAsk returns the ask-synthesis provider. The slot has no fallback, so this
// returns nil whenever its dedicated provider is unconfigured; the ask service
// turns that nil into a clear "synthesis provider not configured" error rather
// than routing synthesis traffic onto the enrichment providers.
func (r *Registry) GetAsk() LLMProvider { return r.GetLLM(SlotAsk) }

// GetReranker returns the rerank provider, or nil if the reranker slot is
// unconfigured. The slot has no fallback, so callers treat nil as "reranking
// unavailable" and skip the rerank stage rather than routing it elsewhere.
func (r *Registry) GetReranker() RerankProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reranker
}

// SlotConfigured reports whether the named slot has a DEDICATED provider built
// (ignoring any fallback), driving the admin "configured" status so operators
// can tell a configured slot from a fallback. Unknown names return false.
func (r *Registry) SlotConfigured(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if name == SlotEmbedding {
		return r.embedding != nil
	}
	if name == SlotReranker {
		return r.reranker != nil
	}
	return r.llm[name] != nil
}

// Reload recreates all providers from a new configuration, swapping them
// atomically under the write lock. Invalidates the cached embedding
// dimension and immediately re-probes the new embedder so the first
// downstream EmbeddingDim caller does not pay the round-trip latency.
// Probe failures are non-fatal; the cache stays empty and a later
// EmbeddingDim call will retry.
//
// Wrapping uses the live receiver r so the indirect tokenCounter /
// embedWrapper closures inside the new wrappers reach the values
// installed on this Registry instance (via WithTokenCounter /
// WithEmbeddingWrapper). Building wrappers through a temporary Registry
// would orphan the closures against fields that were never set.
func (r *Registry) Reload(config RegistryConfig) error {
	r.mu.Lock()
	built, err := r.buildProviders(config)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	r.embedding = built.embedding
	r.llm = built.llm
	r.reranker = built.reranker
	r.llmGates = built.llmGates
	r.embedGates = built.embedGates
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
// Discovered by sending Embed("probe") and reading len(resp.Embeddings[0]);
// works identically across every provider because it measures the
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
// bailing early does not cancel the work; the result still populates
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

// EnrichmentAvailable returns true iff every Required slot is configured. The
// gate behind every enrichment + dreaming surface.
func (r *Registry) EnrichmentAvailable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, d := range Slots {
		if !d.Required {
			continue
		}
		if d.Kind == KindEmbedding {
			if r.embedding == nil {
				return false
			}
		} else if r.llm[d.Name] == nil {
			return false
		}
	}
	return true
}

// load is called from NewRegistry only. It writes the constructed
// providers directly onto r and is non-transactional. Reload uses
// buildProviders so a mid-build failure does not leave r partially
// mutated.
func (r *Registry) load(config RegistryConfig) error {
	built, err := r.buildProviders(config)
	if err != nil {
		return err
	}
	r.embedding = built.embedding
	r.llm = built.llm
	r.reranker = built.reranker
	r.llmGates = built.llmGates
	r.embedGates = built.embedGates
	r.config = config
	return nil
}

// builtProviders stages the providers constructed from a RegistryConfig so
// callers install them atomically under r.mu. A nil field means that slot
// was unconfigured.
type builtProviders struct {
	embedding EmbeddingProvider
	llm       map[string]LLMProvider // LLM-kind slots keyed by slot name
	reranker  RerankProvider         // KindReranker slot, nil when unconfigured
	// Host-keyed gates built for this config, installed on r alongside the
	// providers. nil maps when host gating is disabled.
	llmGates   map[string]*hostGate
	embedGates map[string]*hostGate
}

// buildProviders constructs each configured slot by iterating the canonical
// Slots list, wraps it via r's wrap methods (so closures capture the live
// receiver), and returns the staged values without writing to r. Callers
// install them atomically under r.mu.
func (r *Registry) buildProviders(config RegistryConfig) (builtProviders, error) {
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
		// Read the live-bounds resolver on each breaker evaluation so operator
		// settings changes apply without a rebuild. The atomic is re-read per
		// call (not captured), matching the host-concurrency pattern, so a
		// later WithCircuitBreaker swap is picked up too. nil resolver → the
		// breaker uses c's static thresholds.
		c.BoundsResolver = func() CircuitBreakerBounds {
			if ptr := r.circuitBreakerCfg.Load(); ptr != nil {
				return (*ptr)()
			}
			return CircuitBreakerBounds{}
		}
		return c
	}

	// Host gating is active whenever a concurrency resolver is installed (the
	// server always installs one; tests that don't get the pre-gate behavior).
	// The per-role limit is read live on every call via these closures, so a
	// settings change takes effect within the settings-cache TTL without a
	// restart or a registry reload.
	gatingOn := r.hostConcurrencyCfg.Load() != nil
	// roleLimit builds a live resolver for one role's per-host limit, reading the
	// injected config atomically on each call (0 when no resolver is installed).
	roleLimit := func(pick func(HostConcurrency) int) func(context.Context) int {
		return func(ctx context.Context) int {
			if ptr := r.hostConcurrencyCfg.Load(); ptr != nil {
				return pick((*ptr)(ctx))
			}
			return 0
		}
	}
	llmLimit := roleLimit(func(h HostConcurrency) int { return h.LLM })
	embedLimit := roleLimit(func(h HostConcurrency) int { return h.Embed })
	llmGates := make(map[string]*hostGate)
	embedGates := make(map[string]*hostGate)
	// gateFor returns the gate for host in target, reusing one already built
	// this pass, then the existing gate carried over from the prior config
	// (resized to the new limit so its in-flight count is preserved), else a
	// fresh gate. existing is r's current map for the role, read under the
	// caller's lock.
	gateFor := func(target, existing map[string]*hostGate, host string, lim int) *hostGate {
		if g := target[host]; g != nil {
			return g
		}
		if g := existing[host]; g != nil {
			g.setLimit(lim)
			target[host] = g
			return g
		}
		g := newHostGate(lim)
		target[host] = g
		return g
	}

	built := builtProviders{llm: make(map[string]LLMProvider), llmGates: llmGates, embedGates: embedGates}
	for _, def := range Slots {
		slot := config.slotConfig(def.Name)
		if slot.Type == "" {
			continue // unconfigured
		}
		host := hostKey(slot)
		if def.Kind == KindEmbedding {
			ep, err := createEmbeddingProvider(slot)
			if err != nil {
				return builtProviders{}, fmt.Errorf("%s slot: %w", def.Name, err)
			}
			embedder := r.wrapEmbedding(NewCircuitBreakerEmbedding(ep, breakerCfgFor(slot.Type, "embed")))
			// Host gate sits inside the cache but outside the usage recorder, so
			// a cache hit consumes no permit while a real upstream Embed is
			// bracketed by one. The gate's limit is re-resolved live per call.
			if gatingOn {
				gate := gateFor(embedGates, r.embedGates, host, normHostLimit(embedLimit(context.Background())))
				embedder = newGatedEmbedding(embedder, gate, embedLimit)
			}
			// The cache sits outermost (outside the usage recorder) so a full
			// hit records no token_usage row. Keyed on slot.Model so a model
			// change across Reload cannot return a stale vector.
			if store := r.embedStore.Load(); store != nil {
				embedder = newCachingEmbedding(embedder, slot.Model, store)
			}
			built.embedding = embedder
			continue
		}
		if def.Kind == KindReranker {
			rp, err := createRerankProvider(slot)
			if err != nil {
				return builtProviders{}, fmt.Errorf("%s slot: %w", def.Name, err)
			}
			// Circuit breaker (like embed/LLM) plus usage recording; no host gate.
			// The rerank stage is fail-soft at the call site, so a tripped breaker
			// just keeps the prior order (see CircuitBreakerRerank for why). Token
			// attribution still lands via the recorder.
			built.reranker = r.wrapRerank(
				NewCircuitBreakerRerank(rp, breakerCfgFor(slot.Type, "rerank")))
			continue
		}
		lp, err := createLLMProvider(slot)
		if err != nil {
			return builtProviders{}, fmt.Errorf("%s slot: %w", def.Name, err)
		}
		llmProv := r.wrapLLM(NewCircuitBreakerLLM(lp, breakerCfgFor(slot.Type, def.Name)))
		// Host gate is the outermost wrapper so the permit brackets only the
		// real upstream call and its breaker/recorder chain. Limit is live.
		if gatingOn {
			gate := gateFor(llmGates, r.llmGates, host, normHostLimit(llmLimit(context.Background())))
			llmProv = newGatedLLM(llmProv, gate, llmLimit)
		}
		built.llm[def.Name] = llmProv
	}
	return built, nil
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

// indirectEmbedCacheCounter returns an EmbedCacheRecorder closure that derefs
// r's atomic counter on every call, so the recorder installed on the
// persistent embedStore picks up the value regardless of whether
// WithEmbedCacheCounter ran before or after WithEmbeddingCache. Nil-firing
// when no counter is installed.
func (r *Registry) indirectEmbedCacheCounter() EmbedCacheRecorder {
	return func(hit bool, n int) {
		if ptr := r.embedCacheCounter.Load(); ptr != nil {
			(*ptr)(hit, n)
		}
	}
}

func (r *Registry) wrapLLM(inner LLMProvider) LLMProvider {
	if r.recorder == nil {
		return inner
	}
	return NewUsageRecordingLLM(inner, r.recorder, r.resolver).WithTokenCounter(r.indirectTokenCounter())
}

// wrapRerank wraps a rerank provider in the usage-recording middleware so every
// Rerank call lands a token_usage row stamped OperationRerank. When no recorder
// is configured (e.g. unit tests) the inner provider is returned as-is.
func (r *Registry) wrapRerank(inner RerankProvider) RerankProvider {
	if r.recorder == nil {
		return inner
	}
	return NewUsageRecordingRerank(inner, r.recorder, r.resolver).WithTokenCounter(r.indirectTokenCounter())
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

// WithEmbedCacheCounter installs the callback fired for each embedding-cache
// lookup so Prometheus can track the hit rate. Passing nil disables the
// metric. Safe to call before or after WithEmbeddingCache: the store reads the
// recorder through an indirect closure that derefs this atomic live.
func (r *Registry) WithEmbedCacheCounter(c EmbedCacheRecorder) *Registry {
	if c == nil {
		r.embedCacheCounter.Store(nil)
	} else {
		r.embedCacheCounter.Store(&c)
	}
	return r
}

// WithEmbeddingCache installs the process-wide exact-match embedding cache,
// reading its live configuration (enabled/size/TTL) through cfg on every Embed
// call. Passing nil disables caching. Call before the post-construction Reload
// so the cache wraps the embedding provider, exactly like WithEmbeddingWrapper.
func (r *Registry) WithEmbeddingCache(cfg func(context.Context) EmbedCacheConfig) *Registry {
	if cfg == nil {
		r.embedStore.Store(nil)
		return r
	}
	store := newEmbedCacheStore(cfg)
	store.recorder = r.indirectEmbedCacheCounter()
	r.embedStore.Store(store)
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

// WithHostConcurrency installs the callback that resolves the live per-host
// in-flight limits for LLM and embedding traffic. Read once per buildProviders
// (construction and every Reload), so changing the underlying settings takes
// effect on the next registry reload, which the settings save path already
// triggers. Passing nil disables gating. Returns the receiver for chaining.
func (r *Registry) WithHostConcurrency(fn func(context.Context) HostConcurrency) *Registry {
	if fn == nil {
		r.hostConcurrencyCfg.Store(nil)
	} else {
		r.hostConcurrencyCfg.Store(&fn)
	}
	return r
}

// WithCircuitBreaker installs the callback that resolves the live breaker
// thresholds (max consecutive failures, base reset window, max reset window).
// Each breaker reads it on every open/half-open/failure evaluation via the
// BoundsResolver closure wired in buildProviders, so changing the underlying
// settings takes effect within the settings-cache TTL with no restart and no
// registry reload. The callback must be cheap (a cached settings read); it is
// invoked under a breaker mutex. Passing nil restores the static defaults.
// Returns the receiver for chaining.
func (r *Registry) WithCircuitBreaker(fn func() CircuitBreakerBounds) *Registry {
	if fn == nil {
		r.circuitBreakerCfg.Store(nil)
	} else {
		r.circuitBreakerCfg.Store(&fn)
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
	ptype := NormalizeProviderType(config.Type)
	switch {
	case isOpenAICompatibleType(ptype):
		return NewOpenAIProvider(OpenAIConfig{
			BaseURL:         config.BaseURL,
			APIKey:          config.APIKey,
			DefaultModel:    config.Model,
			Timeout:         slotTimeout(config.Timeout),
			ProviderType:    ptype,
			CustomHeaders:   config.CustomHeaders,
			ExtraBody:       config.ExtraBody,
			DisableThinking: thinkingDisabled(config.DisableThinking),
		}), nil

	case ptype == ProviderTypeGemini:
		return NewGeminiProvider(GeminiConfig{
			APIKey:          config.APIKey,
			DefaultModel:    config.Model,
			BaseURL:         config.BaseURL,
			Timeout:         slotTimeout(config.Timeout),
			CustomHeaders:   config.CustomHeaders,
			DisableThinking: thinkingDisabled(config.DisableThinking),
		}), nil

	case ptype == ProviderTypeAnthropic:
		return NewAnthropicProvider(AnthropicConfig{
			APIKey:             config.APIKey,
			DefaultModel:       config.Model,
			BaseURL:            config.BaseURL,
			Timeout:            slotTimeout(config.Timeout),
			PromptCacheEnabled: config.PromptCacheEnabled,
			JSONModeToolUse:    config.JSONModeToolUse,
			CustomHeaders:      config.CustomHeaders,
		}), nil

	default:
		return nil, fmt.Errorf("unsupported provider type: %q", config.Type)
	}
}

// createEmbeddingProvider is a factory that creates the right EmbeddingProvider
// based on the slot configuration's Type field. Anthropic does not support
// embeddings, so requesting it returns an error.
func createEmbeddingProvider(config SlotConfig) (EmbeddingProvider, error) {
	ptype := NormalizeProviderType(config.Type)
	switch {
	case isOpenAICompatibleType(ptype):
		return NewOpenAIProvider(OpenAIConfig{
			BaseURL:               config.BaseURL,
			APIKey:                config.APIKey,
			DefaultEmbeddingModel: config.Model,
			EmbeddingDimension:    config.Dimension,
			Timeout:               slotTimeout(config.Timeout),
			ProviderType:          ptype,
			CustomHeaders:         config.CustomHeaders,
			ExtraBody:             config.ExtraBody,
		}), nil

	case ptype == ProviderTypeGemini:
		return NewGeminiProvider(GeminiConfig{
			APIKey:                config.APIKey,
			DefaultEmbeddingModel: config.Model,
			BaseURL:               config.BaseURL,
			Timeout:               slotTimeout(config.Timeout),
			CustomHeaders:         config.CustomHeaders,
		}), nil

	case ptype == ProviderTypeAnthropic:
		return nil, fmt.Errorf("anthropic does not support embeddings")

	default:
		return nil, fmt.Errorf("unsupported provider type: %q", config.Type)
	}
}
