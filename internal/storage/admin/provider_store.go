package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ProviderAdminDeps groups the dependencies for ProviderAdminStore.
// memoryRepo, entityRepo, vectorStore, and db are only required for the
// embedding-model switch cascade; tests that don't exercise it may leave
// them nil.
type ProviderAdminDeps struct {
	Registry     *provider.Registry
	SettingsRepo *storage.SettingsRepo
	Settings     *service.SettingsService
	MemoryRepo   *storage.MemoryRepo
	EntityRepo   *storage.EntityRepo
	VectorStore  storage.VectorStore
	DB           storage.DB

	// CascadeCtx, if set, is passed to the detached entity-re-embed
	// goroutine launched after a confirmed model switch. Without it the
	// goroutine would survive server shutdown and write to a closed pool.
	CascadeCtx context.Context
}

type ProviderAdminStore struct {
	deps ProviderAdminDeps

	cascadeMu sync.Mutex // serializes destructive embedding-model swaps

	// contextWindowCache memoizes the last detected context_length per
	// (type|url|model) tuple so back-to-back GetProviderConfig calls don't
	// hammer Ollama's /api/show or OpenRouter's /models on every page render.
	// Entries expire after contextWindowCacheTTL; a slot edit also invalidates
	// the cache key implicitly because the tuple changes.
	contextWindowMu    sync.Mutex
	contextWindowCache map[string]contextWindowCacheEntry
}

type contextWindowCacheEntry struct {
	// effective is the smaller of the model's GGUF ceiling and any
	// Ollama-side num_ctx (Modelfile PARAMETER or runtime override). This
	// is what gets propagated as slot.context_window, the actual budget
	// the pipeline can spend. OpenRouter has no dual concept; effective
	// equals modelMax there.
	effective int
	// modelMax is the model's GGUF-declared ceiling. Stored alongside
	// effective so the UI can render "(model max N)" when it differs.
	modelMax int
	storedAt time.Time
}

const contextWindowCacheTTL = 60 * time.Second

func NewProviderAdminStore(deps ProviderAdminDeps) *ProviderAdminStore {
	return &ProviderAdminStore{deps: deps}
}

func (s *ProviderAdminStore) GetProviderConfig(ctx context.Context) (api.ProviderConfigResponse, error) {
	// Probe every slot concurrently; each call may issue an HTTP request to
	// Ollama (/api/show) or OpenRouter (/models) on a cache miss, so a serial
	// sweep would cost len(Slots) × probe-timeout. errgroup caps it at one
	// probe-timeout regardless of slot count. Results are written into a
	// pre-sized slice by index to preserve canonical order without a mutex.
	resp := make(api.ProviderConfigResponse, len(provider.Slots))
	g, gctx := errgroup.WithContext(ctx)
	for i, def := range provider.Slots {
		g.Go(func() error {
			st := s.slotStatus(gctx, def.Name)
			st.Slot = def.Name
			st.Label = def.Label
			st.Description = def.Description
			st.Required = def.Required
			resp[i] = st
			return nil
		})
	}
	_ = g.Wait()
	return resp, nil
}

func (s *ProviderAdminStore) slotStatus(ctx context.Context, slot string) api.ProviderSlotStatus {
	// Read persisted config from the settings table. An empty Type means the
	// slot is not really configured.
	persisted := s.loadPersistedSlot(ctx, slot)
	if persisted != nil && persisted.Type == "" {
		persisted = nil
	}

	// Check whether a DEDICATED provider for this slot is loaded in the
	// registry. SlotConfigured ignores fallback, so an unconfigured optional
	// slot reads as "not configured" rather than borrowing fact's status.
	alive := false
	if s.deps.Registry != nil {
		alive = s.deps.Registry.SlotConfigured(slot)
	}

	if persisted == nil {
		return api.ProviderSlotStatus{
			Configured: false,
			Status:     "not_configured",
		}
	}

	status := "ok"
	if !alive {
		status = "error"
	}

	// Probe failures leave Dimensions nil; alive/error is carried by status.
	var dimensions *int
	if slot == "embedding" && alive && s.deps.Registry != nil {
		if d, err := s.deps.Registry.EmbeddingDim(ctx); err == nil && d > 0 {
			dimensions = &d
		}
	}

	// Best-effort context-window detection. Only providers that expose it
	// via API are queried; the rest get nil and the UI renders the muted
	// "see provider docs" placeholder. Failures are swallowed; a flaky
	// Ollama instance must not break the whole providers page.
	//
	// effective is the actual ceiling the pipeline can spend (min of
	// model max and any Ollama-side num_ctx); modelMax is the model's
	// GGUF max, surfaced to the UI only when it exceeds the effective
	// value so the user can see the headroom story.
	var contextWindow, contextWindowMax *int
	if alive {
		eff, max := s.detectContextWindow(ctx, persisted)
		if eff > 0 {
			contextWindow = &eff
		}
		if max > 0 && max != eff {
			contextWindowMax = &max
		}
	}

	return api.ProviderSlotStatus{
		Configured:       true,
		Type:             persisted.Type,
		URL:              persisted.URL,
		Model:            persisted.Model,
		Dimensions:       dimensions,
		ContextWindow:    contextWindow,
		ContextWindowMax: contextWindowMax,
		Timeout:          persisted.Timeout,
		Status:           status,
		APIKeySet:        persisted.APIKey != "",
		CustomHeaderKeys: sortedHeaderKeys(persisted.CustomHeaders),
		ExtraBody:        persisted.ExtraBody,
		DisableThinking:  persisted.DisableThinking,
	}
}

// sortedHeaderKeys returns the header names of m in sorted order, or nil when
// empty. Only names are exposed on the read path; values may be secrets.
func sortedHeaderKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(m))
}

// detectContextWindow returns the effective and maximum context window
// for the slot's configured model. effective is the actual budget the
// pipeline can spend (min of the model's GGUF ceiling and any
// Ollama-side num_ctx); modelMax is the model's GGUF ceiling. For
// providers that don't expose either signal via API, returns (0, 0)
// and the UI shows the muted fallback. Probe failures also collapse to
// (0, 0).
//
// Results are cached for contextWindowCacheTTL to avoid hammering
// Ollama or OpenRouter on every providers-page render. Cache keyed on
// the (type, url, model) tuple so any slot edit naturally invalidates.
// The nul-byte separator prevents collisions when a URL contains pipes
// or other separator-like characters.
//
// OpenRouter's /models endpoint reports a single context_length per
// model with no concept of a runtime override, so for OpenRouter slots
// effective == modelMax and the UI suffix collapses.
func (s *ProviderAdminStore) detectContextWindow(ctx context.Context, slot *api.ProviderSlotConfig) (effective, modelMax int) {
	if slot == nil || slot.Model == "" {
		return 0, 0
	}
	// Skip providers that don't expose context_length via API; avoids the
	// per-render cache lookup and write for OpenAI / Anthropic / Gemini and the
	// rest of the OpenAI-compatible family (openai-compatible / vLLM / SGLang).
	if slot.Type != provider.ProviderTypeOllama && slot.Type != provider.ProviderTypeOpenRouter {
		return 0, 0
	}

	cacheKey := slot.Type + "\x00" + slot.URL + "\x00" + slot.Model

	s.contextWindowMu.Lock()
	if s.contextWindowCache == nil {
		s.contextWindowCache = map[string]contextWindowCacheEntry{}
	}
	if entry, ok := s.contextWindowCache[cacheKey]; ok && time.Since(entry.storedAt) < contextWindowCacheTTL {
		s.contextWindowMu.Unlock()
		return entry.effective, entry.modelMax
	}
	s.contextWindowMu.Unlock()

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	switch slot.Type {
	case provider.ProviderTypeOllama:
		client := provider.NewOllamaClient(provider.OllamaConfig{
			BaseURL:       s.resolveOllamaURL(slot.URL),
			CustomHeaders: slot.CustomHeaders,
		})
		if eff, max, err := client.ContextLength(probeCtx, slot.Model); err == nil {
			effective, modelMax = eff, max
		}
	case provider.ProviderTypeOpenRouter:
		if cw, err := provider.OpenRouterContextLength(probeCtx, slot.URL, slot.APIKey, slot.Model, slot.CustomHeaders); err == nil {
			effective, modelMax = cw, cw
		}
	}

	s.contextWindowMu.Lock()
	s.contextWindowCache[cacheKey] = contextWindowCacheEntry{
		effective: effective,
		modelMax:  modelMax,
		storedAt:  time.Now(),
	}
	s.contextWindowMu.Unlock()

	return effective, modelMax
}

func (s *ProviderAdminStore) TestProvider(ctx context.Context, req api.ProviderTestRequest) (*api.ProviderTestResult, error) {
	start := time.Now()

	def, ok := provider.SlotByName(req.Slot)
	if !ok {
		return &api.ProviderTestResult{
			Success: false,
			Message: "unknown slot: " + req.Slot,
		}, nil
	}

	slotCfg := provider.SlotConfig{
		Type:            req.Config.Type,
		BaseURL:         req.Config.URL,
		APIKey:          req.Config.APIKey,
		Model:           req.Config.Model,
		CustomHeaders:   req.Config.CustomHeaders,
		ExtraBody:       req.Config.ExtraBody,
		DisableThinking: req.Config.DisableThinking,
	}
	if req.Config.Timeout != nil {
		slotCfg.Timeout = *req.Config.Timeout
	}

	// Build a single-slot registry and probe it by kind.
	cfg := provider.RegistryConfig{}
	cfg.SetSlotConfig(req.Slot, slotCfg)

	tmpReg, err := provider.NewRegistry(cfg, nil, nil)
	if err != nil {
		return &api.ProviderTestResult{
			Success:   false,
			Message:   fmt.Sprintf("failed to create provider: %v", err),
			LatencyMs: time.Since(start).Milliseconds(),
		}, nil
	}

	notCreated := func() (*api.ProviderTestResult, error) {
		return &api.ProviderTestResult{Success: false, Message: "provider not created", LatencyMs: time.Since(start).Milliseconds()}, nil
	}
	if def.Kind == provider.KindEmbedding {
		p := tmpReg.GetEmbedding()
		if p == nil {
			return notCreated()
		}
		_, err = p.Embed(ctx, &provider.EmbeddingRequest{Input: []string{"test"}})
	} else {
		p := tmpReg.GetLLM(req.Slot)
		if p == nil {
			return notCreated()
		}
		_, err = p.Complete(ctx, &provider.CompletionRequest{
			Messages:    []provider.Message{{Role: "user", Content: "test"}},
			MaxTokens:   10,
			Temperature: 0,
		})
	}

	latency := time.Since(start).Milliseconds()
	if err != nil {
		return &api.ProviderTestResult{
			Success:   false,
			Message:   fmt.Sprintf("test failed: %v", err),
			LatencyMs: latency,
		}, nil
	}

	return &api.ProviderTestResult{
		Success:   true,
		Message:   fmt.Sprintf("%s provider working", req.Slot),
		LatencyMs: latency,
	}, nil
}

func (s *ProviderAdminStore) UpdateProviderSlot(ctx context.Context, slot string, cfg api.ProviderSlotConfig, opts api.UpdateProviderSlotOpts) (*api.UpdateProviderSlotResult, error) {
	// Persist the base URL only; the provider appends its own versioned route
	// path, so a version segment in the stored value would double-stack. This
	// self-heals configs saved before the base-URL-only convention.
	cfg.URL = provider.NormalizeBaseURL(cfg.URL)

	// Load the currently-stored config once: it feeds both the blank-secret
	// merge and the embedding-model-change comparison below.
	existing := s.loadPersistedSlot(ctx, slot)

	// Merge secrets that the client may have intentionally left blank against
	// the stored config. Runs before the embedding-cascade branch so both
	// persistence paths see the merged values.
	cfg = mergeProviderSecrets(cfg, existing, opts)

	// An embedding-model change routes through the destructive cascade.
	// Same-model edits (URL, key, timeout) bypass it.
	if slot == provider.SlotEmbedding && existing != nil {
		oldModel := existing.Model
		if oldModel != "" && cfg.Model != "" && cfg.Model != oldModel {
			return s.switchEmbeddingModel(ctx, oldModel, cfg, opts)
		}
	}

	if err := s.persistAndReload(ctx, slot, cfg); err != nil {
		return nil, err
	}
	return nil, nil
}

// mergeProviderSecrets reconciles client-supplied secrets with the currently
// stored config so blank values mean "keep" rather than "erase":
//   - api_key: ClearAPIKey forces it empty; otherwise a blank incoming key
//     preserves the stored key (fixes the historical wipe where editing any
//     field without re-typing the key dropped it).
//   - custom_headers: the incoming map is the new full set (names absent from it
//     are removed), but a header sent with a blank value keeps its stored value
//     so the UI can render existing (masked) headers and re-save without leaking
//     or losing them.
func mergeProviderSecrets(cfg api.ProviderSlotConfig, existing *api.ProviderSlotConfig, opts api.UpdateProviderSlotOpts) api.ProviderSlotConfig {
	switch {
	case opts.ClearAPIKey:
		cfg.APIKey = ""
	case cfg.APIKey == "" && existing != nil:
		cfg.APIKey = existing.APIKey
	}

	if len(cfg.CustomHeaders) > 0 {
		merged := make(map[string]string, len(cfg.CustomHeaders))
		for name, value := range cfg.CustomHeaders {
			if name == "" {
				continue
			}
			if value == "" {
				if existing != nil {
					if prev, ok := existing.CustomHeaders[name]; ok {
						merged[name] = prev
					}
				}
				continue
			}
			merged[name] = value
		}
		if len(merged) == 0 {
			cfg.CustomHeaders = nil
		} else {
			cfg.CustomHeaders = merged
		}
	}

	return cfg
}

// loadPersistedSlot returns the currently-stored config for a slot, or nil when
// none is stored or it cannot be decoded.
func (s *ProviderAdminStore) loadPersistedSlot(ctx context.Context, slot string) *api.ProviderSlotConfig {
	if s.deps.SettingsRepo == nil {
		return nil
	}
	setting, err := s.deps.SettingsRepo.Get(ctx, "provider."+slot, "global")
	if err != nil || setting == nil {
		return nil
	}
	var cfg api.ProviderSlotConfig
	if err := json.Unmarshal(setting.Value, &cfg); err != nil {
		return nil
	}
	// Normalize legacy type aliases (e.g. "custom" -> "openai-compatible") so the
	// status display, secret-merge, and update comparison all see the canonical
	// value even before the boot-time migration has rewritten the stored row.
	cfg.Type = provider.NormalizeProviderType(cfg.Type)
	return &cfg
}

func (s *ProviderAdminStore) persistAndReload(ctx context.Context, slot string, cfg api.ProviderSlotConfig) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal provider config: %w", err)
	}

	setting := &model.Setting{
		Key:   "provider." + slot,
		Value: json.RawMessage(value),
		Scope: "global",
	}
	if err := s.deps.SettingsRepo.Set(ctx, setting); err != nil {
		return err
	}

	if s.deps.Registry != nil {
		newCfg := s.buildRegistryConfigFromDB(ctx)
		if err := s.deps.Registry.Reload(newCfg); err != nil {
			slog.Warn("provider hot-reload failed", "err", err)
		}
	}
	return nil
}

// switchEmbeddingModel runs the destructive cascade for an
// embedding-model change. Without ConfirmInvalidate it returns row
// counts and NeedsConfirmation=true; with it, truncates every vector
// table/collection (no cross-model retrieval), NULLs embedding_dim,
// persists the new config, force-enqueues memories, and kicks off the
// entity re-embed in a detached goroutine.
func (s *ProviderAdminStore) switchEmbeddingModel(
	ctx context.Context,
	oldModel string,
	cfg api.ProviderSlotConfig,
	opts api.UpdateProviderSlotOpts,
) (*api.UpdateProviderSlotResult, error) {
	if s.deps.MemoryRepo == nil || s.deps.EntityRepo == nil || s.deps.VectorStore == nil || s.deps.DB == nil {
		return nil, fmt.Errorf("embedding-model switch cascade requires memoryRepo, entityRepo, vectorStore, and db; one or more is nil")
	}

	if !opts.ConfirmInvalidate {
		// Counts are only useful in the confirmation-modal payload. The
		// confirmed branch gets exact counts back from the UPDATEs and
		// does not need this pre-count.
		memCount, _ := s.deps.MemoryRepo.CountWithEmbeddingDim(ctx)
		entCount, _ := s.deps.EntityRepo.CountWithEmbeddingDim(ctx)
		return &api.UpdateProviderSlotResult{
			NeedsConfirmation: true,
			OldModel:          oldModel,
			NewModel:          cfg.Model,
			MemoriesAffected:  memCount,
			EntitiesAffected:  entCount,
		}, nil
	}

	// Serialize destructive cascades; concurrent swaps would race on
	// UpdateEmbeddingDimBatch and the goroutine launches below.
	if !s.cascadeMu.TryLock() {
		return nil, fmt.Errorf("an embedding-model switch is already in progress")
	}
	defer s.cascadeMu.Unlock()

	if err := s.deps.VectorStore.TruncateAllVectors(ctx); err != nil {
		return nil, fmt.Errorf("cascade: truncate vectors: %w", err)
	}

	memNulled, err := s.deps.MemoryRepo.ClearAllEmbeddingDims(ctx)
	if err != nil {
		return nil, fmt.Errorf("cascade: NULL memory embedding_dim: %w", err)
	}
	entNulled, err := s.deps.EntityRepo.ClearAllEmbeddingDims(ctx)
	if err != nil {
		return nil, fmt.Errorf("cascade: NULL entity embedding_dim: %w", err)
	}

	if err := s.persistAndReload(ctx, "embedding", cfg); err != nil {
		return nil, fmt.Errorf("cascade: persist + reload: %w", err)
	}

	jobs, err := storage.EnqueueAllLiveMemories(ctx, s.deps.DB)
	if err != nil {
		return nil, fmt.Errorf("cascade: enqueue memory re-embed jobs: %w", err)
	}

	// Detached so the HTTP request returns promptly. Use the
	// server-lifecycle context if available so the loop is cancelled on
	// shutdown rather than writing to a closed pool.
	if s.deps.Registry != nil {
		if embedder := s.deps.Registry.GetEmbedding(); embedder != nil {
			bgCtx := s.deps.CascadeCtx
			if bgCtx == nil {
				bgCtx = context.Background()
			}
			pageSize := s.deps.Settings.ResolveIntWithDefault(bgCtx,
				service.SettingEnrichmentWorkerEmbedInputCap, "global")
			timeout := s.deps.Settings.ResolveDurationSecondsWithDefault(bgCtx,
				service.SettingEnrichmentWorkerEmbedTimeoutSeconds, "global")
			go func(emb provider.EmbeddingProvider) {
				if _, rerr := enrichment.ReembedAllEntities(bgCtx, s.deps.EntityRepo, s.deps.VectorStore, emb, pageSize, timeout); rerr != nil {
					slog.Error("cascade: entity re-embed loop failed", "err", rerr)
				}
			}(embedder)
		}
	}

	return &api.UpdateProviderSlotResult{
		OldModel:            oldModel,
		NewModel:            cfg.Model,
		MemoriesAffected:    memNulled,
		EntitiesAffected:    entNulled,
		MemoryJobsEnqueued:  jobs,
		EntityReembedQueued: true,
	}, nil
}

// LoadProviderRegistryConfig reads every provider slot setting from the
// database (iterating the canonical provider.Slots) and assembles a
// RegistryConfig. Errors fetching or decoding any slot are swallowed so a
// malformed row in one slot doesn't poison the others; the affected slot
// stays empty (treated as unconfigured) and its provider will report
// unavailable until it's repaired through the admin UI.
func LoadProviderRegistryConfig(ctx context.Context, settingsRepo *storage.SettingsRepo) provider.RegistryConfig {
	var cfg provider.RegistryConfig
	promptCache := providerSettingBool(ctx, settingsRepo, service.SettingProviderPromptCacheEnabled)
	anthropicJSONToolUse := providerSettingBool(ctx, settingsRepo, service.SettingProviderAnthropicJSONToolUse)
	for _, def := range provider.Slots {
		setting, err := settingsRepo.Get(ctx, def.SettingKey(), "global")
		if err != nil {
			continue
		}
		var apiCfg api.ProviderSlotConfig
		if err := json.Unmarshal(setting.Value, &apiCfg); err != nil {
			continue
		}
		sc := provider.SlotConfig{
			Type:               provider.NormalizeProviderType(apiCfg.Type),
			BaseURL:            apiCfg.URL,
			APIKey:             apiCfg.APIKey,
			Model:              apiCfg.Model,
			PromptCacheEnabled: promptCache,
			JSONModeToolUse:    anthropicJSONToolUse,
			CustomHeaders:      apiCfg.CustomHeaders,
			ExtraBody:          apiCfg.ExtraBody,
			DisableThinking:    apiCfg.DisableThinking,
		}
		if apiCfg.Timeout != nil {
			sc.Timeout = *apiCfg.Timeout
		}
		cfg.SetSlotConfig(def.Name, sc)
	}
	return cfg
}

// MigrateProviderTypes rewrites any provider slot whose stored type is the
// pre-0.5.4 alias "custom" to its canonical "openai-compatible" value,
// persisting the change. Idempotent and safe to run on every boot: slots that
// are already canonical (or unset) are left untouched. Returns the number of
// slots rewritten. Read/decoded errors on a single slot are skipped so one bad
// row cannot block the others.
func MigrateProviderTypes(ctx context.Context, settingsRepo *storage.SettingsRepo) (int, error) {
	migrated := 0
	for _, def := range provider.Slots {
		setting, err := settingsRepo.Get(ctx, def.SettingKey(), "global")
		if err != nil || setting == nil {
			continue
		}
		var apiCfg api.ProviderSlotConfig
		if err := json.Unmarshal(setting.Value, &apiCfg); err != nil {
			continue
		}
		canonical := provider.NormalizeProviderType(apiCfg.Type)
		if canonical == apiCfg.Type {
			continue // already canonical (or empty/unconfigured)
		}
		apiCfg.Type = canonical
		value, err := json.Marshal(apiCfg)
		if err != nil {
			return migrated, fmt.Errorf("marshal migrated slot %q: %w", def.Name, err)
		}
		if err := settingsRepo.Set(ctx, &model.Setting{
			Key:   def.SettingKey(),
			Value: json.RawMessage(value),
			Scope: "global",
		}); err != nil {
			return migrated, fmt.Errorf("persist migrated slot %q: %w", def.Name, err)
		}
		migrated++
	}
	return migrated, nil
}

// providerSettingBool resolves a boolean-typed global setting from the repo,
// falling back to the registered default when unset or unparseable.
func providerSettingBool(ctx context.Context, repo *storage.SettingsRepo, key string) bool {
	if s, err := repo.Get(ctx, key, "global"); err == nil {
		var v bool
		if json.Unmarshal(s.Value, &v) == nil {
			return v
		}
	}
	def, _ := service.GetDefault(key)
	return def == "true"
}

func (s *ProviderAdminStore) buildRegistryConfigFromDB(ctx context.Context) provider.RegistryConfig {
	return LoadProviderRegistryConfig(ctx, s.deps.SettingsRepo)
}

func (s *ProviderAdminStore) ListOllamaModels(ctx context.Context, ollamaURL string, headers map[string]string) ([]api.OllamaModel, error) {
	url := s.resolveOllamaURL(ollamaURL)
	client := provider.NewOllamaClient(provider.OllamaConfig{
		BaseURL:       url,
		CustomHeaders: s.resolveOllamaHeaders(ollamaURL, headers),
	})

	models, err := client.ListModels(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]api.OllamaModel, len(models))
	for i, m := range models {
		result[i] = api.OllamaModel{
			Name:       m.Name,
			Size:       m.Size,
			ModifiedAt: m.ModifiedAt.Format(time.RFC3339),
		}
	}
	return result, nil
}

func (s *ProviderAdminStore) PullOllamaModel(_ context.Context, modelName string, ollamaURL string, headers map[string]string) error {
	url := s.resolveOllamaURL(ollamaURL)
	client := provider.NewOllamaClient(provider.OllamaConfig{
		BaseURL:       url,
		CustomHeaders: s.resolveOllamaHeaders(ollamaURL, headers),
	})
	// Use a detached context; model pulls can take minutes and must not be
	// cancelled when the HTTP request completes.
	return client.PullModel(context.Background(), modelName, nil)
}

// ListProviderModels enumerates the models an OpenAI-compatible server (vLLM,
// SGLang, or a generic openai-compatible endpoint) reports at GET /v1/models. The
// admin UI calls this to detect the served model id for single-model servers so
// the operator does not have to hand-type it. URL comes from the in-progress edit
// form; headers fall back to the matching saved slot's so a proxied endpoint keeps
// working before re-entry, mirroring resolveOllamaHeaders. The API key is borrowed
// from the matching saved slot only (the form key is never forwarded); a brand-new
// authenticated endpoint can carry auth via custom headers.
func (s *ProviderAdminStore) ListProviderModels(ctx context.Context, url string, headers map[string]string) ([]string, error) {
	resolvedHeaders, apiKey := s.resolveProviderModelCreds(url, headers)
	return provider.ListOpenAIModels(ctx, url, apiKey, resolvedHeaders)
}

// resolveProviderModelCreds resolves the headers and API key to use for an ad-hoc
// /v1/models probe. Forwarded form headers win; otherwise it borrows both headers
// and the API key from a saved slot whose normalized URL matches the probe URL, so
// editing an existing authenticated/proxied slot works without re-entering secrets.
func (s *ProviderAdminStore) resolveProviderModelCreds(url string, formHeaders map[string]string) (map[string]string, string) {
	if s.deps.Registry == nil {
		return formHeaders, ""
	}
	target := provider.NormalizeBaseURL(url)
	cfg := s.deps.Registry.GetConfig()
	for _, slot := range []provider.SlotConfig{
		cfg.Embedding, cfg.Fact, cfg.Entity, cfg.QueryAugment, cfg.IngestionDecision,
	} {
		if slot.BaseURL == "" || provider.NormalizeBaseURL(slot.BaseURL) != target {
			continue
		}
		h := formHeaders
		if len(h) == 0 {
			h = slot.CustomHeaders
		}
		return h, slot.APIKey
	}
	return formHeaders, ""
}

// resolveOllamaHeaders returns the custom headers to attach to an ad-hoc Ollama
// management call. Headers supplied by the in-progress edit form take
// precedence; otherwise it falls back to the headers persisted on the matching
// Ollama slot so a saved proxy config keeps working without re-entry.
func (s *ProviderAdminStore) resolveOllamaHeaders(override string, formHeaders map[string]string) map[string]string {
	if len(formHeaders) > 0 {
		return formHeaders
	}
	if s.deps.Registry == nil {
		return nil
	}
	cfg := s.deps.Registry.GetConfig()
	for _, slot := range []provider.SlotConfig{cfg.Embedding, cfg.Fact, cfg.Entity} {
		if !isOllamaSlot(slot) || len(slot.CustomHeaders) == 0 {
			continue
		}
		// When an explicit URL override is given, only borrow headers from a
		// slot that targets the same host.
		if override != "" && provider.NormalizeBaseURL(slot.BaseURL) != provider.NormalizeBaseURL(override) {
			continue
		}
		return slot.CustomHeaders
	}
	return nil
}

// isOllamaSlot reports whether a slot config targets an Ollama server, by type
// or by the default Ollama port. Single source of truth for the heuristic that
// both resolveOllamaURL and resolveOllamaHeaders depend on.
func isOllamaSlot(slot provider.SlotConfig) bool {
	return strings.Contains(slot.Type, "ollama") || strings.Contains(slot.BaseURL, ":11434")
}

// resolveOllamaURL returns the Ollama server URL to use. If override is
// non-empty it is used directly. Otherwise, the registry config is inspected
// for any slot whose BaseURL contains ":11434" or whose Type contains "ollama".
// Falls back to http://localhost:11434.
func (s *ProviderAdminStore) resolveOllamaURL(override string) string {
	if override != "" {
		return provider.NormalizeBaseURL(override)
	}

	if s.deps.Registry != nil {
		cfg := s.deps.Registry.GetConfig()
		for _, slot := range []provider.SlotConfig{cfg.Embedding, cfg.Fact, cfg.Entity} {
			if isOllamaSlot(slot) && slot.BaseURL != "" {
				return provider.NormalizeBaseURL(slot.BaseURL)
			}
		}
	}

	return "http://localhost:11434"
}
