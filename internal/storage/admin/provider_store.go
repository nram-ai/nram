package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ProviderAdminDeps groups the dependencies for ProviderAdminStore.
// memoryRepo, entityRepo, vectorStore, and db are only required for the
// embedding-model switch cascade; tests that don't exercise it may leave
// them nil.
type ProviderAdminDeps struct {
	Registry     *provider.Registry
	SettingsRepo *storage.SettingsRepo
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
}

func NewProviderAdminStore(deps ProviderAdminDeps) *ProviderAdminStore {
	return &ProviderAdminStore{deps: deps}
}

func (s *ProviderAdminStore) GetProviderConfig(ctx context.Context) (*api.ProviderConfigResponse, error) {
	resp := &api.ProviderConfigResponse{
		Embedding: s.slotStatus(ctx, "embedding"),
		Fact:      s.slotStatus(ctx, "fact"),
		Entity:    s.slotStatus(ctx, "entity"),
	}
	return resp, nil
}

func (s *ProviderAdminStore) slotStatus(ctx context.Context, slot string) api.ProviderSlotStatus {
	// Read persisted config from the settings table.
	var persisted *api.ProviderSlotConfig
	if s.deps.SettingsRepo != nil {
		setting, err := s.deps.SettingsRepo.Get(ctx, "provider."+slot, "global")
		if err == nil {
			var cfg api.ProviderSlotConfig
			if json.Unmarshal(setting.Value, &cfg) == nil && cfg.Type != "" {
				persisted = &cfg
			}
		}
	}

	// Check if the live provider is loaded in the registry.
	alive := false
	if s.deps.Registry != nil {
		switch slot {
		case "embedding":
			alive = s.deps.Registry.GetEmbedding() != nil
		case "fact":
			alive = s.deps.Registry.GetFact() != nil
		case "entity":
			alive = s.deps.Registry.GetEntity() != nil
		}
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

	return api.ProviderSlotStatus{
		Configured: true,
		Type:       persisted.Type,
		URL:        persisted.URL,
		Model:      persisted.Model,
		Dimensions: dimensions,
		Timeout:    persisted.Timeout,
		Status:     status,
	}
}

func (s *ProviderAdminStore) TestProvider(ctx context.Context, req api.ProviderTestRequest) (*api.ProviderTestResult, error) {
	start := time.Now()

	cfg := provider.RegistryConfig{}
	slotCfg := provider.SlotConfig{
		Type:    req.Config.Type,
		BaseURL: req.Config.URL,
		APIKey:  req.Config.APIKey,
		Model:   req.Config.Model,
	}
	if req.Config.Timeout != nil {
		slotCfg.Timeout = *req.Config.Timeout
	}

	switch req.Slot {
	case "embedding":
		cfg.Embedding = slotCfg
	case "fact":
		cfg.Fact = slotCfg
	case "entity":
		cfg.Entity = slotCfg
	default:
		return &api.ProviderTestResult{
			Success: false,
			Message: "unknown slot: " + req.Slot,
		}, nil
	}

	tmpReg, err := provider.NewRegistry(cfg, nil, nil)
	if err != nil {
		return &api.ProviderTestResult{
			Success:   false,
			Message:   fmt.Sprintf("failed to create provider: %v", err),
			LatencyMs: time.Since(start).Milliseconds(),
		}, nil
	}

	switch req.Slot {
	case "embedding":
		p := tmpReg.GetEmbedding()
		if p == nil {
			return &api.ProviderTestResult{Success: false, Message: "provider not created", LatencyMs: time.Since(start).Milliseconds()}, nil
		}
		_, err = p.Embed(ctx, &provider.EmbeddingRequest{Input: []string{"test"}})
	case "fact":
		p := tmpReg.GetFact()
		if p == nil {
			return &api.ProviderTestResult{Success: false, Message: "provider not created", LatencyMs: time.Since(start).Milliseconds()}, nil
		}
		_, err = p.Complete(ctx, &provider.CompletionRequest{
			Messages:    []provider.Message{{Role: "user", Content: "test"}},
			MaxTokens:   10,
			Temperature: 0,
		})
	case "entity":
		p := tmpReg.GetEntity()
		if p == nil {
			return &api.ProviderTestResult{Success: false, Message: "provider not created", LatencyMs: time.Since(start).Milliseconds()}, nil
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
	// An embedding-model change routes through the destructive cascade.
	// Same-model edits (URL, key, timeout) bypass it.
	if slot == "embedding" {
		oldModel := s.currentEmbeddingModel(ctx)
		if oldModel != "" && cfg.Model != "" && cfg.Model != oldModel {
			return s.switchEmbeddingModel(ctx, oldModel, cfg, opts)
		}
	}

	if err := s.persistAndReload(ctx, slot, cfg); err != nil {
		return nil, err
	}
	return nil, nil
}

// currentEmbeddingModel returns "" when no embedding slot is persisted —
// callers treat that as a fresh setup, not a model swap.
func (s *ProviderAdminStore) currentEmbeddingModel(ctx context.Context) string {
	if s.deps.SettingsRepo == nil {
		return ""
	}
	setting, err := s.deps.SettingsRepo.Get(ctx, "provider.embedding", "global")
	if err != nil || setting == nil {
		return ""
	}
	var current api.ProviderSlotConfig
	if err := json.Unmarshal(setting.Value, &current); err != nil {
		return ""
	}
	return current.Model
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
			log.Printf("provider hot-reload failed: %v", err)
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

	// Serialize destructive cascades — concurrent swaps would race on
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

	jobs, err := storage.BackfillReembedAllJobs(ctx, s.deps.DB)
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
			go func(emb provider.EmbeddingProvider) {
				if _, rerr := enrichment.ReembedAllEntities(bgCtx, s.deps.EntityRepo, s.deps.VectorStore, emb); rerr != nil {
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

// buildRegistryConfigFromDB reads all three provider slot settings from the
// database and assembles a RegistryConfig for registry reload.
func (s *ProviderAdminStore) buildRegistryConfigFromDB(ctx context.Context) provider.RegistryConfig {
	var cfg provider.RegistryConfig
	slots := []struct {
		key  string
		dest *provider.SlotConfig
	}{
		{"provider.embedding", &cfg.Embedding},
		{"provider.fact", &cfg.Fact},
		{"provider.entity", &cfg.Entity},
	}
	for _, slot := range slots {
		setting, err := s.deps.SettingsRepo.Get(ctx, slot.key, "global")
		if err != nil {
			continue
		}
		var apiCfg api.ProviderSlotConfig
		if err := json.Unmarshal(setting.Value, &apiCfg); err != nil {
			continue
		}
		sc := provider.SlotConfig{
			Type:    apiCfg.Type,
			BaseURL: apiCfg.URL,
			APIKey:  apiCfg.APIKey,
			Model:   apiCfg.Model,
		}
		if apiCfg.Timeout != nil {
			sc.Timeout = *apiCfg.Timeout
		}
		*slot.dest = sc
	}
	return cfg
}

func (s *ProviderAdminStore) ListOllamaModels(ctx context.Context, ollamaURL string) ([]api.OllamaModel, error) {
	url := s.resolveOllamaURL(ollamaURL)
	client := provider.NewOllamaClient(provider.OllamaConfig{BaseURL: url})

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

func (s *ProviderAdminStore) PullOllamaModel(_ context.Context, modelName string, ollamaURL string) error {
	url := s.resolveOllamaURL(ollamaURL)
	client := provider.NewOllamaClient(provider.OllamaConfig{BaseURL: url})
	// Use a detached context — model pulls can take minutes and must not be
	// cancelled when the HTTP request completes.
	return client.PullModel(context.Background(), modelName, nil)
}

// resolveOllamaURL returns the Ollama server URL to use. If override is
// non-empty it is used directly. Otherwise, the registry config is inspected
// for any slot whose BaseURL contains ":11434" or whose Type contains "ollama".
// Falls back to http://localhost:11434.
func (s *ProviderAdminStore) resolveOllamaURL(override string) string {
	if override != "" {
		return strings.TrimSuffix(strings.TrimSuffix(override, "/"), "/v1")
	}

	if s.deps.Registry != nil {
		cfg := s.deps.Registry.GetConfig()
		for _, slot := range []provider.SlotConfig{cfg.Embedding, cfg.Fact, cfg.Entity} {
			if strings.Contains(slot.Type, "ollama") || strings.Contains(slot.BaseURL, ":11434") {
				if slot.BaseURL != "" {
					return strings.TrimSuffix(strings.TrimSuffix(slot.BaseURL, "/"), "/v1")
				}
			}
		}
	}

	return "http://localhost:11434"
}
