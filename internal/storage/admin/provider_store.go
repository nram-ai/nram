package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ProviderAdminStore implements api.ProviderAdminStore by wrapping the provider
// Registry and SettingsRepo for persistent configuration. The cascade deps
// (memoryRepo, entityRepo, vectorStore, db) are required for the
// embedding-model switch path; passing nil disables that path with a
// runtime error.
type ProviderAdminStore struct {
	registry     *provider.Registry
	settingsRepo *storage.SettingsRepo
	memoryRepo   *storage.MemoryRepo
	entityRepo   *storage.EntityRepo
	vectorStore  storage.VectorStore
	db           storage.DB
}

// NewProviderAdminStore creates a new ProviderAdminStore. memoryRepo,
// entityRepo, vectorStore, and db may be nil in test contexts that do
// not exercise the embedding-model switch cascade.
func NewProviderAdminStore(
	registry *provider.Registry,
	settingsRepo *storage.SettingsRepo,
	memoryRepo *storage.MemoryRepo,
	entityRepo *storage.EntityRepo,
	vectorStore storage.VectorStore,
	db storage.DB,
) *ProviderAdminStore {
	return &ProviderAdminStore{
		registry:     registry,
		settingsRepo: settingsRepo,
		memoryRepo:   memoryRepo,
		entityRepo:   entityRepo,
		vectorStore:  vectorStore,
		db:           db,
	}
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
	if s.settingsRepo != nil {
		setting, err := s.settingsRepo.Get(ctx, "provider."+slot, "global")
		if err == nil {
			var cfg api.ProviderSlotConfig
			if json.Unmarshal(setting.Value, &cfg) == nil && cfg.Type != "" {
				persisted = &cfg
			}
		}
	}

	// Check if the live provider is loaded in the registry.
	alive := false
	if s.registry != nil {
		switch slot {
		case "embedding":
			alive = s.registry.GetEmbedding() != nil
		case "fact":
			alive = s.registry.GetFact() != nil
		case "entity":
			alive = s.registry.GetEntity() != nil
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

	// For the embedding slot, surface the probed dim from the live registry
	// rather than echoing whatever the user might have typed in a previous
	// version. Probe failures (provider down, model mid-load) leave the
	// field nil rather than misleading the UI; the status field carries the
	// alive/error signal separately.
	var dimensions *int
	if slot == "embedding" && alive && s.registry != nil {
		if d, err := s.registry.EmbeddingDim(ctx); err == nil && d > 0 {
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

	tmpReg, err := provider.NewRegistry(cfg)
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
	// Detect an embedding-model swap before persisting. If the model
	// changed, route through the destructive cascade (gated on
	// confirm_invalidate). Non-embedding slot updates and embedding
	// updates that don't touch the model field bypass the cascade
	// entirely.
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

// currentEmbeddingModel reads the persisted provider.embedding setting
// and returns its model field. Empty string means no embedding slot is
// configured yet (treat any incoming model as a fresh setup, not a swap).
func (s *ProviderAdminStore) currentEmbeddingModel(ctx context.Context) string {
	if s.settingsRepo == nil {
		return ""
	}
	setting, err := s.settingsRepo.Get(ctx, "provider.embedding", "global")
	if err != nil || setting == nil {
		return ""
	}
	var current api.ProviderSlotConfig
	if err := json.Unmarshal(setting.Value, &current); err != nil {
		return ""
	}
	return current.Model
}

// persistAndReload writes the slot config to settings and triggers a
// registry hot-reload. Used for both non-cascade updates and the
// post-truncate persist step in switchEmbeddingModel.
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
	if err := s.settingsRepo.Set(ctx, setting); err != nil {
		return err
	}

	if s.registry != nil {
		newCfg := s.buildRegistryConfigFromDB(ctx)
		if err := s.registry.Reload(newCfg); err != nil {
			log.Printf("provider hot-reload failed: %v", err)
		}
	}
	return nil
}

// switchEmbeddingModel runs the destructive cascade triggered by an
// embedding-model change. Sequence:
//   1. Gate: require ConfirmInvalidate=true. Without it, return
//      NeedsConfirmation=true so the UI shows the modal.
//   2. Truncate every memory_vectors_* and entity_vectors_* table/
//      collection — vectors generated by the old model are invalid in
//      the new model's vector space (no cross-model retrieval).
//   3. NULL out memories.embedding_dim and entities.embedding_dim so
//      the re-embed pipeline treats every row as needing fresh vectors.
//   4. Persist the new config and reload the registry. The next
//      EmbeddingDim probe runs against the new embedder.
//   5. Enqueue all live memories via BackfillReembedAllJobs (workers
//      drain in the background).
//   6. Kick off ReembedAllEntities in a goroutine (entities are not
//      driven by the enrichment_queue; they need their own loop).
//
// The HTTP request returns immediately after step 5; entity re-embed
// continues in the background. The result carries row counts so the UI
// can display "N memories invalidated, M entities invalidated" and a
// "re-embed in progress" status.
func (s *ProviderAdminStore) switchEmbeddingModel(
	ctx context.Context,
	oldModel string,
	cfg api.ProviderSlotConfig,
	opts api.UpdateProviderSlotOpts,
) (*api.UpdateProviderSlotResult, error) {
	if s.memoryRepo == nil || s.entityRepo == nil || s.vectorStore == nil || s.db == nil {
		return nil, fmt.Errorf("embedding-model switch cascade requires memoryRepo, entityRepo, vectorStore, and db; one or more is nil")
	}

	// Pre-count rows that would be invalidated. Used both for the
	// confirmation modal payload and for the post-cascade response.
	memCount, _ := s.countEmbeddingDimNotNull(ctx, "memories", true)
	entCount, _ := s.countEmbeddingDimNotNull(ctx, "entities", false)

	if !opts.ConfirmInvalidate {
		return &api.UpdateProviderSlotResult{
			NeedsConfirmation: true,
			OldModel:          oldModel,
			NewModel:          cfg.Model,
			MemoriesAffected:  memCount,
			EntitiesAffected:  entCount,
		}, nil
	}

	if err := s.vectorStore.TruncateAllVectors(ctx); err != nil {
		return nil, fmt.Errorf("cascade: truncate vectors: %w", err)
	}

	memNulled, err := s.memoryRepo.ClearAllEmbeddingDims(ctx)
	if err != nil {
		return nil, fmt.Errorf("cascade: NULL memory embedding_dim: %w", err)
	}
	entNulled, err := s.entityRepo.ClearAllEmbeddingDims(ctx)
	if err != nil {
		return nil, fmt.Errorf("cascade: NULL entity embedding_dim: %w", err)
	}

	if err := s.persistAndReload(ctx, "embedding", cfg); err != nil {
		return nil, fmt.Errorf("cascade: persist + reload: %w", err)
	}

	// Force-enqueue every live memory. Workers drain in the background.
	jobs, err := storage.BackfillReembedAllJobs(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("cascade: enqueue memory re-embed jobs: %w", err)
	}

	// Entity re-embed runs in a detached goroutine so the HTTP request
	// returns promptly. A request-scoped context would be cancelled when
	// the response is written; use Background() so the loop survives.
	if s.registry != nil {
		if embedder := s.registry.GetEmbedding(); embedder != nil {
			go func(emb provider.EmbeddingProvider) {
				if _, rerr := enrichment.ReembedAllEntities(context.Background(), s.entityRepo, s.vectorStore, emb); rerr != nil {
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

// countEmbeddingDimNotNull returns the count of rows in `table` that
// currently have a non-NULL embedding_dim. liveOnly excludes soft-deleted
// rows when the table has a deleted_at column.
func (s *ProviderAdminStore) countEmbeddingDimNotNull(ctx context.Context, table string, liveOnly bool) (int64, error) {
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE embedding_dim IS NOT NULL", table)
	if liveOnly {
		q += " AND deleted_at IS NULL"
	}
	var count int64
	if err := s.db.QueryRow(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("count %s embedding_dim: %w", table, err)
	}
	return count, nil
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
		setting, err := s.settingsRepo.Get(ctx, slot.key, "global")
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

	if s.registry != nil {
		cfg := s.registry.GetConfig()
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
