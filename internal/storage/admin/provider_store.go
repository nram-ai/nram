package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ProviderAdminStore implements api.ProviderAdminStore by wrapping the provider
// Registry and SettingsRepo for persistent configuration.
type ProviderAdminStore struct {
	registry     *provider.Registry
	settingsRepo *storage.SettingsRepo
}

// NewProviderAdminStore creates a new ProviderAdminStore.
func NewProviderAdminStore(registry *provider.Registry, settingsRepo *storage.SettingsRepo) *ProviderAdminStore {
	return &ProviderAdminStore{registry: registry, settingsRepo: settingsRepo}
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

	return api.ProviderSlotStatus{
		Configured: true,
		Type:       persisted.Type,
		URL:        persisted.URL,
		Model:      persisted.Model,
		Dimensions: persisted.Dimensions,
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

func (s *ProviderAdminStore) UpdateProviderSlot(ctx context.Context, slot string, cfg api.ProviderSlotConfig) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal provider config: %w", err)
	}

	key := "provider." + slot
	setting := &model.Setting{
		Key:   key,
		Value: json.RawMessage(value),
		Scope: "global",
	}
	if err := s.settingsRepo.Set(ctx, setting); err != nil {
		return err
	}

	// Hot-reload: rebuild the registry from persisted settings.
	if s.registry != nil {
		newCfg := s.buildRegistryConfigFromDB(ctx)
		if err := s.registry.Reload(newCfg); err != nil {
			log.Printf("provider hot-reload failed: %v", err)
		}
	}

	return nil
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
