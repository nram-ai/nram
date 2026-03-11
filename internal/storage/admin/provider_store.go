package admin

import (
	"context"
	"encoding/json"
	"fmt"
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
		Embedding: s.slotStatus("embedding"),
		Fact:      s.slotStatus("fact"),
		Entity:    s.slotStatus("entity"),
	}
	return resp, nil
}

func (s *ProviderAdminStore) slotStatus(slot string) api.ProviderSlotStatus {
	if s.registry == nil {
		return api.ProviderSlotStatus{
			Configured: false,
			Status:     "not_configured",
		}
	}

	switch slot {
	case "embedding":
		p := s.registry.GetEmbedding()
		if p == nil {
			return api.ProviderSlotStatus{Configured: false, Status: "not_configured"}
		}
		return api.ProviderSlotStatus{Configured: true, Status: "ok"}
	case "fact":
		p := s.registry.GetFact()
		if p == nil {
			return api.ProviderSlotStatus{Configured: false, Status: "not_configured"}
		}
		return api.ProviderSlotStatus{Configured: true, Status: "ok"}
	case "entity":
		p := s.registry.GetEntity()
		if p == nil {
			return api.ProviderSlotStatus{Configured: false, Status: "not_configured"}
		}
		return api.ProviderSlotStatus{Configured: true, Status: "ok"}
	default:
		return api.ProviderSlotStatus{Configured: false, Status: "unknown_slot"}
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
	return s.settingsRepo.Set(ctx, setting)
}

func (s *ProviderAdminStore) ListOllamaModels(ctx context.Context) ([]api.OllamaModel, error) {
	return []api.OllamaModel{}, nil
}

func (s *ProviderAdminStore) PullOllamaModel(ctx context.Context, modelName string) error {
	return fmt.Errorf("ollama pull not yet implemented")
}
