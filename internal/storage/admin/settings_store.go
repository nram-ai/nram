package admin

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// SettingsAdminStore implements api.SettingsAdminStore by wrapping SettingsRepo.
type SettingsAdminStore struct {
	settingsRepo *storage.SettingsRepo
}

// NewSettingsAdminStore creates a new SettingsAdminStore.
func NewSettingsAdminStore(settingsRepo *storage.SettingsRepo) *SettingsAdminStore {
	return &SettingsAdminStore{settingsRepo: settingsRepo}
}

func (s *SettingsAdminStore) ListSettings(ctx context.Context, scope string) ([]model.Setting, error) {
	if scope != "" {
		return s.settingsRepo.ListByScope(ctx, scope)
	}
	return s.settingsRepo.ListAll(ctx)
}

func (s *SettingsAdminStore) UpdateSetting(ctx context.Context, key string, value json.RawMessage, scope string, updatedBy *uuid.UUID) error {
	setting := &model.Setting{
		Key:       key,
		Value:     value,
		Scope:     scope,
		UpdatedBy: updatedBy,
	}
	return s.settingsRepo.Set(ctx, setting)
}

func (s *SettingsAdminStore) GetSettingsSchema(ctx context.Context) ([]api.SettingSchema, error) {
	// Return a predefined set of known settings with their schemas.
	schemas := []api.SettingSchema{
		{Key: "enrichment.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Enable automatic memory enrichment", Category: "enrichment"},
		{Key: "enrichment.batch_size", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Number of memories to process per enrichment batch", Category: "enrichment"},
		{Key: "enrichment.auto_enrich", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Automatically enrich new memories on store", Category: "enrichment"},
		{Key: "memory.default_confidence", Type: "number", DefaultValue: json.RawMessage(`0.9`), Description: "Default confidence score for new memories", Category: "memory"},
		{Key: "memory.default_importance", Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Default importance score for new memories", Category: "memory"},
		{Key: "memory.purge_after_days", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days after soft-delete before hard purge", Category: "memory"},
		{Key: "api.rate_limit_rps", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "API rate limit (requests per second per user)", Category: "api"},
		{Key: "api.rate_limit_burst", Type: "number", DefaultValue: json.RawMessage(`20`), Description: "API rate limit burst size", Category: "api"},
	}
	return schemas, nil
}
