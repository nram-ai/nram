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

func (s *SettingsAdminStore) CountSettings(ctx context.Context, scope string) (int, error) {
	if scope != "" {
		return s.settingsRepo.CountByScope(ctx, scope)
	}
	return s.settingsRepo.CountAll(ctx)
}

func (s *SettingsAdminStore) ListSettings(ctx context.Context, scope string, limit, offset int) ([]model.Setting, error) {
	if scope != "" {
		return s.settingsRepo.ListByScopePaged(ctx, scope, limit, offset)
	}
	return s.settingsRepo.ListAllPaged(ctx, limit, offset)
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
		{Key: "qdrant.addr", Type: "string", DefaultValue: json.RawMessage(`""`), Description: "Qdrant gRPC address (host:port). Changes require server restart.", Category: "qdrant"},
		{Key: "qdrant.api_key", Type: "secret", DefaultValue: json.RawMessage(`""`), Description: "API key for Qdrant authentication. Changes require server restart.", Category: "qdrant"},
		{Key: "qdrant.use_tls", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable TLS for the Qdrant gRPC connection. Changes require server restart.", Category: "qdrant"},
		{Key: "qdrant.pool_size", Type: "number", DefaultValue: json.RawMessage(`3`), Description: "Number of gRPC connections in the pool (1 = no pool). Changes require server restart.", Category: "qdrant"},
		{Key: "qdrant.keepalive_time", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Seconds between keepalive pings (0 = 10s default, -1 = disabled). Changes require server restart.", Category: "qdrant"},
		{Key: "qdrant.keepalive_timeout", Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Seconds to wait for keepalive response before closing connection. Changes require server restart.", Category: "qdrant"},
	}
	return schemas, nil
}
