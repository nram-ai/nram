package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// Well-known setting keys.
const (
	SettingEmbedProvider    = "provider.embedding.type"
	SettingEmbedURL         = "provider.embedding.url"
	SettingEmbedKey         = "provider.embedding.key"
	SettingEmbedModel       = "provider.embedding.model"
	SettingFactProvider     = "provider.fact.type"
	SettingFactURL          = "provider.fact.url"
	SettingFactKey          = "provider.fact.key"
	SettingFactModel        = "provider.fact.model"
	SettingEntityProvider   = "provider.entity.type"
	SettingEntityURL        = "provider.entity.url"
	SettingEntityKey        = "provider.entity.key"
	SettingEntityModel      = "provider.entity.model"
	SettingDedupThreshold   = "enrichment.dedup_threshold"
	SettingFactPrompt       = "enrichment.fact_prompt"
	SettingEntityPrompt     = "enrichment.entity_prompt"
	SettingRankWeightSim    = "ranking.weight.similarity"
	SettingRankWeightRec    = "ranking.weight.recency"
	SettingRankWeightImp    = "ranking.weight.importance"
	SettingRankWeightFreq   = "ranking.weight.frequency"
	SettingRankWeightGraph  = "ranking.weight.graph_relevance"
	SettingTokenRetention   = "usage.token_retention_days"
)

// settingDefaults provides built-in default values for well-known settings.
// These are used when a setting is not found at any scope in the database.
var settingDefaults = map[string]string{
	SettingDedupThreshold:  "0.92",
	SettingRankWeightSim:   "0.5",
	SettingRankWeightRec:   "0.15",
	SettingRankWeightImp:   "0.10",
	SettingRankWeightFreq:  "0.05",
	SettingRankWeightGraph: "0.20",
	SettingTokenRetention:  "365",
}

// SettingsRepository defines the persistence operations needed by the settings service.
type SettingsRepository interface {
	Get(ctx context.Context, key string, scope string) (*model.Setting, error)
	Set(ctx context.Context, setting *model.Setting) error
	Delete(ctx context.Context, key string, scope string) error
	ListByScope(ctx context.Context, scope string) ([]model.Setting, error)
}

// SettingsService provides cascading settings resolution with built-in defaults,
// type-safe accessors, and convenience methods for common settings.
type SettingsService struct {
	repo SettingsRepository
}

// NewSettingsService creates a new SettingsService with the given repository.
func NewSettingsService(repo SettingsRepository) *SettingsService {
	return &SettingsService{repo: repo}
}

// Resolve retrieves a setting value as a string through the cascade hierarchy.
// It first checks the database (which cascades project->user->org->global),
// then falls back to built-in defaults. If no value is found anywhere,
// it returns an empty string with no error.
func (s *SettingsService) Resolve(ctx context.Context, key string, scope string) (string, error) {
	setting, err := s.repo.Get(ctx, key, scope)
	if err == nil {
		return unmarshalJSONString(setting.Value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("resolve setting %q: %w", key, err)
	}

	// Not found in DB; check built-in defaults.
	if def, ok := settingDefaults[key]; ok {
		return def, nil
	}

	return "", nil
}

// ResolveFloat resolves a setting and parses it as a float64.
func (s *SettingsService) ResolveFloat(ctx context.Context, key string, scope string) (float64, error) {
	val, err := s.Resolve(ctx, key, scope)
	if err != nil {
		return 0, err
	}
	if val == "" {
		return 0, fmt.Errorf("setting %q has no value", key)
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("setting %q is not a valid float64: %w", key, err)
	}
	return f, nil
}

// ResolveInt resolves a setting and parses it as an int.
func (s *SettingsService) ResolveInt(ctx context.Context, key string, scope string) (int, error) {
	val, err := s.Resolve(ctx, key, scope)
	if err != nil {
		return 0, err
	}
	if val == "" {
		return 0, fmt.Errorf("setting %q has no value", key)
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("setting %q is not a valid int: %w", key, err)
	}
	return i, nil
}

// Set writes a setting at the given scope.
func (s *SettingsService) Set(ctx context.Context, key string, value string, scope string, updatedBy *uuid.UUID) error {
	jsonVal, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal setting value: %w", err)
	}

	setting := &model.Setting{
		Key:       key,
		Value:     json.RawMessage(jsonVal),
		Scope:     scope,
		UpdatedBy: updatedBy,
	}

	return s.repo.Set(ctx, setting)
}

// Delete removes a setting at the given scope.
func (s *SettingsService) Delete(ctx context.Context, key string, scope string) error {
	return s.repo.Delete(ctx, key, scope)
}

// ListByScope returns all settings for a given scope.
func (s *SettingsService) ListByScope(ctx context.Context, scope string) ([]model.Setting, error) {
	return s.repo.ListByScope(ctx, scope)
}

// unmarshalJSONString attempts to unmarshal a JSON value as a string.
// If the value is a JSON string (e.g., `"hello"`), it returns the unquoted string.
// Otherwise, it returns the raw JSON text as-is (e.g., for numbers or objects).
func unmarshalJSONString(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}
