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

	// Dreaming system-level settings (global scope).
	SettingDreamingEnabled            = "dreaming.enabled"
	SettingDreamMaxTokensPerCycle     = "dreaming.max_tokens_per_cycle"
	SettingDreamMaxTokensPerCall      = "dreaming.max_tokens_per_call"
	SettingDreamCooldown              = "dreaming.cooldown_seconds"
	SettingDreamMinInterval           = "dreaming.min_interval_seconds"
	SettingDreamInitialConfidence     = "dreaming.initial_confidence"
	SettingDreamSupersessionThreshold = "dreaming.supersession_threshold"
	SettingDreamLogRetention          = "dreaming.log_retention_days"
	SettingDreamProjectEnabled        = "dreaming.project.enabled"
	SettingDreamContradictionPrompt   = "dreaming.contradiction_prompt"
	SettingDreamSynthesisPrompt       = "dreaming.synthesis_prompt"
	SettingDreamAlignmentPrompt       = "dreaming.alignment_prompt"

	// Novelty audit. A dream synthesis must contain at least one fact not
	// present in any of its source memories. Hybrid check: max-cosine
	// embedding similarity vs sources gates whether the LLM judge runs.
	// Backfill applies the same rule to historical dream rows incrementally.
	SettingDreamNoveltyEnabled            = "dreaming.novelty.enabled"
	SettingDreamNoveltyEmbedHighThreshold = "dreaming.novelty.embed_high_threshold"
	SettingDreamNoveltyEmbedLowThreshold  = "dreaming.novelty.embed_low_threshold"
	SettingDreamNoveltyJudgePrompt        = "dreaming.novelty.judge_prompt"
	SettingDreamNoveltyJudgeMaxTokens     = "dreaming.novelty.judge_max_tokens"
	SettingDreamNoveltyBackfillPerCycle   = "dreaming.novelty.backfill_per_cycle"

	SettingQdrantAddr             = "qdrant.addr"
	SettingQdrantAPIKey           = "qdrant.api_key"
	SettingQdrantUseTLS           = "qdrant.use_tls"
	SettingQdrantPoolSize         = "qdrant.pool_size"
	SettingQdrantKeepAliveTime    = "qdrant.keepalive_time"
	SettingQdrantKeepAliveTimeout = "qdrant.keepalive_timeout"

	// Reconsolidation settings. Reinforcement on recall is the first biological
	// intervention on the recall path: surfaced memories get their access_count,
	// last_accessed, and confidence nudged asynchronously. Decay is the
	// complementary sleep-side process: unused memories lose confidence over
	// time so the signal stays meaningful.
	SettingReconsolidationMode          = "reconsolidation.mode"
	SettingReconsolidationFactor        = "reconsolidation.factor"
	SettingConfidenceDecayEnabled       = "reconsolidation.decay_enabled"
	SettingConfidenceDecayThresholdDays = "reconsolidation.decay_threshold_days"
	SettingConfidenceDecayRatePerCycle  = "reconsolidation.decay_rate_per_cycle"
	SettingConfidenceFloor              = "reconsolidation.confidence_floor"
)

// Reconsolidation mode values. Default is shadow so the first real deployment
// is observable-only: events are emitted, but no database values change until
// the operator flips the mode to persist.
const (
	ReconsolidationModeOff     = "off"
	ReconsolidationModeShadow  = "shadow"
	ReconsolidationModePersist = "persist"
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
	SettingDreamingEnabled:            "false",
	SettingDreamMaxTokensPerCycle:     "10000",
	SettingDreamMaxTokensPerCall:      "2048",
	SettingDreamCooldown:              "300",
	SettingDreamMinInterval:           "3600",
	SettingDreamInitialConfidence:     "0.3",
	SettingDreamSupersessionThreshold: "0.85",
	SettingDreamLogRetention:          "30",
	SettingDreamContradictionPrompt: `You are a contradiction detector. You do NOT converse. You output JSON only.

Determine if the two statements below contradict each other.

<statement_a>
%s
</statement_a>

<statement_b>
%s
</statement_b>

Output ONLY this JSON, nothing else:
{"contradicts": true, "explanation": "reason"}
or
{"contradicts": false, "explanation": "reason"}`,
	SettingDreamSynthesisPrompt: `You are a knowledge synthesizer. You do NOT converse, greet, or ask questions. You output ONLY the synthesized text.

Combine the following pieces of information into a single concise paragraph that preserves all key facts. Do not lose details. Do not add commentary. Do not prefix with "Here is" or similar.

<information>
%s
</information>

Output ONLY the synthesized text:`,
	SettingDreamAlignmentPrompt: `You are an alignment scorer. You do NOT converse. You output JSON only.

Score how strongly the evidence supports or contradicts the synthesis.

<synthesis>
%s
</synthesis>

<evidence>
%s
</evidence>

Output ONLY this JSON, nothing else:
{"alignment": 0.0, "reasoning": "brief reason"}

alignment must be a float:
1.0 = strong support
0.0 = neutral/unrelated
-1.0 = strong contradiction`,
	SettingDreamNoveltyEnabled:            "true",
	SettingDreamNoveltyEmbedHighThreshold: "0.97",
	SettingDreamNoveltyEmbedLowThreshold:  "0.85",
	SettingDreamNoveltyJudgeMaxTokens:     "512",
	SettingDreamNoveltyBackfillPerCycle:   "50",
	SettingDreamNoveltyJudgePrompt: `You are a novelty auditor. You do NOT converse. You output JSON only.

Given a synthesized memory and the source memories it was derived from, list any facts present in the synthesis that are NOT stated or directly implied by any of the sources. A fact is "novel" only if a careful reader could not derive it from the sources alone.

<synthesis>
%s
</synthesis>

<sources>
%s
</sources>

Output ONLY this JSON, nothing else:
{"novel_facts": ["fact 1", "fact 2"]}

Empty array if every fact in the synthesis is already present in the sources.`,
	SettingQdrantUseTLS:           "false",
	SettingQdrantPoolSize:         "3",
	SettingQdrantKeepAliveTime:    "10",
	SettingQdrantKeepAliveTimeout: "2",

	SettingReconsolidationMode:          ReconsolidationModeShadow,
	SettingReconsolidationFactor:        "0.02",
	SettingConfidenceDecayEnabled:       "false",
	SettingConfidenceDecayThresholdDays: "14",
	SettingConfidenceDecayRatePerCycle:  "0.02",
	SettingConfidenceFloor:              "0.05",
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

// ResolveBool resolves a setting and interprets it as a boolean. "true" and
// "1" are treated as true; every other value (including empty and errors) is
// false. Matches the precedent set by the dream scheduler's enable check.
func (s *SettingsService) ResolveBool(ctx context.Context, key string, scope string) bool {
	val, err := s.Resolve(ctx, key, scope)
	if err != nil {
		return false
	}
	return val == "true" || val == "1"
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
