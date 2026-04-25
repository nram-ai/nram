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

// settingsSchemas is the canonical registry of known settings. It is static
// data, so it is allocated once at package init rather than rebuilt per
// request.
var settingsSchemas = []api.SettingSchema{
	{Key: "enrichment.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Enable automatic memory enrichment", Category: "enrichment"},
	{Key: "enrichment.batch_size", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Number of memories to process per enrichment batch", Category: "enrichment"},
	{Key: "enrichment.auto_enrich", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Automatically enrich new memories on store", Category: "enrichment"},
	{Key: "memory.default_confidence", Type: "number", DefaultValue: json.RawMessage(`0.9`), Description: "Default confidence score for new memories", Category: "memory"},
	{Key: "memory.default_importance", Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Default importance score for new memories", Category: "memory"},
	{Key: "memory.soft_delete_retention_days", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days after soft-delete before a memory row is hard-purged (with its vectors)", Category: "memory"},
	{Key: "api.rate_limit_rps", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "API rate limit (requests per second per user)", Category: "api"},
	{Key: "api.rate_limit_burst", Type: "number", DefaultValue: json.RawMessage(`20`), Description: "API rate limit burst size", Category: "api"},
	{Key: "qdrant.addr", Type: "string", DefaultValue: json.RawMessage(`""`), Description: "Qdrant gRPC address (host:port). Changes require server restart.", Category: "qdrant"},
	{Key: "qdrant.api_key", Type: "secret", DefaultValue: json.RawMessage(`""`), Description: "API key for Qdrant authentication. Changes require server restart.", Category: "qdrant"},
	{Key: "qdrant.use_tls", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable TLS for the Qdrant gRPC connection. Changes require server restart.", Category: "qdrant"},
	{Key: "qdrant.pool_size", Type: "number", DefaultValue: json.RawMessage(`3`), Description: "Number of gRPC connections in the pool (1 = no pool). Changes require server restart.", Category: "qdrant"},
	{Key: "qdrant.keepalive_time", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Seconds between keepalive pings (0 = 10s default, -1 = disabled). Changes require server restart.", Category: "qdrant"},
	{Key: "qdrant.keepalive_timeout", Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Seconds to wait for keepalive response before closing connection. Changes require server restart.", Category: "qdrant"},
	{Key: "dreaming.enabled", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable background dreaming (memory consolidation and graph improvement)", Category: "dreaming"},
	{Key: "dreaming.max_tokens_per_cycle", Type: "number", DefaultValue: json.RawMessage(`10000`), Description: "Maximum total tokens per dream cycle across all phases", Category: "dreaming"},
	{Key: "dreaming.max_tokens_per_call", Type: "number", DefaultValue: json.RawMessage(`2048`), Description: "Maximum tokens for any single LLM call during dreaming", Category: "dreaming"},
	{Key: "dreaming.cooldown_seconds", Type: "number", DefaultValue: json.RawMessage(`300`), Description: "Seconds to wait after the last user change before dreaming (prevents dreaming on partial data)", Category: "dreaming"},
	{Key: "dreaming.min_interval_seconds", Type: "number", DefaultValue: json.RawMessage(`3600`), Description: "Minimum seconds between dream cycles for the same project", Category: "dreaming"},
	{Key: "dreaming.initial_confidence", Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "Starting confidence for dream-synthesized memories (0.0-1.0)", Category: "dreaming"},
	{Key: "dreaming.supersession_threshold", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Confidence level at which a synthesis supersedes its source memories (0.0-1.0)", Category: "dreaming"},
	{Key: "dreaming.log_retention_days", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days to retain detailed dream logs before compressing to summaries", Category: "dreaming"},
	{Key: "dreaming.novelty.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run the novelty audit on dream syntheses; reject low-novelty results", Category: "dreaming_novelty"},
	{Key: "dreaming.novelty.embed_high_threshold", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Cosine similarity at or above which a synthesis is auto-rejected as duplicative of a source (0.0-1.0)", Category: "dreaming_novelty"},
	{Key: "dreaming.novelty.embed_low_threshold", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Cosine similarity below which a synthesis is auto-accepted without running the LLM judge (0.0-1.0)", Category: "dreaming_novelty"},
	{Key: "dreaming.novelty.judge_max_tokens", Type: "number", DefaultValue: json.RawMessage(`512`), Description: "Maximum tokens the novelty judge LLM call may produce", Category: "dreaming_novelty"},
	{Key: "dreaming.novelty.backfill_per_cycle", Type: "number", DefaultValue: json.RawMessage(`500`), Description: "Number of historical dream rows audited per cycle by the novelty backfill sweep", Category: "dreaming_novelty"},
	{Key: "dreaming.novelty.backfill_embed_high_threshold", Type: "number", DefaultValue: json.RawMessage(`0.93`), Description: "More aggressive auto-reject threshold used only by the backfill sweep on historical rows (0.0-1.0, 0 disables the override)", Category: "dreaming_novelty"},
	{Key: "dreaming.consolidation.audit_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.35`), Description: "Fraction of remaining cycle budget reserved for the novelty audit sub-phase (0.0-1.0)", Category: "dreaming_consolidation"},
	{Key: "dreaming.consolidation.reinforce_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.35`), Description: "Fraction of remaining cycle budget reserved for the reinforcement sub-phase (0.0-1.0)", Category: "dreaming_consolidation"},
	{Key: "dreaming.consolidation.consolidate_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.30`), Description: "Fraction of remaining cycle budget reserved for the consolidation sub-phase (0.0-1.0)", Category: "dreaming_consolidation"},
	{Key: "dreaming.contradiction.cap_per_cycle", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Maximum LLM pair-contradiction checks per dream cycle. Bump for faster drain during first-pass backfill on large namespaces, then restore.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.loser_haircut", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Multiplicative confidence haircut applied to the losing side of a contradiction (0.0-1.0). Smaller = harsher penalty. Diminishes on reaffirmation: effective = 1 - (1 - base) / detection_count.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.winner_haircut", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Multiplicative confidence haircut applied to the winning side of a contradiction (0.0-1.0). Acknowledges some uncertainty in any judgment. Same diminishing rule as the loser haircut.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.tie_haircut", Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Multiplicative confidence haircut applied to both sides when the LLM judge cannot pick a winner (0.0-1.0). Same diminishing rule.", Category: "dreaming_contradiction"},
	{Key: "reconsolidation.mode", Type: "enum", DefaultValue: json.RawMessage(`"shadow"`), Description: "Reconsolidation mode: 'shadow' emits events without persisting; 'persist' writes confidence/access updates; 'off' disables reinforcement entirely", Category: "reconsolidation", EnumValues: []string{"shadow", "persist", "off"}},
	{Key: "reconsolidation.factor", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "Per-recall confidence boost applied to reinforced memories (0.0-1.0)", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_enabled", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable sleep-time confidence decay for memories not recalled recently", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_threshold_days", Type: "number", DefaultValue: json.RawMessage(`14`), Description: "Days since last recall before a memory starts losing confidence to decay", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_rate_per_cycle", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "Confidence loss per dream cycle applied to decay-eligible memories (0.0-1.0)", Category: "reconsolidation"},
	{Key: "reconsolidation.confidence_floor", Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Minimum confidence decay will not push below (0.0-1.0)", Category: "reconsolidation"},
}

func (s *SettingsAdminStore) GetSettingsSchema(ctx context.Context) ([]api.SettingSchema, error) {
	return settingsSchemas, nil
}
