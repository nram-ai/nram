package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// SettingsCacheInvalidator optionally consumes (key, scope) changes so a
// settings cache (SettingsService) can drop entries immediately after the
// admin REST API writes. Without it, Resolve* readers see stale values for
// up to settings.cache_ttl_seconds (default 30s) after every UpdateSetting,
// ResetSetting, or ResetAllSettings.
type SettingsCacheInvalidator interface {
	InvalidateCache(key, scope string)
	InvalidateAllCache()
}

// SettingsAdminStore implements api.SettingsAdminStore by wrapping SettingsRepo.
type SettingsAdminStore struct {
	settingsRepo *storage.SettingsRepo
	invalidator  SettingsCacheInvalidator
}

// NewSettingsAdminStore creates a new SettingsAdminStore. invalidator may be
// nil in tests; production wires SettingsService so admin writes propagate
// past the per-key TTL cache immediately.
func NewSettingsAdminStore(settingsRepo *storage.SettingsRepo, invalidator SettingsCacheInvalidator) *SettingsAdminStore {
	return &SettingsAdminStore{settingsRepo: settingsRepo, invalidator: invalidator}
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

// GetSetting returns the setting row stored at (key, scope), or nil if
// absent. Wraps SettingsRepo.Get so the admin API can do indexed lookups
// instead of scanning ListSettings, used by cross-key invariant validation
// where the validator needs the paired setting's value.
func (s *SettingsAdminStore) GetSetting(ctx context.Context, key, scope string) (*model.Setting, error) {
	return s.settingsRepo.Get(ctx, key, scope)
}

// GetCostRates returns the global usage.cost_rates JSON blob raw, so
// the GET handler can hand it to the SPA without re-encoding. Returns
// sql.ErrNoRows pre-seeder; the handler maps that to an empty list.
func (s *SettingsAdminStore) GetCostRates(ctx context.Context) (json.RawMessage, error) {
	setting, err := s.settingsRepo.Get(ctx, service.SettingTokenCostRates, "global")
	if err != nil {
		return nil, err
	}
	return setting.Value, nil
}

func (s *SettingsAdminStore) UpdateSetting(ctx context.Context, key string, value json.RawMessage, scope string, updatedBy *uuid.UUID) error {
	setting := &model.Setting{
		Key:       key,
		Value:     value,
		Scope:     scope,
		UpdatedBy: updatedBy,
	}
	if err := s.settingsRepo.Set(ctx, setting); err != nil {
		return err
	}
	if s.invalidator != nil {
		s.invalidator.InvalidateCache(key, scope)
	}
	return nil
}

// defaultValueForKey resolves the canonical JSON-encoded default for a setting
// key. Non-prompt entries take their default from the schema registry, which
// is the same value the UI advertises as "default". Prompt entries are large
// multi-line strings stored only in service.settingDefaults; they share their
// schema DefaultValue with the runtime map at package init (see settings_store
// init), so a runtime lookup gives the same string content with proper JSON
// encoding.
func defaultValueForKey(key string) (json.RawMessage, bool) {
	for i := range settingsSchemas {
		if settingsSchemas[i].Key == key {
			return settingsSchemas[i].DefaultValue, true
		}
	}
	return nil, false
}

// ResetSetting reverts one setting at (key, scope) to its registered default.
// At scope "global" the row is upserted with the canonical default value, so
// the registry stays seeded and updated_by reflects the admin who reset it.
// At any other scope the override is deleted so the cascade resolver falls
// back to the global default; deleting a nonexistent row is a no-op.
func (s *SettingsAdminStore) ResetSetting(ctx context.Context, key, scope string, updatedBy *uuid.UUID) error {
	if scope != "global" {
		if err := s.settingsRepo.Delete(ctx, key, scope); err != nil {
			return err
		}
		if s.invalidator != nil {
			s.invalidator.InvalidateCache(key, scope)
		}
		return nil
	}
	def, ok := defaultValueForKey(key)
	if !ok {
		return fmt.Errorf("settings reset: key %q is not registered", key)
	}
	setting := &model.Setting{
		Key:       key,
		Value:     def,
		Scope:     "global",
		UpdatedBy: updatedBy,
	}
	if err := s.settingsRepo.Set(ctx, setting); err != nil {
		return err
	}
	if s.invalidator != nil {
		s.invalidator.InvalidateCache(key, "global")
	}
	return nil
}

// ResetAllSettings reverts every registered schema key at the given scope,
// honoring the per-schema OmitFromResetAll flag so credentials and connection
// strings (qdrant.addr, qdrant.api_key, ingestion model) survive a bulk reset.
// At scope "global" performs an atomic upsert across the eligible registry. At
// any other scope deletes only those eligible overrides at the scope so each
// key falls back to its global value. Returns the count of keys reset.
func (s *SettingsAdminStore) ResetAllSettings(ctx context.Context, scope string, updatedBy *uuid.UUID) (int, error) {
	if scope != "global" {
		// Filter by registered keys so legacy/orphan overrides (keys removed
		// from the schema) are preserved, and skip OmitFromResetAll entries
		// so credentials at the scope are not wiped by a bulk reset.
		count := 0
		for i := range settingsSchemas {
			if settingsSchemas[i].OmitFromResetAll {
				continue
			}
			if err := s.settingsRepo.Delete(ctx, settingsSchemas[i].Key, scope); err != nil {
				return count, err
			}
			count++
		}
		if s.invalidator != nil {
			s.invalidator.InvalidateAllCache()
		}
		return count, nil
	}
	batch := make([]model.Setting, 0, len(settingsSchemas))
	for i := range settingsSchemas {
		if settingsSchemas[i].OmitFromResetAll {
			continue
		}
		batch = append(batch, model.Setting{
			Key:       settingsSchemas[i].Key,
			Value:     settingsSchemas[i].DefaultValue,
			Scope:     "global",
			UpdatedBy: updatedBy,
		})
	}
	if err := s.settingsRepo.SetMany(ctx, batch); err != nil {
		return 0, err
	}
	if s.invalidator != nil {
		s.invalidator.InvalidateAllCache()
	}
	return len(batch), nil
}

// settingsSchemas is the canonical registry of known settings. It is static
// data, so it is allocated once at package init rather than rebuilt per
// request.
var settingsSchemas = []api.SettingSchema{
	{Key: "enrichment.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Turn automatic enrichment on or off. When on, every newly stored memory is queued for fact and entity extraction (once providers are configured). When off, the queue holds jobs until you turn it back on.", Category: "enrichment"},
	{Key: "enrichment.batch_size", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "How many memories the enrichment worker processes per batch.", Category: "enrichment", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: service.SettingMemoryDefaultConfidence, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Confidence given to a new memory when the caller does not set one (0.0 to 1.0). Applies to every write path: store, batch store, extracted facts, and import.", Category: "memory", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingMemoryDefaultImportance, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Importance given to a new memory when the caller does not set one (0.0 to 1.0). Applies to every write path: store, batch store, extracted facts, and import.", Category: "memory", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "memory.soft_delete_retention_days", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days a soft-deleted memory is kept before it is permanently purged along with its vectors.", Category: "memory", Min: ptrF(1), Max: ptrF(3650), Step: ptrF(1)},
	{Key: "api.rate_limit_rps", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Sustained API rate limit, in requests per second per user.", Category: "api", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: "api.rate_limit_burst", Type: "number", DefaultValue: json.RawMessage(`20`), Description: "How many requests a user may burst above the per-second limit before being throttled.", Category: "api", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: "qdrant.addr", Type: "string", DefaultValue: json.RawMessage(`""`), Description: "Address of the Qdrant server, as host:port.", Category: "qdrant", RequiresRestart: true, OmitFromResetAll: true},
	{Key: "qdrant.api_key", Type: "secret", DefaultValue: json.RawMessage(`""`), Description: "API key for authenticating to Qdrant.", Category: "qdrant", RequiresRestart: true, OmitFromResetAll: true},
	{Key: "qdrant.use_tls", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Use TLS for the connection to Qdrant.", Category: "qdrant", RequiresRestart: true},
	{Key: "qdrant.pool_size", Type: "number", DefaultValue: json.RawMessage(`3`), Description: "Number of connections kept open to Qdrant (1 = no pooling).", Category: "qdrant", RequiresRestart: true, Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: "qdrant.keepalive_time", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Seconds between keepalive pings to Qdrant (0 = 10s default, -1 = disabled).", Category: "qdrant", RequiresRestart: true, Min: ptrF(-1), Max: ptrF(3600), Step: ptrF(1)},
	{Key: "qdrant.keepalive_timeout", Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Seconds to wait for a keepalive reply before dropping the connection to Qdrant.", Category: "qdrant", RequiresRestart: true, Min: ptrF(1), Max: ptrF(60), Step: ptrF(1)},
	// HNSW (pure-Go SQLite-backed vector index) tuning. AppliesToBackend
	// scopes the UI to SQLite-only deployments; on Postgres+pgvector or
	// Qdrant, these knobs have no effect.
	{Key: service.SettingHNSWM, Type: "number", DefaultValue: json.RawMessage(`16`), Description: "Maximum neighbours per node in the upper layers of the HNSW vector index. Higher values improve search accuracy but make indexes larger and slower to build. Baked in when an index is built, so changes affect only newly built indexes.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(4), Max: ptrF(128), Step: ptrF(2)},
	{Key: service.SettingHNSWEfConstruction, Type: "number", DefaultValue: json.RawMessage(`200`), Description: "Size of the candidate pool used while building an HNSW index. Higher values improve search accuracy at the cost of slower index builds. Applies only to newly built indexes.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(10), Max: ptrF(2000), Step: ptrF(10)},
	{Key: service.SettingHNSWEfSearch, Type: "number", DefaultValue: json.RawMessage(`50`), Description: "Size of the candidate pool used while searching the HNSW index. Higher values improve accuracy at the cost of slower searches. Applied at next boot.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(10), Max: ptrF(2000), Step: ptrF(10)},
	{Key: service.SettingHNSWMaxLoadedIndexes, Type: "number", DefaultValue: json.RawMessage(`64`), Description: "How many HNSW indexes are held in memory before the least-recently-used one is evicted. Each loaded index keeps its full graph in RAM.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: "dreaming.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Turn background dreaming on or off. Dreaming consolidates related memories and improves the knowledge graph during idle time.", Category: "dreaming"},
	{Key: "dreaming.max_tokens_per_cycle", Type: "number", DefaultValue: json.RawMessage(`1024000`), Description: "Total token budget for one dream cycle, shared across all phases. Raising it lets each cycle do more work, at higher LLM cost.", Category: "dreaming", Min: ptrF(1000), Max: ptrF(100000000), Step: ptrF(1000)},
	{Key: "dreaming.max_tokens_per_call", Type: "number", DefaultValue: json.RawMessage(`2048`), Description: "Maximum tokens any single LLM call may produce during dreaming.", Category: "dreaming", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: "dreaming.cooldown_seconds", Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How long to wait after the last change to a project before dreaming over it, so a cycle doesn't run on half-finished work.", Category: "dreaming", Min: ptrF(0), Max: ptrF(86400), Step: ptrF(60)},
	{Key: "dreaming.min_interval_seconds", Type: "number", DefaultValue: json.RawMessage(`600`), Description: "Minimum time between dream cycles for the same project. Lower it to drain a backlog faster, at higher LLM cost.", Category: "dreaming", Min: ptrF(60), Max: ptrF(2592000), Step: ptrF(300)},
	{Key: "dreaming.initial_confidence", Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "Confidence assigned to a memory the moment dreaming synthesizes it (0.0 to 1.0). It earns more confidence as later cycles reaffirm it.", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.supersession_threshold", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Confidence a synthesis must reach before it replaces the source memories it was built from (0.0 to 1.0).", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.log_retention_days", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days of detailed per-cycle dream logs to keep before they are compressed into summaries.", Category: "dreaming", Min: ptrF(1), Max: ptrF(3650), Step: ptrF(1)},
	{Key: service.SettingDreamLLMConcurrency, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "How many of a dream phase's per-item LLM and embedding calls run in parallel. 1 (the default) keeps every phase sequential, safe for a single local GPU. A dream cycle runs alone, so this is the cycle's entire provider concurrency; raise it only with a multi-GPU or hosted provider.", Category: "dreaming", Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: "dreaming.novelty.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Check each new synthesis against the memories it was built from and reject ones that add nothing new.", Category: "dreaming_novelty"},
	{Key: "dreaming.novelty.embed_high_threshold", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "How similar a synthesis must be to a source memory to be rejected outright as a duplicate, with no LLM check (0.0 to 1.0).", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "dreaming.novelty.embed_low_threshold", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Below this similarity to its sources, a synthesis is accepted outright as novel, with no LLM check (0.0 to 1.0). Cases between the two thresholds go to the LLM judge.", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "dreaming.novelty.judge_max_tokens", Type: "number", DefaultValue: json.RawMessage(`512`), Description: "Maximum tokens the novelty judge's LLM call may produce.", Category: "dreaming_novelty", Min: ptrF(64), Max: ptrF(8192), Step: ptrF(64)},
	{Key: "dreaming.novelty.backfill_per_cycle", Type: "number", DefaultValue: json.RawMessage(`500`), Description: "How many previously-created syntheses the audit re-checks per cycle when catching up on history. Raise to clear a backlog faster.", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: "dreaming.novelty.backfill_embed_high_threshold", Type: "number", DefaultValue: json.RawMessage(`0.93`), Description: "A stricter auto-reject similarity used only when re-checking older syntheses (0.0 to 1.0; 0 turns off the override and uses the normal threshold).", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "dreaming.consolidation.audit_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.35`), Description: "Share of the consolidation phase's token budget given to its novelty-audit step (0.0 to 1.0). The three consolidation shares are relative weights, and budget left unspent by earlier steps flows to later ones.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.consolidation.reinforce_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.35`), Description: "Share of the consolidation phase's token budget given to its reinforcement step (0.0 to 1.0). Relative weight; unspent budget from earlier steps flows here.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.consolidation.consolidate_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.30`), Description: "Share of the consolidation phase's token budget given to the merge step that writes the synthesis (0.0 to 1.0). Relative weight; unspent budget from earlier steps flows here.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamEntityDedupFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Share of the cycle's token budget reserved for the entity-dedup phase (0.0 to 1.0). These are relative weights across phases, and budget left unspent by earlier phases flows to later ones. This phase uses no LLM, so 0 (the default) reserves nothing: it runs whenever the cycle has budget left.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamEmbeddingBackfillFraction, Type: "number", DefaultValue: json.RawMessage(`0.10`), Description: "Share of the cycle's token budget reserved for re-embedding memories whose vector is missing (0.0 to 1.0). Relative weight across phases; unspent budget flows to later phases.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamAugmentationBackfillFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Share of the cycle's token budget reserved for the augmentation-backfill phase (0.0 to 1.0). This phase only queues work and makes no LLM calls itself, so 0 (the default) reserves nothing.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamMultiVectorBackfillFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Share of the cycle's token budget reserved for the multi-vector facet backfill phase (0.0 to 1.0). This phase only queues work and makes no LLM calls itself, so 0 (the default) reserves nothing.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamConsolidationEntityBackfillFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Share of the cycle's token budget reserved for the consolidation-entity backfill phase (0.0 to 1.0). This phase only queues work and makes no LLM calls itself, so 0 (the default) reserves nothing.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamParaphraseFraction, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Share of the cycle's token budget reserved for the paraphrase-dedup sweep (0.0 to 1.0). Relative weight across phases; unspent budget flows to later phases.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamTransitiveFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Share of the cycle's token budget reserved for inferring transitive relationships (0.0 to 1.0). This phase uses no LLM, so 0 (the default) reserves nothing: it runs whenever the cycle has budget left.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamContradictionFraction, Type: "number", DefaultValue: json.RawMessage(`0.40`), Description: "Share of the cycle's token budget reserved for contradiction detection (0.0 to 1.0). Caps how much the LLM-judged comparison can spend so it doesn't starve consolidation. Relative weight across phases; unspent budget flows to later phases.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamConsolidationFraction, Type: "number", DefaultValue: json.RawMessage(`0.40`), Description: "Share of the cycle's token budget reserved for consolidation (0.0 to 1.0). The phase splits this further across its audit, reinforce, and consolidate steps (see the Consolidation Budget settings). Relative weight across phases; unspent budget flows to later phases.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamPruningFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Share of the cycle's token budget reserved for pruning (0.0 to 1.0). This phase uses no LLM, so 0 (the default) reserves nothing: it runs whenever the cycle has budget left, so confidence decay still happens every cycle.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamWeightAdjustFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Share of the cycle's token budget reserved for recomputing relationship weights (0.0 to 1.0). This phase uses no LLM, so 0 (the default) reserves nothing: it runs whenever the cycle has budget left.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.cap_per_cycle", Type: "number", DefaultValue: json.RawMessage(`2000`), Description: "Maximum memory pairs the LLM checks for contradictions per cycle. Raise to clear a backlog faster on a large project, then restore.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(100)},
	{Key: "dreaming.contradiction.loser_haircut", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "How much confidence the losing memory in a contradiction keeps (0.0 to 1.0; the value multiplies its confidence). Lower means a harsher penalty. The penalty softens each time the same contradiction is reaffirmed.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.winner_haircut", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "How much confidence the winning memory keeps (0.0 to 1.0; multiplies its confidence). Slightly below 1.0 to reflect that no judgment is certain. Softens on repeat like the loser penalty.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.tie_haircut", Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "How much confidence both memories keep when the judge can't pick a winner (0.0 to 1.0; multiplies confidence). Softens on repeat.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.paraphrase_enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "When two memories are near-identical, retire the lower-confidence one automatically instead of spending an LLM call to compare them. Saves cost and catches duplicates the contradiction judge skips.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.paraphrase_threshold", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "How similar two memories must be for one to be retired automatically as a duplicate of the other (0.0 to 1.0). Keep it high to avoid retiring genuinely different memories.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamParaphraseEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run a similarity sweep over every eligible memory to catch near-duplicates the contradiction phase misses.", Category: "dreaming_paraphrase"},
	{Key: service.SettingDreamParaphraseThreshold, Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "How similar two memories must be for the sweep to retire the lower-confidence one (0.0 to 1.0). Keep it high to avoid retiring genuinely different memories.", Category: "dreaming_paraphrase", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamParaphraseCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum memories the paraphrase sweep examines per cycle. If more remain, they are picked up next cycle.", Category: "dreaming_paraphrase", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: service.SettingDreamParaphraseTopK, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "How many nearest neighbours the sweep compares each memory against. 1 (the default) is conservative; raise to 3 to 5 once you've confirmed your vector store and provider can handle the extra load.", Category: "dreaming_paraphrase", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamParaphraseStaleFetchMax, Type: "number", DefaultValue: json.RawMessage(`50000`), Description: "Maximum unprocessed memories the paraphrase sweep loads per cycle, to bound memory use. When more remain, the oldest are handled first and the rest drain over later cycles.", Category: "dreaming_paraphrase", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(1000)},
	{Key: service.SettingDreamConsolidationStaleFetchMax, Type: "number", DefaultValue: json.RawMessage(`50000`), Description: "Maximum memories the consolidation phase loads per cycle, to bound memory use. Only memories changed since they were last consolidated are considered; the rest drain over later cycles.", Category: "dreaming_consolidation", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(1000)},
	{Key: service.SettingDreamContradictionStaleFetchMax, Type: "number", DefaultValue: json.RawMessage(`50000`), Description: "Maximum memories the contradiction phase loads per cycle, to bound memory use when many are still unchecked (for example the first run on a large project).", Category: "dreaming_contradiction", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(1000)},
	{Key: service.SettingDreamPruningBatchSize, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "How many memories pruning processes at a time. Pruning visits every memory each cycle to apply confidence decay; smaller batches use less memory but run more database transactions.", Category: "dreaming_performance", Min: ptrF(10), Max: ptrF(100000), Step: ptrF(100)},
	{Key: service.SettingDreamEmbeddingBackfillEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Repair memories that lost their embedding: re-embed them when the embedder is healthy, otherwise clear the marker so they stop being treated as embedded.", Category: "dreaming_embedding_backfill"},
	{Key: service.SettingDreamEmbeddingBackfillCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum memories repaired per cycle by the embedding backfill. Raise to clear a large backlog faster, then restore.", Category: "dreaming_embedding_backfill", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: service.SettingDreamAugmentationBackfillEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Automatically re-run query augmentation for memories whose embedding was built from raw content (for example dream syntheses, or stores made while the augment provider was briefly down), so they become searchable by their augmented queries without a manual backfill.", Category: "dreaming_augmentation_backfill"},
	{Key: service.SettingDreamAugmentationBackfillCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum memories queued per cycle for augmentation backfill. If more remain, they are picked up over later cycles.", Category: "dreaming_augmentation_backfill", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: service.SettingDreamMultiVectorBackfillEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Automatically queue the multi-vector facet backfill for vectored memories that have not been faceted yet, so per-topic facets self-drain each cycle without a manual backfill. Also requires enrichment.multi_vector.enabled.", Category: "dreaming_multi_vector_backfill"},
	{Key: service.SettingDreamMultiVectorBackfillCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum memories queued per cycle for multi-vector facet backfill. If more remain, they are picked up over later cycles.", Category: "dreaming_multi_vector_backfill", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: service.SettingDreamConsolidationEntityBackfillCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum consolidation dreams queued per cycle for entity backfill. If more remain, they are picked up over later cycles. Extraction itself is unconditional (no enable toggle); this only paces the recovery load.", Category: "dreaming_consolidation_entity_backfill", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: service.SettingDreamingWeightSupportGain, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "How strongly a relationship's weight is lifted when several confident memories all attest to it (0.0 to 1.0). Higher values reward multi-memory support faster; the weight ceiling and the decay schedule still bound how high it can go.", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamingWeightRecallReinforceDelta, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "How much a relationship's weight grows each time it appears in a recall or graph result (0.0 to 1.0). Capped at the weight ceiling, limited to once per recall, and only applied when reconsolidation is in persist mode.", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "reconsolidation.mode", Type: "enum", DefaultValue: json.RawMessage(`"persist"`), Description: "Controls whether recalling a memory reinforces it. 'persist' writes the confidence and access updates; 'shadow' computes and reports them without saving (for observation); 'off' disables reinforcement entirely.", Category: "reconsolidation", EnumValues: []string{"shadow", "persist", "off"}},
	{Key: "reconsolidation.factor", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "How much confidence a memory gains each time it is recalled (0.0 to 1.0).", Category: "reconsolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "reconsolidation.decay_enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Let memories that haven't been recalled in a while slowly lose confidence during dream cycles.", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_threshold_days", Type: "number", DefaultValue: json.RawMessage(`14`), Description: "How many days a memory can go without being recalled before decay starts reducing its confidence.", Category: "reconsolidation", Min: ptrF(1), Max: ptrF(365), Step: ptrF(1)},
	{Key: "reconsolidation.decay_rate_per_cycle", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "How much confidence a decay-eligible memory loses per dream cycle (0.0 to 1.0).", Category: "reconsolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "reconsolidation.confidence_floor", Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "The lowest confidence decay can drive a memory to; it never decays below this (0.0 to 1.0).", Category: "reconsolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "recall.fusion.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run semantic (vector) and keyword search together and merge the results, instead of vector search alone. Safe to leave on: if keyword search isn't available on your backend, recall quietly falls back to vector-only.", Category: "recall_fusion"},
	{Key: "recall.fusion.rrf_k", Type: "number", DefaultValue: json.RawMessage(`60`), Description: "Smoothing constant for merging the two result lists. Higher values reduce the advantage of being ranked first in either list. 60 is the standard default; most deployments never change it.", Category: "recall_fusion", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: "recall.fusion.vector_weight", Type: "number", DefaultValue: json.RawMessage(`0.60`), Description: "How much semantic (meaning-based) search counts when merging results (0.0 to 1.0), relative to keyword weight. Default 0.60 favours meaning over exact wording; it was tuned on a controlled test set, so validate against your own corpus before changing it.", Category: "recall_fusion", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "recall.fusion.lexical_weight", Type: "number", DefaultValue: json.RawMessage(`0.40`), Description: "How much keyword (exact-wording) search counts when merging results (0.0 to 1.0), relative to vector weight. Raise it when exact tokens matter, like entity names or version strings.", Category: "recall_fusion", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingIngestionDecisionEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "When a new memory looks like a near-duplicate, let the model decide whether to add it, update an existing memory, delete one, or do nothing. When off, every memory is simply added, with no model call.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionShadow, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Try the ingestion decision but never act on it: the choice is logged while every memory is still simply added. Use this to watch what the model would do before letting it update or delete anything. Off by default, so decisions take effect.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "How similar an existing memory must be to a new one before it's offered to the model as a possible duplicate (0.0 to 1.0). Below this, the new memory is just added, with no model call.", Category: "enrichment_ingestion", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDedupThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Fallback duplicate-detection similarity used only when the ingestion-decision step is turned off (0.0 to 1.0). When it is on, the ingestion-decision threshold is used instead.", Category: "enrichment", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingExtractedFactGuardEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "While extracting facts, drop any fact that just restates its source memory (or another fact already extracted from it) and fold its tags into the source instead of storing a near-duplicate.", Category: "enrichment"},
	{Key: service.SettingExtractedFactParaphraseThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "How similar an extracted fact must be to its source memory before it's treated as a restatement and dropped (0.0 to 1.0). Falls back to the duplicate-detection threshold when unset.", Category: "enrichment", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingExtractedFactBackfillBatchSize, Type: "number", DefaultValue: json.RawMessage(`100`), Description: "How many memories the extracted-fact backfill processes per page when you run it from the admin tools. Lower it to ease database load on large projects, raise it to finish sooner. Progress shows up in the enrichment queue.", Category: "enrichment", Min: ptrF(10), Max: ptrF(1000), Step: ptrF(10)},
	{Key: service.SettingTokenRetention, Type: "number", DefaultValue: json.RawMessage(`365`), Description: "How many days token-usage records are kept before being pruned. Raise it for longer audit history; set to 0 to keep them forever.", Category: "usage", Min: ptrF(0), Max: ptrF(3650), Step: ptrF(1)},
	{Key: service.SettingTokenCostRates, Type: "json", DefaultValue: json.RawMessage(`[]`), Description: "Per-model (or per-provider) token prices used to turn token usage into dollar figures in the analytics views. A JSON list of {key, inputCostPer1k, outputCostPer1k} entries. Administrators edit it; everyone else can read the current rates.", Category: "usage"},
	{Key: service.SettingIngestionDecisionTopK, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How many candidate duplicates are shown to the model when it makes an ingestion decision.", Category: "enrichment_ingestion", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},

	{Key: service.SettingQueryAugmentEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Generate a few paraphrased queries for each memory and fold them into what gets embedded, so a memory matches the different ways someone might ask for it. On by default. After turning it on, use the Backfill button to update memories stored beforehand.", Category: "enrichment_query_augment"},
	{Key: service.SettingQueryAugmentCount, Type: "number", DefaultValue: json.RawMessage(`4`), Description: "How many paraphrased queries to generate per memory. 3 to 5 works well; fewer covers less phrasing, more starts crowding the embedding.", Category: "enrichment_query_augment", Min: ptrF(1), Max: ptrF(10), Step: ptrF(1)},
	{Key: service.SettingQueryAugmentMaxInputChars, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Character limit on the combined queries-plus-content text sent to the embedder (0 means no limit). Set it to about 90% of the embedding model's context window to prevent silent truncation; when it triggers, the content tail is trimmed and all generated queries are kept.", Category: "enrichment_query_augment", Min: ptrF(0), Max: ptrF(200000), Step: ptrF(512)},
	{Key: service.SettingQueryAugmentMaxTokens, Type: "number", DefaultValue: json.RawMessage(`2048`), Description: "Maximum tokens the query-augmentation model call may produce. Raise it if you see truncated-JSON parse failures in the logs; 2048 leaves small reasoning models room for their preamble plus a handful of queries.", Category: "enrichment_query_augment", Min: ptrF(128), Max: ptrF(8192), Step: ptrF(128)},

	{Key: service.SettingMultiVectorEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Split each memory into topic facets (plus the whole-memory vector) so a query about one sub-topic of a multi-topic memory still retrieves it strongly. On by default; it adds per-memory sentence-embedding cost during enrichment. Use the Backfill button to facet memories stored before it was enabled.", Category: "enrichment_multi_vector"},
	{Key: service.SettingMultiVectorFacetThreshold, Type: "number", DefaultValue: json.RawMessage(`0.65`), Description: "Minimum cosine similarity for two sentences of a memory to land in the same facet (0.0 to 1.0). Higher values make more, tighter facets; lower values make fewer, broader ones.", Category: "enrichment_multi_vector", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingMultiVectorMaxFacets, Type: "number", DefaultValue: json.RawMessage(`8`), Description: "Maximum vectors stored per memory, counting the whole-memory facet plus topic facets. Caps storage and per-query work for long, many-topic memories.", Category: "enrichment_multi_vector", Min: ptrF(1), Max: ptrF(storage.MaxFacetsUpperBound), Step: ptrF(1)},
	{Key: service.SettingMultiVectorEmbedConcurrency, Type: "number", DefaultValue: json.RawMessage(`4`), Description: "How many memories may have their facet sentences embedded at the same time across all enrichment workers. Bounds the embedding load a bulk multi-vector backfill puts on the embedder; lower it if a backfill overwhelms a modest embedder, raise it on a provider that sustains parallel calls. Takes effect on restart.", Category: "enrichment_multi_vector", Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: service.SettingMultiVectorFacetPresenceCacheTTL, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How long (seconds) recall remembers whether a namespace has topic facets before checking again. Recall skips its per-query topic-facet work when a namespace has none; this caches that check so it runs at most once per interval instead of on every recall. Higher means fewer checks but up to this many seconds before newly-faceted (or de-faceted) memories are noticed by recall. 0 checks on every recall.", Category: "enrichment_multi_vector", Min: ptrF(0), Max: ptrF(3600), Step: ptrF(1)},
	{Key: service.SettingRankWeightSim, Type: "number", DefaultValue: json.RawMessage(`0.50`), Description: "How much semantic match between the query and a memory counts toward its recall rank (0.0 to 1.0). This is the dominant signal; lower it to give the other signals more say.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightRec, Type: "number", DefaultValue: json.RawMessage(`0.15`), Description: "How much a memory's freshness counts toward its recall rank (0.0 to 1.0). Newer memories rank higher, but older ones aren't sharply penalised.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightImp, Type: "number", DefaultValue: json.RawMessage(`0.10`), Description: "How much a memory's importance counts toward its recall rank (0.0 to 1.0). Importance is set per memory; raise this to honour manual curation more strongly.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightFreq, Type: "number", DefaultValue: json.RawMessage(`0.00`), Description: "How much how often a memory has been accessed counts toward its recall rank (0.0 to 1.0). Default 0, because confidence already captures usage on every recall and counting it twice over-weights popular memories. Raise it only if you bypass the reinforcement hook.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightGraph, Type: "number", DefaultValue: json.RawMessage(`0.20`), Description: "How much knowledge-graph connections count toward recall rank (0.0 to 1.0). Boosts memories linked to entities mentioned in the query.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightConf, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "How much a memory's confidence counts toward its recall rank (0.0 to 1.0). Confidence rises on recall and decays when unused, so this favours well-used, well-aligned memories. Start small and raise it once your confidence values have settled.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightOrigin, Type: "number", DefaultValue: json.RawMessage(`0.25`), Description: "A bonus added to memories that live in the project being searched (0.0 to 1.0), so on-topic project memories outrank recently-written global or persona memories, while genuine cross-tier answers (like an identity question answered from the persona tier) still surface. The tested safe range is 0.20 to 0.35. Can be overridden per project.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightMmr, Type: "number", DefaultValue: json.RawMessage(`0.75`), Description: "How aggressively recall removes near-duplicate results in favour of variety (0.0 to 1.0). 1.0 turns the de-duplication off (pure relevance order); 0.7 to 0.8 gently demotes near-identical results without hurting single-fact lookups. Can be overridden per project.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRerankEnabled, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Re-score the top recall candidates with a cross-encoder (or LLM judge) reranker before the final result is returned, reordering by model-judged query relevance. Off by default and requires the Reranker provider slot to be configured. Once enabled it takes effect immediately at the default Rerank weight; set that weight to 0 to suppress its effect without disabling it.", Category: "ranking"},
	{Key: service.SettingRankWeightRerank, Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "How much the reranker's relevance score counts toward recall rank (0.0 to 1.0), added on top of the composite score rather than replacing similarity. Default 0.3, tuned on a live corpus so a confident reranker reorders the close calls without overriding a clearly stronger composite match; set it to 0 to suppress the reranker's effect while leaving it enabled. Only used when Reranking is enabled and a Reranker slot is configured.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRerankCandidates, Type: "number", DefaultValue: json.RawMessage(`25`), Description: "How many top candidates the reranker scores per query, after MMR and before the final result is returned. One cross-encoder pass each, so this bounds the reranking cost. Keep it at or above your typical result limit so a buried answer the reranker would promote isn't cut off first. Used by both recall and ask.", Category: "ranking", Min: ptrF(1), Max: ptrF(200), Step: ptrF(1)},
	{Key: service.SettingRerankMaxDocChars, Type: "number", DefaultValue: json.RawMessage(`1200`), Description: "Maximum characters of each memory sent to the reranker. Cross-encoders only read the first few hundred tokens, so the rest is wasted, and trimming keeps a single long memory from overflowing the reranker server's batch and failing the request. Default 1200 keeps a query+memory pair under a stock llama-server's 512-token batch; raise it only if your reranker server is launched with a larger --ubatch-size. Used by both recall and ask.", Category: "ranking", Min: ptrF(256), Max: ptrF(16000), Step: ptrF(64)},
	{Key: service.SettingRerankJudgeMaxTokens, Type: "number", DefaultValue: json.RawMessage(`16`), Description: "Maximum tokens the LLM-judge reranker may generate per candidate. Only used when the Reranker slot is a generative chat model (detected method \"judge\") rather than a cross-encoder; the judge outputs a single relevance number, so this stays small.", Category: "ranking", Min: ptrF(1), Max: ptrF(256), Step: ptrF(1)},
	{Key: service.SettingRerankJudgeTemperature, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Sampling temperature for the LLM-judge reranker's per-candidate scoring call (0.0 to 1.0). 0 keeps the relevance score as deterministic as a generative model allows. Only used when the Reranker slot's detected method is \"judge\".", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRecallFusionNormalizePerChan, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "When hybrid recall is on, balance each searched tier evenly so a large corpus (like the global tier) doesn't crowd out a small project. Off by default, which keeps the original behaviour.", Category: "recall_fusion"},

	// Enrichment worker pool tuning. Hot-reloadable knobs are read on every
	// worker iteration; restart-required knobs are read once when the pool
	// is constructed at server start.
	{Key: service.SettingEnrichmentWorkerBatchClaimSize, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "How many jobs a worker takes at once, sharing a single embedding call across them. 1 (the default) keeps work small and predictable, safe for a single local GPU. Raise it only when your provider can handle larger embedding batches.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerLLMConcurrency, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "How many of a worker's claimed jobs run their LLM extraction calls (fact + entity) in parallel within a batch. 1 (the default) runs them one at a time, safe for a single local GPU. Raise it only if your provider has spare capacity; parallel calls to a one-GPU Ollama backend queue up and can look like the system is stuck.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerEmbedTimeoutSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How long a worker waits for one embedding call before giving up, in seconds.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerEmbedInputCap, Type: "number", DefaultValue: json.RawMessage(`256`), Description: "Maximum number of texts sent in a single embedding call; larger batches are split. Kept conservative so smaller providers with low per-input limits aren't overwhelmed.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(8192), Step: ptrF(64)},
	{Key: service.SettingEnrichmentWorkerBreakerEscalateSeconds, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How long a provider must stay unreachable before its log messages escalate from informational (likely warming up) to error (a real outage), in seconds.", Category: "enrichment_performance", Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingEnrichmentWorkerMaxBackoffSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Longest a worker waits between checks when the queue is empty, in seconds. Idle workers back off gradually up to this limit.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerCountSQLite, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Number of enrichment workers to run on SQLite. SQLite allows only one writer, so more than one worker brings no benefit.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerCountPostgres, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Number of enrichment workers to run on Postgres. 1 (the default) is safe for any provider. Raise it (say 2 to 4) only when your model and embedding providers can each handle parallel calls; parallel calls to a one-GPU Ollama backend queue up and can look like the system is stuck. Each worker uses its own provider slot.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(128), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerPollIntervalSeconds, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How often idle workers check for new jobs, in seconds.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentPoolTickIntervalSeconds, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How often the enrichment queue's live status (in-flight count, oldest job age, stage breakdown) is refreshed for the admin UI, in seconds.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentIngestionRationaleMaxLen, Type: "number", DefaultValue: json.RawMessage(`500`), Description: "Maximum characters of the model's ingestion-decision explanation kept on a memory; longer text is trimmed.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(10000), Step: ptrF(10)},
	{Key: service.SettingEnrichmentHeartbeatSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How often a worker marks its in-progress jobs as still alive, in seconds. Smaller values let the admin UI and the stuck-job recovery notice a dead worker sooner, at the cost of more database writes.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentStuckThreshold, Type: "number", DefaultValue: json.RawMessage(`1800`), Description: "How long an in-progress job may sit without progress before it's treated as stuck and put back in the queue, in seconds. Set it above your longest normal job so a slow model call isn't mistaken for a dead worker.", Category: "enrichment_performance", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},
	{Key: service.SettingEnrichmentStuckSweep, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How often the system scans for stuck enrichment jobs and requeues them, in seconds.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingEnrichmentClaimMaxAge, Type: "number", DefaultValue: json.RawMessage(`7200`), Description: "Absolute age at which an in-progress job is requeued no matter what, in seconds. A backstop for workers that keep reporting alive but have stopped making progress. Keep it comfortably above your longest normal job.", Category: "enrichment_performance", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},
	{Key: service.SettingEnrichmentFailedRetentionDays, Type: "number", DefaultValue: json.RawMessage(`7`), Description: "How many days permanently-failed enrichment jobs are kept before being deleted, so a flaky provider can't grow the failed list without bound. Set to 0 to keep them forever.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(3650), Step: ptrF(1)},

	// Fact / entity extraction LLM-call tunables. All hot-reloadable, all
	// resolved per call by both ExtractionService (sync) and WorkerPool
	// (async).
	{Key: service.SettingFactExtractionMaxTokens, Type: "number", DefaultValue: json.RawMessage(`4096`), Description: "Maximum tokens the fact-extraction model call may produce. Raise it for dense inputs (research or design docs) whose output gets cut off. Limited by the model's context window.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingEntityExtractionMaxTokens, Type: "number", DefaultValue: json.RawMessage(`4096`), Description: "Maximum tokens the entity-extraction model call may produce. Raise it for entity-dense inputs whose output gets cut off. Limited by the model's context window.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingEnrichmentIngestionDecisionMaxTokens, Type: "number", DefaultValue: json.RawMessage(`512`), Description: "Maximum tokens the ingestion-decision model call may produce. The response is a short JSON verdict (operation, target_id, rationale); raise it only if you see truncated-JSON parse failures from the ingestion phase. Limited by the model's context window.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingEnrichmentConflictMaxTokens, Type: "number", DefaultValue: json.RawMessage(`256`), Description: "Maximum tokens the conflict-detection model call may produce. The response is a short JSON contradiction verdict; raise it only if you see truncated-JSON parse failures from conflict detection. Limited by the model's context window.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingEnrichmentTestPromptMaxTokens, Type: "number", DefaultValue: json.RawMessage(`8192`), Description: "Maximum tokens the admin \"Test prompt\" call may produce. 8192 leaves headroom for reasoning models that spend output budget on a thinking pass before the answer; a tighter cap can truncate them to an empty response. Limited by the model's context window.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingFactExtractionSyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for fact extraction when a memory is enriched immediately on the request path (0.0 to 2.0). Lower is more deterministic. Set this equal to the background value to make both paths behave the same.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingFactExtractionAsyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.2`), Description: "Sampling temperature for fact extraction when it runs in the background worker (0.0 to 2.0). Lower is more deterministic. Set this equal to the request-path value to make both paths behave the same.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEntityExtractionSyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for entity extraction when a memory is enriched immediately on the request path (0.0 to 2.0). Lower is more deterministic.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEntityExtractionAsyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.2`), Description: "Sampling temperature for entity extraction when it runs in the background worker (0.0 to 2.0). Lower is more deterministic.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingRelationshipExtractionMaxTokens, Type: "number", DefaultValue: json.RawMessage(`4096`), Description: "Maximum tokens the relationship-extraction model call may produce. Relationship extraction is a second pass, separate from entity extraction, fed the text plus the extracted entity names; raise it for relationship-dense inputs whose output gets cut off. Limited by the model's context window.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingRelationshipExtractionSyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for relationship extraction when a memory is enriched immediately on the request path (0.0 to 2.0). Lower is more deterministic.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingRelationshipExtractionAsyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.2`), Description: "Sampling temperature for relationship extraction when it runs in the background worker (0.0 to 2.0). Lower is more deterministic.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEntityResolutionCosineEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "On by default: a newly extracted entity is embedded and compared to existing entities before it is created, so a near-duplicate (same meaning, different spelling) is merged at write time instead of waiting for the nightly dedup. It adds one embedding call per genuinely new entity on the ingestion path; turn it off only if a rate-limited or saturated embedder needs the relief (dreaming's entity dedup still merges duplicates after the fact).", Category: "enrichment_performance"},
	{Key: service.SettingEntityResolutionCosineThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "How similar a new entity must be to an existing same-type entity before they're treated as the same and merged at write time (0.0 to 1.0). Keep it high to avoid merging entities that are actually distinct. Only used when write-time entity resolution is on.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingSemanticVocabThreshold, Type: "number", DefaultValue: json.RawMessage(`0.50`), Description: "When an extracted relation or entity type is not in the built-in synonym map, it is embedded and assigned to the nearest canonical term whose description is at least this cosine-similar (0.0 to 1.0); below it, the label falls back to 'related_to' / 'other'. Lower keeps more labels meaningful (higher coverage) at the cost of some loose assignments; raise it to only accept confident matches. Set to 1.0 to disable the embedding fallback and use the static map only.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingExtractionChunkThresholdTokens, Type: "number", DefaultValue: json.RawMessage(`2800`), Description: "Memories whose estimated content is larger than this many tokens are split into overlapping chunks for extraction, so a dense memory no longer gets cut off at the model's output limit. Smaller memories (the large majority) are extracted whole. Set near the model's per-call output budget.", Category: "enrichment_performance", Min: ptrF(256), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingExtractionChunkOverlapTokens, Type: "number", DefaultValue: json.RawMessage(`200`), Description: "How many tokens consecutive extraction chunks share, so an entity or relationship sitting on a chunk boundary is still seen in full by at least one chunk.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(8192), Step: ptrF(32)},
	{Key: service.SettingExtractionContinuationMaxPasses, Type: "number", DefaultValue: json.RawMessage(`2`), Description: "When an extraction call still hits its output limit after chunking, the worker asks the model to continue with only the items it has not yet returned, up to this many follow-up passes. 0 disables continuation. Bounded so a looping model cannot run away.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(10), Step: ptrF(1)},
	{Key: service.SettingExtractionEntityNameMaxChars, Type: "number", DefaultValue: json.RawMessage(`120`), Description: "An extracted entity whose name is longer than this many characters is dropped before it is embedded or stored. A degenerate model can return a whole paragraph where a short entity name was expected; this keeps that out of the graph. 0 disables the length check.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(1000), Step: ptrF(10)},
	{Key: service.SettingExtractionEntityNameMaxWords, Type: "number", DefaultValue: json.RawMessage(`12`), Description: "An extracted entity whose name has more than this many words is dropped: a real entity name is a short noun phrase, so a longer one is almost always a whole sentence the model mis-returned as a name. 0 disables the word-count check.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingExtractionEntityNameMinDistinctWordRatio, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "An extracted entity name is dropped when its repetition looks degenerate: the fraction of distinct words falls below this ratio (a model stuck in a loop repeats the same words), or a short fragment repeats back to back. 0 disables the repetition check.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingExtractionMaxEntitiesPerMemory, Type: "number", DefaultValue: json.RawMessage(`128`), Description: "The most distinct entities one memory's extraction may persist, measured after duplicates are collapsed. The per-name checks above judge one name at a time; this bounds the count, so a model that enumerates hundreds of individually valid-looking names for a single memory cannot flood the graph. Relationships whose endpoints fall outside the kept set are dropped with them. 0 disables the clamp.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(1000), Step: ptrF(8)},

	// Dreaming performance/tuning knobs that complement the existing
	// dreaming.* keys. Hot-reloadable per dream cycle.
	{Key: service.SettingDreamContradictionNeighbors, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "How many nearby memories each one is compared against for contradictions. 1 (the default) is the lightest setting, suitable for a single local GPU. Each step up multiplies the model calls per cycle, so raise it only when your provider can keep up.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamEntityMergeThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "How similar two same-type entities must be before they're merged into one during entity dedup (0.0 to 1.0). Keep it high to avoid merging entities that are actually distinct.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamEntityHygieneEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "On by default: during entity dedup, delete any entity whose name is degenerate (a wall of text, a whole sentence, or a repetition loop) by the same rule the write-time guard uses, so a bad name created before the guard existed self-cleans. Uses the enrichment entity-name limits.", Category: "dreaming_performance"},
	{Key: service.SettingDreamSchedulerPollSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How often the scheduler checks for projects ready to dream, in seconds.", Category: "dreaming_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingDreamHeartbeatInterval, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How often a running dream cycle marks itself as still active, in seconds. Smaller values let the admin UI notice a stalled cycle sooner, at the cost of more database writes.", Category: "dreaming_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingDreamHeartbeatStale, Type: "number", DefaultValue: json.RawMessage(`120`), Description: "How long a running cycle can go without a sign of activity before the admin UI flags it as idle, in seconds. Display only; it does not abandon the cycle.", Category: "dreaming_performance", Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingDreamStuckThreshold, Type: "number", DefaultValue: json.RawMessage(`1800`), Description: "How long a cycle can run without progress before it can be abandoned, manually or automatically, in seconds. Keep it above your longest single phase so a slow but healthy cycle isn't cut short.", Category: "dreaming_performance", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},
	{Key: service.SettingDreamStuckSweep, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How often the system scans for stuck dream cycles and abandons them, in seconds.", Category: "dreaming_performance", RequiresRestart: true, Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},

	// Lifecycle sweep tuning.
	{Key: service.SettingLifecycleSweepIntervalSeconds, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How often the lifecycle sweep runs, in seconds. The sweep expires memories past their TTL and permanently removes soft-deleted ones past their retention window.", Category: "lifecycle", Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingLifecycleBatchSize, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum memories processed in a single lifecycle sweep pass.", Category: "lifecycle", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: service.SettingLifecycleOrphanGraceSeconds, Type: "number", DefaultValue: json.RawMessage(`3600`), Description: "How old an entity must be before it can be deleted as an orphan, in seconds. The grace period protects entities created mid-enrichment, before their relationships are written. It is not the only protection: entities in a namespace with a queued or in-flight enrichment job are skipped regardless of age, so a re-extraction cannot have its entities deleted while its job waits. Orphans in a namespace with a pending job are therefore only collected once that queue drains.", Category: "lifecycle", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},

	// Recall reinforcement event payload bound.
	{Key: service.SettingReinforcementEventMemoryCap, Type: "number", DefaultValue: json.RawMessage(`20`), Description: "Maximum memory IDs listed in a recall-reinforcement event. Keeps event payloads from ballooning on very broad queries.", Category: "reconsolidation", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: service.SettingReinforcementEventRelationshipCap, Type: "number", DefaultValue: json.RawMessage(`20`), Description: "Maximum relationship IDs listed in a relationship-reinforcement event. The relationship-side counterpart to the memory cap above.", Category: "reconsolidation", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},

	// Cascade and settings cache TTLs.
	{Key: service.SettingCascadeCacheTTLSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How long resolved per-project and per-user overrides are cached, in seconds. Changes to overrides take effect within this window.", Category: "performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(3600), Step: ptrF(1)},
	{Key: service.SettingSettingsCacheTTLSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How long a resolved setting is cached before it's re-read from the database, in seconds. This caching window itself takes effect only after a restart.", Category: "performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(3600), Step: ptrF(1)},

	{Key: service.SettingEmbeddingCacheEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Cache embedding vectors keyed by model and exact input text so the same text is never embedded twice. A hit returns the identical vector and skips the provider call, cutting redundant embedding spend (re-enrichment, dream audits, repeated recall queries). No effect on results.", Category: "embedding_cache"},
	{Key: service.SettingEmbeddingCacheMaxEntries, Type: "number", DefaultValue: json.RawMessage(`8192`), Description: "Maximum number of embedding vectors held in the cache. Least-recently-used entries are evicted past this bound. Higher values raise the hit rate at the cost of memory.", Category: "embedding_cache", Min: ptrF(0), Max: ptrF(1000000), Step: ptrF(256)},
	{Key: service.SettingEmbeddingCacheTTLSeconds, Type: "number", DefaultValue: json.RawMessage(`900`), Description: "How long a cached embedding stays valid, in seconds (0 means no expiry). Embeddings are stable for a fixed model, so this is a memory-hygiene control rather than a correctness one.", Category: "embedding_cache", Min: ptrF(0), Max: ptrF(86400), Step: ptrF(60)},

	// Provider prompt delivery.
	{Key: service.SettingProviderPromptCacheEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Mark the system instruction prefix as cacheable on providers that accept an explicit hint (Anthropic cache_control). Below a model's minimum cacheable prefix size the hint is a no-op, so this is safe to leave on; it only pays off on large custom prompts.", Category: "provider_prompt_delivery"},
	{Key: service.SettingProviderAnthropicJSONToolUse, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Coerce Anthropic JSON output via a forced tool_use call. Off by default; enable only for Anthropic-compatible proxies (e.g. OAuth/Claude-Code passthroughs) that drop response formatting. The native api.anthropic.com path does not need it.", Category: "provider_prompt_delivery"},

	// Per-host provider concurrency. Shared across every worker slot and
	// subsystem that targets the same host, so the host sees an aggregate cap.
	{Key: service.SettingProviderLLMHostConcurrency, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Maximum LLM completion requests in flight to a single model host at once, summed across every enrichment worker, dreaming, and ask. Default 1 keeps a fresh install from overwhelming a single-GPU local model (SGLang, llama.cpp, Ollama); raise it to match your host's capacity. Set 0 to disable the limit. Hot-reloads within the settings-cache TTL.", Category: "provider_concurrency", Min: ptrF(0), Max: ptrF(1024), Step: ptrF(1)},
	{Key: service.SettingProviderEmbedHostConcurrency, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Maximum embedding requests in flight to a single embedding host at once, summed across every worker slot and subsystem. Default 1 protects a modest local embedder from a bulk backfill stampede; raise it to match your embedder's capacity. Set 0 to disable the limit. Hot-reloads within the settings-cache TTL.", Category: "provider_concurrency", Min: ptrF(0), Max: ptrF(1024), Step: ptrF(1)},

	// Circuit-breaker backoff. When a provider slot fails repeatedly the breaker
	// opens and backs off exponentially so a downed host is not hammered. All
	// three hot-reload within the settings-cache TTL, no restart.
	{Key: service.SettingProviderCircuitBreakerMaxFailures, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How many consecutive failed calls to a provider slot (LLM or embedding) trip its circuit breaker. Once tripped, calls are rejected instantly instead of hammering a host that is down, until a spaced-out probe succeeds. Default 5.", Category: "provider_concurrency", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingProviderCircuitBreakerResetBaseSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Base wait, in seconds, before a tripped breaker sends its first trial probe. Each failed probe doubles the wait (up to the max below), so a provider that stays down is probed ever less often rather than every base interval forever. Default 30.", Category: "provider_concurrency", Min: ptrF(1), Max: ptrF(3600), Step: ptrF(1)},
	{Key: service.SettingProviderCircuitBreakerResetMaxSeconds, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "Ceiling, in seconds, on the exponentially-growing breaker probe interval. Bounds how long recovery can lag after a long outage. Default 300 (5 minutes); must be >= the base wait.", Category: "provider_concurrency", Min: ptrF(1), Max: ptrF(86400), Step: ptrF(1)},

	// API rate-limit per-user-bucket cleanup.
	{Key: service.SettingAPIRateLimitCleanupSeconds, Type: "number", DefaultValue: json.RawMessage(`60`), Description: "How often the rate limiter clears out tracking for inactive users, in seconds.", Category: "api_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(3600), Step: ptrF(1)},
	{Key: service.SettingAPIRateLimitStaleSeconds, Type: "number", DefaultValue: json.RawMessage(`600`), Description: "How long a user can be idle before their rate-limit tracking is discarded, in seconds.", Category: "api_performance", RequiresRestart: true, Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},

	// Dashboard session JWT timings. Hot-reload, reads via the 30s settings cache.
	{Key: service.SettingAuthSessionTokenTTLSeconds, Type: "number", DefaultValue: json.RawMessage(`86400`), Description: "How long a dashboard login session lasts, in seconds. Default is 86400 (24 hours). Applies to every login method.", Category: "auth", Min: ptrF(60), Max: ptrF(2592000), Step: ptrF(60)},
	{Key: service.SettingAuthSessionRefreshThresholdSeconds, Type: "number", DefaultValue: json.RawMessage(`43200`), Description: "How old an active session must get before it's silently renewed so a working user isn't logged out, in seconds. Default is 43200 (12 hours, half the session lifetime). Must be less than the session lifetime, or renewal never happens and users get logged out abruptly.", Category: "auth", Min: ptrF(30), Max: ptrF(2592000), Step: ptrF(60)},

	// In-process event bus.
	{Key: service.SettingEventsSubscriberBufferSize, Type: "number", DefaultValue: json.RawMessage(`64`), Description: "How many pending events are buffered for each live connection. Advanced: too low drops events during bursts, too high uses more memory per connection.", Category: "events", RequiresRestart: true, Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: service.SettingEventsReplayCapacity, Type: "number", DefaultValue: json.RawMessage(`256`), Description: "How many recent events are kept so a client that briefly disconnects can catch up on reconnect. Advanced: too low misses events across reconnects, too high uses more memory.", Category: "events", RequiresRestart: true, Min: ptrF(1), Max: ptrF(100000), Step: ptrF(10)},
	{Key: service.SettingEventsSSEKeepaliveSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How often a keepalive ping is sent on a live event stream, in seconds, so proxies don't close an idle connection.", Category: "events", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},

	// Diagnostic log store (log_entries). System-operator only.
	{Key: service.SettingLoggingDBCaptureEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Capture diagnostic logs to the database in addition to the console, for the operator Logs page. When off, logs go to the console only and the Logs page stops gaining new entries. Takes effect without a restart.", Category: "logging"},
	{Key: service.SettingLoggingDBLevel, Type: "enum", DefaultValue: json.RawMessage(`"info"`), Description: "Minimum severity written to the log table, independent of the console LOG_LEVEL. Lower levels capture more detail at the cost of a faster-growing table. Takes effect without a restart.", Category: "logging", EnumValues: []string{"debug", "info", "warn", "error"}},
	{Key: service.SettingLoggingRetentionMaxRows, Type: "number", DefaultValue: json.RawMessage(`100000`), Description: "Hard cap on how many log entries are kept. The oldest rows past this count are pruned, giving a fixed-size rolling window. Set 0 to disable the count cap.", Category: "logging", Min: ptrF(0), Max: ptrF(10000000), Step: ptrF(1000)},
	{Key: service.SettingLoggingRetentionMaxAge, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days of log entries to keep before they are pruned. Applied alongside the row cap, whichever limit is reached first. Set 0 to disable the age limit.", Category: "logging", Min: ptrF(0), Max: ptrF(3650), Step: ptrF(1)},

	// Admin graph minimum edge weight.
	{Key: service.SettingGraphDefaultMinWeight, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "The weakest connection shown in the graph view when the caller doesn't specify a minimum (0.0 to 1.0). Raise it to hide low-confidence links.", Category: "api_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingGraphMaxEdges, Type: "number", DefaultValue: json.RawMessage(`2000`), Description: "Maximum connections returned for the graph view. When a project has more, the strongest are returned and the UI shows a partial-view notice. Keeps the 3D graph from stalling on very large projects; raise it on capable machines.", Category: "api_performance", Min: ptrF(100), Max: ptrF(100000), Step: ptrF(100)},

	// Graph visualization layout defaults. Per-project overrides live on the
	// graph page; these supply the fallback when a project has not been tuned.
	{Key: service.SettingGraphCenterGravity, Type: "number", DefaultValue: json.RawMessage(`0.75`), Description: "Default pull toward the centre in the 3D graph layout. Higher values gather separate clusters together; lower values let the connections shape the layout. Can be overridden per project on the graph page.", Category: "graph_visualization", Min: ptrF(0), Max: ptrF(3), Step: ptrF(0.05)},
	{Key: service.SettingGraphChargeStrength, Type: "number", DefaultValue: json.RawMessage(`-100`), Description: "Default spacing force in the 3D graph layout (stored as a negative number; the settings and graph pages show it as a positive 'repulsion' slider). More repulsion spreads nodes apart. Can be overridden per project.", Category: "graph_visualization", Min: ptrF(-100), Max: ptrF(0), Step: ptrF(1)},
	{Key: service.SettingGraphLinkDistance, Type: "number", DefaultValue: json.RawMessage(`100`), Description: "Default length of the links between connected nodes in the 3D graph layout. Lower pulls connected nodes closer; higher spreads the graph out. Can be overridden per project.", Category: "graph_visualization", Min: ptrF(5), Max: ptrF(100), Step: ptrF(1)},

	// Batch store request item cap.
	{Key: service.SettingAPIBatchStoreMaxItems, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum memories allowed in a single batch-store request. Advanced: a higher limit lets one request do more work, so pair it with a request body-size limit at your proxy.", Category: "api_performance", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},

	// Export pagination size.
	{Key: service.SettingExportPageSize, Type: "number", DefaultValue: json.RawMessage(`100`), Description: "How many memories are read at a time while building an export.", Category: "performance", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},

	// Self-service export job knobs.
	{Key: service.SettingExportArtifactDir, Type: "string", DefaultValue: json.RawMessage(`""`), Description: "Folder where export archives are written (one zip per job). Leave empty to use an 'exports' folder in the working directory, which works out of the box. New jobs use the new folder; jobs already running finish in the old one.", Category: "export"},
	{Key: service.SettingExportTTLHours, Type: "number", DefaultValue: json.RawMessage(`168`), Description: "How many hours a finished export is kept for download before it's deleted and marked expired. Default is 168 (7 days).", Category: "export", Min: ptrF(1), Max: ptrF(8760), Step: ptrF(1)},
	{Key: service.SettingExportMaxPerUserPerDay, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How many exports one user can start in a rolling 24-hour window (on top of the one-at-a-time limit). Stops a single account from queueing hundreds of large archives.", Category: "export", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},

	// MCP per-response token budget.
	{Key: service.SettingMCPMaxResultTokens, Type: "number", DefaultValue: json.RawMessage(`22000`), Description: "Token budget for a single MCP tool response, so one call can't fill the client's whole context. Responses over this are trimmed, and very large ones are cut off with a marker noting the truncation. Minimum 100, below which the marker can't fit.", Category: "mcp", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(100)},

	// Recall scoring and pagination. Hot-reloadable. Operators retune
	// recency / over-fetch shape during incident response without redeploy.
	{Key: service.SettingRankingRecencyDecayPerHour, Type: "number", DefaultValue: json.RawMessage(`0.01`), Description: "How quickly a memory's freshness bonus fades with age. As a guide, 0.01 halves the bonus in about 69 hours, 0.02 in about 35 hours, 0.005 in about 138 hours. Lower values let older memories compete more evenly with fresh ones.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.001)},
	{Key: service.SettingRankingGraphHopMultiplier, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "How much the graph bonus shrinks with each extra step away from the query (0.0 to 2.0). Lower values discount distant, indirect connections more. At 0.5 each hop counts roughly half as much as the previous one.", Category: "ranking", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingRecallDefaultLimit, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "How many results recall returns when the caller doesn't ask for a specific number.", Category: "recall", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},
	{Key: service.SettingRecallMaxLimit, Type: "number", DefaultValue: json.RawMessage(`50`), Description: "The most results a single recall can return. Requests for more are capped at this value; use the list tool, which paginates, for larger sets.", Category: "recall", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},
	{Key: service.SettingRecallGraphDefaultDepth, Type: "number", DefaultValue: json.RawMessage(`2`), Description: "How many graph steps recall follows when the caller doesn't specify. 0 turns the graph contribution off. Going deeper costs noticeably more on large projects.", Category: "recall", Min: ptrF(0), Max: ptrF(10), Step: ptrF(1)},
	{Key: service.SettingRecallGraphMaxDepth, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "The deepest graph traversal recall and the graph tool will allow. Requests for more are capped here. The per-recall edge budget still limits total work.", Category: "recall", Min: ptrF(1), Max: ptrF(20), Step: ptrF(1)},
	{Key: service.SettingRecallGraphReserveFraction, Type: "number", DefaultValue: json.RawMessage(`0.15`), Description: "Share of a recall response's space set aside for knowledge-graph context (0.0 to 0.5), so some entities and relationships always come back instead of being squeezed out by memories. 0 leaves the graph out of recall results.", Category: "recall", Min: ptrF(0), Max: ptrF(0.5), Step: ptrF(0.01)},
	{Key: service.SettingRecallGraphVectorActivationEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Let recall find related entities by meaning across the searched project and the global tier, then boost the memories connected to them. Off falls back to matching entities only by exact wording in the current project.", Category: "recall"},
	{Key: service.SettingRecallGraphVectorActivationTopK, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How many related entities to pull from each tier when finding graph connections by meaning.", Category: "recall", Min: ptrF(1), Max: ptrF(50), Step: ptrF(1)},
	{Key: service.SettingRecallGraphMaxEdges, Type: "number", DefaultValue: json.RawMessage(`2000`), Description: "Maximum graph connections recall may follow in one call, shared fairly across starting points. (Separate from the graph-view limit.)", Category: "recall", Min: ptrF(100), Max: ptrF(100000), Step: ptrF(100)},
	{Key: service.SettingRecallOverfetchMultiplier, Type: "number", DefaultValue: json.RawMessage(`3`), Description: "How many extra candidates recall gathers before re-ranking and trimming to the requested count. 3 means fetch three times as many, then keep the best. Higher values can improve ranking quality at some query cost.", Category: "recall", Min: ptrF(1), Max: ptrF(20), Step: ptrF(0.5)},
	{Key: service.SettingRecallOverfetchMin, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Smallest candidate pool recall will gather, so even a request for one result still has enough candidates for the ranker to choose well.", Category: "recall", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},

	// Ask synthesis tool. Off by default so it never spends model tokens until
	// an operator opts in and configures the dedicated ask provider slot. The
	// ask system prompt is a "prompt" setting, surfaced on the Prompt Templates
	// page under category ask_prompts (below).
	{Key: service.SettingAskEnabled, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable the ask tool, which synthesizes a single answer over your recalled memories using a dedicated LLM. Off by default. When off, ask does not appear in the MCP tool list or the REST API. Requires the Ask Synthesis provider slot to be configured.", Category: "ask"},
	{Key: service.SettingAskRerankEnabled, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Re-score the ask neighborhood with the Reranker provider before synthesis, reordering candidates by model-judged query relevance so the most relevant memories lead the prompt. Off by default; requires the Reranker provider slot. Tolerates a non-deterministic (LLM judge) reranker since ask is already an LLM call.", Category: "ask"},
	{Key: service.SettingAskSynthesisTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the ask synthesis call (0.0 to 1.0). Low values keep the answer grounded in the recalled memories.", Category: "ask", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingAskSynthesisMaxTokens, Type: "number", DefaultValue: json.RawMessage(`4096`), Description: "Maximum tokens the ask synthesis call may generate for one answer.", Category: "ask", Min: ptrF(256), Max: ptrF(32768), Step: ptrF(256)},
	{Key: service.SettingAskRecallCandidates, Type: "number", DefaultValue: json.RawMessage(`12`), Description: "How many top recall hits ask pulls as the candidate pool for the synthesis neighborhood (before the relevance floor trims it).", Category: "ask", Min: ptrF(1), Max: ptrF(50), Step: ptrF(1)},
	{Key: service.SettingAskNeighborhoodMinScoreRatio, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Relevance floor: a recall candidate joins the neighborhood only if its fused recall score is at least this fraction of the top hit's. Drops the off-topic tail recall returns to fill its limit, so the neighborhood adapts to how many memories genuinely match. 0 disables.", Category: "ask", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingAskGraphDepth, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "How many graph steps ask follows out from each candidate to gather connected memories recall may have missed. Each connected memory is relevance-gated against the query (see ask.expansion.cosine_floor) before it can join the neighborhood. 0 disables graph expansion.", Category: "ask", Min: ptrF(0), Max: ptrF(5), Step: ptrF(1)},
	{Key: service.SettingAskSiblingsPerCandidate, Type: "number", DefaultValue: json.RawMessage(`3`), Description: "How many same-project, tag-overlapping sibling memories ask considers per candidate. Each is relevance-gated against the query (see ask.expansion.cosine_floor), so tag overlap alone never admits a sibling. 0 disables.", Category: "ask", Min: ptrF(0), Max: ptrF(20), Step: ptrF(1)},
	{Key: service.SettingAskExpansionCosineFloor, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Relevance gate for graph/sibling expansion: a connected memory joins the neighborhood only if its cosine to the query embedding is at least this. Keeps connected-but-off-topic memories out while admitting genuinely related context. Tune to your embedder band (qwen3-embedding strong matches ~0.55+).", Category: "ask", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingAskNeighborhoodMaxMemories, Type: "number", DefaultValue: json.RawMessage(`20`), Description: "Hard cap on the number of memories ask packs into the neighborhood before the synthesis call, bounding prompt size and cost.", Category: "ask", Min: ptrF(1), Max: ptrF(200), Step: ptrF(1)},
	{Key: service.SettingAskConfidenceCosineFloor, Type: "number", DefaultValue: json.RawMessage(`0.35`), Description: "Confidence calibration floor: a cited source's vector cosine at or below this reads as no confidence. Tune to your embedder's similarity band (qwen3-embedding strong matches cluster ~0.55-0.71).", Category: "ask", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingAskConfidenceCosineCeiling, Type: "number", DefaultValue: json.RawMessage(`0.75`), Description: "Confidence calibration ceiling: a cited source's vector cosine at or above this reads as full confidence. Must be greater than the floor.", Category: "ask", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingAskDecompositionEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Break an aggregation/compare/classify question (\"which of my projects are C++ vs TypeScript\") into one focused retrieval sub-query per class before recall, so a dominant class can't bury a minority one in a single broad search. One small extra LLM call per ask on the Ask Synthesis provider; returns no sub-queries for ordinary questions, leaving recall unchanged. Under ask.enabled.", Category: "ask"},
	{Key: service.SettingAskDecompositionMaxSubqueries, Type: "number", DefaultValue: json.RawMessage(`4`), Description: "Maximum number of per-class sub-queries the decomposer may produce, bounding the extra recalls a single ask can fan out into.", Category: "ask", Min: ptrF(2), Max: ptrF(10), Step: ptrF(1)},
	{Key: service.SettingAskDecompositionMaxTokens, Type: "number", DefaultValue: json.RawMessage(`256`), Description: "Maximum tokens the decomposition call may generate. The output is a short JSON list of sub-queries, so this stays small.", Category: "ask", Min: ptrF(64), Max: ptrF(2048), Step: ptrF(64)},
	{Key: service.SettingAskDecompositionTemperature, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Sampling temperature for the decomposition call (0.0 to 1.0). 0 keeps sub-query derivation deterministic.", Category: "ask", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},

	// Pruning thresholds. Shared key between phase_pruning.go (active
	// relationship expiry pass) and phase_weights.go (mid-cycle expiry on
	// weight decay) so the two paths cannot drift.
	{Key: service.SettingDreamPruningRelationshipWeightThreshold, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Relationships whose weight drops below this are removed during pruning (0.0 to 1.0). Raise it to prune weak connections more aggressively.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamPruningEffectivelyZero, Type: "number", DefaultValue: json.RawMessage(`0.001`), Description: "Confidence at or below this is treated as zero when pruning. Catches values driven almost to zero by repeated penalties that an exact zero check would miss.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(0.1), Step: ptrF(0.0001)},

	// Transitive relationship discovery.
	{Key: service.SettingDreamTransitiveMinWeight, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "How strong a chain of two relationships (A to B and B to C) must be before a new A-to-C relationship is inferred (0.0 to 1.0, measured as the two weights multiplied). Keeps weak chains from creating noisy inferred links.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamTransitiveMaxPerCycle, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum inferred relationships created per cycle. Near the project's relationship cap the effective limit shrinks to the room left, so raising this alone won't help; raise the cap instead.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(10)},
	{Key: service.SettingDreamTransitiveNamespaceHardCap, Type: "number", DefaultValue: json.RawMessage(`1000000`), Description: "Maximum relationships a project may hold for the inference phase. As it fills, pruning starts removing the weakest inferred links (see the high- and low-water settings), and inference pauses once the cap is reached. The default is high enough not to limit normal use; lower it to enforce a per-project ceiling.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(10000000), Step: ptrF(100)},
	{Key: service.SettingDreamTransitiveNamespaceHighWater, Type: "number", DefaultValue: json.RawMessage(`0.95`), Description: "How full the relationship cap gets before pruning starts trimming the weakest inferred links, as a fraction (0.95 = 95% full). Must be higher than the low-water mark.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamTransitiveNamespaceLowWater, Type: "number", DefaultValue: json.RawMessage(`0.80`), Description: "How far down pruning trims the relationship count once it starts, as a fraction of the cap (0.80 = down to 80% full). Lower values free up more room and prune less often, but drop more inferred links each time. Must be lower than the high-water mark.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamTransitiveRelations, Type: "json", DefaultValue: json.RawMessage(`["part of","is part of","contains","located in","is located in","depends on","subclass of","is a","type of","ancestor of","descendant of","broader than","narrower than"]`), Description: "Which relationship types may be chained into an inferred link. A relationship is only inferred (A to B and B to C makes A to C) when both steps share the SAME relation AND it appears in this JSON list of relation labels. Defaults to genuinely transitive (containment/hierarchy) relations; relations like 'wife of' or 'related to' are intentionally excluded so they never chain. Labels are matched after normalizing case, spacing, and dashes.", Category: "dreaming_performance"},
	{Key: service.SettingDreamTransitiveMaxFanout, Type: "number", DefaultValue: json.RawMessage(`25`), Description: "Maximum number of inferred links a single intermediate entity may spread for one relation in a cycle. Caps blast radius through highly connected 'hub' entities so one node cannot fan a relation out to its whole neighborhood, even if a non-transitive relation slips into the allowed list.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},

	// Weight-adjustment knobs. Each governs one term in the per-cycle
	// recompute formula. Lower decay_factor / higher dead_source_multiplier
	// extend an edge's lifetime; raise tier2_multiplier to give co-mention
	// support more pull on rising weights.
	{Key: service.SettingDreamWeightTier2Multiplier, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "How much weight is given to indirect support for a relationship, where a memory mentions both endpoints but didn't directly create the link (0.0 to 2.0). Direct support always counts in full.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamWeightDecayWindowDays, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How old a relationship must be before its weight starts to decay. Newer ones are left alone.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(3650), Step: ptrF(1)},
	{Key: service.SettingDreamWeightDecayFactor, Type: "number", DefaultValue: json.RawMessage(`0.95`), Description: "How much weight a relationship keeps per decay step once it's old enough (0.0 to 1.0). With the defaults, a year-old link settles around 60% of its original weight.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamWeightDecayMaxPeriods, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Maximum number of decay steps applied to any one relationship, so very old links settle at a floor instead of fading to nothing.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamWeightDeadSourceMultiplier, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Extra weight reduction when the memory that created a relationship is gone and no remaining memory backs it up (0.0 to 1.0). Pushes unsupported links toward pruning faster than normal decay.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamWeightCeiling, Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Maximum weight any relationship can reach. Lowering it doesn't immediately clamp existing links; each drops to the new ceiling the next time its weight is recomputed.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(10), Step: ptrF(0.1)},

	// Consolidation phase clustering and sampling.
	{Key: service.SettingDreamConsolidationAlignmentSampleSize, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How many existing memories are sampled to judge whether a new synthesis fits the rest of the project. Larger samples judge more accurately but cost more tokens per cluster.", Category: "dreaming_consolidation", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamConsolidationClusterOverlapThreshold, Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "Word-overlap grouping threshold used only when cluster mode is 'lexical' (0.0 to 1.0). Higher values make smaller, tighter groups.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamConsolidationClusterMode, Type: "enum", DefaultValue: json.RawMessage(`"cosine"`), Description: "How consolidation groups related memories. 'cosine' groups by embedding similarity, producing semantically coherent syntheses; 'lexical' uses the older word-overlap heuristic. Cosine falls back to lexical for any memory without a stored vector.", Category: "dreaming_consolidation", EnumValues: []string{"cosine", "lexical"}},
	{Key: service.SettingDreamConsolidationClusterCosineThreshold, Type: "number", DefaultValue: json.RawMessage(`0.65`), Description: "Minimum embedding cosine similarity for two memories to join the same consolidation cluster in cosine mode (0.0 to 1.0). Higher values make tighter, more on-topic syntheses.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},

	// LLM call temperatures.
	{Key: service.SettingDreamSynthesisTemperature, Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "Sampling temperature for the call that merges a cluster of memories into one summary (0.0 to 2.0). A little higher than the judge calls, since the output is prose and benefits from some flexibility.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamAlignmentTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the call that scores how well a synthesis fits existing memories (0.0 to 2.0). Kept low for consistent scores.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamNoveltyJudgeTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the novelty-audit call (0.0 to 2.0). Kept low for consistent results.", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamContradictionTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the contradiction-detection call (0.0 to 2.0). Kept low for consistent results.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEnrichmentConflictTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the call that resolves conflicts during enrichment (0.0 to 2.0). Kept low for consistent results.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEnrichmentIngestionDecisionTemperature, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Sampling temperature for the ingestion-decision call (0.0 to 2.0). Default 0 makes the add/update/delete/skip choice as consistent as possible.", Category: "enrichment_ingestion", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},

	// Heartbeat tick timeout for the dream runner.
	{Key: service.SettingDreamHeartbeatTickTimeoutSeconds, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "How long a dream cycle waits to record that it's still alive before skipping that update, in seconds. Under heavy database contention this turns a stall into a missed beat rather than a frozen cycle.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},

	// Stuck-scan caps. Distinct keys for dreaming and enrichment so the
	// two can be tuned independently.
	{Key: service.SettingDreamStuckScanLimit, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum stuck dream cycles examined in one recovery scan, so a large backlog can't hold up other writes.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(1000000), Step: ptrF(100)},
	{Key: service.SettingEnrichmentStuckScanLimit, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum stuck enrichment jobs examined in one recovery scan, so a large backlog can't hold up other writes.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(1000000), Step: ptrF(100)},
}

// ptrF returns a pointer to v. Convenience helper for the *float64 Min/Max/Step
// fields on SettingSchema so the literals stay readable inline. Marker name
// keeps the registry table from getting noisy with longer helper invocations.
func ptrF(v float64) *float64 { p := v; return &p }

// SettingsSchemas returns a copy of the canonical registry. The slice is
// allocated freshly on each call so callers may sort, filter, or iterate
// without affecting subsequent calls. Used by the bootstrap seeder
// (service.SeedSettingsDefaults) and by the cascade-completeness test.
func SettingsSchemas() []api.SettingSchema {
	out := make([]api.SettingSchema, len(settingsSchemas))
	copy(out, settingsSchemas)
	return out
}

// promptSchemaEntries describes the dreaming-phase prompts surfaced through
// the schema endpoint. Their DefaultValue is filled in at init time from
// service.GetDefault so the value the UI shows as the "default" cannot drift
// from the value the runtime cascade falls back to in service.Resolve.
var promptSchemaEntries = []api.SettingSchema{
	// Each phase exposes one tunable system prompt: the full static instruction
	// (role, rules, and the complete output contract/schema), sent as the system
	// message. The dynamic memory data is wrapped by a hardcoded per-phase code
	// template into the user message and is not a setting; the system prompt is
	// the only operator-tunable LLM template.
	{Key: service.SettingDreamContradictionSystemPrompt, Type: "prompt", Description: "System prompt for the contradiction-detection phase: the task plus the JSON output schema (`contradicts`, `winner` \"a\"/\"b\"/\"tie\"/null, `explanation`). The two statements are supplied as the user message.", Category: "dreaming_prompts"},
	{Key: service.SettingDreamSynthesisSystemPrompt, Type: "prompt", Description: "System prompt for the consolidation synthesis phase: merge the sources into one synthesis with no commentary and output only the synthesized text. The source content is supplied as the user message.", Category: "dreaming_prompts"},
	{Key: service.SettingDreamAlignmentSystemPrompt, Type: "prompt", Description: "System prompt for alignment scoring: the scoring task plus the JSON output schema (`alignment` float in [-1.0, 1.0], `reasoning`). The synthesis and evidence are supplied as the user message.", Category: "dreaming_prompts"},
	{Key: service.SettingDreamNoveltyJudgeSystemPrompt, Type: "prompt", Description: "System prompt for the novelty audit: what counts as a novel fact, the hard rules, and the JSON output schema (`novel_facts` array, empty when duplicative). The synthesis and sources are supplied as the user message.", Category: "dreaming_prompts"},
	{Key: service.SettingIngestionDecisionSystemPrompt, Type: "prompt", Description: "System prompt for the ingestion-decision phase: the ADD/UPDATE/DELETE/NONE choices, hard rules, and the JSON output schema {\"operation\":\"ADD|UPDATE|DELETE|NONE\",\"target_id\":\"uuid|null\",\"rationale\":\"string\"}. The new memory and candidate list are supplied as the user message.", Category: "enrichment_prompts"},
	{Key: service.SettingFactSystemPrompt, Type: "prompt", Description: "System prompt for fact extraction: the fact JSON shape, the hard rules, and the \"return only JSON\" contract. The input content is supplied as the user message.", Category: "enrichment_prompts"},
	{Key: service.SettingEntitySystemPrompt, Type: "prompt", Description: "System prompt for entity extraction (pass 1): the entities JSON shape and the \"return only JSON\" contract. The input content is supplied as the user message. Relationships are extracted by a separate pass (see the Relationship Extraction prompt).", Category: "enrichment_prompts"},
	{Key: service.SettingRelationshipSystemPrompt, Type: "prompt", Description: "System prompt for relationship extraction (pass 2): the relationships JSON shape, the closed relation vocabulary, and the \"return only JSON\" contract. The input content plus the entity names extracted in pass 1 are supplied as the user message.", Category: "enrichment_prompts"},
	{Key: service.SettingQueryAugmentSystemPrompt, Type: "prompt", Description: "System prompt for the query-augmentation phase: the task and the strict JSON-array output rules. The requested query count and memory content are supplied as the user message.", Category: "enrichment_prompts"},
	{Key: service.SettingAskSynthesisSystemPrompt, Type: "prompt", Description: "System prompt for the ask tool: answer only from the supplied memory neighborhood, cite memory ids inline, say \"Not in neighborhood.\" when the answer is absent, no commentary. The question and the tagged neighborhood are supplied as the user message.", Category: "ask_prompts"},
	{Key: service.SettingAskDecompositionSystemPrompt, Type: "prompt", Description: "System prompt for the ask query-decomposition step: rewrite an aggregation/compare/classify question into one focused retrieval sub-query per class as a JSON {\"subqueries\":[...]} list, or an empty list when no breakdown is warranted. The question is supplied as the user message.", Category: "ask_prompts"},
	{Key: service.SettingRerankJudgeSystemPrompt, Type: "prompt", Description: "System prompt for the LLM-judge reranker: instructs the model to output a single relevance number in [0,1] for a (query, document) pair. Only used when the Reranker slot is a generative chat model (detected method \"judge\"), not a cross-encoder. The query and document are supplied as the user message.", Category: "ranking_prompts"},
}

func init() {
	for _, entry := range promptSchemaEntries {
		def, ok := service.GetDefault(entry.Key)
		if !ok {
			// Defensive: a registered prompt schema with no runtime default
			// would make the editor's "reset to default" reset to an empty
			// string. Surface the inconsistency at startup rather than at
			// first edit.
			panic("settings_store: no service default registered for prompt key " + entry.Key)
		}
		raw, err := json.Marshal(def)
		if err != nil {
			panic("settings_store: failed to encode default for " + entry.Key + ": " + err.Error())
		}
		entry.DefaultValue = raw
		settingsSchemas = append(settingsSchemas, entry)
	}

	// Numeric/boolean/enum consistency check. For every schema entry whose
	// type is one of these, the schema's DefaultValue (JSON-encoded) must
	// agree with the parsed value of settingDefaults[key]. Drift between
	// the two is a load-bearing bug; it caused the contradictionCap=30
	// vs schema=2000 split that silently degraded production cycles when
	// the settings repo was briefly unavailable. Surface the inconsistency
	// at process start, not at first cache miss.
	for _, entry := range settingsSchemas {
		switch entry.Type {
		case "number", "boolean", "enum":
		default:
			continue
		}
		def, ok := service.GetDefault(entry.Key)
		if !ok {
			panic("settings_store: schema entry " + entry.Key + " has no runtime default registered in service.settingDefaults")
		}
		if !defaultsAgree(entry.Type, entry.DefaultValue, def) {
			panic("settings_store: default mismatch for " + entry.Key +
				": schema=" + string(entry.DefaultValue) + " runtime=" + def)
		}
	}
}

// defaultsAgree compares a schema's JSON-encoded DefaultValue to the runtime
// default string registered in settingDefaults. Numbers compare numerically
// (1 == 1.0), booleans compare structurally, enums compare as quoted strings.
func defaultsAgree(typ string, schema json.RawMessage, runtime string) bool {
	switch typ {
	case "number":
		var a float64
		if err := json.Unmarshal(schema, &a); err != nil {
			return false
		}
		b, err := strconv.ParseFloat(strings.TrimSpace(runtime), 64)
		if err != nil {
			return false
		}
		return a == b
	case "boolean":
		var a bool
		if err := json.Unmarshal(schema, &a); err != nil {
			return false
		}
		switch strings.TrimSpace(runtime) {
		case "true", "1":
			return a
		case "false", "0":
			return !a
		default:
			return false
		}
	case "enum":
		var a string
		if err := json.Unmarshal(schema, &a); err != nil {
			return false
		}
		return a == strings.TrimSpace(runtime)
	}
	return false
}

func (s *SettingsAdminStore) GetSettingsSchema(ctx context.Context) ([]api.SettingSchema, error) {
	return settingsSchemas, nil
}

func (s *SettingsAdminStore) GetSettingsGroups(ctx context.Context) ([]api.SettingGroup, error) {
	return settingsGroups, nil
}
