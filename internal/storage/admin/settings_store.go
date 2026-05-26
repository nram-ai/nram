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
// instead of scanning ListSettings — used by cross-key invariant validation
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
	{Key: "enrichment.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Enable automatic memory enrichment. When true, every newly stored memory is enqueued for entity/relationship extraction (subject to provider availability). When false, the worker pool idles without claiming jobs and the queue accumulates until re-enabled.", Category: "enrichment"},
	{Key: "enrichment.batch_size", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Number of memories to process per enrichment batch", Category: "enrichment", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: service.SettingMemoryDefaultConfidence, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Confidence assigned to a newly-stored memory when the caller does not specify one (0.0-1.0). Governs every write path: REST store, batch store, content extraction, and bulk import.", Category: "memory", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingMemoryDefaultImportance, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Importance assigned to a newly-stored memory when the caller does not specify one (0.0-1.0). Governs every write path: REST store, batch store, content extraction, and bulk import.", Category: "memory", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "memory.soft_delete_retention_days", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days after soft-delete before a memory row is hard-purged (with its vectors)", Category: "memory", Min: ptrF(1), Max: ptrF(3650), Step: ptrF(1)},
	{Key: "api.rate_limit_rps", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "API rate limit (requests per second per user)", Category: "api", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: "api.rate_limit_burst", Type: "number", DefaultValue: json.RawMessage(`20`), Description: "API rate limit burst size", Category: "api", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: "qdrant.addr", Type: "string", DefaultValue: json.RawMessage(`""`), Description: "Qdrant gRPC address as host:port.", Category: "qdrant", RequiresRestart: true, OmitFromResetAll: true},
	{Key: "qdrant.api_key", Type: "secret", DefaultValue: json.RawMessage(`""`), Description: "API key for Qdrant authentication.", Category: "qdrant", RequiresRestart: true, OmitFromResetAll: true},
	{Key: "qdrant.use_tls", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable TLS for the Qdrant gRPC connection.", Category: "qdrant", RequiresRestart: true},
	{Key: "qdrant.pool_size", Type: "number", DefaultValue: json.RawMessage(`3`), Description: "Number of gRPC connections in the pool (1 = no pool).", Category: "qdrant", RequiresRestart: true, Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: "qdrant.keepalive_time", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Seconds between keepalive pings (0 = 10s default, -1 = disabled).", Category: "qdrant", RequiresRestart: true, Min: ptrF(-1), Max: ptrF(3600), Step: ptrF(1)},
	{Key: "qdrant.keepalive_timeout", Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Seconds to wait for a keepalive response before closing the connection.", Category: "qdrant", RequiresRestart: true, Min: ptrF(1), Max: ptrF(60), Step: ptrF(1)},
	// HNSW (pure-Go SQLite-backed vector index) tuning. AppliesToBackend
	// scopes the UI to SQLite-only deployments; on Postgres+pgvector or
	// Qdrant, these knobs have no effect.
	{Key: service.SettingHNSWM, Type: "number", DefaultValue: json.RawMessage(`16`), Description: "Maximum neighbours per upper layer in the HNSW graph. Baked into each index at construction time; changes apply only to newly-built indexes. Higher values improve recall at the cost of larger indexes and slower writes.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(4), Max: ptrF(128), Step: ptrF(2)},
	{Key: service.SettingHNSWEfConstruction, Type: "number", DefaultValue: json.RawMessage(`200`), Description: "Construction-time candidate pool size for the HNSW graph. Baked into each index at construction time. Higher values improve recall at the cost of slower index builds.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(10), Max: ptrF(2000), Step: ptrF(10)},
	{Key: service.SettingHNSWEfSearch, Type: "number", DefaultValue: json.RawMessage(`50`), Description: "Search-time candidate pool size. Higher values improve recall at the cost of slower searches. Read once at boot when the HNSW cache is constructed.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(10), Max: ptrF(2000), Step: ptrF(10)},
	{Key: service.SettingHNSWMaxLoadedIndexes, Type: "number", DefaultValue: json.RawMessage(`64`), Description: "Maximum HNSW indexes held in memory before LRU eviction. Each loaded index pins its full graph in RAM. Read once at boot.", Category: "hnsw", RequiresRestart: true, AppliesToBackend: []string{storage.BackendSQLite}, Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: "dreaming.enabled", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable background dreaming (memory consolidation and graph improvement)", Category: "dreaming"},
	{Key: "dreaming.max_tokens_per_cycle", Type: "number", DefaultValue: json.RawMessage(`1024000`), Description: "Maximum total tokens per dream cycle across all phases", Category: "dreaming", Min: ptrF(1000), Max: ptrF(100000000), Step: ptrF(1000)},
	{Key: "dreaming.max_tokens_per_call", Type: "number", DefaultValue: json.RawMessage(`2048`), Description: "Maximum tokens for any single LLM call during dreaming", Category: "dreaming", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: "dreaming.cooldown_seconds", Type: "number", DefaultValue: json.RawMessage(`300`), Description: "Seconds to wait after the last user change before dreaming (prevents dreaming on partial data)", Category: "dreaming", Min: ptrF(0), Max: ptrF(86400), Step: ptrF(60)},
	{Key: "dreaming.min_interval_seconds", Type: "number", DefaultValue: json.RawMessage(`600`), Description: "Minimum seconds between dream cycles for the same project", Category: "dreaming", Min: ptrF(60), Max: ptrF(2592000), Step: ptrF(300)},
	{Key: "dreaming.initial_confidence", Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "Starting confidence for dream-synthesized memories (0.0-1.0)", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.supersession_threshold", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Confidence level at which a synthesis supersedes its source memories (0.0-1.0)", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.log_retention_days", Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Days to retain detailed dream logs before compressing to summaries", Category: "dreaming", Min: ptrF(1), Max: ptrF(3650), Step: ptrF(1)},
	{Key: "dreaming.novelty.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run the novelty audit on dream syntheses; reject low-novelty results", Category: "dreaming_novelty"},
	{Key: "dreaming.novelty.embed_high_threshold", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Cosine similarity at or above which a synthesis is auto-rejected as duplicative of a source (0.0-1.0)", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "dreaming.novelty.embed_low_threshold", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Cosine similarity below which a synthesis is auto-accepted without running the LLM judge (0.0-1.0)", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "dreaming.novelty.judge_max_tokens", Type: "number", DefaultValue: json.RawMessage(`512`), Description: "Maximum tokens the novelty judge LLM call may produce", Category: "dreaming_novelty", Min: ptrF(64), Max: ptrF(8192), Step: ptrF(64)},
	{Key: "dreaming.novelty.backfill_per_cycle", Type: "number", DefaultValue: json.RawMessage(`500`), Description: "Number of historical dream rows audited per cycle by the novelty backfill sweep", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: "dreaming.novelty.backfill_embed_high_threshold", Type: "number", DefaultValue: json.RawMessage(`0.93`), Description: "More aggressive auto-reject threshold used only by the backfill sweep on historical rows (0.0-1.0, 0 disables the override)", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "dreaming.consolidation.audit_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.35`), Description: "Relative weight of the novelty audit sub-phase within consolidation (0.0-1.0). Each sub-phase's slice = parent_remaining * weight / sum_of_remaining_sub_phase_weights, so unspent budget from earlier sub-phases automatically flows to later ones.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.consolidation.reinforce_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.35`), Description: "Relative weight of the reinforcement sub-phase within consolidation (0.0-1.0). Each sub-phase's slice = parent_remaining * weight / sum_of_remaining_sub_phase_weights, so unspent budget from earlier sub-phases automatically flows to later ones.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.consolidation.consolidate_budget_fraction", Type: "number", DefaultValue: json.RawMessage(`0.30`), Description: "Relative weight of the consolidation sub-phase within consolidation (0.0-1.0). Each sub-phase's slice = parent_remaining * weight / sum_of_remaining_sub_phase_weights, so unspent budget from earlier sub-phases automatically flows to later ones.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamEntityDedupFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Relative weight of the entity-dedup phase among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. SQL-only phase; default 0.0 means no per-phase slice — the phase shares the root budget and runs whenever the cycle has remaining tokens.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamEmbeddingBackfillFraction, Type: "number", DefaultValue: json.RawMessage(`0.10`), Description: "Relative weight of the embedding-backfill phase among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. Used by the embedder when re-embedding rows whose vectors are missing.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamParaphraseFraction, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Relative weight of the paraphrase-dedup sweep among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. Used by the embedder when probing nearest-neighbours.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamTransitiveFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Relative weight of the transitive relationship-discovery phase among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. SQL-only phase; default 0.0 means no per-phase slice.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamContradictionFraction, Type: "number", DefaultValue: json.RawMessage(`0.40`), Description: "Relative weight of the contradiction-detection phase among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. Caps how many tokens the LLM-judge pair walk can consume so consolidation isn't starved.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamConsolidationFraction, Type: "number", DefaultValue: json.RawMessage(`0.40`), Description: "Relative weight of the consolidation phase among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. The phase splits its slice further across audit/reinforce/consolidate sub-phases via the dreaming.consolidation.{audit,reinforce,consolidate}_budget_fraction settings.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamPruningFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Relative weight of the pruning phase among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. SQL-only phase; default 0.0 means no per-phase slice — pruning runs whenever the cycle has remaining tokens so confidence decay still fires.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamWeightAdjustFraction, Type: "number", DefaultValue: json.RawMessage(`0.0`), Description: "Relative weight of the weight-adjustment phase among LLM-spending phases (0.0-1.0). Each phase's slice = cycle_remaining * weight / sum_of_remaining_phase_weights, so unspent budget from earlier phases automatically flows to later ones. SQL-only phase; default 0.0 means no per-phase slice.", Category: "dreaming_phase_budget", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.cap_per_cycle", Type: "number", DefaultValue: json.RawMessage(`2000`), Description: "Maximum LLM pair-contradiction checks per dream cycle. Bump for faster drain during first-pass backfill on large namespaces, then restore.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(100)},
	{Key: "dreaming.contradiction.loser_haircut", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Multiplicative confidence haircut applied to the losing side of a contradiction (0.0-1.0). Smaller = harsher penalty. Diminishes on reaffirmation: effective = 1 - (1 - base) / detection_count.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.winner_haircut", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Multiplicative confidence haircut applied to the winning side of a contradiction (0.0-1.0). Acknowledges some uncertainty in any judgment. Same diminishing rule as the loser haircut.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.tie_haircut", Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Multiplicative confidence haircut applied to both sides when the LLM judge cannot pick a winner (0.0-1.0). Same diminishing rule.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "dreaming.contradiction.paraphrase_enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Auto-supersede near-duplicate memory pairs (cosine >= paraphrase_threshold) without calling the LLM judge. Lowers LLM cost and closes the paraphrase coverage gap that the contradiction judge intentionally leaves open.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.paraphrase_threshold", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Cosine similarity at or above which the contradictions phase treats a pair as paraphrases and auto-supersedes the lower-confidence side (0.0-1.0). Conservative high values minimize false positives.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamParaphraseEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run the paraphrase-dedup sweep. Catches user-source duplicates the contradiction phase's anchor walk leaves unpaired by running vector_store.Search(top-K) directly on every eligible memory.", Category: "dreaming_paraphrase"},
	{Key: service.SettingDreamParaphraseThreshold, Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Cosine similarity at or above which the sweep auto-supersedes the lower-confidence side of a pair (0.0-1.0). Conservative high values minimize false positives.", Category: "dreaming_paraphrase", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamParaphraseCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum memories visited per dream cycle by the paraphrase sweep. Residual is signalled when more candidates remain than the cap allowed.", Category: "dreaming_paraphrase", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: service.SettingDreamParaphraseTopK, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Top-K nearest neighbours probed per anchor in paraphrase dedup. K=1 (default) is a conservative starting point; raise to 3-5 once you've confirmed your vector store and provider can handle the per-cycle load.", Category: "dreaming_paraphrase", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamParaphraseStaleFetchMax, Type: "number", DefaultValue: json.RawMessage(`50000`), Description: "Maximum stale memories fetched per dream cycle by the paraphrase sweep. Bounds working-set memory by capping how many rows the SQL-level stale predicate returns. When the namespace's stale-row count exceeds this cap, the phase processes the oldest-stale subset and reports residual=true so the next cycle drains the rest.", Category: "dreaming_paraphrase", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(1000)},
	{Key: service.SettingDreamConsolidationStaleFetchMax, Type: "number", DefaultValue: json.RawMessage(`50000`), Description: "Maximum stale memories fetched per dream cycle by the consolidation phase. Bounds the candidate pool to memories whose consolidation_load_checked_at stamp is missing or older than updated_at. The older tail drains across cycles via residual signaling.", Category: "dreaming_consolidation", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(1000)},
	{Key: service.SettingDreamContradictionStaleFetchMax, Type: "number", DefaultValue: json.RawMessage(`50000`), Description: "Maximum stale memories fetched per dream cycle by the contradiction-detection phase. Bounds working-set memory when many memories lack the contradictions_checked_at stamp (e.g. first deploy on a large namespace).", Category: "dreaming_contradiction", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(1000)},
	{Key: service.SettingDreamPruningBatchSize, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Streaming batch size for the pruning phase. Pruning visits every memory each cycle (no stamp gating) for confidence decay; the namespace is iterated one batch at a time. Lower values reduce per-batch memory at the cost of more transactions per cycle.", Category: "dreaming_performance", Min: ptrF(10), Max: ptrF(100000), Step: ptrF(100)},
	{Key: service.SettingDreamEmbeddingBackfillEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run the embedding-backfill phase. Repairs rows whose embedding_dim is set but whose memory_vectors_<dim> row is missing: re-embeds when the embedder is healthy, clears embedding_dim otherwise.", Category: "dreaming_embedding_backfill"},
	{Key: service.SettingDreamEmbeddingBackfillCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum divergent rows repaired per dream cycle by the embedding-backfill phase. Bump to drain a large existing backlog faster, then restore.", Category: "dreaming_embedding_backfill", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(50)},
	{Key: service.SettingDreamingWeightSupportGain, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Multiplier alpha in the weight-adjustment phase: a relationship's weight is multiplied by 1 + alpha * (support - 1) when its supporting memories' summed confidence exceeds 1.0. Higher values let multi-memory attestation lift weights faster; the 2.0 cap and 0.95/30d decay bound the rise either way.", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamingWeightRecallReinforceDelta, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Additive weight bump applied to a relationship each time it surfaces in a user-facing recall (memory_recall include_graph or memory_graph). Clamped at the 2.0 ceiling by the SQL layer. Gated by reconsolidation.mode; throttled to one increment per relationship per recall.", Category: "dreaming", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "reconsolidation.mode", Type: "enum", DefaultValue: json.RawMessage(`"shadow"`), Description: "Reconsolidation mode: 'shadow' emits events without persisting; 'persist' writes confidence/access updates; 'off' disables reinforcement entirely", Category: "reconsolidation", EnumValues: []string{"shadow", "persist", "off"}},
	{Key: "reconsolidation.factor", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "Per-recall confidence boost applied to reinforced memories (0.0-1.0)", Category: "reconsolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "reconsolidation.decay_enabled", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable sleep-time confidence decay for memories not recalled recently", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_threshold_days", Type: "number", DefaultValue: json.RawMessage(`14`), Description: "Days since last recall before a memory starts losing confidence to decay", Category: "reconsolidation", Min: ptrF(1), Max: ptrF(365), Step: ptrF(1)},
	{Key: "reconsolidation.decay_rate_per_cycle", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "Confidence loss per dream cycle applied to decay-eligible memories (0.0-1.0)", Category: "reconsolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "reconsolidation.confidence_floor", Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Minimum confidence decay will not push below (0.0-1.0)", Category: "reconsolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: "recall.fusion.enabled", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable hybrid recall: parallel vector + BM25/tsvector retrieval fused via Reciprocal Rank Fusion. Requires migration 18 applied and a lexical searcher wired.", Category: "recall_fusion"},
	{Key: "recall.fusion.rrf_k", Type: "number", DefaultValue: json.RawMessage(`60`), Description: "RRF constant. Higher values flatten the head of each ranked list; 60 is the canonical Cormack-Clarke-Buettcher default.", Category: "recall_fusion", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: "recall.fusion.vector_weight", Type: "number", DefaultValue: json.RawMessage(`0.60`), Description: "Weight on each vector channel's RRF contribution (0.0-1.0). Together with lexical_weight, controls the relative pull of dense embedding vs sparse keyword evidence. Default 0.60 reflects a synthetic controlled experiment (internal/service/testdata/recall_contamination/, 2026-05-22): 60/40 widened the canonical-vs-contaminant margin without dropping canonical@1, while 50/50 hurt canonical@1 on lex-vulnerable queries. Validate against your own corpus before adopting in production.", Category: "recall_fusion", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: "recall.fusion.lexical_weight", Type: "number", DefaultValue: json.RawMessage(`0.40`), Description: "Weight on each lexical channel's RRF contribution (0.0-1.0). Raise to bias recall toward exact-token matches (entity names, version strings). Default 0.40 paired with vector_weight 0.60 per the 2026-05-22 synthetic probe sweep; revisit if your corpus has many short queries with weak token overlap.", Category: "recall_fusion", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingIngestionDecisionEnabled, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable LLM-driven ingestion decision (ADD/UPDATE/DELETE/NONE) on near-duplicate matches at enrichment time. When off, every memory is treated as ADD without an LLM call.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionShadow, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Shadow mode: compute and log the decision (op, top_score, match_count) but always behave as if it were ADD. Defaults to true so enabling the feature first observes its distribution before acting on UPDATE/DELETE.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Cosine similarity at or above which a candidate match is presented to the LLM judge (0.0-1.0). Below this, the new memory is treated as ADD without an LLM call.", Category: "enrichment_ingestion", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDedupThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Legacy enrichment-side dedup threshold. The cascade resolver prefers SettingIngestionDecisionThreshold; this key is the fallback when the ingestion-decision phase is disabled.", Category: "enrichment", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingExtractedFactGuardEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run the pre-insert paraphrase guard on extracted-fact children during enrichment. When a fact is too similar to its parent (or to a previously-accepted sibling in the same job), the child is suppressed and the fact's tags are merged into the parent.", Category: "enrichment"},
	{Key: service.SettingExtractedFactParaphraseThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Cosine similarity at or above which an extracted-fact child is treated as a paraphrase of its parent (0.0-1.0). Defaults match SettingDedupThreshold; falls back to it when this key is unset.", Category: "enrichment", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingExtractedFactBackfillBatchSize, Type: "number", DefaultValue: json.RawMessage(`100`), Description: "Number of parent memories enqueued per page by the extracted-fact paraphrase backfill admin endpoint. Lower to reduce DB load on large namespaces (>5k memories); raise to finish faster. Each enqueued job is processed independently by the existing enrichment worker pool, so progress is observable through the standard queue admin endpoints.", Category: "enrichment", Min: ptrF(10), Max: ptrF(1000), Step: ptrF(10)},
	{Key: service.SettingTokenRetention, Type: "number", DefaultValue: json.RawMessage(`365`), Description: "Days to retain rows in the token_usage table before the lifecycle sweep prunes them. Operators raise this for audit retention requirements; set to 0 to retain indefinitely.", Category: "usage", Min: ptrF(0), Max: ptrF(3650), Step: ptrF(1)},
	{Key: service.SettingTokenCostRates, Type: "json", DefaultValue: json.RawMessage(`[]`), Description: "Per-group token cost rates used to compute dollar breakdowns in the analytics panel. JSON array of {key, inputCostPer1k, outputCostPer1k} objects keyed by the group dimension shown in usage reports (provider, model, etc.). Edited globally by administrators via PUT /admin/settings; surfaced read-only to all other users via GET /v1/usage/cost_rates.", Category: "usage"},
	{Key: service.SettingIngestionDecisionTopK, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "Maximum number of candidate matches presented to the LLM judge.", Category: "enrichment_ingestion", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingIngestionDecisionModel, Type: "string", DefaultValue: json.RawMessage(`""`), Description: "LLM model name for the ingestion decision. Empty falls back to the fact-extraction provider's model (this is a categorization task, a small model is fine).", Category: "enrichment_ingestion", OmitFromResetAll: true},

	{Key: service.SettingQueryAugmentEnabled, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable query augmentation: at enrichment time generate N paraphrased query forms per memory and prepend them to content before embedding. Off by default. Flip only after the canned recall regression set shows no contamination-probe regressions plus measurable improvement on 3+ of 7 stress angles. After flipping, use the Backfill button below to re-embed pre-flag memories.", Category: "enrichment_query_augment"},
	{Key: service.SettingQueryAugmentCount, Type: "number", DefaultValue: json.RawMessage(`4`), Description: "Number of paraphrased query forms generated per memory. 3-5 is the validated range; below 3 reduces phrasing coverage, above 5 starts crowding the embedding context.", Category: "enrichment_query_augment", Min: ptrF(1), Max: ptrF(10), Step: ptrF(1)},
	{Key: service.SettingQueryAugmentModel, Type: "string", DefaultValue: json.RawMessage(`""`), Description: "LLM model name for query augmentation. Empty falls back to the fact-extraction provider's model (paraphrasing is well within a small model's capability).", Category: "enrichment_query_augment", OmitFromResetAll: true},
	{Key: service.SettingQueryAugmentMaxInputChars, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Byte cap on the concatenated (queries + separator + content) string sent to the embedder. 0 means no cap. Set this to ~90% of the embedding model's context window in characters to defend against silent truncation; when the cap fires the content tail is truncated and all generated queries are preserved in the embed input.", Category: "enrichment_query_augment", Min: ptrF(0), Max: ptrF(200000), Step: ptrF(512)},
	{Key: service.SettingQueryAugmentMaxTokens, Type: "number", DefaultValue: json.RawMessage(`2048`), Description: "Maximum completion tokens for the query-augmentation LLM call. Raise when the LLM emits truncated JSON (parse-failure logs with raw_len approaching the token cap and `unexpected end of JSON input`). 2048 matches the Prompt Templates Test button's hardcoded budget and gives small reasoning-mode models ample headroom for their internal preamble plus 4-5 paraphrased queries. The truncation-prefix recovery path will salvage whatever cleanly-decoded queries the model emitted before the cut, but bumping the cap is the cleaner fix. Hot-reloadable.", Category: "enrichment_query_augment", Min: ptrF(128), Max: ptrF(8192), Step: ptrF(128)},
	{Key: service.SettingRankWeightSim, Type: "number", DefaultValue: json.RawMessage(`0.50`), Description: "Weight on cosine similarity in the recall ranking formula (0.0-1.0). The dominant term: how strongly query-to-memory semantic match contributes to the score. Lower to give other signals more pull.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightRec, Type: "number", DefaultValue: json.RawMessage(`0.15`), Description: "Weight on recency in the recall ranking formula (0.0-1.0). Recency decays as exp(-decay_per_hour * hours_since_creation), so this term favours fresh memories without sharply discarding older ones.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightImp, Type: "number", DefaultValue: json.RawMessage(`0.10`), Description: "Weight on Memory.Importance in the recall ranking formula (0.0-1.0). Importance is operator-set per memory; bump this to honor manual curation more strongly.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightFreq, Type: "number", DefaultValue: json.RawMessage(`0.00`), Description: "Weight on access-count frequency (log-normalized to the result set) in the recall ranking formula (0.0-1.0). Default 0 because reconsolidation already drives Memory.Confidence on every recall, so frequency double-counts the same signal. Re-enable for callers that bypass the reconsolidation hook.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightGraph, Type: "number", DefaultValue: json.RawMessage(`0.20`), Description: "Weight on graph-traversal relevance in the recall ranking formula (0.0-1.0). Boosts memories connected to entities mentioned in the query through the knowledge graph.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightConf, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Weight on Memory.Confidence in the recall ranking formula (0.0-1.0). Confidence is reinforced on each recall and decayed during dream cycles, so this term elevates well-used, well-aligned memories. Start small (0.05) and raise after the confidence distribution stabilises.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightOrigin, Type: "number", DefaultValue: json.RawMessage(`0.00`), Description: "Weight on the project-affinity term: candidates whose home namespace is the recall's primary project get this added to their score (0.0-1.0). Default 0 leaves ranking math unchanged. Raise to lift project-specific memories above otherwise-equivalent globals when a harness scopes recall to a project. Operators tune per project via the ranking_weights override on project settings.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRankWeightMmr, Type: "number", DefaultValue: json.RawMessage(`0.75`), Description: "MMR redundancy-aware rerank lambda (0.0-1.0). Trades off relevance against similarity to already-selected candidates after composite scoring. 1.0 disables the rerank (pure relevance order); 0.0 also disables the rerank (pure-diversity mode is not implemented and would surface anti-correlated noise); 0.7-0.8 is the standard mild-nudge range. Default 0.75 demotes near-identical siblings without regressing single-fact lookups (no sibling to demote against). Project-overridable via mmr_lambda on project ranking_weights settings.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingRecallNamespaceQuotaProjectMin, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Minimum number of result slots reserved for primary-project candidates in the final ranked set. When >0, the post-sort truncation guarantees this many primary-project memories appear (when that many pass the threshold), with the remaining slots filled by globals or shared memories in score order. Default 0 preserves the score-only truncation.", Category: "ranking", Min: ptrF(0), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingRecallFusionNormalizePerChan, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "When hybrid recall fusion is enabled, scale each per-namespace channel's RRF contribution by 1/len(channel) so a deep corpus (global) does not crowd out a sparse corpus (a small project). Default false keeps the pre-feature math.", Category: "recall_fusion"},

	// Enrichment worker pool tuning. Hot-reloadable knobs are read on every
	// worker iteration; restart-required knobs are read once when the pool
	// is constructed at server start.
	{Key: service.SettingEnrichmentWorkerBatchClaimSize, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Maximum jobs claimed per worker iteration. Each claim produces one shared embed call across the batch. Default 1 keeps work granular and predictable — safe for single-GPU local providers (Ollama, llama.cpp). Raise (e.g. to 16) only when your provider can sustain larger concurrent embed batches.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerPreEmbedConcurrency, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Per-batch fan-out cap for fact and entity LLM calls. Default 1 means each job processes its extraction calls sequentially — safe for single-GPU local providers. Raise (e.g. to 4) only when your provider has spare concurrency capacity; concurrent calls to a 1-GPU Ollama backend queue at the model level and look like deadlocks.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerEmbedTimeoutSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Per-call timeout for the shared embed HTTP call inside a worker batch.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerEmbedInputCap, Type: "number", DefaultValue: json.RawMessage(`256`), Description: "Maximum inputs per embedding provider call; larger batches are chunked. Conservative vs OpenAI's 2048-input limit to account for per-input token ceilings on smaller providers.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(8192), Step: ptrF(64)},
	{Key: service.SettingEnrichmentWorkerBreakerEscalateSeconds, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How long a provider circuit breaker must remain open before worker logs escalate from INFO (operational warmup) to ERROR (sustained outage).", Category: "enrichment_performance", Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingEnrichmentWorkerMaxBackoffSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Maximum sleep between empty polls. Workers back off exponentially up to this cap when the queue is idle.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerCountSQLite, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Number of concurrent enrichment workers when the backend is SQLite. A single writer makes multiple workers pointless on SQLite.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(64), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerCountPostgres, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Number of concurrent enrichment workers when the backend is Postgres. Default 1 means one worker pool goroutine, which is safe for any provider. Raise (e.g. to 2-4) only when your LLM and embed providers can each handle parallel calls without queuing — concurrent calls to a 1-GPU Ollama backend queue at the model level and produce apparent deadlocks. Each worker holds its own LLM and embed slot.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(128), Step: ptrF(1)},
	{Key: service.SettingEnrichmentWorkerPollIntervalSeconds, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How often idle workers poll for jobs, in seconds. Read once at pool start.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentPoolTickIntervalSeconds, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "How often the enrichment worker pool publishes enrichment.pool.tick events for the admin UI's live banner (in-flight count, oldest-claim age, stage breakdown). One tick per pool, not per job — cheap. Read once at pool start.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentIngestionRationaleMaxLen, Type: "number", DefaultValue: json.RawMessage(`500`), Description: "Maximum characters retained from the ingestion-decision rationale before truncation when stored on memory metadata.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(10000), Step: ptrF(10)},
	{Key: service.SettingEnrichmentHeartbeatSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How often each enrichment worker stamps heartbeat_at on every job it currently holds, in seconds. Read once at pool start. Smaller values give the admin UI tighter 'live' signal and let the stuck-job sweeper detect dead workers faster, at the cost of more UPDATE traffic on enrichment_queue.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingEnrichmentStuckThreshold, Type: "number", DefaultValue: json.RawMessage(`1800`), Description: "Threshold above which a claimed (status='processing') enrichment job is treated as stuck and auto-requeued by the StuckJobSweeper, based on now() - updated_at. Must exceed the longest legitimate batch runtime so a slow LLM call is not mistaken for a dead worker. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},
	{Key: service.SettingEnrichmentStuckSweep, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How often the stuck-job sweeper scans for claimed enrichment jobs past enrichment.stuck_threshold_seconds and auto-requeues them, in seconds. Read once at sweeper start.", Category: "enrichment_performance", RequiresRestart: true, Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingEnrichmentClaimMaxAge, Type: "number", DefaultValue: json.RawMessage(`7200`), Description: "Backstop cap on claimed_at age. The stuck-job sweeper requeues any in-flight row whose claimed_at exceeds this duration regardless of updated_at, catching wedged workers whose heartbeat is still ticking but whose actual work has stopped progressing. Must comfortably exceed the longest legitimate batch runtime; the cap is the hard wall when heartbeat-based detection fails. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},

	// Fact / entity extraction LLM-call tunables. All hot-reloadable, all
	// resolved per call by both ExtractionService (sync) and WorkerPool
	// (async). repeat_penalty / top_k / min_p are Ollama-only — strict
	// OpenAI endpoints never see them, gated at the OpenAIProvider layer.
	{Key: service.SettingFactExtractionMaxTokens, Type: "number", DefaultValue: json.RawMessage(`4096`), Description: "Maximum completion tokens for the fact-extraction LLM call. Raise when high-density inputs (research, design docs) hit finish_reason=length. Bounded by the model's num_ctx. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingEntityExtractionMaxTokens, Type: "number", DefaultValue: json.RawMessage(`4096`), Description: "Maximum completion tokens for the entity-extraction LLM call. Raise when entity-dense inputs hit finish_reason=length. Bounded by the model's num_ctx. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(128), Max: ptrF(131072), Step: ptrF(128)},
	{Key: service.SettingFactExtractionRepeatPenalty, Type: "number", DefaultValue: json.RawMessage(`1.15`), Description: "Repetition penalty for the fact-extraction call. Sent only to Ollama providers (ignored on strict OpenAI endpoints). 1.0 = no penalty; 1.10–1.20 empirically suppresses degenerate token loops on small qwen models without distorting clean output. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0.5), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEntityExtractionRepeatPenalty, Type: "number", DefaultValue: json.RawMessage(`1.15`), Description: "Repetition penalty for the entity-extraction call. Same rules and Ollama-only gating as fact_extraction.repeat_penalty. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0.5), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingFactExtractionTopK, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Top-K sampling for the fact-extraction call (Ollama-only). 0 disables (field omitted from request); typical Ollama values are 20–40. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(200), Step: ptrF(1)},
	{Key: service.SettingEntityExtractionTopK, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Top-K sampling for the entity-extraction call (Ollama-only). 0 disables (field omitted from request); typical Ollama values are 20–40. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(200), Step: ptrF(1)},
	{Key: service.SettingFactExtractionMinP, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Minimum-probability cutoff for the fact-extraction call (Ollama-only). 0 disables (field omitted from request); typical Ollama values are 0.05–0.10. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingEntityExtractionMinP, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Minimum-probability cutoff for the entity-extraction call (Ollama-only). 0 disables (field omitted from request); typical Ollama values are 0.05–0.10. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingFactExtractionSyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the fact-extraction call on the synchronous HTTP path (ExtractionService.Extract). Default 0.1 preserves pre-refactor behavior; the async worker path historically used 0.2. Operators may converge by setting both keys equal. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingFactExtractionAsyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.2`), Description: "Sampling temperature for the fact-extraction call on the async enrichment-worker path. Default 0.2 preserves pre-refactor behavior; the sync HTTP path historically used 0.1. Operators may converge by setting both keys equal. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEntityExtractionSyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the entity-extraction call on the synchronous HTTP path. Default 0.1 preserves pre-refactor behavior. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEntityExtractionAsyncTemperature, Type: "number", DefaultValue: json.RawMessage(`0.2`), Description: "Sampling temperature for the entity-extraction call on the async enrichment-worker path. Default 0.2 preserves pre-refactor behavior. Hot-reloadable.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},

	// Dreaming performance/tuning knobs that complement the existing
	// dreaming.* keys. Hot-reloadable per dream cycle.
	{Key: service.SettingDreamContradictionNeighbors, Type: "number", DefaultValue: json.RawMessage(`1`), Description: "Top-K nearest neighbours probed per anchor in the contradiction phase. K=1 (default) generates one candidate pair per anchor — minimal LLM-judge load, suitable for single-GPU local providers. Raise (e.g. to 4) only when your provider can sustain higher per-cycle judge volume; each unit increase multiplies LLM judge calls per cycle.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamEntityMergeThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Cosine similarity at or above which the entity-dedup phase merges two entities of the same type via the vector fallback (0.0-1.0). Conservative high values minimize spurious merges.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamSchedulerPollSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How often the dream scheduler checks for eligible projects, in seconds. Read once at scheduler start.", Category: "dreaming_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingDreamHeartbeatInterval, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How often the dream runner stamps heartbeat_at while a phase is executing, in seconds. Read at runner start. Smaller values give the admin UI tighter 'no recent activity' detection at the cost of more UPDATE traffic on dream_cycles.", Category: "dreaming_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},
	{Key: service.SettingDreamHeartbeatStale, Type: "number", DefaultValue: json.RawMessage(`120`), Description: "Threshold above which a running cycle is flagged 'no recent activity' in the admin UI, based on now() - heartbeat_at. Diagnostic only — does not gate Abandon. Hot-reloadable.", Category: "dreaming_performance", Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingDreamStuckThreshold, Type: "number", DefaultValue: json.RawMessage(`1800`), Description: "Threshold above which a running cycle is eligible for Abandon (manual or via the stuck-cycle sweeper), based on now() - updated_at. Must exceed the longest legitimate single-phase runtime; abandoning earlier could discard a cycle that's still making real progress. Hot-reloadable.", Category: "dreaming_performance", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},
	{Key: service.SettingDreamStuckSweep, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How often the stuck-cycle sweeper scans for running cycles past dreaming.stuck_threshold_seconds and auto-abandons them, in seconds. Read once at scheduler start.", Category: "dreaming_performance", RequiresRestart: true, Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},

	// Lifecycle sweep tuning.
	{Key: service.SettingLifecycleSweepIntervalSeconds, Type: "number", DefaultValue: json.RawMessage(`300`), Description: "How often the lifecycle sweep runs (time-to-live expiry plus soft-delete purge), in seconds. Operator changes take effect on the next sweep iteration.", Category: "lifecycle", Min: ptrF(10), Max: ptrF(3600), Step: ptrF(10)},
	{Key: service.SettingLifecycleBatchSize, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum memories processed per lifecycle sweep pass. Hot-reloads on the next sweep.", Category: "lifecycle", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: service.SettingLifecycleOrphanGraceSeconds, Type: "number", DefaultValue: json.RawMessage(`3600`), Description: "Minimum age an entity must reach before becoming eligible for orphan deletion. Protects in-flight enrichment whose entity rows are written before relationships and before vector upsert.", Category: "lifecycle", Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},

	// Recall reinforcement event payload bound.
	{Key: service.SettingReinforcementEventMemoryCap, Type: "number", DefaultValue: json.RawMessage(`20`), Description: "Maximum memory IDs attached to a recall reinforcement event before truncation. Keeps event payloads bounded on very wide queries.", Category: "reconsolidation", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},
	{Key: service.SettingReinforcementEventRelationshipCap, Type: "number", DefaultValue: json.RawMessage(`20`), Description: "Maximum relationship IDs attached to a relationship.reinforced event before truncation. Mirrors event_memory_cap for the relationship-side hook.", Category: "reconsolidation", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(1)},

	// Cascade and settings cache TTLs.
	{Key: service.SettingCascadeCacheTTLSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How long parsed namespace override blobs stay in the cascade resolver cache, in seconds. Operator changes hit eventual consistency within this window.", Category: "performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(3600), Step: ptrF(1)},
	{Key: service.SettingSettingsCacheTTLSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "How long a Resolve hit lives in the settings cache before the next read goes back to the repo, in seconds. The cache time-to-live itself cannot be hot-reloaded.", Category: "performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(3600), Step: ptrF(1)},

	// API rate-limit per-user-bucket cleanup.
	{Key: service.SettingAPIRateLimitCleanupSeconds, Type: "number", DefaultValue: json.RawMessage(`60`), Description: "How often the rate limiter purges stale per-user buckets, in seconds.", Category: "api_performance", RequiresRestart: true, Min: ptrF(1), Max: ptrF(3600), Step: ptrF(1)},
	{Key: service.SettingAPIRateLimitStaleSeconds, Type: "number", DefaultValue: json.RawMessage(`600`), Description: "Per-user rate-limit bucket is removed after this many seconds of inactivity.", Category: "api_performance", RequiresRestart: true, Min: ptrF(60), Max: ptrF(86400), Step: ptrF(60)},

	// Dashboard session JWT timings. Hot-reload — reads via the 30s settings cache.
	{Key: service.SettingAuthSessionTokenTTLSeconds, Type: "number", DefaultValue: json.RawMessage(`86400`), Description: "Lifetime of a dashboard session JWT, in seconds. Default 86400 (24h). All four SPA login flows (password, IdP, WebAuthn, setup wizard) issue tokens with this TTL, and the auth middleware reissues at this same TTL when refreshing.", Category: "auth", Min: ptrF(60), Max: ptrF(2592000), Step: ptrF(60)},
	{Key: service.SettingAuthSessionRefreshThresholdSeconds, Type: "number", DefaultValue: json.RawMessage(`43200`), Description: "How stale an in-flight session JWT must be before the auth middleware silently reissues it (sliding-expiry refresh), in seconds. Default 43200 (12h, half of TTL). Must be less than auth.session_token_ttl_seconds — if it isn't, refresh never fires and active users get bounced to /login at TTL.", Category: "auth", Min: ptrF(30), Max: ptrF(2592000), Step: ptrF(60)},

	// In-process event bus.
	{Key: service.SettingEventsSubscriberBufferSize, Type: "number", DefaultValue: json.RawMessage(`64`), Description: "Per-subscriber channel buffer for the in-memory event bus. Advanced: values too low drop events under burst, values too high inflate memory per subscriber.", Category: "events", RequiresRestart: true, Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},
	{Key: service.SettingEventsReplayCapacity, Type: "number", DefaultValue: json.RawMessage(`256`), Description: "Ring buffer size for server-sent events (SSE) Last-Event-ID reconnection replay. Advanced: values too low miss events on reconnects, values too high inflate memory.", Category: "events", RequiresRestart: true, Min: ptrF(1), Max: ptrF(100000), Step: ptrF(10)},
	{Key: service.SettingEventsSSEKeepaliveSeconds, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Interval between server-sent events (SSE) keepalive pings, in seconds. Prevents intermediaries from closing idle connections.", Category: "events", RequiresRestart: true, Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},

	// Admin graph minimum edge weight.
	{Key: service.SettingGraphDefaultMinWeight, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Default minimum edge weight applied when the graph visualization endpoint is called without an explicit min_weight query parameter (0.0-1.0).", Category: "api_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingGraphMaxEdges, Type: "number", DefaultValue: json.RawMessage(`2000`), Description: "Maximum edges returned from the graph visualization endpoint. When a namespace's active relationship count exceeds this cap, the response carries the top-N edges by weight descending plus a truncated flag and total/returned counts so the admin UI can surface a partial-view banner. Protects the THREE.js force-graph renderer from stalling on very large namespaces; raise on capable rendering environments.", Category: "api_performance", Min: ptrF(100), Max: ptrF(100000), Step: ptrF(100)},

	// Graph visualization d3-force defaults. Per-project overrides live in
	// project.settings; these supply the cascade fallback when a project has
	// not been tuned. The UI exposes charge as a positive "repulsion" knob and
	// flips the sign at the boundary; the stored value is the raw d3 charge.
	{Key: service.SettingGraphCenterGravity, Type: "number", DefaultValue: json.RawMessage(`0.75`), Description: "Default centering force (gravity) applied by the 3D graph visualization. Higher values pull disconnected clusters together more aggressively; lower values let relationship and access-count weighting do more of the layout work. Per-project override available on the graph page.", Category: "graph_visualization", Min: ptrF(0), Max: ptrF(3), Step: ptrF(0.05)},
	{Key: service.SettingGraphChargeStrength, Type: "number", DefaultValue: json.RawMessage(`-15`), Description: "Default many-body charge strength for the 3D graph visualization. Negative values repel nodes (d3 convention); the UI presents this as a positive 'repulsion' knob from 0 to 100. Per-project override available on the graph page.", Category: "graph_visualization", Min: ptrF(-100), Max: ptrF(0), Step: ptrF(1)},
	{Key: service.SettingGraphLinkDistance, Type: "number", DefaultValue: json.RawMessage(`15`), Description: "Default target link distance for the 3D graph visualization. Lower values pull connected nodes tighter; higher values spread the graph out. Per-project override available on the graph page.", Category: "graph_visualization", Min: ptrF(5), Max: ptrF(100), Step: ptrF(1)},

	// Batch store request item cap.
	{Key: service.SettingAPIBatchStoreMaxItems, Type: "number", DefaultValue: json.RawMessage(`1000`), Description: "Maximum items allowed in a single batch store request. Advanced: raising this widens the per-request denial-of-service surface, so pair with reverse-proxy body-size limits.", Category: "api_performance", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},

	// Export pagination size.
	{Key: service.SettingExportPageSize, Type: "number", DefaultValue: json.RawMessage(`100`), Description: "Memories fetched per page when collecting an export. Hot-reloads on the next export.", Category: "performance", Min: ptrF(1), Max: ptrF(10000), Step: ptrF(10)},

	// MCP CallToolResult byte budget in tokens (~2 bytes/token at the
	// charsPerTokenEstimate ratio). Bounds each tool response so a single
	// call cannot consume the whole client context. Min=100 because the
	// truncation sentinel suffix is ~108 bytes; below that the wire cannot
	// signal Tier-3 truncation honestly. Hot-reloadable via the 30s cache.
	{Key: service.SettingMCPMaxResultTokens, Type: "number", DefaultValue: json.RawMessage(`22000`), Description: "Per-tool MCP CallToolResult budget in tokens. Tool responses above this are reduced (Tier 2) or hard-truncated with a sentinel suffix (Tier 3). Min 100; below this the sentinel cannot fit.", Category: "mcp", Min: ptrF(100), Max: ptrF(1000000), Step: ptrF(100)},

	// Recall scoring and pagination. Hot-reloadable. Operators retune
	// recency / over-fetch shape during incident response without redeploy.
	{Key: service.SettingRankingRecencyDecayPerHour, Type: "number", DefaultValue: json.RawMessage(`0.01`), Description: "Decay rate per hour in the recency term: exp(-rate * hours_since_creation). 0.01 ≈ 69h half-life; 0.02 ≈ 35h; 0.005 ≈ 138h. Lower values flatten the curve so older memories rank closer to fresh ones.", Category: "ranking", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.001)},
	{Key: service.SettingRankingGraphHopMultiplier, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Multiplier applied per hop in the graph-traversal contribution. Lower values dampen indirect connections more aggressively. Default 0.5 matches the historical 1/2^hops approximation when hops≈1.", Category: "ranking", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingRecallDefaultLimit, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Default recall result count when the caller passes limit ≤ 0. Hot-reloadable.", Category: "recall", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},
	{Key: service.SettingRecallMaxLimit, Type: "number", DefaultValue: json.RawMessage(`50`), Description: "Maximum recall result count the MCP recall tool will return per call. Caller-supplied limits above this are clamped before the service call. Larger result sets should use the list tool, which has its own pagination. Hot-reloadable.", Category: "recall", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},
	{Key: service.SettingRecallGraphDefaultDepth, Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Default graph traversal depth applied to recall and graph tools when depth is unset. 0 disables graph contribution; deeper traversal raises latency super-linearly with namespace size.", Category: "recall", Min: ptrF(0), Max: ptrF(10), Step: ptrF(1)},
	{Key: service.SettingRecallGraphMaxDepth, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "Maximum graph traversal depth the recall and graph MCP tools will honor per call. Caller-supplied depths above this are clamped before traversal begins. graph.max_edges still caps total work cumulatively across seeds. Hot-reloadable.", Category: "recall", Min: ptrF(1), Max: ptrF(20), Step: ptrF(1)},
	{Key: service.SettingRecallOverfetchMultiplier, Type: "number", DefaultValue: json.RawMessage(`3`), Description: "Multiplier applied to limit when sizing the candidate pool for the score-and-rerank pass. 3.0 means fetch 3× as many rows as the caller asked for, then re-rank and trim. Higher values trade query cost for ranking quality.", Category: "recall", Min: ptrF(1), Max: ptrF(20), Step: ptrF(0.5)},
	{Key: service.SettingRecallOverfetchMin, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Floor on the over-fetch pool size, so small-limit queries (e.g. limit=1) still feed the re-ranker enough candidates to make a meaningful selection.", Category: "recall", Min: ptrF(1), Max: ptrF(1000), Step: ptrF(1)},

	// Pruning thresholds. Shared key between phase_pruning.go (active
	// relationship expiry pass) and phase_weights.go (mid-cycle expiry on
	// weight decay) so the two paths cannot drift.
	{Key: service.SettingDreamPruningRelationshipWeightThreshold, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Active relationships whose weight falls below this threshold are expired during pruning AND mid-cycle inside the weight-adjustment phase (0.0-1.0). Read every cycle; both paths must read the same key — keep them in lockstep.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamPruningEffectivelyZero, Type: "number", DefaultValue: json.RawMessage(`0.001`), Description: "Upper bound for the zero-confidence prune branch. Catches contradiction-haircut underflow that an exact `== 0` check would miss because confidence updates are multiplicative. Anything at or below this is treated as zero.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(0.1), Step: ptrF(0.0001)},

	// Transitive relationship discovery.
	{Key: service.SettingDreamTransitiveMinWeight, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Minimum product weight (rel_ab.weight × rel_bc.weight) for a new transitive relationship to be created (0.0-1.0). Suppresses noise edges that would compound near-zero weights.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamTransitiveMaxPerCycle, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum transitive relationships created per dream cycle. Hard ceiling per cycle, independent of namespace_hard_cap. When the namespace is near namespace_hard_cap the effective per-cycle limit shrinks to the remaining headroom (hard_cap − active) and raising this knob has no effect — raise namespace_hard_cap or let the pressure-driven prune relieve saturation instead.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(100000), Step: ptrF(10)},
	{Key: service.SettingDreamTransitiveNamespaceHardCap, Type: "number", DefaultValue: json.RawMessage(`1000000`), Description: "Maximum active relationship count for the transitive phase. The pruning phase begins expiring the lowest-weight transitive (inferred) edges once active count exceeds hard_cap × namespace_high_water, draining down to hard_cap × namespace_low_water. The transitive phase no-ops entirely at or above hard_cap. Default sets the cap high enough that namespace size does not throttle the transitive phase under normal use; lower to enforce a per-namespace ceiling and let the pressure-prune drain to low_water.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(10000000), Step: ptrF(100)},
	{Key: service.SettingDreamTransitiveNamespaceHighWater, Type: "number", DefaultValue: json.RawMessage(`0.95`), Description: "Fraction of namespace_hard_cap at which the pruning phase begins expiring the lowest-weight transitive (inferred) relationships. 0.95 means start draining once active count reaches 95% of the hard cap. Must be greater than namespace_low_water.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamTransitiveNamespaceLowWater, Type: "number", DefaultValue: json.RawMessage(`0.80`), Description: "Fraction of namespace_hard_cap that the pressure-driven prune drains down to. With high_water=0.95 and low_water=0.80 at hard_cap=10000, the prune fires at ≥9500 active and stops at 8000. Lower values create more headroom (longer between prune events) at the cost of dropping more inferred edges per pass. Must be strictly less than namespace_high_water.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},

	// Weight-adjustment knobs. Each governs one term in the per-cycle
	// recompute formula. Lower decay_factor / higher dead_source_multiplier
	// extend an edge's lifetime; raise tier2_multiplier to give co-mention
	// support more pull on rising weights.
	{Key: service.SettingDreamWeightTier2Multiplier, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Multiplier applied to a Tier-2 supporter's confidence (memory touches both endpoints but not via direct lineage). Tier-1 supporters always contribute mem.Confidence at full weight.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamWeightDecayWindowDays, Type: "number", DefaultValue: json.RawMessage(`30`), Description: "Age past which a relationship begins to decay. Edges younger than this skip the decay loop entirely. The decay schedule is decay_factor^(periods), capped by decay_max_periods.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(3650), Step: ptrF(1)},
	{Key: service.SettingDreamWeightDecayFactor, Type: "number", DefaultValue: json.RawMessage(`0.95`), Description: "Per-period multiplier applied to a relationship's weight once it crosses the decay window. With defaults (0.95, 30d window, 10 periods cap) a 12-month-old edge floors at ~0.60× its starting weight.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.01)},
	{Key: service.SettingDreamWeightDecayMaxPeriods, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Maximum decay periods applied to any single edge. Caps total decay so very old edges floor at decay_factor^max_periods and stop sliding toward zero.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamWeightDeadSourceMultiplier, Type: "number", DefaultValue: json.RawMessage(`0.5`), Description: "Multiplier applied when the relationship's recorded source memory is soft-deleted AND no live memory still attests the edge. Drives dead-source rows toward the pruning floor faster than decay alone.", Category: "dreaming_performance", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},
	{Key: service.SettingDreamWeightCeiling, Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Hard ceiling on relationship weight. Lowering this does NOT retroactively clamp existing rows — the SQL UPDATE clause's separate ceiling governs persisted writes; this setting governs only the per-cycle recompute. Existing rows above the new ceiling drop to it on their next weight-adjust pass.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(10), Step: ptrF(0.1)},

	// Consolidation phase clustering and sampling.
	{Key: service.SettingDreamConsolidationAlignmentSampleSize, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "Number of user memories sampled per cluster when scoring how well a synthesis aligns with the broader namespace. Larger samples improve alignment fidelity at the cost of more LLM tokens per cluster.", Category: "dreaming_consolidation", Min: ptrF(1), Max: ptrF(100), Step: ptrF(1)},
	{Key: service.SettingDreamConsolidationClusterOverlapThreshold, Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "Word-overlap fraction at which the heuristic clusterer treats two memories as belonging to the same cluster (0.0-1.0). Drives which memories get bundled into a synthesis prompt; higher values produce smaller, tighter clusters.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(1), Step: ptrF(0.05)},

	// LLM call temperatures.
	{Key: service.SettingDreamSynthesisTemperature, Type: "number", DefaultValue: json.RawMessage(`0.3`), Description: "Sampling temperature for the consolidation synthesis call. Synthesis is free-text output (not JSON), so a slightly higher temperature than the JSON-only judges helps produce coherent paragraph-form merges.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamAlignmentTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the alignment-scoring call. The output is JSON-only ({alignment, reasoning}), so a low temperature keeps the score deterministic; raise sparingly when the judge needs latitude on partial-overlap evidence.", Category: "dreaming_consolidation", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamNoveltyJudgeTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the novelty-audit LLM call. Default 0.1 keeps the JSON-only output deterministic.", Category: "dreaming_novelty", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingDreamContradictionTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the contradiction-detection LLM call. Default 0.1 keeps the JSON-only output deterministic.", Category: "dreaming_contradiction", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEnrichmentConflictTemperature, Type: "number", DefaultValue: json.RawMessage(`0.1`), Description: "Sampling temperature for the enrichment-pipeline conflict-resolution LLM call. Default 0.1 keeps the JSON-only output deterministic.", Category: "enrichment_performance", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},
	{Key: service.SettingEnrichmentIngestionDecisionTemperature, Type: "number", DefaultValue: json.RawMessage(`0`), Description: "Sampling temperature for the ingestion-decision LLM call. Default 0 (greedy) maximises determinism on a categorical decision (ADD/UPDATE/DELETE/NONE).", Category: "enrichment_ingestion", Min: ptrF(0), Max: ptrF(2), Step: ptrF(0.05)},

	// Heartbeat tick timeout for the dream runner.
	{Key: service.SettingDreamHeartbeatTickTimeoutSeconds, Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Maximum time in seconds a single TickHeartbeat write may block before the runner skips the tick. A stuck writer would otherwise stall the runner; this cap converts contention into a missed beat instead of a frozen cycle.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(600), Step: ptrF(1)},

	// Stuck-scan caps. Distinct keys for dreaming and enrichment so the
	// two can be tuned independently.
	{Key: service.SettingDreamStuckScanLimit, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum stale dream cycles fetched per stuck-scan pass. Bounds the working-set size so a flood of stuck cycles cannot block writers behind the scan transaction.", Category: "dreaming_performance", Min: ptrF(1), Max: ptrF(1000000), Step: ptrF(100)},
	{Key: service.SettingEnrichmentStuckScanLimit, Type: "number", DefaultValue: json.RawMessage(`5000`), Description: "Maximum stale enrichment jobs fetched per stuck-scan pass. Bounds the working-set size so a flood of stuck jobs cannot block writers behind the scan transaction.", Category: "enrichment_performance", Min: ptrF(1), Max: ptrF(1000000), Step: ptrF(100)},
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
	{Key: service.SettingDreamContradictionPrompt, Type: "prompt", Description: "LLM prompt used by the contradiction-detection phase. Two %s placeholders for Statement A and Statement B. Must return JSON with `contradicts`, `winner` (\"a\"/\"b\"/\"tie\"/null), and `explanation`.", Category: "dreaming_prompts"},
	{Key: service.SettingDreamSynthesisPrompt, Type: "prompt", Description: "LLM prompt used by the consolidation phase to merge a cluster of memories into a single synthesis. One %s placeholder for the combined source content. Must return only the synthesized text.", Category: "dreaming_prompts"},
	{Key: service.SettingDreamAlignmentPrompt, Type: "prompt", Description: "LLM prompt used to score how strongly new evidence supports or contradicts an existing synthesis. Two %s placeholders for synthesis and evidence. Must return JSON with an `alignment` float in [-1.0, 1.0] and `reasoning`.", Category: "dreaming_prompts"},
	{Key: service.SettingDreamNoveltyJudgePrompt, Type: "prompt", Description: "LLM prompt used by the novelty audit to decide whether a synthesis introduces facts not present in its sources. Two %s placeholders for synthesis and sources. Must return JSON with a `novel_facts` array (empty when the synthesis is duplicative).", Category: "dreaming_prompts"},
	{Key: service.SettingIngestionDecisionPrompt, Type: "prompt", Description: "LLM prompt used by the ingestion-decision phase. Three placeholders in order: %d for top_k (rendered into the instructions), %s for the new memory content, %s for the candidate list. Must return JSON {\"operation\":\"ADD|UPDATE|DELETE|NONE\",\"target_id\":\"uuid|null\",\"rationale\":\"string\"}.", Category: "enrichment_prompts"},
	{Key: service.SettingFactPrompt, Type: "prompt", Description: "LLM prompt for fact extraction during enrichment. One %s placeholder for the input content. Must return a JSON array of {content, confidence, tags} objects (the parser also accepts \"fact\" as an alias for \"content\").", Category: "enrichment_prompts"},
	{Key: service.SettingEntityPrompt, Type: "prompt", Description: "LLM prompt for entity and relationship extraction during enrichment. One %s placeholder for the input content. Must return JSON {entities:[{name,type,properties}], relationships:[{source,target,relation,weight,temporal}]}.", Category: "enrichment_prompts"},
	{Key: service.SettingQueryAugmentPrompt, Type: "prompt", Description: "LLM prompt for the query-augmentation phase. Named placeholders {content} and {N} are substituted with the memory content and the requested query count (string replace, not fmt.Sprintf, so extra braces in your text are safe). Must return a JSON array of strings.", Category: "enrichment_prompts"},
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
	// the two is a load-bearing bug — it caused the contradictionCap=30
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
				" — schema=" + string(entry.DefaultValue) + " runtime=" + def)
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
