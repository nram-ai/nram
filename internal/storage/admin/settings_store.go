package admin

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
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
	{Key: "dreaming.max_tokens_per_cycle", Type: "number", DefaultValue: json.RawMessage(`1024000`), Description: "Maximum total tokens per dream cycle across all phases", Category: "dreaming"},
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
	{Key: "dreaming.contradiction.cap_per_cycle", Type: "number", DefaultValue: json.RawMessage(`2000`), Description: "Maximum LLM pair-contradiction checks per dream cycle. Bump for faster drain during first-pass backfill on large namespaces, then restore.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.loser_haircut", Type: "number", DefaultValue: json.RawMessage(`0.85`), Description: "Multiplicative confidence haircut applied to the losing side of a contradiction (0.0-1.0). Smaller = harsher penalty. Diminishes on reaffirmation: effective = 1 - (1 - base) / detection_count.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.winner_haircut", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Multiplicative confidence haircut applied to the winning side of a contradiction (0.0-1.0). Acknowledges some uncertainty in any judgment. Same diminishing rule as the loser haircut.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.tie_haircut", Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Multiplicative confidence haircut applied to both sides when the LLM judge cannot pick a winner (0.0-1.0). Same diminishing rule.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.paraphrase_enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Auto-supersede near-duplicate memory pairs (cosine >= paraphrase_threshold) without calling the LLM judge. Lowers LLM cost and closes the paraphrase coverage gap that the contradiction judge intentionally leaves open.", Category: "dreaming_contradiction"},
	{Key: "dreaming.contradiction.paraphrase_threshold", Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Cosine similarity at or above which the contradictions phase treats a pair as paraphrases and auto-supersedes the lower-confidence side (0.0-1.0). Conservative high values minimize false positives.", Category: "dreaming_contradiction"},
	{Key: service.SettingDreamParaphraseEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run the paraphrase-dedup sweep. Catches user-source duplicates the contradiction phase's anchor walk leaves unpaired by running vector_store.Search(top-K) directly on every eligible memory.", Category: "dreaming_paraphrase"},
	{Key: service.SettingDreamParaphraseThreshold, Type: "number", DefaultValue: json.RawMessage(`0.97`), Description: "Cosine similarity at or above which the sweep auto-supersedes the lower-confidence side of a pair (0.0-1.0). Conservative high values minimize false positives.", Category: "dreaming_paraphrase"},
	{Key: service.SettingDreamParaphraseCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`500`), Description: "Maximum memories visited per dream cycle by the paraphrase sweep. Residual is signalled when more candidates remain than the cap allowed.", Category: "dreaming_paraphrase"},
	{Key: service.SettingDreamParaphraseTopK, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "Top-K nearest neighbours probed per anchor. Two duplicates only need to be top-K of each other (not of every other anchor) to be paired.", Category: "dreaming_paraphrase"},
	{Key: service.SettingDreamEmbeddingBackfillEnabled, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Run the embedding-backfill phase. Repairs rows whose embedding_dim is set but whose memory_vectors_<dim> row is missing — re-embeds when the embedder is healthy, clears embedding_dim otherwise.", Category: "dreaming_embedding_backfill"},
	{Key: service.SettingDreamEmbeddingBackfillCapPerCycle, Type: "number", DefaultValue: json.RawMessage(`200`), Description: "Maximum divergent rows repaired per dream cycle by the embedding-backfill phase. Bump to drain a large existing backlog faster, then restore.", Category: "dreaming_embedding_backfill"},
	{Key: "reconsolidation.mode", Type: "enum", DefaultValue: json.RawMessage(`"shadow"`), Description: "Reconsolidation mode: 'shadow' emits events without persisting; 'persist' writes confidence/access updates; 'off' disables reinforcement entirely", Category: "reconsolidation", EnumValues: []string{"shadow", "persist", "off"}},
	{Key: "reconsolidation.factor", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "Per-recall confidence boost applied to reinforced memories (0.0-1.0)", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_enabled", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable sleep-time confidence decay for memories not recalled recently", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_threshold_days", Type: "number", DefaultValue: json.RawMessage(`14`), Description: "Days since last recall before a memory starts losing confidence to decay", Category: "reconsolidation"},
	{Key: "reconsolidation.decay_rate_per_cycle", Type: "number", DefaultValue: json.RawMessage(`0.02`), Description: "Confidence loss per dream cycle applied to decay-eligible memories (0.0-1.0)", Category: "reconsolidation"},
	{Key: "reconsolidation.confidence_floor", Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Minimum confidence decay will not push below (0.0-1.0)", Category: "reconsolidation"},
	{Key: "recall.fusion.enabled", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable hybrid recall: parallel vector + BM25/tsvector retrieval fused via Reciprocal Rank Fusion. Requires migration 18 applied and a lexical searcher wired.", Category: "recall_fusion"},
	{Key: "recall.fusion.rrf_k", Type: "number", DefaultValue: json.RawMessage(`60`), Description: "RRF constant. Higher values flatten the head of each ranked list; 60 is the canonical Cormack-Clarke-Buettcher default.", Category: "recall_fusion"},
	{Key: "recall.fusion.vector_weight", Type: "number", DefaultValue: json.RawMessage(`0.70`), Description: "Weight on each vector channel's RRF contribution (0.0-1.0). Together with lexical_weight, controls the relative pull of dense embedding vs sparse keyword evidence.", Category: "recall_fusion"},
	{Key: "recall.fusion.lexical_weight", Type: "number", DefaultValue: json.RawMessage(`0.30`), Description: "Weight on each lexical channel's RRF contribution (0.0-1.0). Raise to bias recall toward exact-token matches (entity names, version strings).", Category: "recall_fusion"},
	{Key: service.SettingIngestionDecisionEnabled, Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable LLM-driven ingestion decision (ADD/UPDATE/DELETE/NONE) on near-duplicate matches at enrichment time. When off, every memory is treated as ADD without an LLM call.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionShadow, Type: "boolean", DefaultValue: json.RawMessage(`true`), Description: "Shadow mode: compute and log the decision (op, top_score, match_count) but always behave as if it were ADD. Defaults to true so enabling the feature first observes its distribution before acting on UPDATE/DELETE.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionThreshold, Type: "number", DefaultValue: json.RawMessage(`0.92`), Description: "Cosine similarity at or above which a candidate match is presented to the LLM judge (0.0-1.0). Below this, the new memory is treated as ADD without an LLM call.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionTopK, Type: "number", DefaultValue: json.RawMessage(`5`), Description: "Maximum number of candidate matches presented to the LLM judge.", Category: "enrichment_ingestion"},
	{Key: service.SettingIngestionDecisionModel, Type: "string", DefaultValue: json.RawMessage(`""`), Description: "LLM model name for the ingestion decision. Empty falls back to the fact-extraction provider's model (this is a categorization task, a small model is fine).", Category: "enrichment_ingestion"},
	{Key: service.SettingRankWeightSim, Type: "number", DefaultValue: json.RawMessage(`0.50`), Description: "Weight on cosine similarity in the recall ranking formula (0.0-1.0). The dominant term: how strongly query-to-memory semantic match contributes to the score. Lower to give other signals more pull.", Category: "ranking"},
	{Key: service.SettingRankWeightRec, Type: "number", DefaultValue: json.RawMessage(`0.15`), Description: "Weight on recency in the recall ranking formula (0.0-1.0). Recency decays as exp(-0.01 * hours_since_creation), so this term favours fresh memories without sharply discarding older ones.", Category: "ranking"},
	{Key: service.SettingRankWeightImp, Type: "number", DefaultValue: json.RawMessage(`0.10`), Description: "Weight on Memory.Importance in the recall ranking formula (0.0-1.0). Importance is operator-set per memory; bump this to honor manual curation more strongly.", Category: "ranking"},
	{Key: service.SettingRankWeightFreq, Type: "number", DefaultValue: json.RawMessage(`0.00`), Description: "Weight on access-count frequency (log-normalized to the result set) in the recall ranking formula (0.0-1.0). Default 0 because reconsolidation already drives Memory.Confidence on every recall — Frequency double-counts the same signal. Re-enable for callers that bypass the reconsolidation hook.", Category: "ranking"},
	{Key: service.SettingRankWeightGraph, Type: "number", DefaultValue: json.RawMessage(`0.20`), Description: "Weight on graph-traversal relevance in the recall ranking formula (0.0-1.0). Boosts memories connected to entities mentioned in the query through the knowledge graph.", Category: "ranking"},
	{Key: service.SettingRankWeightConf, Type: "number", DefaultValue: json.RawMessage(`0.05`), Description: "Weight on Memory.Confidence in the recall ranking formula (0.0-1.0). Confidence is reinforced on each recall and decayed during dream cycles, so this term elevates well-used, well-aligned memories. Start small (0.05) and raise after the confidence distribution stabilises.", Category: "ranking"},
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
}

func (s *SettingsAdminStore) GetSettingsSchema(ctx context.Context) ([]api.SettingSchema, error) {
	return settingsSchemas, nil
}
