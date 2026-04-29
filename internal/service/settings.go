package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// Well-known setting keys.
const (
	SettingEmbedProvider     = "provider.embedding.type"
	SettingEmbedURL          = "provider.embedding.url"
	SettingEmbedKey          = "provider.embedding.key"
	SettingEmbedModel        = "provider.embedding.model"
	SettingFactProvider      = "provider.fact.type"
	SettingFactURL           = "provider.fact.url"
	SettingFactKey           = "provider.fact.key"
	SettingFactModel         = "provider.fact.model"
	SettingEntityProvider    = "provider.entity.type"
	SettingEntityURL         = "provider.entity.url"
	SettingEntityKey         = "provider.entity.key"
	SettingEntityModel       = "provider.entity.model"
	SettingEnrichmentEnabled = "enrichment.enabled"
	SettingDedupThreshold    = "enrichment.dedup_threshold"
	SettingFactPrompt        = "enrichment.fact_prompt"
	SettingEntityPrompt      = "enrichment.entity_prompt"
	SettingRankWeightSim     = "ranking.weight.similarity"
	SettingRankWeightRec     = "ranking.weight.recency"
	SettingRankWeightImp     = "ranking.weight.importance"
	SettingRankWeightFreq    = "ranking.weight.frequency"
	SettingRankWeightGraph   = "ranking.weight.graph_relevance"
	SettingRankWeightConf    = "ranking.weight.confidence"
	SettingTokenRetention    = "usage.token_retention_days"

	// Hybrid recall fusion. Off by default; flipping enabled turns on
	// parallel vector + BM25/tsvector retrieval with RRF fusion. The two
	// weights govern each channel's RRF contribution; rrf_k is the
	// canonical Cormack-Clarke-Buettcher constant (60 dampens deep-tail
	// noise without flattening the head of either ranked list).
	SettingRecallFusionEnabled = "recall.fusion.enabled"
	SettingRecallFusionK       = "recall.fusion.rrf_k"
	SettingRecallFusionVecW    = "recall.fusion.vector_weight"
	SettingRecallFusionLexW    = "recall.fusion.lexical_weight"

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
	// Backfill path uses a more aggressive auto-reject threshold than
	// synthesis-time auditing. These are historical rows already written;
	// a more confident "this is duplicative" cutoff lets the sweep clean
	// up clear dupes without burning LLM-judge calls where the judge has
	// been observed to let obvious duplicates through. Override the
	// synthesis-path SettingDreamNoveltyEmbedHighThreshold when set; if
	// unset or <= 0, the backfill path uses the synthesis threshold.
	SettingDreamNoveltyBackfillEmbedHighThreshold = "dreaming.novelty.backfill_embed_high_threshold"

	// Consolidation sub-phase budget fractions. The three sub-phases (audit,
	// reinforce, consolidate) each get a reserved slice of the cycle's
	// remaining budget at entry so one sub-phase cannot starve another.
	// Fractions are interpreted relative to the parent budget's remaining
	// tokens at sub-phase entry; oversubscription is permitted (the root cap
	// always wins) but starves later sub-slices.
	SettingDreamConsolidationAuditFraction       = "dreaming.consolidation.audit_budget_fraction"
	SettingDreamConsolidationReinforceFraction   = "dreaming.consolidation.reinforce_budget_fraction"
	SettingDreamConsolidationConsolidateFraction = "dreaming.consolidation.consolidate_budget_fraction"

	// Contradiction-detection cap. Bounds LLM pair-check calls per cycle so
	// the phase cannot starve the rest of the pipeline. Residual is driven
	// by the per-memory contradictions_checked_at stamp, not by this cap; the
	// cap exists purely as a budget guard. Operators bump this during first
	// pass drains on namespaces with large unstamped backlogs.
	SettingDreamContradictionCap = "dreaming.contradiction.cap_per_cycle"

	// Contradiction confidence haircuts. Multiplicative factors applied to
	// memory.confidence at the time a conflicts_with edge is written. The
	// factor is diminished on reaffirmation: effective = 1 - (1 - base) / N
	// where N is the count of prior conflicts_with edges between the pair.
	// Loser is the side the LLM judge marks as less likely correct; winner
	// takes a smaller haircut acknowledging some uncertainty in any judgment;
	// tie applies the same haircut to both sides when the judge cannot pick.
	SettingDreamContradictionLoserHaircut        = "dreaming.contradiction.loser_haircut"
	SettingDreamContradictionWinnerHaircut       = "dreaming.contradiction.winner_haircut"
	SettingDreamContradictionTieHaircut          = "dreaming.contradiction.tie_haircut"
	SettingDreamContradictionParaphraseEnabled   = "dreaming.contradiction.paraphrase_enabled"
	SettingDreamContradictionParaphraseThreshold = "dreaming.contradiction.paraphrase_threshold"

	// Paraphrase dedup phase: dedicated sweep that catches user-source
	// duplicates the contradiction phase's anchor walk does not pair.
	SettingDreamParaphraseEnabled     = "dreaming.paraphrase.enabled"
	SettingDreamParaphraseThreshold   = "dreaming.paraphrase.threshold"
	SettingDreamParaphraseCapPerCycle = "dreaming.paraphrase.cap_per_cycle"
	SettingDreamParaphraseTopK        = "dreaming.paraphrase.top_k"

	// Embedding-backfill phase: scans for memories whose embedding_dim is
	// recorded but whose corresponding memory_vectors_<dim> row is missing
	// (the no_vector divergence the paraphrase phase observes), then either
	// re-embeds via the live embedder or clears embedding_dim so the row
	// state matches the vector store. Runs before paraphrase dedup so the
	// downstream phase sees the repaired state in the same cycle.
	SettingDreamEmbeddingBackfillEnabled     = "dreaming.embedding_backfill.enabled"
	SettingDreamEmbeddingBackfillCapPerCycle = "dreaming.embedding_backfill.cap_per_cycle"

	// Retention for soft-deleted memories. Rows past this age are hard-deleted
	// by the retention sweeper and their vector rows are CASCADEd alongside.
	SettingMemorySoftDeleteRetentionDays = "memory.soft_delete_retention_days"

	// Ingestion-decision phase. Runs as the first step of enrichment: when a
	// new memory has near-neighbours above the configured similarity threshold,
	// an LLM judges whether to ADD as-is, mark as superseding an existing row
	// (UPDATE), discard the new row (DELETE), or proceed with no lineage edge
	// (NONE). Shadow mode computes and logs the decision but always behaves as
	// if it were ADD, so the distribution can be observed before flipping the
	// behavior on. Empty model falls back to the fact-extraction provider.
	SettingIngestionDecisionEnabled   = "enrichment.ingestion_decision.enabled"
	SettingIngestionDecisionShadow    = "enrichment.ingestion_decision.shadow_mode"
	SettingIngestionDecisionThreshold = "enrichment.ingestion_decision.threshold"
	SettingIngestionDecisionTopK      = "enrichment.ingestion_decision.top_k"
	SettingIngestionDecisionModel     = "enrichment.ingestion_decision.model"
	SettingIngestionDecisionPrompt    = "enrichment.ingestion_decision.prompt"

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

	// Recall reinforcement event memory cap. Caps how many memory IDs are
	// attached to a recall event before truncation, bounding event payload
	// growth on very wide queries.
	SettingReinforcementEventMemoryCap = "reconsolidation.event_memory_cap"

	// Cascade resolver cache TTL. How long a parsed override blob stays in
	// memory before the next read goes back to the repo. Operator changes
	// to project/user settings hit eventual consistency within this window.
	// Read once at process start; changes require server restart.
	SettingCascadeCacheTTLSeconds = "cascade.cache_ttl_seconds"

	// Settings cache TTL. How long a Resolve hit lives in memory before
	// the next read goes back to the repo. Read once at process start;
	// changes require server restart (the cache TTL itself cannot be
	// hot-reloaded without self-reference).
	SettingSettingsCacheTTLSeconds = "settings.cache_ttl_seconds"

	// Enrichment worker pool tuning. The pool claims jobs in batches and
	// fans LLM calls out per-job before issuing one shared embed call;
	// the knobs below cap each layer.
	SettingEnrichmentWorkerBatchClaimSize             = "enrichment.worker.batch_claim_size"
	SettingEnrichmentWorkerPreEmbedConcurrency        = "enrichment.worker.pre_embed_concurrency"
	SettingEnrichmentWorkerEmbedTimeoutSeconds        = "enrichment.worker.embed_timeout_seconds"
	SettingEnrichmentWorkerEmbedInputCap              = "enrichment.worker.embed_input_cap"
	SettingEnrichmentWorkerBreakerEscalateSeconds     = "enrichment.worker.breaker_error_escalate_seconds"
	SettingEnrichmentWorkerMaxBackoffSeconds          = "enrichment.worker.max_backoff_seconds"
	SettingEnrichmentWorkerCountSQLite                = "enrichment.worker.count_sqlite"
	SettingEnrichmentWorkerCountPostgres              = "enrichment.worker.count_postgres"
	SettingEnrichmentWorkerPollIntervalSeconds        = "enrichment.worker.poll_interval_seconds"
	SettingEnrichmentIngestionRationaleMaxLen         = "enrichment.ingestion.rationale_max_len"

	// Dreaming worker tuning beyond what the existing dreaming.* keys cover.
	SettingDreamContradictionNeighbors = "dreaming.contradiction.neighbors_per_anchor"
	SettingDreamEntityMergeThreshold   = "dreaming.entity_merge.cosine_threshold"
	SettingDreamSchedulerPollSeconds   = "dreaming.scheduler.poll_interval_seconds"

	// Lifecycle sweep tuning. SweepInterval is read at start (restart);
	// BatchSize / OrphanGrace are read on every sweep so they hot-reload.
	SettingLifecycleSweepIntervalSeconds = "lifecycle.sweep_interval_seconds"
	SettingLifecycleBatchSize            = "lifecycle.batch_size"
	SettingLifecycleOrphanGraceSeconds   = "lifecycle.orphan_grace_seconds"

	// API rate-limit per-user-bucket cleanup. Read once at startup; changes
	// require server restart.
	SettingAPIRateLimitCleanupSeconds = "api.rate_limit.cleanup_interval_seconds"
	SettingAPIRateLimitStaleSeconds   = "api.rate_limit.stale_after_seconds"

	// In-process event bus. subscriber_buffer_size is the per-subscriber
	// channel buffer (drops events on full); replay_capacity is the ring
	// buffer for SSE Last-Event-ID reconnection. Both read once at startup
	// — wrong values can stall subscribers or balloon memory, so both are
	// restart-required and flagged as advanced in their descriptions.
	SettingEventsSubscriberBufferSize  = "events.subscriber_buffer_size"
	SettingEventsReplayCapacity        = "events.replay_capacity"
	SettingEventsSSEKeepaliveSeconds   = "events.sse_keepalive_seconds"

	// Admin graph visualization minimum edge weight. Hot-reloadable.
	SettingGraphDefaultMinWeight = "graph.default_min_weight"

	// Batch store request item cap. Raising this widens the per-request DoS
	// surface; description warns and the value is bounded by an internal
	// safety floor in BatchStore validation.
	SettingAPIBatchStoreMaxItems = "api.batch_store.max_items"

	// Export pagination size for memories. Hot-reloadable.
	SettingExportPageSize = "export.page_size"
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
	SettingEnrichmentEnabled:          "true",
	SettingDedupThreshold:             "0.92",
	SettingRankWeightSim:              "0.50",
	SettingRankWeightRec:              "0.15",
	SettingRankWeightImp:              "0.10",
	SettingRankWeightFreq:             "0.00",
	SettingRankWeightGraph:            "0.20",
	SettingRankWeightConf:             "0.05",
	SettingRecallFusionEnabled:        "false",
	SettingRecallFusionK:              "60",
	SettingRecallFusionVecW:           "0.70",
	SettingRecallFusionLexW:           "0.30",
	SettingTokenRetention:             "365",
	SettingDreamingEnabled:            "false",
	SettingDreamMaxTokensPerCycle:     "1024000",
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

When they contradict, also identify which is more likely correct and set "winner" to "a", "b", or "tie". Use "tie" when the contradiction is real but neither side is clearly right (subjective claims, partial overlap, claims about different time periods, equally plausible interpretations).

Output ONLY this JSON, nothing else:
{"contradicts": true, "winner": "a", "explanation": "reason"}
or
{"contradicts": true, "winner": "b", "explanation": "reason"}
or
{"contradicts": true, "winner": "tie", "explanation": "reason"}
or
{"contradicts": false, "winner": null, "explanation": "reason"}`,
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
	SettingDreamNoveltyEnabled:                    "true",
	SettingDreamNoveltyEmbedHighThreshold:         "0.97",
	SettingDreamNoveltyEmbedLowThreshold:          "0.85",
	SettingDreamNoveltyJudgeMaxTokens:             "512",
	SettingDreamNoveltyBackfillPerCycle:           "500",
	SettingDreamNoveltyBackfillEmbedHighThreshold: "0.93",

	SettingDreamConsolidationAuditFraction:       "0.35",
	SettingDreamConsolidationReinforceFraction:   "0.35",
	SettingDreamConsolidationConsolidateFraction: "0.30",

	SettingDreamContradictionCap:                 "2000",
	SettingDreamContradictionLoserHaircut:        "0.85",
	SettingDreamContradictionWinnerHaircut:       "0.97",
	SettingDreamContradictionTieHaircut:          "0.92",
	SettingDreamContradictionParaphraseEnabled:   "true",
	SettingDreamContradictionParaphraseThreshold: "0.97",

	SettingDreamEmbeddingBackfillEnabled:     "true",
	SettingDreamEmbeddingBackfillCapPerCycle: "200",

	SettingDreamParaphraseEnabled:     "true",
	SettingDreamParaphraseThreshold:   "0.97",
	SettingDreamParaphraseCapPerCycle: "500",
	SettingDreamParaphraseTopK:        "5",

	SettingMemorySoftDeleteRetentionDays: "30",

	SettingFactPrompt: `You are a fact extraction engine. Given a text, extract all discrete facts as a JSON array. Each fact should be a JSON object with these fields:
- "content": the fact statement (string)
- "confidence": how confident you are in this fact, 0.0 to 1.0 (number)
- "tags": relevant tags for categorization (array of strings)

Return ONLY valid JSON. Do not include markdown fences or explanation.

Text:
%s`,
	SettingEntityPrompt: `You are an entity and relationship extraction engine. Given a text, extract all named entities and relationships between them as JSON.

Return a JSON object with two fields:
- "entities": array of objects with fields:
  - "name": the entity name (string)
  - "type": the entity type, e.g. "person", "organization", "location", "concept" (string)
  - "properties": optional key-value pairs (object)
- "relationships": array of objects with fields:
  - "source": source entity name (string)
  - "target": target entity name (string)
  - "relation": the relationship type (string)
  - "weight": confidence/strength 0.0 to 1.0 (number)
  - "temporal": "current", "as of <date>", "previously", or "no longer" (string, default "current")

Return ONLY valid JSON. Do not include markdown fences or explanation.

Text:
%s`,

	SettingIngestionDecisionEnabled:   "false",
	SettingIngestionDecisionShadow:    "true",
	SettingIngestionDecisionThreshold: "0.92",
	SettingIngestionDecisionTopK:      "5",
	SettingIngestionDecisionModel:     "",
	SettingIngestionDecisionPrompt: `You are an ingestion decision engine. You do NOT converse. You output JSON only.

A new memory has just arrived. Below is its content, followed by up to %d candidate near-neighbour memories that already exist (with their IDs and creation times). Decide what to do with the new memory.

Choose exactly one operation:
- "ADD": the new memory is genuinely distinct from every candidate; keep it as a separate row.
- "UPDATE": the new memory is an updated, more specific, more recent, or otherwise improved version of one specific candidate. The old candidate should be marked superseded by the new memory.
- "DELETE": the new memory is itself redundant — every fact it states is already present in one of the candidates, which remains the canonical record. Discard the new memory.
- "NONE": the new memory overlaps with one or more candidates but is not a clean update or duplicate (e.g. partial overlap, different aspect of the same topic). Keep the new memory but do not record any lineage edge.

Hard rules:
- target_id is required for UPDATE and DELETE and must be one of the candidate IDs given below verbatim. For ADD and NONE, set target_id to null.
- Pick UPDATE only when one specific candidate is clearly superseded; if multiple candidates would each need updating, choose NONE.
- Pick DELETE only when the new memory adds nothing not already in the named candidate.
- When in doubt between ADD and NONE, prefer ADD.
- Rationale must be one short sentence (under 200 characters) that names the candidate ID you compared against (when applicable).

<new_memory>
%s
</new_memory>

<candidates>
%s
</candidates>

Output ONLY this JSON, nothing else:
{"operation": "ADD", "target_id": null, "rationale": "..."}
or
{"operation": "UPDATE", "target_id": "candidate-uuid", "rationale": "..."}
or
{"operation": "DELETE", "target_id": "candidate-uuid", "rationale": "..."}
or
{"operation": "NONE", "target_id": null, "rationale": "..."}`,
	SettingDreamNoveltyJudgePrompt: `You are a novelty auditor. You do NOT converse. You output JSON only.

Given a synthesized memory and the source memories it was derived from, list any facts present in the synthesis that are NOT stated or directly implied by any of the sources. A fact is "novel" only if a careful reader could not derive it from the sources alone.

Hard rules:
- Rewording is NEVER novelty. If the synthesis says the same thing with different words, it is not novel.
- Reorganization is NEVER novelty. Reordering, combining, or restructuring source content is not novel.
- Summarization is NEVER novelty. Compressing or generalizing source content is not novel.
- A fact is novel ONLY if it introduces a new entity, relationship, quantity, date, cause, or consequence absent from every source.
- When in doubt, return an empty array.

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

	SettingReinforcementEventMemoryCap: "20",

	SettingCascadeCacheTTLSeconds:  "30",
	SettingSettingsCacheTTLSeconds: "30",

	SettingEnrichmentWorkerBatchClaimSize:         "16",
	SettingEnrichmentWorkerPreEmbedConcurrency:    "4",
	SettingEnrichmentWorkerEmbedTimeoutSeconds:    "30",
	SettingEnrichmentWorkerEmbedInputCap:          "256",
	SettingEnrichmentWorkerBreakerEscalateSeconds: "300",
	SettingEnrichmentWorkerMaxBackoffSeconds:      "30",
	SettingEnrichmentWorkerCountSQLite:            "1",
	SettingEnrichmentWorkerCountPostgres:          "2",
	SettingEnrichmentWorkerPollIntervalSeconds:    "5",
	SettingEnrichmentIngestionRationaleMaxLen:     "500",

	SettingDreamContradictionNeighbors: "4",
	SettingDreamEntityMergeThreshold:   "0.92",
	SettingDreamSchedulerPollSeconds:   "30",

	SettingLifecycleSweepIntervalSeconds: "300",
	SettingLifecycleBatchSize:            "100",
	SettingLifecycleOrphanGraceSeconds:   "3600",

	SettingAPIRateLimitCleanupSeconds: "60",
	SettingAPIRateLimitStaleSeconds:   "600",

	SettingEventsSubscriberBufferSize: "64",
	SettingEventsReplayCapacity:       "256",
	SettingEventsSSEKeepaliveSeconds:  "30",

	SettingGraphDefaultMinWeight: "0.1",

	SettingAPIBatchStoreMaxItems: "100",

	SettingExportPageSize: "100",

	// Display-only keys: registered in the admin schema for UI completeness
	// but not yet wired to any consumer. Listed here so the init-time
	// consistency check passes; remove once a consumer is added (and either
	// promote to a Setting* constant or delete the schema entry).
	"enrichment.batch_size":     "10",
	"enrichment.auto_enrich":    "false",
	"memory.default_confidence": "0.9",
	"memory.default_importance": "0.5",
	"api.rate_limit_rps":        "10",
	"api.rate_limit_burst":      "20",
}

// GetDefault returns the built-in default for the given setting key. The
// boolean reports whether the key is registered. Used by callers that need
// the same fallback the runtime cascade lands on (e.g. the schema admin
// surface) so they cannot drift from the values applied at Resolve time.
func GetDefault(key string) (string, bool) {
	v, ok := settingDefaults[key]
	return v, ok
}

// ResolveOrDefault returns the configured value for key, treating an empty
// stored value as "use the default" — appropriate for prompt-shaped settings
// where "" is never a valid configuration. A nil settings pointer routes
// straight to GetDefault, so test callers can pass a typed nil without a
// guard. A *SettingsService parameter (rather than an interface) sidesteps
// the typed-nil-interface trap.
func ResolveOrDefault(ctx context.Context, s *SettingsService, key, scope string) string {
	if s != nil {
		if v, _ := s.Resolve(ctx, key, scope); v != "" {
			return v
		}
	}
	def, _ := GetDefault(key)
	return def
}

// SettingsRepository defines the persistence operations needed by the settings service.
type SettingsRepository interface {
	Get(ctx context.Context, key string, scope string) (*model.Setting, error)
	Set(ctx context.Context, setting *model.Setting) error
	Delete(ctx context.Context, key string, scope string) error
	ListByScope(ctx context.Context, scope string) ([]model.Setting, error)
}

// Settings cache TTL bounds how long a Resolve hit lives in memory before
// the next read goes back to the repo. Operator changes via Set / Delete
// invalidate the affected key immediately; the TTL covers writes from
// outside the SettingsService (direct SQL, restore-from-backup) and bounds
// the staleness hot-path callers have to tolerate. Read once at
// SettingsService construction from SettingSettingsCacheTTLSeconds; runtime
// changes require server restart (the cache TTL itself cannot hot-reload
// without self-reference).

type settingsCacheEntry struct {
	value     string
	expiresAt time.Time
}

// SettingsService provides cascading settings resolution with built-in defaults,
// type-safe accessors, and convenience methods for common settings. Resolve
// hits a small TTL cache so worker loops and per-job cascade resolutions do
// not hammer the repo for values that change rarely.
type SettingsService struct {
	repo     SettingsRepository
	mu       sync.RWMutex
	cache    map[string]settingsCacheEntry
	cacheTTL time.Duration
}

// NewSettingsService creates a new SettingsService with the given repository.
// The cache TTL is bootstrapped from the registered default for
// SettingSettingsCacheTTLSeconds because the service itself is the resolver
// for that key — using the resolver before it has a TTL would self-reference.
// Operators wanting to change the cache TTL must update the setting and
// restart.
func NewSettingsService(repo SettingsRepository) *SettingsService {
	def := settingDefaults[SettingSettingsCacheTTLSeconds]
	secs, err := strconv.Atoi(def)
	if err != nil || secs < 1 {
		secs = 30
	}
	s := &SettingsService{
		repo:     repo,
		cache:    make(map[string]settingsCacheEntry),
		cacheTTL: time.Duration(secs) * time.Second,
	}
	// Promote a stored value if present — Resolve goes through the repo,
	// not through s.cache, so this lookup is safe even before cacheTTL is
	// finalized.
	if repo != nil {
		if setting, err := repo.Get(context.Background(), SettingSettingsCacheTTLSeconds, "global"); err == nil {
			val := unmarshalJSONString(setting.Value)
			if v, perr := strconv.Atoi(val); perr == nil && v >= 1 {
				s.cacheTTL = time.Duration(v) * time.Second
			}
		}
	}
	return s
}

func settingsCacheKey(key, scope string) string {
	return key + "\x00" + scope
}

// Resolve retrieves a setting value as a string through the cascade hierarchy.
// It first checks the database (which cascades project->user->org->global),
// then falls back to built-in defaults. If no value is found anywhere,
// it returns an empty string with no error.
func (s *SettingsService) Resolve(ctx context.Context, key string, scope string) (string, error) {
	cacheKey := settingsCacheKey(key, scope)
	now := time.Now()
	s.mu.RLock()
	if entry, ok := s.cache[cacheKey]; ok && entry.expiresAt.After(now) {
		s.mu.RUnlock()
		return entry.value, nil
	}
	s.mu.RUnlock()

	setting, err := s.repo.Get(ctx, key, scope)
	var value string
	if err == nil {
		value = unmarshalJSONString(setting.Value)
	} else if errors.Is(err, sql.ErrNoRows) {
		if def, ok := settingDefaults[key]; ok {
			value = def
		}
	} else {
		// Real DB errors do not cache — the caller may want to retry.
		return "", fmt.Errorf("resolve setting %q: %w", key, err)
	}

	s.mu.Lock()
	s.cache[cacheKey] = settingsCacheEntry{value: value, expiresAt: now.Add(s.cacheTTL)}
	s.mu.Unlock()
	return value, nil
}

// invalidateCache drops the cached entry for one (key, scope) pair so the
// next Resolve hits the repo. Called from Set / Delete to make operator
// changes visible immediately.
func (s *SettingsService) invalidateCache(key, scope string) {
	s.mu.Lock()
	delete(s.cache, settingsCacheKey(key, scope))
	s.mu.Unlock()
}

// ResolveFloatInRange resolves a numeric setting and clamps it through a
// range filter, returning fallback when the configured value is missing,
// unparseable, or outside [min, max]. Used for boot-time hydration helpers
// where the caller wants a single guaranteed-valid float and an explicit
// default — collapses the common `if v, err := ResolveFloat(...); err == nil
// && v >= min && v <= max { dst = v }` block.
func (s *SettingsService) ResolveFloatInRange(ctx context.Context, key, scope string, min, max, fallback float64) float64 {
	if v, err := s.ResolveFloat(ctx, key, scope); err == nil && v >= min && v <= max {
		return v
	}
	return fallback
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

// ResolveIntWithDefault resolves an int setting, falling back to the value
// registered in settingDefaults when the resolved value is missing, empty, or
// fails to parse. The init-time consistency check in storage/admin enforces
// that every numeric schema entry has a matching settingDefaults entry, so a
// missing default is a programmer error and we panic to surface it.
func (s *SettingsService) ResolveIntWithDefault(ctx context.Context, key, scope string) int {
	if s != nil {
		if v, err := s.ResolveInt(ctx, key, scope); err == nil {
			return v
		}
	}
	def, ok := settingDefaults[key]
	if !ok {
		panic("settings: ResolveIntWithDefault called for key with no registered default: " + key)
	}
	i, err := strconv.Atoi(def)
	if err != nil {
		panic("settings: registered default for " + key + " is not a valid int: " + def)
	}
	return i
}

// ResolveFloatWithDefault resolves a float setting, falling back to the value
// registered in settingDefaults. Same panic-on-missing-default contract as
// ResolveIntWithDefault.
func (s *SettingsService) ResolveFloatWithDefault(ctx context.Context, key, scope string) float64 {
	if s != nil {
		if v, err := s.ResolveFloat(ctx, key, scope); err == nil {
			return v
		}
	}
	def, ok := settingDefaults[key]
	if !ok {
		panic("settings: ResolveFloatWithDefault called for key with no registered default: " + key)
	}
	f, err := strconv.ParseFloat(def, 64)
	if err != nil {
		panic("settings: registered default for " + key + " is not a valid float: " + def)
	}
	return f
}

// ResolveDurationSecondsWithDefault resolves an int setting interpreted as a
// number of seconds, returning the corresponding time.Duration. Falls back to
// the registered default if the configured value is missing or unparseable.
func (s *SettingsService) ResolveDurationSecondsWithDefault(ctx context.Context, key, scope string) time.Duration {
	return time.Duration(s.ResolveIntWithDefault(ctx, key, scope)) * time.Second
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

	if err := s.repo.Set(ctx, setting); err != nil {
		return err
	}
	s.invalidateCache(key, scope)
	return nil
}

// Delete removes a setting at the given scope.
func (s *SettingsService) Delete(ctx context.Context, key string, scope string) error {
	if err := s.repo.Delete(ctx, key, scope); err != nil {
		return err
	}
	s.invalidateCache(key, scope)
	return nil
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
