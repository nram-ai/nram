package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// Provider slot config is stored in the settings table as a JSON blob under
// the key provider.<slot> for each slot in provider.Slots (the canonical
// list); see provider.SlotDef.SettingKey. The .type/.url/.key/.model
// sub-field constants below address individual fields surfaced to the admin
// schema.

// Well-known setting keys.
const (
	SettingEmbedProvider = "provider.embedding.type"
	SettingEmbedURL      = "provider.embedding.url"
	SettingEmbedKey      = "provider.embedding.key"
	SettingEmbedModel    = "provider.embedding.model"
	// Exact-match embedding cache. Output-neutral: a hit returns the same
	// vector the provider would have produced, skipping a redundant call.
	SettingEmbeddingCacheEnabled    = "provider.embedding_cache.enabled"
	SettingEmbeddingCacheMaxEntries = "provider.embedding_cache.max_entries"
	SettingEmbeddingCacheTTLSeconds = "provider.embedding_cache.ttl_seconds"
	SettingFactProvider             = "provider.fact.type"
	SettingFactURL                  = "provider.fact.url"
	SettingFactKey                  = "provider.fact.key"
	SettingFactModel                = "provider.fact.model"
	SettingEntityProvider           = "provider.entity.type"
	SettingEntityURL                = "provider.entity.url"
	SettingEntityKey                = "provider.entity.key"
	SettingEntityModel              = "provider.entity.model"
	SettingEnrichmentEnabled        = "enrichment.enabled"
	// SettingEnrichmentPaused is the operator pause control for the worker
	// pool. It is a runtime control flag, not an operator-config knob, so it
	// is intentionally absent from settingDefaults and the UI settings schema;
	// ResolveBool returns false (unpaused) when it is unset.
	SettingEnrichmentPaused = "enrichment.paused"
	SettingDedupThreshold   = "enrichment.dedup_threshold"

	// Pre-insert paraphrase guard run on each extracted-fact child during
	// enrichment. When a fact's cosine to its parent (or a previously-accepted
	// sibling in the same job) is at or above the threshold, the child is not
	// inserted; its tags are merged into the parent and a lineage row with
	// relation extracted_fact_suppressed is written. Fall back to
	// SettingDedupThreshold when the threshold key is unset.
	SettingExtractedFactGuardEnabled        = "enrichment.extracted_fact_guard_enabled"
	SettingExtractedFactParaphraseThreshold = "enrichment.extracted_fact_paraphrase_threshold"
	SettingExtractedFactBackfillBatchSize   = "enrichment.extracted_fact_backfill_batch_size"
	SettingRankWeightSim                    = "ranking.weight.similarity"
	SettingRankWeightRec                    = "ranking.weight.recency"
	SettingRankWeightImp                    = "ranking.weight.importance"
	SettingRankWeightFreq                   = "ranking.weight.frequency"
	SettingRankWeightGraph                  = "ranking.weight.graph_relevance"
	SettingRankWeightConf                   = "ranking.weight.confidence"
	SettingRankWeightOrigin                 = "ranking.weight.origin"
	SettingRankWeightMmr                    = "ranking.weight.mmr_lambda"
	SettingTokenRetention                   = "usage.token_retention_days"
	SettingTokenCostRates                   = "usage.cost_rates"

	// Hybrid recall fusion. Off by default; flipping enabled turns on
	// parallel vector + BM25/tsvector retrieval with RRF fusion. The two
	// weights govern each channel's RRF contribution; rrf_k is the
	// canonical Cormack-Clarke-Buettcher constant (60 dampens deep-tail
	// noise without flattening the head of either ranked list).
	SettingRecallFusionEnabled          = "recall.fusion.enabled"
	SettingRecallFusionK                = "recall.fusion.rrf_k"
	SettingRecallFusionVecW             = "recall.fusion.vector_weight"
	SettingRecallFusionLexW             = "recall.fusion.lexical_weight"
	SettingRecallFusionNormalizePerChan = "recall.fusion.normalize_per_channel"

	// Dreaming system-level settings (global scope).
	SettingDreamingEnabled            = "dreaming.enabled"
	SettingDreamMaxTokensPerCycle     = "dreaming.max_tokens_per_cycle"
	SettingDreamMaxTokensPerCall      = "dreaming.max_tokens_per_call"
	SettingDreamCooldown              = "dreaming.cooldown_seconds"
	SettingDreamMinInterval           = "dreaming.min_interval_seconds"
	SettingDreamInitialConfidence     = "dreaming.initial_confidence"
	SettingDreamSupersessionThreshold = "dreaming.supersession_threshold"
	SettingDreamLogRetention          = "dreaming.log_retention_days"
	// SettingDreamLLMConcurrency bounds how many of a dream phase's per-item
	// LLM/embedding calls run in parallel. 1 (default) keeps every phase
	// sequential; raise only with a multi-GPU or hosted provider. A cycle runs
	// alone (the scheduler gates on enrichment being idle), so this fan-out is
	// the cycle's entire provider concurrency.
	SettingDreamLLMConcurrency = "dreaming.llm_concurrency"

	// Novelty audit. A dream synthesis must contain at least one fact not
	// present in any of its source memories. Hybrid check: max-cosine
	// embedding similarity vs sources gates whether the LLM judge runs.
	// Backfill applies the same rule to historical dream rows incrementally.
	SettingDreamNoveltyEnabled            = "dreaming.novelty.enabled"
	SettingDreamNoveltyEmbedHighThreshold = "dreaming.novelty.embed_high_threshold"
	SettingDreamNoveltyEmbedLowThreshold  = "dreaming.novelty.embed_low_threshold"
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

	// Per-phase budget fractions applied at the runner level. Each LLM-spending
	// phase receives a SubSlice of dreaming.max_tokens_per_cycle so one phase
	// cannot consume the whole envelope and starve later phases. Fractions are
	// interpreted relative to the cycle TOTAL (not Remaining at phase entry),
	// so reservations are stable across cycles and operator-tunable.
	// SQL-only phases default to 0.0, which means "no per-phase slice; share
	// the root budget": they run normally because they don't call WrapLLMCall
	// and therefore don't consume the budget. Hot-reloadable per cycle.
	SettingDreamEntityDedupFraction          = "dreaming.entity_dedup.budget_fraction"
	SettingDreamEmbeddingBackfillFraction    = "dreaming.embedding_backfill.budget_fraction"
	SettingDreamAugmentationBackfillFraction = "dreaming.augmentation_backfill.budget_fraction"
	SettingDreamMultiVectorBackfillFraction  = "dreaming.multi_vector_backfill.budget_fraction"
	SettingDreamParaphraseFraction           = "dreaming.paraphrase_dedup.budget_fraction"
	SettingDreamTransitiveFraction           = "dreaming.transitive.budget_fraction"
	SettingDreamContradictionFraction        = "dreaming.contradiction.budget_fraction"
	SettingDreamConsolidationFraction        = "dreaming.consolidation.budget_fraction"
	SettingDreamPruningFraction              = "dreaming.pruning.budget_fraction"
	SettingDreamWeightAdjustFraction         = "dreaming.weight_adjustment.budget_fraction"

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

	// Per-cycle stale-fetch caps for the three dream phases that load
	// candidate pools via the SQL-level stale predicate. Caps the maximum
	// number of stale rows the phase pulls into memory in a single cycle
	// so working-set size is bounded by setting rather than by namespace
	// size. When stale row count exceeds the cap, the phase processes the
	// oldest-stale subset and reports residual=true so the next cycle
	// drains the rest.
	SettingDreamParaphraseStaleFetchMax    = "dreaming.paraphrase.stale_fetch_max"
	SettingDreamConsolidationStaleFetchMax = "dreaming.consolidation.stale_fetch_max"
	SettingDreamContradictionStaleFetchMax = "dreaming.contradiction.stale_fetch_max"

	// Pruning phase streaming chunk. Pruning processes the namespace one
	// batch at a time (not stamp-driven) because it has zero LLM cost and
	// must visit every memory each cycle for confidence decay. This
	// controls the per-batch row count: lower values reduce per-batch
	// memory at the cost of more transactions per cycle.
	SettingDreamPruningBatchSize = "dreaming.pruning.batch_size"

	// Embedding-backfill phase: scans for memories whose embedding_dim is
	// recorded but whose corresponding memory_vectors_<dim> row is missing
	// (the no_vector divergence the paraphrase phase observes), then either
	// re-embeds via the live embedder or clears embedding_dim so the row
	// state matches the vector store. Runs before paraphrase dedup so the
	// downstream phase sees the repaired state in the same cycle.
	SettingDreamEmbeddingBackfillEnabled     = "dreaming.embedding_backfill.enabled"
	SettingDreamEmbeddingBackfillCapPerCycle = "dreaming.embedding_backfill.cap_per_cycle"

	// Augmentation-backfill phase. Enqueues query-augmentation enrichment jobs
	// for live memories whose embedding was never built from augmented queries
	// (augmented_embedding_at IS NULL), e.g. dream syntheses or stores whose
	// augmentation step was skipped because the augment provider was briefly
	// unavailable. Automates what the admin "backfill augmentation" button does,
	// so stranded rows self-heal each cycle. Enqueue-only (no LLM calls in the
	// phase), so it carries no budget fraction.
	SettingDreamAugmentationBackfillEnabled     = "dreaming.augmentation_backfill.enabled"
	SettingDreamAugmentationBackfillCapPerCycle = "dreaming.augmentation_backfill.cap_per_cycle"

	// Multi-vector-backfill phase. Enqueues facet-only enrichment jobs
	// (JobMarkerOnlyMultiVector) for live memories that carry a stored vector
	// but have not been faceted (faceted_at IS NULL), automating what the admin
	// "backfill multi-vector" button does so faceting self-drains each cycle.
	// Gated additionally by enrichment.multi_vector.enabled. Enqueue-only (no
	// LLM calls in the phase), so it carries no budget fraction.
	SettingDreamMultiVectorBackfillEnabled     = "dreaming.multi_vector_backfill.enabled"
	SettingDreamMultiVectorBackfillCapPerCycle = "dreaming.multi_vector_backfill.cap_per_cycle"

	// Weight-adjustment phase tuning. support_gain is the multiplier alpha in
	// weight *= 1 + alpha * (support - 1) when a relationship's supporting
	// memories sum to more than one unit of confidence; with the default 0.05
	// a relationship attested by three Confidence=1.0 memories rises 10% per
	// cycle, slow enough that orphan decay still wins on truly dead edges.
	// recall_reinforce_delta is the additive weight bump applied per recall
	// touch; capped by the existing 2.0 ceiling at the SQL layer.
	SettingDreamingWeightSupportGain          = "dreaming.weight.support_gain"
	SettingDreamingWeightRecallReinforceDelta = "dreaming.weight.recall_reinforce_delta"

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

	// Query augmentation. When enabled, the enrichment worker generates N
	// paraphrased query forms per memory at ingest time and prepends them to
	// the memory content before embedding, so a single vector captures both
	// the fact and the ways someone might phrase a query for it. Off by
	// default; flip only after the canned recall regression set shows no
	// contamination-probe regressions plus measurable improvement on 3 or
	// more of 7 stress angles. The model comes from the query_augment provider
	// slot (which falls back to the fact provider).
	SettingQueryAugmentEnabled       = "enrichment.query_augment.enabled"
	SettingQueryAugmentCount         = "enrichment.query_augment.count"
	SettingQueryAugmentMaxInputChars = "enrichment.query_augment.max_input_chars"
	SettingQueryAugmentMaxTokens     = "enrichment.query_augment.max_tokens"

	// Multi-vector facets: split a memory into sentence-clustered topic facets
	// (plus the pooled whole-memory facet 0) so a query about one sub-topic of a
	// multi-topic memory retrieves it at that sub-topic's strength. On by
	// default; it adds per-memory sentence-embedding cost during enrichment, and
	// the facet-only backfill recovers facets for memories stored beforehand.
	SettingMultiVectorEnabled          = "enrichment.multi_vector.enabled"
	SettingMultiVectorFacetThreshold   = "enrichment.multi_vector.facet_threshold"
	SettingMultiVectorMaxFacets        = "enrichment.multi_vector.max_facets"
	SettingMultiVectorEmbedConcurrency = "enrichment.multi_vector.embed_concurrency"

	SettingQdrantAddr             = "qdrant.addr"
	SettingQdrantAPIKey           = "qdrant.api_key"
	SettingQdrantUseTLS           = "qdrant.use_tls"
	SettingQdrantPoolSize         = "qdrant.pool_size"
	SettingQdrantKeepAliveTime    = "qdrant.keepalive_time"
	SettingQdrantKeepAliveTimeout = "qdrant.keepalive_timeout"

	// HNSW (pure-Go SQLite-backed vector index) tuning. All four are read
	// once at boot when the HNSW cache is constructed, so changes require a
	// server restart. M and EfConstruction additionally only affect newly-
	// built indexes: existing indexes carry their construction-time values
	// in their on-disk snapshot.
	SettingHNSWM                = "hnsw.m"
	SettingHNSWEfConstruction   = "hnsw.ef_construction"
	SettingHNSWEfSearch         = "hnsw.ef_search"
	SettingHNSWMaxLoadedIndexes = "hnsw.max_loaded_indexes"

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

	// Recall reinforcement event relationship cap. Mirrors the memory cap for
	// the relationship.reinforced event payload. Caps how many relationship
	// IDs are attached before truncation.
	SettingReinforcementEventRelationshipCap = "reconsolidation.relationship_event_cap"

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
	SettingEnrichmentWorkerBatchClaimSize         = "enrichment.worker.batch_claim_size"
	SettingEnrichmentWorkerLLMConcurrency         = "enrichment.worker.llm_concurrency"
	SettingEnrichmentWorkerEmbedTimeoutSeconds    = "enrichment.worker.embed_timeout_seconds"
	SettingEnrichmentWorkerEmbedInputCap          = "enrichment.worker.embed_input_cap"
	SettingEnrichmentWorkerBreakerEscalateSeconds = "enrichment.worker.breaker_error_escalate_seconds"
	SettingEnrichmentWorkerMaxBackoffSeconds      = "enrichment.worker.max_backoff_seconds"
	SettingEnrichmentWorkerCountSQLite            = "enrichment.worker.count_sqlite"
	SettingEnrichmentWorkerCountPostgres          = "enrichment.worker.count_postgres"
	SettingEnrichmentWorkerPollIntervalSeconds    = "enrichment.worker.poll_interval_seconds"
	// SettingEnrichmentPoolTickIntervalSeconds controls how often the worker
	// pool publishes enrichment.pool.tick events for the admin UI's live
	// banner. Cheap (one tick per pool, not per job), so a fast cadence is
	// fine. Default 5s.
	SettingEnrichmentPoolTickIntervalSeconds  = "enrichment.pool_tick_interval_seconds"
	SettingEnrichmentIngestionRationaleMaxLen = "enrichment.ingestion.rationale_max_len"

	// Stuck-job detection and recovery for the enrichment worker pool.
	// Mirror the dreaming.stuck_* / dreaming.heartbeat_interval_seconds
	// design: workers tick heartbeat_at every HeartbeatSeconds while a job
	// is claimed; the StuckJobSweeper triggers on updated_at staleness past
	// StuckThreshold (the safer signal, heartbeat is for diagnostics) and
	// requeues stuck rows on a SweepSeconds cadence. Threshold must exceed
	// the longest legitimate batch runtime so a slow LLM is not mistaken
	// for a dead worker.
	SettingEnrichmentHeartbeatSeconds = "enrichment.worker.heartbeat_seconds"
	SettingEnrichmentStuckThreshold   = "enrichment.stuck_threshold_seconds"
	SettingEnrichmentStuckSweep       = "enrichment.stuck_sweep_seconds"
	// Backstop: requeue any in-flight row whose claimed_at exceeds this
	// duration, regardless of updated_at. The runtime sweeper's primary
	// signal is updated_at staleness, which a still-ticking heartbeat
	// (TickHeartbeat matches on DB-column claimed_by, not on what the
	// worker is actively processing) can mask indefinitely. The cap is the
	// hard wall: any claim that has lived longer than the maximum plausible
	// batch runtime is considered wedged and gets requeued. Must exceed
	// every legitimate single-job duration; default 7200s (2h) is well
	// above any expected runtime.
	SettingEnrichmentClaimMaxAge = "enrichment.claim_max_age_seconds"

	// Retention for permanently-failed enrichment jobs. The StuckJobSweeper
	// hard-deletes rows in status='failed' whose updated_at is older than this
	// many days so the failed backlog cannot grow without bound: a single-GPU
	// local provider routinely accumulates tens of thousands of failed jobs
	// that nothing else ever reaps. 0 disables pruning. Default 7 days: after a
	// week a failed job is stale (its memory's content has almost certainly
	// moved on), so reaping it keeps the queue lean.
	SettingEnrichmentFailedRetentionDays = "enrichment.failed_retention_days"

	// Fact and entity extraction LLM-call tunables. Resolved per call by both
	// ExtractionService (sync HTTP path) and WorkerPool (async queue worker)
	// so changes hot-reload within the cascade cache TTL. max_tokens caps
	// completion tokens; raise when high-density inputs hit
	// finish_reason=length. Temperature is split sync vs async
	// to preserve pre-refactor behavior (sync was 0.1, async was 0.2);
	// operators converge by setting both keys equal.
	SettingFactExtractionMaxTokens          = "enrichment.fact_extraction.max_tokens"
	SettingEntityExtractionMaxTokens        = "enrichment.entity_extraction.max_tokens"
	SettingFactExtractionSyncTemperature    = "enrichment.fact_extraction.sync.temperature"
	SettingFactExtractionAsyncTemperature   = "enrichment.fact_extraction.async.temperature"
	SettingEntityExtractionSyncTemperature  = "enrichment.entity_extraction.sync.temperature"
	SettingEntityExtractionAsyncTemperature = "enrichment.entity_extraction.async.temperature"

	// Write-path entity resolution: embed the candidate name and cosine-match
	// against existing entity vectors before creating a new entity, so a
	// near-duplicate is merged at creation instead of only in dreaming. Disabled
	// by setting the enabled flag false; threshold matches the dreaming merge.
	SettingEntityResolutionCosineEnabled   = "enrichment.entity_resolution.cosine_enabled"
	SettingEntityResolutionCosineThreshold = "enrichment.entity_resolution.cosine_threshold"

	// Semantic vocabulary classifier: when an extracted relation or entity type
	// is not in the static synonym map, embed it and assign the nearest canonical
	// term whose gloss is at least this cosine-similar, instead of dropping it to
	// related to / other. Keeps the meaningful-label rate high on an 8b model's
	// open verb/type vocabulary. Set the threshold to 1 to effectively disable.
	SettingSemanticVocabThreshold = "enrichment.semantic_vocab.threshold"

	// Extraction robustness: split a memory whose estimated content tokens
	// exceed the threshold into overlapping chunks, extract per chunk, and merge,
	// so dense memories no longer truncate at the model's max_tokens. The
	// continuation cap bounds how many finish_reason=length follow-up passes run.
	SettingExtractionChunkThresholdTokens  = "enrichment.extraction.chunk_threshold_tokens"
	SettingExtractionChunkOverlapTokens    = "enrichment.extraction.chunk_overlap_tokens"
	SettingExtractionContinuationMaxPasses = "enrichment.extraction.continuation_max_passes"

	// Dreaming worker tuning beyond what the existing dreaming.* keys cover.
	SettingDreamContradictionNeighbors = "dreaming.contradiction.neighbors_per_anchor"
	SettingDreamEntityMergeThreshold   = "dreaming.entity_merge.cosine_threshold"
	SettingDreamSchedulerPollSeconds   = "dreaming.scheduler.poll_interval_seconds"

	// Stuck-cycle detection and recovery. The runner ticks heartbeat_at every
	// HeartbeatInterval while a phase is executing; the admin UI surfaces
	// "no recent activity" when now() - heartbeat_at > HeartbeatStale. The
	// abandon button (and the StuckCycleSweeper) fire only when
	// now() - updated_at > StuckThreshold, which is intentionally
	// conservative: it must exceed the longest legitimate single-phase
	// runtime so we never abandon a cycle that might still complete.
	SettingDreamHeartbeatInterval = "dreaming.heartbeat_interval_seconds"
	SettingDreamHeartbeatStale    = "dreaming.heartbeat_stale_seconds"
	SettingDreamStuckThreshold    = "dreaming.stuck_threshold_seconds"
	SettingDreamStuckSweep        = "dreaming.stuck_sweep_seconds"

	// Lifecycle sweep tuning. SweepInterval is read at start (restart);
	// BatchSize / OrphanGrace are read on every sweep so they hot-reload.
	SettingLifecycleSweepIntervalSeconds = "lifecycle.sweep_interval_seconds"
	SettingLifecycleBatchSize            = "lifecycle.batch_size"
	SettingLifecycleOrphanGraceSeconds   = "lifecycle.orphan_grace_seconds"

	// API rate-limit per-user-bucket cleanup. Read once at startup; changes
	// require server restart.
	SettingAPIRateLimitCleanupSeconds = "api.rate_limit.cleanup_interval_seconds"
	SettingAPIRateLimitStaleSeconds   = "api.rate_limit.stale_after_seconds"

	// Dashboard session JWT timings. token_ttl is the lifetime applied to
	// every session JWT issued by the SPA login flows (password, IdP,
	// WebAuthn, setup). refresh_threshold is how stale an in-flight session
	// JWT must be before the auth middleware silently reissues it via the
	// X-Refreshed-Token response header. Both hot-reload via the settings
	// cache. The threshold should be less than the TTL; otherwise refresh
	// will never fire and the session collapses back to the fixed-TTL
	// (force-relogin) behavior.
	SettingAuthSessionTokenTTLSeconds         = "auth.session_token_ttl_seconds"
	SettingAuthSessionRefreshThresholdSeconds = "auth.session_refresh_threshold_seconds"

	// In-process event bus. subscriber_buffer_size is the per-subscriber
	// channel buffer (drops events on full); replay_capacity is the ring
	// buffer for SSE Last-Event-ID reconnection. Both read once at startup:
	// wrong values can stall subscribers or balloon memory, so both are
	// restart-required and flagged as advanced in their descriptions.
	SettingEventsSubscriberBufferSize = "events.subscriber_buffer_size"
	SettingEventsReplayCapacity       = "events.replay_capacity"
	SettingEventsSSEKeepaliveSeconds  = "events.sse_keepalive_seconds"

	// Diagnostic log store (log_entries). db_capture_enabled toggles writing
	// diagnostic logs to the database in addition to the console; db_level is the
	// minimum level captured to the table, independent of the console LOG_LEVEL;
	// the two retention limits bound the rolling window (count is the hard cap,
	// age is secondary). All hot-reloaded by the logging settings refresher.
	SettingLoggingDBCaptureEnabled = "logging.db_capture_enabled"
	SettingLoggingDBLevel          = "logging.db_level"
	SettingLoggingRetentionMaxRows = "logging.retention_max_rows"
	SettingLoggingRetentionMaxAge  = "logging.retention_max_age_days"

	// Admin graph visualization minimum edge weight. Hot-reloadable.
	SettingGraphDefaultMinWeight = "graph.default_min_weight"

	// Server-side ceiling on edges returned from the /v1/graph endpoint. When
	// the active relationship count for a namespace exceeds this value, the
	// handler returns the top-N edges by weight descending and sets
	// truncated=true on the response with total_edges / returned_edges counts
	// so the admin UI can surface the partial-view banner. Exists because the
	// React force-graph renderer (react-force-graph-3d) constructs a THREE.js
	// Group per node and stalls past low thousands of edges; the cap protects
	// the browser, not the data layer. Operators with capable rendering
	// environments can raise it through the normal settings cascade.
	SettingGraphMaxEdges = "graph.max_edges"

	// Graph visualization d3-force parameters. System defaults are used when a
	// project has not stored its own override. center_gravity is the centering
	// force strength (forceCenter); charge_strength is the many-body repulsion
	// (negative values repel, d3 convention; the UI exposes this as a positive
	// "repulsion" knob and flips the sign at the boundary); link_distance is
	// the target edge length passed to forceLink. Per-project overrides ship
	// through the project settings JSON; per-cycle reads happen client-side, so
	// changes hot-reload without server round-trips beyond the projects fetch.
	SettingGraphCenterGravity  = "graph.center_gravity"
	SettingGraphChargeStrength = "graph.charge_strength"
	SettingGraphLinkDistance   = "graph.link_distance"

	// Batch store request item cap. Raising this widens the per-request DoS
	// surface; description warns and the value is bounded by an internal
	// safety floor in BatchStore validation.
	SettingAPIBatchStoreMaxItems = "api.batch_store.max_items"

	// Export pagination size for memories. Hot-reloadable.
	SettingExportPageSize = "export.page_size"

	// Self-service export job knobs. ArtifactDir is the filesystem root the
	// worker writes per-user zips into (one file per job under
	// <root>/<user_id>/<job_id>.zip); empty falls through to <cwd>/exports
	// so the default works in dev without operator setup. TTLHours bounds how
	// long a completed artifact survives before the cleanup sweep deletes it
	// and flips the row to status='expired'. MaxPerUserPerDay caps how many
	// exports a single user can enqueue in a rolling 24h window, which prevents one
	// account from queueing hundreds of large zips.
	SettingExportArtifactDir      = "export.artifact_dir"
	SettingExportTTLHours         = "export.ttl_hours"
	SettingExportMaxPerUserPerDay = "export.max_per_user_per_day"

	// Recall scoring and pagination. Hot-reloadable. Operators reach for
	// these during incident response to retune the recency / over-fetch
	// math without redeploying.
	//
	// recency_decay_per_hour drives the exp(-rate * hours_since_creation)
	// term in computeScore: 0.01 → ~69h half-life, 0.02 → ~35h, 0.005 →
	// ~138h. graph_hop_multiplier scales the per-hop relevance reduction
	// applied to graph-traversal contributions. default_limit is the
	// fallback when callers pass limit <= 0; default_depth is the same for
	// graph_depth. overfetch_multiplier widens the candidate pool the
	// score-and-rerank pass selects from (limit * mul); overfetch_min
	// floors the result so small limits still get a workable pool.
	SettingRankingRecencyDecayPerHour = "ranking.recency.decay_per_hour"
	SettingRankingGraphHopMultiplier  = "ranking.graph.hop_multiplier"
	SettingRecallDefaultLimit         = "recall.default_limit"
	SettingRecallMaxLimit             = "recall.max_limit"
	SettingRecallGraphDefaultDepth    = "recall.graph.default_depth"
	SettingRecallGraphMaxDepth        = "recall.graph.max_depth"
	SettingRecallGraphReserveFraction = "recall.graph.reserve_fraction"
	SettingRecallOverfetchMultiplier  = "recall.overfetch_multiplier"
	SettingRecallOverfetchMin         = "recall.overfetch_min"
	// Cross-namespace vector-channel entity activation: recall activates
	// graph entities by vector similarity across the [project, global]
	// aperture (in addition to the lexical name match) and boosts their
	// connected memories. topk sizes the per-namespace entity fetch.
	SettingRecallGraphVectorActivationEnabled = "recall.graph.vector_activation.enabled"
	SettingRecallGraphVectorActivationTopK    = "recall.graph.vector_activation.topk"
	// SettingRecallGraphMaxEdges is the per-recall graph traversal edge
	// budget, decoupled from SettingGraphMaxEdges (the visualization-endpoint
	// renderer cap). Split fairly per seed inside the recall graph block.
	SettingRecallGraphMaxEdges = "recall.graph.max_edges"

	// Pruning thresholds. relationship_weight_threshold gates the active
	// relationship expiry pass AND the mid-cycle expiry inside the weight
	// adjustment phase: both must read the same key so they cannot drift.
	// effectively_zero is the upper bound for the zero-confidence prune
	// branch (catches contradiction-haircut underflow that an exact `== 0`
	// check would miss).
	SettingDreamPruningRelationshipWeightThreshold = "dreaming.pruning.relationship_weight_threshold"
	SettingDreamPruningEffectivelyZero             = "dreaming.pruning.effectively_zero"

	// Transitive relationship discovery. min_weight is the minimum product
	// weight (rel_ab.weight * rel_bc.weight) for a new transitive edge to
	// be created. max_per_cycle caps creations per cycle; namespace_hard_cap
	// halts creation entirely when the active relationship count exceeds it.
	// namespace_high_water / namespace_low_water drive the pressure-based
	// prune in the pruning phase: once active relationships exceed
	// hard_cap * high_water, the lowest-weight transitive edges are expired
	// down to hard_cap * low_water. low_water must be strictly less than
	// high_water; the SettingsService validator rejects misconfigurations.
	SettingDreamTransitiveMinWeight          = "dreaming.transitive.min_weight"
	SettingDreamTransitiveMaxPerCycle        = "dreaming.transitive.max_per_cycle"
	SettingDreamTransitiveNamespaceHardCap   = "dreaming.transitive.namespace_hard_cap"
	SettingDreamTransitiveNamespaceHighWater = "dreaming.transitive.namespace_high_water"
	SettingDreamTransitiveNamespaceLowWater  = "dreaming.transitive.namespace_low_water"

	// Transitive semantic gates. A relation is only chained (A→B, B→C ⇒ A→C)
	// when BOTH hops carry the SAME relation and that relation is listed in
	// relations (a JSON array of canonical labels). This stops the two ways
	// inference used to pollute the graph: copying the first hop's label onto
	// an unrelated second hop, and treating non-transitive relations (e.g.
	// "wife of") as transitive. max_fanout caps how many same-relation targets
	// a single intermediate node may propagate, bounding blast radius through
	// hub entities even if a non-transitive relation is mistakenly listed.
	SettingDreamTransitiveRelations = "dreaming.transitive.relations"
	SettingDreamTransitiveMaxFanout = "dreaming.transitive.max_fanout"

	// Weight adjustment knobs. tier2_multiplier scales co-mention support
	// (memory touches both endpoints but isn't direct lineage). decay_window_days
	// is the age threshold past which 0.95-per-period decay applies; decay_factor
	// is the per-period multiplier; decay_max_periods caps decay so a very old
	// edge floors at decay_factor^max_periods (~0.60 with defaults).
	// dead_source_multiplier is the halving applied when an edge's recorded
	// singular source memory is soft-deleted AND no live memory still attests
	// the edge. ceiling clamps recomputed weights: informational only for
	// existing rows above the cap until they're naturally rewritten.
	SettingDreamWeightTier2Multiplier      = "dreaming.weight.tier2_multiplier"
	SettingDreamWeightDecayWindowDays      = "dreaming.weight.decay_window_days"
	SettingDreamWeightDecayFactor          = "dreaming.weight.decay_factor"
	SettingDreamWeightDecayMaxPeriods      = "dreaming.weight.decay_max_periods"
	SettingDreamWeightDeadSourceMultiplier = "dreaming.weight.dead_source_multiplier"
	SettingDreamWeightCeiling              = "dreaming.weight.ceiling"

	// Consolidation phase clustering and sampling. alignment_sample_size
	// is the count of user memories used to score alignment per cluster.
	// cluster_overlap_threshold is the word-overlap fraction at which the
	// heuristic clusterer treats two memories as belonging to the same
	// cluster (drives which memories get bundled into a synthesis prompt).
	SettingDreamConsolidationAlignmentSampleSize     = "dreaming.consolidation.alignment_sample_size"
	SettingDreamConsolidationClusterOverlapThreshold = "dreaming.consolidation.cluster_overlap_threshold"
	// cluster_mode selects how consolidation groups candidate memories:
	// "cosine" (default) groups by embedding similarity (semantically coherent
	// clusters); "lexical" uses the legacy word-overlap heuristic. Cosine falls
	// back to lexical for any candidate that has no stored vector.
	// cluster_cosine_threshold is the minimum cosine-to-anchor for cosine mode.
	SettingDreamConsolidationClusterMode            = "dreaming.consolidation.cluster_mode"
	SettingDreamConsolidationClusterCosineThreshold = "dreaming.consolidation.cluster_cosine_threshold"

	// LLM call temperatures. All four point at the OpenAI-compatible
	// completion temperature parameter for their respective phases. Default
	// 0.1 is conservative (more deterministic) which is appropriate for
	// JSON-only outputs; alignment uses 0.3 to allow some judgment latitude
	// when scoring evidence overlap.
	SettingDreamSynthesisTemperature              = "dreaming.synthesis.temperature"
	SettingDreamAlignmentTemperature              = "dreaming.alignment.temperature"
	SettingDreamNoveltyJudgeTemperature           = "dreaming.novelty.judge.temperature"
	SettingDreamContradictionTemperature          = "dreaming.contradiction.temperature"
	SettingEnrichmentConflictTemperature          = "enrichment.conflict.temperature"
	SettingEnrichmentIngestionDecisionTemperature = "enrichment.ingestion_decision.temperature"
	SettingEnrichmentConflictMaxTokens            = "enrichment.conflict.max_tokens"
	SettingEnrichmentIngestionDecisionMaxTokens   = "enrichment.ingestion_decision.max_tokens"
	// SettingEnrichmentTestPromptMaxTokens caps the admin "Test prompt" call.
	// It is a tuning affordance for the admin test surface, not a runtime phase.
	SettingEnrichmentTestPromptMaxTokens = "enrichment.test_prompt.max_tokens"

	// Heartbeat tick timeout for the dream runner. Caps how long a single
	// TickHeartbeat write may block before being skipped. Larger values risk
	// stalling the cycle on a slow DB; smaller values risk spurious "no
	// recent activity" markers if the DB is briefly contended. Hot-reloadable.
	SettingDreamHeartbeatTickTimeoutSeconds = "dreaming.heartbeat_tick_timeout_seconds"

	// Stuck-scan caps. Bounds a single ListStale / ListStaleClaimed call so
	// a flood doesn't lock the writer. Distinct keys for dreaming and
	// enrichment so the two can be tuned independently: the workloads have
	// different stuck-job cardinalities under load.
	SettingDreamStuckScanLimit      = "dreaming.stuck_scan_limit"
	SettingEnrichmentStuckScanLimit = "enrichment.stuck_scan_limit"

	// Default importance / confidence applied to newly-stored memories
	// when the caller does not specify a value. Surfaces the four-way
	// hardcoded duplication that import.go / extract.go / store.go /
	// batch_store.go each carried as separate `0.5` / `1.0` literals.
	// Operators tune these to bias new memories toward / away from the
	// reinforcement floor at the cost of differential decay behavior.
	SettingMemoryDefaultImportance = "memory.default_importance"
	SettingMemoryDefaultConfidence = "memory.default_confidence"

	// MCP CallToolResult per-tool byte budget, expressed in tokens (multiplied
	// by charsPerTokenEstimate at the MCP layer). Bounds the structured +
	// text wire so a single tool response cannot blow the model's effective
	// context. Hot-reloadable. The admin schema enforces Min=100; below that,
	// the truncation sentinel suffix (~108 bytes) cannot fit, which would
	// silently strip the wire signal for clients detecting Tier-3 truncation.
	SettingMCPMaxResultTokens = "mcp.max_result_tokens"
)

// Reconsolidation mode values. Default is shadow so the first real deployment
// is observable-only: events are emitted, but no database values change until
// the operator flips the mode to persist.
const (
	ReconsolidationModeOff     = "off"
	ReconsolidationModeShadow  = "shadow"
	ReconsolidationModePersist = "persist"
)

// Per-phase system-prompt keys. Each LLM phase's tunable instruction (its role,
// rules, and complete output contract/schema) is stored under one of these keys
// and sent as the system message (a stable, cacheable prefix). The dynamic
// memory data is wrapped by a hardcoded per-phase code template and sent as the
// user message via provider.BuildMessages; that wrapper is not a setting. The
// system prompt is the only tunable LLM template.
const (
	SettingFactSystemPrompt               = "enrichment.fact_system_prompt"
	SettingEntitySystemPrompt             = "enrichment.entity_system_prompt"
	SettingIngestionDecisionSystemPrompt  = "enrichment.ingestion_decision.system_prompt"
	SettingQueryAugmentSystemPrompt       = "enrichment.query_augment.system_prompt"
	SettingDreamContradictionSystemPrompt = "dreaming.contradiction_system_prompt"
	SettingDreamSynthesisSystemPrompt     = "dreaming.synthesis_system_prompt"
	SettingDreamAlignmentSystemPrompt     = "dreaming.alignment_system_prompt"
	SettingDreamNoveltyJudgeSystemPrompt  = "dreaming.novelty.judge_system_prompt"
	SettingAskSynthesisSystemPrompt       = "ask.synthesis.system_prompt"
	SettingAskDecompositionSystemPrompt   = "ask.decomposition.system_prompt"
)

// Ask synthesis tool settings. The ask tool runs recall over a wide aperture,
// assembles a neighborhood, and makes one LLM call to synthesize an answer.
// Gated OFF by default (SettingAskEnabled) so it never spends model tokens
// until an operator opts in and configures the dedicated ask provider slot.
const (
	SettingAskEnabled                 = "ask.enabled"
	SettingAskSynthesisTemperature    = "ask.synthesis.temperature"
	SettingAskSynthesisMaxTokens      = "ask.synthesis.max_tokens"
	SettingAskRecallCandidates        = "ask.recall.candidates"
	SettingAskGraphDepth              = "ask.graph.depth"
	SettingAskSiblingsPerCandidate    = "ask.siblings.per_candidate"
	SettingAskNeighborhoodMaxMemories = "ask.neighborhood.max_memories"
	// Confidence calibration maps a cited source's absolute vector cosine onto
	// [0, 1]: a cosine at or below the floor reads as no confidence, at or above
	// the ceiling as full confidence, linear between. Defaults are tuned to the
	// observed qwen3-embedding band (strong matches cluster ~0.55-0.71), so a
	// genuine top hit reads high rather than lukewarm. Tunable per embedder.
	SettingAskConfidenceCosineFloor   = "ask.confidence.cosine_floor"
	SettingAskConfidenceCosineCeiling = "ask.confidence.cosine_ceiling"
	// Neighborhood relevance floor: a recall candidate joins the synthesis
	// neighborhood only if its fused recall score is at least this fraction of
	// the top candidate's. Recall returns up to its limit even when the tail is
	// weak (and its fused ranking can float a high-importance but off-topic
	// memory up), so this drops that tail and makes the neighborhood adaptive to
	// how many memories genuinely match. 0 disables the floor.
	SettingAskNeighborhoodMinScoreRatio = "ask.neighborhood.min_score_ratio"
	// Expansion relevance floor: a graph- or sibling-connected memory joins the
	// neighborhood only if its cosine to the query embedding is at least this.
	// Tag overlap and entity connectivity are not relevance, so the expansion is
	// gated on the actual query match; this keeps connected-but-off-topic
	// memories out while still pulling in genuinely related context recall
	// missed. Tuned to the embedder band (qwen3-embedding strong matches ~0.55+).
	SettingAskExpansionCosineFloor = "ask.expansion.cosine_floor"
	// Query decomposition: before recall, an aggregation/compare/classify question
	// is broken into one focused retrieval sub-query per class so the minority
	// class is not buried by the majority in a single broad recall. Each
	// sub-query is recalled and floored against its own top, then the results are
	// unioned. enabled gates it (under SettingAskEnabled); max_subqueries caps the
	// fan-out; the decomposition completion reuses the ask synthesis provider.
	SettingAskDecompositionEnabled       = "ask.decomposition.enabled"
	SettingAskDecompositionMaxSubqueries = "ask.decomposition.max_subqueries"
	SettingAskDecompositionMaxTokens     = "ask.decomposition.max_tokens"
	SettingAskDecompositionTemperature   = "ask.decomposition.temperature"
)

// Provider prompt-delivery and Ollama keep-warm runtime settings.
const (
	// SettingProviderPromptCacheEnabled controls whether providers that accept
	// explicit cache hints (Anthropic cache_control) mark the system prefix as
	// cacheable. Below a model's minimum cacheable prefix the hint is a no-op.
	SettingProviderPromptCacheEnabled = "provider.prompt_cache.enabled"

	// SettingProviderAnthropicJSONToolUse controls whether JSONMode requests to
	// the Anthropic provider are coerced into a forced `emit_json` tool_use call.
	// Off by default — the native api.anthropic.com path does not need it; enable
	// only for Anthropic-compatible proxies that drop response formatting.
	SettingProviderAnthropicJSONToolUse = "provider.anthropic.json_tool_use.enabled"

	// SettingProviderLLMHostConcurrency bounds how many LLM completion requests
	// may be in flight against a single upstream host at once, aggregated across
	// every worker slot and subsystem (enrichment, dreaming, ask) that targets
	// that host. Default 1 so a fresh install cannot stampede a single-GPU local
	// model; raise it per the host's capacity. A value <= 0 disables the gate.
	SettingProviderLLMHostConcurrency = "provider.llm.host_concurrency"

	// SettingProviderEmbedHostConcurrency is the embedding counterpart: max
	// in-flight embedding requests per host, aggregated across every slot and
	// subsystem. Default 1 for the same reason; raise per the embedder's
	// capacity. A value <= 0 disables the gate.
	SettingProviderEmbedHostConcurrency = "provider.embed.host_concurrency"
)

// Default system-prompt text for each phase: the full static instruction (role,
// rules, and the complete output contract/schema) sent as the system message.
// Pure static text with no fmt verbs; the dynamic memory data is injected by the
// per-phase code wrapper into the user message.
const (
	// minifiedJSONInstruction is appended to every JSON-returning system prompt
	// so the model emits compact JSON. Pretty-printing wastes ~35-43% of output
	// tokens on whitespace; nram parses these responses with encoding/json,
	// which ignores formatting, so this is pure output-token (and latency)
	// savings. The string-value clause stops the model from stripping spaces
	// inside content, queries, rationales, and explanations.
	minifiedJSONInstruction = "Return the JSON minified onto a single line: no spaces, newlines, or indentation between JSON tokens. Do not change whitespace inside string values."

	factSystemPromptText = `You are a fact extraction engine. Given a text, extract all discrete facts as a JSON array. Each fact should be a JSON object with these fields:
- "content": the fact statement (string)
- "confidence": how confident you are in this fact, 0.0 to 1.0 (number)
- "tags": relevant tags for categorization (array of strings)

Hard rules:
- Do NOT emit a fact whose content merely restates the input. If the input is already a single atomic fact, return an empty array [].
- Do NOT emit a fact that differs from the input only by punctuation, capitalization, or whitespace.
- Tag-only deltas are NOT a reason to emit a fact. If the only thing you would add is a new tag on otherwise-identical content, return an empty array; the calling system merges tags from suppressed facts into the parent automatically.
- Only emit facts that introduce a new entity, relationship, quantity, date, cause, consequence, or other proposition not already explicit in the input.
- Do NOT repeat a fact you have already emitted, and do NOT loop the same cluster of facts. Each fact must be distinct. Stop once every distinct fact is listed; never pad the output.

Return ONLY valid JSON. Do not include markdown fences or explanation.` + "\n\n" + minifiedJSONInstruction

	// entitySystemPromptText enumerates the closed entity-type and relation
	// vocabularies as a soft constraint; CanonicalEntityType / CanonicalRelationVocab
	// enforce them deterministically on the write path. The lists here are kept
	// in step with model.CanonicalEntityTypes / model.CanonicalRelations by
	// TestEntitySystemPromptListsClosedVocab (drift guard).
	entitySystemPromptText = `You are an entity and relationship extraction engine. Given a text, extract the named entities and the relationships between them as JSON.

Return a JSON object with two fields:
- "entities": array of objects with fields:
  - "name": the entity's proper name, as short as possible (string)
  - "type": one of EXACTLY these types (string): person, organization, location, product, event, role, date, concept, technology, software, code_symbol, file, data_store, system, configuration, command, vcs_ref, credential, identifier, metric, document, research_artifact, medication, medical_condition, biomarker. If none fit, use "other". Never invent a type outside this list.
  - "properties": optional key-value pairs (object)
- "relationships": array of objects with fields:
  - "source": source entity name (string)
  - "target": target entity name (string)
  - "relation": one of EXACTLY these relations (string). Map your verb to the closest one; do NOT invent verbs. Guide:
    - member of: employment/study/affiliation (worked at, studied at, joined, member of)
    - produces: creation/authorship (authored, founded, built, developed, created, wrote)
    - uses: consumes/operates/calls (uses, written in, calls, deployed, adopted)
    - depends on: needs/hosted by (requires, served by, runs on, relies on)
    - affects: manages/leads/changes (managed, led, oversaw, modifies, influences)
    - family of: kinship (married to, mother of, brother of, child of)
    - has property: traits/titles/credentials (has, held title, earned, characterized by)
    - located in: place (lives in, based in, near)
    - part of / has part / is a / references / implements / supports / compares to / interacts with: structural/semantic links
    If truly none fit, use "related to". Never output a relation outside this list.
  - "weight": confidence/strength 0.0 to 1.0 (number)
  - "temporal": "current", "as of <date>", "previously", or "no longer" (string, default "current")

Hard rules:
- An entity is a NAMED thing (a person, place, system, file, drug, etc.), not a statement. Do NOT extract whole sentences, claims, opinions, questions, code snippets, SQL, shell commands, or file contents as entities. A name longer than a short phrase is almost always wrong.
- Do NOT repeat an entity or relationship you have already emitted, and do NOT loop. Each entity and relationship must be distinct.

Return ONLY valid JSON. Do not include markdown fences or explanation.` + "\n\n" + minifiedJSONInstruction

	ingestionDecisionSystemPromptText = `You are an ingestion decision engine. You do NOT converse. You output JSON only.

A new memory has just arrived. Below it are the candidate near-neighbour memories that already exist (with their IDs and creation times). Decide what to do with the new memory by comparing its facts to each candidate's facts.

Operations (choose exactly one):
- "ADD": the new memory contributes at least one fact not in any candidate, and it does not simply correct or fully restate one candidate.
- "UPDATE": the new memory should REPLACE exactly one candidate, because it is a newer or more specific version of that candidate's facts. The replaced candidate is discarded.
- "DELETE": the new memory adds nothing; every fact it states is already present in one candidate (it equals that candidate or is a subset of it).
- "NONE": overlaps a candidate but is not a clean replacement or duplicate.

How to choose:
1. States any fact no candidate contains, without merely correcting a value? -> ADD.
2. Same subject as one candidate but a newer/more specific/corrected version? -> UPDATE.
3. All facts already covered by one candidate (equal or subset)? -> DELETE.
4. Otherwise -> NONE.

Critical: UPDATE DISCARDS the candidate. A vaguer or less detailed memory never supersedes a more detailed one; that is DELETE.

Worked examples:
- new "Ben enjoys coffee." / candidate "Ben enjoys coffee and tea." -> DELETE.
- new "The cache TTL is 600 seconds." / candidate "The cache TTL is 300 seconds." -> UPDATE.
- new "Ben enjoys coffee and cycles to work." / candidate "Ben enjoys tea." -> ADD.

Hard rules:
- target_id required for UPDATE/DELETE (verbatim candidate ID); null for ADD/NONE.
- Rationale: one short sentence (under 200 characters) naming the candidate ID compared against.

Output ONLY this JSON, nothing else:
{"operation":"ADD","target_id":null,"rationale":"..."}
or
{"operation":"UPDATE","target_id":"candidate-uuid","rationale":"..."}
or
{"operation":"DELETE","target_id":"candidate-uuid","rationale":"..."}
or
{"operation":"NONE","target_id":null,"rationale":"..."}` + "\n\n" + minifiedJSONInstruction

	queryAugmentSystemPromptText = `You are a query augmentation engine. You do NOT converse. You output JSON only.

Given a memory's content, generate the requested number of short, distinct natural-language questions or phrases a user might use to retrieve this memory. Vary the phrasings: cover synonyms, partial-fact lookups, and the most likely way the information would be asked about. Keep each query under 120 characters and avoid restating the memory verbatim.

OUTPUT FORMAT, read carefully:
- Output ONLY a JSON array of strings.
- EVERY element MUST be wrapped in DOUBLE QUOTES ("..."). Not single quotes. Not backticks. Not bare words.
- No prose before or after the array. No markdown fences (no ` + "```" + `). No trailing commas. No comments.
- Use \" to escape a literal double quote inside an element.

CORRECT:   ["what time does X start","X start time","schedule for X"]
WRONG (missing quotes):   [what time does X start, X start time, schedule for X]
WRONG (single quotes):    ['what time does X start', 'X start time']
WRONG (fenced / prose):   Here you go: ` + "```json" + ` [...] ` + "```" + "\n\n" + minifiedJSONInstruction

	contradictionSystemPromptText = `You are a contradiction detector. You do NOT converse. You output JSON only.

Determine if the two statements below contradict each other.

When they contradict, also identify which is more likely correct and set "winner" to "a", "b", or "tie". Use "tie" when the contradiction is real but neither side is clearly right (subjective claims, partial overlap, claims about different time periods, equally plausible interpretations).

Output ONLY this JSON, nothing else:
{"contradicts":true,"winner":"a","explanation":"reason"}
or
{"contradicts":true,"winner":"b","explanation":"reason"}
or
{"contradicts":true,"winner":"tie","explanation":"reason"}
or
{"contradicts":false,"winner":null,"explanation":"reason"}` + "\n\n" + minifiedJSONInstruction

	synthesisSystemPromptText = `You are a knowledge synthesizer. You do NOT converse, greet, or ask questions. You output ONLY the synthesized text.

Combine the following pieces of information into a single concise paragraph that preserves all key facts. Do not lose details. Do not add commentary. Do not prefix with "Here is" or similar.

Output ONLY the synthesized text:`

	askSynthesisSystemPromptText = `You answer the user's question using only the memory neighborhood provided in the user message; no outside knowledge.

Combine facts across multiple memories when the question calls for it: a list, comparison, or classification usually spans several memories. Summarize in your own words and keep it concise (a few sentences); give specific terms, names, and numbers exactly, but do not copy a memory's full text or reproduce long passages.

Lead with the fact itself, not a restatement of the question. Cite the memory ids you draw on inline, in square brackets right after the claim they support, e.g. [a1b2c3d4]; never start a line or sentence with a bracketed id, and never gather ids at the end.

If the neighborhood genuinely does not contain the answer, reply exactly: Not in neighborhood.

Answer directly, with no preamble.`

	askDecompositionSystemPromptText = `You rewrite a user's question into focused retrieval sub-queries for a memory search. You do NOT answer the question. You output JSON only.

If answering the question requires enumerating, comparing, or classifying across a dimension (for example "which of my projects are written in C++ and which in TypeScript", "compare X and Y", "list each by category"), output one retrieval query per distinct class or value of that dimension. A single broad query lets a dominant class bury a minority one; a focused per-class query retrieves each class's own cluster.

Make each sub-query keyword-rich, not a bare paraphrase of the question: name the class plus the specific technologies, tools, file types, and terms that distinguish it, so the query lands in that class's cluster of memories. For example, for a C++ class write "C++17 native library, CMake, gcc, header files, elliptic curve" rather than just "projects written in C++"; for a TypeScript class write "TypeScript, tsc, package.json, Node, npm, .ts files".

If the question is a single-topic lookup that needs no breakdown, output an empty list.

Output ONLY this JSON, nothing else:
{"subqueries":["C++17 native library, CMake, gcc, header files","TypeScript, tsc, package.json, Node, npm"]}
or, when no decomposition is warranted:
{"subqueries":[]}` + "\n\n" + minifiedJSONInstruction

	alignmentSystemPromptText = `You are an alignment scorer. You do NOT converse. You output JSON only.

Score how strongly the evidence supports or contradicts the synthesis.

Output ONLY this JSON, nothing else:
{"alignment":0.0,"reasoning":"brief reason"}

alignment must be a float:
1.0 = strong support
0.0 = neutral/unrelated
-1.0 = strong contradiction` + "\n\n" + minifiedJSONInstruction

	noveltyJudgeSystemPromptText = `You are a novelty auditor. You do NOT converse. You output JSON only.

Given a synthesized memory and the source memories it was derived from, list any facts present in the synthesis that are NOT stated or directly implied by any of the sources. A fact is "novel" only if a careful reader could not derive it from the sources alone.

Hard rules:
- Rewording is NEVER novelty. If the synthesis says the same thing with different words, it is not novel.
- Reorganization is NEVER novelty. Reordering, combining, or restructuring source content is not novel.
- Summarization is NEVER novelty. Compressing or generalizing source content is not novel.
- A fact is novel ONLY if it introduces a new entity, relationship, quantity, date, cause, or consequence absent from every source.
- When in doubt, return an empty array.

Output ONLY this JSON, nothing else:
{"novel_facts":["fact 1","fact 2"]}

Empty array if every fact in the synthesis is already present in the sources.` + "\n\n" + minifiedJSONInstruction
)

// settingDefaults provides built-in default values for well-known settings.
// These are used when a setting is not found at any scope in the database.
var settingDefaults = map[string]string{
	SettingEnrichmentEnabled:                      "true",
	SettingDedupThreshold:                         "0.92",
	SettingExtractedFactGuardEnabled:              "true",
	SettingExtractedFactParaphraseThreshold:       "0.92",
	SettingExtractedFactBackfillBatchSize:         "100",
	SettingRankWeightSim:                          "0.50",
	SettingRankWeightRec:                          "0.15",
	SettingRankWeightImp:                          "0.10",
	SettingRankWeightFreq:                         "0.00",
	SettingRankWeightGraph:                        "0.20",
	SettingRankWeightConf:                         "0.05",
	SettingRankWeightOrigin:                       "0.25",
	SettingRankWeightMmr:                          "0.75",
	SettingRecallFusionEnabled:                    "true",
	SettingRecallFusionK:                          "60",
	SettingRecallFusionVecW:                       "0.60",
	SettingRecallFusionLexW:                       "0.40",
	SettingRecallFusionNormalizePerChan:           "false",
	SettingTokenRetention:                         "365",
	SettingTokenCostRates:                         "[]",
	SettingDreamingEnabled:                        "true",
	SettingDreamMaxTokensPerCycle:                 "1024000",
	SettingDreamMaxTokensPerCall:                  "2048",
	SettingDreamCooldown:                          "300",
	SettingDreamMinInterval:                       "600",
	SettingDreamInitialConfidence:                 "0.3",
	SettingDreamSupersessionThreshold:             "0.85",
	SettingDreamLogRetention:                      "30",
	SettingDreamLLMConcurrency:                    "1",
	SettingDreamContradictionSystemPrompt:         contradictionSystemPromptText,
	SettingDreamSynthesisSystemPrompt:             synthesisSystemPromptText,
	SettingDreamAlignmentSystemPrompt:             alignmentSystemPromptText,
	SettingDreamNoveltyEnabled:                    "true",
	SettingDreamNoveltyEmbedHighThreshold:         "0.97",
	SettingDreamNoveltyEmbedLowThreshold:          "0.85",
	SettingDreamNoveltyJudgeMaxTokens:             "512",
	SettingDreamNoveltyBackfillPerCycle:           "500",
	SettingDreamNoveltyBackfillEmbedHighThreshold: "0.93",

	SettingDreamConsolidationAuditFraction:       "0.35",
	SettingDreamConsolidationReinforceFraction:   "0.35",
	SettingDreamConsolidationConsolidateFraction: "0.30",

	SettingDreamEntityDedupFraction:          "0.0",
	SettingDreamEmbeddingBackfillFraction:    "0.10",
	SettingDreamAugmentationBackfillFraction: "0.0",
	SettingDreamMultiVectorBackfillFraction:  "0.0",
	SettingDreamParaphraseFraction:           "0.05",
	SettingDreamTransitiveFraction:           "0.0",
	SettingDreamContradictionFraction:        "0.40",
	SettingDreamConsolidationFraction:        "0.40",
	SettingDreamPruningFraction:              "0.0",
	SettingDreamWeightAdjustFraction:         "0.0",

	SettingDreamContradictionCap:                 "2000",
	SettingDreamContradictionLoserHaircut:        "0.85",
	SettingDreamContradictionWinnerHaircut:       "0.97",
	SettingDreamContradictionTieHaircut:          "0.92",
	SettingDreamContradictionParaphraseEnabled:   "true",
	SettingDreamContradictionParaphraseThreshold: "0.97",

	SettingDreamEmbeddingBackfillEnabled:     "true",
	SettingDreamEmbeddingBackfillCapPerCycle: "1000",

	SettingDreamAugmentationBackfillEnabled:     "true",
	SettingDreamAugmentationBackfillCapPerCycle: "1000",

	SettingDreamMultiVectorBackfillEnabled:     "true",
	SettingDreamMultiVectorBackfillCapPerCycle: "1000",

	SettingDreamParaphraseEnabled:     "true",
	SettingDreamParaphraseThreshold:   "0.97",
	SettingDreamParaphraseCapPerCycle: "5000",
	SettingDreamParaphraseTopK:        "1",

	SettingDreamParaphraseStaleFetchMax:    "50000",
	SettingDreamConsolidationStaleFetchMax: "50000",
	SettingDreamContradictionStaleFetchMax: "50000",
	SettingDreamPruningBatchSize:           "5000",

	SettingMemorySoftDeleteRetentionDays: "30",

	SettingFactSystemPrompt:   factSystemPromptText,
	SettingEntitySystemPrompt: entitySystemPromptText,

	SettingAskEnabled:                    "false",
	SettingAskSynthesisSystemPrompt:      askSynthesisSystemPromptText,
	SettingAskSynthesisTemperature:       "0.1",
	SettingAskSynthesisMaxTokens:         "4096",
	SettingAskRecallCandidates:           "12",
	SettingAskGraphDepth:                 "1",
	SettingAskSiblingsPerCandidate:       "3",
	SettingAskNeighborhoodMaxMemories:    "20",
	SettingAskConfidenceCosineFloor:      "0.35",
	SettingAskConfidenceCosineCeiling:    "0.75",
	SettingAskNeighborhoodMinScoreRatio:  "0.5",
	SettingAskExpansionCosineFloor:       "0.5",
	SettingAskDecompositionEnabled:       "true",
	SettingAskDecompositionMaxSubqueries: "4",
	SettingAskDecompositionMaxTokens:     "256",
	SettingAskDecompositionTemperature:   "0",
	SettingAskDecompositionSystemPrompt:  askDecompositionSystemPromptText,

	SettingIngestionDecisionEnabled:      "true",
	SettingIngestionDecisionShadow:       "false",
	SettingIngestionDecisionThreshold:    "0.92",
	SettingIngestionDecisionTopK:         "5",
	SettingIngestionDecisionSystemPrompt: ingestionDecisionSystemPromptText,

	SettingQueryAugmentEnabled:           "true",
	SettingQueryAugmentCount:             "4",
	SettingMultiVectorEnabled:            "true",
	SettingMultiVectorFacetThreshold:     "0.65",
	SettingMultiVectorMaxFacets:          "8",
	SettingMultiVectorEmbedConcurrency:   "4",
	SettingQueryAugmentMaxInputChars:     "0",
	SettingQueryAugmentMaxTokens:         "2048",
	SettingQueryAugmentSystemPrompt:      queryAugmentSystemPromptText,
	SettingDreamNoveltyJudgeSystemPrompt: noveltyJudgeSystemPromptText,
	SettingQdrantUseTLS:                  "false",
	SettingQdrantPoolSize:                "3",
	SettingQdrantKeepAliveTime:           "10",
	SettingQdrantKeepAliveTimeout:        "2",

	SettingHNSWM:                "16",
	SettingHNSWEfConstruction:   "200",
	SettingHNSWEfSearch:         "50",
	SettingHNSWMaxLoadedIndexes: "64",

	SettingReconsolidationMode:          ReconsolidationModePersist,
	SettingReconsolidationFactor:        "0.02",
	SettingConfidenceDecayEnabled:       "true",
	SettingConfidenceDecayThresholdDays: "14",
	SettingConfidenceDecayRatePerCycle:  "0.02",
	SettingConfidenceFloor:              "0.05",

	SettingReinforcementEventMemoryCap:       "20",
	SettingReinforcementEventRelationshipCap: "20",

	SettingDreamingWeightSupportGain:          "0.05",
	SettingDreamingWeightRecallReinforceDelta: "0.05",

	SettingCascadeCacheTTLSeconds:  "30",
	SettingSettingsCacheTTLSeconds: "30",

	SettingEmbeddingCacheEnabled:    "true",
	SettingEmbeddingCacheMaxEntries: "8192",
	SettingEmbeddingCacheTTLSeconds: "900",

	SettingProviderPromptCacheEnabled: "true",

	SettingProviderAnthropicJSONToolUse: "false",

	SettingProviderLLMHostConcurrency:   "1",
	SettingProviderEmbedHostConcurrency: "1",

	// Concurrency-shaped defaults are intentionally set to 1 ("safe-for-Ollama").
	// A 1-GPU local provider (Ollama on a workstation, llama.cpp, etc.) is the
	// most common nram backend and the easiest to overload: concurrent calls
	// queue at the model level and look like deadlocks to the operator. The
	// startup load-warning helper (internal/service/load_warnings.go) flags
	// any of these knobs raised above 1 so an operator who is intentionally
	// running a hosted/multi-GPU provider sees a reminder of the risk.
	SettingEnrichmentWorkerBatchClaimSize:         "1",
	SettingEnrichmentWorkerLLMConcurrency:         "1",
	SettingEnrichmentWorkerEmbedTimeoutSeconds:    "30",
	SettingEnrichmentWorkerEmbedInputCap:          "256",
	SettingEnrichmentWorkerBreakerEscalateSeconds: "300",
	SettingEnrichmentWorkerMaxBackoffSeconds:      "30",
	SettingEnrichmentWorkerCountSQLite:            "1",
	SettingEnrichmentWorkerCountPostgres:          "1",
	SettingEnrichmentWorkerPollIntervalSeconds:    "5",
	SettingEnrichmentPoolTickIntervalSeconds:      "5",
	SettingEnrichmentIngestionRationaleMaxLen:     "500",
	SettingEnrichmentHeartbeatSeconds:             "30",
	SettingEnrichmentStuckThreshold:               "1800",
	SettingEnrichmentStuckSweep:                   "300",
	SettingEnrichmentClaimMaxAge:                  "7200",
	SettingEnrichmentFailedRetentionDays:          "7",

	SettingFactExtractionMaxTokens:          "4096",
	SettingEntityExtractionMaxTokens:        "4096",
	SettingFactExtractionSyncTemperature:    "0.1",
	SettingFactExtractionAsyncTemperature:   "0.2",
	SettingEntityExtractionSyncTemperature:  "0.1",
	SettingEntityExtractionAsyncTemperature: "0.2",

	SettingEntityResolutionCosineEnabled:   "true",
	SettingEntityResolutionCosineThreshold: "0.92",

	SettingSemanticVocabThreshold: "0.50",

	SettingExtractionChunkThresholdTokens:  "2800",
	SettingExtractionChunkOverlapTokens:    "200",
	SettingExtractionContinuationMaxPasses: "2",

	SettingDreamContradictionNeighbors: "1",
	SettingDreamEntityMergeThreshold:   "0.92",
	SettingDreamSchedulerPollSeconds:   "30",
	SettingDreamHeartbeatInterval:      "30",
	SettingDreamHeartbeatStale:         "120",
	SettingDreamStuckThreshold:         "1800",
	SettingDreamStuckSweep:             "300",

	SettingLifecycleSweepIntervalSeconds: "300",
	SettingLifecycleBatchSize:            "1000",
	SettingLifecycleOrphanGraceSeconds:   "3600",

	SettingAPIRateLimitCleanupSeconds: "60",
	SettingAPIRateLimitStaleSeconds:   "600",

	SettingAuthSessionTokenTTLSeconds:         "86400",
	SettingAuthSessionRefreshThresholdSeconds: "43200",

	SettingEventsSubscriberBufferSize: "64",
	SettingEventsReplayCapacity:       "256",
	SettingEventsSSEKeepaliveSeconds:  "30",

	SettingLoggingDBCaptureEnabled: "true",
	SettingLoggingDBLevel:          "info",
	SettingLoggingRetentionMaxRows: "100000",
	SettingLoggingRetentionMaxAge:  "30",

	SettingGraphDefaultMinWeight: "0.1",
	SettingGraphMaxEdges:         "2000",
	SettingGraphCenterGravity:    "0.75",
	SettingGraphChargeStrength:   "-100",
	SettingGraphLinkDistance:     "100",

	SettingAPIBatchStoreMaxItems: "1000",

	SettingExportPageSize:         "100",
	SettingExportArtifactDir:      "",
	SettingExportTTLHours:         "168",
	SettingExportMaxPerUserPerDay: "5",

	// Recall scoring and pagination defaults. recency_decay_per_hour matches
	// the historical hardcoded math.Exp(-0.01 * hours) in computeScore.
	// graph_hop_multiplier mirrors the historical 1.0 / 2.0 product with the
	// hop count approximated as 1. overfetch_multiplier=3.0 matches the
	// previous topK := limit*3 literal; overfetch_min floors result for
	// small limits. default_limit and default_depth match the previous
	// caller-not-specified fallbacks.
	SettingRankingRecencyDecayPerHour:         "0.01",
	SettingRankingGraphHopMultiplier:          "0.5",
	SettingRecallDefaultLimit:                 "10",
	SettingRecallMaxLimit:                     "50",
	SettingRecallGraphDefaultDepth:            "2",
	SettingRecallGraphMaxDepth:                "5",
	SettingRecallGraphReserveFraction:         "0.15",
	SettingRecallOverfetchMultiplier:          "3",
	SettingRecallOverfetchMin:                 "10",
	SettingRecallGraphVectorActivationEnabled: "true",
	SettingRecallGraphVectorActivationTopK:    "5",
	SettingRecallGraphMaxEdges:                "2000",

	SettingDreamPruningRelationshipWeightThreshold: "0.05",
	SettingDreamPruningEffectivelyZero:             "0.001",

	SettingDreamTransitiveMinWeight:          "0.1",
	SettingDreamTransitiveMaxPerCycle:        "5000",
	SettingDreamTransitiveNamespaceHardCap:   "1000000",
	SettingDreamTransitiveNamespaceHighWater: "0.95",
	SettingDreamTransitiveNamespaceLowWater:  "0.80",
	// This default list is also frozen, byte-for-byte, into the one-time
	// cleanup migration migrations/{postgres,sqlite}/000056_prune_invalid_
	// transitive_edges.up.sql. Editing the curated set here changes runtime
	// inference going forward; the migration copy is a point-in-time snapshot
	// and must NOT be edited retroactively (ship a new migration instead).
	SettingDreamTransitiveRelations: `["part of","is part of","contains","located in","is located in","depends on","subclass of","is a","type of","ancestor of","descendant of","broader than","narrower than"]`,
	SettingDreamTransitiveMaxFanout: "25",

	SettingDreamWeightTier2Multiplier:      "0.5",
	SettingDreamWeightDecayWindowDays:      "30",
	SettingDreamWeightDecayFactor:          "0.95",
	SettingDreamWeightDecayMaxPeriods:      "10",
	SettingDreamWeightDeadSourceMultiplier: "0.5",
	SettingDreamWeightCeiling:              "2",

	SettingDreamConsolidationAlignmentSampleSize:     "5",
	SettingDreamConsolidationClusterOverlapThreshold: "0.3",
	SettingDreamConsolidationClusterMode:             "cosine",
	SettingDreamConsolidationClusterCosineThreshold:  "0.65",

	SettingDreamSynthesisTemperature:              "0.3",
	SettingDreamAlignmentTemperature:              "0.1",
	SettingDreamNoveltyJudgeTemperature:           "0.1",
	SettingDreamContradictionTemperature:          "0.1",
	SettingEnrichmentConflictTemperature:          "0.1",
	SettingEnrichmentIngestionDecisionTemperature: "0",
	SettingEnrichmentConflictMaxTokens:            "256",
	SettingEnrichmentIngestionDecisionMaxTokens:   "512",
	SettingEnrichmentTestPromptMaxTokens:          "8192",

	SettingDreamHeartbeatTickTimeoutSeconds: "10",

	SettingDreamStuckScanLimit:      "5000",
	SettingEnrichmentStuckScanLimit: "5000",

	// memory.default_confidence is 1.0 to match how the import/extract/store
	// write paths treat "no operator override". Operator-set values cascade
	// through Resolve normally; this is only the registered default.
	SettingMemoryDefaultImportance: "0.5",
	SettingMemoryDefaultConfidence: "1",

	// MCP CallToolResult per-tool budget in tokens. 22000 matches the prior
	// hardcoded default (44000 bytes at ~2 chars/token) and leaves headroom
	// against typical 200k-context clients without burning the whole window
	// on a single tool call. Operators tune via /admin/settings.
	SettingMCPMaxResultTokens: "22000",

	// Display-only keys: registered in the admin schema for UI completeness
	// but not yet wired to any consumer. Listed here so the init-time
	// consistency check passes; remove once a consumer is added (and either
	// promote to a Setting* constant or delete the schema entry).
	"enrichment.batch_size": "10",
	"api.rate_limit_rps":    "10",
	"api.rate_limit_burst":  "20",
}

// GetDefault returns the built-in default for the given setting key. The
// boolean reports whether the key is registered. Used by callers that need
// the same fallback the runtime cascade lands on (e.g. the schema admin
// surface) so they cannot drift from the values applied at Resolve time.
func GetDefault(key string) (string, bool) {
	v, ok := settingDefaults[key]
	return v, ok
}

// GetDefaultFloat returns the registered float default for key. Panics if
// the key has no registered default or its default is not parseable as a
// float: both are programmer errors, surfaced eagerly so a typo cannot
// silently fall through to a zero value at runtime.
func GetDefaultFloat(key string) float64 {
	def, ok := settingDefaults[key]
	if !ok {
		panic("settings: GetDefaultFloat called for key with no registered default: " + key)
	}
	f, err := strconv.ParseFloat(def, 64)
	if err != nil {
		panic("settings: registered default for " + key + " is not a valid float: " + def)
	}
	return f
}

// GetDefaultInt returns the registered int default for key. Same panic
// contract as GetDefaultFloat.
func GetDefaultInt(key string) int {
	def, ok := settingDefaults[key]
	if !ok {
		panic("settings: GetDefaultInt called for key with no registered default: " + key)
	}
	i, err := strconv.Atoi(def)
	if err != nil {
		panic("settings: registered default for " + key + " is not a valid int: " + def)
	}
	return i
}

// GetDefaultString returns the registered string default for key. Panics if
// the key has no registered default. Unlike the numeric helpers there is no
// parse step: the raw registered string is the value.
func GetDefaultString(key string) string {
	def, ok := settingDefaults[key]
	if !ok {
		panic("settings: GetDefaultString called for key with no registered default: " + key)
	}
	return def
}

// ResolveOrDefault returns the configured value for key, treating an empty
// stored value as "use the default", appropriate for prompt-shaped settings
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

// PromptCacheEnabled reports whether providers that accept explicit cache hints
// should mark the system prefix as cacheable. Nil-safe; defaults to true.
func PromptCacheEnabled(ctx context.Context, s *SettingsService) bool {
	return ResolveOrDefault(ctx, s, SettingProviderPromptCacheEnabled, "global") == "true"
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
// for that key: using the resolver before it has a TTL would self-reference.
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
	// Promote a stored value if present: Resolve goes through the repo,
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
	// Grab whatever is in the cache up front so we can fall back to it
	// on a real DB error below. A fresh entry short-circuits as before;
	// a stale entry is kept around for the stale-while-revalidate path.
	s.mu.RLock()
	cached, hasCached := s.cache[cacheKey]
	s.mu.RUnlock()
	if hasCached && cached.expiresAt.After(now) {
		return cached.value, nil
	}

	setting, err := s.repo.Get(ctx, key, scope)
	var value string
	if err == nil {
		value = unmarshalJSONString(setting.Value)
	} else if errors.Is(err, sql.ErrNoRows) {
		if def, ok := settingDefaults[key]; ok {
			value = def
		}
	} else {
		// Real DB errors degrade to stale-while-revalidate: if the cache
		// still has a value (just expired), serve it. This avoids the
		// failure mode where a transient pool exhaustion or connection
		// reset between TTL boundaries silently flips a boolean setting
		// to its registered default (e.g., recall.fusion.enabled going
		// from operator-set true to false for one request) inside every
		// downstream consumer that uses ResolveBool / ResolveFloatInRange.
		// Only return an error when there is no cached value at all (cold
		// start with the DB unreachable). The next call will retry the DB.
		if hasCached {
			slog.Warn("settings: serving stale cached value on resolve failure",
				"key", key,
				"scope", scope,
				"error", err,
				"stale_age", now.Sub(cached.expiresAt),
			)
			return cached.value, nil
		}
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

// InvalidateCache is the exported counterpart for callers that write to the
// settings repo through a different path (admin REST handler going via
// SettingsAdminStore instead of SettingsService.Set / Delete). Without an
// explicit invalidation those writes would not be visible to Resolve* readers
// until the entry's cache TTL elapsed (default 30s).
func (s *SettingsService) InvalidateCache(key, scope string) {
	s.invalidateCache(key, scope)
}

// InvalidateAllCache drops every cached entry, used after bulk operations
// (admin reset-all) where listing the affected keys to invalidate one-by-one
// is wasted work compared to clearing the map.
func (s *SettingsService) InvalidateAllCache() {
	s.mu.Lock()
	s.cache = make(map[string]settingsCacheEntry)
	s.mu.Unlock()
}

// ResolveFloatInRange resolves a numeric setting and clamps it through a
// range filter, returning fallback when the configured value is missing,
// unparseable, or outside [min, max]. Used for boot-time hydration helpers
// where the caller wants a single guaranteed-valid float and an explicit
// default: collapses the common `if v, err := ResolveFloat(...); err == nil
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
// boolish is the single truthy contract for settings values: "true" or "1".
func boolish(v string) bool {
	return v == "true" || v == "1"
}

func (s *SettingsService) ResolveBool(ctx context.Context, key string, scope string) bool {
	val, err := s.Resolve(ctx, key, scope)
	if err != nil {
		return false
	}
	return boolish(val)
}

// ResolveBoolWithDefault resolves a boolean setting, falling back to the value
// registered in settingDefaults when the service is nil (test-only constructor
// path) or the resolve fails. Mirrors ResolveIntWithDefault's nil-safety so a
// default-true setting (e.g. recall.graph.vector_activation.enabled) is not
// silently flipped to false by ResolveBool's error-is-false contract when
// s.settings is unset. The init-time consistency check guarantees a registered
// default exists; a missing one is a programmer error and panics.
func (s *SettingsService) ResolveBoolWithDefault(ctx context.Context, key, scope string) bool {
	if s != nil {
		if v, err := s.Resolve(ctx, key, scope); err == nil {
			return boolish(v)
		}
	}
	def, ok := settingDefaults[key]
	if !ok {
		panic("settings: ResolveBoolWithDefault called for key with no registered default: " + key)
	}
	return boolish(def)
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

// ResolveStringWithDefault resolves a string setting, falling back to the
// value registered in settingDefaults when the configured value is missing
// or the SettingsService itself is nil (test stubs). Same panic-on-missing-
// default contract as the typed siblings.
func (s *SettingsService) ResolveStringWithDefault(ctx context.Context, key, scope string) string {
	if s != nil {
		if v, err := s.Resolve(ctx, key, scope); err == nil && v != "" {
			return v
		}
	}
	def, ok := settingDefaults[key]
	if !ok {
		panic("settings: ResolveStringWithDefault called for key with no registered default: " + key)
	}
	return def
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

// resolveDefaultImportance returns the registered default importance for
// a newly-stored memory. Resolves through the settings service when wired,
// falls back to the registered default in settingDefaults otherwise.
// Shared by store, batch_store, extract, and import to keep the four
// write paths in sync without re-coding the literal at each site.
func resolveDefaultImportance(ctx context.Context, s *SettingsService) float64 {
	if s == nil {
		return GetDefaultFloat(SettingMemoryDefaultImportance)
	}
	return s.ResolveFloatWithDefault(ctx, SettingMemoryDefaultImportance, "global")
}

// resolveDefaultConfidence is the confidence companion to
// resolveDefaultImportance.
func resolveDefaultConfidence(ctx context.Context, s *SettingsService) float64 {
	if s == nil {
		return GetDefaultFloat(SettingMemoryDefaultConfidence)
	}
	return s.ResolveFloatWithDefault(ctx, SettingMemoryDefaultConfidence, "global")
}

// AllSettingKeys returns every key registered in the runtime defaults map.
// Used by the cascade-completeness test (asserts every key has a schema row)
// and by the bootstrap seeder (inserts a row for every key on first boot).
// The returned slice is unsorted and freshly allocated; callers may sort or
// filter without affecting subsequent calls.
func AllSettingKeys() []string {
	out := make([]string, 0, len(settingDefaults))
	for k := range settingDefaults {
		out = append(out, k)
	}
	return out
}

// noopSettingsRepo always reports "no row" so SettingsService.Resolve falls
// through to the registered default. Used by tests and by code paths that
// intentionally have no operator-tunable settings backend.
type noopSettingsRepo struct{}

func (noopSettingsRepo) Get(context.Context, string, string) (*model.Setting, error) {
	return nil, sql.ErrNoRows
}
func (noopSettingsRepo) Set(context.Context, *model.Setting) error    { return nil }
func (noopSettingsRepo) Delete(context.Context, string, string) error { return nil }
func (noopSettingsRepo) ListByScope(context.Context, string) ([]model.Setting, error) {
	return nil, nil
}

// NewNoopSettingsService returns a SettingsService backed by a stub repo that
// always reports "no row". Resolve / ResolveFloat / ResolveInt / ResolveBool
// fall through to settingDefaults; Set / Delete / ListByScope are no-ops. Use
// in tests and in any process slot that genuinely has no operator-tunable
// settings backend wired (e.g. a CLI tool that only reads behaviour from
// registered defaults).
func NewNoopSettingsService() *SettingsService {
	return NewSettingsService(noopSettingsRepo{})
}
