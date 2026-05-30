// Package enrichment provides background worker pool processing for the nram
// enrichment pipeline. Workers claim jobs from the enrichment queue, run fact
// and entity extraction via LLM providers, generate embeddings, persist
// results, and record token usage.
package enrichment

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
	"github.com/nram-ai/nram/internal/tags"
)

// Ingestion-decision operation codes. The LLM judge returns one of these as
// "operation"; ADDFallback is internal-only and recorded when the LLM call or
// JSON parse fails twice (fail-open: keep the new memory, no lineage edge).
const (
	IngestionOpAdd         = "ADD"
	IngestionOpUpdate      = "UPDATE"
	IngestionOpDelete      = "DELETE"
	IngestionOpNone        = "NONE"
	IngestionOpAddFallback = "ADD-FALLBACK"
)

// Worker tunables (batch claim size, pre-embed concurrency, embed input cap,
// embed timeout, breaker escalation window, max backoff) are resolved through
// the SettingsService cascade — see service.SettingEnrichmentWorker* keys in
// internal/service/settings.go. Defaults live in service.settingDefaults.

// asCircuitOpen extracts the *provider.CircuitOpenError from an error chain
// (returns nil, false if absent) so callers can pull provider name and timing
// out of breaker-open errors for structured logging and worker cooldown.
func asCircuitOpen(err error) (*provider.CircuitOpenError, bool) {
	if coe, ok := errors.AsType[*provider.CircuitOpenError](err); ok {
		return coe, true
	}
	return nil, false
}

// logBreakerOrError logs a job-level error. Breaker-open errors print at INFO
// during the first SettingEnrichmentWorkerBreakerEscalateSeconds of an open
// window (a fresh trip is not a code bug — Ollama warming up, provider rate
// limit, brief network blip). Sustained breaker-open trips and all other
// errors print at ERROR.
func (wp *WorkerPool) logBreakerOrError(ctx context.Context, msg string, err error, attrs ...any) {
	if coe, ok := asCircuitOpen(err); ok {
		escalateAfter := wp.settings.ResolveDurationSecondsWithDefault(ctx,
			service.SettingEnrichmentWorkerBreakerEscalateSeconds, "global")
		level := slog.LevelInfo
		if time.Since(coe.OpenSince) >= escalateAfter {
			level = slog.LevelError
		}
		retryIn := max(time.Until(coe.RetryAt).Round(time.Second), 0)
		extra := append([]any{}, attrs...)
		extra = append(extra,
			"provider", coe.Provider,
			"open_for", time.Since(coe.OpenSince).Round(time.Second).String(),
			"retry_in", retryIn.String(),
			"cause", causeString(coe.Cause),
		)
		slog.Log(context.Background(), level, msg, extra...)
		return
	}
	extra := append([]any{}, attrs...)
	extra = append(extra, "err", err)
	slog.Error(msg, extra...)
}

func causeString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// errNonTransient is a sentinel passed to requeueOrFail when the caller has
// already decided the failure should burn a queue attempt (e.g., a mixed
// transient/non-transient batch). It carries no message of its own; the
// caller supplies the failMsg.
var errNonTransient = errors.New("non-transient")

// isTransientLLMErr reports whether err represents a transient provider state
// the worker can recover from without operator intervention (currently:
// circuit breaker open). Transient failures use queue.Release so the job is
// re-queued without bumping its attempts counter, preventing a slow Ollama
// warmup from exhausting max_attempts and stranding the queue.
func isTransientLLMErr(err error) bool {
	_, ok := asCircuitOpen(err)
	return ok
}

// extractionFailPayload returns the structured *service.ExtractionFailure
// when err carries one (so the JSON encoder produces the structured
// last_error envelope), and falls back to err.Error() otherwise. Used at
// the boundary between LLM-call helpers and the queue-fail path.
func extractionFailPayload(err error) any {
	if fail, ok := errors.AsType[*service.ExtractionFailure](err); ok {
		return fail
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// requeueOrFail releases jobID back to pending without bumping attempts when
// err is transient (breaker open), and otherwise marks it failed with the
// given payload. payload is JSON-marshalled into last_error: pass an
// *service.ExtractionFailure for parse failures (so finish_reason,
// prompt_tokens, completion_tokens, and raw_response land on the queue
// row), or a plain string for non-extraction failures.
func (wp *WorkerPool) requeueOrFail(ctx context.Context, workerID string, jobID uuid.UUID, err error, payload any) {
	if isTransientLLMErr(err) {
		if relErr := wp.queue.Release(ctx, jobID, workerID); relErr != nil {
			logClaimLostOr(relErr, "enrichment: queue release after transient failure",
				"job", jobID, "worker", workerID)
		}
		return
	}
	if failErr := wp.queue.Fail(ctx, jobID, workerID, payload); failErr != nil {
		logClaimLostOr(failErr, "enrichment: queue fail",
			"job", jobID, "worker", workerID)
	}
}

// logClaimLostOr drops ErrClaimLost at INFO (expected race with the stuck
// sweeper) and surfaces other errors at WARN.
func logClaimLostOr(err error, msg string, attrs ...any) {
	if errors.Is(err, storage.ErrClaimLost) {
		slog.Info(msg+": claim lost (row was reassigned to another worker)", attrs...)
		return
	}
	slog.Warn(msg, append(attrs, "err", err)...)
}

// ---------------------------------------------------------------------------
// Dependency-inversion interfaces
// ---------------------------------------------------------------------------

// MemoryReader retrieves a memory by ID, individually or in batch. The
// batch path is the read used by FindNearMatches to hydrate near-neighbour
// content in a single round-trip rather than topK sequential lookups.
type MemoryReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]model.Memory, error)
}

// MemoryUpdater persists changes to an existing memory. The partial
// setters are how the finalize path avoids clobbering a concurrent
// supersede with a stale full-row write — each touches only the
// columns it intentionally mutates. MutateInLock is the only safe path
// for any read-modify-write merge on Tags or Metadata, since full-row
// Update without the lock has a lost-update window between workers.
type MemoryUpdater interface {
	Update(ctx context.Context, mem *model.Memory) error
	MutateInLock(ctx context.Context, id uuid.UUID, mutate func(*model.Memory) (write bool, err error)) (*model.Memory, error)
	UpdateEmbeddingDim(ctx context.Context, id uuid.UUID, dim int) error
	MarkEnriched(ctx context.Context, id, namespaceID uuid.UUID, embeddingDim *int, metadata json.RawMessage, augmentedQueries []string, augmentedEmbeddingAt *time.Time) error
	MarkSupersededBy(ctx context.Context, oldID, namespaceID, newID uuid.UUID) error
}

// MemoryCreator persists a new memory record.
type MemoryCreator interface {
	Create(ctx context.Context, mem *model.Memory) error
}

// QueueClaimer manages enrichment job lifecycle in the queue.
// MarkStepCompleted appends a step name to the job's steps_completed array
// (idempotent) so retries skip phases that already ran. Release resets a
// claimed job to pending without bumping attempts — used when the worker
// defers a job (e.g., the enrichment-available gate is closed) rather than
// failing it.
type QueueClaimer interface {
	// Enqueue inserts a new pending job. Used by the worker to schedule
	// augmentation + embedding for extracted-fact children it creates
	// mid-job, mirroring the dream-synthesis enqueue in
	// internal/dreaming/phase_consolidation.go. The insert is not deduped at
	// the queue level; a child is enqueued exactly once because a parent
	// retry skips fact extraction (HasExtractedFactChildren probe +
	// steps_completed), so no second generation of children is created.
	Enqueue(ctx context.Context, item *model.EnrichmentJob) error
	ClaimNext(ctx context.Context, workerID string) (*model.EnrichmentJob, error)
	ClaimNextBatch(ctx context.Context, workerID string, max int) ([]*model.EnrichmentJob, error)
	// Pass workerID to enable the stale-write guard (returns
	// storage.ErrClaimLost if the row's claimed_by has changed since the
	// worker claimed it); pass "" for unguarded operator paths.
	Complete(ctx context.Context, id uuid.UUID, workerID string) error
	CompleteWithWarning(ctx context.Context, id uuid.UUID, workerID string, payload any) error
	Fail(ctx context.Context, id uuid.UUID, workerID string, payload any) error
	Release(ctx context.Context, id uuid.UUID, workerID string) error
	MarkStepCompleted(ctx context.Context, id uuid.UUID, step string) error
	// SetQueryAugmentSkipReason records why the query-augmentation step did
	// not land in the persisted vector. Written by finalizeJob on every job
	// (no-op when the column already matches) so EnrichmentMonitor can
	// render a specific cause (disabled, content_empty, provider_unavailable,
	// llm_error, parse_error) next to the "skipped" row, and so a successful
	// retry of a previously-skipped job clears the stale label. workerID
	// guards against a stale worker overwriting a newer worker's write; pass
	// "" for unguarded operator paths.
	SetQueryAugmentSkipReason(ctx context.Context, id uuid.UUID, workerID string, reason string) error
	TickHeartbeat(ctx context.Context, workerID string) (int, error)
}

// EntityUpserter creates or updates an entity record and supports lookup
// by name similarity so that relationship resolution can find entities
// created by prior enrichment jobs. UpdateEmbeddingDimBatch records the dim
// for many ids in one round-trip so per-job entity writes amortize.
type EntityUpserter interface {
	Upsert(ctx context.Context, entity *model.Entity) error
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
	UpdateEmbeddingDimBatch(ctx context.Context, ids []uuid.UUID, dim int) error
}

// RelationshipCreator persists a new relationship between entities, with
// dedup support to avoid creating duplicate edges. HasBySourceMemory is the
// probe runPreEmbed uses to detect that entity extraction has already
// produced edges for a memory and skip the LLM step. BatchCreate persists
// all extracted relationships for a single memory in one transaction.
type RelationshipCreator interface {
	Create(ctx context.Context, rel *model.Relationship) error
	BatchCreate(ctx context.Context, rels []*model.Relationship) (model.BatchCreateResult, error)
	FindActiveByTriple(ctx context.Context, namespaceID, sourceID, targetID uuid.UUID, relation string) (*model.Relationship, error)
	UpdateWeight(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, weight float64) error
	HasBySourceMemory(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) (bool, error)
}

// LineageCreator records parent-child lineage between memories.
// HasExtractedFactChildren is the probe runPreEmbed uses to detect that
// fact extraction has already produced children for a memory and skip the
// LLM step. FindChildIDsByRelation is used by the paraphrase-guard backfill
// sweep to enumerate a parent's extracted-fact children.
type LineageCreator interface {
	Create(ctx context.Context, lineage *model.MemoryLineage) error
	HasExtractedFactChildren(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) (bool, error)
	FindChildIDsByRelation(ctx context.Context, namespaceID uuid.UUID, parentID uuid.UUID, relations []string) ([]uuid.UUID, error)
}

// VectorWriter upserts embedding vectors for memories and entities. Kind
// selects which table family the single-vector Upsert targets; UpsertBatch
// reads Kind from each item. Delete drops a single vector by parent ID and
// is invoked when ingestion-decision supersedes a target memory so the
// stored vector does not outlive the row's superseded state.
// GetByIDs is read-side, used by the paraphrase-guard backfill sweep
// to compare a parent's embedding against its existing extracted-fact
// children without re-embedding through the LLM provider.
type VectorWriter interface {
	Upsert(ctx context.Context, kind storage.VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error
	UpsertBatch(ctx context.Context, items []storage.VectorUpsertItem) error
	Delete(ctx context.Context, kind storage.VectorKind, id uuid.UUID) error
	GetByIDs(ctx context.Context, kind storage.VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error)
}

// MemorySoftDeleter soft-deletes a memory row and purges its vector. Used
// only by the ingestion-decision DELETE branch; everything else takes the
// memory through enrichment normally.
type MemorySoftDeleter interface {
	SoftDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
}

// ---------------------------------------------------------------------------
// Parsed extraction types — aliased to the canonical service types so
// the worker and the synchronous HTTP path share one definition. All
// extraction parsing and LLM-call logic now lives in
// internal/service/extraction_llm.go.
// ---------------------------------------------------------------------------

type (
	extractedFact          = service.ExtractedFact
	extractedEntity        = service.ExtractedEntityData
	extractedRelationship  = service.ExtractedRelation
	entityExtractionResult = service.EntityExtractionResult
)

// ---------------------------------------------------------------------------
// WorkerConfig / WorkerPool
// ---------------------------------------------------------------------------

// WorkerConfig controls the behavior of the enrichment worker pool. Workers
// and PollInterval are read once at construction; changing them at runtime
// requires a server restart (the pool is sized by the goroutine count
// spawned in Start). Defaults come from the SettingsService cascade —
// SettingEnrichmentWorkerCountSQLite / *CountPostgres / *PollIntervalSeconds.
type WorkerConfig struct {
	Workers      int           // number of concurrent workers; 0 → resolve from settings
	PollInterval time.Duration // how often idle workers poll for jobs; 0 → resolve from settings
	Backend      string        // "sqlite" or "postgres" — selects which worker-count setting applies
}

func (c WorkerConfig) withDefaults(ctx context.Context, settings *service.SettingsService) WorkerConfig {
	if c.Workers <= 0 {
		key := service.SettingEnrichmentWorkerCountPostgres
		if c.Backend == "sqlite" {
			key = service.SettingEnrichmentWorkerCountSQLite
		}
		c.Workers = max(settings.ResolveIntWithDefault(ctx, key, "global"), 1)
	}
	if c.PollInterval <= 0 {
		c.PollInterval = max(settings.ResolveDurationSecondsWithDefault(ctx,
			service.SettingEnrichmentWorkerPollIntervalSeconds, "global"), time.Second)
	}
	return c
}

// WorkerPool manages a set of background goroutines that process enrichment
// jobs from the queue.
type WorkerPool struct {
	config            WorkerConfig
	memories          MemoryReader
	memUpdater        MemoryUpdater
	memCreator        MemoryCreator
	memSoftDeleter    MemorySoftDeleter
	queue             QueueClaimer
	entities          EntityUpserter
	relationships     RelationshipCreator
	lineage           LineageCreator
	vectorStore       VectorWriter
	factProvider      func() provider.LLMProvider
	entityProvider    func() provider.LLMProvider
	embedProvider     func() provider.EmbeddingProvider
	ingestionProvider func() provider.LLMProvider
	deduplicator      *Deduplicator
	settings          *service.SettingsService
	cascade           *service.CascadeResolver
	metrics           *metrics.Metrics

	idleWorkers atomic.Int32

	bus      events.EventBus
	progress *progressTracker

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// workerIDs holds the per-process ephemeral IDs minted at Start. Set
	// once before the worker goroutines launch and only read after Start
	// returns (or after a Stop()/Start() cycle finishes Start), so no
	// synchronization is required for the production read/write pattern.
	// Exposed via WorkerIDsSnapshot for tests that need to assert the
	// minted IDs without racing the worker goroutines.
	workerIDs []string
}

// NewWorkerPool creates a new enrichment worker pool. Provider functions may
// return nil to indicate that particular capability is unavailable.
// memSoftDeleter, ingestionProvider, deduplicator, and settings together
// activate the ingestion-decision phase; passing nil for any of them turns
// the phase off and the pool runs as if it were not present.
//
// token_usage rows are written by the UsageRecordingProvider middleware
// wrapping the registry-issued providers; the pool itself does not record.
func NewWorkerPool(
	config WorkerConfig,
	memories MemoryReader,
	memUpdater MemoryUpdater,
	memCreator MemoryCreator,
	memSoftDeleter MemorySoftDeleter,
	queue QueueClaimer,
	entities EntityUpserter,
	relationships RelationshipCreator,
	lineage LineageCreator,
	vectorStore VectorWriter,
	factProvider func() provider.LLMProvider,
	entityProvider func() provider.LLMProvider,
	embedProvider func() provider.EmbeddingProvider,
	ingestionProvider func() provider.LLMProvider,
	deduplicator *Deduplicator,
	settings *service.SettingsService,
	cascade *service.CascadeResolver,
	bus events.EventBus,
) *WorkerPool {
	if settings == nil {
		// Many call sites depend on Resolve*WithDefault — passing nil settings
		// would panic deep inside the pool's run loop. Fail at construction so
		// the offender is obvious; tests use service.NewNoopSettingsService().
		panic("enrichment: NewWorkerPool requires non-nil settings")
	}
	return &WorkerPool{
		config:            config.withDefaults(context.Background(), settings),
		memories:          memories,
		memUpdater:        memUpdater,
		memCreator:        memCreator,
		memSoftDeleter:    memSoftDeleter,
		queue:             queue,
		entities:          entities,
		relationships:     relationships,
		lineage:           lineage,
		vectorStore:       vectorStore,
		factProvider:      factProvider,
		entityProvider:    entityProvider,
		embedProvider:     embedProvider,
		ingestionProvider: ingestionProvider,
		deduplicator:      deduplicator,
		settings:          settings,
		cascade:           cascade,
		bus:               bus,
		progress:          newProgressTracker(bus, settings),
	}
}

// WithMetrics attaches the Prometheus metrics sink. Returns the same pool
// for chaining at construction time.
func (wp *WorkerPool) WithMetrics(m *metrics.Metrics) *WorkerPool {
	wp.metrics = m
	return wp
}

// heartbeatTickTimeout caps how long a single TickHeartbeat write may block.
// Losing the heartbeat is what makes a long batch look frozen to the
// StuckJobSweeper, so a stuck writer must deadline rather than stall the loop.
const heartbeatTickTimeout = 10 * time.Second

// Start launches the configured number of worker goroutines. Each loops
// until the pool is stopped: claim a job, process it, repeat (or sleep on
// empty queue).
//
// Worker IDs are namespaced with a per-process nonce minted at Start time
// (e.g. "b3f1a2c8-worker-0"). The nonce is required for correctness, not
// readability: heartbeats only refresh updated_at for rows whose claimed_by
// matches a live worker, so a stable name like "worker-0" would let a new
// process — or a horizontally-scaled sibling instance — refresh claims it
// does not own, masking dead-worker recovery indefinitely.
func (wp *WorkerPool) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	wp.cancel = cancel

	// uuid.New().String() returns 36 chars; the first 8 are unique enough
	// across any plausible cluster size and keep log lines short.
	processNonce := uuid.New().String()[:8]

	workerIDs := make([]string, wp.config.Workers)
	for i := range wp.config.Workers {
		workerID := fmt.Sprintf("%s-worker-%d", processNonce, i)
		workerIDs[i] = workerID
	}
	wp.workerIDs = workerIDs
	for _, workerID := range workerIDs {
		wp.wg.Add(1)
		go wp.run(ctx, workerID)
	}

	// Pool-level tick loop: emits enrichment.pool.tick events with the
	// in-flight count, oldest-claim age, and stage breakdown so the admin
	// banner stays live without per-job heartbeats.
	if wp.progress != nil {
		wp.wg.Go(func() {
			wp.progress.runTickLoop(ctx)
		})
	}

	// Per-worker heartbeat so the StuckJobSweeper can distinguish a long
	// batch from a dead worker.
	wp.wg.Add(1)
	go wp.runHeartbeat(ctx, workerIDs)
}

// runHeartbeat periodically stamps heartbeat_at/updated_at for every row
// currently held by each worker in the pool. The loop must survive a single
// bad tick (panic, DB lock, slow write); losing it is what makes long
// batches look stalled to the sweeper.
//
// No initial tick: with per-process ephemeral worker IDs, no row in the DB
// carries a claimed_by matching this pool's IDs at start time, so an
// initial tick would always be a no-op. Refreshing rows owned by a prior
// process (the prior "carry a fresh heartbeat_at" behavior) is exactly
// what masked dead-worker recovery, so we never want that path back.
func (wp *WorkerPool) runHeartbeat(ctx context.Context, workerIDs []string) {
	defer wp.wg.Done()

	interval := wp.settings.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentHeartbeatSeconds, "global")
	if interval < time.Second {
		// Defends against an operator setting 0 or a negative value that
		// would tight-loop the heartbeat goroutine. Falls back to the
		// registered default rather than the prior 30-second hardcode.
		interval = time.Duration(service.GetDefaultInt(service.SettingEnrichmentHeartbeatSeconds)) * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			wp.tickHeartbeats(ctx, workerIDs)
		}
	}
}

// tickHeartbeats runs one heartbeat fan-out across the given worker IDs.
// Extracted from runHeartbeat so tests can drive the tick directly without
// waiting on the production timer; production code only calls it from the
// ticker loop above.
func (wp *WorkerPool) tickHeartbeats(ctx context.Context, workerIDs []string) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("enrichment: heartbeat tick panic recovered",
				"panic", rec, "stack", string(debug.Stack()))
		}
	}()

	tickCtx, cancel := context.WithTimeout(ctx, heartbeatTickTimeout)
	defer cancel()

	for _, workerID := range workerIDs {
		n, err := wp.queue.TickHeartbeat(tickCtx, workerID)
		if err != nil && ctx.Err() == nil {
			slog.Warn("enrichment: heartbeat tick failed",
				"worker", workerID, "err", err)
			continue
		}
		if n > 0 {
			slog.Debug("enrichment: heartbeat tick",
				"worker", workerID, "claimed_jobs", n)
		}
	}
}

// Stop cancels the background context and blocks until all workers finish.
func (wp *WorkerPool) Stop() {
	if wp.cancel != nil {
		wp.cancel()
	}
	wp.wg.Wait()
}

// IsIdle returns true when all workers are sleeping (no jobs being processed).
func (wp *WorkerPool) IsIdle() bool {
	return wp.idleWorkers.Load() == int32(wp.config.Workers)
}

// WorkerIDsSnapshot returns a copy of the ephemeral worker IDs minted by
// the most recent Start. Empty until Start runs; safe to call after Start
// returns. Exposed for tests; production code has no reason to read this
// (the IDs are an internal correctness mechanism, not an operator surface).
func (wp *WorkerPool) WorkerIDsSnapshot() []string {
	out := make([]string, len(wp.workerIDs))
	copy(out, wp.workerIDs)
	return out
}

func (wp *WorkerPool) run(ctx context.Context, workerID string) {
	defer wp.wg.Done()

	// Consecutive empty polls — used for backoff.
	emptyPolls := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Hot-reloadable each iteration: settings.* knobs are read here so
		// operator changes propagate within the settings cache TTL without
		// needing a worker restart.
		maxBackoff := wp.settings.ResolveIntWithDefault(ctx,
			service.SettingEnrichmentWorkerMaxBackoffSeconds, "global")
		batchClaim := wp.settings.ResolveIntWithDefault(ctx,
			service.SettingEnrichmentWorkerBatchClaimSize, "global")

		// System-level enrichment.enabled is the master toggle. When false
		// the worker stays idle without claiming, so per-job state is not
		// disturbed and re-enabling resumes the existing queue cleanly.
		// Per-namespace overrides (project / user) are honoured below in
		// runPreEmbed once the job is claimed and the memory is loaded.
		if wp.cascade != nil && !wp.cascade.ResolveEnrichmentEnabled(ctx, uuid.Nil) {
			emptyPolls++
			wp.idleWorkers.Add(1)
			wp.sleepWithBackoff(ctx, emptyPolls, maxBackoff)
			wp.idleWorkers.Add(-1)
			continue
		}

		// Idle without claiming when any LLM slot is unconfigured; jobs
		// stay pending and resume on the next poll once the admin
		// configures the missing slot.
		if wp.factProvider() == nil || wp.entityProvider() == nil || wp.embedProvider() == nil {
			emptyPolls++
			wp.idleWorkers.Add(1)
			wp.sleepWithBackoff(ctx, emptyPolls, maxBackoff)
			wp.idleWorkers.Add(-1)
			continue
		}

		jobs, err := wp.queue.ClaimNextBatch(ctx, workerID, batchClaim)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// No rows = empty queue, not an error.
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Error("enrichment: claim error", "worker", workerID, "err", err)
			}
			emptyPolls++
			wp.idleWorkers.Add(1)
			wp.sleepWithBackoff(ctx, emptyPolls, maxBackoff)
			wp.idleWorkers.Add(-1)
			continue
		}
		if len(jobs) == 0 {
			emptyPolls++
			wp.idleWorkers.Add(1)
			wp.sleepWithBackoff(ctx, emptyPolls, maxBackoff)
			wp.idleWorkers.Add(-1)
			continue
		}

		// Reset backoff on successful claim.
		emptyPolls = 0

		cooldown := wp.processBatch(ctx, workerID, jobs)
		// If a breaker tripped during this batch, pause this worker until
		// the breaker is allowed to probe again (its RetryAt). Without this
		// pause the worker would immediately re-claim the same jobs, run
		// them straight into the still-open breaker, Release them, and burn
		// CPU until the breaker recovers — exactly the "needs a restart"
		// symptom users were seeing.
		if !cooldown.IsZero() {
			wp.idleWorkers.Add(1)
			wp.sleepUntil(ctx, cooldown)
			wp.idleWorkers.Add(-1)
		}
	}
}

// sleepUntil blocks until the given deadline or context cancellation. A small
// floor prevents busy-spinning if the deadline has already passed; a small
// jitter prevents two workers from waking simultaneously after a shared
// breaker trip.
func (wp *WorkerPool) sleepUntil(ctx context.Context, deadline time.Time) {
	wait := max(time.Until(deadline), 500*time.Millisecond)
	jitter := time.Duration(rand.Int63n(int64(time.Second)))
	wait += jitter

	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// sleepWithBackoff waits for the poll interval plus jitter and exponential
// backoff based on consecutive empty polls. This reduces contention when the
// queue is idle and prevents synchronized polling spikes from multiple workers.
func (wp *WorkerPool) sleepWithBackoff(ctx context.Context, emptyPolls, maxBackoffSec int) {
	base := wp.config.PollInterval

	// Exponential backoff: double the interval for each consecutive empty poll,
	// capped at maxBackoffSec.
	backoff := base
	for i := 0; i < emptyPolls && i < 5; i++ {
		backoff *= 2
	}
	maxDuration := time.Duration(maxBackoffSec) * time.Second
	if backoff > maxDuration {
		backoff = maxDuration
	}

	// Add jitter: ±25% to prevent synchronized polling.
	jitter := time.Duration(rand.Int63n(int64(backoff/2))) - backoff/4
	wait := max(backoff+jitter, time.Second)

	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// ---------------------------------------------------------------------------
// processJob / processBatch — the enrichment pipeline
// ---------------------------------------------------------------------------

// entityFact is the per-job entity record carried into the shared embed call
// so the entity's canonical text can be embedded alongside the parent and
// child memories. The pointer is retained so the embed_dim write-back can
// flow through to the in-memory model object as well as the DB row.
type entityFact struct {
	id        uuid.UUID
	canonical string
	ent       *model.Entity
}

// pendingJob is the per-job state carried between pre-embed, embed, and
// finalize phases. embedStart indexes into the shared batched embed response
// for the parent memory; embedEntStart marks where this job's entity
// canonicals begin in the same batch. Extracted-fact children are not
// embedded here — each carries its own enrichment job enqueued in
// runPreEmbed. Ingestion-decision fields are populated by runIngestionDecision
// when the phase is enabled; parentEmbedFromPhase / shortCircuitDelete are
// derived from them.
type pendingJob struct {
	job           *model.EnrichmentJob
	mem           *model.Memory
	workerID      string // owner of the claim — passed to *Owned write methods so a sweeper-requeued row is not silently overwritten by this worker.
	entities      []entityFact
	factUsage     *provider.TokenUsage
	factModel     string
	factProvider  string
	entityUsage   *provider.TokenUsage
	entityModel   string
	entityProv    string
	embedStart    int
	embedEntStart int

	parentEmbedding []float32

	ingestionDecision   string
	ingestionTarget     *uuid.UUID
	ingestionRationale  string
	ingestionMatchN     int
	ingestionTopScore   float64
	ingestionShadowOp   string
	ingestionUsage      *provider.TokenUsage
	ingestionModel      string
	ingestionProvName   string
	ingestionEmbedUsage *provider.TokenUsage
	ingestionEmbedProv  string
	ingestionEmbedModel string

	// vectorWriteFailed signals runEmbedBatch already failed the queue
	// row; finalizeJob must skip to keep embedding_dim from being
	// persisted with no matching vector.
	vectorWriteFailed bool

	// partialRecoveryWarning is the structured payload finalizeJob writes
	// to last_error via CompleteWithWarning when at least one of the
	// extraction legs returned a longest-valid-prefix recovery (truncation
	// or degenerate-loop). nil = clean parse, finalize via Complete.
	partialRecoveryWarning any

	// Query-augmentation phase output. Populated only when runQueryAugment
	// returns a non-nil result. augmentedQueries are persisted onto the
	// memory row in finalizeJob; augmentedContent (when non-empty) replaces
	// mem.Content in the parent slot of runEmbedBatch's input slice.
	//
	// embedUsedAugmented is the load-bearing flag: true only when the parent
	// embed slot actually consumed augmentedContent. When ingestion-decision
	// pre-computed parentEmbedding and that reuse path wins, augmentation is
	// dropped at the embed boundary; recording the marker in that case would
	// claim the vector is augmented when it is not, permanently hiding the
	// row from the backfill query that targets augmented_embedding_at IS NULL.
	augmentedQueries    []string
	augmentedContent    string
	augmentedUsage      *provider.TokenUsage
	augmentedModel      string
	augmentedProvName   string
	augmentedTruncBytes int
	embedUsedAugmented  bool

	// queryAugmentSkipReason carries the structured cause when the phase did
	// not produce augmented content (disabled flag, empty memory, provider
	// unavailable, LLM error, parse error). Empty when the phase ran
	// successfully. finalizeJob writes this onto enrichment_queue so the
	// EnrichmentMonitor accordion can label a "skipped" row with a cause.
	queryAugmentSkipReason string
}

func (wp *WorkerPool) processJob(ctx context.Context, workerID string, job *model.EnrichmentJob) error {
	p, err := wp.runPreEmbed(ctx, workerID, job)
	if err != nil {
		wp.recordEnrichmentOutcome(nil, err)
		return err
	}
	// runPreEmbed returns (nil, nil) when a per-namespace gate skipped the
	// job; the queue entry is already Complete-marked, so there is nothing
	// further to do for this caller. Cooperative skips are intentionally
	// NOT counted as completed or failed — they did not produce
	// enrichment, but they also are not a failure operators should alert
	// on. Recording them under either status would distort both rates.
	if p == nil {
		return nil
	}
	wp.applyQueryAugment(ctx, p)
	wp.runEmbedBatch(ctx, []*pendingJob{p})
	err = wp.finalizeJob(ctx, p)
	wp.recordEnrichmentOutcome(p, err)
	return err
}

// recordEnrichmentOutcome bumps the enrichment outcome counter once per
// terminated job. status="completed" only when finalizeJob returned nil
// AND the vector write succeeded — runEmbedBatch failures already marked
// the queue row failed but cause finalizeJob to return nil via the
// vectorWriteFailed early-return, so the err alone is not sufficient.
// Pass p==nil for failure paths that happened before pendingJob construction
// (e.g. runPreEmbed errors). nil-safe so tests without a metrics sink no-op.
func (wp *WorkerPool) recordEnrichmentOutcome(p *pendingJob, err error) {
	if wp.metrics == nil {
		return
	}
	status := "completed"
	if err != nil || (p != nil && p.vectorWriteFailed) {
		status = "failed"
	}
	wp.metrics.EnrichmentsTotal.WithLabelValues(status).Inc()
}

// applyQueryAugment runs the augmentation phase for a single pending job and
// stamps its output onto the struct. No-op when the feature flag is off or the
// phase fails (fail-soft inside runQueryAugment). Lives here so processJob and
// processBatch share one call site.
func (wp *WorkerPool) applyQueryAugment(ctx context.Context, p *pendingJob) {
	res, skip := wp.runQueryAugment(ctx, p.job, p.mem)
	if res == nil {
		p.queryAugmentSkipReason = skip
		return
	}
	p.augmentedQueries = res.queries
	p.augmentedContent = res.augmentedContent
	p.augmentedUsage = res.usage
	p.augmentedModel = res.model
	p.augmentedProvName = res.providerName
	p.augmentedTruncBytes = res.truncatedBytes
}

// processBatch runs pre-embed in parallel across claimed jobs (bounded by
// SettingEnrichmentWorkerPreEmbedConcurrency), then makes one shared embed
// call, then finalizes each. Bounded concurrency keeps LLM provider rate
// limits safe. Returns the soonest breaker RetryAt observed across the
// batch's failures (zero if none) so the worker loop can pause until the
// breaker is allowed to probe again instead of hot-spinning.
func (wp *WorkerPool) processBatch(ctx context.Context, workerID string, jobs []*model.EnrichmentJob) time.Time {
	started := time.Now()
	slog.Info("enrichment: batch claimed", "worker", workerID, "jobs", len(jobs))

	preEmbedFanOut := max(wp.settings.ResolveIntWithDefault(ctx,
		service.SettingEnrichmentWorkerPreEmbedConcurrency, "global"), 1)

	results := make([]*pendingJob, len(jobs))
	preEmbedErrs := make([]error, len(jobs))
	jobStartTimes := make([]time.Time, len(jobs))
	sem := make(chan struct{}, preEmbedFanOut)
	var wg sync.WaitGroup
	for i, job := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, job *model.EnrichmentJob) {
			defer func() { <-sem; wg.Done() }()
			jobStartTimes[i] = time.Now().UTC()
			wp.progress.JobStarted(ctx, job, nil, workerID)
			p, err := wp.runPreEmbed(ctx, workerID, job)
			if err != nil {
				preEmbedErrs[i] = err
				wp.logBreakerOrError(ctx, "enrichment: batch pre-embed failed",
					err, "worker", workerID, "job", job.ID)
				wp.progress.JobCompleted(ctx, job.ID, job.MemoryID, job.NamespaceID,
					workerID, jobStartTimes[i], 0, 0, 0, err)
				wp.recordEnrichmentOutcome(nil, err)
				return
			}
			if p == nil {
				// Cascade-skipped: queue is already Complete-marked. Clear
				// the in-flight entry so the UI does not show a stale row.
				// Cooperative skips are intentionally not counted (see
				// processJob); they are neither completed nor failed.
				wp.progress.JobCompleted(ctx, job.ID, job.MemoryID, job.NamespaceID,
					workerID, jobStartTimes[i], 0, 0, 0, nil)
				return
			}
			wp.progress.SetStage(job.ID, StageEmbed)
			results[i] = p
		}(i, job)
	}
	wg.Wait()

	// Earliest RetryAt across any breaker-open errors observed. The worker
	// loop sleeps until that time before claiming again so a tripped breaker
	// does not produce a tight Release/Claim/Release loop while the upstream
	// provider is recovering.
	cooldown := earliestBreakerRetry(preEmbedErrs)

	pendings := make([]*pendingJob, 0, len(jobs))
	for _, p := range results {
		if p != nil {
			pendings = append(pendings, p)
		}
	}
	slog.Info("enrichment: pre-embed done",
		"worker", workerID,
		"claimed", len(jobs),
		"kept", len(pendings),
		"duration_ms", time.Since(started).Milliseconds(),
	)
	if len(pendings) == 0 {
		return cooldown
	}
	// Augmentation issues one LLM call per pending job. Reuse the
	// pre-embed fan-out setting so a batch of N jobs does not serialize
	// N synchronous LLM calls before the shared embed even starts.
	if len(pendings) > 1 {
		augSem := make(chan struct{}, preEmbedFanOut)
		var augWG sync.WaitGroup
		for _, p := range pendings {
			augWG.Add(1)
			augSem <- struct{}{}
			go func(p *pendingJob) {
				defer func() { <-augSem; augWG.Done() }()
				wp.applyQueryAugment(ctx, p)
			}(p)
		}
		augWG.Wait()
	} else {
		wp.applyQueryAugment(ctx, pendings[0])
	}
	wp.runEmbedBatch(ctx, pendings)
	// Map from job.ID back to its index in jobs so we can recover the
	// start timestamp captured in the goroutine above.
	idxByID := make(map[uuid.UUID]int, len(jobs))
	for i, j := range jobs {
		idxByID[j.ID] = i
	}
	for _, p := range pendings {
		wp.progress.SetStage(p.job.ID, StageFinalize)
		err := wp.finalizeJob(ctx, p)
		wp.recordEnrichmentOutcome(p, err)
		if err != nil {
			wp.logBreakerOrError(ctx, "enrichment: batch finalize failed",
				err, "worker", workerID, "job", p.job.ID)
		}
		startedAt := time.Now().UTC()
		if i, ok := idxByID[p.job.ID]; ok && !jobStartTimes[i].IsZero() {
			startedAt = jobStartTimes[i]
		}
		wp.progress.JobCompleted(ctx, p.job.ID, p.job.MemoryID, p.job.NamespaceID,
			workerID, startedAt, 0, 0, 0, err)
	}
	slog.Info("enrichment: batch done",
		"worker", workerID,
		"jobs", len(jobs),
		"finalized", len(pendings),
		"duration_ms", time.Since(started).Milliseconds(),
	)
	return cooldown
}

// earliestBreakerRetry returns the soonest RetryAt time across any
// CircuitOpenError in errs. Zero time if none of the errors were breaker-open.
func earliestBreakerRetry(errs []error) time.Time {
	var earliest time.Time
	for _, err := range errs {
		coe, ok := asCircuitOpen(err)
		if !ok {
			continue
		}
		if earliest.IsZero() || coe.RetryAt.Before(earliest) {
			earliest = coe.RetryAt
		}
	}
	return earliest
}

// runPreEmbed runs fact/entity extraction, child-memory creation, and
// entity/relationship upsert for a single job. On fatal failure it marks the
// job failed and returns an error; on success returns a pendingJob with
// parent+children ready for the shared embed step.
func (wp *WorkerPool) runPreEmbed(ctx context.Context, workerID string, job *model.EnrichmentJob) (*pendingJob, error) {
	mem, err := wp.memories.GetByID(ctx, job.MemoryID)
	if err != nil {
		failErr := wp.queue.Fail(ctx, job.ID, workerID, fmt.Sprintf("memory lookup: %v", err))
		if failErr != nil {
			logClaimLostOr(failErr, "enrichment: fail-mark error", "job", job.ID, "worker", workerID)
		}
		return nil, fmt.Errorf("memory lookup: %w", err)
	}

	// Paraphrase-guard backfill jobs carry a sentinel marker in
	// StepsCompleted. Detect it BEFORE the per-namespace cascade gate so an
	// admin-initiated sweep runs regardless of whether the namespace has
	// enrichment_enabled=false (the operator may have opted the namespace
	// out of automatic enrichment while still wanting to clean up legacy
	// near-duplicate children). The sweep does not call LLM providers, so
	// running it past the cascade gate cannot leak unwanted LLM spend.
	// Already-completed sweeps (StepExtractedFactParaphraseGuard present)
	// short-circuit to Complete so a retry is a no-op.
	earlySteps := stepDoneSet(job.StepsCompleted)
	if earlySteps[model.JobMarkerOnlyParaphraseGuard] {
		if !earlySteps[model.StepExtractedFactParaphraseGuard] {
			if err := wp.runExtractedFactParaphraseGuardSweep(ctx, workerID, job, mem); err != nil {
				wp.requeueOrFail(ctx, workerID, job.ID, err, fmt.Sprintf("paraphrase guard backfill: %v", err))
				return nil, fmt.Errorf("paraphrase guard backfill: %w", err)
			}
			if err := wp.queue.MarkStepCompleted(ctx, job.ID, model.StepExtractedFactParaphraseGuard); err != nil {
				slog.Warn("enrichment: mark step completed (paraphrase guard)", "job", job.ID, "err", err)
			}
		}
		if err := wp.queue.Complete(ctx, job.ID, workerID); err != nil {
			logClaimLostOr(err, "enrichment: complete paraphrase-guard backfill", "job", job.ID, "worker", workerID)
		}
		return nil, nil
	}

	// Per-namespace enrichment_enabled cascade. A project or user may opt
	// their namespace out of enrichment even while the system-level toggle
	// is on. Mark the job complete (not failed) so the queue does not
	// retry; the memory simply stays unenriched until the toggle flips
	// back. Returning (nil, nil) signals to callers that nothing further
	// should happen with this job.
	if wp.cascade != nil && !wp.cascade.ResolveEnrichmentEnabled(ctx, mem.NamespaceID) {
		if err := wp.queue.Complete(ctx, job.ID, workerID); err != nil {
			logClaimLostOr(err, "enrichment: complete-skipped error", "job", job.ID, "worker", workerID)
		}
		slog.Info("enrichment: skipped per cascade",
			"job", job.ID, "memory", mem.ID, "namespace", mem.NamespaceID)
		return nil, nil
	}

	// Stamp namespace + memory context for the UsageRecordingProvider
	// middleware so every provider call emitted by this job lands a
	// token_usage row attributed to the right namespace and memory. The
	// middleware resolves org/user/project lazily via its injected
	// resolver when no UsageContext is pre-stamped on ctx.
	ctx = provider.WithNamespaceID(ctx, mem.NamespaceID)
	ctx = provider.WithMemoryID(ctx, mem.ID)

	// Ingestion-decision phase. Runs first so a DELETE decision can short-
	// circuit fact/entity extraction (no point spending LLM tokens on a
	// memory we are about to soft-delete). Already-enriched memories skip
	// the phase: re-judging would create duplicate lineage edges.
	ingestion := wp.runIngestionDecision(ctx, job, mem)
	if ingestion != nil && ingestion.decision == IngestionOpDelete {
		p := &pendingJob{job: job, mem: mem, workerID: workerID}
		p.applyIngestion(ingestion)
		return p, nil
	}

	// Race window: a slot can be removed via /admin/providers after the
	// batch is claimed. Release (no attempts bump) so the backlog drains
	// automatically when the admin restores the slot.
	if wp.factProvider() == nil || wp.entityProvider() == nil || wp.embedProvider() == nil {
		if relErr := wp.queue.Release(ctx, job.ID, workerID); relErr != nil {
			logClaimLostOr(relErr, "enrichment: release on closed gate", "job", job.ID, "worker", workerID)
		}
		return nil, fmt.Errorf("enrichment gate closed mid-batch; job released")
	}

	// Per-step skip gates. mem.Enriched is the cheap signal — finalizeJob
	// sets it only after every phase persisted, so it covers fully-enriched
	// memories without any extra DB round-trips. job.StepsCompleted catches
	// retries of a job that partially advanced before failing. The lineage
	// and relationship probes catch historical memories whose extraction
	// predates step tracking (e.g. memories enriched before steps_completed
	// was wired into the worker, or before mem.Enriched was set on the
	// synchronous write path). Probe errors fail open — run the step rather
	// than skip on a transient DB hiccup.
	//
	// DREAM-RECURSION GUARD — worker-side enforcement of the dream-of-dream
	// cascade prevention contract. isDream is the explicit signal so the
	// guard is readable here regardless of whether Enriched ever decouples
	// from "skip memory-creating phases" in the future. Both clauses are
	// load-bearing — either alone is sufficient. Symmetric sites:
	//
	//   - internal/dreaming/phase_consolidation.go (synthMemory creation,
	//       "DREAM-RECURSION GUARD — first prong")
	//   - internal/dreaming/phase_consolidation.go (consolidate() candidate
	//       filter, "DREAM-RECURSION GUARD — second prong")
	//   - internal/enrichment/phase_ingestion.go (runIngestionDecision
	//       Enriched/origin early-return)
	//
	// Contract enforcer: internal/dreaming/dream_recursion_guard_test.go
	// (TestDreamRecursionGuard_EndToEnd, table-driven across Enriched=true
	// AND Enriched=false to pin each clause independently).
	isDream := mem.IsDream()
	stepDone := stepDoneSet(job.StepsCompleted)
	skipFact := isDream || mem.Enriched || stepDone[model.StepFactExtraction]
	skipEntity := isDream || mem.Enriched || stepDone[model.StepEntityExtraction]

	if !skipFact {
		if has, probeErr := wp.lineage.HasExtractedFactChildren(ctx, mem.NamespaceID, mem.ID); probeErr != nil {
			slog.Warn("enrichment: probe extracted-fact lineage", "job", job.ID, "memory", mem.ID, "err", probeErr)
		} else if has {
			skipFact = true
		}
	}
	if !skipEntity {
		if has, probeErr := wp.relationships.HasBySourceMemory(ctx, mem.NamespaceID, mem.ID); probeErr != nil {
			slog.Warn("enrichment: probe source-memory relationships", "job", job.ID, "memory", mem.ID, "err", probeErr)
		} else if has {
			skipEntity = true
		}
	}

	var (
		factEnv   *service.FactExtractionEnvelope
		entEnv    *service.EntityExtractionEnvelope
		factErr   error
		entityErr error
	)

	if !skipFact {
		factEnv, factErr = wp.extractFacts(ctx, wp.factProvider(), mem.Content)
	}
	if !skipEntity {
		entEnv, entityErr = wp.extractEntities(ctx, wp.entityProvider(), mem.Content)
	}

	var (
		facts        []extractedFact
		entResult    *entityExtractionResult
		factUsage    *provider.TokenUsage
		entityUsage  *provider.TokenUsage
		factModel    string
		entityModel  string
		factProvider string
		entityProv   string
	)
	if factEnv != nil {
		facts = factEnv.Facts
		u := factEnv.Usage
		factUsage = &u
		factModel = factEnv.Model
		factProvider = factEnv.ProviderName
	}
	if entEnv != nil {
		entResult = entEnv.Result
		u := entEnv.Usage
		entityUsage = &u
		entityModel = entEnv.Model
		entityProv = entEnv.ProviderName
	}

	if factErr != nil && entityErr != nil {
		joined := errors.Join(factErr, entityErr)
		// Treat as transient only when *both* legs are: if one leg is a real
		// fault, burning a queue attempt is the right policy.
		if isTransientLLMErr(factErr) && isTransientLLMErr(entityErr) {
			wp.requeueOrFail(ctx, workerID, job.ID, factErr, joined.Error())
		} else {
			wp.requeueOrFail(ctx, workerID, job.ID, errNonTransient, joined.Error())
		}
		return nil, fmt.Errorf("extraction failed: %w", joined)
	}
	if factErr != nil {
		wp.requeueOrFail(ctx, workerID, job.ID, factErr, extractionFailPayload(factErr))
		return nil, fmt.Errorf("fact extraction: %w", factErr)
	}
	// Entity-only failure is intentionally soft: facts may have succeeded and
	// the job can still produce useful output. The job continues with empty
	// entities. (Both-failed and fact-only branches above already handle the
	// hard cases.)

	// Pre-insert paraphrase guard. Each extracted fact is compared by cosine
	// similarity to the parent memory and to previously-accepted siblings in
	// this job. When max similarity is at or above threshold, the child is
	// suppressed and its tags are merged into the parent via a
	// LineageExtractedFactSuppressed audit row. Fail-open on embed errors:
	// it is better to leak a paraphrase than to silently drop a fact.
	guardEnabled := wp.settings.ResolveBool(ctx, service.SettingExtractedFactGuardEnabled, "global")
	threshold := wp.settings.ResolveFloatWithDefault(ctx, service.SettingExtractedFactParaphraseThreshold, "global")
	if threshold <= 0 || threshold > 1 {
		threshold = 0.92
	}
	var parentEmbed []float32
	if ingestion != nil {
		parentEmbed = ingestion.parentEmbedding
	}
	parentEmbedTried := false

	childCount := 0
	acceptedSiblingEmbeds := make([][]float32, 0, len(facts))

	for _, fact := range facts {
		if guardEnabled && fact.Content != "" {
			ep := wp.embedProvider()
			var candEmbed []float32
			if ep != nil {
				er, embedErr := ep.Embed(provider.WithOperation(ctx, provider.OperationEmbedding), &provider.EmbeddingRequest{Input: []string{fact.Content}})
				switch {
				case embedErr != nil:
					slog.Warn("enrichment: extracted-fact guard candidate embed",
						"job", job.ID, "memory", mem.ID, "reason", "embed_error", "err", embedErr)
				case er == nil || len(er.Embeddings) == 0:
					slog.Warn("enrichment: extracted-fact guard candidate embed",
						"job", job.ID, "memory", mem.ID, "reason", "empty_response")
				default:
					candEmbed = er.Embeddings[0]
				}
				if candEmbed != nil && len(parentEmbed) == 0 && !parentEmbedTried {
					parentEmbedTried = true
					pe, perr := ep.Embed(provider.WithOperation(ctx, provider.OperationEmbedding), &provider.EmbeddingRequest{Input: []string{mem.Content}})
					switch {
					case perr != nil:
						slog.Warn("enrichment: extracted-fact guard parent embed",
							"job", job.ID, "memory", mem.ID, "reason", "embed_error", "err", perr)
					case pe == nil || len(pe.Embeddings) == 0:
						slog.Warn("enrichment: extracted-fact guard parent embed",
							"job", job.ID, "memory", mem.ID, "reason", "empty_response")
					default:
						parentEmbed = pe.Embeddings[0]
					}
				}
			}
			if candEmbed != nil {
				var maxSim float64
				against := ""
				if len(parentEmbed) > 0 {
					if s := hnsw.CosineSimilarity(candEmbed, parentEmbed); s > maxSim {
						maxSim = s
						against = "parent"
					}
				}
				for i, sib := range acceptedSiblingEmbeds {
					if s := hnsw.CosineSimilarity(candEmbed, sib); s > maxSim {
						maxSim = s
						against = fmt.Sprintf("sibling:%d", i)
					}
				}
				if maxSim >= threshold {
					if mErr := wp.mergeTagsIntoParent(ctx, mem, nil, fact.Tags, fact.Content, maxSim, against, "fact_extraction_guard"); mErr != nil {
						slog.Warn("enrichment: merge suppressed-fact tags",
							"job", job.ID, "memory", mem.ID, "err", mErr)
					} else {
						slog.Info("enrichment: extracted-fact guard suppressed",
							"job", job.ID, "memory", mem.ID, "cosine", maxSim, "against", against, "tags", fact.Tags)
					}
					continue
				}
				acceptedSiblingEmbeds = append(acceptedSiblingEmbeds, candEmbed)
			}
		}

		childID := uuid.New()
		child := &model.Memory{
			ID:          childID,
			NamespaceID: mem.NamespaceID,
			Content:     fact.Content,
			Confidence:  fact.Confidence,
			Tags:        mergeTags(mem.Tags, fact.Tags),
			Source:      mem.Source,
			Origin:      mem.Origin,
			Importance:  0.5,
			Enriched:    true,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}
		if err := wp.memCreator.Create(ctx, child); err != nil {
			slog.Error("enrichment: create child memory", "job", job.ID, "err", err)
			continue
		}
		childCount++

		parentID := mem.ID
		lin := &model.MemoryLineage{
			ID:          uuid.New(),
			NamespaceID: mem.NamespaceID,
			MemoryID:    childID,
			ParentID:    &parentID,
			Relation:    model.LineageExtractedFact,
			CreatedAt:   time.Now().UTC(),
		}
		if err := wp.lineage.Create(ctx, lin); err != nil {
			slog.Error("enrichment: create lineage", "job", job.ID, "err", err)
		}

		// Schedule the child's own augmentation + embedding by enqueuing a
		// fresh enrichment job, mirroring the dream-synthesis enqueue in
		// internal/dreaming/phase_consolidation.go. The child carries
		// Enriched=true, so when its job is claimed the ingestion-decision,
		// fact-extraction, and entity-extraction phases all short-circuit
		// (phase_ingestion.go and runPreEmbed skipFact/skipEntity); only
		// query augmentation and embedding run. No dream-recursion concern:
		// fact children are never dream-origin (dream memories skip fact
		// extraction at skipFact=isDream), so a child job cannot create a
		// further derivative that feeds a dream cycle.
		//
		// Enqueue failure is non-fatal: the child row already exists, and
		// EnrichService.BackfillAugmentation is the operator recovery route
		// for any child stranded without a vector — the same guarantee the
		// dream path relies on.
		now := time.Now().UTC()
		childJob := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    childID,
			NamespaceID: mem.NamespaceID,
			Status:      model.EnrichmentStatusPending,
			Priority:    0,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := wp.queue.Enqueue(ctx, childJob); err != nil {
			slog.Warn("enrichment: enqueue child augmentation job",
				"job", job.ID, "child", childID, "err", err)
		}
	}

	if childCount > 0 {
		slog.Info("enrichment: scheduled extracted-fact child augmentation jobs",
			"job", job.ID, "memory", mem.ID, "children", childCount)
	}

	// Mark fact_extraction as completed when the LLM call succeeded —
	// even if 0 facts came back. The signal is "the step ran", not "the
	// step produced output", so a legitimate 0-fact memory does not
	// re-extract on the next claim.
	if !skipFact {
		if err := wp.queue.MarkStepCompleted(ctx, job.ID, model.StepFactExtraction); err != nil {
			slog.Warn("enrichment: mark step completed (fact)", "job", job.ID, "err", err)
		}
	}

	entities := wp.upsertEntitiesAndRelationships(ctx, job, mem, entResult)

	if !skipEntity && entityErr == nil {
		if err := wp.queue.MarkStepCompleted(ctx, job.ID, model.StepEntityExtraction); err != nil {
			slog.Warn("enrichment: mark step completed (entity)", "job", job.ID, "err", err)
		}
	}

	p := &pendingJob{
		job:                    job,
		mem:                    mem,
		workerID:               workerID,
		entities:               entities,
		factUsage:              factUsage,
		factModel:              factModel,
		factProvider:           factProvider,
		entityUsage:            entityUsage,
		entityModel:            entityModel,
		entityProv:             entityProv,
		partialRecoveryWarning: buildPartialRecoveryWarning(factEnv, entEnv),
	}
	p.applyIngestion(ingestion)
	return p, nil
}

// partialRecoveryLeg is one entry in the warning payload finalizeJob writes
// to last_error via CompleteWithWarning. Each leg records the diagnostic
// data the operator needs to confirm the recovery was acceptable.
type partialRecoveryLeg struct {
	Phase            string `json:"phase"`
	Reason           string `json:"reason"`
	FinishReason     string `json:"finish_reason,omitempty"`
	PromptTokens     int    `json:"prompt_tokens,omitempty"`
	CompletionTokens int    `json:"completion_tokens,omitempty"`
	Model            string `json:"model,omitempty"`
	Provider         string `json:"provider,omitempty"`
	FactsRecovered   int    `json:"facts_recovered,omitempty"`
	EntitiesRec      int    `json:"entities_recovered,omitempty"`
	RelationsRec     int    `json:"relationships_recovered,omitempty"`
}

// buildPartialRecoveryWarning returns the last_error payload for
// CompleteWithWarning when at least one leg recovered from a truncated or
// looping response. Returns nil for clean-parse jobs so finalizeJob routes
// through plain Complete.
func buildPartialRecoveryWarning(factEnv *service.FactExtractionEnvelope, entEnv *service.EntityExtractionEnvelope) any {
	var warnings []partialRecoveryLeg
	if factEnv != nil && factEnv.PartialRecovery {
		warnings = append(warnings, partialRecoveryLeg{
			Phase:            service.ExtractionPhaseFact,
			Reason:           service.ExtractionReasonPartialRecovery,
			FinishReason:     factEnv.FinishReason,
			PromptTokens:     factEnv.Usage.PromptTokens,
			CompletionTokens: factEnv.Usage.CompletionTokens,
			Model:            factEnv.Model,
			Provider:         factEnv.ProviderName,
			FactsRecovered:   len(factEnv.Facts),
		})
	}
	if entEnv != nil && entEnv.PartialRecovery {
		w := partialRecoveryLeg{
			Phase:            service.ExtractionPhaseEntity,
			Reason:           service.ExtractionReasonPartialRecovery,
			FinishReason:     entEnv.FinishReason,
			PromptTokens:     entEnv.Usage.PromptTokens,
			CompletionTokens: entEnv.Usage.CompletionTokens,
			Model:            entEnv.Model,
			Provider:         entEnv.ProviderName,
		}
		if entEnv.Result != nil {
			w.EntitiesRec = len(entEnv.Result.Entities)
			w.RelationsRec = len(entEnv.Result.Relationships)
		}
		warnings = append(warnings, w)
	}
	if len(warnings) == 0 {
		return nil
	}
	return map[string]any{"warnings": warnings}
}

// stepDoneSet parses an EnrichmentJob.StepsCompleted JSON payload into a
// presence set. Tolerates NULL, empty, or malformed inputs by returning an
// empty set — the worker will then re-run the step rather than skip on
// bad data.
func stepDoneSet(raw json.RawMessage) map[string]bool {
	out := map[string]bool{}
	if len(raw) == 0 {
		return out
	}
	var steps []string
	if err := json.Unmarshal(raw, &steps); err != nil {
		return out
	}
	for _, s := range steps {
		if s != "" {
			out[s] = true
		}
	}
	return out
}

// upsertEntitiesAndRelationships persists extracted entities and relationships,
// resolving missing references against the DB and dedup-ing against existing
// edges. Returns the entityFact list of every entity (extracted + stubbed via
// relationship resolution) so runEmbedBatch can embed their canonical names
// in the same provider call as the parent and child memories.
func (wp *WorkerPool) upsertEntitiesAndRelationships(ctx context.Context, job *model.EnrichmentJob, mem *model.Memory, entResult *entityExtractionResult) []entityFact {
	if entResult == nil {
		return nil
	}

	collected := make([]entityFact, 0, len(entResult.Entities))
	seen := make(map[uuid.UUID]bool, len(entResult.Entities))

	addFact := func(ent *model.Entity) {
		if ent == nil || seen[ent.ID] {
			return
		}
		seen[ent.ID] = true
		collected = append(collected, entityFact{
			id:        ent.ID,
			canonical: ent.Canonical,
			ent:       ent,
		})
	}

	entityNameToID := make(map[string]uuid.UUID)
	for idx := range entResult.Entities {
		ent := &entResult.Entities[idx]
		entID := uuid.New()
		props, _ := json.Marshal(ent.Properties)

		modelEntity := &model.Entity{
			ID:           entID,
			NamespaceID:  mem.NamespaceID,
			Name:         ent.Name,
			Canonical:    strings.ToLower(ent.Name),
			EntityType:   ent.Type,
			Properties:   props,
			MentionCount: 1,
			CreatedAt:    time.Now().UTC(),
			UpdatedAt:    time.Now().UTC(),
		}
		if err := wp.entities.Upsert(ctx, modelEntity); err != nil {
			slog.Error("enrichment: upsert entity", "job", job.ID, "entity", ent.Name, "err", err)
			continue
		}
		entityNameToID[ent.Name] = modelEntity.ID
		addFact(modelEntity)
	}

	// Collect all extracted relationships for this memory; flush via
	// one BatchCreate after the entity-resolution loop.
	relCandidates := make([]*model.Relationship, 0, len(entResult.Relationships))
	for _, rel := range entResult.Relationships {
		srcID, srcOK := entityNameToID[rel.Source]
		tgtID, tgtOK := entityNameToID[rel.Target]

		if !srcOK {
			ent, err := wp.resolveOrCreateEntity(ctx, mem.NamespaceID, rel.Source)
			if err != nil {
				slog.Error("enrichment: resolve source entity", "entity", rel.Source, "err", err)
			} else {
				srcID, srcOK = ent.ID, true
				entityNameToID[rel.Source] = ent.ID
				addFact(ent)
			}
		}
		if !tgtOK {
			ent, err := wp.resolveOrCreateEntity(ctx, mem.NamespaceID, rel.Target)
			if err != nil {
				slog.Error("enrichment: resolve target entity", "entity", rel.Target, "err", err)
			} else {
				tgtID, tgtOK = ent.ID, true
				entityNameToID[rel.Target] = ent.ID
				addFact(ent)
			}
		}

		if !srcOK || !tgtOK {
			slog.Warn("enrichment: skip relationship, entity resolution failed",
				"source", rel.Source, "srcResolved", srcOK,
				"target", rel.Target, "tgtResolved", tgtOK)
			continue
		}

		memID := mem.ID
		relCandidates = append(relCandidates, &model.Relationship{
			ID:           uuid.New(),
			NamespaceID:  mem.NamespaceID,
			SourceID:     srcID,
			TargetID:     tgtID,
			Relation:     rel.Relation,
			Weight:       rel.Weight,
			SourceMemory: &memID,
			ValidFrom:    mem.CreatedAt,
			CreatedAt:    time.Now().UTC(),
		})
	}

	if len(relCandidates) > 0 {
		if _, err := wp.relationships.BatchCreate(ctx, relCandidates); err != nil {
			slog.Error("enrichment: batch create relationships", "job", job.ID, "count", len(relCandidates), "err", err)
		}
	}

	return collected
}

// runEmbedBatch runs one embed provider call covering every pendingJob's
// parent + child-fact contents AND every entity canonical, chunking at
// embedInputCap, then writes all vectors via UpsertBatch. Memory and entity
// items share the same provider call to keep RTT cost at one per batch.
// Per-job token usage is attributed with a largest-remainder allocation so
// the per-job rows sum to exactly the provider-billed aggregate.
func (wp *WorkerPool) runEmbedBatch(ctx context.Context, pendings []*pendingJob) {
	if len(pendings) == 0 {
		return
	}
	if wp.embedProvider == nil || wp.vectorStore == nil {
		return
	}
	ep := wp.embedProvider()
	if ep == nil {
		return
	}

	inputs := make([]string, 0, len(pendings)*2)
	for _, p := range pendings {
		if p.shortCircuitDelete() {
			// New memory is going to be soft-deleted; do not embed or
			// upsert anything for it. Children/entities are not produced
			// for short-circuited jobs.
			continue
		}
		// When the augmentation phase produced content, the parent vector
		// must be rebuilt from the queries+separator+content blob. If the
		// ingestion-decision phase pre-computed an embedding for the raw
		// content, discard it here so the downstream branches take the
		// fresh-embed path and the stored vector matches the augmented
		// input the operator asked for. Costs one extra embed call per
		// affected job; the augmented vector is the load-bearing signal.
		if p.augmentedContent != "" && p.parentEmbedFromPhase() {
			slog.Info("enrichment: query_augment superseded ingestion-decision pre-embed",
				"job", p.job.ID, "memory", p.mem.ID, "queries", len(p.augmentedQueries))
			p.parentEmbedding = nil
		}
		p.embedStart = len(inputs)
		// Reuse the embedding the ingestion-decision phase already
		// computed for this content (only reachable when augmentation
		// did not run). Otherwise embed against augmentedContent when
		// present, falling back to raw mem.Content.
		//
		// Extracted-fact children are NOT embedded here. Each child carries
		// its own enrichment job (enqueued in runPreEmbed) that augments and
		// embeds it against its augmented blob, so embedding it inline would
		// produce a redundant raw vector that the child's job immediately
		// supersedes.
		if !p.parentEmbedFromPhase() {
			parentInput := p.mem.Content
			if p.augmentedContent != "" {
				parentInput = p.augmentedContent
				p.embedUsedAugmented = true
			}
			inputs = append(inputs, parentInput)
		}

		p.embedEntStart = len(inputs)
		for _, e := range p.entities {
			inputs = append(inputs, e.canonical)
		}
	}

	var (
		embeddings [][]float32
		modelName  string
	)
	if len(inputs) > 0 {
		dim := storage.BestEmbeddingDimension(ep.Dimensions())
		var (
			usage provider.TokenUsage
			err   error
		)
		// For batched embed across multiple pendings, attribute the call to
		// the first pending's namespace so the row has a non-nil
		// namespace_id. Per-job split is intentionally not recovered here;
		// per-batch granularity is sufficient for analytics, and request_id
		// correlation can recover finer attribution when needed.
		batchCtx := ctx
		if len(pendings) > 0 {
			batchCtx = provider.WithNamespaceID(batchCtx, pendings[0].mem.NamespaceID)
		}
		embedStarted := time.Now()
		embeddings, _, modelName, err = wp.embedChunked(batchCtx, ep, inputs, dim)
		_ = modelName
		_ = usage
		if err != nil {
			wp.logBreakerOrError(ctx, "enrichment: batched embed",
				err, "jobs", len(pendings), "inputs", len(inputs))
			if isTransientLLMErr(err) {
				for _, p := range pendings {
					if p.shortCircuitDelete() {
						continue
					}
					if relErr := wp.queue.Release(ctx, p.job.ID, p.workerID); relErr != nil {
						logClaimLostOr(relErr, "enrichment: queue release after transient embed failure",
							"job", p.job.ID, "worker", p.workerID)
					}
				}
			}
			return
		}
		slog.Info("enrichment: embedded",
			"jobs", len(pendings),
			"inputs", len(inputs),
			"duration_ms", time.Since(embedStarted).Milliseconds(),
		)
	}

	items := make([]storage.VectorUpsertItem, 0, len(inputs)+len(pendings))
	for _, p := range pendings {
		if p.shortCircuitDelete() {
			continue
		}

		// Parent vector: either reused from the ingestion-decision phase
		// or read out of the freshly produced embeddings slice.
		var parentVec []float32
		if p.parentEmbedFromPhase() {
			parentVec = p.parentEmbedding
		} else if p.embedStart < len(embeddings) {
			parentVec = embeddings[p.embedStart]
		}
		if d := len(parentVec); d > 0 {
			p.mem.EmbeddingDim = &d
			items = append(items, storage.VectorUpsertItem{
				Kind:        storage.VectorKindMemory,
				ID:          p.mem.ID,
				NamespaceID: p.mem.NamespaceID,
				Embedding:   parentVec,
				Dimension:   d,
			})
		}

		// Extracted-fact children are not embedded in this batch (see the
		// input-assembly note above); they are vectored by their own
		// enrichment jobs. The embed slice therefore runs parent → entities
		// with no child rows in between.
		for j, e := range p.entities {
			idx := p.embedEntStart + j
			if idx >= len(embeddings) {
				break
			}
			vec := embeddings[idx]
			if d := len(vec); d > 0 {
				if e.ent != nil {
					e.ent.EmbeddingDim = &d
				}
				items = append(items, storage.VectorUpsertItem{
					Kind:        storage.VectorKindEntity,
					ID:          e.id,
					NamespaceID: p.mem.NamespaceID,
					Embedding:   vec,
					Dimension:   d,
				})
			}
		}
	}
	if len(items) == 0 {
		return
	}
	upsertStarted := time.Now()
	if err := wp.vectorStore.UpsertBatch(ctx, items); err != nil {
		slog.Error("enrichment: upsert vectors batch",
			"jobs", len(pendings), "items", len(items), "err", err)
		// Fail each pending so finalizeJob skips persisting embedding_dim
		// without a matching vector; queue retry policy requeues.
		for _, p := range pendings {
			if p.shortCircuitDelete() {
				continue
			}
			p.vectorWriteFailed = true
			if failErr := wp.queue.Fail(ctx, p.job.ID, p.workerID, fmt.Sprintf("vector upsert batch: %v", err)); failErr != nil {
				logClaimLostOr(failErr, "enrichment: queue fail after vector batch failure",
					"job", p.job.ID, "worker", p.workerID)
			}
		}
		return
	}
	slog.Info("enrichment: vectors upserted",
		"jobs", len(pendings),
		"items", len(items),
		"duration_ms", time.Since(upsertStarted).Milliseconds(),
	)
}

// embedChunked makes one or more Embed calls so each request respects
// SettingEnrichmentWorkerEmbedInputCap. Returned embeddings preserve input
// order; returned usage is the sum across chunks; returned model is the
// last non-empty model string.
func (wp *WorkerPool) embedChunked(ctx context.Context, ep provider.EmbeddingProvider, inputs []string, dim int) ([][]float32, provider.TokenUsage, string, error) {
	var (
		out   = make([][]float32, 0, len(inputs))
		usage provider.TokenUsage
		model string
	)
	cap := max(wp.settings.ResolveIntWithDefault(ctx,
		service.SettingEnrichmentWorkerEmbedInputCap, "global"), 1)
	timeout := wp.settings.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentWorkerEmbedTimeoutSeconds, "global")
	for start := 0; start < len(inputs); start += cap {
		end := min(start+cap, len(inputs))
		embedCtx, cancel := context.WithTimeout(ctx, timeout)
		embedCtx = provider.WithOperation(embedCtx, provider.OperationEmbedding)
		resp, err := ep.Embed(embedCtx, &provider.EmbeddingRequest{
			Input:     inputs[start:end],
			Dimension: dim,
		})
		cancel()
		if err != nil {
			return nil, provider.TokenUsage{}, "", err
		}
		out = append(out, resp.Embeddings...)
		usage.PromptTokens += resp.Usage.PromptTokens
		usage.CompletionTokens += resp.Usage.CompletionTokens
		usage.TotalTokens += resp.Usage.TotalTokens
		if resp.Model != "" {
			model = resp.Model
		}
	}
	return out, usage, model, nil
}

// finalizeJob marks the memory enriched, records LLM token usage, and
// completes the queue row. Short-circuit DELETE pendings (the LLM ingestion
// judge marked the new memory as redundant) take a separate path: the memory
// is soft-deleted instead of marked enriched, and only the ingestion-decision
// token usage is recorded.
func (wp *WorkerPool) finalizeJob(ctx context.Context, p *pendingJob) error {
	// Stamp the augmentation skip reason FIRST so every downstream branch
	// (vectorWriteFailed early-return, shortCircuitDelete, MarkEnriched
	// failure, happy path) records the value the augmentation phase
	// decided. The reason reflects augmentation-phase outcome and is
	// independent of whether the surrounding job ends in success or
	// failure. The repo's UPDATE predicate makes this a no-op when the
	// column already matches, so a successful job whose column was already
	// NULL pays no extra round-trip. workerID guards against a stale worker
	// clobbering a newer one after a sweeper-requeue.
	if err := wp.queue.SetQueryAugmentSkipReason(ctx, p.job.ID, p.workerID, p.queryAugmentSkipReason); err != nil {
		slog.Warn("enrichment: set query_augment skip reason", "job", p.job.ID, "err", err)
	}
	if p.vectorWriteFailed {
		// runEmbedBatch already marked the queue row failed; skipping
		// the memory Update here is what stops embedding_dim from being
		// persisted without a matching vector row.
		return nil
	}
	if p.shortCircuitDelete() {
		return wp.finalizeShortCircuitDelete(ctx, p)
	}

	// UPDATE: insert a supersedes lineage edge and mark the target memory
	// superseded by the new one. Failures here log but do not abort the
	// finalize (the new memory still gets enriched and recall improves).
	if p.ingestionDecision == IngestionOpUpdate && p.ingestionTarget != nil {
		wp.applyIngestionUpdate(ctx, p)
	}
	if p.ingestionDecision != "" {
		slog.Info("enrichment: ingestion_decision_apply",
			"job", p.job.ID,
			"memory", p.mem.ID,
			"op", p.ingestionDecision,
			"target_id", uuidPtrString(p.ingestionTarget),
			"shadow_op", p.ingestionShadowOp)
	}

	stampIngestionMetadataOn(p.mem, p)

	// Single partial UPDATE so a concurrent memory_update that supersedes
	// this row keeps its supersede pointer. The augmentation marker
	// (augmented_queries, augmented_embedding_at) lands atomically with
	// the enriched flag so a transient DB error cannot leave the row in
	// (enriched=true, augmented_embedding_at=NULL), which is exactly the
	// state the backfill query targets. The marker columns are only
	// written when embedUsedAugmented==true AND p.augmentedQueries is
	// populated: if ingestion-decision's pre-embed won the parent slot,
	// the stored vector is not augmented and we must not claim otherwise.
	// Coupling the two values guarantees we never write a timestamp
	// without the matching queries, or vice versa.
	//
	// One `now` is computed up front and shared between augmented_embedding_at
	// and the in-memory mem.UpdatedAt so they cannot drift across a
	// second boundary. The persisted updated_at column is stamped inside
	// MarkEnriched and may differ by one second under second-resolution
	// RFC3339 formatting; that drift is bounded and harmless for
	// auditing because updated_at >= augmented_embedding_at always holds
	// to within sub-second clock skew.
	//
	// Soft-delete behavior: MarkEnriched's WHERE filters deleted_at IS
	// NULL, so a row soft-deleted between embed and finalize will return
	// sql.ErrNoRows here and the job is failed (bounded retries until
	// MaxAttempts). The marker is intentionally NOT written on tombstones.
	var stampedMetadata json.RawMessage
	if p.ingestionDecision != "" {
		stampedMetadata = p.mem.Metadata
	}
	now := time.Now().UTC()
	var augmentedQueries []string
	var augmentedEmbeddingAt *time.Time
	if p.embedUsedAugmented && p.mem.EmbeddingDim != nil && len(p.augmentedQueries) > 0 {
		augmentedQueries = p.augmentedQueries
		augmentedEmbeddingAt = &now
	}
	p.mem.Enriched = true
	p.mem.UpdatedAt = now
	if err := wp.memUpdater.MarkEnriched(ctx, p.mem.ID, p.mem.NamespaceID, p.mem.EmbeddingDim, stampedMetadata, augmentedQueries, augmentedEmbeddingAt); err != nil {
		if failErr := wp.queue.Fail(ctx, p.job.ID, p.workerID, fmt.Sprintf("update memory enriched: %v", err)); failErr != nil {
			logClaimLostOr(failErr, "enrichment: fail-mark after memory update", "job", p.job.ID, "worker", p.workerID)
		}
		return fmt.Errorf("update memory: %w", err)
	}

	// Extracted-fact children are not embedded in this job, so there is no
	// child embedding_dim to persist here. Each child's own enrichment job
	// stamps its embedding_dim via MarkEnriched when it augments and embeds.

	// Group entity dim writes by dim so a single batch covers the whole job.
	// In practice every entity in one cycle lands at the same dim, so this is
	// almost always a single round-trip.
	entityIDsByDim := make(map[int][]uuid.UUID)
	for _, e := range p.entities {
		if e.ent != nil && e.ent.EmbeddingDim != nil {
			entityIDsByDim[*e.ent.EmbeddingDim] = append(entityIDsByDim[*e.ent.EmbeddingDim], e.id)
		}
	}
	for dim, ids := range entityIDsByDim {
		if err := wp.entities.UpdateEmbeddingDimBatch(ctx, ids, dim); err != nil {
			slog.Warn("enrichment: update entity embedding_dim batch", "dim", dim, "count", len(ids), "err", err)
		}
	}

	// Token usage for fact_extraction, entity_extraction, embedding, and
	// ingestion_decision is recorded by the UsageRecordingProvider
	// middleware on every wrapped provider call. No manual write needed.

	// Stamp the embedding step on success. EmbeddingDim being set means
	// runEmbedBatch produced a vector for this pending (either via the
	// batch embed call or reused from the ingestion-decision phase). The
	// step marker survives even if Complete fails below, so a retry of
	// this same job will skip re-embedding.
	if p.mem.EmbeddingDim != nil {
		if err := wp.queue.MarkStepCompleted(ctx, p.job.ID, model.StepEmbedding); err != nil {
			slog.Warn("enrichment: mark step completed (embedding)", "job", p.job.ID, "err", err)
		}
	}

	// Stamp the query_augmentation step only when the persisted vector was
	// actually built from augmented input. The marker condition mirrors the
	// augmented_queries / augmented_embedding_at writes above so the queue
	// row's step list and the memory row's marker columns agree: present
	// here iff the timestamp is set on the memory. The skip-reason column
	// is written at the top of finalizeJob so it covers every early-return
	// branch too.
	if p.embedUsedAugmented && p.mem.EmbeddingDim != nil && len(p.augmentedQueries) > 0 {
		if err := wp.queue.MarkStepCompleted(ctx, p.job.ID, model.StepQueryAugmentation); err != nil {
			slog.Warn("enrichment: mark step completed (query_augmentation)", "job", p.job.ID, "err", err)
		}
	}

	if p.partialRecoveryWarning != nil {
		if err := wp.queue.CompleteWithWarning(ctx, p.job.ID, p.workerID, p.partialRecoveryWarning); err != nil {
			if errors.Is(err, storage.ErrClaimLost) {
				slog.Info("enrichment: complete-with-warning dropped — claim lost", "job", p.job.ID, "worker", p.workerID)
				return nil
			}
			return fmt.Errorf("complete job (with warning): %w", err)
		}
	} else if err := wp.queue.Complete(ctx, p.job.ID, p.workerID); err != nil {
		if errors.Is(err, storage.ErrClaimLost) {
			slog.Info("enrichment: complete dropped — claim lost", "job", p.job.ID, "worker", p.workerID)
			return nil
		}
		return fmt.Errorf("complete job: %w", err)
	}

	return nil
}

// resolveOrCreateEntity looks up an existing entity in the database by
// canonical name within the given namespace. If no match is found, it creates
// a stub entity so that relationships extracted by the LLM are never dropped.
func (wp *WorkerPool) resolveOrCreateEntity(ctx context.Context, namespaceID uuid.UUID, name string) (*model.Entity, error) {
	canonical := strings.ToLower(strings.TrimSpace(name))
	similar, err := wp.entities.FindBySimilarity(ctx, namespaceID, canonical, "", 10)
	if err != nil {
		return nil, err
	}
	for i := range similar {
		if similar[i].Canonical == canonical {
			return &similar[i], nil
		}
	}

	// Entity doesn't exist — create it so the relationship is preserved.
	now := time.Now().UTC()
	entity := &model.Entity{
		ID:           uuid.New(),
		NamespaceID:  namespaceID,
		Name:         name,
		Canonical:    canonical,
		EntityType:   "unknown",
		Properties:   json.RawMessage(`{}`),
		MentionCount: 1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := wp.entities.Upsert(ctx, entity); err != nil {
		return nil, fmt.Errorf("create stub entity %q: %w", name, err)
	}
	slog.Debug("enrichment: created stub entity from relationship reference", "entity", name, "id", entity.ID)
	return entity, nil
}

// ---------------------------------------------------------------------------
// LLM extraction helpers
// ---------------------------------------------------------------------------

func (wp *WorkerPool) extractFacts(
	ctx context.Context,
	llm provider.LLMProvider,
	content string,
) (*service.FactExtractionEnvelope, error) {
	opts := service.ResolveCallOptions(ctx, wp.settings, service.FactCallOptionKeys(false))
	return service.ExtractFactsLLM(ctx, llm, wp.settings, content, opts)
}

func (wp *WorkerPool) extractEntities(
	ctx context.Context,
	llm provider.LLMProvider,
	content string,
) (*service.EntityExtractionEnvelope, error) {
	opts := service.ResolveCallOptions(ctx, wp.settings, service.EntityCallOptionKeys(false))
	return service.ExtractEntitiesLLM(ctx, llm, wp.settings, content, opts)
}

func mergeTags(parent, child []string) []string {
	combined := make([]string, 0, len(parent)+len(child))
	combined = append(combined, parent...)
	combined = append(combined, child...)
	return tags.Normalize(combined)
}

// mergeTagsIntoParent absorbs a suppressed extracted-fact's tags into the
// parent memory and writes a LineageExtractedFactSuppressed audit row.
// The inMemoryParent argument is the live in-memory parent struct the
// caller holds; on a real tag delta this function mutates it in place
// (Tags + Metadata) so that downstream code paths in the same job —
// notably finalizeJob's MarkEnriched(stampedMetadata) at worker.go ~1794,
// which is a partial-column write of metadata — do not clobber the
// freshly-written suppression stamp with a stale snapshot. childMemoryID
// is non-nil only on the backfill sweep path (a real child memory exists
// to reference in the audit JSON); the pre-insert guard passes nil because
// the suppressed fact never became a memory row.
//
// The Get → mutate → Update happens inside MutateInLock, which holds a
// cross-process row lock on the parent memory id (pg_advisory_xact_lock
// on Postgres, in-process mutex on SQLite). Without that lock, two
// concurrent enrichment jobs targeting the same parent would each read
// the same baseline, compute their tag unions independently, and the
// second write would clobber the first — silently losing absorbed tags.
func (wp *WorkerPool) mergeTagsIntoParent(
	ctx context.Context,
	inMemoryParent *model.Memory,
	childMemoryID *uuid.UUID,
	newTags []string,
	suppressedContent string,
	score float64,
	against string,
	source string,
) error {
	var delta []string
	now := time.Now().UTC()
	fresh, err := wp.memUpdater.MutateInLock(ctx, inMemoryParent.ID, func(mem *model.Memory) (bool, error) {
		merged := mergeTags(mem.Tags, newTags)
		delta = tagDelta(mem.Tags, merged)
		if len(delta) == 0 {
			return false, nil
		}
		mem.Tags = merged
		mem.UpdatedAt = now
		stampSuppressedFactMetadata(mem, score)
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("merge parent tags: %w", err)
	}
	if len(delta) > 0 {
		// Propagate the freshly-persisted state back into the caller's
		// in-memory copy so a subsequent MarkEnriched / Update in the
		// same job does not overwrite the stamp.
		inMemoryParent.Tags = fresh.Tags
		inMemoryParent.Metadata = fresh.Metadata
		inMemoryParent.UpdatedAt = fresh.UpdatedAt
	}

	if wp.lineage == nil {
		return nil
	}
	auditCtx := map[string]any{
		"source":             source,
		"suppressed_content": suppressedContent,
		"suppressed_tags":    newTags,
		"merged_tags_delta":  delta,
		"cosine_score":       score,
		"against":            against,
	}
	if childMemoryID != nil {
		auditCtx["child_id"] = childMemoryID.String()
	}
	ctxBytes, _ := json.Marshal(auditCtx)
	parentRef := inMemoryParent.ID
	lin := &model.MemoryLineage{
		ID:          uuid.New(),
		NamespaceID: inMemoryParent.NamespaceID,
		MemoryID:    inMemoryParent.ID,
		ParentID:    &parentRef,
		Relation:    model.LineageExtractedFactSuppressed,
		Context:     ctxBytes,
		CreatedAt:   now,
	}
	if err := wp.lineage.Create(ctx, lin); err != nil {
		return fmt.Errorf("create suppression lineage: %w", err)
	}
	return nil
}

// sweepHeartbeatEvery bounds how many children the paraphrase-guard backfill
// sweep processes before ticking the worker heartbeat. The stuck-claim sweeper
// reaps claims older than enrichment.stuck_threshold_seconds (default ~60s), so
// for parents with many children the sweep must heartbeat or the claim is
// silently reassigned mid-run.
const sweepHeartbeatEvery = 50

// runExtractedFactParaphraseGuardSweep is the worker-side handler for a
// paraphrase-guard backfill job. It enumerates the parent's extracted-fact
// children (restricted to storage.FactExtractionRelations so synthesis /
// non-fact children are never touched), fetches their stored embeddings,
// compares cosine similarity to the parent's stored embedding, and for
// each child at or above threshold supersedes the child first (the
// load-bearing dedup write), then merges its tags into the parent, then
// purges its vector. Operations are idempotent on retry: already-superseded
// children short-circuit at the top of the loop so a re-claim after a
// partial failure does not re-stamp the parent or duplicate lineage rows.
// Returns nil when no children exist or the sweep ran to completion.
// Returns a wrapped error only on repo-level failures so the queue's
// retry/fail path applies.
func (wp *WorkerPool) runExtractedFactParaphraseGuardSweep(ctx context.Context, workerID string, job *model.EnrichmentJob, parent *model.Memory) error {
	if wp.lineage == nil || wp.memories == nil || wp.memUpdater == nil || wp.vectorStore == nil {
		return fmt.Errorf("paraphrase guard backfill: missing repos")
	}

	threshold := wp.resolveParaphraseGuardThreshold(ctx)

	childIDs, err := wp.lineage.FindChildIDsByRelation(ctx, parent.NamespaceID, parent.ID, storage.FactExtractionRelations)
	if err != nil {
		return fmt.Errorf("find children: %w", err)
	}
	if len(childIDs) == 0 {
		slog.Info("enrichment: paraphrase backfill no children",
			"job", job.ID, "parent", parent.ID)
		return nil
	}

	parentVec, err := wp.fetchSingleVector(ctx, parent.ID, parent.EmbeddingDim)
	if err != nil {
		return fmt.Errorf("fetch parent embedding: %w", err)
	}
	if len(parentVec) == 0 {
		slog.Warn("enrichment: paraphrase backfill parent has no embedding",
			"job", job.ID, "parent", parent.ID)
		return nil
	}

	suppressed := 0
	checked := 0
	skippedDead := 0
	processedSinceHeartbeat := 0
	for _, cid := range childIDs {
		processedSinceHeartbeat++
		if processedSinceHeartbeat >= sweepHeartbeatEvery {
			processedSinceHeartbeat = 0
			if _, hErr := wp.queue.TickHeartbeat(ctx, workerID); hErr != nil {
				slog.Warn("enrichment: paraphrase backfill heartbeat",
					"job", job.ID, "err", hErr)
			}
		}

		child, err := wp.memories.GetByID(ctx, cid)
		if err != nil {
			slog.Warn("enrichment: paraphrase backfill child lookup",
				"job", job.ID, "child", cid, "err", err)
			continue
		}
		// Skip children that became dead between candidate enumeration
		// (in ListEnrichedParentsWithExtractedChildren / FindChildIDsByRelation)
		// and the worker claim. Also short-circuits retries against children
		// already superseded by an earlier sweep attempt, which is what
		// makes the loop idempotent: mergeTagsIntoParent + lineage write
		// never fire twice for the same (parent, child) pair.
		if child.DeletedAt != nil || child.SupersededBy != nil {
			skippedDead++
			continue
		}

		childVec, err := wp.fetchSingleVector(ctx, cid, child.EmbeddingDim)
		if err != nil {
			slog.Warn("enrichment: paraphrase backfill child embed fetch",
				"job", job.ID, "child", cid, "err", err)
			continue
		}
		if len(childVec) == 0 {
			continue
		}
		checked++
		sim := hnsw.CosineSimilarity(childVec, parentVec)
		if sim < threshold {
			continue
		}

		// Supersede first: this is the load-bearing write that flips the
		// child to dead. If it fails (e.g. ErrConcurrentSupersede because
		// another path already superseded the child), bail out for this
		// iteration WITHOUT merging tags or writing the lineage row so a
		// retry does not double-count.
		if err := wp.memUpdater.MarkSupersededBy(ctx, cid, parent.NamespaceID, parent.ID); err != nil {
			slog.Warn("enrichment: paraphrase backfill supersede child",
				"job", job.ID, "child", cid, "err", err)
			continue
		}
		cidCopy := cid
		if mErr := wp.mergeTagsIntoParent(ctx, parent, &cidCopy, child.Tags, child.Content, sim, "backfill", "fact_extraction_backfill"); mErr != nil {
			slog.Warn("enrichment: paraphrase backfill merge tags",
				"job", job.ID, "parent", parent.ID, "child", cid, "err", mErr)
			// Child is already superseded so it will not resurface in
			// recall; the missing tag-merge is a soft loss but does not
			// produce duplicate audit rows on retry (next claim sees
			// child.SupersededBy != nil and short-circuits above).
			continue
		}
		if err := wp.vectorStore.Delete(ctx, storage.VectorKindMemory, cid); err != nil {
			slog.Warn("enrichment: paraphrase backfill vector purge",
				"job", job.ID, "child", cid, "err", err)
		}
		suppressed++
	}

	slog.Info("enrichment: paraphrase backfill complete",
		"job", job.ID, "parent", parent.ID,
		"children", len(childIDs), "checked", checked,
		"suppressed", suppressed, "skipped_dead", skippedDead,
		"threshold", threshold)
	return nil
}

// resolveParaphraseGuardThreshold returns the effective cosine threshold for
// the paraphrase-guard suppression decision. Resolution order matches the
// contract documented on SettingExtractedFactParaphraseThreshold:
//  1. The new key when an operator has set it.
//  2. SettingDedupThreshold (ingestion-decision's threshold) so operators
//     who already tuned that knob get the inherited value.
//  3. 0.92 as a final hardcoded floor.
func (wp *WorkerPool) resolveParaphraseGuardThreshold(ctx context.Context) float64 {
	if v, err := wp.settings.ResolveFloat(ctx, service.SettingExtractedFactParaphraseThreshold, "global"); err == nil && v > 0 && v <= 1 {
		return v
	}
	if v, err := wp.settings.ResolveFloat(ctx, service.SettingDedupThreshold, "global"); err == nil && v > 0 && v <= 1 {
		return v
	}
	return 0.92
}

// fetchSingleVector retrieves one stored embedding by ID, using the row's
// own EmbeddingDim if available. Production vector stores key on (kind, id,
// dim) so passing the parent's dim for a child embedded at a different dim
// silently returns nothing; resolving per-row prevents that miss.
//
// When the row's EmbeddingDim is nil or non-positive (legacy rows whose dim
// was never recorded, or rows whose embedding write failed), the call
// returns (nil, nil) instead of forwarding dim=0 to GetByIDs. Every
// production store (pgvector, qdrant, hnsw) rejects dim=0 with a hard
// error, which would kill the entire sweep over an otherwise-healthy
// parent. Empty-vector return matches the sweep's existing "no embedding
// available, skip" handling, so a parent or child with an unknown dim is
// quietly skipped instead of crashing the job.
func (wp *WorkerPool) fetchSingleVector(ctx context.Context, id uuid.UUID, dim *int) ([]float32, error) {
	if dim == nil || *dim <= 0 {
		return nil, nil
	}
	out, err := wp.vectorStore.GetByIDs(ctx, storage.VectorKindMemory, []uuid.UUID{id}, *dim)
	if err != nil {
		return nil, err
	}
	return out[id], nil
}

// tagDelta returns the tags present in merged that were not present in
// original. Both inputs are assumed already normalized.
func tagDelta(original, merged []string) []string {
	if len(merged) == len(original) {
		return nil
	}
	seen := make(map[string]struct{}, len(original))
	for _, t := range original {
		seen[t] = struct{}{}
	}
	delta := make([]string, 0, len(merged)-len(original))
	for _, t := range merged {
		if _, ok := seen[t]; !ok {
			delta = append(delta, t)
		}
	}
	return delta
}

// stampSuppressedFactMetadata accumulates suppressed-fact audit data onto
// the parent memory's metadata JSON. Tracks count and the largest cosine
// observed so an operator can spot a parent that is absorbing many
// near-duplicate facts.
func stampSuppressedFactMetadata(mem *model.Memory, score float64) {
	meta := map[string]any{}
	if len(mem.Metadata) > 0 {
		_ = json.Unmarshal(mem.Metadata, &meta)
		if meta == nil {
			meta = map[string]any{}
		}
	}
	prev, _ := meta["tags_merged_from_suppressed_fact"].(map[string]any)
	if prev == nil {
		prev = map[string]any{}
	}
	count := 1
	if c, ok := prev["count"].(float64); ok {
		count = int(c) + 1
	}
	cosMax := score
	if cm, ok := prev["cosine_max"].(float64); ok && cm > cosMax {
		cosMax = cm
	}
	prev["count"] = count
	prev["last_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	prev["cosine_max"] = cosMax
	meta["tags_merged_from_suppressed_fact"] = prev
	encoded, err := json.Marshal(meta)
	if err != nil {
		return
	}
	mem.Metadata = encoded
}
