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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
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
	var coe *provider.CircuitOpenError
	if errors.As(err, &coe) {
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
		retryIn := time.Until(coe.RetryAt).Round(time.Second)
		if retryIn < 0 {
			retryIn = 0
		}
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
	var fail *service.ExtractionFailure
	if errors.As(err, &fail) {
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
// row), or a plain string for non-extraction failures (memory lookup,
// vector upsert, lifecycle errors). Release errors are logged at WARN
// since the job is in an indeterminate but self-healing state.
func (wp *WorkerPool) requeueOrFail(ctx context.Context, jobID uuid.UUID, err error, payload any) {
	if isTransientLLMErr(err) {
		if relErr := wp.queue.Release(ctx, jobID); relErr != nil {
			slog.Warn("enrichment: queue release after transient failure",
				"job", jobID, "err", relErr)
		}
		return
	}
	if failErr := wp.queue.Fail(ctx, jobID, payload); failErr != nil {
		slog.Warn("enrichment: queue fail",
			"job", jobID, "err", failErr)
	}
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

// MemoryUpdater persists changes to an existing memory.
// UpdateEmbeddingDim is a focused setter so child memories created in the
// same job can record their dim without rewriting every column.
type MemoryUpdater interface {
	Update(ctx context.Context, mem *model.Memory) error
	UpdateEmbeddingDim(ctx context.Context, id uuid.UUID, dim int) error
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
	ClaimNext(ctx context.Context, workerID string) (*model.EnrichmentJob, error)
	ClaimNextBatch(ctx context.Context, workerID string, max int) ([]*model.EnrichmentJob, error)
	Complete(ctx context.Context, id uuid.UUID) error
	CompleteWithWarning(ctx context.Context, id uuid.UUID, payload any) error
	Fail(ctx context.Context, id uuid.UUID, payload any) error
	Release(ctx context.Context, id uuid.UUID) error
	MarkStepCompleted(ctx context.Context, id uuid.UUID, step string) error
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
// produced edges for a memory and skip the LLM step.
type RelationshipCreator interface {
	Create(ctx context.Context, rel *model.Relationship) error
	FindActiveByTriple(ctx context.Context, namespaceID, sourceID, targetID uuid.UUID, relation string) (*model.Relationship, error)
	UpdateWeight(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, weight float64) error
	HasBySourceMemory(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) (bool, error)
}

// LineageCreator records parent-child lineage between memories.
// HasExtractedFactChildren is the probe runPreEmbed uses to detect that
// fact extraction has already produced children for a memory and skip the
// LLM step.
type LineageCreator interface {
	Create(ctx context.Context, lineage *model.MemoryLineage) error
	HasExtractedFactChildren(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) (bool, error)
}

// VectorWriter upserts embedding vectors for memories and entities. Kind
// selects which table family the single-vector Upsert targets; UpsertBatch
// reads Kind from each item. Delete drops a single vector by parent ID and
// is invoked when ingestion-decision supersedes a target memory so the
// stored vector does not outlive the row's superseded state.
type VectorWriter interface {
	Upsert(ctx context.Context, kind storage.VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error
	UpsertBatch(ctx context.Context, items []storage.VectorUpsertItem) error
	Delete(ctx context.Context, kind storage.VectorKind, id uuid.UUID) error
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
		c.Workers = settings.ResolveIntWithDefault(ctx, key, "global")
		if c.Workers < 1 {
			c.Workers = 1
		}
	}
	if c.PollInterval <= 0 {
		c.PollInterval = settings.ResolveDurationSecondsWithDefault(ctx,
			service.SettingEnrichmentWorkerPollIntervalSeconds, "global")
		if c.PollInterval < time.Second {
			c.PollInterval = time.Second
		}
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

	idleWorkers atomic.Int32

	cancel context.CancelFunc
	wg     sync.WaitGroup
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
) *WorkerPool {
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
	}
}

// Start launches the configured number of worker goroutines. Each loops
// until the pool is stopped: claim a job, process it, repeat (or sleep on
// empty queue).
func (wp *WorkerPool) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	wp.cancel = cancel

	for i := range wp.config.Workers {
		workerID := fmt.Sprintf("worker-%d", i)
		wp.wg.Add(1)
		go wp.run(ctx, workerID)
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
	wait := time.Until(deadline)
	if wait < 500*time.Millisecond {
		wait = 500 * time.Millisecond
	}
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
	wait := backoff + jitter
	if wait < time.Second {
		wait = time.Second
	}

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

type childFact struct {
	id      uuid.UUID
	content string
	mem     *model.Memory
}

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
// finalize phases. embedStart/embedCount index into the shared batched embed
// response for the parent + children; embedEntStart/embedEntCount cover the
// entity canonicals appended after them in the same batch. Ingestion-decision
// fields are populated by runIngestionDecision when the phase is enabled;
// parentEmbedFromPhase / shortCircuitDelete are derived from them.
type pendingJob struct {
	job           *model.EnrichmentJob
	mem           *model.Memory
	children      []childFact
	entities      []entityFact
	factUsage     *provider.TokenUsage
	factModel     string
	factProvider  string
	entityUsage   *provider.TokenUsage
	entityModel   string
	entityProv    string
	embedStart    int
	embedCount    int
	embedEntStart int
	embedEntCount int

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
}

func (wp *WorkerPool) processJob(ctx context.Context, workerID string, job *model.EnrichmentJob) error {
	p, err := wp.runPreEmbed(ctx, job)
	if err != nil {
		return err
	}
	// runPreEmbed returns (nil, nil) when a per-namespace gate skipped the
	// job; the queue entry is already Complete-marked, so there is nothing
	// further to do for this caller.
	if p == nil {
		return nil
	}
	wp.runEmbedBatch(ctx, []*pendingJob{p})
	return wp.finalizeJob(ctx, p)
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

	preEmbedFanOut := wp.settings.ResolveIntWithDefault(ctx,
		service.SettingEnrichmentWorkerPreEmbedConcurrency, "global")
	if preEmbedFanOut < 1 {
		preEmbedFanOut = 1
	}

	results := make([]*pendingJob, len(jobs))
	preEmbedErrs := make([]error, len(jobs))
	sem := make(chan struct{}, preEmbedFanOut)
	var wg sync.WaitGroup
	for i, job := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, job *model.EnrichmentJob) {
			defer func() { <-sem; wg.Done() }()
			p, err := wp.runPreEmbed(ctx, job)
			if err != nil {
				preEmbedErrs[i] = err
				wp.logBreakerOrError(ctx, "enrichment: batch pre-embed failed",
					err, "worker", workerID, "job", job.ID)
				return
			}
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
	wp.runEmbedBatch(ctx, pendings)
	for _, p := range pendings {
		if err := wp.finalizeJob(ctx, p); err != nil {
			wp.logBreakerOrError(ctx, "enrichment: batch finalize failed",
				err, "worker", workerID, "job", p.job.ID)
		}
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
func (wp *WorkerPool) runPreEmbed(ctx context.Context, job *model.EnrichmentJob) (*pendingJob, error) {
	mem, err := wp.memories.GetByID(ctx, job.MemoryID)
	if err != nil {
		failErr := wp.queue.Fail(ctx, job.ID, fmt.Sprintf("memory lookup: %v", err))
		if failErr != nil {
			slog.Error("enrichment: fail-mark error", "job", job.ID, "err", failErr)
		}
		return nil, fmt.Errorf("memory lookup: %w", err)
	}

	// Per-namespace enrichment_enabled cascade. A project or user may opt
	// their namespace out of enrichment even while the system-level toggle
	// is on. Mark the job complete (not failed) so the queue does not
	// retry; the memory simply stays unenriched until the toggle flips
	// back. Returning (nil, nil) signals to callers that nothing further
	// should happen with this job.
	if wp.cascade != nil && !wp.cascade.ResolveEnrichmentEnabled(ctx, mem.NamespaceID) {
		if err := wp.queue.Complete(ctx, job.ID); err != nil {
			slog.Error("enrichment: complete-skipped error", "job", job.ID, "err", err)
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
		p := &pendingJob{job: job, mem: mem}
		p.applyIngestion(ingestion)
		return p, nil
	}

	hasFact := wp.factProvider() != nil
	hasEntity := wp.entityProvider() != nil
	hasEmbed := wp.embedProvider() != nil

	// Race window: a slot can be removed via /admin/providers after the
	// batch is claimed. Release (no attempts bump) so the backlog drains
	// automatically when the admin restores the slot.
	if !(hasFact && hasEntity && hasEmbed) {
		_ = wp.queue.Release(ctx, job.ID)
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
	stepDone := stepDoneSet(job.StepsCompleted)
	skipFact := mem.Enriched || stepDone[model.StepFactExtraction]
	skipEntity := mem.Enriched || stepDone[model.StepEntityExtraction]

	if hasFact && !skipFact {
		if has, probeErr := wp.lineage.HasExtractedFactChildren(ctx, mem.NamespaceID, mem.ID); probeErr != nil {
			slog.Warn("enrichment: probe extracted-fact lineage", "job", job.ID, "memory", mem.ID, "err", probeErr)
		} else if has {
			skipFact = true
		}
	}
	if hasEntity && !skipEntity {
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

	if hasFact && !skipFact {
		factEnv, factErr = wp.extractFacts(ctx, wp.factProvider(), mem.Content)
	}
	if hasEntity && !skipEntity {
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

	if (hasFact && factErr != nil) && (hasEntity && entityErr != nil) {
		joined := errors.Join(factErr, entityErr)
		// Treat as transient only when *both* legs are: if one leg is a real
		// fault, burning a queue attempt is the right policy.
		if isTransientLLMErr(factErr) && isTransientLLMErr(entityErr) {
			wp.requeueOrFail(ctx, job.ID, factErr, joined.Error())
		} else {
			wp.requeueOrFail(ctx, job.ID, errNonTransient, joined.Error())
		}
		return nil, fmt.Errorf("extraction failed: %w", joined)
	}
	if hasFact && factErr != nil {
		wp.requeueOrFail(ctx, job.ID, factErr, extractionFailPayload(factErr))
		return nil, fmt.Errorf("fact extraction: %w", factErr)
	}
	// Entity-only failure is intentionally soft: facts may have succeeded and
	// the job can still produce useful output. The job continues with empty
	// entities. (Both-failed and fact-only branches above already handle the
	// hard cases.)

	children := make([]childFact, 0, len(facts))
	for _, fact := range facts {
		childID := uuid.New()
		child := &model.Memory{
			ID:          childID,
			NamespaceID: mem.NamespaceID,
			Content:     fact.Content,
			Confidence:  fact.Confidence,
			Tags:        mergeTags(mem.Tags, fact.Tags),
			Source:      mem.Source,
			Importance:  0.5,
			Enriched:    true,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}
		if err := wp.memCreator.Create(ctx, child); err != nil {
			slog.Error("enrichment: create child memory", "job", job.ID, "err", err)
			continue
		}
		children = append(children, childFact{id: childID, content: fact.Content, mem: child})

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
	}

	// Mark fact_extraction as completed when the LLM call succeeded —
	// even if 0 facts came back. The signal is "the step ran", not "the
	// step produced output", so a legitimate 0-fact memory does not
	// re-extract on the next claim.
	if hasFact && !skipFact && factErr == nil {
		if err := wp.queue.MarkStepCompleted(ctx, job.ID, model.StepFactExtraction); err != nil {
			slog.Warn("enrichment: mark step completed (fact)", "job", job.ID, "err", err)
		}
	}

	entities := wp.upsertEntitiesAndRelationships(ctx, job, mem, entResult)

	if hasEntity && !skipEntity && entityErr == nil {
		if err := wp.queue.MarkStepCompleted(ctx, job.ID, model.StepEntityExtraction); err != nil {
			slog.Warn("enrichment: mark step completed (entity)", "job", job.ID, "err", err)
		}
	}

	p := &pendingJob{
		job:                    job,
		mem:                    mem,
		children:               children,
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
		r := &model.Relationship{
			ID:           uuid.New(),
			NamespaceID:  mem.NamespaceID,
			SourceID:     srcID,
			TargetID:     tgtID,
			Relation:     rel.Relation,
			Weight:       rel.Weight,
			SourceMemory: &memID,
			ValidFrom:    mem.CreatedAt,
			CreatedAt:    time.Now().UTC(),
		}
		if err := wp.relationships.Create(ctx, r); err != nil {
			slog.Error("enrichment: create relationship", "job", job.ID, "err", err)
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
		p.embedStart = len(inputs)
		// Reuse the embedding the ingestion-decision phase already
		// computed for this content. The vector is upserted in the
		// upsert loop below; we just keep it out of the embed call.
		if !p.parentEmbedFromPhase() {
			inputs = append(inputs, p.mem.Content)
		}
		for _, c := range p.children {
			inputs = append(inputs, c.content)
		}
		if p.parentEmbedFromPhase() {
			p.embedCount = len(p.children)
		} else {
			p.embedCount = 1 + len(p.children)
		}

		p.embedEntStart = len(inputs)
		for _, e := range p.entities {
			inputs = append(inputs, e.canonical)
		}
		p.embedEntCount = len(p.entities)
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
					if relErr := wp.queue.Release(ctx, p.job.ID); relErr != nil {
						slog.Warn("enrichment: queue release after transient embed failure",
							"job", p.job.ID, "err", relErr)
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

		// Children index from embedStart (parent absent) or embedStart+1
		// (parent present in the embed batch).
		childOffset := p.embedStart
		if !p.parentEmbedFromPhase() {
			childOffset = p.embedStart + 1
		}
		for i, c := range p.children {
			idx := childOffset + i
			if idx >= len(embeddings) {
				break
			}
			vec := embeddings[idx]
			if d := len(vec); d > 0 {
				if c.mem != nil {
					c.mem.EmbeddingDim = &d
				}
				items = append(items, storage.VectorUpsertItem{
					Kind:        storage.VectorKindMemory,
					ID:          c.id,
					NamespaceID: p.mem.NamespaceID,
					Embedding:   vec,
					Dimension:   d,
				})
			}
		}
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
			if failErr := wp.queue.Fail(ctx, p.job.ID, fmt.Sprintf("vector upsert batch: %v", err)); failErr != nil {
				slog.Warn("enrichment: queue fail after vector batch failure",
					"job", p.job.ID, "err", failErr)
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
	cap := wp.settings.ResolveIntWithDefault(ctx,
		service.SettingEnrichmentWorkerEmbedInputCap, "global")
	if cap < 1 {
		cap = 1
	}
	timeout := wp.settings.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentWorkerEmbedTimeoutSeconds, "global")
	for start := 0; start < len(inputs); start += cap {
		end := start + cap
		if end > len(inputs) {
			end = len(inputs)
		}
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

	stampIngestionMetadata(p)

	p.mem.Enriched = true
	p.mem.UpdatedAt = time.Now().UTC()
	if err := wp.memUpdater.Update(ctx, p.mem); err != nil {
		_ = wp.queue.Fail(ctx, p.job.ID, fmt.Sprintf("update memory enriched: %v", err))
		return fmt.Errorf("update memory: %w", err)
	}

	for _, c := range p.children {
		if c.mem != nil && c.mem.EmbeddingDim != nil {
			if err := wp.memUpdater.UpdateEmbeddingDim(ctx, c.id, *c.mem.EmbeddingDim); err != nil {
				slog.Warn("enrichment: update child embedding_dim", "child", c.id, "err", err)
			}
		}
	}

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

	if p.partialRecoveryWarning != nil {
		if err := wp.queue.CompleteWithWarning(ctx, p.job.ID, p.partialRecoveryWarning); err != nil {
			return fmt.Errorf("complete job (with warning): %w", err)
		}
	} else if err := wp.queue.Complete(ctx, p.job.ID); err != nil {
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
	seen := make(map[string]bool, len(parent)+len(child))
	result := make([]string, 0, len(parent)+len(child))
	for _, t := range parent {
		if !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}
	for _, t := range child {
		if !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}
	return result
}
