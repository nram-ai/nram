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

// workerEmbedTimeout bounds a single embed HTTP call inside the worker.
const workerEmbedTimeout = 30 * time.Second

// batchClaimSize caps the jobs claimed per worker iteration; one claim
// produces one shared embed call.
const batchClaimSize = 16

// preEmbedConcurrency caps fan-out of per-job fact/entity LLM calls inside a
// single processBatch invocation. Small enough to stay under per-minute rate
// limits on typical accounts.
const preEmbedConcurrency = 4

// embedInputCap caps inputs per provider call; larger batches are chunked.
// Conservative vs. OpenAI's 2048-input limit to account for per-input token
// ceilings on nearest providers.
const embedInputCap = 256

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
// (idempotent) so retries skip phases that already ran.
type QueueClaimer interface {
	ClaimNext(ctx context.Context, workerID string) (*model.EnrichmentJob, error)
	ClaimNextBatch(ctx context.Context, workerID string, max int) ([]*model.EnrichmentJob, error)
	Complete(ctx context.Context, id uuid.UUID) error
	Fail(ctx context.Context, id uuid.UUID, errMsg string) error
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
// Parsed extraction types
// ---------------------------------------------------------------------------

type extractedFact struct {
	Content    string   `json:"content"`
	Fact       string   `json:"fact"` // alternate field name some LLMs use
	Confidence float64  `json:"confidence"`
	Tags       []string `json:"tags"`
}

// text returns the fact content, preferring "content" over "fact".
func (f extractedFact) text() string {
	if f.Content != "" {
		return f.Content
	}
	return f.Fact
}

type extractedEntity struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

type extractedRelationship struct {
	Source   string  `json:"source"`
	Target   string  `json:"target"`
	Relation string  `json:"relation"`
	Weight   float64 `json:"weight"`
}

type entityExtractionResult struct {
	Entities      []extractedEntity       `json:"entities"`
	Relationships []extractedRelationship `json:"relationships"`
}

// ---------------------------------------------------------------------------
// WorkerConfig / WorkerPool
// ---------------------------------------------------------------------------

// WorkerConfig controls the behavior of the enrichment worker pool.
type WorkerConfig struct {
	Workers      int           // number of concurrent workers (default: 1 for SQLite, 2 for Postgres)
	PollInterval time.Duration // how often idle workers poll for jobs, default 5s
	Backend      string        // "sqlite" or "postgres" — used to tune defaults
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Workers <= 0 {
		if c.Backend == "sqlite" {
			c.Workers = 1 // single writer makes multiple workers pointless on SQLite
		} else {
			c.Workers = 2
		}
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
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
		config:            config.withDefaults(),
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
	const maxBackoff = 30 // seconds

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

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

		jobs, err := wp.queue.ClaimNextBatch(ctx, workerID, batchClaimSize)
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

		wp.processBatch(ctx, workerID, jobs)
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
// preEmbedConcurrency), then makes one shared embed call, then finalizes
// each. Bounded concurrency keeps LLM provider rate limits safe.
func (wp *WorkerPool) processBatch(ctx context.Context, workerID string, jobs []*model.EnrichmentJob) {
	results := make([]*pendingJob, len(jobs))
	sem := make(chan struct{}, preEmbedConcurrency)
	var wg sync.WaitGroup
	for i, job := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, job *model.EnrichmentJob) {
			defer func() { <-sem; wg.Done() }()
			p, err := wp.runPreEmbed(ctx, job)
			if err != nil {
				slog.Error("enrichment: batch pre-embed failed", "worker", workerID, "job", job.ID, "err", err)
				return
			}
			results[i] = p
		}(i, job)
	}
	wg.Wait()

	pendings := make([]*pendingJob, 0, len(jobs))
	for _, p := range results {
		if p != nil {
			pendings = append(pendings, p)
		}
	}
	if len(pendings) == 0 {
		return
	}
	wp.runEmbedBatch(ctx, pendings)
	for _, p := range pendings {
		if err := wp.finalizeJob(ctx, p); err != nil {
			slog.Error("enrichment: batch finalize failed", "worker", workerID, "job", p.job.ID, "err", err)
		}
	}
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

	if !hasFact && !hasEntity && !hasEmbed {
		_ = wp.queue.Fail(ctx, job.ID, "no providers configured")
		return nil, fmt.Errorf("no providers configured, job requeued")
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
		facts        []extractedFact
		entResult    *entityExtractionResult
		factUsage    *provider.TokenUsage
		entityUsage  *provider.TokenUsage
		factModel    string
		entityModel  string
		factProvider string
		entityProv   string
		factErr      error
		entityErr    error
	)

	if hasFact && !skipFact {
		facts, factUsage, factModel, factProvider, factErr = wp.extractFacts(ctx, wp.factProvider(), mem.Content)
	}
	if hasEntity && !skipEntity {
		entResult, entityUsage, entityModel, entityProv, entityErr = wp.extractEntities(ctx, wp.entityProvider(), mem.Content)
	}

	if (hasFact && factErr != nil) && (hasEntity && entityErr != nil) {
		errMsg := fmt.Sprintf("fact: %v; entity: %v", factErr, entityErr)
		_ = wp.queue.Fail(ctx, job.ID, errMsg)
		return nil, fmt.Errorf("all extractions failed: %s", errMsg)
	}
	if hasFact && factErr != nil {
		_ = wp.queue.Fail(ctx, job.ID, fmt.Sprintf("fact extraction: %v", factErr))
		return nil, fmt.Errorf("fact extraction: %w", factErr)
	}

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
		job:          job,
		mem:          mem,
		children:     children,
		entities:     entities,
		factUsage:    factUsage,
		factModel:    factModel,
		factProvider: factProvider,
		entityUsage:  entityUsage,
		entityModel:  entityModel,
		entityProv:   entityProv,
	}
	p.applyIngestion(ingestion)
	return p, nil
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
		embeddings, _, modelName, err = wp.embedChunked(batchCtx, ep, inputs, dim)
		_ = modelName
		_ = usage
		if err != nil {
			slog.Error("enrichment: batched embed", "jobs", len(pendings), "err", err)
			return
		}
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
	if err := wp.vectorStore.UpsertBatch(ctx, items); err != nil {
		slog.Error("enrichment: upsert vectors batch", "jobs", len(pendings), "items", len(items), "err", err)
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
	}
}

// embedChunked makes one or more Embed calls so each request respects
// embedInputCap. Returned embeddings preserve input order; returned usage is
// the sum across chunks; returned model is the last non-empty model string.
func (wp *WorkerPool) embedChunked(ctx context.Context, ep provider.EmbeddingProvider, inputs []string, dim int) ([][]float32, provider.TokenUsage, string, error) {
	var (
		out   = make([][]float32, 0, len(inputs))
		usage provider.TokenUsage
		model string
	)
	for start := 0; start < len(inputs); start += embedInputCap {
		end := start + embedInputCap
		if end > len(inputs) {
			end = len(inputs)
		}
		embedCtx, cancel := context.WithTimeout(ctx, workerEmbedTimeout)
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

	if err := wp.queue.Complete(ctx, p.job.ID); err != nil {
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
) ([]extractedFact, *provider.TokenUsage, string, string, error) {
	prompt := fmt.Sprintf(service.ResolveOrDefault(ctx, wp.settings, service.SettingFactPrompt, "global"), content)
	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationFactExtraction), &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   2048,
		Temperature: 0.2,
		JSONMode:    true,
	})
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("fact LLM call: %w", err)
	}

	facts, parseErr := parseFactResponse(resp.Content)
	if parseErr != nil {
		return nil, &resp.Usage, resp.Model, llm.Name(), fmt.Errorf("parse facts: %w", parseErr)
	}
	return facts, &resp.Usage, resp.Model, llm.Name(), nil
}

func (wp *WorkerPool) extractEntities(
	ctx context.Context,
	llm provider.LLMProvider,
	content string,
) (*entityExtractionResult, *provider.TokenUsage, string, string, error) {
	prompt := fmt.Sprintf(service.ResolveOrDefault(ctx, wp.settings, service.SettingEntityPrompt, "global"), content)
	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationEntityExtraction), &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   2048,
		Temperature: 0.2,
		JSONMode:    true,
	})
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("entity LLM call: %w", err)
	}

	result, parseErr := parseEntityResponse(resp.Content)
	if parseErr != nil {
		return nil, &resp.Usage, resp.Model, llm.Name(), fmt.Errorf("parse entities: %w", parseErr)
	}
	return result, &resp.Usage, resp.Model, llm.Name(), nil
}

// ---------------------------------------------------------------------------
// Response parsing (JSON mode guarantees valid JSON from the LLM)
// ---------------------------------------------------------------------------

// parseFactResponse parses a fact extraction response. With JSON mode enabled
// the LLM is constrained to produce valid JSON, so only direct unmarshal with
// a string-array fallback is needed.
func parseFactResponse(raw string) ([]extractedFact, error) {
	raw = strings.TrimSpace(raw)

	// Try array of structured facts.
	var facts []extractedFact
	if err := json.Unmarshal([]byte(raw), &facts); err == nil && len(facts) > 0 {
		for i := range facts {
			facts[i].Content = facts[i].text()
		}
		return facts, nil
	}

	// Single fact object instead of array.
	var single extractedFact
	if err := json.Unmarshal([]byte(raw), &single); err == nil && single.text() != "" {
		single.Content = single.text()
		return []extractedFact{single}, nil
	}

	// Wrapper object with a "facts" key.
	var wrapper struct {
		Facts []extractedFact `json:"facts"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err == nil && len(wrapper.Facts) > 0 {
		for i := range wrapper.Facts {
			wrapper.Facts[i].Content = wrapper.Facts[i].text()
		}
		return wrapper.Facts, nil
	}

	// Plain string array.
	var strs []string
	if err := json.Unmarshal([]byte(raw), &strs); err == nil && len(strs) > 0 {
		facts = make([]extractedFact, len(strs))
		for i, s := range strs {
			facts[i] = extractedFact{Content: s, Confidence: 0.8}
		}
		return facts, nil
	}

	preview := raw
	if len(preview) > 200 {
		preview = preview[:200] + "..."
	}
	return nil, fmt.Errorf("unable to parse fact JSON from LLM response: %q", preview)
}

// parseEntityResponse parses an entity/relationship extraction response.
// With JSON mode enabled the LLM is constrained to produce valid JSON.
func parseEntityResponse(raw string) (*entityExtractionResult, error) {
	raw = strings.TrimSpace(raw)

	var result entityExtractionResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		preview := raw
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		return nil, fmt.Errorf("unable to parse entity JSON from LLM response: %q", preview)
	}
	return &result, nil
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
