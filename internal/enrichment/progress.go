package enrichment

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// Stage names emitted in enrichment.job.* and enrichment.pool.tick payloads.
// They mirror the worker's own pipeline phases (pre-embed → embed → finalize)
// so the admin UI can show what part of the pipeline is currently slow.
const (
	StageStarted  = "started"
	StagePreEmbed = "pre_embed"
	StageEmbed    = "embed"
	StageFinalize = "finalize"
)

// inFlightJob is the per-job state the pool tick aggregates over. Reads are
// snapshot-style (one tick takes a single pass over the sync.Map), and stage
// transitions are short, so a plain sync.Map keyed by job ID is sufficient.
type inFlightJob struct {
	JobID       uuid.UUID
	MemoryID    uuid.UUID
	NamespaceID uuid.UUID
	WorkerID    string
	ClaimedAt   time.Time
	StartedAt   time.Time
	stage       string
	mu          sync.RWMutex
}

func (j *inFlightJob) Stage() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.stage
}

func (j *inFlightJob) SetStage(s string) {
	j.mu.Lock()
	j.stage = s
	j.mu.Unlock()
}

// progressTracker owns the live-job map and event emission for the pool.
// One instance per WorkerPool. nil-bus is supported (every emit is gated)
// so existing tests that construct the pool without a bus keep compiling.
type progressTracker struct {
	bus      events.EventBus
	settings *service.SettingsService

	jobs sync.Map // map[uuid.UUID]*inFlightJob

	mu     sync.Mutex
	paused bool
}

func newProgressTracker(bus events.EventBus, settings *service.SettingsService) *progressTracker {
	return &progressTracker{bus: bus, settings: settings}
}

// jobScope returns the SSE scope used for a specific job's events. The
// admin/enrichment monitor subscribes empty-scope so this is purely for
// downstream consumers that want to filter by namespace.
func jobScope(namespaceID uuid.UUID) string {
	return "namespace:" + namespaceID.String()
}

// JobStarted records the job and emits enrichment.job.started.
func (t *progressTracker) JobStarted(ctx context.Context, job *model.EnrichmentJob, mem *model.Memory, workerID string) {
	if t == nil {
		return
	}
	claimed := time.Now().UTC()
	if job.ClaimedAt != nil {
		claimed = *job.ClaimedAt
	}
	in := &inFlightJob{
		JobID:       job.ID,
		MemoryID:    job.MemoryID,
		NamespaceID: job.NamespaceID,
		WorkerID:    workerID,
		ClaimedAt:   claimed,
		StartedAt:   time.Now().UTC(),
		stage:       StagePreEmbed,
	}
	t.jobs.Store(job.ID, in)

	memID := job.MemoryID
	if mem != nil {
		memID = mem.ID
	}
	events.Emit(ctx, t.bus, events.EnrichmentJobStarted, jobScope(job.NamespaceID), map[string]any{
		"job_id":       job.ID.String(),
		"memory_id":    memID.String(),
		"namespace_id": job.NamespaceID.String(),
		"worker_id":    workerID,
		"started_at":   in.StartedAt,
		"stage":        StageStarted,
	})
}

// SetStage updates the live stage for a job. No-op if the job is not in flight.
func (t *progressTracker) SetStage(jobID uuid.UUID, stage string) {
	if t == nil {
		return
	}
	v, ok := t.jobs.Load(jobID)
	if !ok {
		return
	}
	v.(*inFlightJob).SetStage(stage)
}

// JobCompleted removes the job from the live map and emits
// enrichment.job.completed. err may be nil on success; usage may be nil if
// the job did not reach an LLM call (e.g. cascade-skipped).
func (t *progressTracker) JobCompleted(
	ctx context.Context,
	jobID, memoryID, namespaceID uuid.UUID,
	workerID string,
	startedAt time.Time,
	tokensTotal, tokensPrompt, tokensCompletion int,
	err error,
) {
	if t == nil {
		return
	}
	t.jobs.Delete(jobID)

	now := time.Now().UTC()
	latency := time.Since(startedAt)
	if startedAt.IsZero() {
		latency = 0
	}
	payload := map[string]any{
		"job_id":       jobID.String(),
		"memory_id":    memoryID.String(),
		"namespace_id": namespaceID.String(),
		"worker_id":    workerID,
		"ended_at":     now,
		"latency_ms":   latency.Milliseconds(),
		"ok":           err == nil,
		"tokens": map[string]int{
			"prompt":     tokensPrompt,
			"completion": tokensCompletion,
			"total":      tokensTotal,
		},
	}
	if err != nil {
		payload["error"] = err.Error()
	}
	events.Emit(ctx, t.bus, events.EnrichmentJobCompleted, jobScope(namespaceID), payload)
}

// SetPaused records the worker-pool paused state for the next tick.
func (t *progressTracker) SetPaused(paused bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.paused = paused
	t.mu.Unlock()
}

func (t *progressTracker) snapshotPaused() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.paused
}

// EmitTick publishes one enrichment.pool.tick event. Called from the
// pool-level ticker goroutine.
func (t *progressTracker) EmitTick(ctx context.Context) {
	if t == nil {
		return
	}
	var (
		inFlight int
		oldest   time.Time
		byStage  = make(map[string]int)
	)
	t.jobs.Range(func(_, v any) bool {
		j := v.(*inFlightJob)
		inFlight++
		byStage[j.Stage()]++
		if oldest.IsZero() || j.ClaimedAt.Before(oldest) {
			oldest = j.ClaimedAt
		}
		return true
	})

	payload := map[string]any{
		"in_flight": inFlight,
		"by_stage":  byStage,
		"paused":    t.snapshotPaused(),
		"timestamp": time.Now().UTC(),
	}
	if !oldest.IsZero() {
		payload["oldest_claim_at"] = oldest
		payload["oldest_claim_age_ms"] = time.Since(oldest).Milliseconds()
	} else {
		payload["oldest_claim_age_ms"] = int64(0)
	}
	events.Emit(ctx, t.bus, events.EnrichmentPoolTick, "", payload)
}

// runTickLoop runs a single pool-level ticker goroutine. interval is read
// once at startup from service.SettingEnrichmentPoolTickIntervalSeconds.
// Returns when ctx is canceled. Bus-nil is a no-op (every EmitTick is gated).
func (t *progressTracker) runTickLoop(ctx context.Context) {
	if t == nil || t.bus == nil {
		return
	}
	interval := time.Duration(0)
	if t.settings != nil {
		interval = t.settings.ResolveDurationSecondsWithDefault(ctx,
			service.SettingEnrichmentPoolTickIntervalSeconds, "global")
	}
	if interval < time.Second {
		interval = 5 * time.Second
	}
	slog.Info("enrichment: pool tick loop started", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	t.EmitTick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.EmitTick(ctx)
		}
	}
}
