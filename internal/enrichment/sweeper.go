package enrichment

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// stuckJobStore is the storage subset the sweeper relies on. Defined here so
// tests can substitute a fake repo without standing up a full database.
type stuckJobStore interface {
	ListStaleClaimed(ctx context.Context, threshold time.Duration, limit int) ([]*model.EnrichmentJob, error)
	RequeueStale(ctx context.Context, id uuid.UUID, reason string) (bool, error)
}

// sweeperSettingsResolver is the slice of *service.SettingsService the
// sweeper actually touches. Mirrors dreaming/interfaces.go's SettingsResolver
// shape so test fakes can be small.
type sweeperSettingsResolver interface {
	ResolveDurationSecondsWithDefault(ctx context.Context, key, scope string) time.Duration
	ResolveIntWithDefault(ctx context.Context, key, scope string) int
}

// StuckJobSweeper periodically scans enrichment_queue for rows in
// status='processing' whose updated_at has gone past
// enrichment.stuck_threshold_seconds and auto-requeues them via Retry
// semantics (attempts++ so a genuine poison-pill memory still hits
// max_attempts and stops looping).
//
// The threshold must exceed the longest legitimate batch runtime so a slow
// LLM call is not mistaken for a dead worker; the heartbeat goroutine in
// WorkerPool advances updated_at every enrichment.worker.heartbeat_seconds
// so a live worker holding a long batch does not look stale.
type StuckJobSweeper struct {
	queueRepo stuckJobStore
	settings  sweeperSettingsResolver
	eventBus  events.EventBus

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewStuckJobSweeper constructs a sweeper. queueRepo is the
// EnrichmentQueueRepo in production; tests can substitute any stuckJobStore.
// eventBus may be nil to suppress enrichment.job.requeued events.
func NewStuckJobSweeper(
	queueRepo stuckJobStore,
	settings sweeperSettingsResolver,
	eventBus events.EventBus,
) *StuckJobSweeper {
	return &StuckJobSweeper{
		queueRepo: queueRepo,
		settings:  settings,
		eventBus:  eventBus,
	}
}

// Start launches the sweeper loop in a background goroutine. Independent of
// the WorkerPool's lifecycle so a wedged pool (which is exactly the failure
// mode this sweeper recovers from) can't also block the recovery path.
// The sweep interval is read once at start; changing it requires a server
// restart. The threshold is read every sweep tick so it hot-reloads.
func (s *StuckJobSweeper) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.run(ctx)
}

// Stop signals the sweeper goroutine to exit and waits for it to finish.
// Should be called BEFORE WorkerPool.Stop in graceful shutdown so a final
// sweep cannot race a worker that's normally finishing its current batch.
func (s *StuckJobSweeper) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *StuckJobSweeper) run(ctx context.Context) {
	defer s.wg.Done()

	interval := s.settings.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentStuckSweep, "global")
	if interval < time.Second {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Sweep(ctx); err != nil {
				slog.Warn("enrichment: stuck-job sweep failed", "err", err)
			}
		}
	}
}

// Sweep finds enrichment_queue rows in status='processing' with updated_at
// older than the configured stuck threshold and resets each back to pending
// via RequeueStale. Idempotent under multi-instance sweep: RequeueStale's
// `WHERE status='processing'` guard ensures a row already requeued by another
// instance returns (false, nil) and is skipped here.
//
// Failures on individual rows are logged and the loop continues — one bad
// row should not block recovery of the rest of the batch. A failure to even
// list the stale rows is returned as an error so the run loop can log it.
func (s *StuckJobSweeper) Sweep(ctx context.Context) error {
	threshold := s.settings.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentStuckThreshold, "global")
	if threshold < time.Second {
		threshold = 30 * time.Minute
	}

	scanLimit := s.settings.ResolveIntWithDefault(ctx, service.SettingEnrichmentStuckScanLimit, "global")
	jobs, err := s.queueRepo.ListStaleClaimed(ctx, threshold, scanLimit)
	if err != nil {
		return fmt.Errorf("list stale claimed: %w", err)
	}
	if len(jobs) == 0 {
		return nil
	}

	now := time.Now().UTC()
	for _, job := range jobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		staleFor := now.Sub(job.UpdatedAt)
		reason := fmt.Sprintf("stuck_sweeper: stale %s without progress; worker %s likely gone",
			staleFor.Round(time.Second), claimedByOrUnknown(job.ClaimedBy))

		requeued, err := s.queueRepo.RequeueStale(ctx, job.ID, reason)
		if err != nil {
			slog.Warn("enrichment: failed to requeue stuck job",
				"job", job.ID, "memory", job.MemoryID, "err", err)
			continue
		}
		if !requeued {
			// Race: row transitioned out of 'processing' between
			// ListStaleClaimed and RequeueStale (e.g. the original worker
			// resurrected and Complete'd it, or another sweeper instance got
			// here first). Nothing to do.
			continue
		}

		slog.Warn("enrichment: requeued stuck job",
			"job", job.ID,
			"memory", job.MemoryID,
			"namespace", job.NamespaceID,
			"stale_for", staleFor.Round(time.Second),
			"claimed_by", claimedByOrUnknown(job.ClaimedBy),
			"attempts", job.Attempts+1,
			"max_attempts", job.MaxAttempts)

		if s.eventBus != nil {
			events.Emit(ctx, s.eventBus, events.EnrichmentJobRequeued,
				"namespace:"+job.NamespaceID.String(),
				map[string]any{
					"job_id":       job.ID.String(),
					"memory_id":    job.MemoryID.String(),
					"namespace_id": job.NamespaceID.String(),
					"claimed_by":   claimedByOrUnknown(job.ClaimedBy),
					"stale_for_ms": staleFor.Milliseconds(),
					"attempts":     job.Attempts + 1,
					"max_attempts": job.MaxAttempts,
					"reason":       "stuck_sweeper",
				})
		}
	}

	return nil
}

func claimedByOrUnknown(claimedBy *string) string {
	if claimedBy == nil {
		return "unknown"
	}
	return *claimedBy
}
