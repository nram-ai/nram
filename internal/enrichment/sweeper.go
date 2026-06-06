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
	ListStaleClaimed(ctx context.Context, updatedThreshold, claimedAtMaxAge time.Duration, limit int) ([]*model.EnrichmentJob, error)
	RequeueStale(ctx context.Context, id uuid.UUID, reason string) (bool, error)
	DeleteFailedBefore(ctx context.Context, cutoff time.Time, limit int) (int, error)
}

// sweeperSettingsResolver is the slice of *service.SettingsService the
// sweeper actually touches. Mirrors dreaming/interfaces.go's SettingsResolver
// shape so test fakes can be small.
type sweeperSettingsResolver interface {
	ResolveDurationSecondsWithDefault(ctx context.Context, key, scope string) time.Duration
	ResolveIntWithDefault(ctx context.Context, key, scope string) int
}

// StuckJobSweeper periodically scans enrichment_queue for rows in
// status='processing' that match either of two stale-claim signals and
// auto-requeues them via Retry semantics (attempts++ so a genuine
// poison-pill memory still hits max_attempts and stops looping):
//
//   - updated_at staleness past enrichment.stuck_threshold_seconds: the
//     usual signal; a live worker advances updated_at every
//     enrichment.worker.heartbeat_seconds so a slow LLM call is not
//     mistaken for a dead worker.
//   - claimed_at age past enrichment.claim_max_age_seconds: the backstop;
//     fires regardless of updated_at to recover claims that have outlived
//     every plausible batch runtime (a wedged provider call still ticking
//     heartbeats, a sibling instance refreshing under a colliding
//     claimed_by in pre-fix deployments, etc.).
//
// The updated_at threshold must exceed the longest legitimate batch runtime;
// the claim_max_age cap must exceed it by a comfortable margin since it is
// the hard wall when heartbeat-based detection fails.
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

// Sweep finds enrichment_queue rows in status='processing' that match
// either the updated_at staleness threshold or the claimed_at hard cap and
// resets each back to pending via RequeueStale. Idempotent under
// multi-instance sweep: RequeueStale's `WHERE status='processing'` guard
// ensures a row already requeued by another instance returns (false, nil)
// and is skipped here.
//
// Failures on individual rows are logged and the loop continues; one bad
// row should not block recovery of the rest of the batch. A failure to even
// list the stale rows is returned as an error so the run loop can log it.
//
// Each sweep also prunes permanently-failed jobs past their retention window
// (pruneFailedRetention), so the failed backlog cannot grow without bound.
// That pass runs first and independently of the stale-claim recovery so an
// empty stale set (the common case) does not skip it.
func (s *StuckJobSweeper) Sweep(ctx context.Context) error {
	s.pruneFailedRetention(ctx)

	threshold := s.settings.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentStuckThreshold, "global")
	if threshold < time.Second {
		threshold = 30 * time.Minute
	}

	// claimMaxAge is the backstop signal. An operator may explicitly set it
	// to 0 (or below) to disable the cap and rely solely on updated_at
	// staleness; ListStaleClaimed's predicate has `(? > 0 AND ...)` gating
	// for exactly this case. Only normalize negatives to 0; leave zero
	// alone so it reaches the predicate as "disabled".
	claimMaxAge := max(s.settings.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentClaimMaxAge, "global"), 0)

	scanLimit := s.settings.ResolveIntWithDefault(ctx, service.SettingEnrichmentStuckScanLimit, "global")
	jobs, err := s.queueRepo.ListStaleClaimed(ctx, threshold, claimMaxAge, scanLimit)
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
		claimAge := time.Duration(0)
		if job.ClaimedAt != nil {
			claimAge = now.Sub(*job.ClaimedAt)
		}

		// Distinguish which signal fired so operators reading
		// last_requeue_reason can tell a "heartbeat stopped" recovery
		// (the common case) from a "claim outlived the cap" recovery
		// (the backstop, indicates a wedged worker whose heartbeat is
		// still firing).
		var (
			reason       string
			reasonCode   string
			signalDetail string
		)
		switch {
		case claimMaxAge > 0 && claimAge >= claimMaxAge:
			reasonCode = "stuck_sweeper_claim_age"
			signalDetail = fmt.Sprintf("claim age %s exceeded backstop cap %s",
				claimAge.Round(time.Second), claimMaxAge.Round(time.Second))
			reason = fmt.Sprintf("stuck_sweeper: %s; worker %s likely wedged",
				signalDetail, claimedByOrUnknown(job.ClaimedBy))
		default:
			reasonCode = "stuck_sweeper"
			signalDetail = fmt.Sprintf("stale %s without progress", staleFor.Round(time.Second))
			reason = fmt.Sprintf("stuck_sweeper: %s; worker %s likely gone",
				signalDetail, claimedByOrUnknown(job.ClaimedBy))
		}

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
			"claim_age", claimAge.Round(time.Second),
			"reason_code", reasonCode,
			"claimed_by", claimedByOrUnknown(job.ClaimedBy),
			"attempts", job.Attempts+1,
			"max_attempts", job.MaxAttempts)

		if s.eventBus != nil {
			events.Emit(ctx, s.eventBus, events.EnrichmentJobRequeued,
				"namespace:"+job.NamespaceID.String(),
				map[string]any{
					"job_id":        job.ID.String(),
					"memory_id":     job.MemoryID.String(),
					"namespace_id":  job.NamespaceID.String(),
					"claimed_by":    claimedByOrUnknown(job.ClaimedBy),
					"stale_for_ms":  staleFor.Milliseconds(),
					"claim_age_ms":  claimAge.Milliseconds(),
					"attempts":      job.Attempts + 1,
					"max_attempts":  job.MaxAttempts,
					"reason":        reasonCode,
					"reason_detail": signalDetail,
				})
		}
	}

	return nil
}

// pruneFailedRetention hard-deletes failed enrichment jobs whose updated_at is
// older than enrichment.failed_retention_days, bounded to stuck_scan_limit rows
// per sweep so a large backlog drains over several ticks rather than locking
// the writer in one statement. A retention of 0 (or below) disables pruning.
// Errors are logged, not returned: retention is best-effort and must not block
// the stale-claim recovery that follows it in Sweep.
func (s *StuckJobSweeper) pruneFailedRetention(ctx context.Context) {
	days := s.settings.ResolveIntWithDefault(ctx,
		service.SettingEnrichmentFailedRetentionDays, "global")
	if days <= 0 {
		return
	}
	limit := s.settings.ResolveIntWithDefault(ctx,
		service.SettingEnrichmentStuckScanLimit, "global")
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	deleted, err := s.queueRepo.DeleteFailedBefore(ctx, cutoff, limit)
	if err != nil {
		slog.Warn("enrichment: failed-job retention prune failed", "err", err)
		return
	}
	if deleted > 0 {
		slog.Info("enrichment: pruned failed jobs past retention",
			"count", deleted, "retention_days", days)
	}
}

func claimedByOrUnknown(claimedBy *string) string {
	if claimedBy == nil {
		return "unknown"
	}
	return *claimedBy
}
