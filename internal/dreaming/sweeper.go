package dreaming

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/periodic"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// CycleCanceller is the subset of Scheduler the sweeper needs to interrupt
// in-flight cycles owned by THIS instance. Defined here (consumer-side) so
// tests can substitute a fake without dragging the full Scheduler in.
type CycleCanceller interface {
	CancelCycle(id uuid.UUID) bool
}

// stuckCycleStore is the storage subset the sweeper relies on. Defined here
// so tests can substitute a fake repo without standing up a full database.
type stuckCycleStore interface {
	ListStale(ctx context.Context, threshold time.Duration, limit int) ([]model.DreamCycle, error)
	Abandon(ctx context.Context, id uuid.UUID, reason string) (bool, error)
}

// StuckCycleSweeper periodically scans for running cycles whose updated_at
// has gone past dreaming.stuck_threshold_seconds and abandons them. Worker
// crashes (deploy mid-cycle, OOM, SIGKILL) leave dream_cycles rows in
// status='running' with no one to finalize them; without this sweep, the
// monitor stat counts are wrong forever and rollback is unavailable.
//
// The threshold is intentionally conservative; abandoning earlier could
// discard a cycle that's still making real progress on a long single phase.
// The admin UI uses heartbeat_at (a tighter signal) for diagnostic display
// only; the abandon trigger here uses updated_at so the sweep stays safe
// even if heartbeat_at drifts.
type StuckCycleSweeper struct {
	cycleRepo stuckCycleStore
	canceller CycleCanceller
	settings  SettingsResolver
	eventBus  events.EventBus

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewStuckCycleSweeper constructs a sweeper. canceller may be nil during
// tests that don't exercise local cancellation; cross-instance abandon
// works without it via the DB write.
func NewStuckCycleSweeper(
	cycleRepo *storage.DreamCycleRepo,
	canceller CycleCanceller,
	settings SettingsResolver,
	eventBus events.EventBus,
) *StuckCycleSweeper {
	return &StuckCycleSweeper{
		cycleRepo: cycleRepo,
		canceller: canceller,
		settings:  settings,
		eventBus:  eventBus,
	}
}

// Start launches the sweeper loop in a background goroutine. Independent of
// the dream Scheduler's lifecycle so a long-running cycle on this instance
// (which blocks the scheduler's main loop) can't also block the sweeper
// that's supposed to detect and recover from it. A sweep runs immediately at
// startup so a restart abandons already-stuck cycles without waiting a full
// interval; the sweep interval is re-read every tick, so it can be changed
// without a restart.
func (s *StuckCycleSweeper) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.run(ctx)
}

// Stop signals the sweeper goroutine to exit and waits for it to finish.
func (s *StuckCycleSweeper) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// sweepInterval resolves dreaming.stuck_sweep_seconds live, clamping a
// non-positive (or unset) value to the 300s default. Read every tick so an
// interval change hot-reloads without a restart.
func (s *StuckCycleSweeper) sweepInterval(ctx context.Context) time.Duration {
	intervalSecs, _ := s.settings.ResolveInt(ctx, service.SettingDreamStuckSweep, "global")
	if intervalSecs <= 0 {
		intervalSecs = 300
	}
	return time.Duration(intervalSecs) * time.Second
}

func (s *StuckCycleSweeper) run(ctx context.Context) {
	defer s.wg.Done()

	// periodic.Run sweeps once at startup (so a restart abandons already-stuck
	// cycles without waiting a full interval) then re-resolves sweepInterval
	// each tick (so dreaming.stuck_sweep_seconds hot-reloads).
	periodic.Run(ctx, s.sweepInterval, func(ctx context.Context, startup bool) {
		if err := s.Sweep(ctx); err != nil {
			if startup {
				slog.Warn("dreaming: startup stuck-cycle sweep failed", "err", err)
			} else {
				slog.Warn("dreaming: stuck-cycle sweep failed", "err", err)
			}
		}
	})
}

// Sweep finds cycles whose status='running' and updated_at is older than the
// configured stuck threshold, cancels them locally if owned, then writes the
// DB row to failed. Idempotent: a cycle that's already terminal is skipped
// by the repo's WHERE guard.
func (s *StuckCycleSweeper) Sweep(ctx context.Context) error {
	thresholdSecs, _ := s.settings.ResolveInt(ctx, service.SettingDreamStuckThreshold, "global")
	if thresholdSecs <= 0 {
		thresholdSecs = 1800
	}
	threshold := time.Duration(thresholdSecs) * time.Second

	scanLimit := s.settings.ResolveIntWithDefault(ctx, service.SettingDreamStuckScanLimit, "global")
	cycles, err := s.cycleRepo.ListStale(ctx, threshold, scanLimit)
	if err != nil {
		return fmt.Errorf("list stale cycles: %w", err)
	}

	now := time.Now().UTC()
	for _, c := range cycles {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		staleFor := now.Sub(c.UpdatedAt)
		reason := fmt.Sprintf("stuck for %s without progress; worker likely gone", staleFor.Round(time.Second))

		// Same-instance fast path: cancel the in-flight ctx so the runner
		// exits at the next ctx-aware checkpoint instead of finishing the
		// current phase. Best-effort: false return means a different
		// instance owns the cycle, in which case the DB write below is
		// the only signal that propagates.
		if s.canceller != nil {
			s.canceller.CancelCycle(c.ID)
		}

		abandoned, err := s.cycleRepo.Abandon(ctx, c.ID, reason)
		if err != nil {
			slog.Warn("dreaming: failed to abandon stuck cycle",
				"cycle", c.ID, "project", c.ProjectID, "err", err)
			continue
		}
		if !abandoned {
			// Race: cycle transitioned to a terminal state between ListStale
			// and Abandon (e.g. the runner finished naturally). Nothing to do.
			continue
		}

		slog.Warn("dreaming: abandoned stuck cycle",
			"cycle", c.ID, "project", c.ProjectID,
			"stale_for", staleFor.Round(time.Second),
			"phase", c.Phase)

		events.Emit(ctx, s.eventBus, events.DreamCycleFailed,
			"project:"+c.ProjectID.String(),
			map[string]string{
				"cycle_id":   c.ID.String(),
				"project_id": c.ProjectID.String(),
				"error":      reason,
				"reason":     "stuck_sweeper",
			})
	}

	return nil
}
