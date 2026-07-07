package logging

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nram-ai/nram/internal/periodic"
)

// LogPruner is the storage capability the retention sweeper needs.
// *storage.LogEntryRepo satisfies it.
type LogPruner interface {
	DeleteOlderThan(ctx context.Context, before time.Time) (int64, error)
	DeleteBeyondCount(ctx context.Context, keep int) (int64, error)
}

// RetentionLimits is the rolling-window policy: MaxRows is the count cap (the
// hard ceiling, 0 to disable), MaxAgeDays is the age cap (0 to disable).
type RetentionLimits struct {
	MaxRows    int
	MaxAgeDays int
}

// RetentionSweeper prunes the log_entries table to the configured rolling
// window. The age limit runs first, then the count cap, matching the plan's
// "count-primary plus optional age" semantics: the count cap is the guaranteed
// ceiling regardless of age.
type RetentionSweeper struct {
	repo   LogPruner
	limits func(ctx context.Context) RetentionLimits
}

// NewRetentionSweeper builds a sweeper. limits is resolved on each sweep so
// operator changes to the retention settings apply without a restart.
func NewRetentionSweeper(repo LogPruner, limits func(ctx context.Context) RetentionLimits) *RetentionSweeper {
	return &RetentionSweeper{repo: repo, limits: limits}
}

// Sweep applies the age limit then the count cap once.
func (s *RetentionSweeper) Sweep(ctx context.Context) error {
	lim := s.limits(ctx)
	if lim.MaxAgeDays > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -lim.MaxAgeDays)
		if _, err := s.repo.DeleteOlderThan(ctx, cutoff); err != nil {
			return fmt.Errorf("log retention age sweep: %w", err)
		}
	}
	if lim.MaxRows > 0 {
		if _, err := s.repo.DeleteBeyondCount(ctx, lim.MaxRows); err != nil {
			return fmt.Errorf("log retention count sweep: %w", err)
		}
	}
	return nil
}

// Run sweeps once immediately and then on the given interval until ctx is done.
// Sweep errors are reported to stderr (never through slog, to avoid feeding the
// log table from its own janitor). The interval is fixed for the life of the
// call, so periodic.Run's per-tick re-resolution just returns the same value.
func (s *RetentionSweeper) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Hour
	}
	periodic.Run(ctx, periodic.Fixed(interval),
		func(ctx context.Context, _ bool) {
			if err := s.Sweep(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "logging: retention sweep failed: %v\n", err)
			}
		})
}
