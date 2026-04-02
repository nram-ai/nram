package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// IdleChecker reports whether the enrichment worker pool is idle.
type IdleChecker interface {
	IsIdle() bool
}

// Phase defines the interface for each dream processing phase.
type Phase interface {
	// Name returns the phase identifier (e.g. "entity_dedup").
	Name() string

	// Execute runs the phase logic. It should respect the token budget
	// and log all mutations via the DreamLogWriter.
	Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) error
}

// PhaseSummaryEntry captures per-phase statistics for the cycle record.
type PhaseSummaryEntry struct {
	Phase      string `json:"phase"`
	TokensUsed int    `json:"tokens_used"`
	Operations int    `json:"operations"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
	Skipped    bool   `json:"skipped,omitempty"`
}

// Runner orchestrates the fixed-order dream phase pipeline for a single cycle.
type Runner struct {
	cycleRepo  *storage.DreamCycleRepo
	logRepo    *storage.DreamLogRepo
	idleCheck  IdleChecker
	phases     []Phase
}

// NewRunner creates a new Runner with the given phases in execution order.
func NewRunner(
	cycleRepo *storage.DreamCycleRepo,
	logRepo *storage.DreamLogRepo,
	idleCheck IdleChecker,
	phases ...Phase,
) *Runner {
	return &Runner{
		cycleRepo: cycleRepo,
		logRepo:   logRepo,
		idleCheck: idleCheck,
		phases:    phases,
	}
}

// Execute runs the dream phase pipeline for the given cycle.
// It updates the cycle status throughout and returns any fatal error.
func (r *Runner) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget) error {
	// Mark cycle as running.
	if err := r.cycleRepo.Start(ctx, cycle.ID); err != nil {
		return fmt.Errorf("dream runner start cycle: %w", err)
	}

	logger := NewDreamLogWriter(r.logRepo, cycle.ID, cycle.ProjectID)
	summaries := make([]PhaseSummaryEntry, 0, len(r.phases))

	var lastErr error
	for _, phase := range r.phases {
		// Check if enrichment woke up between phases — yield if so.
		if r.idleCheck != nil && !r.idleCheck.IsIdle() {
			slog.Info("dreaming: enrichment active, aborting before phase",
				"phase", phase.Name(), "cycle", cycle.ID)
			break
		}

		if ctx.Err() != nil {
			break
		}

		if budget.Exhausted() {
			slog.Info("dreaming: budget exhausted, stopping pipeline",
				"cycle", cycle.ID, "used", budget.Used(), "total", budget.Total())
			summaries = append(summaries, PhaseSummaryEntry{
				Phase:   phase.Name(),
				Skipped: true,
			})
			continue
		}

		if err := r.cycleRepo.UpdateStatus(ctx, cycle.ID, model.DreamStatusRunning, phase.Name(), budget.Used()); err != nil {
			slog.Warn("dreaming: failed to update cycle status", "err", err)
		}

		slog.Info("dreaming: starting phase", "phase", phase.Name(), "cycle", cycle.ID,
			"budget_remaining", budget.Remaining())

		tokensBefore := budget.Used()
		logger.ResetOpCount()
		start := time.Now()

		err := phase.Execute(ctx, cycle, budget, logger)

		elapsed := time.Since(start)
		tokensConsumed := budget.Used() - tokensBefore

		entry := PhaseSummaryEntry{
			Phase:      phase.Name(),
			TokensUsed: tokensConsumed,
			Operations: logger.OpCount(),
			DurationMs: elapsed.Milliseconds(),
		}

		if err != nil {
			if errors.Is(err, ErrBudgetExhausted) {
				slog.Info("dreaming: budget exhausted during phase",
					"phase", phase.Name(), "cycle", cycle.ID)
				entry.Error = "budget exhausted"
				summaries = append(summaries, entry)
				break
			}

			slog.Error("dreaming: phase failed",
				"phase", phase.Name(), "cycle", cycle.ID, "err", err)
			entry.Error = err.Error()
			summaries = append(summaries, entry)
			lastErr = err
			break
		}

		summaries = append(summaries, entry)
		slog.Info("dreaming: phase completed", "phase", phase.Name(),
			"cycle", cycle.ID, "tokens", tokensConsumed, "duration_ms", elapsed.Milliseconds())
	}

	summaryJSON, err := json.Marshal(summaries)
	if err != nil {
		slog.Error("dreaming: failed to marshal phase summary", "err", err)
		summaryJSON = []byte(`[]`)
	}

	if lastErr != nil {
		if err := r.cycleRepo.Fail(ctx, cycle.ID, lastErr.Error()); err != nil {
			slog.Error("dreaming: failed to mark cycle as failed", "err", err)
		}
		return lastErr
	}

	if err := r.cycleRepo.Complete(ctx, cycle.ID, summaryJSON); err != nil {
		return fmt.Errorf("dream runner complete cycle: %w", err)
	}

	return nil
}
