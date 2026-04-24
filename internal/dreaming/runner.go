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
	//
	// The first return value indicates whether the phase left residual
	// work behind — for example, a bounded-batch phase that hit its
	// per-cycle cap with more candidates pending. The scheduler uses
	// this signal to decide whether to clear the project dirty flag:
	// "all phases returned nil" is not the same as "all work is done."
	Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error)
}

// PhaseSummaryEntry captures per-phase statistics for the cycle record.
type PhaseSummaryEntry struct {
	Phase          string `json:"phase"`
	TokensUsed     int    `json:"tokens_used"`
	Operations     int    `json:"operations"`
	DurationMs     int64  `json:"duration_ms"`
	Error          string `json:"error,omitempty"`
	Skipped        bool   `json:"skipped,omitempty"`
	HasResidual    bool   `json:"has_residual,omitempty"`
	ResidualReason string `json:"residual_reason,omitempty"`
}

// Runner orchestrates the fixed-order dream phase pipeline for a single cycle.
type Runner struct {
	cycleRepo *storage.DreamCycleRepo
	logRepo   *storage.DreamLogRepo
	idleCheck IdleChecker
	phases    []Phase
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
// Returns (allPhasesCompleted, hasResidual, error).
//
// allPhasesCompleted is true only when every phase in the pipeline ran to
// completion (no break on budget exhaustion, no error). hasResidual is true
// if any phase signaled residual work (a bounded-batch phase hit its cap
// with more candidates pending). A cycle can complete all phases yet still
// carry residual — the scheduler uses both signals to decide whether the
// project dirty flag is safe to clear.
func (r *Runner) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget) (bool, bool, error) {
	if err := r.cycleRepo.Start(ctx, cycle.ID); err != nil {
		return false, false, fmt.Errorf("dream runner start cycle: %w", err)
	}

	logger := NewDreamLogWriter(r.logRepo, cycle.ID, cycle.ProjectID)
	summaries := make([]PhaseSummaryEntry, 0, len(r.phases))
	completedPhases := 0
	hasResidual := false

	var lastErr error
	for _, phase := range r.phases {
		if r.idleCheck != nil && !r.idleCheck.IsIdle() {
			slog.Info("dreaming: enrichment active, aborting before phase",
				"phase", phase.Name(), "cycle", cycle.ID)
			break
		}

		if ctx.Err() != nil {
			break
		}

		if budget.Exhausted() {
			slog.Info("dreaming: phase skipped, budget exhausted",
				"phase", phase.Name(), "cycle", cycle.ID,
				"used", budget.Used(), "total", budget.Total())
			hasResidual = true
			summaries = append(summaries, PhaseSummaryEntry{
				Phase:          phase.Name(),
				Skipped:        true,
				HasResidual:    true,
				ResidualReason: "budget_exhausted_before_phase",
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

		phaseResidual, err := phase.Execute(ctx, cycle, budget, logger)

		elapsed := time.Since(start)
		tokensConsumed := budget.Used() - tokensBefore

		entry := PhaseSummaryEntry{
			Phase:       phase.Name(),
			TokensUsed:  tokensConsumed,
			Operations:  logger.OpCount(),
			DurationMs:  elapsed.Milliseconds(),
			HasResidual: phaseResidual,
		}
		if phaseResidual {
			entry.ResidualReason = "phase_reported_residual"
			hasResidual = true
		}

		if err != nil {
			if errors.Is(err, ErrBudgetExhausted) {
				slog.Info("dreaming: budget exhausted during phase",
					"phase", phase.Name(), "cycle", cycle.ID)
				entry.Error = "budget exhausted"
				entry.HasResidual = true
				entry.ResidualReason = "budget_exhausted_during_phase"
				hasResidual = true
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

		completedPhases++
		summaries = append(summaries, entry)
		slog.Info("dreaming: phase completed", "phase", phase.Name(),
			"cycle", cycle.ID, "tokens", tokensConsumed, "duration_ms", elapsed.Milliseconds(),
			"has_residual", phaseResidual)
	}

	summaryJSON, err := json.Marshal(summaries)
	if err != nil {
		slog.Error("dreaming: failed to marshal phase summary", "err", err)
		summaryJSON = []byte(`[]`)
	}

	allCompleted := completedPhases == len(r.phases)

	if lastErr != nil {
		if err := r.cycleRepo.Fail(ctx, cycle.ID, lastErr.Error(), budget.Used()); err != nil {
			slog.Error("dreaming: failed to mark cycle as failed", "err", err)
		}
		return false, hasResidual, lastErr
	}

	if err := r.cycleRepo.Complete(ctx, cycle.ID, summaryJSON, budget.Used()); err != nil {
		return allCompleted, hasResidual, fmt.Errorf("dream runner complete cycle: %w", err)
	}

	return allCompleted, hasResidual, nil
}
