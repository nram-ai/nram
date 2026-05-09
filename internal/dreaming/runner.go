package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// phaseFractionKeys maps a phase name to its budget-fraction setting key.
// Phases not in the map (or whose key resolves to <= 0) get the root budget
// passed through unchanged. SQL-only phases default to 0.0 so they share the
// root and run whenever the cycle has remaining tokens; LLM-spending phases
// default to a positive fraction so the runner carves them a SubSlice that
// caps how much of the cycle envelope they can consume.
var phaseFractionKeys = map[string]string{
	model.DreamPhaseEntityDedup:       service.SettingDreamEntityDedupFraction,
	model.DreamPhaseEmbeddingBackfill: service.SettingDreamEmbeddingBackfillFraction,
	model.DreamPhaseParaphraseDedup:   service.SettingDreamParaphraseFraction,
	model.DreamPhaseTransitive:        service.SettingDreamTransitiveFraction,
	model.DreamPhaseContradictions:    service.SettingDreamContradictionFraction,
	model.DreamPhaseConsolidation:     service.SettingDreamConsolidationFraction,
	model.DreamPhasePruning:           service.SettingDreamPruningFraction,
	model.DreamPhaseWeightAdjust:      service.SettingDreamWeightAdjustFraction,
}

// Heartbeat tick timeout is resolved per-cycle from
// service.SettingDreamHeartbeatTickTimeoutSeconds. Losing the heartbeat is
// the failure mode that makes long phases look frozen to the
// StuckCycleSweeper, so a stuck SQLite writer or transient Postgres blip
// must deadline rather than stall the loop.

// IdleChecker reports whether the enrichment worker pool is idle.
type IdleChecker interface {
	IsIdle() bool
}

// Stable codes emitted as PhaseSummaryEntry.ResidualReason. The first four are
// owned by the runner and override any phase-supplied code on
// ErrBudgetExhausted; the rest are owned by individual phases and name the
// specific in-phase limit that fired. Keep RESIDUAL_REASON_LABELS in
// ui/src/pages/DreamingMonitor.tsx in sync with this set.
const (
	ResidualReasonBudgetExhaustedBeforePhase  = "budget_exhausted_before_phase"
	ResidualReasonPhaseSliceZero              = "phase_slice_zero"
	ResidualReasonBudgetExhaustedDuringPhase  = "budget_exhausted_during_phase"
	ResidualReasonPhaseSliceExhausted         = "phase_slice_exhausted"
	ResidualReasonMoreCandidatesThanBatch     = "more_candidates_than_batch"
	ResidualReasonParaphraseUnvisited         = "paraphrase_unvisited_candidates"
	ResidualReasonTransitivePerCycleCap       = "transitive_per_cycle_cap"
	ResidualReasonDispatchCapReached          = "dispatch_cap_reached"
	ResidualReasonPhaseBudgetStopped          = "phase_budget_stopped"
	ResidualReasonAuditStaleRemaining         = "audit_stale_remaining"
	ResidualReasonReinforceCapHit             = "reinforce_cap_hit"
	ResidualReasonConsolidateClustersRemaining = "consolidate_clusters_remaining"
	ResidualReasonStaleFetchCap               = "stale_fetch_cap"
)

// PhaseResult captures the outcome of a single phase. ResidualDetail is
// optional structured info (cap value, counts) attached to phase-supplied
// reasons; the runner overrides ResidualReason on ErrBudgetExhausted.
// SubPhases is optional: phases that further slice their budget internally
// (currently only consolidation) may report a per-sub-phase rollup so the
// admin UI can render a nested breakdown without a second fetch.
type PhaseResult struct {
	HasResidual    bool
	ResidualReason string
	ResidualDetail map[string]any
	SubPhases      []SubPhaseSummary
}

// Phase defines the interface for each dream processing phase.
type Phase interface {
	Name() string
	Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error)
}

// PhaseSummaryEntry captures per-phase statistics for the cycle record.
// SliceCap is zero/omitted for SQL-only phases (frac=0) that share the root.
type PhaseSummaryEntry struct {
	Phase          string            `json:"phase"`
	TokensUsed     int               `json:"tokens_used"`
	Operations     int               `json:"operations"`
	DurationMs     int64             `json:"duration_ms"`
	SliceCap       int               `json:"slice_cap,omitempty"`
	Error          string            `json:"error,omitempty"`
	Skipped        bool              `json:"skipped,omitempty"`
	HasResidual    bool              `json:"has_residual,omitempty"`
	ResidualReason string            `json:"residual_reason,omitempty"`
	ResidualDetail map[string]any    `json:"residual_detail,omitempty"`
	SubPhases      []SubPhaseSummary `json:"sub_phases,omitempty"`
}

// SubPhaseSummary captures per-sub-phase statistics for phases that further
// slice their budget internally. Currently emitted only by consolidation,
// which splits its slice across backfill_audit / reinforce / consolidate.
type SubPhaseSummary struct {
	Name        string `json:"name"`
	TokensUsed  int    `json:"tokens_used"`
	SliceCap    int    `json:"slice_cap,omitempty"`
	HasResidual bool   `json:"has_residual,omitempty"`
}

// cycleProgressRepo is the subset of *storage.DreamCycleRepo that Runner
// depends on, narrowed to an interface so the runner (and its heartbeat
// goroutine in particular) can be unit-tested against a fake without
// spinning up a database. *storage.DreamCycleRepo satisfies it implicitly.
type cycleProgressRepo interface {
	Start(ctx context.Context, id uuid.UUID) error
	UpdateStatus(ctx context.Context, id uuid.UUID, status, phase string) error
	UpdatePhaseSummary(ctx context.Context, id uuid.UUID, summary json.RawMessage) error
	TickProgress(ctx context.Context, id uuid.UUID) (int, error)
	Complete(ctx context.Context, id uuid.UUID, summary json.RawMessage) error
	Fail(ctx context.Context, id uuid.UUID, errMsg string) error
}

// Runner orchestrates the fixed-order dream phase pipeline for a single cycle.
type Runner struct {
	cycleRepo         cycleProgressRepo
	logRepo           *storage.DreamLogRepo
	idleCheck         IdleChecker
	heartbeatInterval time.Duration
	bus               events.EventBus
	settings          SettingsResolver
	phases            []Phase
}

// NewRunner creates a new Runner with the given phases in execution order.
// heartbeatInterval controls how often the runner stamps heartbeat_at on the
// cycle row while a phase is executing; zero falls back to 30 seconds. The
// admin UI uses heartbeat_at to surface "no recent activity" with a tighter
// window than phase-boundary updated_at can give.
//
// bus is the event bus used to publish per-phase, per-LLM-call, and
// heartbeat events for live UI updates. May be nil — phases bind a
// nil-bus tracker to ctx in that case, so emits become no-ops and the
// rest of the pipeline is unaffected. Test fixtures rely on this.
//
// settings resolves per-phase budget fractions on each cycle. May be nil; in
// that case every phase receives the root budget unchanged (preserves
// pre-feature behaviour for tests that construct a Runner directly).
func NewRunner(
	cycleRepo *storage.DreamCycleRepo,
	logRepo *storage.DreamLogRepo,
	idleCheck IdleChecker,
	heartbeatInterval time.Duration,
	bus events.EventBus,
	settings SettingsResolver,
	phases ...Phase,
) *Runner {
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}
	return &Runner{
		cycleRepo:         cycleRepo,
		logRepo:           logRepo,
		idleCheck:         idleCheck,
		heartbeatInterval: heartbeatInterval,
		bus:               bus,
		settings:          settings,
		phases:            phases,
	}
}

// phaseFraction resolves the configured budget-fraction for a phase, clamped
// to [0,1]. Returns 0.0 (meaning "no per-phase slice") when the resolver is
// nil, the phase has no fraction key registered, or the value is out of range.
//
// Distinct from phase_consolidation.go's resolveFraction(): that helper
// clamps to (0,1] and returns a fallback default on out-of-range input
// because its sub-phase fractions must be positive. The runner instead
// treats 0.0 as a first-class "no slice, share root" signal for SQL-only
// phases, so the two cannot be unified without regressing one caller.
// resolveHeartbeatTickTimeout reads the per-cycle heartbeat tick timeout.
// Falls back to the registered default when settings is nil (test path).
func (r *Runner) resolveHeartbeatTickTimeout(ctx context.Context) time.Duration {
	if r.settings == nil {
		return time.Duration(service.GetDefaultInt(service.SettingDreamHeartbeatTickTimeoutSeconds)) * time.Second
	}
	return r.settings.ResolveDurationSecondsWithDefault(ctx, service.SettingDreamHeartbeatTickTimeoutSeconds, "global")
}

func (r *Runner) phaseFraction(ctx context.Context, phaseName string) float64 {
	if r.settings == nil {
		return 0
	}
	key, ok := phaseFractionKeys[phaseName]
	if !ok {
		return 0
	}
	v, err := r.settings.ResolveFloat(ctx, key, "global")
	if err != nil || v < 0 || v > 1 {
		return 0
	}
	return v
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

	// Tracker carries the cycle's event-emission state (in-flight LLM call,
	// current phase). Bound to the ctx passed to phases so they can wrap
	// LLM calls with WrapLLMCall without an interface change.
	tracker := NewCycleTracker(r.bus, cycle.ID, cycle.ProjectID)
	phaseCtx := WithCycleTracker(ctx, tracker)
	// cycle_id stamping drives token_usage attribution; TickProgress sums by it.
	phaseCtx = provider.WithCycleID(phaseCtx, cycle.ID)

	// Heartbeat goroutine. Stamps heartbeat_at every heartbeatInterval and
	// emits dream.cycle.heartbeat with tokens_used + in-flight call. The
	// repo's WHERE status='running' guard makes a late tick against a
	// terminal row a no-op, so a race against cycle exit is harmless.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go r.heartbeat(hbCtx, cycle.ID, tracker, budget)

	logger := NewDreamLogWriter(r.logRepo, cycle.ID, cycle.ProjectID)
	summaries := make([]PhaseSummaryEntry, 0, len(r.phases))
	completedPhases := 0
	hasResidual := false

	// Resolve every phase's fraction once at cycle start. The proportional-
	// of-remaining math below sums fractions of later phases on each
	// iteration, so caching avoids O(N²) settings lookups.
	phaseFracs := make([]float64, len(r.phases))
	for i, phase := range r.phases {
		phaseFracs[i] = r.phaseFraction(phaseCtx, phase.Name())
	}

	var lastErr error
	for i, phase := range r.phases {
		if r.idleCheck != nil && !r.idleCheck.IsIdle() {
			slog.Info("dreaming: enrichment active, aborting before phase",
				"phase", phase.Name(), "cycle", cycle.ID)
			break
		}

		if ctx.Err() != nil {
			break
		}

		// frac==0 (SQL-only phases) passes the root budget through; frac>0
		// carves a proportional-of-remaining SubSlice so under-spend from
		// earlier LLM phases flows into later ones via Remaining().
		frac := phaseFracs[i]
		phaseBudget := budget
		sliceCap := 0
		if frac > 0 {
			sumRemaining := frac
			for j := i + 1; j < len(r.phases); j++ {
				if phaseFracs[j] > 0 {
					sumRemaining += phaseFracs[j]
				}
			}
			sliceCap = budget.ProportionalSliceCap(frac, sumRemaining)
			phaseBudget = budget.SubSlice(sliceCap)
		}

		if phaseBudget.Exhausted() {
			// Exhausted at this level means either:
			//   - root drained (frac==0 path, or frac>0 with parent empty), or
			//   - sliceCap==0 (degenerate frac that rounded to zero).
			// Both are operationally "no budget for this phase"; distinguish
			// the slice-zero case in the residual reason for ops visibility.
			reason := ResidualReasonBudgetExhaustedBeforePhase
			if frac > 0 && sliceCap == 0 {
				reason = ResidualReasonPhaseSliceZero
			}
			slog.Info("dreaming: phase skipped, budget exhausted",
				"phase", phase.Name(), "cycle", cycle.ID,
				"used", budget.Used(), "total", budget.Total(),
				"slice_cap", sliceCap, "reason", reason)
			hasResidual = true
			summaries = append(summaries, PhaseSummaryEntry{
				Phase:          phase.Name(),
				SliceCap:       sliceCap,
				Skipped:        true,
				HasResidual:    true,
				ResidualReason: reason,
			})
			r.persistPartialSummary(ctx, cycle.ID, summaries)
			continue
		}

		if err := r.cycleRepo.UpdateStatus(ctx, cycle.ID, model.DreamStatusRunning, phase.Name()); err != nil {
			slog.Warn("dreaming: failed to update cycle status", "err", err)
		}

		slog.Info("dreaming: starting phase", "phase", phase.Name(), "cycle", cycle.ID,
			"phase_index", i+1, "of", len(r.phases),
			"slice_cap", sliceCap,
			"slice_remaining", phaseBudget.Remaining(),
			"root_remaining", budget.Remaining())

		tracker.SetPhase(phase.Name())
		tracker.EmitPhaseStarted(ctx, phase.Name(), budget.Used())

		tokensBefore := budget.Used()
		logger.ResetOpCount()
		start := time.Now()

		result, err := phase.Execute(phaseCtx, cycle, phaseBudget, logger)

		elapsed := time.Since(start)
		tokensConsumed := budget.Used() - tokensBefore

		entry := PhaseSummaryEntry{
			Phase:          phase.Name(),
			TokensUsed:     tokensConsumed,
			Operations:     logger.OpCount(),
			DurationMs:     elapsed.Milliseconds(),
			SliceCap:       sliceCap,
			HasResidual:    result.HasResidual,
			ResidualReason: result.ResidualReason,
			ResidualDetail: result.ResidualDetail,
			SubPhases:      result.SubPhases,
		}
		if result.HasResidual {
			hasResidual = true
		}

		if err != nil {
			if errors.Is(err, ErrBudgetExhausted) {
				// A phase reporting ErrBudgetExhausted means its budget ran
				// out — but with per-phase slicing that may be the slice's
				// local cap, not the root cap. Only break the cycle when the
				// root is genuinely drained; otherwise let the next phase
				// claim its own slice and proceed.
				rootExhausted := budget.Exhausted()
				slog.Info("dreaming: budget exhausted during phase",
					"phase", phase.Name(), "cycle", cycle.ID,
					"root_exhausted", rootExhausted,
					"root_used", budget.Used(), "root_total", budget.Total())
				entry.Error = "budget exhausted"
				entry.HasResidual = true
				if rootExhausted {
					entry.ResidualReason = ResidualReasonBudgetExhaustedDuringPhase
				} else {
					entry.ResidualReason = ResidualReasonPhaseSliceExhausted
				}
				hasResidual = true
				summaries = append(summaries, entry)
				r.persistPartialSummary(ctx, cycle.ID, summaries)
				tracker.EmitPhaseCompleted(ctx, phase.Name(), tokensConsumed,
					logger.OpCount(), elapsed.Milliseconds(), true, entry.Error)
				if rootExhausted {
					break
				}
				continue
			}

			slog.Error("dreaming: phase failed",
				"phase", phase.Name(), "cycle", cycle.ID, "err", err)
			entry.Error = err.Error()
			summaries = append(summaries, entry)
			r.persistPartialSummary(ctx, cycle.ID, summaries)
			tracker.EmitPhaseCompleted(ctx, phase.Name(), tokensConsumed,
				logger.OpCount(), elapsed.Milliseconds(), result.HasResidual, entry.Error)
			lastErr = err
			break
		}

		completedPhases++
		summaries = append(summaries, entry)
		r.persistPartialSummary(ctx, cycle.ID, summaries)
		slog.Info("dreaming: phase completed", "phase", phase.Name(),
			"cycle", cycle.ID, "phase_index", i+1, "of", len(r.phases),
			"tokens", tokensConsumed, "duration_ms", elapsed.Milliseconds(),
			"has_residual", result.HasResidual, "residual_reason", result.ResidualReason)
		tracker.EmitPhaseCompleted(ctx, phase.Name(), tokensConsumed,
			logger.OpCount(), elapsed.Milliseconds(), result.HasResidual, "")
	}

	summaryJSON, err := json.Marshal(summaries)
	if err != nil {
		slog.Error("dreaming: failed to marshal phase summary", "err", err)
		summaryJSON = []byte(`[]`)
	}

	allCompleted := completedPhases == len(r.phases)

	if lastErr != nil {
		if err := r.cycleRepo.Fail(ctx, cycle.ID, lastErr.Error()); err != nil {
			slog.Error("dreaming: failed to mark cycle as failed", "err", err)
		}
		return false, hasResidual, lastErr
	}

	if err := r.cycleRepo.Complete(ctx, cycle.ID, summaryJSON); err != nil {
		return allCompleted, hasResidual, fmt.Errorf("dream runner complete cycle: %w", err)
	}

	return allCompleted, hasResidual, nil
}

// persistPartialSummary best-effort writes the running slice between phases so
// the UI sees breakdowns mid-cycle. Marshal/write errors are swallowed —
// stalling a cycle on a transient DB blip is worse than missing one tick.
func (r *Runner) persistPartialSummary(ctx context.Context, cycleID uuid.UUID, summaries []PhaseSummaryEntry) {
	partial, err := json.Marshal(summaries)
	if err != nil {
		slog.Warn("dreaming: failed to marshal partial phase summary",
			"cycle", cycleID, "err", err)
		return
	}
	if err := r.cycleRepo.UpdatePhaseSummary(ctx, cycleID, partial); err != nil {
		slog.Warn("dreaming: failed to persist partial phase summary",
			"cycle", cycleID, "err", err)
	}
}

// heartbeat ticks dream_cycles.{heartbeat_at, updated_at, tokens_used} every
// heartbeatInterval and publishes dream.cycle.heartbeat. The loop must
// survive a single bad tick (panic, DB lock, slow SUM) — losing it is what
// makes long phases look stalled to the sweeper. Defenses:
//
//  1. defer recover() so a panicked tick doesn't kill the goroutine.
//  2. Per-tick deadline so a stuck writer doesn't stall the loop.
//  3. Fallback EmitHeartbeat(budget.Used()) so SSE listeners stay alive
//     when TickProgress transiently errors.
func (r *Runner) heartbeat(ctx context.Context, cycleID uuid.UUID, tracker *CycleTracker, budget *TokenBudget) {
	tickTimeout := r.resolveHeartbeatTickTimeout(ctx)
	tick := func() {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("dreaming: heartbeat tick panic recovered",
					"cycle", cycleID, "panic", rec, "stack", string(debug.Stack()))
			}
		}()

		tickCtx, cancel := context.WithTimeout(ctx, tickTimeout)
		defer cancel()

		used, err := r.cycleRepo.TickProgress(tickCtx, cycleID)
		if err != nil && ctx.Err() == nil {
			slog.Warn("dreaming: tick failed", "cycle", cycleID, "err", err)
			// budget.Used() is a strict lower bound on what landed in
			// token_usage; safe to surface to SSE while the DB recovers.
			used = budget.Used()
		}

		tracker.EmitHeartbeat(ctx, used)
	}

	// Initial tick on entry so the row carries a fresh heartbeat_at without
	// waiting for the first interval to elapse.
	tick()

	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}
