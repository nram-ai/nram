package dreaming

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// MentionRecomputePhase re-normalizes every entity's mention_count in the
// cycle's project to the canonical value: the number of distinct live
// (non-deleted, non-superseded) source memories on edges touching the entity.
//
// It runs last, after pruning has expired edges and weight_adjustment has
// bumped counts, so it reflects the final edge set and can correct a count
// DOWNWARD, which the weights phase's monotonic +1 bump cannot. The reap paths
// (forget/supersede/lifecycle sweep) already keep counts consistent on delete;
// this phase heals the residual drift (an edge expired by pruning, a
// weights-phase over-count) reliably every cycle rather than incidentally, and
// scoped to the one project's namespace so it never pays the whole-table cost of
// the operator RepairGraph. Pure SQL, zero tokens.
type MentionRecomputePhase struct {
	entities MentionCountRecomputer
}

// NewMentionRecomputePhase constructs the phase.
func NewMentionRecomputePhase(entities MentionCountRecomputer) *MentionRecomputePhase {
	return &MentionRecomputePhase{entities: entities}
}

func (p *MentionRecomputePhase) Name() string { return model.DreamPhaseMentionRecompute }

func (p *MentionRecomputePhase) Execute(ctx context.Context, cycle *model.DreamCycle, _ *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	// RecomputeMentionCountsByNamespace only rewrites entities whose count
	// actually changed, so corrected is the number of counts it healed this
	// cycle (zero in the steady state).
	corrected, err := p.entities.RecomputeMentionCountsByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		// Non-fatal: a recompute failure leaves prior counts in place and the
		// next cycle retries. Returning the error would fail the whole cycle in
		// the runner, so record it in the phase summary and move on.
		slog.Warn("dreaming: mention-recompute phase failed",
			"namespace", cycle.NamespaceID, "err", err)
		p.writePhaseSummary(ctx, logger, map[string]any{"corrected": 0, "errors": 1})
		return PhaseResult{}, nil
	}

	p.writePhaseSummary(ctx, logger, map[string]any{"corrected": corrected})
	if tracker := CycleTrackerFromContext(ctx); tracker != nil {
		tracker.EmitPhaseProgress(ctx, 1, 1, "entities")
	}
	return PhaseResult{}, nil
}

// writePhaseSummary records the corrected-count as a phase_summary log row, the
// same mechanism the other SQL-only phases use; the entry is not counted in the
// cycle op tally but feeds the cycle-detail UI's per-phase metric strip.
func (p *MentionRecomputePhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any) {
	_ = logger.LogOperation(ctx, model.DreamPhaseMentionRecompute, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
