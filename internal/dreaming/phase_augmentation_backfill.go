package dreaming

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// AugmentationBackfillPhase enqueues a query-augmentation enrichment job for
// every live memory whose vector was built from raw content rather than from
// augmented queries (augmented_embedding_at IS NULL). Such rows arise when the
// enrichment worker's augmentation step is skipped (most often because the
// dedicated query-augment provider was briefly unavailable while a dream cycle
// synthesized a burst of memories) and the job completes anyway, embedding the
// raw content and leaving the row stranded with no automatic recovery.
//
// This phase automates the admin "backfill augmentation" path so stranded rows
// self-heal each cycle. Enqueue dedups against the partial unique index
// idx_enrichment_queue_pending_memory, so re-running across cycles while the
// worker drains never piles up duplicate pending jobs. It runs before
// consolidation so it only sweeps rows stranded by prior cycles, not the fresh
// syntheses consolidation enqueues itself.
//
// The phase issues no LLM calls of its own (the augmentation work happens later
// in the enrichment worker), so it consumes no dream token budget.
type AugmentationBackfillPhase struct {
	lister   AugmentationBacklogLister
	queue    EnrichmentQueueWriter
	settings SettingsResolver
}

// NewAugmentationBackfillPhase constructs the phase. lister and queue must be
// non-nil for the phase to do work; when either is nil the phase is a no-op.
func NewAugmentationBackfillPhase(
	lister AugmentationBacklogLister,
	queue EnrichmentQueueWriter,
	settings SettingsResolver,
) *AugmentationBackfillPhase {
	return &AugmentationBackfillPhase{
		lister:   lister,
		queue:    queue,
		settings: settings,
	}
}

// Name returns the phase identifier.
func (p *AugmentationBackfillPhase) Name() string { return model.DreamPhaseAugmentationBackfill }

// Execute lists up to the per-cycle cap of un-augmented candidates for the
// cycle's namespace and enqueues an augmentation job for each. Reports residual
// when more candidates exist than the cap allowed so operators can see the
// backlog draining across cycles.
func (p *AugmentationBackfillPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	if p.settings != nil && !p.settings.ResolveBool(ctx, service.SettingDreamAugmentationBackfillEnabled, "global") {
		return PhaseResult{}, nil
	}
	if p.lister == nil || p.queue == nil {
		return PhaseResult{}, nil
	}

	cap, _ := p.settings.ResolveInt(ctx, service.SettingDreamAugmentationBackfillCapPerCycle, "global")
	if cap <= 0 {
		cap = 1000
	}

	stats := map[string]any{
		"sub_phase":     model.DreamPhaseAugmentationBackfill,
		"candidates":    0,
		"enqueued":      0,
		"errors":        0,
		"per_cycle_cap": cap,
	}
	tokensBefore := 0
	if budget != nil {
		tokensBefore = budget.Used()
	}

	// Probe one row beyond the cap so the residual signal can distinguish
	// "all clean" from "cap reached with more pending".
	candidates, err := p.lister.ListAugmentationBackfillCandidates(ctx, []uuid.UUID{cycle.NamespaceID}, cap+1)
	if err != nil {
		slog.Warn("dreaming: augmentation backfill list failed",
			"cycle", cycle.ID, "namespace", cycle.NamespaceID, "err", err)
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return PhaseResult{}, nil
	}

	foundTotal := len(candidates)
	toProcess := candidates
	if len(toProcess) > cap {
		toProcess = toProcess[:cap]
	}

	enqueued := 0
	errCount := 0
	now := time.Now().UTC()
	for _, memID := range toProcess {
		job := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    memID,
			NamespaceID: cycle.NamespaceID,
			Status:      model.EnrichmentStatusPending,
			Priority:    0,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		// Enqueue dedups against an existing unclaimed-pending job for the same
		// memory (partial unique index), so inserted==false means the memory is
		// already queued from a prior cycle and is left to drain.
		inserted, enqErr := p.queue.Enqueue(ctx, job)
		if enqErr != nil {
			slog.Warn("dreaming: augmentation backfill enqueue failed",
				"cycle", cycle.ID, "memory", memID, "err", enqErr)
			errCount++
			continue
		}
		if inserted {
			enqueued++
		}
	}

	stats["candidates"] = foundTotal
	stats["enqueued"] = enqueued
	stats["errors"] = errCount
	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)

	if foundTotal > cap {
		return PhaseResult{
			HasResidual:    true,
			ResidualReason: ResidualReasonMoreCandidatesThanBatch,
			ResidualDetail: map[string]any{
				"candidates":    foundTotal,
				"visited":       len(toProcess),
				"per_cycle_cap": cap,
			},
		}, nil
	}
	return PhaseResult{}, nil
}

func (p *AugmentationBackfillPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any, budget *TokenBudget, tokensBefore int) {
	if budget != nil {
		stats["tokens_spent"] = budget.Used() - tokensBefore
		stats["budget_remaining"] = budget.Remaining()
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseAugmentationBackfill, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
