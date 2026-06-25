package dreaming

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// ConsolidationEntityBackfillPhase enqueues an entity-only enrichment job for
// every active consolidation dream that still lacks entity-graph coverage
// (origin=dream, live, non-empty source_memory_ids, and no relationship yet
// sourced by it). It recovers coverage for syntheses created before dreams were
// extracted (the consolidation-erases-coverage fix), draining a little each
// cycle until none remain.
//
// Each job carries StepsCompleted=["fact_extraction"], the shape the manual
// recovery runbook validated: the worker's skipFact stays hard-on for dreams
// (no extracted_fact child memories — the only dream-of-dream cascade vector),
// while skipEntity runs for consolidation syntheses (graph rows only, never
// memories). Enqueue dedups against the partial unique index
// idx_enrichment_queue_pending_memory, so re-running across cycles while the
// worker drains never piles up duplicate pending jobs, and the candidate query
// drops a dream once entity extraction has run (the entity_extracted_at stamp;
// see ListDreamEntityBackfillCandidates), so a completed backfill stops
// re-selecting it even for an entity-only synthesis that produced no
// relationships.
//
// It runs after the multi-vector backfill phase and BEFORE consolidation, the
// same placement as the augmentation backfill: it sweeps dreams stranded by
// PRIOR cycles, while the fresh syntheses this cycle's consolidation enqueues
// get entity extraction directly through their own enrichment job.
//
// The phase issues no LLM calls of its own (entity extraction happens later in
// the enrichment worker), so it consumes no dream token budget.
type ConsolidationEntityBackfillPhase struct {
	lister   DreamEntityBacklogLister
	queue    EnrichmentQueueWriter
	settings SettingsResolver
}

// NewConsolidationEntityBackfillPhase constructs the phase. lister and queue
// must be non-nil for the phase to do work; when either is nil the phase is a
// no-op. There is intentionally no enable toggle: extracting entities from
// consolidation dreams is unconditional whenever dreaming runs (a disable
// switch would be a footgun that strands heavily-consolidated projects). Only
// the per-cycle cap is operator-tunable, to pace the recovery load.
func NewConsolidationEntityBackfillPhase(
	lister DreamEntityBacklogLister,
	queue EnrichmentQueueWriter,
	settings SettingsResolver,
) *ConsolidationEntityBackfillPhase {
	return &ConsolidationEntityBackfillPhase{
		lister:   lister,
		queue:    queue,
		settings: settings,
	}
}

// Name returns the phase identifier.
func (p *ConsolidationEntityBackfillPhase) Name() string {
	return model.DreamPhaseConsolidationEntityBackfill
}

// Execute lists up to the per-cycle cap of uncovered consolidation dreams for
// the cycle's namespace and enqueues an entity-only job for each. Reports
// residual when more candidates exist than the cap allowed so operators can see
// the backlog draining across cycles.
func (p *ConsolidationEntityBackfillPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	if p.lister == nil || p.queue == nil {
		return PhaseResult{}, nil
	}

	cap, _ := p.settings.ResolveInt(ctx, service.SettingDreamConsolidationEntityBackfillCapPerCycle, "global")
	if cap <= 0 {
		cap = 1000
	}

	stats := map[string]any{
		"sub_phase":     model.DreamPhaseConsolidationEntityBackfill,
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
	candidates, err := p.lister.ListDreamEntityBackfillCandidates(ctx, []uuid.UUID{cycle.NamespaceID}, cap+1)
	if err != nil {
		slog.Warn("dreaming: consolidation entity backfill list failed",
			"cycle", cycle.ID, "namespace", cycle.NamespaceID, "err", err)
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return PhaseResult{}, nil
	}

	foundTotal := len(candidates)
	toProcess := candidates
	if len(toProcess) > cap {
		toProcess = toProcess[:cap]
	}

	// Skip fact extraction (no child memories); leave entity_extraction
	// unstamped so the worker runs it for these consolidation syntheses.
	// Marshalled once; identical for every job.
	factDone, mErr := json.Marshal([]string{model.StepFactExtraction})
	if mErr != nil {
		slog.Warn("dreaming: consolidation entity backfill marshal steps failed",
			"cycle", cycle.ID, "err", mErr)
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return PhaseResult{}, nil
	}

	enqueued := 0
	errCount := 0
	now := time.Now().UTC()
	for _, cand := range toProcess {
		job := &model.EnrichmentJob{
			ID:             uuid.New(),
			MemoryID:       cand.ID,
			NamespaceID:    cycle.NamespaceID,
			Status:         model.EnrichmentStatusPending,
			Priority:       0,
			Attempts:       0,
			MaxAttempts:    3,
			StepsCompleted: factDone,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		// Enqueue dedups against an existing unclaimed-pending job for the same
		// memory (partial unique index), so inserted==false means the memory is
		// already queued from a prior cycle and is left to drain.
		inserted, enqErr := p.queue.Enqueue(ctx, job)
		if enqErr != nil {
			slog.Warn("dreaming: consolidation entity backfill enqueue failed",
				"cycle", cycle.ID, "memory", cand.ID, "err", enqErr)
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

func (p *ConsolidationEntityBackfillPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any, budget *TokenBudget, tokensBefore int) {
	if budget != nil {
		stats["tokens_spent"] = budget.Used() - tokensBefore
		stats["budget_remaining"] = budget.Remaining()
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseConsolidationEntityBackfill, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
