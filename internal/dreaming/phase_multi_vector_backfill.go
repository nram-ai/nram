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

// MultiVectorBackfillPhase enqueues a facet-only enrichment job for every live,
// vectored memory that has not yet been faceted (faceted_at IS NULL AND
// embedding_dim IS NOT NULL). Each job carries the JobMarkerOnlyMultiVector
// sentinel in StepsCompleted, so the enrichment worker reuses the stored
// facet-0 vector and runs only the per-topic sentence embeds: no
// ingestion-decision, no fact/entity extraction, no query-augmentation, and no
// whole-memory re-embed.
//
// This automates the admin "backfill multi-vector" path so faceting self-drains
// each cycle, mirroring AugmentationBackfillPhase. Enqueue dedups against the
// partial unique index idx_enrichment_queue_pending_memory, so re-running across
// cycles while the worker drains never piles up duplicate pending jobs. It runs
// after the embedding-backfill phase so any embedding_dim restored this cycle is
// already visible to ListMultiVectorBackfillCandidates.
//
// The phase issues no LLM calls of its own (the facet sentence-embeds happen
// later in the enrichment worker), so it consumes no dream token budget.
type MultiVectorBackfillPhase struct {
	lister   MultiVectorBacklogLister
	queue    EnrichmentQueueWriter
	settings SettingsResolver
}

// NewMultiVectorBackfillPhase constructs the phase. lister and queue must be
// non-nil for the phase to do work; when either is nil the phase is a no-op.
func NewMultiVectorBackfillPhase(
	lister MultiVectorBacklogLister,
	queue EnrichmentQueueWriter,
	settings SettingsResolver,
) *MultiVectorBackfillPhase {
	return &MultiVectorBackfillPhase{
		lister:   lister,
		queue:    queue,
		settings: settings,
	}
}

// Name returns the phase identifier.
func (p *MultiVectorBackfillPhase) Name() string { return model.DreamPhaseMultiVectorBackfill }

// Execute lists up to the per-cycle cap of un-faceted candidates for the
// cycle's namespace and enqueues a facet-only job for each. Reports residual
// when more candidates exist than the cap allowed so operators can see the
// backlog draining across cycles.
func (p *MultiVectorBackfillPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	if p.settings != nil && !p.settings.ResolveBool(ctx, service.SettingDreamMultiVectorBackfillEnabled, "global") {
		return PhaseResult{}, nil
	}
	// Multi-vector faceting is a global switch. While it is off, the enrichment
	// worker's facet sweep is a clean no-op and never stamps faceted_at, so
	// enqueuing backfill jobs would re-select the same rows every cycle without
	// ever faceting them. Skip the phase until the feature is re-enabled, at
	// which point these rows (faceted_at still NULL) are picked up again
	// naturally; like a disabled switch it is transient, so we gate enqueuing
	// rather than marking rows terminal. Mirrors AugmentationBackfillPhase's
	// gate on SettingQueryAugmentEnabled.
	if p.settings != nil && !p.settings.ResolveBool(ctx, service.SettingMultiVectorEnabled, "global") {
		return PhaseResult{}, nil
	}
	if p.lister == nil || p.queue == nil {
		return PhaseResult{}, nil
	}

	cap, _ := p.settings.ResolveInt(ctx, service.SettingDreamMultiVectorBackfillCapPerCycle, "global")
	if cap <= 0 {
		cap = 1000
	}

	stats := map[string]any{
		"sub_phase":     model.DreamPhaseMultiVectorBackfill,
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
	candidates, err := p.lister.ListMultiVectorBackfillCandidates(ctx, []uuid.UUID{cycle.NamespaceID}, cap+1)
	if err != nil {
		slog.Warn("dreaming: multi-vector backfill list failed",
			"cycle", cycle.ID, "namespace", cycle.NamespaceID, "err", err)
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return PhaseResult{}, nil
	}

	foundTotal := len(candidates)
	toProcess := candidates
	if len(toProcess) > cap {
		toProcess = toProcess[:cap]
	}

	// The facet-only marker the enrichment worker routes on (runPreEmbed ->
	// runMultiVectorFacetSweep). Marshalled once; identical for every job.
	markerBytes, mErr := json.Marshal([]string{model.JobMarkerOnlyMultiVector})
	if mErr != nil {
		slog.Warn("dreaming: multi-vector backfill marshal marker failed",
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
			StepsCompleted: markerBytes,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		// Enqueue dedups against an existing unclaimed-pending job for the same
		// memory (partial unique index), so inserted==false means the memory is
		// already queued from a prior cycle and is left to drain.
		inserted, enqErr := p.queue.Enqueue(ctx, job)
		if enqErr != nil {
			slog.Warn("dreaming: multi-vector backfill enqueue failed",
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

func (p *MultiVectorBackfillPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any, budget *TokenBudget, tokensBefore int) {
	if budget != nil {
		stats["tokens_spent"] = budget.Used() - tokensBefore
		stats["budget_remaining"] = budget.Remaining()
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseMultiVectorBackfill, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
