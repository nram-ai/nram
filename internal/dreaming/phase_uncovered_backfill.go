package dreaming

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// UncoveredEnqueuer enqueues a full enrichment job for every live memory that
// currently holds no enrichment job at all, returning the number of jobs
// inserted. Satisfied by *storage.UncoveredBackfiller in production (a thin
// adapter over storage.EnqueueUncoveredMemories) and by a fake in tests. The
// concrete implementation gates the full INSERT behind a hasUncoveredMemory
// probe (an indexed anti-join, LIMIT 1); in the steady state where nothing is
// uncovered that probe still scans the live-memory set to prove the negative,
// so it is O(live memories) index seeks — cheap at typical scale, not free.
type UncoveredEnqueuer interface {
	EnqueueUncoveredMemories(ctx context.Context) (int64, error)
}

// UncoveredBackfillPhase re-enqueues a full enrichment job for every live
// memory that holds no pending or in-flight enrichment job. It is the
// unconditional safety net that replaces the removed
// NRAM_ENABLE_ENRICHMENT_BACKFILL boot hook: any memory that slips through the
// write-time enqueue (enrichment disabled at creation, a failed enqueue, a
// direct import or migration) is otherwise invisible to graph traversal and has
// no extracted facts until an operator runs the --backfill-enrichment CLI flag
// by hand (which also exits the process, so it cannot run against a live
// server). The dreaming EmbeddingBackfillPhase already re-embeds such rows
// (restoring vector recall), but it never enqueues a job, so facts, entities,
// and relationships stay absent; this phase closes that residual by enqueuing
// the full pipeline.
//
// Gated on enrichment.enabled: enqueuing full jobs while enrichment is off just
// grows a pending backlog the worker never drains, and "enrichment disabled at
// creation" is itself a cause of uncovered-ness, so the heal is meaningful only
// once enrichment is actually on. No dedicated enable toggle: like the
// unconditional ConsolidationEntityBackfillPhase, disabling this heal would mean
// accepting permanently graph-invisible memories.
//
// Design tradeoff: EnqueueUncoveredMemories is a single global bulk enqueue, not
// a namespace-scoped, per-cycle-capped finder like the other backfill phases.
// This reuses the one already-tested bulk path instead of duplicating the
// enqueue SQL, at two costs the sibling phases avoid: the first cycle to run
// after a backlog appears enqueues the ENTIRE backlog at once (no per-cycle cap
// or residual to pace it), and the phase attributes its enqueued count to
// whichever project's cycle ran the sweep first — other cycles in the same poll
// see the probe return false and report zero, so the count is not per-namespace.
// Both are acceptable for a rarely-firing global safety net; a namespace-scoped
// capped finder would be the framework-consistent alternative if pacing or
// per-tenant attribution ever matters. The phase issues no LLM calls of its own
// (the worker does the embed/extract when it drains the jobs), so it consumes no
// dream token budget.
type UncoveredBackfillPhase struct {
	enqueuer UncoveredEnqueuer
	settings SettingsResolver
}

// NewUncoveredBackfillPhase constructs the phase. enqueuer must be non-nil for
// the phase to do work; when it is nil the phase is a no-op.
func NewUncoveredBackfillPhase(enqueuer UncoveredEnqueuer, settings SettingsResolver) *UncoveredBackfillPhase {
	return &UncoveredBackfillPhase{enqueuer: enqueuer, settings: settings}
}

// Name returns the phase identifier.
func (p *UncoveredBackfillPhase) Name() string { return model.DreamPhaseUncoveredBackfill }

// Execute enqueues a full enrichment job for every generally-uncovered memory
// and reports the count in a phase summary. Returns no residual: the bulk
// enqueue is a single one-shot INSERT (deduped against the partial unique
// index), not a per-cycle-capped scan.
func (p *UncoveredBackfillPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	if p.settings != nil && !p.settings.ResolveBool(ctx, service.SettingEnrichmentEnabled, "global") {
		return PhaseResult{}, nil
	}
	if p.enqueuer == nil {
		return PhaseResult{}, nil
	}

	stats := map[string]any{
		"enqueued": 0,
		"errors":   0,
	}
	tokensBefore := 0
	if budget != nil {
		tokensBefore = budget.Used()
	}

	enqueued, err := p.enqueuer.EnqueueUncoveredMemories(ctx)
	if err != nil {
		slog.Warn("dreaming: uncovered-memory backfill enqueue failed",
			"cycle", cycle.ID, "namespace", cycle.NamespaceID, "err", err)
		stats["errors"] = 1
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return PhaseResult{}, nil
	}

	stats["enqueued"] = enqueued
	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
	return PhaseResult{}, nil
}

func (p *UncoveredBackfillPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any, budget *TokenBudget, tokensBefore int) {
	if budget != nil {
		stats["tokens_spent"] = budget.Used() - tokensBefore
		stats["budget_remaining"] = budget.Remaining()
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseUncoveredBackfill, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
