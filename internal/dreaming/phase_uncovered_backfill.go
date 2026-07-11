package dreaming

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// UncoveredEnqueuer enqueues a full enrichment job for every live memory that
// has never been enriched (durable memories.enriched = false) and does not
// already hold an active job, returning the number of jobs inserted. Satisfied
// by *storage.UncoveredBackfiller in production (a thin adapter over
// storage.EnqueueUncoveredMemories) and by a fake in tests. The concrete
// implementation gates the full INSERT behind a hasUncoveredMemory probe (an
// indexed anti-join over the idx_memories_enriched partial index, LIMIT 1) that
// short-circuits to a no-op in the steady state where every memory is enriched.
type UncoveredEnqueuer interface {
	EnqueueUncoveredMemories(ctx context.Context) (int64, error)
}

// UncoveredBackfillPhase enqueues a full enrichment job for every live memory
// that has never been enriched — the durable memories.enriched flag is false. It
// is the unconditional safety net that replaces the removed
// NRAM_ENABLE_ENRICHMENT_BACKFILL boot hook: any memory that slips through the
// write-time enqueue (enrichment disabled at creation, a failed enqueue, a
// direct import or migration) never gets enriched = true, is invisible to graph
// traversal, and has no extracted facts until an operator runs the
// --backfill-enrichment CLI flag by hand (which also exits the process, so it
// cannot run against a live server). The dreaming EmbeddingBackfillPhase already
// re-embeds such rows (restoring vector recall), but it never enqueues a job, so
// facts, entities, and relationships stay absent; this phase closes that residual
// by enqueuing the full pipeline.
//
// Coverage is keyed on the durable memories.enriched flag, NOT on the presence of
// an enrichment_queue row. enriched is stamped true at finalizeJob and survives
// clearing the queue (completed rows are pruned by the admin clear-completed
// endpoint and by memory/namespace deletion), so a queue-row test would re-flag
// every completed memory and re-enqueue the whole corpus every cycle. Once a
// memory is enriched = true it is never re-enqueued here.
//
// Gated on enrichment.enabled: enqueuing full jobs while enrichment is off just
// grows a pending backlog the worker never drains, and "enrichment disabled at
// creation" is itself a cause of uncovered-ness, so the heal is meaningful only
// once enrichment is actually on. No dedicated enable toggle: like the
// unconditional ConsolidationEntityBackfillPhase, disabling this heal would mean
// accepting permanently graph-invisible memories.
//
// Design tradeoff: EnqueueUncoveredMemories is a single global bulk enqueue, not
// a namespace-scoped, per-cycle-capped finder like the other backfill phases. The
// sweep is deliberately namespace-blind: a cycle running for one namespace
// enqueues never-enriched memories across ALL namespaces, unlike the four sibling
// phases which scope to cycle.NamespaceID. That is intended for an unconditional
// safety net — any uncovered memory should heal on the next cycle regardless of
// which namespace's cycle happens to run the sweep first.
// This reuses the one already-tested bulk path instead of duplicating the enqueue
// SQL. Because the enriched predicate makes the steady-state candidate set empty,
// the missing per-cycle cap is moot in practice: the only unbounded case is the
// first cycle after a genuine backlog of never-enriched rows appears (a bulk
// import), which enqueues that real backlog once. The phase issues no LLM calls
// of its own (the worker does the embed/extract when it drains the jobs), so it
// consumes no dream token budget.
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

// Execute enqueues a full enrichment job for every never-enriched memory
// (memories.enriched = false) with no active job and reports the count in a
// phase summary. Returns no residual: the bulk enqueue is a single one-shot
// INSERT (deduped against the partial unique index), not a per-cycle-capped scan.
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
