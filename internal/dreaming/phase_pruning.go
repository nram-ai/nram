package dreaming

import (
	"context"
	"log/slog"
	"time"

	"github.com/nram-ai/nram/internal/model"
)

// PruningPhase removes low-value content from the knowledge graph:
// - Superseded memories with zero access since supersession
// - Very low confidence memories past a minimum age
// - Expired relationships
//
// This phase has zero token cost (heuristic-based).
type PruningPhase struct {
	memories MemoryReader
	memWriter MemoryWriter
}

// NewPruningPhase creates a new pruning phase.
func NewPruningPhase(memories MemoryReader, memWriter MemoryWriter) *PruningPhase {
	return &PruningPhase{
		memories:  memories,
		memWriter: memWriter,
	}
}

func (p *PruningPhase) Name() string { return model.DreamPhasePruning }

func (p *PruningPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) error {
	memories, err := p.memories.ListByNamespace(ctx, cycle.NamespaceID, 1000, 0)
	if err != nil {
		return err
	}

	pruned := 0
	now := time.Now().UTC()

	for _, mem := range memories {
		if mem.DeletedAt != nil {
			continue
		}

		shouldPrune, reason := p.shouldPrune(&mem, now)
		if !shouldPrune {
			continue
		}

		// Log before pruning.
		_ = logger.LogOperation(ctx, model.DreamPhasePruning,
			model.DreamOpMemoryDeleted, "memory", mem.ID,
			&mem, map[string]string{"reason": reason})

		if err := p.memWriter.SoftDelete(ctx, mem.ID); err != nil {
			slog.Warn("dreaming: prune failed", "memory", mem.ID, "err", err)
			continue
		}

		pruned++
	}

	if pruned > 0 {
		slog.Info("dreaming: pruned memories", "count", pruned, "cycle", cycle.ID)
	}

	return nil
}

func (p *PruningPhase) shouldPrune(mem *model.Memory, now time.Time) (bool, string) {
	// Superseded memories with zero access since they were superseded.
	if mem.SupersededBy != nil && mem.AccessCount == 0 {
		// Only prune if superseded for at least 7 days.
		if now.Sub(mem.UpdatedAt) > 7*24*time.Hour {
			return true, "superseded_no_access"
		}
	}

	// Very low confidence dream-originated memories older than 30 days.
	src := model.MemorySource(mem)
	if src == model.DreamSource && mem.Confidence < 0.1 && now.Sub(mem.CreatedAt) > 30*24*time.Hour {
		return true, "low_confidence_dream"
	}

	return false, ""
}
