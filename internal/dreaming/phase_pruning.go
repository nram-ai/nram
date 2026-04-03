package dreaming

import (
	"context"
	"log/slog"
	"time"

	"github.com/nram-ai/nram/internal/model"
)

const (
	// pruneRelationshipWeightThreshold — active relationships with weight
	// below this value are expired during pruning.
	pruneRelationshipWeightThreshold = 0.05
)

// PruningPhase removes low-value content from the knowledge graph:
// - Superseded memories with zero access since supersession
// - Very low confidence dream-originated memories past a minimum age
// - Low-weight relationships (below pruneRelationshipWeightThreshold)
// - Dangling relationships pointing to non-existent entities
//
// This phase has zero token cost (heuristic-based).
type PruningPhase struct {
	memories  MemoryReader
	memWriter MemoryWriter
	relWriter RelationshipWriter
}

// NewPruningPhase creates a new pruning phase.
func NewPruningPhase(memories MemoryReader, memWriter MemoryWriter, relWriter RelationshipWriter) *PruningPhase {
	return &PruningPhase{
		memories:  memories,
		memWriter: memWriter,
		relWriter: relWriter,
	}
}

func (p *PruningPhase) Name() string { return model.DreamPhasePruning }

func (p *PruningPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) error {
	// Phase 1: Prune memories.
	if err := p.pruneMemories(ctx, cycle, logger); err != nil {
		slog.Warn("dreaming: memory pruning had errors", "err", err)
	}

	// Phase 2: Expire low-weight relationships.
	if err := p.pruneRelationships(ctx, cycle, logger); err != nil {
		slog.Warn("dreaming: relationship pruning had errors", "err", err)
	}

	return nil
}

func (p *PruningPhase) pruneMemories(ctx context.Context, cycle *model.DreamCycle, logger *DreamLogWriter) error {
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

		if err := p.memWriter.SoftDelete(ctx, mem.ID, cycle.NamespaceID); err != nil {
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

func (p *PruningPhase) pruneRelationships(ctx context.Context, cycle *model.DreamCycle, logger *DreamLogWriter) error {
	expired, err := p.relWriter.ExpireLowWeight(ctx, cycle.NamespaceID, pruneRelationshipWeightThreshold)
	if err != nil {
		return err
	}

	if expired > 0 {
		_ = logger.LogOperation(ctx, model.DreamPhasePruning,
			model.DreamOpRelationshipExpired, "namespace", cycle.NamespaceID,
			nil, map[string]interface{}{
				"expired_count": expired,
				"threshold":     pruneRelationshipWeightThreshold,
			})
		slog.Info("dreaming: pruned low-weight relationships",
			"count", expired, "threshold", pruneRelationshipWeightThreshold, "cycle", cycle.ID)
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
