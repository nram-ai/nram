package dreaming

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// WeightAdjustmentPhase recalibrates relationship weights and entity mention
// counts based on the current state of the knowledge graph. Relationships
// supported by multiple high-confidence memories are strengthened; orphaned
// or low-confidence relationships decay.
//
// This phase has zero token cost (heuristic-based).
type WeightAdjustmentPhase struct {
	entities      EntityReader
	entityWriter  EntityWriter
	relationships RelationshipReader
	relWriter     RelationshipWriter
	memories      MemoryReader
}

// NewWeightAdjustmentPhase creates a new weight adjustment phase.
func NewWeightAdjustmentPhase(
	entities EntityReader,
	entityWriter EntityWriter,
	relationships RelationshipReader,
	relWriter RelationshipWriter,
	memories MemoryReader,
) *WeightAdjustmentPhase {
	return &WeightAdjustmentPhase{
		entities:      entities,
		entityWriter:  entityWriter,
		relationships: relationships,
		relWriter:     relWriter,
		memories:      memories,
	}
}

func (p *WeightAdjustmentPhase) Name() string { return model.DreamPhaseWeightAdjust }

func (p *WeightAdjustmentPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error) {
	rels, err := p.relationships.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return false, err
	}

	// Pre-fetch all source memories to avoid N+1 queries.
	sourceMemoryIDs := make(map[uuid.UUID]bool)
	for _, rel := range rels {
		if rel.SourceMemory != nil {
			sourceMemoryIDs[*rel.SourceMemory] = true
		}
	}
	sourceMemories := make(map[uuid.UUID]*model.Memory)
	for memID := range sourceMemoryIDs {
		mem, err := p.memories.GetByID(ctx, memID)
		if err == nil {
			sourceMemories[memID] = mem
		}
	}

	adjusted := 0
	expired := 0
	now := time.Now().UTC()

	for _, rel := range rels {
		if rel.ValidUntil != nil {
			continue
		}

		newWeight := p.calculateWeight(&rel, now, sourceMemories)
		if newWeight == rel.Weight {
			continue
		}

		// Expire relationships that have decayed below the pruning threshold
		// rather than keeping them alive at near-zero weight.
		if newWeight < 0.05 {
			if err := p.relWriter.Expire(ctx, rel.ID, rel.NamespaceID); err != nil {
				slog.Warn("dreaming: expire decayed relationship failed", "relationship", rel.ID, "err", err)
				continue
			}
			if err := logger.LogOperation(ctx, model.DreamPhaseWeightAdjust,
				model.DreamOpRelationshipExpired, "relationship", rel.ID,
				map[string]interface{}{"weight": rel.Weight},
				map[string]interface{}{"weight": newWeight, "reason": "decayed_below_threshold"}); err != nil {
				slog.Warn("dreaming: log operation failed", "err", err)
			}
			expired++
			continue
		}

		if err := p.relWriter.UpdateWeight(ctx, rel.ID, rel.NamespaceID, newWeight); err != nil {
			slog.Warn("dreaming: weight update failed", "relationship", rel.ID, "err", err)
			continue
		}

		if err := logger.LogOperation(ctx, model.DreamPhaseWeightAdjust,
			model.DreamOpRelationshipUpdated, "relationship", rel.ID,
			map[string]interface{}{"weight": rel.Weight},
			map[string]interface{}{"weight": newWeight}); err != nil {
			slog.Warn("dreaming: log operation failed", "err", err)
		}

		adjusted++
	}

	// Reuse loaded relationships for mention count recalibration.
	p.recalibrateMentionCounts(ctx, cycle.NamespaceID, rels, logger)

	if adjusted > 0 || expired > 0 {
		slog.Info("dreaming: weight adjustments", "adjusted", adjusted, "expired", expired, "cycle", cycle.ID)
	}

	// Weight adjustment scans every active relationship in one pass; no
	// residual work can be left behind.
	return false, nil
}

func (p *WeightAdjustmentPhase) calculateWeight(rel *model.Relationship, now time.Time, sourceMemories map[uuid.UUID]*model.Memory) float64 {
	weight := rel.Weight

	age := now.Sub(rel.ValidFrom)
	if age > 30*24*time.Hour {
		decayFactor := 0.95
		periods := age.Hours() / (30 * 24)
		for i := 0; i < int(periods) && i < 10; i++ {
			weight *= decayFactor
		}
	}

	if rel.SourceMemory != nil {
		mem, found := sourceMemories[*rel.SourceMemory]
		if !found {
			weight *= 0.5
		} else if mem.DeletedAt != nil {
			weight *= 0.5
		} else {
			weight *= mem.Confidence
		}
	}

	if weight < 0 {
		weight = 0
	}
	if weight > 2.0 {
		weight = 2.0
	}

	return weight
}

// recalibrateMentionCounts updates entity mention counts to reflect
// the actual number of active relationships.
func (p *WeightAdjustmentPhase) recalibrateMentionCounts(
	ctx context.Context,
	namespaceID uuid.UUID,
	allRels []model.Relationship,
	logger *DreamLogWriter,
) {
	entities, err := p.entities.ListByNamespace(ctx, namespaceID)
	if err != nil {
		return
	}

	// Build per-entity active relationship counts from already-loaded data.
	entityRelCounts := make(map[uuid.UUID]int)
	for _, rel := range allRels {
		if rel.ValidUntil == nil {
			entityRelCounts[rel.SourceID]++
			entityRelCounts[rel.TargetID]++
		}
	}

	for _, entity := range entities {
		activeCount := entityRelCounts[entity.ID]

		// Mention count should be at least the number of active relationships.
		if activeCount > entity.MentionCount {
			if err := p.entityWriter.Upsert(ctx, &model.Entity{
				ID:           entity.ID,
				NamespaceID:  entity.NamespaceID,
				Name:         entity.Name,
				Canonical:    entity.Canonical,
				EntityType:   entity.EntityType,
				MentionCount: activeCount,
				Properties:   entity.Properties,
				Metadata:     entity.Metadata,
			}); err != nil {
				slog.Warn("dreaming: entity mention count update failed", "entity", entity.ID, "err", err)
				continue
			}

			if err := logger.LogOperation(ctx, model.DreamPhaseWeightAdjust,
				model.DreamOpEntityUpdated, "entity", entity.ID,
				map[string]interface{}{"mention_count": entity.MentionCount},
				map[string]interface{}{"mention_count": activeCount}); err != nil {
				slog.Warn("dreaming: log operation failed", "err", err)
			}
		}
	}
}
