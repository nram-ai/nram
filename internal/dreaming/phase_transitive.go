package dreaming

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TransitivePhase discovers implied relationships by traversing the knowledge
// graph. If A→B and B→C exist but A→C does not, a transitive relationship
// is created with weight = product of intermediate weights.
// This phase has zero token cost (pure graph traversal).
type TransitivePhase struct {
	entities      EntityReader
	relationships RelationshipReader
	relWriter     RelationshipWriter
}

// NewTransitivePhase creates a new transitive relationship discovery phase.
func NewTransitivePhase(
	entities EntityReader,
	relationships RelationshipReader,
	relWriter RelationshipWriter,
) *TransitivePhase {
	return &TransitivePhase{
		entities:      entities,
		relationships: relationships,
		relWriter:     relWriter,
	}
}

func (p *TransitivePhase) Name() string { return model.DreamPhaseTransitive }

func (p *TransitivePhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) error {
	entities, err := p.entities.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return err
	}

	if len(entities) < 3 {
		return nil
	}

	// Build adjacency map for quick lookup.
	allRels, err := p.relationships.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return err
	}

	// Build edge lookup: (sourceID, targetID, relation) → relationship.
	type edgeKey struct {
		source, target uuid.UUID
		relation       string
	}
	edges := make(map[edgeKey]*model.Relationship, len(allRels))
	// Outgoing edges per entity.
	outgoing := make(map[uuid.UUID][]model.Relationship)

	for i := range allRels {
		rel := &allRels[i]
		if rel.ValidUntil != nil {
			continue // skip expired relationships
		}
		key := edgeKey{rel.SourceID, rel.TargetID, rel.Relation}
		edges[key] = rel
		outgoing[rel.SourceID] = append(outgoing[rel.SourceID], *rel)
	}

	// Cap new relationships to prevent explosion on dense graphs.
	// Allow up to 2x the current relationship count or 500, whichever is smaller.
	maxNew := len(allRels) * 2
	if maxNew > 500 {
		maxNew = 500
	}
	if maxNew < 10 {
		maxNew = 10
	}

	created := 0
	for _, entityA := range entities {
		if created >= maxNew {
			break
		}

		relsAB := outgoing[entityA.ID]

		for _, relAB := range relsAB {
			if created >= maxNew {
				break
			}

			entityB := relAB.TargetID
			relsBC := outgoing[entityB]

			for _, relBC := range relsBC {
				if created >= maxNew {
					break
				}

				entityC := relBC.TargetID

				// Skip self-loops.
				if entityC == entityA.ID {
					continue
				}

				// Check if A→C already exists with same relation type.
				key := edgeKey{entityA.ID, entityC, relAB.Relation}
				if _, exists := edges[key]; exists {
					continue
				}

				// Create transitive relationship.
				transitiveWeight := relAB.Weight * relBC.Weight
				newRel := &model.Relationship{
					ID:           uuid.New(),
					NamespaceID:  cycle.NamespaceID,
					SourceID:     entityA.ID,
					TargetID:     entityC,
					Relation:     relAB.Relation,
					Weight:       transitiveWeight,
					SourceMemory: relAB.SourceMemory,
					ValidFrom:    relAB.ValidFrom,
				}

				if err := p.relWriter.Create(ctx, newRel); err != nil {
					slog.Warn("dreaming: transitive relationship creation failed", "err", err)
					continue
				}

				// Log the operation.
				_ = logger.LogOperation(ctx, model.DreamPhaseTransitive,
					model.DreamOpRelationshipCreated, "relationship", newRel.ID, nil, newRel)

				// Add to edge map to prevent duplicates within this cycle.
				edges[key] = newRel
				created++
			}
		}
	}

	if created > 0 {
		slog.Info("dreaming: transitive discovery created relationships",
			"count", created, "cycle", cycle.ID)
	}

	return nil
}
