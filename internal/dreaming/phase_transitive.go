package dreaming

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

const (
	// transitiveMinWeight is the minimum product-weight for a transitive
	// relationship to be created. This prevents near-zero-weight noise edges
	// from accumulating (e.g. 0.01 * 0.01 = 0.0001).
	transitiveMinWeight = 0.1

	// transitiveMaxPerCycle caps new relationships per dream cycle to prevent
	// runaway graph growth in dense namespaces.
	transitiveMaxPerCycle = 200

	// transitiveHardCap is the absolute maximum number of active relationships
	// in a namespace. Transitive creation stops entirely when this is exceeded.
	transitiveHardCap = 5000

	// transitivePropertySource is stored in the Properties JSON of transitive
	// relationships so future cycles can identify and exclude them from input.
	transitivePropertySource = "transitive"
)

// TransitivePhase discovers implied relationships by traversing the knowledge
// graph. If A→B and B→C exist but A→C does not, a transitive relationship
// is created with weight = product of intermediate weights.
//
// Guards against relationship explosion:
//   - Excludes previously-inferred transitive edges from input (no chaining)
//   - Requires product-weight >= transitiveMinWeight
//   - Caps new relationships per cycle at transitiveMaxPerCycle
//   - Stops entirely when namespace exceeds transitiveHardCap active relationships
//
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

func (p *TransitivePhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error) {
	entities, err := p.entities.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return false, err
	}

	if len(entities) < 3 {
		return false, nil
	}

	// Hard cap: if namespace already has too many relationships, skip entirely.
	// Treat this as a no-residual condition — more cycles won't help, only
	// pruning the graph or raising the cap will unstick it.
	totalActive, err := p.relationships.CountActiveByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return false, err
	}
	if totalActive >= transitiveHardCap {
		slog.Info("dreaming: transitive phase skipped, namespace at hard cap",
			"active", totalActive, "cap", transitiveHardCap, "cycle", cycle.ID)
		return false, nil
	}

	// Build adjacency map for quick lookup.
	allRels, err := p.relationships.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return false, err
	}

	// Build edge lookup: (sourceID, targetID, relation) → relationship.
	type edgeKey struct {
		source, target uuid.UUID
		relation       string
	}
	edges := make(map[edgeKey]*model.Relationship, len(allRels))
	// Outgoing edges per entity — only from non-transitive, non-expired relationships.
	outgoing := make(map[uuid.UUID][]model.Relationship)

	for i := range allRels {
		rel := &allRels[i]
		if rel.ValidUntil != nil {
			continue // skip expired relationships
		}
		key := edgeKey{rel.SourceID, rel.TargetID, rel.Relation}
		edges[key] = rel

		// Exclude previously-inferred transitive edges from input so they
		// cannot chain into further transitive inferences (A→C transitive
		// should not produce A→D just because C→D exists).
		if isTransitiveRelationship(rel) {
			continue
		}
		outgoing[rel.SourceID] = append(outgoing[rel.SourceID], *rel)
	}

	// Per-cycle cap.
	maxNew := transitiveMaxPerCycle
	// Also respect hard cap headroom.
	headroom := transitiveHardCap - totalActive
	if headroom < maxNew {
		maxNew = headroom
	}
	if maxNew <= 0 {
		return false, nil
	}

	transitiveProps := json.RawMessage(`{"source":"` + transitivePropertySource + `"}`)

	created := 0
	// truncated tracks whether we stopped iterating because of the per-cycle
	// cap with more potential inferences still available. When true, the
	// phase reports residual so the next cycle picks up the rest.
	truncated := false
	for _, entityA := range entities {
		if created >= maxNew {
			truncated = true
			break
		}

		relsAB := outgoing[entityA.ID]

		for _, relAB := range relsAB {
			if created >= maxNew {
				truncated = true
				break
			}

			entityB := relAB.TargetID
			relsBC := outgoing[entityB]

			for _, relBC := range relsBC {
				if created >= maxNew {
					truncated = true
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

				// Minimum weight threshold to avoid near-zero noise.
				transitiveWeight := relAB.Weight * relBC.Weight
				if transitiveWeight < transitiveMinWeight {
					continue
				}

				// Create transitive relationship, marked as such.
				newRel := &model.Relationship{
					ID:           uuid.New(),
					NamespaceID:  cycle.NamespaceID,
					SourceID:     entityA.ID,
					TargetID:     entityC,
					Relation:     relAB.Relation,
					Weight:       transitiveWeight,
					Properties:   transitiveProps,
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
			"count", created, "cycle", cycle.ID, "truncated", truncated)
	}

	return truncated, nil
}

// isTransitiveRelationship checks whether a relationship was created by the
// transitive closure phase by inspecting its Properties JSON.
func isTransitiveRelationship(rel *model.Relationship) bool {
	if rel.Properties == nil || len(rel.Properties) == 0 {
		return false
	}
	var props map[string]interface{}
	if err := json.Unmarshal(rel.Properties, &props); err != nil {
		return false
	}
	src, _ := props["source"].(string)
	return src == transitivePropertySource
}
