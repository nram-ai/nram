package dreaming

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// transitivePropertySource is stored in the Properties JSON of transitive
// relationships so future cycles can identify and exclude them from input.
const transitivePropertySource = "transitive"

// TransitivePhase discovers implied relationships by traversing the knowledge
// graph. If A→B and B→C exist but A→C does not, a transitive relationship
// is created with weight = product of intermediate weights.
//
// Guards against relationship explosion:
//   - Excludes previously-inferred transitive edges from input (no chaining)
//   - Requires product-weight >= dreaming.transitive.min_weight
//   - Caps new relationships per cycle at dreaming.transitive.max_per_cycle
//   - Stops entirely when namespace exceeds
//     dreaming.transitive.namespace_hard_cap active relationships
//
// When the per-cycle creation count is bounded by the hard-cap headroom
// rather than by max_per_cycle, the phase emits an informational
// ResidualReasonTransitiveHardCapApproach in the phase summary but does
// not set HasResidual=true; the pruning phase's pressure-driven prune
// is the right loop to make progress, and signalling residual would keep
// the project dirty and trigger an unproductive re-cycle.
//
// This phase has zero token cost (pure graph traversal).
type TransitivePhase struct {
	entities      EntityReader
	relationships RelationshipReader
	relWriter     RelationshipWriter
	settings      SettingsResolver
}

// NewTransitivePhase creates a new transitive relationship discovery phase.
// settings may be nil; the three transitive thresholds fall back to the
// values registered in service.settingDefaults.
func NewTransitivePhase(
	entities EntityReader,
	relationships RelationshipReader,
	relWriter RelationshipWriter,
	settings SettingsResolver,
) *TransitivePhase {
	return &TransitivePhase{
		entities:      entities,
		relationships: relationships,
		relWriter:     relWriter,
		settings:      settings,
	}
}

func (p *TransitivePhase) Name() string { return model.DreamPhaseTransitive }

func (p *TransitivePhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	minWeight := p.resolveFloat(ctx, service.SettingDreamTransitiveMinWeight)
	maxPerCycle := p.resolveInt(ctx, service.SettingDreamTransitiveMaxPerCycle)
	hardCap := p.resolveInt(ctx, service.SettingDreamTransitiveNamespaceHardCap)
	// Semantic gates: only relations in this set may chain, and only when both
	// hops carry the same relation; maxFanout bounds propagation through hubs.
	transitiveRelations := p.resolveTransitiveRelations(ctx)
	maxFanout := p.resolveInt(ctx, service.SettingDreamTransitiveMaxFanout)

	// Operator-quiesce / misconfig short-circuit: skip the expensive
	// ListByNamespace + adjacency build entirely when this cycle cannot
	// possibly create any new edges. Previously the work happened and was
	// only discarded after the headroom clamp at the maxNew computation.
	if maxPerCycle <= 0 {
		return PhaseResult{}, nil
	}

	entities, err := p.entities.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return PhaseResult{}, err
	}

	if len(entities) < 3 {
		return PhaseResult{}, nil
	}

	// Hard cap: if namespace already has too many relationships, skip entirely.
	// Treat this as a no-residual condition; more cycles won't help, only
	// pruning the graph or raising the cap will unstick it.
	totalActive, err := p.relationships.CountActiveByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return PhaseResult{}, err
	}
	if totalActive >= hardCap {
		slog.Info("dreaming: transitive phase skipped, namespace at hard cap",
			"active", totalActive, "cap", hardCap, "cycle", cycle.ID)
		return PhaseResult{}, nil
	}

	// Build adjacency map for quick lookup. Note: this loads every active
	// relationship in the namespace into memory. At namespace_hard_cap=1M
	// (current default) a saturated namespace produces a multi-hundred-MB
	// allocation per cycle; the adjacency-build step has no streaming or
	// sampling fallback. Tracked as a follow-up; the current short-circuits
	// above cover the easy cases (quiesce, sparse namespaces, at-hard-cap),
	// but a deeply populated namespace will still pay the full slice cost.
	allRels, err := p.relationships.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return PhaseResult{}, err
	}

	// Build edge lookup: (sourceID, targetID, relation) → relationship.
	type edgeKey struct {
		source, target uuid.UUID
		relation       string
	}
	// relSource keys the same-relation adjacency index: an intermediate's
	// outgoing hops for one relation, so the inner loop can fetch B's matching
	// hops in O(1) instead of rescanning all of outgoing[B] on every A→B edge.
	type relSource struct {
		entity   uuid.UUID
		relation string
	}
	edges := make(map[edgeKey]*model.Relationship, len(allRels))
	// Outgoing edges per entity (source iteration) and per (entity, relation)
	// (same-relation lookup), both only from non-transitive, non-expired edges.
	// sameRelationOut holds pointers into allRels (stable for the phase) to
	// avoid copying the structs a second time.
	outgoing := make(map[uuid.UUID][]model.Relationship)
	sameRelationOut := make(map[relSource][]*model.Relationship)

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
		rk := relSource{rel.SourceID, rel.Relation}
		sameRelationOut[rk] = append(sameRelationOut[rk], rel)
	}

	// Per-cycle cap, then clamp to remaining hard-cap headroom. After the
	// clamp `maxNew == headroom` iff headroom was the binding constraint,
	// which the residual branch below relies on to label the cause correctly.
	maxNew := maxPerCycle
	headroom := hardCap - totalActive
	if headroom < maxNew {
		maxNew = headroom
	}
	if maxNew <= 0 {
		return PhaseResult{}, nil
	}

	transitiveProps := json.RawMessage(`{"source":"` + transitivePropertySource + `"}`)

	// Candidates accumulate inside the triple loop and flush via one
	// BatchCreate after the loops. `attempts` counts iteration progress
	// (caps via maxNew on attempts, not successes; the old per-row path
	// could iterate past failures and accumulate up to maxNew successes,
	// but with batching we cannot know success-vs-skip until after the
	// batch fires, so the cap binds on attempts). The reported `created`
	// at the end uses BatchCreate's Affected count, so accounting
	// reflects actual persistence rather than attempts.
	candidates := make([]*model.Relationship, 0, maxNew)
	attempts := 0
	// truncated tracks whether we stopped iterating because of the per-cycle
	// cap with more potential inferences still available. When true, the
	// phase reports residual so the next cycle picks up the rest.
	truncated := false

	tracker := CycleTrackerFromContext(ctx)
	progressStep := progressEmitStep(len(entities))
	for entityIdx, entityA := range entities {
		if shouldEmitProgress(entityIdx, len(entities), progressStep) {
			if tracker != nil {
				tracker.EmitPhaseProgress(ctx, entityIdx+1, len(entities), "entities")
			}
			slog.Info("dreaming: transitive progress",
				"cycle", cycle.ID,
				"entity", entityIdx+1, "of", len(entities))
		}
		if attempts >= maxNew {
			truncated = true
			break
		}

		relsAB := outgoing[entityA.ID]

		for _, relAB := range relsAB {
			if attempts >= maxNew {
				truncated = true
				break
			}

			// Semantic gate (default-deny): only relations explicitly marked
			// transitive may chain. Without this, non-transitive relations like
			// "wife of" or symmetric ones like "related to" produce invalid
			// inferences. relAB.Relation is stored canonical, so it matches the
			// canonicalized set keys directly.
			if !transitiveRelations[relAB.Relation] {
				continue
			}

			entityB := relAB.TargetID

			// Same-relation closure (true r∘r): only chain B→C hops that carry
			// the SAME relation as A→B, fetched in O(1) from the precomputed
			// index. The previous code copied relAB.Relation onto any relBC
			// regardless of relBC's own relation, which is what produced edges
			// like "Emma --wife of--> sglang" (A--wife of-->B, B--anything-->C).
			relsBC := sameRelationOut[relSource{entityB, relAB.Relation}]

			// Fan-out cap: an intermediate node that points to more than
			// maxFanout same-relation targets is treated as a hub and skipped,
			// bounding blast radius even if a non-transitive relation is wrongly
			// listed. maxFanout <= 0 disables the cap.
			if maxFanout > 0 && len(relsBC) > maxFanout {
				continue
			}

			for _, relBC := range relsBC {
				if attempts >= maxNew {
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
				if transitiveWeight < minWeight {
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

				candidates = append(candidates, newRel)

				// Add to edge map to prevent duplicates within this cycle.
				edges[key] = newRel
				attempts++
			}
		}
	}

	var batchRes model.BatchCreateResult
	if len(candidates) > 0 {
		res, err := p.relWriter.BatchCreate(ctx, candidates)
		if err != nil {
			// Match the prior per-row tolerance: log and proceed with
			// an empty result rather than aborting the phase (and the
			// dream cycle) on a transient batch-level error.
			slog.Warn("dreaming: transitive batch create failed", "err", err, "cycle", cycle.ID)
		} else {
			batchRes = res
			for _, rel := range candidates {
				// Skipped rows have rel.ID = uuid.Nil; filter them out
				// so dream_log does not record relationships that were
				// never persisted (rollback would chase phantom ids).
				if rel.ID == uuid.Nil {
					continue
				}
				_ = logger.LogOperation(ctx, model.DreamPhaseTransitive, "",
					model.DreamOpRelationshipCreated, "relationship", rel.ID, nil, rel)
			}
		}
	}

	// Report the actually-persisted count, not the attempt count.
	created := int(batchRes.Affected)
	if attempts > 0 {
		slog.Info("dreaming: transitive discovery created relationships",
			"attempts", attempts, "created", created, "skipped", batchRes.Skipped,
			"cycle", cycle.ID, "truncated", truncated)
	}

	if !truncated {
		return PhaseResult{}, nil
	}
	detail := map[string]any{
		"created":       created,
		"per_cycle_cap": maxPerCycle,
		"hard_cap":      hardCap,
		"active":        totalActive,
	}
	if maxNew != headroom {
		return PhaseResult{
			HasResidual:    true,
			ResidualReason: ResidualReasonTransitivePerCycleCap,
			ResidualDetail: detail,
		}, nil
	}
	// Headroom was the binding constraint. Re-running creates no new edges
	// until the pruning phase's pressure branch drains room, so do not set
	// HasResidual (which would keep the project dirty and re-cycle).
	slog.Warn("dreaming: transitive headroom exhausted; namespace approaching hard cap",
		"active", totalActive, "hard_cap", hardCap,
		"created", created, "cycle", cycle.ID)
	return PhaseResult{
		ResidualReason: ResidualReasonTransitiveHardCapApproach,
		ResidualDetail: detail,
	}, nil
}

// resolveFloat reads a float setting via the *WithDefault helper, falling
// back to the registered default when settings is nil (test path).
func (p *TransitivePhase) resolveFloat(ctx context.Context, key string) float64 {
	if p.settings == nil {
		return service.GetDefaultFloat(key)
	}
	return p.settings.ResolveFloatWithDefault(ctx, key, "global")
}

// resolveInt reads an int setting via the *WithDefault helper, falling back
// to the registered default when settings is nil (test path).
func (p *TransitivePhase) resolveInt(ctx context.Context, key string) int {
	if p.settings == nil {
		return service.GetDefaultInt(key)
	}
	return p.settings.ResolveIntWithDefault(ctx, key, "global")
}

// resolveString reads a string setting via the *WithDefault helper, falling
// back to the registered default when settings is nil (test path).
func (p *TransitivePhase) resolveString(ctx context.Context, key string) string {
	if p.settings == nil {
		return service.GetDefaultString(key)
	}
	return p.settings.ResolveStringWithDefault(ctx, key, "global")
}

// resolveTransitiveRelations parses the relations setting (a JSON array of
// relation labels) into a set of canonical labels. A malformed or empty
// configured value falls back to the registered default, so a bad edit cannot
// silently empty the set, which would either disable inference entirely (an
// empty set is default-deny) or, worse if treated as "allow all", re-open the
// graph to every relation. Returning the default on parse failure keeps the
// gate conservative.
func (p *TransitivePhase) resolveTransitiveRelations(ctx context.Context) map[string]bool {
	set := parseTransitiveRelations(p.resolveString(ctx, service.SettingDreamTransitiveRelations))
	if len(set) == 0 {
		set = parseTransitiveRelations(service.GetDefaultString(service.SettingDreamTransitiveRelations))
	}
	return set
}

// parseTransitiveRelations unmarshals a JSON string array and returns the
// canonicalized labels as a set. Returns nil on malformed JSON so callers can
// detect the empty case and fall back to the default.
func parseTransitiveRelations(raw string) map[string]bool {
	var labels []string
	if err := json.Unmarshal([]byte(raw), &labels); err != nil {
		return nil
	}
	set := make(map[string]bool, len(labels))
	for _, l := range labels {
		if c := model.CanonicalRelation(l); c != "" {
			set[c] = true
		}
	}
	return set
}

// isTransitiveRelationship checks whether a relationship was created by the
// transitive closure phase by inspecting its Properties JSON.
func isTransitiveRelationship(rel *model.Relationship) bool {
	if len(rel.Properties) == 0 {
		return false
	}
	var props map[string]any
	if err := json.Unmarshal(rel.Properties, &props); err != nil {
		return false
	}
	src, _ := props["source"].(string)
	return src == transitivePropertySource
}
