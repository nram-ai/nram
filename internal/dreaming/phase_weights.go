package dreaming

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// WeightAdjustmentPhase recalibrates relationship weights and entity mention
// counts based on the current state of the knowledge graph. Relationship
// weights rise when supported by multiple non-deleted, high-confidence
// memories, either direct lineage (memories whose enrichment produced an
// edge between the same endpoints) or co-mention (memories that produced
// rows touching both endpoints separately), and decay otherwise. Recall
// traffic raises weight via the recall-side reinforcement hook in
// internal/service/recall_reinforce.go (RecallService.reinforceRels);
// this phase is the sleep-side complement that reflects the supporting
// memory state, not the recall pattern.
//
// This phase has zero token cost (heuristic-based).
type WeightAdjustmentPhase struct {
	entities      EntityReader
	entityWriter  EntityWriter
	relationships RelationshipReader
	relWriter     RelationshipWriter
	memories      MemoryReader
	settings      SettingsResolver
}

// NewWeightAdjustmentPhase creates a new weight adjustment phase.
func NewWeightAdjustmentPhase(
	entities EntityReader,
	entityWriter EntityWriter,
	relationships RelationshipReader,
	relWriter RelationshipWriter,
	memories MemoryReader,
	settings SettingsResolver,
) *WeightAdjustmentPhase {
	return &WeightAdjustmentPhase{
		entities:      entities,
		entityWriter:  entityWriter,
		relationships: relationships,
		relWriter:     relWriter,
		memories:      memories,
		settings:      settings,
	}
}

func (p *WeightAdjustmentPhase) Name() string { return model.DreamPhaseWeightAdjust }

// supportIndex pre-computes the per-pair direct-lineage set and the
// per-entity co-mention set from one pass over active relationships in the
// namespace. Tier 1 lookups are O(1); Tier 2 lookups are O(min(|ts|,|tt|))
// per relationship, where ts and tt are the source-memory sets touching the
// edge's two endpoints. pairKey is the same canonical-order key the
// contradictions phase uses for memory-pair dedup.
type supportIndex struct {
	directByPair       map[pairKey]map[uuid.UUID]struct{}
	memsTouchingEntity map[uuid.UUID]map[uuid.UUID]struct{}
}

func buildSupportIndex(rels []model.Relationship) (supportIndex, map[uuid.UUID]bool) {
	idx := supportIndex{
		directByPair:       make(map[pairKey]map[uuid.UUID]struct{}),
		memsTouchingEntity: make(map[uuid.UUID]map[uuid.UUID]struct{}),
	}
	allMemoryIDs := make(map[uuid.UUID]bool)

	for _, rel := range rels {
		if rel.ValidUntil != nil {
			continue
		}
		if rel.SourceMemory == nil {
			continue
		}
		sm := *rel.SourceMemory
		allMemoryIDs[sm] = true

		pair := orderedPairKey(rel.SourceID, rel.TargetID)
		if idx.directByPair[pair] == nil {
			idx.directByPair[pair] = make(map[uuid.UUID]struct{})
		}
		idx.directByPair[pair][sm] = struct{}{}

		if idx.memsTouchingEntity[rel.SourceID] == nil {
			idx.memsTouchingEntity[rel.SourceID] = make(map[uuid.UUID]struct{})
		}
		idx.memsTouchingEntity[rel.SourceID][sm] = struct{}{}

		if idx.memsTouchingEntity[rel.TargetID] == nil {
			idx.memsTouchingEntity[rel.TargetID] = make(map[uuid.UUID]struct{})
		}
		idx.memsTouchingEntity[rel.TargetID][sm] = struct{}{}
	}

	return idx, allMemoryIDs
}

// supportSums returns the contribution of supporting memories for one
// relationship. Tier 1 (direct lineage) memories contribute mem.Confidence;
// Tier 2 (co-mention only, not in Tier 1) contribute tier2Multiplier *
// mem.Confidence. Soft-deleted and zero-confidence memories are filtered
// out at sum time; they contribute neither support nor a tier count.
func supportSums(
	rel *model.Relationship,
	idx supportIndex,
	sourceMemories map[uuid.UUID]*model.Memory,
	tier2Multiplier float64,
) (support float64, tier1Count, tier2Count int) {
	tier1 := idx.directByPair[orderedPairKey(rel.SourceID, rel.TargetID)]
	for m := range tier1 {
		mem, ok := sourceMemories[m]
		if !ok || mem == nil || mem.DeletedAt != nil || mem.Confidence <= 0 {
			continue
		}
		support += mem.Confidence
		tier1Count++
	}

	ts := idx.memsTouchingEntity[rel.SourceID]
	tt := idx.memsTouchingEntity[rel.TargetID]
	small, large := ts, tt
	if len(tt) < len(ts) {
		small, large = tt, ts
	}
	for m := range small {
		if _, inT1 := tier1[m]; inT1 {
			continue
		}
		if _, inLarge := large[m]; !inLarge {
			continue
		}
		mem, ok := sourceMemories[m]
		if !ok || mem == nil || mem.DeletedAt != nil || mem.Confidence <= 0 {
			continue
		}
		support += tier2Multiplier * mem.Confidence
		tier2Count++
	}
	return support, tier1Count, tier2Count
}

func (p *WeightAdjustmentPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	tokensBefore := budget.Used()

	rels, err := p.relationships.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return PhaseResult{}, err
	}

	idx, allMemoryIDs := buildSupportIndex(rels)

	// One batch fetch over every memory that could contribute support for
	// any row in this pass; replaces a per-row GetByID round trip.
	memIDs := make([]uuid.UUID, 0, len(allMemoryIDs))
	for memID := range allMemoryIDs {
		memIDs = append(memIDs, memID)
	}
	sourceMemories := make(map[uuid.UUID]*model.Memory, len(memIDs))
	if len(memIDs) > 0 {
		batch, err := p.memories.GetBatch(ctx, memIDs, []uuid.UUID{cycle.NamespaceID})
		if err != nil {
			slog.Warn("dreaming: weight_adjustment source-memory batch failed",
				"err", err, "count", len(memIDs), "cycle", cycle.ID)
		} else {
			for i := range batch {
				sourceMemories[batch[i].ID] = &batch[i]
			}
		}
	}

	tuning := p.resolveWeightTuning(ctx)

	var (
		directionUp   int
		directionDown int
		directionSame int
		expired       int
		tier1Total    int
		tier2Total    int
		visited       int
	)
	now := time.Now().UTC()

	tracker := CycleTrackerFromContext(ctx)
	progressStep := progressEmitStep(len(rels))

	// Collect-then-flush: per-row Expire and UpdateWeight calls inside
	// this loop become one BatchExpire and one BatchUpdateWeight call
	// after the loop. expireOps and weightOps carry the per-row log
	// payload (old + new weight) that LogOperation replays after each
	// batch returns.
	type expireOp struct {
		id                   uuid.UUID
		oldWeight, newWeight float64
	}
	type weightOp struct {
		id                   uuid.UUID
		oldWeight, newWeight float64
	}
	var expireOps []expireOp
	var weightOps []weightOp

	for i, rel := range rels {
		if shouldEmitProgress(i, len(rels), progressStep) {
			if tracker != nil {
				tracker.EmitPhaseProgress(ctx, i+1, len(rels), "relationships")
			}
			slog.Info("dreaming: weight adjust progress",
				"cycle", cycle.ID,
				"relationship", i+1, "of", len(rels))
		}
		if rel.ValidUntil != nil {
			continue
		}
		visited++

		newWeight, t1, t2 := p.calculateWeight(&rel, now, sourceMemories, idx, tuning)
		tier1Total += t1
		tier2Total += t2

		if newWeight == rel.Weight {
			directionSame++
			continue
		}

		// Expire relationships that have decayed below the pruning threshold
		// rather than keeping them alive at near-zero weight. Shares the
		// pruning-phase threshold key so the two paths cannot drift.
		if newWeight < tuning.expiryThreshold {
			expireOps = append(expireOps, expireOp{id: rel.ID, oldWeight: rel.Weight, newWeight: newWeight})
			continue
		}

		weightOps = append(weightOps, weightOp{id: rel.ID, oldWeight: rel.Weight, newWeight: newWeight})

		switch {
		case newWeight > rel.Weight:
			directionUp++
		case newWeight < rel.Weight:
			directionDown++
		default:
			directionSame++
		}
	}

	if len(expireOps) > 0 {
		ids := make([]uuid.UUID, len(expireOps))
		for i, op := range expireOps {
			ids[i] = op.id
		}
		// Match the prior per-row tolerance: a batch-level failure here
		// logs warn and is treated as "no expires committed this pass."
		// Returning err would also discard the accumulated weightOps
		// below, and the time-based decay formula would over-penalize
		// the same edges on the next cycle's retry.
		if _, err := p.relWriter.BatchExpire(ctx, cycle.NamespaceID, ids); err != nil {
			slog.Warn("dreaming: batch expire decayed relationships failed", "cycle", cycle.ID, "err", err)
		} else {
			for _, op := range expireOps {
				if err := logger.LogOperation(ctx, model.DreamPhaseWeightAdjust, "",
					model.DreamOpRelationshipExpired, "relationship", op.id,
					map[string]any{"weight": op.oldWeight},
					map[string]any{"weight": op.newWeight, "reason": "decayed_below_threshold"}); err != nil {
					slog.Warn("dreaming: log operation failed", "err", err)
				}
				expired++
			}
		}
	}

	if len(weightOps) > 0 {
		items := make([]model.WeightUpdateItem, len(weightOps))
		for i, op := range weightOps {
			items[i] = model.WeightUpdateItem{ID: op.id, Weight: op.newWeight}
		}
		if _, err := p.relWriter.BatchUpdateWeight(ctx, cycle.NamespaceID, items); err != nil {
			slog.Warn("dreaming: batch update weight failed", "cycle", cycle.ID, "err", err)
		} else {
			for _, op := range weightOps {
				if err := logger.LogOperation(ctx, model.DreamPhaseWeightAdjust, "",
					model.DreamOpRelationshipUpdated, "relationship", op.id,
					map[string]any{"weight": op.oldWeight},
					map[string]any{"weight": op.newWeight}); err != nil {
					slog.Warn("dreaming: log operation failed", "err", err)
				}
			}
		}
	}

	// Reuse loaded relationships for mention count recalibration.
	p.recalibrateMentionCounts(ctx, cycle.NamespaceID, rels, logger)

	if directionUp > 0 || directionDown > 0 || expired > 0 {
		slog.Info("dreaming: weight adjustments",
			"direction_up", directionUp,
			"direction_down", directionDown,
			"direction_same", directionSame,
			"expired", expired,
			"cycle", cycle.ID)
	}

	p.writePhaseSummary(ctx, logger, map[string]any{
		"sub_phase":        "weight_adjustment",
		"direction_up":     directionUp,
		"direction_down":   directionDown,
		"direction_same":   directionSame,
		"expired":          expired,
		"visited":          visited,
		"tier1_supporters": tier1Total,
		"tier2_supporters": tier2Total,
		"support_gain":     tuning.supportGain,
	}, budget, tokensBefore)

	// Weight adjustment scans every active relationship in one pass; no
	// residual work can be left behind.
	return PhaseResult{}, nil
}

// calculateWeight returns the new weight for rel along with the Tier 1 /
// Tier 2 counts so the phase summary can reveal whether multi-memory
// support is reaching any rows. The decay loop runs only on edges past the
// configured age window; the support multiplier lifts weight only when
// summed memory confidence exceeds one unit; the empty-support guard
// biases dead-source rows toward the pruning floor.
func (p *WeightAdjustmentPhase) calculateWeight(
	rel *model.Relationship,
	now time.Time,
	sourceMemories map[uuid.UUID]*model.Memory,
	idx supportIndex,
	tuning weightTuning,
) (float64, int, int) {
	weight := rel.Weight

	age := now.Sub(rel.ValidFrom)
	decayWindow := time.Duration(tuning.decayWindowDays) * 24 * time.Hour
	if age > decayWindow {
		periods := age.Hours() / (float64(tuning.decayWindowDays) * 24)
		for i := 0; i < int(periods) && i < tuning.decayMaxPeriods; i++ {
			weight *= tuning.decayFactor
		}
	}

	support, t1, t2 := supportSums(rel, idx, sourceMemories, tuning.tier2Multiplier)
	if support > 1.0 {
		weight = weight * (1 + tuning.supportGain*(support-1))
	}

	// Empty-support guard: when no live memory in this namespace attests
	// the edge AND the row's recorded singular source is soft-deleted,
	// scale the weight by deadSourceMultiplier (default 0.5) so dead-source
	// rows fall through to the pruning floor faster. Missing-source rows
	// (source row not loaded) get no extra penalty; decay alone removes
	// them.
	if support <= 0 && rel.SourceMemory != nil {
		if mem, ok := sourceMemories[*rel.SourceMemory]; ok && mem != nil && mem.DeletedAt != nil {
			weight *= tuning.deadSourceMultiplier
		}
	}

	if weight < 0 {
		weight = 0
	}
	if weight > tuning.ceiling {
		weight = tuning.ceiling
	}

	return weight, t1, t2
}

// weightTuning bundles the per-cycle resolved values for every weight-
// adjustment knob. Resolved once at phase entry so the per-relationship
// hot loop reads from a fixed snapshot.
type weightTuning struct {
	supportGain          float64
	tier2Multiplier      float64
	decayWindowDays      int
	decayFactor          float64
	decayMaxPeriods      int
	deadSourceMultiplier float64
	ceiling              float64
	expiryThreshold      float64
}

// resolveWeightTuning loads every knob through *WithDefault. nil settings
// falls back to the registered defaults so test paths work without a stub.
func (p *WeightAdjustmentPhase) resolveWeightTuning(ctx context.Context) weightTuning {
	if p.settings == nil {
		return weightTuning{
			supportGain:          service.GetDefaultFloat(service.SettingDreamingWeightSupportGain),
			tier2Multiplier:      service.GetDefaultFloat(service.SettingDreamWeightTier2Multiplier),
			decayWindowDays:      service.GetDefaultInt(service.SettingDreamWeightDecayWindowDays),
			decayFactor:          service.GetDefaultFloat(service.SettingDreamWeightDecayFactor),
			decayMaxPeriods:      service.GetDefaultInt(service.SettingDreamWeightDecayMaxPeriods),
			deadSourceMultiplier: service.GetDefaultFloat(service.SettingDreamWeightDeadSourceMultiplier),
			ceiling:              service.GetDefaultFloat(service.SettingDreamWeightCeiling),
			expiryThreshold:      service.GetDefaultFloat(service.SettingDreamPruningRelationshipWeightThreshold),
		}
	}
	return weightTuning{
		supportGain:          p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamingWeightSupportGain, "global"),
		tier2Multiplier:      p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamWeightTier2Multiplier, "global"),
		decayWindowDays:      p.settings.ResolveIntWithDefault(ctx, service.SettingDreamWeightDecayWindowDays, "global"),
		decayFactor:          p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamWeightDecayFactor, "global"),
		decayMaxPeriods:      p.settings.ResolveIntWithDefault(ctx, service.SettingDreamWeightDecayMaxPeriods, "global"),
		deadSourceMultiplier: p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamWeightDeadSourceMultiplier, "global"),
		ceiling:              p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamWeightCeiling, "global"),
		expiryThreshold:      p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamPruningRelationshipWeightThreshold, "global"),
	}
}

// writePhaseSummary emits a slog.Info line plus a DreamOpPhaseSummary
// dream_log entry. The direction triad and tier counts in stats are how a
// future regression like the monotonic-decay bug surfaces in dream_logs
// without per-op spot-checking.
func (p *WeightAdjustmentPhase) writePhaseSummary(
	ctx context.Context,
	logger *DreamLogWriter,
	stats map[string]any,
	budget *TokenBudget,
	tokensBefore int,
) {
	stats["tokens_spent"] = budget.Used() - tokensBefore
	stats["budget_remaining"] = budget.Remaining()

	args := make([]any, 0, len(stats)*2)
	for k, v := range stats {
		args = append(args, k, v)
	}
	slog.Info("dreaming: weight_adjustment complete", args...)

	if logger == nil {
		return
	}
	if err := logger.LogOperation(ctx, model.DreamPhaseWeightAdjust, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats); err != nil {
		slog.Warn("dreaming: log phase summary failed",
			"phase", model.DreamPhaseWeightAdjust, "err", err)
	}
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

			if err := logger.LogOperation(ctx, model.DreamPhaseWeightAdjust, "",
				model.DreamOpEntityUpdated, "entity", entity.ID,
				map[string]any{"mention_count": entity.MentionCount},
				map[string]any{"mention_count": activeCount}); err != nil {
				slog.Warn("dreaming: log operation failed", "err", err)
			}
		}
	}
}
