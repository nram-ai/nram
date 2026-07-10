package dreaming

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// Prune reasons emitted by shouldPrune. Logged and surfaced upstream; pin them
// as constants so the strings stay refactor-safe.
const (
	pruneReasonSuperseded     = "superseded_no_access"
	pruneReasonZeroConfidence = "zero_confidence"
	pruneReasonLowConfidence  = "low_confidence_dream"
)

// PruningPhase removes low-value content from the knowledge graph:
//   - Decays confidence of memories untouched beyond a threshold (if enabled)
//   - Soft-deletes superseded memories with zero access since supersession
//   - Soft-deletes very low confidence dream-originated memories past a minimum age
//   - Expires low-weight relationships (below pruneRelationshipWeightThreshold)
//   - Pressure-prunes the lowest-weight transitive (inferred) relationships
//     when the namespace exceeds hard_cap * namespace_high_water, draining
//     down to hard_cap * namespace_low_water. User-asserted edges are never
//     touched by this branch. This is the relief valve that lets the
//     transitive phase keep producing new inferences once a namespace fills
//     up; without it, a saturated namespace would either stall (hard cap)
//     or loop on tiny per-cycle headroom.
//   - Leaves dangling relationships pointing to non-existent entities
//
// Decay is the sleep-side complement to the recall-side reinforcement
// performed by the service layer's BumpReinforcement: both work together so
// confidence becomes a meaningful, self-adjusting signal rather than a
// static write-time value.
//
// This phase has zero token cost (heuristic-based).
type PruningPhase struct {
	memories  MemoryReader
	memWriter MemoryWriter
	relReader RelationshipReader
	relWriter RelationshipWriter
	settings  SettingsResolver
}

// NewPruningPhase creates a new pruning phase. settings may be nil, in which
// case confidence decay is permanently disabled regardless of configuration.
// relReader may be nil; the pressure-driven transitive prune is a no-op when
// the reader is absent (test paths that do not exercise that branch can omit
// it without wiring a relationship counter).
func NewPruningPhase(memories MemoryReader, memWriter MemoryWriter, relReader RelationshipReader, relWriter RelationshipWriter, settings SettingsResolver) *PruningPhase {
	return &PruningPhase{
		memories:  memories,
		memWriter: memWriter,
		relReader: relReader,
		relWriter: relWriter,
		settings:  settings,
	}
}

func (p *PruningPhase) Name() string { return model.DreamPhasePruning }

func (p *PruningPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	batchSize := p.resolveBatchSize(ctx)

	visited := 0
	decayed := 0
	pruned := 0

	iterErr := iterateMemoriesByNamespace(ctx, p.memories, cycle.NamespaceID, batchSize, func(batch []model.Memory) error {
		visited += len(batch)
		// Decay must run before threshold-based pruning so the confidence
		// threshold check reads post-decay values.
		batchDecayed, err := p.applyConfidenceDecay(ctx, cycle, batch)
		if err != nil {
			slog.Warn("dreaming: confidence decay had errors", "err", err)
		}
		decayed += batchDecayed

		batchPruned, err := p.pruneMemories(ctx, cycle, batch, logger)
		if err != nil {
			slog.Warn("dreaming: memory pruning had errors", "err", err)
		}
		pruned += batchPruned

		// No "of": iterateMemoriesByNamespace streams via cursor and the
		// namespace total is not known upfront.
		slog.Debug("dreaming: pruning progress",
			"cycle", cycle.ID, "batch_size", len(batch),
			"visited", visited, "decayed", decayed, "pruned", pruned)
		return nil
	})
	if iterErr != nil {
		slog.Warn("dreaming: pruning failed to iterate memories", "err", iterErr)
	}

	relationshipsExpired, err := p.pruneRelationships(ctx, cycle, logger)
	if err != nil {
		slog.Warn("dreaming: relationship pruning had errors", "err", err)
	}

	pressureExpired, err := p.pruneTransitiveUnderPressure(ctx, cycle, logger)
	if err != nil {
		slog.Warn("dreaming: pressure-driven transitive pruning had errors", "err", err)
	}

	p.writePhaseSummary(ctx, logger, map[string]any{
		"visited":                     visited,
		"decayed":                     decayed,
		"pruned":                      pruned,
		"relationships_expired":       relationshipsExpired,
		"transitive_pressure_expired": pressureExpired,
		"batch_size":                  batchSize,
	})

	// Pruning is deterministic per cycle: it streams every memory in the
	// namespace through per-batch decay and prune ops, and expires every
	// low-weight relationship in one pass. No residual.
	return PhaseResult{}, nil
}

// resolveBatchSize reads dreaming.pruning.batch_size, falling back to the
// registered default when the setting is missing or invalid.
func (p *PruningPhase) resolveBatchSize(ctx context.Context) int {
	if p.settings == nil {
		return service.GetDefaultInt(service.SettingDreamPruningBatchSize)
	}
	v := p.settings.ResolveIntWithDefault(ctx, service.SettingDreamPruningBatchSize, "global")
	if v <= 0 {
		return service.GetDefaultInt(service.SettingDreamPruningBatchSize)
	}
	return v
}

// applyConfidenceDecay scales confidence of memories whose last_accessed is
// older than the configured threshold. Mutates post-decay values onto the
// provided slice so the subsequent prune step sees them without re-reading.
// Returns the number of memories whose confidence was actually scaled.
func (p *PruningPhase) applyConfidenceDecay(ctx context.Context, cycle *model.DreamCycle, memories []model.Memory) (int, error) {
	if p.settings == nil || !p.settings.ResolveBool(ctx, service.SettingConfidenceDecayEnabled, "global") {
		return 0, nil
	}

	threshold := p.settings.ResolveFloatWithDefault(ctx, service.SettingConfidenceDecayThresholdDays, "global")
	if threshold <= 0 {
		threshold = service.GetDefaultFloat(service.SettingConfidenceDecayThresholdDays)
	}
	rate := p.settings.ResolveFloatWithDefault(ctx, service.SettingConfidenceDecayRatePerCycle, "global")
	if rate <= 0 || rate >= 1 {
		rate = service.GetDefaultFloat(service.SettingConfidenceDecayRatePerCycle)
	}
	floor := p.settings.ResolveFloatWithDefault(ctx, service.SettingConfidenceFloor, "global")
	if floor < 0 || floor > 1 {
		floor = service.GetDefaultFloat(service.SettingConfidenceFloor)
	}

	now := time.Now().UTC()
	thresholdDuration := time.Duration(threshold * 24.0 * float64(time.Hour))
	multiplier := 1.0 - rate

	var eligible []uuid.UUID
	eligibleIdx := make(map[uuid.UUID]int)
	for i, mem := range memories {
		if mem.DeletedAt != nil || mem.Confidence <= floor {
			continue
		}
		// last_accessed is the reconsolidation-side signal. When a memory has
		// never been accessed, fall back to created_at so brand-new but
		// unconsulted memories age out.
		reference := mem.CreatedAt
		if mem.LastAccessed != nil {
			reference = *mem.LastAccessed
		}
		if now.Sub(reference) < thresholdDuration {
			continue
		}
		eligible = append(eligible, mem.ID)
		eligibleIdx[mem.ID] = i
	}

	if len(eligible) == 0 {
		return 0, nil
	}

	rows, err := p.memWriter.DecayConfidence(ctx, eligible, multiplier, floor)
	if err != nil {
		return 0, err
	}

	// Mirror the SQL clamp into the caller's slice so the next pruning step
	// sees the post-decay values without a second read.
	for _, id := range eligible {
		idx := eligibleIdx[id]
		newVal := memories[idx].Confidence * multiplier
		if newVal < floor {
			newVal = floor
		}
		memories[idx].Confidence = newVal
	}

	slog.Info("dreaming: decayed memory confidence",
		"count", rows, "rate", rate, "threshold_days", threshold, "floor", floor,
		"cycle", cycle.ID)
	return int(rows), nil
}

// pruneMemories soft-deletes memories matching shouldPrune and returns the count.
func (p *PruningPhase) pruneMemories(ctx context.Context, cycle *model.DreamCycle, memories []model.Memory, logger *DreamLogWriter) (int, error) {
	pruned := 0
	now := time.Now().UTC()
	zeroFloor := p.resolveEffectivelyZero(ctx)

	tracker := CycleTrackerFromContext(ctx)
	progressStep := progressEmitStep(len(memories))

	for i, mem := range memories {
		// Emit at the top so the UI sees motion regardless of how many rows
		// the shouldPrune/DeletedAt continues skip; most pruning passes
		// touch a small fraction of the working set.
		if tracker != nil && shouldEmitProgress(i, len(memories), progressStep) {
			tracker.EmitPhaseProgress(ctx, i+1, len(memories), "memories")
		}
		if mem.DeletedAt != nil {
			continue
		}

		shouldPrune, reason := p.shouldPrune(&mem, now, zeroFloor)
		if !shouldPrune {
			continue
		}

		_ = logger.LogOperation(ctx, model.DreamPhasePruning, "",
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

	return pruned, nil
}

// pruneRelationships expires every relationship below the weight threshold and returns the count.
func (p *PruningPhase) pruneRelationships(ctx context.Context, cycle *model.DreamCycle, logger *DreamLogWriter) (int64, error) {
	threshold := p.resolveRelationshipWeightThreshold(ctx)
	expired, err := p.relWriter.ExpireLowWeight(ctx, cycle.NamespaceID, threshold)
	if err != nil {
		return 0, err
	}

	if expired > 0 {
		_ = logger.LogOperation(ctx, model.DreamPhasePruning, "",
			model.DreamOpRelationshipExpired, "namespace", cycle.NamespaceID,
			nil, map[string]any{
				"expired_count": expired,
				"threshold":     threshold,
			})
		slog.Info("dreaming: pruned low-weight relationships",
			"count", expired, "threshold", threshold, "cycle", cycle.ID)
	}

	return expired, nil
}

// resolveRelationshipWeightThreshold returns the threshold below which an
// active relationship is expired. Shared with WeightAdjustmentPhase via the
// SettingDreamPruningRelationshipWeightThreshold key; both paths must read
// the same registry-backed value or they will drift.
func (p *PruningPhase) resolveRelationshipWeightThreshold(ctx context.Context) float64 {
	if p.settings == nil {
		return service.GetDefaultFloat(service.SettingDreamPruningRelationshipWeightThreshold)
	}
	return p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamPruningRelationshipWeightThreshold, "global")
}

// pruneTransitiveUnderPressure expires the lowest-weight transitive
// relationships in a namespace once the active relationship count exceeds
// hard_cap * high_water, draining down to hard_cap * low_water. Only
// transitive (properties.source = "transitive") edges are targeted;
// user-asserted relationships are preserved unconditionally.
//
// This is the relief valve for the transitive phase: without it, a
// namespace that reaches hard_cap traps the transitive phase into either
// no-op'ing (>= hard_cap) or producing only headroom-clamped output that
// other phases (entity_dedup merges, consolidation demotion cascades) chew
// back away each cycle, keeping the project dirty in perpetuity.
//
// No-ops when relReader is nil (test paths), when totalActive is below
// the high-water threshold, or when the drain target is misconfigured.
func (p *PruningPhase) pruneTransitiveUnderPressure(ctx context.Context, cycle *model.DreamCycle, logger *DreamLogWriter) (int64, error) {
	if p.relReader == nil {
		return 0, nil
	}

	hardCap := p.resolveInt(ctx, service.SettingDreamTransitiveNamespaceHardCap)
	if hardCap <= 0 {
		return 0, nil
	}
	highWater := p.resolveFloat(ctx, service.SettingDreamTransitiveNamespaceHighWater)
	lowWater := p.resolveFloat(ctx, service.SettingDreamTransitiveNamespaceLowWater)
	// Defensive: a misconfigured pair (low_water >= high_water, or either
	// outside [0, 1]) would either never fire or never converge. The API
	// validator rejects this on PUT, but a manual DB edit can still land
	// it; bail rather than thrash.
	if highWater <= 0 || highWater > 1 || lowWater < 0 || lowWater >= highWater {
		return 0, nil
	}

	totalActive, err := p.relReader.CountActiveByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return 0, err
	}

	highThreshold := int(float64(hardCap) * highWater)
	lowThreshold := int(float64(hardCap) * lowWater)
	if totalActive < highThreshold {
		return 0, nil
	}
	target := totalActive - lowThreshold
	if target <= 0 {
		return 0, nil
	}

	// Per-cycle drain ceiling: reuse the streaming pruning batch size so a
	// single cycle cannot issue an unbounded UPDATE (at the 1M hard_cap
	// default, raw target can be 150k). Pressure relief is incremental;
	// successive cycles drain further toward low_water. The trigger fires
	// every cycle until totalActive falls below high_water, so cap'd cycles
	// just take more wall-clock to converge rather than dropping work.
	if perCycle := p.resolveInt(ctx, service.SettingDreamPruningBatchSize); perCycle > 0 && target > perCycle {
		target = perCycle
	}

	expired, err := p.relWriter.ExpireLowestNTransitive(ctx, cycle.NamespaceID, target)
	if err != nil {
		return 0, err
	}
	if expired > 0 {
		_ = logger.LogOperation(ctx, model.DreamPhasePruning, "",
			model.DreamOpRelationshipExpired, "namespace", cycle.NamespaceID,
			nil, map[string]any{
				"expired_count": expired,
				"trigger":       DreamPruningTriggerTransitivePressure,
				"total_before":  totalActive,
				"hard_cap":      hardCap,
				"high_water":    highWater,
				"low_water":     lowWater,
				"drained_to":    totalActive - int(expired),
			})
		slog.Info("dreaming: pressure-pruned transitive relationships",
			"count", expired, "namespace", cycle.NamespaceID,
			"before", totalActive, "after", totalActive-int(expired),
			"hard_cap", hardCap, "cycle", cycle.ID)
	}
	return expired, nil
}

func (p *PruningPhase) resolveInt(ctx context.Context, key string) int {
	if p.settings == nil { // test path
		return service.GetDefaultInt(key)
	}
	return p.settings.ResolveIntWithDefault(ctx, key, "global")
}

func (p *PruningPhase) resolveFloat(ctx context.Context, key string) float64 {
	if p.settings == nil { // test path
		return service.GetDefaultFloat(key)
	}
	return p.settings.ResolveFloatWithDefault(ctx, key, "global")
}

// resolveEffectivelyZero returns the upper bound of the zero-confidence
// prune branch. See SettingDreamPruningEffectivelyZero for why an exact zero
// check is insufficient (multiplicative haircuts only reach zero on float
// underflow).
func (p *PruningPhase) resolveEffectivelyZero(ctx context.Context) float64 {
	if p.settings == nil {
		return service.GetDefaultFloat(service.SettingDreamPruningEffectivelyZero)
	}
	return p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamPruningEffectivelyZero, "global")
}

func (p *PruningPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any) {
	_ = logger.LogOperation(ctx, model.DreamPhasePruning, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}

func (p *PruningPhase) shouldPrune(mem *model.Memory, now time.Time, zeroFloor float64) (bool, string) {
	// Project-description backing memories are system-owned, reconciled from the
	// projects table by the project_description_sync phase. Dreaming must never
	// prune them out from under that phase; it owns their lifecycle.
	if isProjectDescription(mem) {
		return false, ""
	}

	// Superseded memories with zero access since they were superseded. The
	// supersede clock reads SupersededAt so unrelated row touches that bump
	// UpdatedAt do not reset the 7d countdown. UpdatedAt is the fallback for
	// rows that predate the SupersededAt column.
	if mem.SupersededBy != nil && mem.AccessCount == 0 {
		since := mem.UpdatedAt
		if mem.SupersededAt != nil {
			since = *mem.SupersededAt
		}
		if now.Sub(since) > 7*24*time.Hour {
			return true, pruneReasonSuperseded
		}
	}

	// Effectively-zero confidence is the kill signal regardless of source.
	// See SettingDreamPruningEffectivelyZero for why an exact-zero check is
	// insufficient (multiplicative haircuts only reach zero on underflow).
	if mem.Confidence <= zeroFloor && now.Sub(mem.UpdatedAt) > 7*24*time.Hour {
		return true, pruneReasonZeroConfidence
	}

	if mem.IsDream() && mem.Confidence < 0.1 && now.Sub(mem.CreatedAt) > 30*24*time.Hour {
		return true, pruneReasonLowConfidence
	}

	return false, ""
}
