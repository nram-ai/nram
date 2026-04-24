package dreaming

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

const (
	// pruneRelationshipWeightThreshold — active relationships with weight
	// below this value are expired during pruning.
	pruneRelationshipWeightThreshold = 0.05

	// defaultConfidenceDecayThresholdDays is the fallback when the setting
	// is missing or unparseable. Memories untouched longer than this many
	// days are eligible for decay on each pruning cycle.
	defaultConfidenceDecayThresholdDays = 14.0
	// defaultConfidenceDecayRatePerCycle is the per-cycle multiplicative
	// reduction applied to eligible memories (1 - rate multiplies confidence).
	defaultConfidenceDecayRatePerCycle = 0.02
	// defaultConfidenceFloor is the lower bound decay will not cross.
	defaultConfidenceFloor = 0.05
)

// Prune reasons emitted by shouldPrune. Logged and surfaced upstream; pin them
// as constants so the strings stay refactor-safe.
const (
	pruneReasonSuperseded     = "superseded_no_access"
	pruneReasonZeroConfidence = "zero_confidence"
	pruneReasonLowConfidence  = "low_confidence_dream"
)

// PruningPhase removes low-value content from the knowledge graph:
// - Decays confidence of memories untouched beyond a threshold (if enabled)
// - Soft-deletes superseded memories with zero access since supersession
// - Soft-deletes very low confidence dream-originated memories past a minimum age
// - Expires low-weight relationships (below pruneRelationshipWeightThreshold)
// - Leaves dangling relationships pointing to non-existent entities
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
	relWriter RelationshipWriter
	settings  SettingsResolver
}

// NewPruningPhase creates a new pruning phase. settings may be nil, in which
// case confidence decay is permanently disabled regardless of configuration.
func NewPruningPhase(memories MemoryReader, memWriter MemoryWriter, relWriter RelationshipWriter, settings SettingsResolver) *PruningPhase {
	return &PruningPhase{
		memories:  memories,
		memWriter: memWriter,
		relWriter: relWriter,
		settings:  settings,
	}
}

func (p *PruningPhase) Name() string { return model.DreamPhasePruning }

func (p *PruningPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error) {
	memories, err := p.memories.ListByNamespace(ctx, cycle.NamespaceID, 1000, 0)
	if err != nil {
		slog.Warn("dreaming: pruning failed to list memories", "err", err)
	} else {
		// Decay must run before threshold-based pruning so the confidence
		// threshold check reads post-decay values.
		if err := p.applyConfidenceDecay(ctx, cycle, memories); err != nil {
			slog.Warn("dreaming: confidence decay had errors", "err", err)
		}
		if err := p.pruneMemories(ctx, cycle, memories, logger); err != nil {
			slog.Warn("dreaming: memory pruning had errors", "err", err)
		}
	}

	if err := p.pruneRelationships(ctx, cycle, logger); err != nil {
		slog.Warn("dreaming: relationship pruning had errors", "err", err)
	}

	// Pruning is deterministic per cycle: it visits every memory and
	// expires every low-weight relationship in one pass. No residual.
	return false, nil
}

// applyConfidenceDecay scales confidence of memories whose last_accessed is
// older than the configured threshold. Mutates post-decay values onto the
// provided slice so the subsequent prune step sees them without re-reading.
func (p *PruningPhase) applyConfidenceDecay(ctx context.Context, cycle *model.DreamCycle, memories []model.Memory) error {
	if p.settings == nil || !p.settings.ResolveBool(ctx, service.SettingConfidenceDecayEnabled, "global") {
		return nil
	}

	threshold, err := p.settings.ResolveFloat(ctx, service.SettingConfidenceDecayThresholdDays, "global")
	if err != nil || threshold <= 0 {
		threshold = defaultConfidenceDecayThresholdDays
	}
	rate, err := p.settings.ResolveFloat(ctx, service.SettingConfidenceDecayRatePerCycle, "global")
	if err != nil || rate <= 0 || rate >= 1 {
		rate = defaultConfidenceDecayRatePerCycle
	}
	floor, err := p.settings.ResolveFloat(ctx, service.SettingConfidenceFloor, "global")
	if err != nil || floor < 0 || floor > 1 {
		floor = defaultConfidenceFloor
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
		return nil
	}

	rows, err := p.memWriter.DecayConfidence(ctx, eligible, multiplier, floor)
	if err != nil {
		return err
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
	return nil
}

func (p *PruningPhase) pruneMemories(ctx context.Context, cycle *model.DreamCycle, memories []model.Memory, logger *DreamLogWriter) error {
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

	// Hard zero-confidence is the explicit kill signal regardless of source.
	if mem.Confidence == 0 && now.Sub(mem.UpdatedAt) > 7*24*time.Hour {
		return true, pruneReasonZeroConfidence
	}

	src := model.MemorySource(mem)
	if src == model.DreamSource && mem.Confidence < 0.1 && now.Sub(mem.CreatedAt) > 30*24*time.Hour {
		return true, pruneReasonLowConfidence
	}

	return false, ""
}
