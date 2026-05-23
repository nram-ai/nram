package dreaming

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// NoveltyAuditStampKey is the metadata key stamped on dream memories once
// the novelty audit has visited them. Declared here so the CLI and any
// other external caller can test for the marker without duplicating the
// string literal.
const NoveltyAuditStampKey = "novelty_audited_at"

// ReinforceCheckedStampKey is the metadata key stamped on a synthesis once
// the reinforce sub-phase has scored it against current evidence. Cleared
// implicitly by any Update() that bumps updated_at, so a confidence change
// re-stales the row for the next cycle.
const ReinforceCheckedStampKey = "reinforce_checked_at"

// ConsolidationClusterStampKey marks every source memory in a cluster the
// consolidate sub-phase reached a stable verdict on (audit-rejection or
// successful synthesis). Anchored to UpdatedAt and written via
// UpdateMetadata so stamp == UpdatedAt does not self-invalidate.
const ConsolidationClusterStampKey = "consolidation_cluster_checked_at"

// ConsolidationClusterFingerprintKey carries the cluster's member-ID hash
// alongside the timestamp stamp. Without it, a member migrating out
// between cycles can leave stamp-fresh survivors that re-cluster into a
// structurally different group the timestamp check cannot detect.
const ConsolidationClusterFingerprintKey = "consolidation_cluster_fingerprint"

// ConsolidationLoadCheckedStampKey marks every memory the consolidation
// phase pulled into its candidate pool this cycle, regardless of which
// sub-phase verdict followed. Drives the SQL-level stale predicate
// ListByNamespaceStale so the per-cycle working-set is bounded by the
// stale-row count rather than by the namespace's total size. Anchored to
// UpdatedAt: any memory whose row is mutated by another phase (paraphrase
// supersession, contradiction haircut, reinforcement) advances UpdatedAt
// and re-enters the consolidation candidate pool next cycle without
// further coordination.
const ConsolidationLoadCheckedStampKey = "consolidation_load_checked_at"

// ConsolidationPhase consolidates clusters of related memories into synthesis
// memories and reinforces/erodes existing syntheses based on new evidence.
//
// Consolidation: clusters related memories, uses LLM to synthesize, creates
// new memories with low initial confidence alongside originals.
//
// Reinforcement: evaluates existing syntheses against new evidence, adjusting
// confidence proportionally. When confidence crosses the supersession threshold,
// originals are superseded.
type ConsolidationPhase struct {
	memories         MemoryReader
	memWriter        MemoryWriter
	lineage          LineageWriter
	llmProvider      LLMProviderFunc
	embedderProvider EmbeddingProviderFunc
	settings         SettingsResolver
	vectorPurger     VectorPurger
}

// AttachVectorPurger wires a VectorPurger so dream-side state transitions
// that hide a memory from recall (demotion, supersession) also drop the
// associated vector from the active store. Nil is safe and disables the
// purge hook — behaviour reverts to leaving stale vectors indexed.
func (p *ConsolidationPhase) AttachVectorPurger(vp VectorPurger) {
	p.vectorPurger = vp
}

// NewConsolidationPhase creates a new consolidation and reinforcement phase.
// embedderProvider may be nil; when nil, the novelty audit degrades to LLM-only
// judgement (every borderline call), and pre-write audits will fail closed if
// the embedding provider is unavailable. token_usage rows are written by the
// UsageRecordingProvider middleware wrapping the registry-issued providers;
// no per-phase recorder is needed.
func NewConsolidationPhase(
	memories MemoryReader,
	memWriter MemoryWriter,
	lineage LineageWriter,
	llmProvider LLMProviderFunc,
	embedderProvider EmbeddingProviderFunc,
	settings SettingsResolver,
) *ConsolidationPhase {
	return &ConsolidationPhase{
		memories:         memories,
		memWriter:        memWriter,
		lineage:          lineage,
		llmProvider:      llmProvider,
		embedderProvider: embedderProvider,
		settings:         settings,
	}
}

func (p *ConsolidationPhase) Name() string { return model.DreamPhaseConsolidation }

func (p *ConsolidationPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	// Stamp namespace context once so every provider call emitted by this
	// phase lands a token_usage row attributed to the right scope. The
	// UsageRecordingProvider middleware reads namespace_id from ctx and,
	// when no UsageContext is pre-stamped, falls back to its injected
	// resolver to populate org/user/project.
	ctx = provider.WithNamespaceID(ctx, cycle.NamespaceID)

	llm := p.llmProvider()
	if llm == nil {
		slog.Info("dreaming: no LLM provider for consolidation, skipping")
		return PhaseResult{}, nil
	}

	// Load only memories whose consolidation-load stamp is missing or older
	// than updated_at. The SQL-level stale predicate keeps the working-set
	// bounded to staleFetchMax rather than namespace size; the older tail
	// drains across cycles via residual signaling. Any row another phase
	// mutates this cycle (paraphrase / contradiction / reinforce) advances
	// updated_at, re-entering the candidate pool next cycle without
	// additional coordination.
	staleFetchMax := p.settings.ResolveIntWithDefault(ctx, service.SettingDreamConsolidationStaleFetchMax, "global")
	allMemories, err := p.memories.ListByNamespaceStale(ctx, cycle.NamespaceID, ConsolidationLoadCheckedStampKey, staleFetchMax)
	if err != nil {
		return PhaseResult{}, err
	}

	auditFrac := resolveFraction(ctx, p.settings, service.SettingDreamConsolidationAuditFraction)
	reinforceFrac := resolveFraction(ctx, p.settings, service.SettingDreamConsolidationReinforceFraction)
	consolidateFrac := resolveFraction(ctx, p.settings, service.SettingDreamConsolidationConsolidateFraction)
	sumRemaining := auditFrac + reinforceFrac + consolidateFrac

	staleFetchCapHit := len(allMemories) >= staleFetchMax
	var auditResid, reinforceResid, consolidateResid bool
	subPhases := make([]SubPhaseSummary, 0, 3)

	// Audit first so backlog drain cannot be starved by reinforce.
	if !budget.Exhausted() {
		perCycleCap, _ := p.settings.ResolveInt(ctx, service.SettingDreamNoveltyBackfillPerCycle, "global")
		auditBudget := budget.SubSlice(budget.ProportionalSliceCap(auditFrac, sumRemaining))
		var aerr error
		auditResid, aerr = p.AuditExistingDreams(ctx, cycle, auditBudget, logger, llm, allMemories, perCycleCap)
		if aerr != nil {
			slog.Warn("dreaming: backfill audit had errors", "err", aerr)
		}
		subPhases = append(subPhases, SubPhaseSummary{
			Name:        model.DreamSubPhaseBackfillAudit,
			TokensUsed:  auditBudget.Used(),
			SliceCap:    auditBudget.Total(),
			HasResidual: auditResid,
		})
		sumRemaining -= auditFrac
	}

	if !budget.Exhausted() {
		reinforceBudget := budget.SubSlice(budget.ProportionalSliceCap(reinforceFrac, sumRemaining))
		var rerr error
		reinforceResid, rerr = p.reinforce(ctx, cycle, reinforceBudget, logger, llm, allMemories)
		if rerr != nil {
			slog.Warn("dreaming: reinforcement sub-phase had errors", "err", rerr)
		}
		subPhases = append(subPhases, SubPhaseSummary{
			Name:        model.DreamSubPhaseReinforce,
			TokensUsed:  reinforceBudget.Used(),
			SliceCap:    reinforceBudget.Total(),
			HasResidual: reinforceResid,
		})
		sumRemaining -= reinforceFrac
	}

	if !budget.Exhausted() {
		consolidateBudget := budget.SubSlice(budget.ProportionalSliceCap(consolidateFrac, sumRemaining))
		var cerr error
		consolidateResid, cerr = p.consolidate(ctx, cycle, consolidateBudget, logger, llm, allMemories)
		subPhases = append(subPhases, SubPhaseSummary{
			Name:        model.DreamSubPhaseConsolidate,
			TokensUsed:  consolidateBudget.Used(),
			SliceCap:    consolidateBudget.Total(),
			HasResidual: consolidateResid,
		})
		if cerr != nil {
			// Stamp before bailing out so partial progress survives.
			p.stampConsolidateLoad(ctx, allMemories)
			result := p.buildResidualResult(staleFetchCapHit, auditResid, reinforceResid, consolidateResid, staleFetchMax, len(allMemories))
			result.SubPhases = subPhases
			return result, cerr
		}
	}

	// Stamp every loaded memory so it does not re-enter the candidate pool
	// next cycle unless something else mutates the row (any update bumps
	// updated_at, which invalidates the stamp). This is the load-level
	// fairness primitive that makes the SQL stale predicate progress
	// across cycles.
	p.stampConsolidateLoad(ctx, allMemories)

	result := p.buildResidualResult(staleFetchCapHit, auditResid, reinforceResid, consolidateResid, staleFetchMax, len(allMemories))
	result.SubPhases = subPhases
	return result, nil
}

// Dominance: stale_fetch_cap (bounds the working set) > consolidate (latest
// sub-phase, survived audit + reinforce) > reinforce > audit.
func (p *ConsolidationPhase) buildResidualResult(
	staleFetchCap, audit, reinforce, consolidate bool,
	staleFetchMax, loaded int,
) PhaseResult {
	if !staleFetchCap && !audit && !reinforce && !consolidate {
		return PhaseResult{}
	}
	detail := map[string]any{
		"stale_fetch_cap_hit":            staleFetchCap,
		"audit_stale_remaining":          audit,
		"reinforce_cap_hit":              reinforce,
		"consolidate_clusters_remaining": consolidate,
		"stale_fetch_max":                staleFetchMax,
		"loaded":                         loaded,
	}
	var reason string
	switch {
	case staleFetchCap:
		reason = ResidualReasonStaleFetchCap
	case consolidate:
		reason = ResidualReasonConsolidateClustersRemaining
	case reinforce:
		reason = ResidualReasonReinforceCapHit
	default:
		reason = ResidualReasonAuditStaleRemaining
	}
	return PhaseResult{
		HasResidual:    true,
		ResidualReason: reason,
		ResidualDetail: detail,
	}
}

// resolveFraction resolves a fractional setting, clamping to (0,1]. On error
// or out-of-range value it returns the registered default from
// service.settingDefaults so the registry remains the single source of
// truth (no per-call-site fallback literal that can drift).
func resolveFraction(ctx context.Context, settings SettingsResolver, key string) float64 {
	v, err := settings.ResolveFloat(ctx, key, "global")
	if err == nil && v > 0 && v <= 1 {
		return v
	}
	return service.GetDefaultFloat(key)
}

// reinforce evaluates existing dream-originated synthesis memories against
// new evidence, adjusting confidence proportionally. Returns true for the
// residual flag when the sub-phase visited fewer syntheses than existed
// (for example, when the per-sub-slice budget was exhausted mid-pass).
func (p *ConsolidationPhase) reinforce(
	ctx context.Context,
	cycle *model.DreamCycle,
	budget *TokenBudget,
	logger *DreamLogWriter,
	llm provider.LLMProvider,
	allMemories []model.Memory,
) (bool, error) {
	const subPhase = model.DreamSubPhaseReinforce
	var syntheses []model.Memory
	var userMemories []model.Memory
	for _, m := range allMemories {
		if m.DeletedAt != nil {
			continue
		}
		src := model.MemorySource(&m)
		if src == model.DreamSource {
			syntheses = append(syntheses, m)
		} else {
			userMemories = append(userMemories, m)
		}
	}

	stale := collectReinforceStale(syntheses)

	stats := map[string]interface{}{
		"sub_phase":           "reinforce",
		"syntheses_total":     len(syntheses),
		"syntheses_stale":     len(stale),
		"user_memories":       len(userMemories),
		"alignment_calls":     0,
		"confidence_adjusted": 0,
		"supersessions":       0,
		"skipped_budget":      0,
		"errors_scoring":      0,
		"errors_update":       0,
	}
	tokensBefore := budget.Used()

	if len(stale) == 0 || len(userMemories) == 0 {
		slog.Info("dreaming: reinforce starting (no-op)",
			"cycle", cycle.ID, "syntheses", len(syntheses), "stale", len(stale),
			"user_memories", len(userMemories), "budget_remaining", budget.Remaining())
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return false, nil
	}

	slog.Info("dreaming: reinforce starting",
		"cycle", cycle.ID, "syntheses", len(syntheses), "stale", len(stale),
		"user_memories", len(userMemories), "budget_remaining", budget.Remaining())

	supersessionThreshold := p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamSupersessionThreshold, "global")
	alignmentSampleSize := p.settings.ResolveIntWithDefault(ctx, service.SettingDreamConsolidationAlignmentSampleSize, "global")
	alignmentTemperature := p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamAlignmentTemperature, "global")

	alignmentPromptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamAlignmentPrompt, "global")

	visited := 0
	for i := range stale {
		synthesis := stale[i].mem
		meta := stale[i].meta
		if budget.Exhausted() {
			stats["skipped_budget"] = stats["skipped_budget"].(int) + 1
			break
		}
		visited++

		// Get a sample of user memories to evaluate against.
		sample := sampleMemories(userMemories, alignmentSampleSize)
		if len(sample) == 0 {
			continue
		}

		// Pre-flight budget check using the same prompt we'll send.
		prompt := renderAlignmentPrompt(alignmentPromptTemplate, &synthesis, sample)
		estCost := EstimateTokens(prompt) + budget.PerCallCap()
		if !budget.CanAfford(estCost) {
			slog.Info("dreaming: alignment call skipped (estimated cost exceeds remaining budget)",
				"cycle", cycle.ID, "synthesis", synthesis.ID,
				"alignment", visited, "of", len(stale),
				"estimate", estCost, "budget_remaining", budget.Remaining())
			stats["skipped_budget"] = stats["skipped_budget"].(int) + 1
			break
		}

		callStart := time.Now()
		alignmentCtx := provider.WithMemoryID(ctx, synthesis.ID)
		alignment, usage, err := p.scoreAlignment(alignmentCtx, llm, synthesis.ID, prompt, budget, alignmentTemperature)
		callTokens := 0
		if usage != nil {
			callTokens = usage.TotalTokens
		}
		stats["alignment_calls"] = stats["alignment_calls"].(int) + 1
		slog.Info("dreaming: alignment call",
			"cycle", cycle.ID, "synthesis", synthesis.ID,
			"alignment", visited, "of", len(stale),
			"latency_ms", time.Since(callStart).Milliseconds(),
			"tokens", callTokens,
			"budget_remaining", budget.Remaining())

		if errors.Is(err, ErrBudgetExhausted) {
			slog.Info("dreaming: alignment loop stopped on budget exhaustion",
				"cycle", cycle.ID, "synthesis", synthesis.ID,
				"alignment", visited, "of", len(stale),
				"tokens", callTokens,
				"budget_remaining", budget.Remaining())
			break
		}

		if err != nil {
			slog.Warn("dreaming: alignment scoring failed", "synthesis", synthesis.ID, "err", err)
			stats["errors_scoring"] = stats["errors_scoring"].(int) + 1
			continue
		}

		// Adjust confidence proportionally.
		oldConfidence := synthesis.Confidence
		newConfidence := oldConfidence + (alignment * (1 - oldConfidence))
		if newConfidence < 0 {
			newConfidence = 0
		}
		if newConfidence > 1 {
			newConfidence = 1
		}

		if newConfidence == oldConfidence {
			p.stampReinforce(ctx, &synthesis, meta)
			continue
		}

		// Write first, then log — ensures log matches actual state.
		synthesis.Confidence = newConfidence
		synthesis.UpdatedAt = time.Now().UTC()
		if err := p.memWriter.UpdateConfidence(ctx, synthesis.ID, synthesis.NamespaceID, newConfidence); err != nil {
			slog.Warn("dreaming: confidence update failed", "err", err)
			stats["errors_update"] = stats["errors_update"].(int) + 1
			continue
		}

		stats["confidence_adjusted"] = stats["confidence_adjusted"].(int) + 1
		if err := logger.LogOperation(ctx, model.DreamPhaseConsolidation, subPhase,
			model.DreamOpConfidenceAdjusted, "memory", synthesis.ID,
			map[string]interface{}{"confidence": oldConfidence},
			map[string]interface{}{"confidence": newConfidence, "alignment": alignment}); err != nil {
			slog.Warn("dreaming: log confidence adjustment failed", "err", err)
		}

		// Check supersession threshold.
		if newConfidence >= supersessionThreshold {
			stats["supersessions"] = stats["supersessions"].(int) + 1
			p.supersedeOriginals(ctx, cycle, &synthesis, logger)
		}
	}

	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
	return visited < len(stale), nil
}

// renderAlignmentPrompt builds the alignment-scoring prompt so it can be
// inspected for budget estimation before the LLM call.
func renderAlignmentPrompt(template string, synthesis *model.Memory, evidence []model.Memory) string {
	var evidenceTexts []string
	for _, e := range evidence {
		evidenceTexts = append(evidenceTexts, e.Content)
	}
	return fmt.Sprintf(template, synthesis.Content, strings.Join(evidenceTexts, "\n---\n"))
}

// scoreAlignment asks the LLM how strongly recent evidence supports or
// contradicts an existing synthesis. Returns a value from -1.0 (strong
// contradiction) to 1.0 (strong support).
func (p *ConsolidationPhase) scoreAlignment(
	ctx context.Context,
	llm provider.LLMProvider,
	synthesisID uuid.UUID,
	prompt string,
	budget *TokenBudget,
	temperature float64,
) (float64, *provider.TokenUsage, error) {
	resp, usage, err := WrapLLMCall(ctx, budget, OpAlignmentScore, llm.Name(),
		synthesisID.String(),
		func(ctx context.Context) (*provider.CompletionResponse, *provider.TokenUsage, error) {
			ctx = provider.WithOperation(ctx, provider.OperationDreamAlignmentScoring)
			r, e := llm.Complete(ctx, &provider.CompletionRequest{
				Messages: []provider.Message{
					{Role: "user", Content: prompt},
				},
				MaxTokens:   budget.PerCallCap(),
				Temperature: temperature,
				JSONMode:    true,
			})
			return r, usageOrEstimateLLM(r, prompt, budget, llm.Name(), model.DreamPhaseConsolidation), e
		})
	if err != nil {
		return 0, usage, err
	}

	var result struct {
		Alignment float64 `json:"alignment"`
		Reasoning string  `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(resp.Content)), &result); err != nil {
		return 0, usage, fmt.Errorf("parse alignment response: %w", err)
	}

	// Clamp to [-1, 1].
	if result.Alignment < -1 {
		result.Alignment = -1
	}
	if result.Alignment > 1 {
		result.Alignment = 1
	}

	return result.Alignment, usage, nil
}

// supersedeOriginals marks the source memories of a synthesis as superseded
// once the synthesis has reached sufficient confidence. Called only from the
// reinforce sub-phase, so the log entry is attributed accordingly.
func (p *ConsolidationPhase) supersedeOriginals(
	ctx context.Context,
	cycle *model.DreamCycle,
	synthesis *model.Memory,
	logger *DreamLogWriter,
) {
	const subPhase = model.DreamSubPhaseReinforce
	// Same lineage fallback as the audit path: prefer metadata, but
	// recover from memory_lineage when source_memory_ids is missing.
	// Without this, a clobbered synthesis cannot supersede its sources
	// even after reaching the supersession confidence threshold.
	meta := decodeMetadata(synthesis.Metadata)
	for _, memID := range p.resolveSourceMemoryIDs(ctx, synthesis, meta) {
		original, err := p.memories.GetByID(ctx, memID)
		if err != nil || original.SupersededBy != nil {
			continue
		}

		now := time.Now().UTC()
		original.SupersededBy = &synthesis.ID
		original.SupersededAt = &now
		original.UpdatedAt = now
		original.EmbeddingDim = nil // vector is purged below; keep row state in sync
		if err := p.memWriter.MarkSupersededBy(ctx, original.ID, original.NamespaceID, synthesis.ID); err != nil {
			slog.Warn("dreaming: supersession update failed", "memory", memID, "err", err)
			continue
		}

		// Recall should surface the synthesis, not its sources. Row
		// remains addressable by ID for lineage/rollback.
		p.purgeVector(ctx, memID)

		if err := logger.LogOperation(ctx, model.DreamPhaseConsolidation, subPhase,
			model.DreamOpMemorySuperseded, "memory", memID,
			map[string]interface{}{"superseded_by": nil},
			map[string]interface{}{"superseded_by": synthesis.ID.String()}); err != nil {
			slog.Warn("dreaming: log supersession failed", "err", err)
		}
	}
}

// purgeVector drops the vector. Best-effort; errors are logged.
func (p *ConsolidationPhase) purgeVector(ctx context.Context, id uuid.UUID) {
	if p.vectorPurger == nil {
		return
	}
	if err := p.vectorPurger.Delete(ctx, storage.VectorKindMemory, id); err != nil {
		slog.Warn("dreaming: vector purge failed", "memory", id, "err", err)
	}
}

// auditExistingDreams applies the novelty audit to historical dream memories
// that were created before the audit existed (or before the user enabled it).
// Bounded by SettingDreamNoveltyBackfillPerCycle so the work drains across
// cycles rather than torching a single cycle's token budget on a large
// backlog. Idempotency comes from the metadata.novelty_audited_at marker —
// once audited, a memory is never reconsidered.
//
// On audit failure the memory is demoted in place: Confidence is set to 0 and
// metadata.low_novelty is set to true (with a reason). The recall service
// excludes such memories from competitive ranking.
//
// Backfill is limited to the cycle's working set (allMemories). When the
// dreaming pipeline gets broader pagination, backfill benefits automatically.
// AuditExistingDreams applies the novelty audit to historical dream memories
// up to the supplied per-invocation cap. perCycleCap <= 0 disables the
// pass (returns residual=false). Respects SettingDreamNoveltyEnabled and
// the supplied token budget. Callers: the regular dream cycle (Execute)
// reads SettingDreamNoveltyBackfillPerCycle; the one-shot backfill CLI
// passes an explicit value.
func (p *ConsolidationPhase) AuditExistingDreams(
	ctx context.Context,
	cycle *model.DreamCycle,
	budget *TokenBudget,
	logger *DreamLogWriter,
	llm provider.LLMProvider,
	allMemories []model.Memory,
	perCycleCap int,
) (bool, error) {
	if !p.settings.ResolveBool(ctx, service.SettingDreamNoveltyEnabled, "global") {
		return false, nil
	}

	if perCycleCap <= 0 {
		return false, nil
	}

	// Count candidates (unstamped dream memories) via a byte-level substring
	// check on the raw metadata so we avoid a JSON unmarshal per memory. The
	// stamp key is only ever written by writeAuditDecision so collisions with
	// user-supplied metadata are vanishingly rare.
	stampMarker := []byte(NoveltyAuditStampKey)
	eligible := 0
	for i := range allMemories {
		m := &allMemories[i]
		if m.DeletedAt != nil || model.MemorySource(m) != model.DreamSource {
			continue
		}
		if bytes.Contains(m.Metadata, stampMarker) {
			continue
		}
		eligible++
	}

	stats := map[string]interface{}{
		"sub_phase":              "backfill_audit",
		"candidates_total":       eligible,
		"per_cycle_cap":          perCycleCap,
		"audited":                0,
		"passed":                 0,
		"demoted":                0,
		"orphans_demoted":        0,
		"fetch_errors":           0,
		"audit_errors":           0,
		"persistent_audit_errors": 0,
		"skipped_budget":         0,
		"embedding_calls":        0,
		"judge_calls":            0,
		"embedding_tokens_spent": 0,
	}
	tokensBefore := budget.Used()

	slog.Info("dreaming: backfill audit starting",
		"cycle", cycle.ID, "candidates", eligible,
		"per_cycle_cap", perCycleCap, "budget_remaining", budget.Remaining())

	if eligible == 0 {
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return false, nil
	}

	// Hoisted out of the loop: a single settings lookup per call rather
	// than per memory. Falls through to the synthesis threshold inside
	// auditNovelty when zero.
	backfillHigh, _ := p.settings.ResolveFloat(ctx, service.SettingDreamNoveltyBackfillEmbedHighThreshold, "global")

	processed := 0
	capHit := false
	for i := range allMemories {
		if processed >= perCycleCap {
			capHit = true
			break
		}
		if budget.Exhausted() {
			stats["skipped_budget"] = stats["skipped_budget"].(int) + 1
			break
		}

		mem := allMemories[i]
		if mem.DeletedAt != nil {
			continue
		}
		if model.MemorySource(&mem) != model.DreamSource {
			continue
		}

		meta := decodeMetadata(mem.Metadata)
		if _, alreadyAudited := meta[NoveltyAuditStampKey]; alreadyAudited {
			continue
		}

		processed++
		stats["audited"] = stats["audited"].(int) + 1

		// resolveSourceMemoryIDs prefers metadata.source_memory_ids and
		// falls back to memory_lineage when metadata is empty (catches
		// historical damage from the metadata-clobbering bug fixed in
		// the same change-set, plus any future regression of the same
		// pattern). On lineage hit, the recovered IDs are written back
		// into metadata in place so next cycle takes the fast path.
		sourceIDs := p.resolveSourceMemoryIDs(ctx, &mem, meta)
		if len(sourceIDs) == 0 {
			// Orphan synthesis: no recoverable lineage to compare against.
			p.demoteDream(ctx, logger, &mem, meta, "orphan_no_sources")
			stats["orphans_demoted"] = stats["orphans_demoted"].(int) + 1
			stats["demoted"] = stats["demoted"].(int) + 1
			slog.Info("dreaming: backfill audit result",
				"cycle", cycle.ID, "memory", mem.ID,
				"audit", processed, "of", eligible,
				"reason", "orphan_no_sources",
				"passed", false, "embed_tokens", 0, "llm_tokens", 0,
				"budget_remaining", budget.Remaining())
			continue
		}

		fetched, err := p.memories.GetBatch(ctx, sourceIDs)
		if err != nil {
			slog.Warn("dreaming: backfill source fetch failed",
				"memory", mem.ID, "err", err)
			stats["fetch_errors"] = stats["fetch_errors"].(int) + 1
			continue
		}
		sources := make([]model.Memory, 0, len(fetched))
		for _, src := range fetched {
			if src.DeletedAt == nil {
				sources = append(sources, src)
			}
		}
		if len(sources) == 0 {
			p.demoteDream(ctx, logger, &mem, meta, "orphan_sources_missing")
			stats["orphans_demoted"] = stats["orphans_demoted"].(int) + 1
			stats["demoted"] = stats["demoted"].(int) + 1
			slog.Info("dreaming: backfill audit result",
				"cycle", cycle.ID, "memory", mem.ID,
				"audit", processed, "of", eligible,
				"reason", "orphan_sources_missing",
				"passed", false, "embed_tokens", 0, "llm_tokens", 0,
				"budget_remaining", budget.Remaining())
			continue
		}

		callStart := time.Now()
		auditCtx := provider.WithMemoryID(ctx, mem.ID)
		passed, reason, auditUsage, embedTokens, auditErr := p.auditNovelty(auditCtx, llm, budget, mem.Content, sources, backfillHigh, provider.OperationDreamNoveltyBackfill)
		llmTokens := 0
		if auditUsage != nil {
			llmTokens = auditUsage.TotalTokens
			stats["judge_calls"] = stats["judge_calls"].(int) + 1
		}
		if embedTokens > 0 {
			stats["embedding_calls"] = stats["embedding_calls"].(int) + 1
			stats["embedding_tokens_spent"] = stats["embedding_tokens_spent"].(int) + embedTokens
		}
		slog.Info("dreaming: backfill audit result",
			"cycle", cycle.ID, "memory", mem.ID,
			"audit", processed, "of", eligible,
			"reason", reason,
			"passed", passed, "latency_ms", time.Since(callStart).Milliseconds(),
			"embed_tokens", embedTokens, "llm_tokens", llmTokens,
			"budget_remaining", budget.Remaining())

		if errors.Is(auditErr, ErrBudgetExhausted) {
			slog.Info("dreaming: backfill audit loop stopped on budget exhaustion",
				"cycle", cycle.ID, "memory", mem.ID,
				"audit", processed, "of", eligible,
				"budget_remaining", budget.Remaining())
			break
		}
		if auditErr != nil {
			persistent := isPersistentEmbedError(auditErr)
			slog.Warn("dreaming: backfill novelty audit error",
				"memory", mem.ID, "err", auditErr, "reason", reason,
				"persistent", persistent)
			stats["audit_errors"] = stats["audit_errors"].(int) + 1
			if persistent {
				// Without stamping, has_residual=true loops forever and the
				// project dirty flag never clears (scheduler.go:251).
				p.writeAuditDecision(ctx, logger, &mem, meta, "embed_error_persistent", false)
				stats["persistent_audit_errors"] = stats["persistent_audit_errors"].(int) + 1
			}
			continue
		}

		if passed {
			p.stampAudited(ctx, &mem, meta, reason)
			stats["passed"] = stats["passed"].(int) + 1
			continue
		}

		p.demoteDream(ctx, logger, &mem, meta, reason)
		stats["demoted"] = stats["demoted"].(int) + 1
	}

	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
	return capHit || processed < eligible, nil
}

// stampAudited records that a dream memory passed the novelty audit so
// future cycles skip it. Mutates only the metadata field.
func (p *ConsolidationPhase) stampAudited(ctx context.Context, mem *model.Memory, meta map[string]interface{}, reason string) {
	p.writeAuditDecision(ctx, nil, mem, meta, reason, false)
}

// persistentEmbedErrorRegex matches HTTP 4xx in OpenAI-shaped error
// messages (provider/openai.go:336 wraps as "API error (NNN): ...").
var persistentEmbedErrorRegex = regexp.MustCompile(`API error \(4\d{2}\)`)

// persistentEmbedPhrases catches context-overflow shapes from providers
// whose errors do not surface the HTTP status code in the message.
var persistentEmbedPhrases = []string{
	"context length",
	"maximum context",
	"context window",
	"too long",
	"token limit",
	"exceeds the maximum",
	"input is too large",
}

// isPersistentEmbedError returns true for embed errors that will fail
// identically on retry (HTTP 4xx, context overflow). Persistent errors
// are stamped on the synthesis so it exits eligibility; transient ones
// (5xx, network, timeout) fall through and retry next cycle.
func isPersistentEmbedError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if persistentEmbedErrorRegex.MatchString(msg) {
		return true
	}
	lower := strings.ToLower(msg)
	for _, phrase := range persistentEmbedPhrases {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	return false
}

// demoteDream zeroes Confidence and stamps low_novelty so the recall service
// excludes the row from competitive ranking. Logs DreamOpMemoryDemoted.
func (p *ConsolidationPhase) demoteDream(ctx context.Context, logger *DreamLogWriter, mem *model.Memory, meta map[string]interface{}, reason string) {
	p.writeAuditDecision(ctx, logger, mem, meta, reason, true)
}

// writeAuditDecision is the shared write path for stampAudited and demoteDream.
// demote=false uses UpdateMetadata so the audit stamp survives the next cycle;
// demote=true takes the full Update path because Confidence/low_novelty/
// EmbeddingDim are real state changes — a stale stamp on demote is harmless
// since low_novelty filtering keeps the row out of future audit eligibility.
//
// All call sites originate in the backfill_audit sub-phase, so the log entry
// is attributed accordingly when demote=true.
func (p *ConsolidationPhase) writeAuditDecision(
	ctx context.Context,
	logger *DreamLogWriter,
	mem *model.Memory,
	meta map[string]interface{},
	reason string,
	demote bool,
) {
	const subPhase = model.DreamSubPhaseBackfillAudit
	if meta == nil {
		meta = map[string]interface{}{}
	}
	beforeConfidence := mem.Confidence

	stampValue := mem.UpdatedAt.UTC()
	if demote {
		stampValue = time.Now().UTC()
	}
	meta[NoveltyAuditStampKey] = stampValue.Format(time.RFC3339Nano)
	meta["novelty_audit_reason"] = reason
	if demote {
		meta["low_novelty"] = true
		meta["low_novelty_reason"] = reason
	}

	encoded, err := encodeStampWrite(mem.Metadata, meta)
	if err != nil {
		slog.Warn("dreaming: audit metadata marshal failed", "memory", mem.ID, "err", err)
		return
	}
	mem.Metadata = encoded

	if !demote {
		if err := p.memWriter.UpdateMetadata(ctx, mem.ID, mem.NamespaceID, encoded); err != nil {
			slog.Warn("dreaming: audit stamp update failed", "memory", mem.ID, "err", err)
		}
		return
	}

	mem.Confidence = 0
	mem.EmbeddingDim = nil
	mem.UpdatedAt = stampValue
	if err := p.memWriter.Demote(ctx, mem.ID, mem.NamespaceID, encoded); err != nil {
		slog.Warn("dreaming: audit update failed", "memory", mem.ID, "demote", true, "err", err)
		return
	}

	// Demoted dreams are excluded from recall via isLowNovelty, so
	// the vector is dead weight in the index.
	p.purgeVector(ctx, mem.ID)
	if logger != nil {
		_ = logger.LogOperation(ctx, model.DreamPhaseConsolidation, subPhase,
			model.DreamOpMemoryDemoted, "memory", mem.ID,
			map[string]interface{}{"confidence": beforeConfidence},
			map[string]interface{}{
				"confidence":  0,
				"low_novelty": true,
				"reason":      reason,
			})
	}
}

// staleSynthesis carries a synthesis alongside its pre-decoded metadata so the
// reinforce stamp path does not have to re-parse on completion.
type staleSynthesis struct {
	mem  model.Memory
	meta map[string]interface{}
}

// collectReinforceStale returns the subset of syntheses whose
// reinforce_checked_at stamp is missing or strictly before their UpdatedAt.
// The byte-level marker check skips the in-memory staleness recheck for any
// synthesis that cannot possibly be fresh; meta is always decoded from
// mem.Metadata so the downstream stamp writer has the row's full field set
// to merge over (UpdateMetadata is a full-column overwrite, not a JSONB
// merge — passing a partial map drops any field not in it).
func collectReinforceStale(syntheses []model.Memory) []staleSynthesis {
	stampMarker := []byte(ReinforceCheckedStampKey)
	stale := make([]staleSynthesis, 0, len(syntheses))
	for i := range syntheses {
		m := syntheses[i]
		meta := decodeMetadata(m.Metadata)
		if !bytes.Contains(m.Metadata, stampMarker) {
			stale = append(stale, staleSynthesis{mem: m, meta: meta})
			continue
		}
		if isReinforceStale(&m, meta) {
			stale = append(stale, staleSynthesis{mem: m, meta: meta})
		}
	}
	return stale
}

// parseStampTime returns the time recorded under key in meta. ok is false
// when the key is absent, the value is non-string/empty, or the timestamp
// fails both RFC3339Nano and RFC3339 parses — in every case the caller
// should treat the row as stale. RFC3339 fallback covers stamps written by
// older versions that did not use the nano variant.
func parseStampTime(meta map[string]interface{}, key string) (time.Time, bool) {
	raw, ok := meta[key]
	if !ok {
		return time.Time{}, false
	}
	s, ok := raw.(string)
	if !ok || s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// isReinforceStale mirrors isStale (contradiction phase) and isParaphraseStale:
// no stamp, malformed stamp, or stamp strictly before UpdatedAt → eligible.
// Equal stamp and UpdatedAt is fresh — the stamp path writes
// mem.UpdatedAt.UTC() through UpdateMetadata, which does not bump updated_at,
// so a just-stamped row reports stamp == updated_at and stays fresh next cycle.
func isReinforceStale(mem *model.Memory, meta map[string]interface{}) bool {
	t, ok := parseStampTime(meta, ReinforceCheckedStampKey)
	if !ok {
		return true
	}
	return t.Before(mem.UpdatedAt)
}

// stampReinforce records the visit stamp anchored to mem.UpdatedAt via
// UpdateMetadata so the staleness check (stamp < UpdatedAt) does not
// self-invalidate next cycle. Mirrors stampParaphrase and the non-demote
// half of writeAuditDecision: persist failures are logged, never returned —
// a failed stamp leaves the row stale so the next cycle retries.
//
// The merge through encodeStampWrite is the defensive backstop: even if a
// caller passes a partial or empty meta map, fields already on the row
// (notably source_memory_ids and dream_cycle_id on fresh syntheses) are
// preserved through the write.
func (p *ConsolidationPhase) stampReinforce(
	ctx context.Context, mem *model.Memory, meta map[string]interface{},
) {
	if meta == nil {
		meta = map[string]interface{}{}
	}
	meta[ReinforceCheckedStampKey] = mem.UpdatedAt.UTC().Format(time.RFC3339Nano)
	encoded, err := encodeStampWrite(mem.Metadata, meta)
	if err != nil {
		slog.Warn("dreaming: reinforce stamp marshal failed", "memory", mem.ID, "err", err)
		return
	}
	mem.Metadata = encoded
	if err := p.memWriter.UpdateMetadata(ctx, mem.ID, mem.NamespaceID, encoded); err != nil {
		slog.Warn("dreaming: reinforce stamp persist failed", "memory", mem.ID, "err", err)
	}
}

// staleCluster carries a cluster alongside its current fingerprint and
// per-member pre-decoded metadata so the consolidate stamp path does not
// re-parse on completion. Parallel to staleSynthesis.
type staleCluster struct {
	members     []model.Memory
	metas       []map[string]interface{}
	fingerprint string
}

// clusterFingerprint returns an order-independent hash of the member ID
// set. clusterMemories' anchor-walk produces non-deterministic iteration
// order across cycles, so the stamp comparison must be sort-invariant.
func clusterFingerprint(cluster []model.Memory) string {
	ids := make([]string, len(cluster))
	for i, m := range cluster {
		ids[i] = m.ID.String()
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "|")))
	return hex.EncodeToString(sum[:8])
}

// collectConsolidateStale returns the eligible-cluster count alongside the
// subset that needs a re-visit. A cluster is stale when ANY member is
// missing the stamp marker, has a stamp before its own UpdatedAt, or
// carries a fingerprint different from the cluster's current fingerprint —
// the last condition catches survivor-only reshapes that timestamp checks
// alone miss.
func collectConsolidateStale(clusters [][]model.Memory) (stale []staleCluster, eligible int) {
	stampMarker := []byte(ConsolidationClusterStampKey)
	stale = make([]staleCluster, 0, len(clusters))
	for _, cluster := range clusters {
		if len(cluster) < 2 {
			continue
		}
		eligible++
		fp := clusterFingerprint(cluster)
		metas := make([]map[string]interface{}, len(cluster))
		clusterStale := false
		for i := range cluster {
			m := &cluster[i]
			metas[i] = decodeMetadata(m.Metadata)
			if !bytes.Contains(m.Metadata, stampMarker) {
				clusterStale = true
				continue
			}
			if isClusterMemberStale(m, metas[i], fp) {
				clusterStale = true
			}
		}
		if clusterStale {
			stale = append(stale, staleCluster{members: cluster, metas: metas, fingerprint: fp})
		}
	}
	return stale, eligible
}

// isClusterMemberStale extends the time-stamp check (see isReinforceStale)
// with a fingerprint check so a survivor-only cluster reshape stales even
// when every member's UpdatedAt is unchanged.
func isClusterMemberStale(mem *model.Memory, meta map[string]interface{}, currentFingerprint string) bool {
	t, ok := parseStampTime(meta, ConsolidationClusterStampKey)
	if !ok || t.Before(mem.UpdatedAt) {
		return true
	}
	fp, _ := meta[ConsolidationClusterFingerprintKey].(string)
	return fp == "" || fp != currentFingerprint
}

// stampConsolidateCluster writes the visit stamp + cluster fingerprint onto
// every cluster member via UpdateMetadata so the staleness check does not
// self-invalidate next cycle. Mirrors stampReinforce / stampParaphrase /
// the non-demote half of writeAuditDecision: persist failures are logged,
// never returned — a failed stamp leaves that row stale so the next cycle
// retries the cluster (which is fine; the cluster's other members will
// still appear stale due to the failed member, surfacing the cluster).
func (p *ConsolidationPhase) stampConsolidateCluster(
	ctx context.Context,
	members []model.Memory,
	metas []map[string]interface{},
	fingerprint string,
) {
	for i := range members {
		meta := metas[i]
		if meta == nil {
			meta = map[string]interface{}{}
		}
		meta[ConsolidationClusterStampKey] = members[i].UpdatedAt.UTC().Format(time.RFC3339Nano)
		meta[ConsolidationClusterFingerprintKey] = fingerprint
		encoded, err := encodeStampWrite(members[i].Metadata, meta)
		if err != nil {
			slog.Warn("dreaming: consolidate cluster stamp marshal failed",
				"memory", members[i].ID, "err", err)
			continue
		}
		members[i].Metadata = encoded
		if err := p.memWriter.UpdateMetadata(ctx, members[i].ID, members[i].NamespaceID, encoded); err != nil {
			slog.Warn("dreaming: consolidate cluster stamp persist failed",
				"memory", members[i].ID, "err", err)
		}
	}
}

// stampConsolidateLoad writes the load-level visit stamp on every memory
// the phase pulled into its candidate pool this cycle. Anchored to each
// row's own UpdatedAt and persisted via UpdateMetadata (which deliberately
// does not bump updated_at), so the staleness check stamp < updated_at
// does not self-invalidate next cycle. Persist failures are logged, never
// returned — a failed stamp leaves that row stale, which simply reschedules
// it for the next cycle. Independent of the per-cluster stamp written by
// stampConsolidateCluster: a memory may participate in clustering and
// receive both stamps, or end up unclustered and receive only this one.
func (p *ConsolidationPhase) stampConsolidateLoad(ctx context.Context, members []model.Memory) {
	for i := range members {
		mem := &members[i]
		if mem.DeletedAt != nil {
			continue
		}
		meta := decodeMetadata(mem.Metadata)
		meta[ConsolidationLoadCheckedStampKey] = mem.UpdatedAt.UTC().Format(time.RFC3339Nano)
		encoded, err := json.Marshal(meta)
		if err != nil {
			slog.Warn("dreaming: consolidate load stamp marshal failed",
				"memory", mem.ID, "err", err)
			continue
		}
		mem.Metadata = encoded
		if err := p.memWriter.UpdateMetadata(ctx, mem.ID, mem.NamespaceID, encoded); err != nil {
			slog.Warn("dreaming: consolidate load stamp persist failed",
				"memory", mem.ID, "err", err)
		}
	}
}

// decodeMetadata returns a mutable map from the raw JSON metadata, or an empty
// map if the bytes are missing or unparseable.
func decodeMetadata(raw json.RawMessage) map[string]interface{} {
	if len(raw) == 0 {
		return map[string]interface{}{}
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return map[string]interface{}{}
	}
	return m
}

// encodeStampWrite returns the bytes of mem.Metadata with the caller's
// updates merged on top. UpdateMetadata is a full-column overwrite (see
// storage/memory_repo.go UpdateMetadata) — not a JSONB merge — so a stamp
// writer that marshals only its own keys silently drops every other field
// on the row. This helper makes the writer the safety net: even if the
// caller's `updates` map is empty or partial, every on-disk field survives.
//
// Precedence: caller's `updates` keys win over on-disk keys (the caller is
// the one mutating state). To delete a field, the caller must explicitly
// set it to nil in `updates` — missing keys are preserved as-is.
func encodeStampWrite(onDisk json.RawMessage, updates map[string]interface{}) ([]byte, error) {
	merged := decodeMetadata(onDisk)
	for k, v := range updates {
		merged[k] = v
	}
	return json.Marshal(merged)
}

// resolveSourceMemoryIDs returns the source memory IDs for a synthesis,
// preferring metadata.source_memory_ids and falling back to the
// memory_lineage table (relation = synthesized_from) when metadata is
// missing or empty. When the lineage fallback fires, the recovered IDs
// are also merged back into the row's metadata so the next cycle takes
// the fast path — this is the runtime self-heal that complements the
// historical-damage migration.
//
// meta is mutated in place to mirror the metadata write so callers that
// reuse the map further down the cycle see the recovered IDs without
// re-decoding. Returns nil with nil error when neither source has any
// parents (true orphan — caller should demote).
func (p *ConsolidationPhase) resolveSourceMemoryIDs(
	ctx context.Context,
	mem *model.Memory,
	meta map[string]interface{},
) []uuid.UUID {
	if ids := extractSourceMemoryIDs(meta); len(ids) > 0 {
		return ids
	}
	if p.lineage == nil {
		return nil
	}
	parents, err := p.lineage.FindParentIDsByRelation(ctx, mem.NamespaceID, mem.ID, model.LineageSynthesizedFrom)
	if err != nil {
		slog.Warn("dreaming: lineage parent lookup failed",
			"memory", mem.ID, "err", err)
		return nil
	}
	if len(parents) == 0 {
		return nil
	}

	parentStrs := make([]string, len(parents))
	for i, parent := range parents {
		parentStrs[i] = parent.String()
	}
	updates := map[string]interface{}{
		model.DreamMetaSourceMemoryIDs: parentStrs,
	}
	encoded, encErr := encodeStampWrite(mem.Metadata, updates)
	if encErr != nil {
		slog.Warn("dreaming: lineage source self-heal marshal failed",
			"memory", mem.ID, "err", encErr)
		return parents
	}
	if persistErr := p.memWriter.UpdateMetadata(ctx, mem.ID, mem.NamespaceID, encoded); persistErr != nil {
		slog.Warn("dreaming: lineage source self-heal persist failed",
			"memory", mem.ID, "err", persistErr)
		return parents
	}
	mem.Metadata = encoded
	for k, v := range updates {
		meta[k] = v
	}
	slog.Info("dreaming: source_memory_ids recovered from lineage",
		"memory", mem.ID, "parents", len(parents))
	return parents
}

// extractSourceMemoryIDs pulls the source_memory_ids array out of a decoded
// metadata map and returns it as parsed UUIDs. Skips entries that are not
// strings or do not parse as UUIDs.
func extractSourceMemoryIDs(meta map[string]interface{}) []uuid.UUID {
	raw, ok := meta["source_memory_ids"]
	if !ok {
		return nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := make([]uuid.UUID, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			continue
		}
		id, err := uuid.Parse(s)
		if err != nil {
			continue
		}
		out = append(out, id)
	}
	return out
}

// consolidate clusters related memories and creates synthesis memories.
// Returns residual=true when the sub-phase left eligible clusters
// unvisited (typically because the sub-slice budget was exhausted).
func (p *ConsolidationPhase) consolidate(
	ctx context.Context,
	cycle *model.DreamCycle,
	budget *TokenBudget,
	logger *DreamLogWriter,
	llm provider.LLMProvider,
	allMemories []model.Memory,
) (bool, error) {
	const subPhase = model.DreamSubPhaseConsolidate
	// Filter to non-deleted, non-dream, non-superseded memories.
	var candidates []model.Memory
	for _, m := range allMemories {
		if m.DeletedAt != nil || m.SupersededBy != nil {
			continue
		}
		src := model.MemorySource(&m)
		if src == model.DreamSource {
			continue
		}
		candidates = append(candidates, m)
	}

	stats := map[string]interface{}{
		"sub_phase":              "consolidate",
		"candidates_total":       len(candidates),
		"clusters_total":         0,
		"clusters_eligible":      0,
		"clusters_stale":         0,
		"synthesis_calls":        0,
		"audit_calls":            0,
		"created":                0,
		"rejected":               0,
		"skipped_budget":         0,
		"errors_synth":           0,
		"errors_audit":           0,
		"errors_create":          0,
		"embedding_calls":        0,
		"embedding_tokens_spent": 0,
	}
	tokensBefore := budget.Used()

	if len(candidates) < 3 {
		slog.Info("dreaming: consolidate starting (insufficient candidates)",
			"cycle", cycle.ID, "candidates", len(candidates),
			"budget_remaining", budget.Remaining())
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return false, nil
	}

	// Simple clustering: group by overlapping content (batches of related memories).
	clusterOverlap := p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamConsolidationClusterOverlapThreshold, "global")
	clusters := p.clusterMemories(candidates, clusterOverlap)
	stale, eligibleClusters := collectConsolidateStale(clusters)
	stats["clusters_total"] = len(clusters)
	stats["clusters_eligible"] = eligibleClusters
	stats["clusters_stale"] = len(stale)

	slog.Info("dreaming: consolidate starting",
		"cycle", cycle.ID, "candidates", len(candidates),
		"clusters", len(clusters), "eligible_clusters", eligibleClusters,
		"stale_clusters", len(stale), "budget_remaining", budget.Remaining())

	initialConfidence := p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamInitialConfidence, "global")
	synthesisTemperature := p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamSynthesisTemperature, "global")

	synthesisPromptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamSynthesisPrompt, "global")
	noveltyEnabled := p.settings.ResolveBool(ctx, service.SettingDreamNoveltyEnabled, "global")

	clustersVisited := 0
	for si := range stale {
		cluster := stale[si].members
		metas := stale[si].metas
		fingerprint := stale[si].fingerprint
		if budget.Exhausted() {
			stats["skipped_budget"] = stats["skipped_budget"].(int) + 1
			break
		}
		clustersVisited++

		prompt := renderSynthesisPrompt(synthesisPromptTemplate, cluster)
		estCost := EstimateTokens(prompt) + budget.PerCallCap()
		if !budget.CanAfford(estCost) {
			slog.Info("dreaming: synthesis call skipped (estimated cost exceeds remaining budget)",
				"cycle", cycle.ID, "cluster_size", len(cluster),
				"cluster", clustersVisited, "of", len(stale),
				"estimate", estCost, "budget_remaining", budget.Remaining())
			stats["skipped_budget"] = stats["skipped_budget"].(int) + 1
			break
		}

		synthStart := time.Now()
		synthesisContent, usage, err := p.synthesize(ctx, llm, prompt, budget, synthesisTemperature)
		synthTokens := 0
		if usage != nil {
			synthTokens = usage.TotalTokens
		}
		stats["synthesis_calls"] = stats["synthesis_calls"].(int) + 1
		slog.Info("dreaming: synthesis call",
			"cycle", cycle.ID, "cluster_size", len(cluster),
			"cluster", clustersVisited, "of", len(stale),
			"latency_ms", time.Since(synthStart).Milliseconds(),
			"tokens", synthTokens,
			"budget_remaining", budget.Remaining())

		if errors.Is(err, ErrBudgetExhausted) {
			slog.Info("dreaming: synthesis loop stopped on budget exhaustion",
				"cycle", cycle.ID, "cluster_size", len(cluster),
				"cluster", clustersVisited, "of", len(stale),
				"tokens", synthTokens,
				"budget_remaining", budget.Remaining())
			break
		}

		if err != nil {
			slog.Warn("dreaming: synthesis failed", "err", err)
			stats["errors_synth"] = stats["errors_synth"].(int) + 1
			continue
		}

		if synthesisContent == "" {
			continue
		}

		// Novelty audit: drop the synthesis if it does not contain at least
		// one fact absent from the source cluster. The audit charges its own
		// LLM usage against the dream budget when the borderline judge fires.
		if noveltyEnabled {
			auditStart := time.Now()
			passed, reason, auditUsage, embedTokens, auditErr := p.auditNovelty(ctx, llm, budget, synthesisContent, cluster, 0, provider.OperationDreamNoveltyAudit)
			llmTokens := 0
			if auditUsage != nil {
				llmTokens = auditUsage.TotalTokens
			}
			stats["audit_calls"] = stats["audit_calls"].(int) + 1
			if embedTokens > 0 {
				stats["embedding_calls"] = stats["embedding_calls"].(int) + 1
				stats["embedding_tokens_spent"] = stats["embedding_tokens_spent"].(int) + embedTokens
			}
			slog.Info("dreaming: synthesis novelty audit",
				"cycle", cycle.ID,
				"cluster", clustersVisited, "of", len(stale),
				"reason", reason, "passed", passed,
				"latency_ms", time.Since(auditStart).Milliseconds(),
				"embed_tokens", embedTokens, "llm_tokens", llmTokens,
				"budget_remaining", budget.Remaining())

			if errors.Is(auditErr, ErrBudgetExhausted) {
				slog.Info("dreaming: synthesis audit loop stopped on budget exhaustion",
					"cycle", cycle.ID,
					"cluster", clustersVisited, "of", len(stale),
					"budget_remaining", budget.Remaining())
				break
			}
			if auditErr != nil {
				slog.Warn("dreaming: novelty audit error",
					"err", auditErr, "reason", reason)
				stats["errors_audit"] = stats["errors_audit"].(int) + 1
				continue
			}
			if !passed {
				rejectedSources := make([]string, len(cluster))
				for i, m := range cluster {
					rejectedSources[i] = m.ID.String()
				}
				_ = logger.LogOperation(ctx, model.DreamPhaseConsolidation, subPhase,
					model.DreamOpMemoryRejected, "memory", uuid.Nil,
					nil,
					map[string]interface{}{
						"reason":            reason,
						"source_memory_ids": rejectedSources,
					})
				slog.Info("dreaming: synthesis rejected by novelty audit",
					"cycle", cycle.ID,
					"cluster", clustersVisited, "of", len(stale),
					"reason", reason, "sources", len(cluster),
					"budget_remaining", budget.Remaining())
				stats["rejected"] = stats["rejected"].(int) + 1
				p.stampConsolidateCluster(ctx, cluster, metas, fingerprint)
				continue
			}
		}

		// Collect source memory IDs.
		sourceIDs := make([]string, len(cluster))
		for i, m := range cluster {
			sourceIDs[i] = m.ID.String()
		}

		metadata, _ := json.Marshal(map[string]interface{}{
			model.DreamMetaCycleID:    cycle.ID.String(),
			model.DreamMetaSourceMemoryIDs: sourceIDs,
		})

		source := model.DreamSource
		synthMemory := &model.Memory{
			ID:          uuid.New(),
			NamespaceID: cycle.NamespaceID,
			Content:     synthesisContent,
			Source:      &source,
			Confidence:  initialConfidence,
			Importance:  0.5,
			Enriched:    true,
			Metadata:    metadata,
		}

		if err := p.memWriter.Create(ctx, synthMemory); err != nil {
			slog.Warn("dreaming: synthesis memory creation failed", "err", err)
			stats["errors_create"] = stats["errors_create"].(int) + 1
			continue
		}

		// Create lineage entries linking synthesis to sources.
		for _, srcMem := range cluster {
			parentID := srcMem.ID
			_ = p.lineage.Create(ctx, &model.MemoryLineage{
				ID:          uuid.New(),
				NamespaceID: cycle.NamespaceID,
				MemoryID:    synthMemory.ID,
				ParentID:    &parentID,
				Relation:    model.LineageSynthesizedFrom,
			})
		}

		stats["created"] = stats["created"].(int) + 1
		// Log the operation.
		_ = logger.LogOperation(ctx, model.DreamPhaseConsolidation, subPhase,
			model.DreamOpMemoryCreated, "memory", synthMemory.ID,
			nil, synthMemory)
		p.stampConsolidateCluster(ctx, cluster, metas, fingerprint)
	}

	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
	return clustersVisited < len(stale), nil
}

// renderSynthesisPrompt builds the synthesis prompt so it can be inspected
// for budget estimation before the LLM call.
func renderSynthesisPrompt(template string, cluster []model.Memory) string {
	contents := make([]string, 0, len(cluster))
	for _, m := range cluster {
		contents = append(contents, m.Content)
	}
	return fmt.Sprintf(template, strings.Join(contents, "\n---\n"))
}

// synthesize asks the LLM to produce a consolidated summary from a cluster.
func (p *ConsolidationPhase) synthesize(
	ctx context.Context,
	llm provider.LLMProvider,
	prompt string,
	budget *TokenBudget,
	temperature float64,
) (string, *provider.TokenUsage, error) {
	resp, usage, err := WrapLLMCall(ctx, budget, OpSynthesis, llm.Name(), "",
		func(ctx context.Context) (*provider.CompletionResponse, *provider.TokenUsage, error) {
			ctx = provider.WithOperation(ctx, provider.OperationDreamSynthesis)
			r, e := llm.Complete(ctx, &provider.CompletionRequest{
				Messages: []provider.Message{
					{Role: "user", Content: prompt},
				},
				MaxTokens:   budget.PerCallCap(),
				Temperature: temperature,
			})
			return r, usageOrEstimateLLM(r, prompt, budget, llm.Name(), model.DreamPhaseConsolidation), e
		})
	if err != nil {
		return "", usage, err
	}
	return strings.TrimSpace(resp.Content), usage, nil
}

// auditNovelty checks whether a candidate synthesis contains at least one
// fact not present in any of its source memories. Hybrid path:
//   - Compute max cosine similarity between candidate and source embeddings.
//   - maxSim >= high threshold ⇒ reject (clearly duplicative).
//   - maxSim <= low threshold  ⇒ accept (clearly novel).
//   - otherwise call the LLM judge and trust its JSON verdict.
//
// embedHighOverride > 0 substitutes for SettingDreamNoveltyEmbedHighThreshold
// (the backfill path passes SettingDreamNoveltyBackfillEmbedHighThreshold to
// pull the auto-reject cut earlier than synthesis-time auditing).
//
// Failure modes are closed: on embedding error or judge JSON parse failure
// the audit returns (false, ...). usage may be non-nil even on failure when
// the LLM call already happened, so callers must spend it before handling
// err. embedTokens is reported on every path that performed the embedding
// call so callers can charge it to the dream budget.
func (p *ConsolidationPhase) auditNovelty(
	ctx context.Context,
	llm provider.LLMProvider,
	budget *TokenBudget,
	candidate string,
	sources []model.Memory,
	embedHighOverride float64,
	llmOperation provider.Operation,
) (passed bool, reason string, usage *provider.TokenUsage, embedTokens int, err error) {
	if len(sources) == 0 {
		return false, "no_sources", nil, 0, nil
	}

	// embedHighOverride is the backfill-path's more aggressive threshold;
	// when the caller leaves it 0 (synthesis-time path), fall back to the
	// general novelty.embed_high_threshold setting via the registry default.
	high := embedHighOverride
	if high <= 0 {
		high = p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamNoveltyEmbedHighThreshold, "global")
	}
	low := p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamNoveltyEmbedLowThreshold, "global")

	var embedder provider.EmbeddingProvider
	if p.embedderProvider != nil {
		embedder = p.embedderProvider()
	}

	if embedder != nil {
		// Batch embed candidate + every source in one call so we pay one
		// network round-trip per audit instead of N+1.
		inputs := make([]string, 0, len(sources)+1)
		inputs = append(inputs, candidate)
		for _, s := range sources {
			inputs = append(inputs, s.Content)
		}
		resp, embUsage, embErr := WrapLLMCall(ctx, budget, OpNoveltyAuditEmbed,
			embedder.Name(), "",
			func(ctx context.Context) (*provider.EmbeddingResponse, *provider.TokenUsage, error) {
				ctx = provider.WithOperation(ctx, provider.OperationDreamNoveltyEmbedding)
				r, e := embedder.Embed(ctx, &provider.EmbeddingRequest{
					Input:     inputs,
					Dimension: storage.BestEmbeddingDimension(embedder.Dimensions()),
				})
				return r, usageOrEstimateEmbed(r, inputs), e
			})
		if embErr != nil || resp == nil || len(resp.Embeddings) != len(inputs) {
			return false, "embed_error", nil, 0, embErr
		}
		if embUsage != nil {
			embedTokens = embUsage.TotalTokens
		}
		candEmb := resp.Embeddings[0]
		maxSim := 0.0
		for i := 1; i < len(resp.Embeddings); i++ {
			sim := hnsw.CosineSimilarity(candEmb, resp.Embeddings[i])
			if sim > maxSim {
				maxSim = sim
			}
		}
		if maxSim >= high {
			return false, "embed_high_sim", nil, embedTokens, nil
		}
		if maxSim <= low {
			return true, "embed_low_sim", nil, embedTokens, nil
		}
		// Borderline ⇒ fall through to the LLM judge.
	}

	promptTpl, _ := p.settings.Resolve(ctx, service.SettingDreamNoveltyJudgePrompt, "global")
	if promptTpl == "" {
		// Without a judge prompt we cannot adjudicate borderline cases.
		// Fail closed when the embedder pre-filter did not already decide.
		return false, "no_judge_prompt", nil, embedTokens, nil
	}

	sourceTexts := make([]string, 0, len(sources))
	for _, s := range sources {
		sourceTexts = append(sourceTexts, s.Content)
	}
	prompt := fmt.Sprintf(promptTpl, candidate, strings.Join(sourceTexts, "\n---\n"))

	maxTokens := p.settings.ResolveIntWithDefault(ctx, service.SettingDreamNoveltyJudgeMaxTokens, "global")

	if llmOperation == "" {
		llmOperation = provider.OperationDreamNoveltyAudit
	}
	noveltyTemperature := p.settings.ResolveFloatWithDefault(ctx, service.SettingDreamNoveltyJudgeTemperature, "global")
	resp, judgeUsage, err := WrapLLMCall(ctx, budget, OpNoveltyAuditLLM, llm.Name(), "",
		func(ctx context.Context) (*provider.CompletionResponse, *provider.TokenUsage, error) {
			r, e := llm.Complete(provider.WithOperation(ctx, llmOperation), &provider.CompletionRequest{
				Messages: []provider.Message{
					{Role: "user", Content: prompt},
				},
				MaxTokens:   maxTokens,
				Temperature: noveltyTemperature,
				JSONMode:    true,
			})
			return r, usageOrEstimateLLM(r, prompt, budget, llm.Name(), model.DreamPhaseConsolidation), e
		})
	if err != nil {
		return false, "judge_call_error", judgeUsage, embedTokens, err
	}

	var parsed struct {
		NovelFacts []string `json:"novel_facts"`
	}
	if jerr := json.Unmarshal([]byte(strings.TrimSpace(resp.Content)), &parsed); jerr != nil {
		return false, "judge_parse_error", judgeUsage, embedTokens, nil
	}
	return len(parsed.NovelFacts) > 0, "llm_judge", judgeUsage, embedTokens, nil
}

// clusterMemories groups related memories using simple content overlap.
// Each memory appears in at most one cluster. overlapThreshold is the
// word-overlap fraction at or above which two memories are placed in the
// same cluster.
func (p *ConsolidationPhase) clusterMemories(memories []model.Memory, overlapThreshold float64) [][]model.Memory {
	if len(memories) == 0 {
		return nil
	}

	// Pre-compute word sets to avoid redundant extraction.
	wordSets := make(map[uuid.UUID]map[string]bool, len(memories))
	for _, m := range memories {
		wordSets[m.ID] = extractWords(m.Content)
	}

	assigned := make(map[uuid.UUID]bool)
	var clusters [][]model.Memory

	for i, anchor := range memories {
		if assigned[anchor.ID] {
			continue
		}

		cluster := []model.Memory{anchor}
		assigned[anchor.ID] = true

		anchorWords := wordSets[anchor.ID]

		for j := i + 1; j < len(memories); j++ {
			candidate := memories[j]
			if assigned[candidate.ID] {
				continue
			}

			overlap := wordOverlap(anchorWords, wordSets[candidate.ID])

			if overlap >= overlapThreshold {
				cluster = append(cluster, candidate)
				assigned[candidate.ID] = true
			}
		}

		clusters = append(clusters, cluster)
	}

	return clusters
}

func extractWords(s string) map[string]bool {
	words := make(map[string]bool)
	for _, w := range strings.Fields(strings.ToLower(s)) {
		if len(w) > 3 {
			words[w] = true
		}
	}
	return words
}

func wordOverlap(a, b map[string]bool) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	overlap := 0
	for w := range a {
		if b[w] {
			overlap++
		}
	}
	smaller := len(a)
	if len(b) < smaller {
		smaller = len(b)
	}
	return float64(overlap) / float64(smaller)
}

func sampleMemories(memories []model.Memory, n int) []model.Memory {
	if len(memories) <= n {
		return memories
	}
	// Take the first n (most recent by default from ListByNamespace ordering).
	return memories[:n]
}

// writePhaseSummary emits a slog.Info line and a DreamOpPhaseSummary dream_log
// row for a consolidation sub-phase. tokens_spent is computed as the delta of
// budget.Used() across the sub-phase so it captures every call (LLM and
// embedding) even if a specific counter was missed.
func (p *ConsolidationPhase) writePhaseSummary(
	ctx context.Context,
	logger *DreamLogWriter,
	stats map[string]interface{},
	budget *TokenBudget,
	tokensBefore int,
) {
	stats["tokens_spent"] = budget.Used() - tokensBefore
	stats["budget_remaining"] = budget.Remaining()

	subPhase, _ := stats["sub_phase"].(string)
	args := make([]any, 0, len(stats)*2)
	for k, v := range stats {
		args = append(args, k, v)
	}
	slog.Info("dreaming: "+subPhase+" complete", args...)

	if logger == nil {
		return
	}
	if err := logger.LogOperation(ctx, model.DreamPhaseConsolidation, subPhase,
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats); err != nil {
		slog.Warn("dreaming: log phase summary failed",
			"sub_phase", subPhase, "err", err)
	}
}
