package dreaming

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

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
	tokens           TokenRecorder
	usageCtx         UsageContextResolver
}

// NewConsolidationPhase creates a new consolidation and reinforcement phase.
// embedderProvider may be nil; when nil, the novelty audit degrades to LLM-only
// judgement (every borderline call), and pre-write audits will fail closed if
// the embedding provider is unavailable.
func NewConsolidationPhase(
	memories MemoryReader,
	memWriter MemoryWriter,
	lineage LineageWriter,
	llmProvider LLMProviderFunc,
	embedderProvider EmbeddingProviderFunc,
	settings SettingsResolver,
	tokens TokenRecorder,
	usageCtx UsageContextResolver,
) *ConsolidationPhase {
	return &ConsolidationPhase{
		memories:         memories,
		memWriter:        memWriter,
		lineage:          lineage,
		llmProvider:      llmProvider,
		embedderProvider: embedderProvider,
		settings:         settings,
		tokens:           tokens,
		usageCtx:         usageCtx,
	}
}

func (p *ConsolidationPhase) Name() string { return model.DreamPhaseConsolidation }

func (p *ConsolidationPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) error {
	llm := p.llmProvider()
	if llm == nil {
		slog.Info("dreaming: no LLM provider for consolidation, skipping")
		return nil
	}

	// Load all memories once for both sub-phases.
	allMemories, err := p.memories.ListByNamespace(ctx, cycle.NamespaceID, 1000, 0)
	if err != nil {
		return err
	}

	// Run reinforcement first (evaluates existing syntheses).
	if err := p.reinforce(ctx, cycle, budget, logger, llm, allMemories); err != nil {
		slog.Warn("dreaming: reinforcement sub-phase had errors", "err", err)
	}

	// Backfill the novelty audit on historical dream memories. Bounded per
	// cycle by SettingDreamNoveltyBackfillPerCycle and resumable across
	// cycles via the metadata.novelty_audited_at marker, so a large backlog
	// drains incrementally without shocking the per-cycle token budget.
	if !budget.Exhausted() {
		if err := p.auditExistingDreams(ctx, cycle, budget, logger, llm, allMemories); err != nil {
			slog.Warn("dreaming: backfill audit had errors", "err", err)
		}
	}

	// Then consolidation (creates new syntheses).
	if !budget.Exhausted() {
		if err := p.consolidate(ctx, cycle, budget, logger, llm, allMemories); err != nil {
			return err
		}
	}

	return nil
}

// reinforce evaluates existing dream-originated synthesis memories against
// new evidence, adjusting confidence proportionally.
func (p *ConsolidationPhase) reinforce(
	ctx context.Context,
	cycle *model.DreamCycle,
	budget *TokenBudget,
	logger *DreamLogWriter,
	llm provider.LLMProvider,
	allMemories []model.Memory,
) error {
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

	if len(syntheses) == 0 || len(userMemories) == 0 {
		return nil
	}

	initialConfidence, _ := p.settings.ResolveFloat(ctx, service.SettingDreamInitialConfidence, "global")
	if initialConfidence == 0 {
		initialConfidence = 0.3
	}
	supersessionThreshold, _ := p.settings.ResolveFloat(ctx, service.SettingDreamSupersessionThreshold, "global")
	if supersessionThreshold == 0 {
		supersessionThreshold = 0.85
	}

	alignmentPromptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamAlignmentPrompt, "global")

	for _, synthesis := range syntheses {
		if budget.Exhausted() {
			break
		}

		// Get a sample of user memories to evaluate against.
		sample := sampleMemories(userMemories, 5)
		if len(sample) == 0 {
			continue
		}

		// Pre-flight budget check using the same prompt we'll send.
		prompt := renderAlignmentPrompt(alignmentPromptTemplate, &synthesis, sample)
		estCost := EstimateTokens(prompt) + budget.PerCallCap()
		if budget.Remaining() < estCost {
			slog.Info("dreaming: alignment call skipped (estimated cost exceeds remaining budget)",
				"estimate", estCost, "remaining", budget.Remaining())
			break
		}

		alignment, usage, err := p.scoreAlignment(ctx, llm, prompt, budget)

		// Account for usage before handling the error. scoreAlignment returns
		// non-nil usage on parse errors too (the LLM call already happened).
		var spendErr error
		if usage != nil {
			spendErr = budget.Spend(usage.TotalTokens)
			p.record(ctx, llm, cycle, "dream_alignment_scoring", &synthesis.ID, usage)
		}

		if err != nil {
			slog.Warn("dreaming: alignment scoring failed", "synthesis", synthesis.ID, "err", err)
			if spendErr != nil {
				break
			}
			continue
		}

		if spendErr != nil {
			break
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
			continue
		}

		// Write first, then log — ensures log matches actual state.
		synthesis.Confidence = newConfidence
		synthesis.UpdatedAt = time.Now().UTC()
		if err := p.memWriter.Update(ctx, &synthesis); err != nil {
			slog.Warn("dreaming: confidence update failed", "err", err)
			continue
		}

		if err := logger.LogOperation(ctx, model.DreamPhaseConsolidation,
			model.DreamOpConfidenceAdjusted, "memory", synthesis.ID,
			map[string]interface{}{"confidence": oldConfidence},
			map[string]interface{}{"confidence": newConfidence, "alignment": alignment}); err != nil {
			slog.Warn("dreaming: log confidence adjustment failed", "err", err)
		}

		// Check supersession threshold.
		if newConfidence >= supersessionThreshold {
			p.supersedeOriginals(ctx, cycle, &synthesis, logger)
		}
	}

	return nil
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
	prompt string,
	budget *TokenBudget,
) (float64, *provider.TokenUsage, error) {
	resp, err := llm.Complete(ctx, &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   budget.PerCallCap(),
		Temperature: 0.1,
		JSONMode:    true,
	})
	if err != nil {
		return 0, nil, err
	}

	usage := resp.Usage
	if usage.TotalTokens == 0 {
		if budget.MarkZeroUsageWarned() {
			slog.Warn("dreaming: provider returned zero token usage; estimating from prompt/response length",
				"provider", llm.Name(), "phase", model.DreamPhaseConsolidation)
		}
		usage.PromptTokens = EstimateTokens(prompt)
		usage.CompletionTokens = EstimateTokens(resp.Content)
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	}

	var result struct {
		Alignment float64 `json:"alignment"`
		Reasoning string  `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(resp.Content)), &result); err != nil {
		return 0, &usage, fmt.Errorf("parse alignment response: %w", err)
	}

	// Clamp to [-1, 1].
	if result.Alignment < -1 {
		result.Alignment = -1
	}
	if result.Alignment > 1 {
		result.Alignment = 1
	}

	return result.Alignment, &usage, nil
}

// record persists a token usage row for a consolidation-phase LLM call.
func (p *ConsolidationPhase) record(
	ctx context.Context,
	llm provider.LLMProvider,
	cycle *model.DreamCycle,
	operation string,
	memoryID *uuid.UUID,
	usage *provider.TokenUsage,
) {
	if p.tokens == nil || usage == nil {
		return
	}
	rec := &model.TokenUsage{
		ID:           uuid.New(),
		NamespaceID:  cycle.NamespaceID,
		Operation:    operation,
		Provider:     llm.Name(),
		TokensInput:  usage.PromptTokens,
		TokensOutput: usage.CompletionTokens,
		MemoryID:     memoryID,
		CreatedAt:    time.Now().UTC(),
	}
	if p.usageCtx != nil {
		if uc, err := p.usageCtx.ResolveUsageContext(ctx, cycle.NamespaceID); err == nil && uc != nil {
			rec.OrgID = uc.OrgID
			rec.UserID = uc.UserID
			rec.ProjectID = uc.ProjectID
		}
	}
	if err := p.tokens.Record(ctx, rec); err != nil {
		slog.Warn("dreaming: token usage record failed", "phase", model.DreamPhaseConsolidation, "operation", operation, "err", err)
	}
}

// supersedeOriginals marks the source memories of a synthesis as superseded
// once the synthesis has reached sufficient confidence.
func (p *ConsolidationPhase) supersedeOriginals(
	ctx context.Context,
	cycle *model.DreamCycle,
	synthesis *model.Memory,
	logger *DreamLogWriter,
) {
	for _, memID := range extractSourceMemoryIDs(decodeMetadata(synthesis.Metadata)) {
		original, err := p.memories.GetByID(ctx, memID)
		if err != nil || original.SupersededBy != nil {
			continue
		}

		original.SupersededBy = &synthesis.ID
		original.UpdatedAt = time.Now().UTC()
		if err := p.memWriter.Update(ctx, original); err != nil {
			slog.Warn("dreaming: supersession update failed", "memory", memID, "err", err)
			continue
		}

		if err := logger.LogOperation(ctx, model.DreamPhaseConsolidation,
			model.DreamOpMemorySuperseded, "memory", memID,
			map[string]interface{}{"superseded_by": nil},
			map[string]interface{}{"superseded_by": synthesis.ID.String()}); err != nil {
			slog.Warn("dreaming: log supersession failed", "err", err)
		}
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
func (p *ConsolidationPhase) auditExistingDreams(
	ctx context.Context,
	cycle *model.DreamCycle,
	budget *TokenBudget,
	logger *DreamLogWriter,
	llm provider.LLMProvider,
	allMemories []model.Memory,
) error {
	if !p.settings.ResolveBool(ctx, service.SettingDreamNoveltyEnabled, "global") {
		return nil
	}

	perCycleCap, _ := p.settings.ResolveInt(ctx, service.SettingDreamNoveltyBackfillPerCycle, "global")
	if perCycleCap <= 0 {
		return nil
	}

	processed := 0
	for i := range allMemories {
		if processed >= perCycleCap {
			break
		}
		if budget.Exhausted() {
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
		if _, alreadyAudited := meta["novelty_audited_at"]; alreadyAudited {
			continue
		}

		processed++

		sourceIDs := extractSourceMemoryIDs(meta)
		if len(sourceIDs) == 0 {
			// Orphan synthesis: no recoverable lineage to compare against.
			p.demoteDream(ctx, logger, &mem, meta, "orphan_no_sources")
			continue
		}

		fetched, err := p.memories.GetBatch(ctx, sourceIDs)
		if err != nil {
			slog.Warn("dreaming: backfill source fetch failed",
				"memory", mem.ID, "err", err)
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
			continue
		}

		passed, reason, auditUsage, auditErr := p.auditNovelty(ctx, llm, mem.Content, sources)
		if auditUsage != nil {
			_ = budget.Spend(auditUsage.TotalTokens)
			p.record(ctx, llm, cycle, "dream_novelty_backfill", &mem.ID, auditUsage)
		}
		if auditErr != nil {
			slog.Warn("dreaming: backfill novelty audit error",
				"memory", mem.ID, "err", auditErr, "reason", reason)
			// Do not stamp on hard error; let a later cycle retry.
			continue
		}

		if passed {
			p.stampAudited(ctx, &mem, meta, reason)
			continue
		}

		p.demoteDream(ctx, logger, &mem, meta, reason)
	}

	return nil
}

// stampAudited records that a dream memory passed the novelty audit so
// future cycles skip it. Mutates only the metadata field.
func (p *ConsolidationPhase) stampAudited(ctx context.Context, mem *model.Memory, meta map[string]interface{}, reason string) {
	p.writeAuditDecision(ctx, nil, mem, meta, reason, false)
}

// demoteDream zeroes Confidence and stamps low_novelty so the recall service
// excludes the row from competitive ranking. Logs DreamOpMemoryDemoted.
func (p *ConsolidationPhase) demoteDream(ctx context.Context, logger *DreamLogWriter, mem *model.Memory, meta map[string]interface{}, reason string) {
	p.writeAuditDecision(ctx, logger, mem, meta, reason, true)
}

// writeAuditDecision is the shared write path for stampAudited and demoteDream.
// When demote is true, Confidence is zeroed and low_novelty markers are added;
// when logger is non-nil and demote fired, DreamOpMemoryDemoted is recorded.
func (p *ConsolidationPhase) writeAuditDecision(
	ctx context.Context,
	logger *DreamLogWriter,
	mem *model.Memory,
	meta map[string]interface{},
	reason string,
	demote bool,
) {
	if meta == nil {
		meta = map[string]interface{}{}
	}
	beforeConfidence := mem.Confidence

	now := time.Now().UTC()
	meta["novelty_audited_at"] = now.Format(time.RFC3339)
	meta["novelty_audit_reason"] = reason
	if demote {
		meta["low_novelty"] = true
		meta["low_novelty_reason"] = reason
	}

	encoded, err := json.Marshal(meta)
	if err != nil {
		slog.Warn("dreaming: audit metadata marshal failed", "memory", mem.ID, "err", err)
		return
	}
	mem.Metadata = encoded
	if demote {
		mem.Confidence = 0
	}
	mem.UpdatedAt = now
	if err := p.memWriter.Update(ctx, mem); err != nil {
		slog.Warn("dreaming: audit update failed", "memory", mem.ID, "demote", demote, "err", err)
		return
	}

	if demote && logger != nil {
		_ = logger.LogOperation(ctx, model.DreamPhaseConsolidation,
			model.DreamOpMemoryDemoted, "memory", mem.ID,
			map[string]interface{}{"confidence": beforeConfidence},
			map[string]interface{}{
				"confidence":  0,
				"low_novelty": true,
				"reason":      reason,
			})
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
func (p *ConsolidationPhase) consolidate(
	ctx context.Context,
	cycle *model.DreamCycle,
	budget *TokenBudget,
	logger *DreamLogWriter,
	llm provider.LLMProvider,
	allMemories []model.Memory,
) error {
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

	if len(candidates) < 3 {
		return nil
	}

	// Simple clustering: group by overlapping content (batches of related memories).
	clusters := p.clusterMemories(candidates)

	initialConfidence, _ := p.settings.ResolveFloat(ctx, service.SettingDreamInitialConfidence, "global")
	if initialConfidence == 0 {
		initialConfidence = 0.3
	}

	synthesisPromptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamSynthesisPrompt, "global")
	noveltyEnabled := p.settings.ResolveBool(ctx, service.SettingDreamNoveltyEnabled, "global")

	for _, cluster := range clusters {
		if budget.Exhausted() {
			break
		}

		if len(cluster) < 2 {
			continue
		}

		prompt := renderSynthesisPrompt(synthesisPromptTemplate, cluster)
		estCost := EstimateTokens(prompt) + budget.PerCallCap()
		if budget.Remaining() < estCost {
			slog.Info("dreaming: synthesis call skipped (estimated cost exceeds remaining budget)",
				"estimate", estCost, "remaining", budget.Remaining())
			break
		}

		synthesisContent, usage, err := p.synthesize(ctx, llm, prompt, budget)
		if err != nil {
			slog.Warn("dreaming: synthesis failed", "err", err)
			continue
		}

		if usage != nil {
			spendErr := budget.Spend(usage.TotalTokens)
			p.record(ctx, llm, cycle, "dream_synthesis", nil, usage)
			if spendErr != nil {
				break
			}
		}

		if synthesisContent == "" {
			continue
		}

		// Novelty audit: drop the synthesis if it does not contain at least
		// one fact absent from the source cluster. The audit charges its own
		// LLM usage against the dream budget when the borderline judge fires.
		if noveltyEnabled {
			passed, reason, auditUsage, auditErr := p.auditNovelty(ctx, llm, synthesisContent, cluster)
			if auditUsage != nil {
				_ = budget.Spend(auditUsage.TotalTokens)
				p.record(ctx, llm, cycle, "dream_novelty_audit", nil, auditUsage)
			}
			if auditErr != nil {
				slog.Warn("dreaming: novelty audit error",
					"err", auditErr, "reason", reason)
				continue
			}
			if !passed {
				rejectedSources := make([]string, len(cluster))
				for i, m := range cluster {
					rejectedSources[i] = m.ID.String()
				}
				_ = logger.LogOperation(ctx, model.DreamPhaseConsolidation,
					model.DreamOpMemoryRejected, "memory", uuid.Nil,
					nil,
					map[string]interface{}{
						"reason":            reason,
						"source_memory_ids": rejectedSources,
					})
				slog.Info("dreaming: synthesis rejected by novelty audit",
					"reason", reason, "sources", len(cluster))
				continue
			}
		}

		// Collect source memory IDs.
		sourceIDs := make([]string, len(cluster))
		for i, m := range cluster {
			sourceIDs[i] = m.ID.String()
		}

		metadata, _ := json.Marshal(map[string]interface{}{
			"dream_cycle_id":   cycle.ID.String(),
			"source_memory_ids": sourceIDs,
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

		// Log the operation.
		_ = logger.LogOperation(ctx, model.DreamPhaseConsolidation,
			model.DreamOpMemoryCreated, "memory", synthMemory.ID,
			nil, synthMemory)
	}

	return nil
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
) (string, *provider.TokenUsage, error) {
	resp, err := llm.Complete(ctx, &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   budget.PerCallCap(),
		Temperature: 0.3,
	})
	if err != nil {
		return "", nil, err
	}

	usage := resp.Usage
	if usage.TotalTokens == 0 {
		if budget.MarkZeroUsageWarned() {
			slog.Warn("dreaming: provider returned zero token usage; estimating from prompt/response length",
				"provider", llm.Name(), "phase", model.DreamPhaseConsolidation)
		}
		usage.PromptTokens = EstimateTokens(prompt)
		usage.CompletionTokens = EstimateTokens(resp.Content)
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	}

	return strings.TrimSpace(resp.Content), &usage, nil
}

// auditNovelty checks whether a candidate synthesis contains at least one fact
// not present in any of its source memories. Hybrid path:
//   - Compute max cosine similarity between candidate and source embeddings.
//   - maxSim >= high threshold ⇒ reject (clearly duplicative).
//   - maxSim <= low threshold  ⇒ accept (clearly novel).
//   - otherwise call the LLM judge and trust its JSON verdict.
//
// Failure modes are closed: on embedding error or judge JSON parse failure the
// audit returns (false, ...). usage may be non-nil even on failure when the
// LLM call already happened, so callers must spend it before handling err.
func (p *ConsolidationPhase) auditNovelty(
	ctx context.Context,
	llm provider.LLMProvider,
	candidate string,
	sources []model.Memory,
) (passed bool, reason string, usage *provider.TokenUsage, err error) {
	if len(sources) == 0 {
		return false, "no_sources", nil, nil
	}

	high, _ := p.settings.ResolveFloat(ctx, service.SettingDreamNoveltyEmbedHighThreshold, "global")
	if high <= 0 {
		high = 0.97
	}
	low, _ := p.settings.ResolveFloat(ctx, service.SettingDreamNoveltyEmbedLowThreshold, "global")
	if low <= 0 {
		low = 0.85
	}

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
		resp, embErr := embedder.Embed(ctx, &provider.EmbeddingRequest{
			Input:     inputs,
			Dimension: pickEmbedderDim(embedder.Dimensions()),
		})
		if embErr != nil || resp == nil || len(resp.Embeddings) != len(inputs) {
			return false, "embed_error", nil, embErr
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
			return false, "embed_high_sim", nil, nil
		}
		if maxSim <= low {
			return true, "embed_low_sim", nil, nil
		}
		// Borderline ⇒ fall through to the LLM judge.
	}

	promptTpl, _ := p.settings.Resolve(ctx, service.SettingDreamNoveltyJudgePrompt, "global")
	if promptTpl == "" {
		// Without a judge prompt we cannot adjudicate borderline cases.
		// Fail closed when the embedder pre-filter did not already decide.
		return false, "no_judge_prompt", nil, nil
	}

	sourceTexts := make([]string, 0, len(sources))
	for _, s := range sources {
		sourceTexts = append(sourceTexts, s.Content)
	}
	prompt := fmt.Sprintf(promptTpl, candidate, strings.Join(sourceTexts, "\n---\n"))

	maxTokens, _ := p.settings.ResolveInt(ctx, service.SettingDreamNoveltyJudgeMaxTokens, "global")
	if maxTokens <= 0 {
		maxTokens = 512
	}

	resp, err := llm.Complete(ctx, &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   maxTokens,
		Temperature: 0.1,
		JSONMode:    true,
	})
	if err != nil {
		return false, "judge_call_error", nil, err
	}

	u := resp.Usage
	if u.TotalTokens == 0 {
		u.PromptTokens = EstimateTokens(prompt)
		u.CompletionTokens = EstimateTokens(resp.Content)
		u.TotalTokens = u.PromptTokens + u.CompletionTokens
	}

	var parsed struct {
		NovelFacts []string `json:"novel_facts"`
	}
	if jerr := json.Unmarshal([]byte(strings.TrimSpace(resp.Content)), &parsed); jerr != nil {
		return false, "judge_parse_error", &u, nil
	}
	return len(parsed.NovelFacts) > 0, "llm_judge", &u, nil
}

// pickEmbedderDim returns the largest dimension reported by the provider, or
// 0 to let the provider use its native default. Audits only need internal
// consistency between candidate and source vectors, not compatibility with
// the vector store.
func pickEmbedderDim(dims []int) int {
	best := 0
	for _, d := range dims {
		if d > best {
			best = d
		}
	}
	return best
}

// clusterMemories groups related memories using simple content overlap.
// Each memory appears in at most one cluster.
func (p *ConsolidationPhase) clusterMemories(memories []model.Memory) [][]model.Memory {
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

			if overlap >= 0.3 {
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
