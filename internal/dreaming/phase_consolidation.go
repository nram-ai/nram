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
	memories      MemoryReader
	memWriter     MemoryWriter
	lineage       LineageWriter
	llmProvider   LLMProviderFunc
	settings      SettingsResolver
	tokens        TokenRecorder
	usageCtx      UsageContextResolver
}

// NewConsolidationPhase creates a new consolidation and reinforcement phase.
func NewConsolidationPhase(
	memories MemoryReader,
	memWriter MemoryWriter,
	lineage LineageWriter,
	llmProvider LLMProviderFunc,
	settings SettingsResolver,
	tokens TokenRecorder,
	usageCtx UsageContextResolver,
) *ConsolidationPhase {
	return &ConsolidationPhase{
		memories:    memories,
		memWriter:   memWriter,
		lineage:     lineage,
		llmProvider: llmProvider,
		settings:    settings,
		tokens:      tokens,
		usageCtx:    usageCtx,
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
	// Extract source memory IDs from metadata.
	var meta map[string]interface{}
	if synthesis.Metadata != nil {
		_ = json.Unmarshal(synthesis.Metadata, &meta)
	}

	sourceIDsRaw, ok := meta["source_memory_ids"]
	if !ok {
		return
	}

	sourceIDs, ok := sourceIDsRaw.([]interface{})
	if !ok {
		return
	}

	for _, idRaw := range sourceIDs {
		idStr, ok := idRaw.(string)
		if !ok {
			continue
		}
		memID, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}

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
