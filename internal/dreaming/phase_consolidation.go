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
}

// NewConsolidationPhase creates a new consolidation and reinforcement phase.
func NewConsolidationPhase(
	memories MemoryReader,
	memWriter MemoryWriter,
	lineage LineageWriter,
	llmProvider LLMProviderFunc,
	settings SettingsResolver,
) *ConsolidationPhase {
	return &ConsolidationPhase{
		memories:    memories,
		memWriter:   memWriter,
		lineage:     lineage,
		llmProvider: llmProvider,
		settings:    settings,
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

	for _, synthesis := range syntheses {
		if budget.Exhausted() {
			break
		}

		// Get a sample of user memories to evaluate against.
		sample := sampleMemories(userMemories, 5)
		if len(sample) == 0 {
			continue
		}

		alignment, usage, err := p.scoreAlignment(ctx, llm, &synthesis, sample, budget)
		if err != nil {
			slog.Warn("dreaming: alignment scoring failed", "synthesis", synthesis.ID, "err", err)
			continue
		}

		if usage != nil {
			if err := budget.Spend(usage.TotalTokens); err != nil {
				break
			}
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

// scoreAlignment asks the LLM how strongly recent evidence supports or
// contradicts an existing synthesis. Returns a value from -1.0 (strong
// contradiction) to 1.0 (strong support).
func (p *ConsolidationPhase) scoreAlignment(
	ctx context.Context,
	llm provider.LLMProvider,
	synthesis *model.Memory,
	evidence []model.Memory,
	budget *TokenBudget,
) (float64, *provider.TokenUsage, error) {
	var evidenceTexts []string
	for _, e := range evidence {
		evidenceTexts = append(evidenceTexts, e.Content)
	}

	promptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamAlignmentPrompt, "global")
	prompt := fmt.Sprintf(promptTemplate, synthesis.Content, strings.Join(evidenceTexts, "\n---\n"))

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

	var result struct {
		Alignment float64 `json:"alignment"`
		Reasoning string  `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(resp.Content)), &result); err != nil {
		return 0, &resp.Usage, fmt.Errorf("parse alignment response: %w", err)
	}

	// Clamp to [-1, 1].
	if result.Alignment < -1 {
		result.Alignment = -1
	}
	if result.Alignment > 1 {
		result.Alignment = 1
	}

	return result.Alignment, &resp.Usage, nil
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

	for _, cluster := range clusters {
		if budget.Exhausted() {
			break
		}

		if len(cluster) < 2 {
			continue
		}

		synthesisContent, usage, err := p.synthesize(ctx, llm, cluster, budget)
		if err != nil {
			slog.Warn("dreaming: synthesis failed", "err", err)
			continue
		}

		if usage != nil {
			if err := budget.Spend(usage.TotalTokens); err != nil {
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
				ID:       uuid.New(),
				MemoryID: synthMemory.ID,
				ParentID: &parentID,
				Relation: "synthesized_from",
			})
		}

		// Log the operation.
		_ = logger.LogOperation(ctx, model.DreamPhaseConsolidation,
			model.DreamOpMemoryCreated, "memory", synthMemory.ID,
			nil, synthMemory)
	}

	return nil
}

// synthesize asks the LLM to produce a consolidated summary from a cluster.
func (p *ConsolidationPhase) synthesize(
	ctx context.Context,
	llm provider.LLMProvider,
	cluster []model.Memory,
	budget *TokenBudget,
) (string, *provider.TokenUsage, error) {
	var contents []string
	for _, m := range cluster {
		contents = append(contents, m.Content)
	}

	promptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamSynthesisPrompt, "global")
	prompt := fmt.Sprintf(promptTemplate, strings.Join(contents, "\n---\n"))

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

	return strings.TrimSpace(resp.Content), &resp.Usage, nil
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
