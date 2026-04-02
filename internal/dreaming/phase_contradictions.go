package dreaming

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
)

// ContradictionPhase detects contradictions between memories that reference
// the same entities. It uses vector similarity to find candidate pairs,
// then an LLM to verify whether they actually contradict.
type ContradictionPhase struct {
	memories    MemoryReader
	lineage     LineageWriter
	llmProvider LLMProviderFunc
	settings    SettingsResolver
}

// NewContradictionPhase creates a new contradiction detection phase.
func NewContradictionPhase(
	memories MemoryReader,
	lineage LineageWriter,
	llmProvider LLMProviderFunc,
	settings SettingsResolver,
) *ContradictionPhase {
	return &ContradictionPhase{
		memories:    memories,
		lineage:     lineage,
		llmProvider: llmProvider,
		settings:    settings,
	}
}

func (p *ContradictionPhase) Name() string { return model.DreamPhaseContradictions }

func (p *ContradictionPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) error {
	llm := p.llmProvider()
	if llm == nil {
		slog.Info("dreaming: no LLM provider for contradiction detection, skipping")
		return nil
	}

	// List all memories in the namespace.
	memories, err := p.memories.ListByNamespace(ctx, cycle.NamespaceID, 500, 0)
	if err != nil {
		return err
	}

	if len(memories) < 2 {
		return nil
	}

	// Compare pairs using LLM with pairwise comparison capped to a reasonable limit.
	pairs := p.findCandidatePairs(memories)

	contradictions := 0
	for _, pair := range pairs {
		if budget.Exhausted() {
			break
		}

		found, explanation, usage, err := p.checkContradiction(ctx, llm, &pair[0], &pair[1], budget)
		if err != nil {
			slog.Warn("dreaming: contradiction check failed", "err", err)
			continue
		}

		if usage != nil {
			if err := budget.Spend(usage.TotalTokens); err != nil {
				break
			}
		}

		if !found {
			continue
		}

		// Record the contradiction as a lineage entry.
		lineageEntry := &model.MemoryLineage{
			ID:       uuid.New(),
			MemoryID: pair[0].ID,
			ParentID: &pair[1].ID,
			Relation: "conflicts_with",
		}
		if err := p.lineage.Create(ctx, lineageEntry); err != nil {
			slog.Warn("dreaming: lineage creation failed", "err", err)
			continue
		}

		// Log the operation.
		_ = logger.LogOperation(ctx, model.DreamPhaseContradictions,
			model.DreamOpContradictionDetected, "memory", pair[0].ID,
			nil, map[string]interface{}{
				"conflicting_id": pair[1].ID.String(),
				"explanation":    explanation,
			})

		contradictions++
	}

	if contradictions > 0 {
		slog.Info("dreaming: contradictions detected",
			"count", contradictions, "cycle", cycle.ID)
	}

	return nil
}

// findCandidatePairs identifies pairs of memories that might contradict,
// using pairwise comparison capped to a reasonable limit.
func (p *ContradictionPhase) findCandidatePairs(memories []model.Memory) [][2]model.Memory {
	var pairs [][2]model.Memory

	// Simple pairwise comparison with a reasonable limit.
	limit := len(memories)
	if limit > 50 {
		limit = 50
	}

	for i := 0; i < limit; i++ {
		for j := i + 1; j < limit; j++ {
			pairs = append(pairs, [2]model.Memory{memories[i], memories[j]})
		}

		// Cap total pairs to prevent runaway LLM calls.
		if len(pairs) > 100 {
			break
		}
	}

	return pairs
}

func (p *ContradictionPhase) checkContradiction(
	ctx context.Context,
	llm provider.LLMProvider,
	a, b *model.Memory,
	budget *TokenBudget,
) (bool, string, *provider.TokenUsage, error) {
	promptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamContradictionPrompt, "global")
	prompt := fmt.Sprintf(promptTemplate, a.Content, b.Content)

	resp, err := llm.Complete(ctx, &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   budget.PerCallCap(),
		Temperature: 0.1,
		JSONMode:    true,
	})
	if err != nil {
		return false, "", nil, err
	}

	var result struct {
		Contradicts bool   `json:"contradicts"`
		Explanation string `json:"explanation"`
	}

	content := strings.TrimSpace(resp.Content)
	if err := json.Unmarshal([]byte(content), &result); err != nil {
		return false, "", &resp.Usage, fmt.Errorf("parse contradiction response: %w", err)
	}

	return result.Contradicts, result.Explanation, &resp.Usage, nil
}
