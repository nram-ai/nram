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

// maxContradictionPairs hard-caps the number of pair comparisons per cycle
// regardless of budget state. Each pair costs one LLM call, and on a local
// provider that can be ~25s; cap keeps a single phase bounded even when
// zero-usage responses prevent the budget from advancing.
const maxContradictionPairs = 30

// ContradictionPhase detects contradictions between memories that reference
// the same entities. It uses vector similarity to find candidate pairs,
// then an LLM to verify whether they actually contradict.
type ContradictionPhase struct {
	memories    MemoryReader
	lineage     LineageWriter
	llmProvider LLMProviderFunc
	settings    SettingsResolver
	tokens      TokenRecorder
	usageCtx    UsageContextResolver
}

// NewContradictionPhase creates a new contradiction detection phase.
func NewContradictionPhase(
	memories MemoryReader,
	lineage LineageWriter,
	llmProvider LLMProviderFunc,
	settings SettingsResolver,
	tokens TokenRecorder,
	usageCtx UsageContextResolver,
) *ContradictionPhase {
	return &ContradictionPhase{
		memories:    memories,
		lineage:     lineage,
		llmProvider: llmProvider,
		settings:    settings,
		tokens:      tokens,
		usageCtx:    usageCtx,
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

	promptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamContradictionPrompt, "global")

	contradictions := 0
	for _, pair := range pairs {
		if budget.Exhausted() {
			break
		}

		// Pre-flight budget check using the 4-bytes-per-token heuristic on
		// the prompt plus the per-call output cap. Local providers that
		// omit usage fields will still advance the budget via the post-call
		// fallback below, so this guard prevents starting calls we can't
		// afford to record.
		estPrompt := fmt.Sprintf(promptTemplate, pair[0].Content, pair[1].Content)
		estCost := EstimateTokens(estPrompt) + budget.PerCallCap()
		if budget.Remaining() < estCost {
			slog.Info("dreaming: contradiction call skipped (estimated cost exceeds remaining budget)",
				"estimate", estCost, "remaining", budget.Remaining())
			break
		}

		found, explanation, usage, err := p.checkContradiction(ctx, llm, &pair[0], &pair[1], estPrompt, budget)

		// Account for usage before handling the error. Parse-error paths
		// still return non-nil usage from the LLM call, and we must record
		// what it cost even if we can't use the result.
		var spendErr error
		if usage != nil {
			spendErr = budget.Spend(usage.TotalTokens)
			p.record(ctx, llm, cycle, &pair[0], usage)
		}

		if err != nil {
			slog.Warn("dreaming: contradiction check failed", "err", err)
			if spendErr != nil {
				break
			}
			continue
		}

		if spendErr != nil {
			break
		}

		if !found {
			continue
		}

		// Record the contradiction as a lineage entry.
		lineageEntry := &model.MemoryLineage{
			ID:          uuid.New(),
			NamespaceID: cycle.NamespaceID,
			MemoryID:    pair[0].ID,
			ParentID:    &pair[1].ID,
			Relation:    model.LineageConflictsWith,
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
// using pairwise comparison capped to maxContradictionPairs. The cap check
// is inside the inner loop so the count stops exactly at the cap rather
// than overshooting by a full row of j iterations.
func (p *ContradictionPhase) findCandidatePairs(memories []model.Memory) [][2]model.Memory {
	pairs := make([][2]model.Memory, 0, maxContradictionPairs)

	for i := 0; i < len(memories); i++ {
		for j := i + 1; j < len(memories); j++ {
			if len(pairs) >= maxContradictionPairs {
				return pairs
			}
			pairs = append(pairs, [2]model.Memory{memories[i], memories[j]})
		}
	}

	return pairs
}

func (p *ContradictionPhase) checkContradiction(
	ctx context.Context,
	llm provider.LLMProvider,
	a, b *model.Memory,
	prompt string,
	budget *TokenBudget,
) (bool, string, *provider.TokenUsage, error) {
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

	usage := resp.Usage
	// Fall back to the 4-bytes-per-token heuristic when the provider omits
	// the usage field (e.g. Ollama's OpenAI-compat endpoint). Otherwise the
	// budget never advances and the cycle burns through every candidate.
	if usage.TotalTokens == 0 {
		if budget.MarkZeroUsageWarned() {
			slog.Warn("dreaming: provider returned zero token usage; estimating from prompt/response length",
				"provider", llm.Name(), "phase", model.DreamPhaseContradictions)
		}
		usage.PromptTokens = EstimateTokens(prompt)
		usage.CompletionTokens = EstimateTokens(resp.Content)
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	}

	var result struct {
		Contradicts bool   `json:"contradicts"`
		Explanation string `json:"explanation"`
	}

	content := strings.TrimSpace(resp.Content)
	if err := json.Unmarshal([]byte(content), &result); err != nil {
		return false, "", &usage, fmt.Errorf("parse contradiction response: %w", err)
	}

	return result.Contradicts, result.Explanation, &usage, nil
}

// record persists a token usage row for the contradiction check.
func (p *ContradictionPhase) record(
	ctx context.Context,
	llm provider.LLMProvider,
	cycle *model.DreamCycle,
	mem *model.Memory,
	usage *provider.TokenUsage,
) {
	if p.tokens == nil || usage == nil {
		return
	}
	rec := &model.TokenUsage{
		ID:           uuid.New(),
		NamespaceID:  cycle.NamespaceID,
		Operation:    "dream_contradiction",
		Provider:     llm.Name(),
		TokensInput:  usage.PromptTokens,
		TokensOutput: usage.CompletionTokens,
		MemoryID:     &mem.ID,
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
		slog.Warn("dreaming: token usage record failed", "phase", model.DreamPhaseContradictions, "err", err)
	}
}
