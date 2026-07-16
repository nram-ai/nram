package service

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// decomposeQuery asks the synthesis LLM to break an aggregation/compare/classify
// question into one focused retrieval sub-query per class, so a dominant class
// cannot bury a minority one in a single broad recall (the broad-aggregation
// defect). It returns nil — and the caller falls back to the single-recall path
// — for a non-aggregation question (the model returns an empty list) or on any
// provider/parse failure, so decomposition is strictly additive and fail-soft.
//
// The call reuses the ask synthesis provider and stamps the same usage context
// and OperationAskSynthesis as synthesize, so its tokens fold into the ask tool's
// existing line in token analytics rather than appearing as a separate operation.
// The question is nonce-fenced and the system prompt is GuardedSystem-wrapped,
// matching the synthesis call's prompt-injection defense.
func (s *AskService) decomposeQuery(
	ctx context.Context,
	llm provider.LLMProvider,
	req *AskRequest,
	primaryProjectID, primaryNS uuid.UUID,
) []string {
	if !s.settings.ResolveBoolWithDefault(ctx, SettingAskDecompositionEnabled, "global") {
		return nil
	}
	system := s.settings.ResolveStringWithDefault(ctx, SettingAskDecompositionSystemPrompt, "global")
	temperature := s.settings.ResolveFloatWithDefault(ctx, SettingAskDecompositionTemperature, "global")
	maxTokens := s.settings.ResolveIntWithDefault(ctx, SettingAskDecompositionMaxTokens, "global")
	maxSubqueries := s.settings.ResolveIntWithDefault(ctx, SettingAskDecompositionMaxSubqueries, "global")
	if maxSubqueries <= 0 {
		return nil
	}

	user := provider.Fence("question", strings.TrimSpace(req.Query))

	usageCtx := provider.WithUsageContext(ctx, model.NewUsageContext(req.UserID, primaryProjectID, req.OrgID))
	usageCtx = provider.WithNamespaceID(usageCtx, primaryNS)
	usageCtx = provider.WithAPIKeyID(usageCtx, req.APIKeyID)
	usageCtx = provider.WithOperation(usageCtx, provider.OperationAskSynthesis)

	resp, err := llm.Complete(usageCtx, &provider.CompletionRequest{
		Messages:    provider.BuildGuardedMessages(system, user),
		MaxTokens:   maxTokens,
		Temperature: provider.Float64(temperature),
	})
	if err != nil || resp == nil {
		return nil
	}
	subs := parseDecomposition(resp.Content)
	if len(subs) > maxSubqueries {
		subs = subs[:maxSubqueries]
	}
	return subs
}

// parseDecomposition extracts the sub-query list from the decomposition model's
// reply. The contract is {"subqueries":[...]}; it delegates to ParseLLMStringList,
// so it also tolerates a bare JSON array, any-key object envelope, mixed element
// types, missing/mixed quoting, and truncated arrays (the small-model failure
// modes the query-augment path already learned). Returns nil on any parse
// failure or an empty list — both mean "do not decompose".
func parseDecomposition(raw string) []string {
	subs, err := ParseLLMStringList(raw)
	if err != nil {
		return nil
	}
	return subs
}
