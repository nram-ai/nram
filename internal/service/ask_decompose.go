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

	usageCtx := provider.WithUsageContext(ctx, model.NewUsageContext(req.UserID, primaryProjectID, req.OrgID))
	usageCtx = provider.WithNamespaceID(usageCtx, primaryNS)
	usageCtx = provider.WithAPIKeyID(usageCtx, req.APIKeyID)
	usageCtx = provider.WithOperation(usageCtx, provider.OperationAskSynthesis)

	return decomposeSubqueries(ctx, llm, req.Query, s.settings, usageCtx)
}

// decomposeSubqueries runs the LLM decomposer over a query and returns the
// focused sub-queries, or nil when the model declines to decompose or on any
// provider/parse failure (fail-soft). It does NOT gate on any enabled setting;
// each caller gates independently (ask on ask.decomposition.enabled, recall on
// recall.decomposition.enabled) so the two paths toggle separately while sharing
// one prompt, parser, and knobs. The caller passes a usageCtx already stamped
// with attribution and the operation so decomposition spend lands on the right
// analytics line. The question is nonce-fenced and the system prompt is
// GuardedSystem-wrapped, matching the synthesis call's prompt-injection defense.
func decomposeSubqueries(ctx context.Context, llm provider.LLMProvider, query string, settings *SettingsService, usageCtx context.Context) []string {
	// settings may be nil in test/unwired paths; the Resolve*WithDefault methods
	// are nil-receiver-safe and fall back to the registered defaults.
	system := settings.ResolveStringWithDefault(ctx, SettingAskDecompositionSystemPrompt, "global")
	temperature := settings.ResolveFloatWithDefault(ctx, SettingAskDecompositionTemperature, "global")
	maxTokens := settings.ResolveIntWithDefault(ctx, SettingAskDecompositionMaxTokens, "global")
	maxSubqueries := settings.ResolveIntWithDefault(ctx, SettingAskDecompositionMaxSubqueries, "global")
	if maxSubqueries <= 0 {
		return nil
	}

	user := provider.Fence("question", strings.TrimSpace(query))

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
