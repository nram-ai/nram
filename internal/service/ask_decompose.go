package service

import (
	"context"
	"encoding/json"
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

	pid := primaryProjectID
	uc := &model.UsageContext{UserID: req.UserID, ProjectID: &pid}
	if req.OrgID != uuid.Nil {
		org := req.OrgID
		uc.OrgID = &org
	}
	usageCtx := provider.WithUsageContext(ctx, uc)
	usageCtx = provider.WithNamespaceID(usageCtx, primaryNS)
	usageCtx = provider.WithAPIKeyID(usageCtx, req.APIKeyID)
	usageCtx = provider.WithOperation(usageCtx, provider.OperationAskSynthesis)

	resp, err := llm.Complete(usageCtx, &provider.CompletionRequest{
		Messages:    provider.BuildMessages(provider.GuardedSystem(system), user),
		MaxTokens:   maxTokens,
		Temperature: temperature,
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
// reply. The contract is {"subqueries":[...]}; it also tolerates a bare JSON
// array of strings (a small model dropping the envelope). Returns nil on any
// parse failure or an empty list — both mean "do not decompose". Whitespace-only
// entries are dropped; an empty result after cleaning is treated as no
// decomposition.
func parseDecomposition(raw string) []string {
	body := strings.TrimSpace(raw)
	// Clip to the outermost JSON object, else the outermost array, ignoring any
	// markdown fences or prose the model wrapped around it.
	if start, end := strings.Index(body, "{"), strings.LastIndex(body, "}"); start >= 0 && end > start {
		body = body[start : end+1]
	} else if start, end := strings.Index(body, "["), strings.LastIndex(body, "]"); start >= 0 && end > start {
		body = body[start : end+1]
	} else {
		return nil
	}

	var raws []string
	var envelope struct {
		Subqueries []string `json:"subqueries"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err == nil && envelope.Subqueries != nil {
		raws = envelope.Subqueries
	} else {
		var arr []string
		if err := json.Unmarshal([]byte(body), &arr); err != nil {
			return nil
		}
		raws = arr
	}

	cleaned := make([]string, 0, len(raws))
	for _, q := range raws {
		if q = strings.TrimSpace(q); q != "" {
			cleaned = append(cleaned, q)
		}
	}
	if len(cleaned) == 0 {
		return nil
	}
	return cleaned
}
