package service

import (
	"context"
	"strings"

	"github.com/nram-ai/nram/internal/provider"
)

// ExtractRelationshipsLLM runs the relationship-extraction pass (pass 2) over the
// content, fed the entity names extracted in pass 1. It is the relationship
// counterpart to ExtractEntitiesLLM: an over-budget memory is split into the
// same overlapping chunks, each chunk is asked for the relationships among the
// provided entities grounded in that chunk's text, and the results are merged
// (cross-chunk duplicate edges collapse on the canonical triple downstream).
//
// Because the relationship pass is a separate LLM call with its own max_tokens
// budget, a dense entity pass that truncated (pass 1) can no longer starve
// relationships out of a combined response. With no entities there are no
// possible relationships, so an empty name list short-circuits to an empty
// result without a model call.
//
// Cross-chunk relationships are best-effort: a chunk only sees its own slice of
// the text, so an edge is found only when a single chunk's text supports both
// endpoints (the chunk overlap keeps a boundary-straddling edge intact in at
// least one chunk).
func ExtractRelationshipsLLM(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	entityNames []string,
	opts CallOptions,
) (*RelationExtractionEnvelope, error) {
	if len(entityNames) == 0 {
		return &RelationExtractionEnvelope{Result: &RelationExtractionResult{}}, nil
	}

	chunks := chunkExtractionContent(ctx, settings, content)
	if len(chunks) <= 1 {
		return extractRelationshipsWithContinuation(ctx, llm, settings, content, entityNames, opts)
	}

	var merged *RelationExtractionEnvelope
	var firstErr error
	seenRel := make(map[string]bool)
	for _, ch := range chunks {
		env, err := extractRelationshipsWithContinuation(ctx, llm, settings, ch, entityNames, opts)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		merged = mergeRelationEnvelope(merged, env, seenRel)
	}
	if merged == nil {
		return nil, firstErr
	}
	if firstErr != nil {
		merged.PartialRecovery = true
	}
	return merged, nil
}

// extractRelationshipsOnce runs the relationship prompt once over the content +
// entity names and parses the response. Returns *ExtractionFailure on call or
// parse failure (use errors.As). A clean empty result (finish=stop) is a valid
// outcome — many chunks legitimately have no relationships among the entities.
func extractRelationshipsOnce(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	entityNames []string,
	opts CallOptions,
) (*RelationExtractionEnvelope, error) {
	system := ResolveOrDefault(ctx, settings, SettingRelationshipSystemPrompt, "global")
	user := RenderRelationshipUser(content, entityNames)
	messages := provider.BuildGuardedMessages(system, user)
	req := buildExtractionRequest(messages, opts)

	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationRelationshipExtraction), req)
	if err != nil {
		return nil, buildExtractionFailure(ExtractionPhaseRelationship, ExtractionReasonLLMCallFailed, err.Error(), nil, llm.Name())
	}

	if strings.TrimSpace(resp.Content) == "" {
		return nil, buildExtractionFailure(ExtractionPhaseRelationship, ExtractionReasonEmptyResponse,
			"relationship extraction returned an empty response body", resp, llm.Name())
	}

	result, partial, parseErr := parseRelationships(resp.Content)
	if parseErr != nil {
		return nil, buildExtractionFailure(ExtractionPhaseRelationship, ExtractionReasonParseFailed, parseErr.Error(), resp, llm.Name())
	}

	emptyResult := result == nil || len(result.Relationships) == 0
	if partial && emptyResult && provider.IsTruncated(resp.FinishReason) {
		return nil, buildExtractionFailure(ExtractionPhaseRelationship, ExtractionReasonLengthNoRecover,
			"relationship extraction hit max_tokens and longest-valid-prefix recovery yielded zero relationships",
			resp, llm.Name())
	}

	return &RelationExtractionEnvelope{
		Result:          result,
		Usage:           resp.Usage,
		Model:           resp.Model,
		ProviderName:    llm.Name(),
		FinishReason:    resp.FinishReason,
		PartialRecovery: partial,
		RawResponse:     resp.Content,
	}, nil
}

// extractRelationshipsWithContinuation is the relationship counterpart to
// extractEntitiesWithContinuation: a single relationship call followed by
// bounded continuation passes while the result is truncated.
func extractRelationshipsWithContinuation(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	entityNames []string,
	opts CallOptions,
) (*RelationExtractionEnvelope, error) {
	env, err := extractRelationshipsOnce(ctx, llm, settings, content, entityNames, opts)
	if err != nil {
		return nil, err
	}
	maxChars, maxWords, minRatio := settings.ExtractEntityNameLimits(ctx)
	scrubRelationResult(env.Result, maxChars, maxWords, minRatio)
	maxPasses := settings.ResolveIntWithDefault(ctx, SettingExtractionContinuationMaxPasses, "global")
	if maxPasses <= 0 || env.Result == nil {
		return env, nil
	}
	seenRel := make(map[string]bool, len(env.Result.Relationships))
	for _, r := range env.Result.Relationships {
		seenRel[relationKey(r)] = true
	}
	for pass := 0; pass < maxPasses && provider.IsTruncated(env.FinishReason); pass++ {
		more, cerr := continueRelationshipsOnce(ctx, llm, settings, content, entityNames, env.Result.Relationships, opts)
		if cerr != nil || more == nil || more.Result == nil {
			break
		}
		scrubRelationResult(more.Result, maxChars, maxWords, minRatio)
		added := 0
		for _, r := range more.Result.Relationships {
			k := relationKey(r)
			if k == "" || seenRel[k] {
				continue
			}
			seenRel[k] = true
			env.Result.Relationships = append(env.Result.Relationships, r)
			added++
		}
		addUsage(&env.Usage, more.Usage)
		env.FinishReason = more.FinishReason
		if added > 0 {
			env.PartialRecovery = true
		} else {
			break
		}
	}
	return env, nil
}

// continueRelationshipsOnce re-runs relationship extraction with the
// already-found relationships listed so the model returns only additional ones.
// The system prompt is unchanged; the addendum is appended to the user message.
func continueRelationshipsOnce(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	entityNames []string,
	found []ExtractedRelation,
	opts CallOptions,
) (*RelationExtractionEnvelope, error) {
	system := ResolveOrDefault(ctx, settings, SettingRelationshipSystemPrompt, "global")
	var sb strings.Builder
	for _, r := range found {
		sb.WriteString("- ")
		sb.WriteString(r.Source)
		sb.WriteString(" -> ")
		sb.WriteString(r.Relation)
		sb.WriteString(" -> ")
		sb.WriteString(r.Target)
		sb.WriteByte('\n')
	}
	user := RenderRelationshipUser(content, entityNames) +
		"\n\nYou already extracted these relationships:\n" + sb.String() +
		"\nReturn ONLY additional distinct relationships not already listed, in the same JSON object format. If there are none, return {\"relationships\":[]}."
	messages := provider.BuildGuardedMessages(system, user)
	req := buildExtractionRequest(messages, opts)
	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationRelationshipExtraction), req)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(resp.Content) == "" {
		return &RelationExtractionEnvelope{FinishReason: resp.FinishReason}, nil
	}
	result, partial, perr := parseRelationships(resp.Content)
	if perr != nil {
		// A malformed continuation must not poison the good first-pass result.
		return &RelationExtractionEnvelope{FinishReason: resp.FinishReason}, nil
	}
	return &RelationExtractionEnvelope{
		Result:          result,
		FinishReason:    resp.FinishReason,
		PartialRecovery: partial,
		Usage:           resp.Usage,
	}, nil
}

// mergeRelationEnvelope folds src into dst, deduping relationships by the
// normalized (source, relation, target) triple. seen carries the dedup set
// across chunks. dst==nil starts a fresh envelope stamped with src's identity.
func mergeRelationEnvelope(dst, src *RelationExtractionEnvelope, seenRel map[string]bool) *RelationExtractionEnvelope {
	if src == nil || src.Result == nil {
		return dst
	}
	if dst == nil {
		dst = &RelationExtractionEnvelope{
			Result:       &RelationExtractionResult{},
			Model:        src.Model,
			ProviderName: src.ProviderName,
		}
	}
	for _, r := range src.Result.Relationships {
		k := relationKey(r)
		if k == "" || seenRel[k] {
			continue
		}
		seenRel[k] = true
		dst.Result.Relationships = append(dst.Result.Relationships, r)
	}
	addUsage(&dst.Usage, src.Usage)
	dst.FinishReason = src.FinishReason
	if src.PartialRecovery {
		dst.PartialRecovery = true
	}
	return dst
}

// scrubRelationResult drops relationships whose source or target name is
// degenerate (a wall of text, a whole sentence, or a repetition loop), in place,
// mirroring the relationship half of scrubEntityResult. It also drops a
// self-relationship where source and target are the same after normalization,
// since an edge from an entity to itself carries no graph signal.
func scrubRelationResult(res *RelationExtractionResult, maxChars, maxWords int, minRatio float64) {
	if res == nil || len(res.Relationships) == 0 {
		return
	}
	kept := res.Relationships[:0]
	for _, r := range res.Relationships {
		if IsDegenerateEntityName(r.Source, maxChars, maxWords, minRatio) ||
			IsDegenerateEntityName(r.Target, maxChars, maxWords, minRatio) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(r.Source), strings.TrimSpace(r.Target)) {
			continue
		}
		kept = append(kept, r)
	}
	res.Relationships = kept
}
