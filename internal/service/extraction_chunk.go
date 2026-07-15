package service

import (
	"context"
	"strings"

	"github.com/nram-ai/nram/internal/provider"
)

// ExtractFactsLLM runs fact extraction over the content, splitting an
// over-budget memory into overlapping chunks (so dense memories no longer
// truncate at the model's output limit) and, when a single call still hits the
// length cap, running bounded continuation passes. For a small memory (the
// large majority) it is a single call. Returns *ExtractionFailure on failure.
func ExtractFactsLLM(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*FactExtractionEnvelope, error) {
	chunks := chunkExtractionContent(ctx, settings, content)
	if len(chunks) <= 1 {
		return extractFactsWithContinuation(ctx, llm, settings, content, opts)
	}

	var merged *FactExtractionEnvelope
	var firstErr error
	seen := make(map[string]bool)
	for _, ch := range chunks {
		env, err := extractFactsWithContinuation(ctx, llm, settings, ch, opts)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		merged = mergeFactEnvelope(merged, env, seen)
	}
	if merged == nil {
		// Every chunk failed; surface the first failure unchanged.
		return nil, firstErr
	}
	if firstErr != nil {
		merged.PartialRecovery = true
	}
	return merged, nil
}

// ExtractEntitiesLLM is the entity counterpart to ExtractFactsLLM: chunk an
// over-budget memory, extract per chunk, and merge entities/relationships
// (cross-chunk duplicates collapse on the canonical key downstream anyway).
func ExtractEntitiesLLM(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*EntityExtractionEnvelope, error) {
	chunks := chunkExtractionContent(ctx, settings, content)
	if len(chunks) <= 1 {
		return extractEntitiesWithContinuation(ctx, llm, settings, content, opts)
	}

	var merged *EntityExtractionEnvelope
	var firstErr error
	seenEnt := make(map[string]bool)
	seenRel := make(map[string]bool)
	for _, ch := range chunks {
		env, err := extractEntitiesWithContinuation(ctx, llm, settings, ch, opts)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		merged = mergeEntityEnvelope(merged, env, seenEnt, seenRel)
	}
	if merged == nil {
		return nil, firstErr
	}
	if firstErr != nil {
		merged.PartialRecovery = true
	}
	return merged, nil
}

// chunkExtractionContent splits content into overlapping word windows when its
// estimated token count exceeds enrichment.extraction.chunk_threshold_tokens.
// Small content returns a single chunk (the original string). Splitting is
// word-based: extraction does not depend on exact formatting, and the overlap
// keeps an entity or relation that straddles a boundary intact in at least one
// chunk.
//
// Window size comes from a document-wide tokens-per-word average, which is only
// an estimate: a region that tokenizes denser than the document as a whole (a
// code block amid prose) overshoots it. Each window is therefore measured and
// subdivided until it fits. The bound is not absolute, and callers must not size
// a prompt as if it were: text with no interior whitespace (a base64 blob, a
// minified line) is a single word, cannot be divided any further, and is emitted
// whole however large it is.
//
// The average-based loop is kept on purpose rather than replaced by measured
// packing, because it fixes the window COUNT and the count is the pass's output
// budget: each window is a separate extraction call with its own max_tokens, so
// fewer windows means fewer relationships a dense memory can yield. Subdividing
// only ever adds windows. Repacking to a measured fit collapses the count on
// content whose raw token count exceeds the threshold while its
// whitespace-collapsed form does not (this function measures the raw string but
// emits joined words), and that has been measured to cost real relationships;
// TestChunkExtractionContent_NeverReducesChunkCount pins it.
//
// Known limitation: the average is document-wide, so a single dense word drags
// it up and shreds the surrounding prose into many small windows. Undoing that
// needs the count invariant relaxed, so it is not addressed here.
func chunkExtractionContent(ctx context.Context, settings *SettingsService, content string) []string {
	if settings == nil {
		return []string{content}
	}
	threshold := settings.ResolveIntWithDefault(ctx, SettingExtractionChunkThresholdTokens, "global")
	if threshold <= 0 {
		return []string{content}
	}
	total := provider.EstimateTokens("", content)
	if total <= threshold {
		return []string{content}
	}
	words := strings.Fields(content)
	if len(words) == 0 {
		return []string{content}
	}
	overlap := settings.ResolveIntWithDefault(ctx, SettingExtractionChunkOverlapTokens, "global")

	tokPerWord := float64(total) / float64(len(words))
	if tokPerWord <= 0 {
		tokPerWord = 1
	}
	chunkWords := int(float64(threshold) / tokPerWord)
	if chunkWords < 1 {
		return []string{content}
	}
	overlapWords := max(int(float64(overlap)/tokPerWord), 0)
	if overlapWords >= chunkWords {
		overlapWords = chunkWords / 4
	}
	step := chunkWords - overlapWords
	if step < 1 {
		step = chunkWords
	}

	out := make([]string, 0, (len(words)+step-1)/step)
	for start := 0; start < len(words); start += step {
		end := min(start+chunkWords, len(words))
		out = emitWindow(words[start:end], threshold, overlapWords, out)
		if end == len(words) {
			break
		}
	}
	return out
}

// emitWindow appends words as one window when it fits the threshold, and
// otherwise halves it and recurses until every piece does. It is the only place
// a window is emitted, so the token count is measured on exactly the string that
// gets appended rather than on a reconstruction of it. words must be non-empty,
// which the caller's loop guarantees.
//
// The second half carries the same overlap lookback the top-level windows use,
// so subdividing does not introduce a seam that no window covers.
//
// Termination: a single word is emitted as-is (it cannot be divided further),
// and both halves are strictly shorter than the input, since the lookback is
// clamped below the midpoint.
func emitWindow(words []string, threshold, overlapWords int, out []string) []string {
	joined := strings.Join(words, " ")
	if len(words) == 1 || provider.EstimateTokens("", joined) <= threshold {
		return append(out, joined)
	}
	mid := len(words) / 2
	out = emitWindow(words[:mid], threshold, overlapWords, out)

	back := overlapWords
	if back >= mid {
		back = mid / 4
	}
	return emitWindow(words[mid-back:], threshold, overlapWords, out)
}

// extractFactsWithContinuation runs a single fact extraction and, while the
// result is truncated (finish_reason=length) and continuation passes remain,
// asks the model to continue with only the facts it has not yet returned. The
// pass count is bounded by enrichment.extraction.continuation_max_passes so a
// looping model cannot run away; a pass that adds nothing new also stops the
// loop. A continuation-call error is swallowed (the first-pass result stands).
func extractFactsWithContinuation(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*FactExtractionEnvelope, error) {
	env, err := extractFactsOnce(ctx, llm, settings, content, opts)
	if err != nil {
		return nil, err
	}
	maxPasses := settings.ResolveIntWithDefault(ctx, SettingExtractionContinuationMaxPasses, "global")
	if maxPasses <= 0 {
		return env, nil
	}
	seen := make(map[string]bool, len(env.Facts))
	for _, f := range env.Facts {
		seen[factKey(f)] = true
	}
	for pass := 0; pass < maxPasses && provider.IsTruncated(env.FinishReason); pass++ {
		more, cerr := continueFactsOnce(ctx, llm, settings, content, env.Facts, opts)
		if cerr != nil || more == nil {
			break
		}
		added := 0
		for _, f := range more.Facts {
			k := factKey(f)
			if k == "" || seen[k] {
				continue
			}
			seen[k] = true
			env.Facts = append(env.Facts, f)
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

// continueFactsOnce re-runs fact extraction with the already-found facts listed
// so the model returns only additional ones. The system prompt is unchanged; the
// addendum is appended to the user message.
func continueFactsOnce(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	found []ExtractedFact,
	opts CallOptions,
) (*FactExtractionEnvelope, error) {
	system := ResolveOrDefault(ctx, settings, SettingFactSystemPrompt, "global")
	var sb strings.Builder
	for _, f := range found {
		sb.WriteString("- ")
		sb.WriteString(f.text())
		sb.WriteByte('\n')
	}
	user := RenderExtractionUser(content) +
		"\n\nYou already extracted these facts:\n" + sb.String() +
		"\nReturn ONLY additional distinct facts not already listed, in the same JSON array format. If there are none, return []."
	messages := provider.BuildGuardedMessages(system, user)
	req := buildExtractionRequest(messages, opts)
	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationFactExtraction), req)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(resp.Content) == "" {
		return &FactExtractionEnvelope{FinishReason: resp.FinishReason}, nil
	}
	facts, partial, perr := parseFacts(resp.Content)
	if perr != nil {
		// A malformed continuation must not poison the good first-pass result.
		return &FactExtractionEnvelope{FinishReason: resp.FinishReason}, nil
	}
	return &FactExtractionEnvelope{
		Facts:           facts,
		Usage:           resp.Usage,
		FinishReason:    resp.FinishReason,
		PartialRecovery: partial,
	}, nil
}

// extractEntitiesWithContinuation is the entity counterpart to
// extractFactsWithContinuation.
func extractEntitiesWithContinuation(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*EntityExtractionEnvelope, error) {
	env, err := extractEntitiesOnce(ctx, llm, settings, content, opts)
	if err != nil {
		return nil, err
	}
	maxChars, maxWords, minRatio := settings.ExtractEntityNameLimits(ctx)
	scrubEntityResult(env.Result, maxChars, maxWords, minRatio)
	maxPasses := settings.ResolveIntWithDefault(ctx, SettingExtractionContinuationMaxPasses, "global")
	if maxPasses <= 0 || env.Result == nil {
		return env, nil
	}
	seenEnt := make(map[string]bool, len(env.Result.Entities))
	for _, e := range env.Result.Entities {
		seenEnt[entityKey(e)] = true
	}
	seenRel := make(map[string]bool, len(env.Result.Relationships))
	for _, r := range env.Result.Relationships {
		seenRel[relationKey(r)] = true
	}
	for pass := 0; pass < maxPasses && provider.IsTruncated(env.FinishReason); pass++ {
		more, cerr := continueEntitiesOnce(ctx, llm, settings, content, env.Result.Entities, opts)
		if cerr != nil || more == nil || more.Result == nil {
			break
		}
		// Scrub before counting so a degenerate continuation (a model that has
		// fallen into a repetition loop) contributes zero new entities and the
		// added==0 check below breaks the loop instead of feeding it back.
		scrubEntityResult(more.Result, maxChars, maxWords, minRatio)
		added := 0
		for _, e := range more.Result.Entities {
			k := entityKey(e)
			if k == "" || seenEnt[k] {
				continue
			}
			seenEnt[k] = true
			env.Result.Entities = append(env.Result.Entities, e)
			added++
		}
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

func continueEntitiesOnce(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	found []ExtractedEntityData,
	opts CallOptions,
) (*EntityExtractionEnvelope, error) {
	system := ResolveOrDefault(ctx, settings, SettingEntitySystemPrompt, "global")
	var sb strings.Builder
	for _, e := range found {
		sb.WriteString("- ")
		sb.WriteString(e.Name)
		sb.WriteByte('\n')
	}
	user := RenderExtractionUser(content) +
		"\n\nYou already extracted these entities:\n" + sb.String() +
		"\nReturn ONLY additional entities not already listed, in the same JSON object format. If there are none, return {\"entities\":[]}."
	messages := provider.BuildGuardedMessages(system, user)
	req := buildExtractionRequest(messages, opts)
	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationEntityExtraction), req)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(resp.Content) == "" {
		return &EntityExtractionEnvelope{FinishReason: resp.FinishReason}, nil
	}
	result, partial, perr := parseEntities(resp.Content)
	if perr != nil {
		return &EntityExtractionEnvelope{FinishReason: resp.FinishReason}, nil
	}
	return &EntityExtractionEnvelope{
		Result:          result,
		FinishReason:    resp.FinishReason,
		PartialRecovery: partial,
		Usage:           resp.Usage,
	}, nil
}

// mergeFactEnvelope folds src into dst, deduping facts by normalized content.
// seen carries the dedup set across chunks. dst==nil starts a fresh envelope
// stamped with src's model/provider identity.
func mergeFactEnvelope(dst, src *FactExtractionEnvelope, seen map[string]bool) *FactExtractionEnvelope {
	if src == nil {
		return dst
	}
	if dst == nil {
		dst = &FactExtractionEnvelope{Model: src.Model, ProviderName: src.ProviderName}
	}
	for _, f := range src.Facts {
		k := factKey(f)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		dst.Facts = append(dst.Facts, f)
	}
	addUsage(&dst.Usage, src.Usage)
	dst.FinishReason = src.FinishReason
	if src.PartialRecovery {
		dst.PartialRecovery = true
	}
	return dst
}

func mergeEntityEnvelope(dst, src *EntityExtractionEnvelope, seenEnt, seenRel map[string]bool) *EntityExtractionEnvelope {
	if src == nil || src.Result == nil {
		return dst
	}
	if dst == nil {
		dst = &EntityExtractionEnvelope{
			Result:       &EntityExtractionResult{},
			Model:        src.Model,
			ProviderName: src.ProviderName,
		}
	}
	for _, e := range src.Result.Entities {
		k := entityKey(e)
		if k == "" || seenEnt[k] {
			continue
		}
		seenEnt[k] = true
		dst.Result.Entities = append(dst.Result.Entities, e)
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

func addUsage(dst *provider.TokenUsage, src provider.TokenUsage) {
	dst.PromptTokens += src.PromptTokens
	dst.CompletionTokens += src.CompletionTokens
	dst.TotalTokens += src.TotalTokens
}

func factKey(f ExtractedFact) string {
	return strings.ToLower(strings.TrimSpace(f.text()))
}

func entityKey(e ExtractedEntityData) string {
	name := strings.ToLower(strings.TrimSpace(e.Name))
	if name == "" {
		return ""
	}
	return name + "\x00" + strings.ToLower(strings.TrimSpace(e.Type))
}

func relationKey(r ExtractedRelation) string {
	s := strings.ToLower(strings.TrimSpace(r.Source))
	t := strings.ToLower(strings.TrimSpace(r.Target))
	if s == "" || t == "" {
		return ""
	}
	return s + "\x00" + strings.ToLower(strings.TrimSpace(r.Relation)) + "\x00" + t
}

// scrubEntityResult drops entities whose name is degenerate and relationships
// whose source or target name is degenerate, in place. A degenerate endpoint and
// its entity fail the same predicate, so dropping such relationships also stops
// the worker from re-creating the garbage name as a stub from the relationship.
func scrubEntityResult(res *EntityExtractionResult, maxChars, maxWords int, minRatio float64) {
	if res == nil {
		return
	}
	if len(res.Entities) > 0 {
		kept := res.Entities[:0]
		for _, e := range res.Entities {
			if IsDegenerateEntityName(e.Name, maxChars, maxWords, minRatio) {
				continue
			}
			kept = append(kept, e)
		}
		res.Entities = kept
	}
	if len(res.Relationships) > 0 {
		kept := res.Relationships[:0]
		for _, r := range res.Relationships {
			if IsDegenerateEntityName(r.Source, maxChars, maxWords, minRatio) ||
				IsDegenerateEntityName(r.Target, maxChars, maxWords, minRatio) {
				continue
			}
			kept = append(kept, r)
		}
		res.Relationships = kept
	}
}

// CapEntityResult bounds a single memory's extracted entities to maxEntities,
// in place, and reports how many were dropped. The per-name guards judge one
// name at a time; this is the volume clamp that catches an enumeration-flood of
// individually valid names. It is a no-op (returns 0) when maxEntities <= 0 or
// the entity count is already within the cap, so the common path and the
// legitimate relationship stub-creation path are untouched.
//
// When the cap fires it keeps the first maxEntities entities in extraction order
// and drops any relationship whose source or target is no longer among the kept
// entities (case-insensitive, trimmed) — otherwise a dropped entity would be
// re-created downstream as a stub from a surviving relationship endpoint,
// defeating the clamp.
func CapEntityResult(res *EntityExtractionResult, maxEntities int) int {
	if res == nil || maxEntities <= 0 || len(res.Entities) <= maxEntities {
		return 0
	}
	dropped := len(res.Entities) - maxEntities
	res.Entities = res.Entities[:maxEntities]
	if len(res.Relationships) > 0 {
		norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
		keptNames := make(map[string]struct{}, len(res.Entities))
		for _, e := range res.Entities {
			keptNames[norm(e.Name)] = struct{}{}
		}
		kept := func(name string) bool { _, ok := keptNames[norm(name)]; return ok }
		keptRels := res.Relationships[:0]
		for _, r := range res.Relationships {
			if !kept(r.Source) || !kept(r.Target) {
				continue
			}
			keptRels = append(keptRels, r)
		}
		res.Relationships = keptRels
	}
	return dropped
}
