package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nram-ai/nram/internal/provider"
)

// CallOptions controls per-invocation LLM parameters for the extraction
// helpers. Resolved per call so changes hot-reload within the cache TTL.
type CallOptions struct {
	MaxTokens   int
	Temperature float64
}

// callOptionKeys names the per-call settings keys that vary per
// (phase, sync-or-async-temperature) tuple. ResolveCallOptions reads each
// in turn so all four extraction call sites share one resolution body.
type callOptionKeys struct {
	MaxTokens   string
	Temperature string
}

// FactCallOptionKeys / EntityCallOptionKeys return the keys for the named
// extraction phase. sync==true selects the sync-HTTP-path temperature key;
// false selects the async-worker-path key. Reading separate temperature
// keys per path preserves the pre-refactor 0.1/0.2 split; operators
// converge by setting both keys equal.
func FactCallOptionKeys(sync bool) callOptionKeys {
	tmp := SettingFactExtractionAsyncTemperature
	if sync {
		tmp = SettingFactExtractionSyncTemperature
	}
	return callOptionKeys{
		MaxTokens:   SettingFactExtractionMaxTokens,
		Temperature: tmp,
	}
}

func EntityCallOptionKeys(sync bool) callOptionKeys {
	tmp := SettingEntityExtractionAsyncTemperature
	if sync {
		tmp = SettingEntityExtractionSyncTemperature
	}
	return callOptionKeys{
		MaxTokens:   SettingEntityExtractionMaxTokens,
		Temperature: tmp,
	}
}

// RelationshipCallOptionKeys returns the keys for the relationship-extraction
// pass (pass 2). The relationship pass gets its own max_tokens budget so it is
// never starved by a dense entity pass that truncated. sync selects the
// request-path temperature key; false selects the async-worker key.
func RelationshipCallOptionKeys(sync bool) callOptionKeys {
	tmp := SettingRelationshipExtractionAsyncTemperature
	if sync {
		tmp = SettingRelationshipExtractionSyncTemperature
	}
	return callOptionKeys{
		MaxTokens:   SettingRelationshipExtractionMaxTokens,
		Temperature: tmp,
	}
}

// ResolveCallOptions reads the extraction tunables from the settings cascade.
func ResolveCallOptions(ctx context.Context, s *SettingsService, keys callOptionKeys) CallOptions {
	return CallOptions{
		MaxTokens:   s.ResolveIntWithDefault(ctx, keys.MaxTokens, "global"),
		Temperature: s.ResolveFloatWithDefault(ctx, keys.Temperature, "global"),
	}
}

// FactExtractionEnvelope carries the parsed result and the diagnostic
// metadata callers may want to thread into structured failure rows.
type FactExtractionEnvelope struct {
	Facts           []ExtractedFact
	Usage           provider.TokenUsage
	Model           string
	ProviderName    string
	FinishReason    string
	PartialRecovery bool
	RawResponse     string
}

// EntityExtractionEnvelope is the entity counterpart to FactExtractionEnvelope.
type EntityExtractionEnvelope struct {
	Result          *EntityExtractionResult
	Usage           provider.TokenUsage
	Model           string
	ProviderName    string
	FinishReason    string
	PartialRecovery bool
	RawResponse     string
}

// RelationExtractionEnvelope is the relationship-pass counterpart to
// EntityExtractionEnvelope. It carries the pass-2 (relationship-only) result and
// the same diagnostic metadata so the worker can record an independent
// relationship_extraction leg in the partial-recovery warning.
type RelationExtractionEnvelope struct {
	Result          *RelationExtractionResult
	Usage           provider.TokenUsage
	Model           string
	ProviderName    string
	FinishReason    string
	PartialRecovery bool
	RawResponse     string
}

// ExtractionFailure is the structured payload written to
// enrichment_queue.last_error so admin views can distinguish cap-hit,
// malformed JSON, and degenerate-loop outcomes without re-running the call.
// Implements error so it flows through fmt.Errorf %w / errors.As.
type ExtractionFailure struct {
	Phase            string `json:"phase"`
	Reason           string `json:"reason"`
	Detail           string `json:"error"`
	FinishReason     string `json:"finish_reason,omitempty"`
	PromptTokens     int    `json:"prompt_tokens,omitempty"`
	CompletionTokens int    `json:"completion_tokens,omitempty"`
	Model            string `json:"model,omitempty"`
	Provider         string `json:"provider,omitempty"`
	RawResponse      string `json:"raw_response,omitempty"`
}

// Extraction phase tags written into ExtractionFailure.Phase.
const (
	ExtractionPhaseFact         = "fact_extraction"
	ExtractionPhaseEntity       = "entity_extraction"
	ExtractionPhaseRelationship = "relationship_extraction"
)

// Extraction failure reasons written into ExtractionFailure.Reason. Stable
// strings so admin tooling can switch on them.
const (
	ExtractionReasonLLMCallFailed   = "llm_call_failed"
	ExtractionReasonParseFailed     = "parse_failed"
	ExtractionReasonLengthNoRecover = "length_no_recovery"
	ExtractionReasonPartialRecovery = "partial_recovery"
	ExtractionReasonEmptyResponse   = "empty_response"
)

// Error implements the error interface.
func (e *ExtractionFailure) Error() string {
	if e == nil {
		return "<nil ExtractionFailure>"
	}
	if e.Detail != "" {
		return fmt.Sprintf("%s/%s: %s", e.Phase, e.Reason, e.Detail)
	}
	return fmt.Sprintf("%s/%s", e.Phase, e.Reason)
}

// buildExtractionFailure constructs a parse/length-no-recovery failure with
// the diagnostic fields the queue row needs. resp may be nil for call-site
// failures that don't have a CompletionResponse yet.
func buildExtractionFailure(phase, reason, detail string, resp *provider.CompletionResponse, providerName string) *ExtractionFailure {
	f := &ExtractionFailure{
		Phase:    phase,
		Reason:   reason,
		Detail:   detail,
		Provider: providerName,
	}
	if resp != nil {
		f.FinishReason = resp.FinishReason
		f.PromptTokens = resp.Usage.PromptTokens
		f.CompletionTokens = resp.Usage.CompletionTokens
		f.Model = resp.Model
		f.RawResponse = resp.Content
	}
	return f
}

// AsExtractionFailure unwraps err to *ExtractionFailure if present.
func AsExtractionFailure(err error) (*ExtractionFailure, bool) {
	if fail, ok := errors.AsType[*ExtractionFailure](err); ok {
		return fail, true
	}
	return nil, false
}

// buildExtractionRequest constructs the LLM request body shared by the
// fact and entity helpers.
func buildExtractionRequest(messages []provider.Message, opts CallOptions) *provider.CompletionRequest {
	return &provider.CompletionRequest{
		Messages:    messages,
		MaxTokens:   opts.MaxTokens,
		Temperature: provider.Float64(opts.Temperature),
		JSONMode:    true,
	}
}

// RenderExtractionUser builds the user message for the fact- and
// entity-extraction phases: the raw input content under a "Text:" label. This
// dynamic wrapper is code, not a setting; the tunable instruction (role, rules,
// and "return only JSON" contract) lives entirely in the *_system_prompt key,
// sent as the system message. Exported so the admin test surface renders the
// exact user message the runtime phases send.
func RenderExtractionUser(content string) string {
	return provider.Fence("text", content)
}

// RenderRelationshipUser builds the user message for the relationship-extraction
// pass: the source text plus the entity names extracted in pass 1, each
// nonce-fenced so neither body can forge the other's delimiter. The entity names
// are one per line. Like RenderExtractionUser this dynamic wrapper is code, not a
// setting; the tunable instruction lives in SettingRelationshipSystemPrompt.
// Exported so the admin test surface renders the exact user message the runtime
// relationship pass sends.
func RenderRelationshipUser(content string, entityNames []string) string {
	return provider.Fence("text", content) + "\n\n" + provider.Fence("entities", strings.Join(entityNames, "\n"))
}

// RenderContradictionUser builds the user message for the contradiction check.
// Shared by the dreaming contradiction phase and the enrichment conflict
// detector (which also share SettingDreamContradictionSystemPrompt). Each
// statement is nonce-fenced so neither body can forge the other's delimiter.
func RenderContradictionUser(statementA, statementB string) string {
	return provider.Fence("statement_a", statementA) + "\n\n" + provider.Fence("statement_b", statementB)
}

// extractFactsOnce runs the fact-extraction prompt once over the given content
// and parses the response. ExtractFactsLLM wraps it with chunking + continuation.
// Returns *ExtractionFailure on call or parse failure (use errors.As).
func extractFactsOnce(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*FactExtractionEnvelope, error) {
	system := ResolveOrDefault(ctx, settings, SettingFactSystemPrompt, "global")
	user := RenderExtractionUser(content)
	messages := provider.BuildGuardedMessages(system, user)
	req := buildExtractionRequest(messages, opts)

	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationFactExtraction), req)
	if err != nil {
		return nil, buildExtractionFailure(ExtractionPhaseFact, ExtractionReasonLLMCallFailed, err.Error(), nil, llm.Name())
	}

	// An empty completion is never a valid extraction (parseFacts would
	// otherwise return zero facts with no error, silently recording success).
	// Surface it as a failure for every provider, not just Anthropic.
	if strings.TrimSpace(resp.Content) == "" {
		return nil, buildExtractionFailure(ExtractionPhaseFact, ExtractionReasonEmptyResponse,
			"fact extraction returned an empty response body", resp, llm.Name())
	}

	facts, partial, parseErr := parseFacts(resp.Content)
	if parseErr != nil {
		return nil, buildExtractionFailure(ExtractionPhaseFact, ExtractionReasonParseFailed, parseErr.Error(), resp, llm.Name())
	}

	if partial && len(facts) == 0 && provider.IsTruncated(resp.FinishReason) {
		return nil, buildExtractionFailure(ExtractionPhaseFact, ExtractionReasonLengthNoRecover,
			"fact extraction hit max_tokens and longest-valid-prefix recovery yielded zero facts",
			resp, llm.Name())
	}

	return &FactExtractionEnvelope{
		Facts:           facts,
		Usage:           resp.Usage,
		Model:           resp.Model,
		ProviderName:    llm.Name(),
		FinishReason:    resp.FinishReason,
		PartialRecovery: partial,
		RawResponse:     resp.Content,
	}, nil
}

// extractEntitiesOnce runs the entity-extraction prompt once over the given
// content. ExtractEntitiesLLM wraps it with chunking + continuation.
// Returns *ExtractionFailure on call or parse failure (use errors.As).
func extractEntitiesOnce(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*EntityExtractionEnvelope, error) {
	system := ResolveOrDefault(ctx, settings, SettingEntitySystemPrompt, "global")
	user := RenderExtractionUser(content)
	messages := provider.BuildGuardedMessages(system, user)
	req := buildExtractionRequest(messages, opts)

	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationEntityExtraction), req)
	if err != nil {
		return nil, buildExtractionFailure(ExtractionPhaseEntity, ExtractionReasonLLMCallFailed, err.Error(), nil, llm.Name())
	}

	// An empty completion is never a valid extraction (parseEntities would
	// otherwise return an empty result with no error, silently recording
	// success). Surface it as a failure for every provider, not just Anthropic.
	if strings.TrimSpace(resp.Content) == "" {
		return nil, buildExtractionFailure(ExtractionPhaseEntity, ExtractionReasonEmptyResponse,
			"entity extraction returned an empty response body", resp, llm.Name())
	}

	result, partial, parseErr := parseEntities(resp.Content)
	if parseErr != nil {
		return nil, buildExtractionFailure(ExtractionPhaseEntity, ExtractionReasonParseFailed, parseErr.Error(), resp, llm.Name())
	}

	emptyResult := result == nil ||
		(len(result.Entities) == 0 && len(result.Relationships) == 0)
	if partial && emptyResult && provider.IsTruncated(resp.FinishReason) {
		return nil, buildExtractionFailure(ExtractionPhaseEntity, ExtractionReasonLengthNoRecover,
			"entity extraction hit max_tokens and longest-valid-prefix recovery yielded zero entities",
			resp, llm.Name())
	}

	return &EntityExtractionEnvelope{
		Result:          result,
		Usage:           resp.Usage,
		Model:           resp.Model,
		ProviderName:    llm.Name(),
		FinishReason:    resp.FinishReason,
		PartialRecovery: partial,
		RawResponse:     resp.Content,
	}, nil
}

// ---------------------------------------------------------------------------
// Parsers (clean parse + longest-valid-prefix recovery)
// ---------------------------------------------------------------------------

// parseFacts parses the raw response, retrying on a de-fenced copy if the raw
// parse fails (see deFenceRetry). An empty body is treated as a clean
// zero-fact result here; the ExtractFactsLLM envelope is the layer that
// distinguishes an empty completion (a failure) from a valid empty result.
func parseFacts(raw string) ([]ExtractedFact, bool, error) {
	return deFenceRetry(raw, parseFactsRaw)
}

// parseFactsRaw handles array, single-object, and wrapper shapes; falls
// through to longest-valid-prefix recovery on truncation. Recovered facts
// are deduped case-insensitively to defeat degenerate-loop output.
func parseFactsRaw(raw string) (facts []ExtractedFact, partialRecovery bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}

	// Array of structured facts.
	var arr []ExtractedFact
	if uerr := json.Unmarshal([]byte(raw), &arr); uerr == nil {
		if len(arr) == 0 {
			return nil, false, nil
		}
		normalizeFacts(arr)
		return arr, false, nil
	}

	// Single fact object.
	var single ExtractedFact
	if uerr := json.Unmarshal([]byte(raw), &single); uerr == nil && single.text() != "" {
		single.Fact = single.text()
		return []ExtractedFact{single}, false, nil
	}

	// Wrapper {"facts": [...]}.
	var wrapper struct {
		Facts []ExtractedFact `json:"facts"`
	}
	if uerr := json.Unmarshal([]byte(raw), &wrapper); uerr == nil {
		if len(wrapper.Facts) > 0 {
			normalizeFacts(wrapper.Facts)
			return wrapper.Facts, false, nil
		}
		// Valid empty wrapper.
		if strings.HasPrefix(raw, "{") {
			return nil, false, nil
		}
	}

	// Recovery path. Try longest-valid-prefix on the array shape.
	recovered, recErr := recoverFactsArrayPrefix(raw)
	if recErr != nil {
		return nil, false, fmt.Errorf("failed to parse fact extraction response as JSON: %w", recErr)
	}
	if len(recovered) == 0 {
		// Empty recovery: surface as failure so the caller can decide
		// whether to mark length_no_recovery.
		return nil, true, nil
	}
	normalizeFacts(recovered)
	return dedupeFacts(recovered), true, nil
}

// parseEntities parses an entity/relationship extraction response. Clean
// parses go through json.Unmarshal; truncated responses fall through to
// per-array longest-valid-prefix recovery on the "entities" and
// "relationships" keys independently: a truncation that severs the
// "relationships" array still recovers all complete "entities" entries.
// parseEntities parses the raw response, retrying on a de-fenced copy if the
// raw parse fails (see deFenceRetry). As with parseFacts, the empty-completion
// failure distinction lives in the ExtractEntitiesLLM envelope, not here.
func parseEntities(raw string) (*EntityExtractionResult, bool, error) {
	return deFenceRetry(raw, parseEntitiesRaw)
}

func parseEntitiesRaw(raw string) (result *EntityExtractionResult, partialRecovery bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return &EntityExtractionResult{}, false, nil
	}

	var clean EntityExtractionResult
	if uerr := json.Unmarshal([]byte(raw), &clean); uerr == nil {
		return &clean, false, nil
	}

	rec, recErr := recoverEntitiesObjectPrefix(raw)
	if recErr != nil {
		return nil, false, fmt.Errorf("failed to parse entity extraction response as JSON: %w", recErr)
	}
	return rec, true, nil
}

// parseRelationships parses a relationship-only extraction response (pass 2),
// retrying on a de-fenced copy if the raw parse fails (see deFenceRetry). It is
// the relationship-pass counterpart to parseEntities; the empty-completion
// failure distinction lives in extractRelationshipsOnce, not here.
func parseRelationships(raw string) (*RelationExtractionResult, bool, error) {
	return deFenceRetry(raw, parseRelationshipsRaw)
}

func parseRelationshipsRaw(raw string) (result *RelationExtractionResult, partialRecovery bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return &RelationExtractionResult{}, false, nil
	}

	var clean RelationExtractionResult
	if uerr := json.Unmarshal([]byte(raw), &clean); uerr == nil {
		return &clean, false, nil
	}

	rec, recErr := recoverRelationshipsObjectPrefix(raw)
	if recErr != nil {
		return nil, false, fmt.Errorf("failed to parse relationship extraction response as JSON: %w", recErr)
	}
	return rec, true, nil
}

// recoverRelationshipsObjectPrefix walks an object containing an optional
// "relationships" array, recovering whatever was cleanly decoded from it. It is
// the relationship-only counterpart to recoverEntitiesObjectPrefix and reuses
// streamRelationArray for the array prefix.
func recoverRelationshipsObjectPrefix(raw string) (*RelationExtractionResult, error) {
	dec := json.NewDecoder(strings.NewReader(raw))

	first, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := first.(json.Delim)
	if !ok || delim != '{' {
		return nil, fmt.Errorf("expected JSON object, got %v", first)
	}

	out := &RelationExtractionResult{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			// Truncated key: return what we have.
			return out, nil
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected object key, got %T", keyTok)
		}

		switch key {
		case "relationships":
			openTok, err := dec.Token()
			if err != nil {
				return out, nil
			}
			d, ok := openTok.(json.Delim)
			if !ok || d != '[' {
				return nil, fmt.Errorf("\"relationships\" is not a JSON array")
			}
			out.Relationships = streamRelationArray(dec)
			if !consumeArrayClose(dec) {
				// Array truncated mid-element; decoder state is unrecoverable.
				return out, nil
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return out, nil
			}
		}
	}
	return out, nil
}

// normalizeFacts copies the canonical text into both Fact and Content so
// callers can read whichever field they prefer. Different prompts populate
// different keys (the canonical prompt uses "content"; legacy callers read
// "fact"); writing both eliminates branch-by-prompt at every consumer.
func normalizeFacts(facts []ExtractedFact) {
	for i := range facts {
		t := facts[i].text()
		facts[i].Fact = t
		facts[i].Content = t
	}
}

// dedupeFacts collapses recovered facts by lower-cased trimmed Content.
// Defensive against the degenerate-loop pattern observed on small qwen
// models: the model emits a handful of legitimate facts then loops the
// same cluster until max_tokens cuts.
func dedupeFacts(facts []ExtractedFact) []ExtractedFact {
	seen := make(map[string]bool, len(facts))
	out := make([]ExtractedFact, 0, len(facts))
	for _, f := range facts {
		key := strings.ToLower(strings.TrimSpace(f.text()))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, f)
	}
	return out
}

// recoverFactsArrayPrefix walks the response with json.Decoder, returning
// every cleanly-decoded ExtractedFact up to the first decode error. Handles
// both bare-array (`[ {...}, {...} ]`) and wrapper (`{"facts": [...] }`)
// shapes; for wrapper, advances tokens until the "facts" key is reached.
func recoverFactsArrayPrefix(raw string) ([]ExtractedFact, error) {
	dec := json.NewDecoder(strings.NewReader(raw))

	first, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := first.(json.Delim)
	if !ok {
		return nil, fmt.Errorf("expected JSON object or array, got %T", first)
	}

	switch delim {
	case '[':
		return streamFactArray(dec), nil
	case '{':
		return streamFactsFromObject(dec)
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}

// streamFactsFromObject walks an object's keys looking for "facts", then
// streams its array prefix. Other keys' values are skipped via RawMessage
// so a malformed value before "facts" is detected; a truncation inside
// "facts" is the recovery target.
func streamFactsFromObject(dec *json.Decoder) ([]ExtractedFact, error) {
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected object key, got %T", keyTok)
		}
		if key == "facts" {
			openTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			d, ok := openTok.(json.Delim)
			if !ok || d != '[' {
				return nil, fmt.Errorf("\"facts\" is not a JSON array")
			}
			return streamFactArray(dec), nil
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("no \"facts\" key found in object")
}

// streamFactArray decodes ExtractedFact entries from the decoder's current
// position (which must be inside a `[`). Stops on the first decode error
// (truncation), returning whatever was successfully decoded.
func streamFactArray(dec *json.Decoder) []ExtractedFact {
	var out []ExtractedFact
	for dec.More() {
		var f ExtractedFact
		if err := dec.Decode(&f); err != nil {
			break
		}
		out = append(out, f)
	}
	return out
}

// consumeArrayClose drains the closing ']' of the array the decoder is
// currently inside. Returns whether the close was consumed cleanly; on
// truncation (ErrUnexpectedEOF or syntax error) the decoder state is
// unrecoverable and the caller must not continue parsing siblings.
func consumeArrayClose(dec *json.Decoder) (clean bool) {
	tok, err := dec.Token()
	if err != nil {
		return false
	}
	d, ok := tok.(json.Delim)
	return ok && d == ']'
}

// recoverEntitiesObjectPrefix walks an object containing optional "entities"
// and "relationships" arrays, recovering whatever was cleanly decoded from
// each. A truncation in either array does not poison the other.
func recoverEntitiesObjectPrefix(raw string) (*EntityExtractionResult, error) {
	dec := json.NewDecoder(strings.NewReader(raw))

	first, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := first.(json.Delim)
	if !ok || delim != '{' {
		return nil, fmt.Errorf("expected JSON object, got %v", first)
	}

	out := &EntityExtractionResult{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			// Truncated key: return what we have.
			return out, nil
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected object key, got %T", keyTok)
		}

		switch key {
		case "entities":
			openTok, err := dec.Token()
			if err != nil {
				return out, nil
			}
			d, ok := openTok.(json.Delim)
			if !ok || d != '[' {
				return nil, fmt.Errorf("\"entities\" is not a JSON array")
			}
			out.Entities = streamEntityArray(dec)
			if !consumeArrayClose(dec) {
				// Entities array truncated mid-element. Decoder state is
				// unrecoverable; return what we have so far.
				return out, nil
			}
		case "relationships":
			openTok, err := dec.Token()
			if err != nil {
				return out, nil
			}
			d, ok := openTok.(json.Delim)
			if !ok || d != '[' {
				return nil, fmt.Errorf("\"relationships\" is not a JSON array")
			}
			out.Relationships = streamRelationArray(dec)
			if !consumeArrayClose(dec) {
				return out, nil
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return out, nil
			}
		}
	}
	return out, nil
}

func streamEntityArray(dec *json.Decoder) []ExtractedEntityData {
	var out []ExtractedEntityData
	for dec.More() {
		var e ExtractedEntityData
		if err := dec.Decode(&e); err != nil {
			break
		}
		out = append(out, e)
	}
	return out
}

func streamRelationArray(dec *json.Decoder) []ExtractedRelation {
	var out []ExtractedRelation
	for dec.More() {
		var r ExtractedRelation
		if err := dec.Decode(&r); err != nil {
			break
		}
		out = append(out, r)
	}
	return out
}

// StripCodeFence trims a UTF-8 BOM and surrounding whitespace, then removes a
// leading triple-backtick or triple-tilde fence (with an optional language tag
// like ```json) and a matching trailing fence. Trailing prose after the closing
// fence is discarded — the closing fence is located by the FIRST line-anchored
// "\n```" (or "\n~~~") so a second fenced block in the trailing prose
// ("FYI: ```py\nprint(1)\n```") does not leak into the body.
//
// It also recovers from a preamble before the fence: if the input does not
// start with a fence but contains one at a line start ("Here you go:\n```json\n…"),
// the function re-anchors at that line. Single-line payloads carry the language
// tag inline (```json{"x":1}```); the tag is stripped, unless doing so would
// consume the entire body (e.g. a bare ```123```), in which case the untrimmed
// body is kept.
//
// Inputs without a fence are returned trimmed unchanged, so callers can run it
// as a fallback after a raw parse without risking corruption of valid JSON.
func StripCodeFence(s string) string {
	// Strip a UTF-8 BOM if present, before any whitespace trimming.
	s = strings.TrimPrefix(s, "\ufeff")
	s = strings.TrimSpace(s)

	fence, ok := detectLeadingFence(s)
	if !ok {
		// Re-anchor on a preamble: the first line-anchored fence after a prose
		// lead-in. Backtick fences take precedence over tilde.
		if i := strings.Index(s, "\n```"); i >= 0 {
			s = s[i+1:]
			fence = "```"
		} else if i := strings.Index(s, "\n~~~"); i >= 0 {
			s = s[i+1:]
			fence = "~~~"
		} else {
			return s
		}
	}

	// Split off the opener line; everything after the first newline is the body.
	_, body, multiline := strings.Cut(s, "\n")
	if !multiline {
		// Single-line fenced payload like ```{"x":1}``` or ```json{"x":1}```.
		// Strip both fences first, then the language tag — but only if doing so
		// leaves a non-empty body (so a bare ```123``` keeps "123").
		single := strings.TrimPrefix(s, fence)
		single = strings.TrimSuffix(single, fence)
		if trimmed := trimLanguageTag(single); trimmed != "" {
			single = trimmed
		}
		return strings.TrimSpace(single)
	}

	// Multi-line: the opener line (carrying any language hint) is already
	// dropped; search for the FIRST line-anchored closing fence so trailing
	// prose containing another fenced block does not bleed into the body.
	if idx := strings.Index(body, "\n"+fence); idx >= 0 {
		body = body[:idx]
	} else {
		// No line-anchored close; tolerate a bare trailing fence and otherwise
		// treat the rest as the body (a truncated stream).
		body = strings.TrimSuffix(body, fence)
	}
	return strings.TrimSpace(body)
}

// detectLeadingFence reports the fence marker that opens s (``` or ~~~), or
// false when s has no leading fence.
func detectLeadingFence(s string) (string, bool) {
	switch {
	case strings.HasPrefix(s, "```"):
		return "```", true
	case strings.HasPrefix(s, "~~~"):
		return "~~~", true
	default:
		return "", false
	}
}

// trimLanguageTag drops a leading alphanumeric language hint like "json" or
// "json5" that appears immediately after the opening fence on a single-line
// payload, e.g. ```json{"x":1}``` → {"x":1}```.
func trimLanguageTag(s string) string {
	return strings.TrimLeftFunc(s, func(r rune) bool {
		return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
	})
}

// UnmarshalJSONLenient unmarshals raw into dst, retrying on the de-fenced body
// if the raw parse fails and raw carries a markdown code fence. Raw-first means
// valid JSON never passes through StripCodeFence, while a provider or relay that
// wraps the JSON in a fence still parses. Use it wherever an LLM JSON response
// is decoded into a single value; the fact/entity parsers use deFenceRetry
// instead because they also return a partial-recovery flag.
func UnmarshalJSONLenient(raw string, dst any) error {
	err := json.Unmarshal([]byte(raw), dst)
	if err == nil {
		return nil
	}
	if stripped := StripCodeFence(raw); stripped != strings.TrimSpace(raw) {
		return json.Unmarshal([]byte(stripped), dst)
	}
	return err
}

// deFenceRetry runs parse on raw, retrying on the de-fenced body if the raw
// parse fails and raw carries a fence. It is the (T, bool, error) sibling of
// UnmarshalJSONLenient, shared by parseFacts/parseEntities whose parsers carry
// a partial-recovery flag and so don't fit a plain json.Unmarshal signature.
func deFenceRetry[T any](raw string, parse func(string) (T, bool, error)) (T, bool, error) {
	v, partial, err := parse(raw)
	if err != nil {
		if stripped := StripCodeFence(raw); stripped != strings.TrimSpace(raw) {
			if v2, p2, e2 := parse(stripped); e2 == nil {
				return v2, p2, nil
			}
		}
	}
	return v, partial, err
}
