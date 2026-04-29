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
// RepeatPenalty / TopK / MinP are Ollama extensions; nil pointer = omit.
type CallOptions struct {
	MaxTokens     int
	Temperature   float64
	RepeatPenalty *float64
	TopK          *int
	MinP          *float64
}

// callOptionKeys names the four settings keys that vary per
// (phase, sync-or-async-temperature) tuple. ResolveCallOptions reads each
// in turn so all four extraction call sites share one resolution body.
type callOptionKeys struct {
	MaxTokens     string
	Temperature   string
	RepeatPenalty string
	TopK          string
	MinP          string
}

// FactCallOptionKeys / EntityCallOptionKeys return the keys for the named
// extraction phase. sync==true selects the sync-HTTP-path temperature key;
// false selects the async-worker-path key. Reading separate temperature
// keys per path preserves the pre-refactor 0.1/0.2 split — operators
// converge by setting both keys equal.
func FactCallOptionKeys(sync bool) callOptionKeys {
	tmp := SettingFactExtractionAsyncTemperature
	if sync {
		tmp = SettingFactExtractionSyncTemperature
	}
	return callOptionKeys{
		MaxTokens:     SettingFactExtractionMaxTokens,
		Temperature:   tmp,
		RepeatPenalty: SettingFactExtractionRepeatPenalty,
		TopK:          SettingFactExtractionTopK,
		MinP:          SettingFactExtractionMinP,
	}
}

func EntityCallOptionKeys(sync bool) callOptionKeys {
	tmp := SettingEntityExtractionAsyncTemperature
	if sync {
		tmp = SettingEntityExtractionSyncTemperature
	}
	return callOptionKeys{
		MaxTokens:     SettingEntityExtractionMaxTokens,
		Temperature:   tmp,
		RepeatPenalty: SettingEntityExtractionRepeatPenalty,
		TopK:          SettingEntityExtractionTopK,
		MinP:          SettingEntityExtractionMinP,
	}
}

// ResolveCallOptions reads the five extraction tunables from the settings
// cascade. RepeatPenalty / TopK / MinP land as nil when their resolved
// value is non-positive (the zero-as-omit contract).
func ResolveCallOptions(ctx context.Context, s *SettingsService, keys callOptionKeys) CallOptions {
	opts := CallOptions{
		MaxTokens:   s.ResolveIntWithDefault(ctx, keys.MaxTokens, "global"),
		Temperature: s.ResolveFloatWithDefault(ctx, keys.Temperature, "global"),
	}
	if rp := s.ResolveFloatWithDefault(ctx, keys.RepeatPenalty, "global"); rp > 0 {
		opts.RepeatPenalty = &rp
	}
	if k := s.ResolveIntWithDefault(ctx, keys.TopK, "global"); k > 0 {
		opts.TopK = &k
	}
	if mp := s.ResolveFloatWithDefault(ctx, keys.MinP, "global"); mp > 0 {
		opts.MinP = &mp
	}
	return opts
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
	ExtractionPhaseFact   = "fact_extraction"
	ExtractionPhaseEntity = "entity_extraction"
)

// Extraction failure reasons written into ExtractionFailure.Reason. Stable
// strings so admin tooling can switch on them.
const (
	ExtractionReasonLLMCallFailed   = "llm_call_failed"
	ExtractionReasonParseFailed     = "parse_failed"
	ExtractionReasonLengthNoRecover = "length_no_recovery"
	ExtractionReasonPartialRecovery = "partial_recovery"
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
	var fail *ExtractionFailure
	if errors.As(err, &fail) {
		return fail, true
	}
	return nil, false
}

// buildExtractionRequest constructs the LLM request body shared by the
// fact and entity helpers.
func buildExtractionRequest(prompt string, opts CallOptions) *provider.CompletionRequest {
	return &provider.CompletionRequest{
		Messages:      []provider.Message{{Role: "user", Content: prompt}},
		MaxTokens:     opts.MaxTokens,
		Temperature:   opts.Temperature,
		JSONMode:      true,
		RepeatPenalty: opts.RepeatPenalty,
		TopK:          opts.TopK,
		MinP:          opts.MinP,
	}
}

// ExtractFactsLLM runs the fact-extraction prompt and parses the response.
// Returns *ExtractionFailure on call or parse failure (use errors.As).
func ExtractFactsLLM(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*FactExtractionEnvelope, error) {
	prompt := fmt.Sprintf(ResolveOrDefault(ctx, settings, SettingFactPrompt, "global"), content)
	req := buildExtractionRequest(prompt, opts)

	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationFactExtraction), req)
	if err != nil {
		return nil, buildExtractionFailure(ExtractionPhaseFact, ExtractionReasonLLMCallFailed, err.Error(), nil, llm.Name())
	}

	facts, partial, parseErr := parseFacts(resp.Content)
	if parseErr != nil {
		return nil, buildExtractionFailure(ExtractionPhaseFact, ExtractionReasonParseFailed, parseErr.Error(), resp, llm.Name())
	}

	if partial && len(facts) == 0 && resp.FinishReason == "length" {
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

// ExtractEntitiesLLM runs the entity-extraction prompt and parses the response.
// Returns *ExtractionFailure on call or parse failure (use errors.As).
func ExtractEntitiesLLM(
	ctx context.Context,
	llm provider.LLMProvider,
	settings *SettingsService,
	content string,
	opts CallOptions,
) (*EntityExtractionEnvelope, error) {
	prompt := fmt.Sprintf(ResolveOrDefault(ctx, settings, SettingEntityPrompt, "global"), content)
	req := buildExtractionRequest(prompt, opts)

	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationEntityExtraction), req)
	if err != nil {
		return nil, buildExtractionFailure(ExtractionPhaseEntity, ExtractionReasonLLMCallFailed, err.Error(), nil, llm.Name())
	}

	result, partial, parseErr := parseEntities(resp.Content)
	if parseErr != nil {
		return nil, buildExtractionFailure(ExtractionPhaseEntity, ExtractionReasonParseFailed, parseErr.Error(), resp, llm.Name())
	}

	emptyResult := result == nil ||
		(len(result.Entities) == 0 && len(result.Relationships) == 0)
	if partial && emptyResult && resp.FinishReason == "length" {
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

// parseFacts handles array, single-object, and wrapper shapes; falls
// through to longest-valid-prefix recovery on truncation. Recovered facts
// are deduped case-insensitively to defeat degenerate-loop output.
func parseFacts(raw string) (facts []ExtractedFact, partialRecovery bool, err error) {
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
		// Empty recovery — surface as failure so the caller can decide
		// whether to mark length_no_recovery.
		return nil, true, nil
	}
	normalizeFacts(recovered)
	return dedupeFacts(recovered), true, nil
}

// parseEntities parses an entity/relationship extraction response. Clean
// parses go through json.Unmarshal; truncated responses fall through to
// per-array longest-valid-prefix recovery on the "entities" and
// "relationships" keys independently — a truncation that severs the
// "relationships" array still recovers all complete "entities" entries.
func parseEntities(raw string) (result *EntityExtractionResult, partialRecovery bool, err error) {
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
			// Truncated key — return what we have.
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
