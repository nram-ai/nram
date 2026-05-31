package enrichment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
)

// QueryAugmentSeparator marks the boundary between generated paraphrase queries
// and the original memory content inside the augmented embedding input. Kept
// exported so the preview UI can render the exact string the embedder sees and
// so tests can assert the split deterministically.
const QueryAugmentSeparator = "\n---\n"

// queryAugmentSettings is the snapshot taken once per phase invocation so the
// resolver lookups happen once and the rest of the phase reads from a local
// struct.
type queryAugmentSettings struct {
	enabled       bool
	count         int
	model         string
	prompt        string
	maxInputChars int
	maxTokens     int
}

// QueryAugmentDefaultMaxTokens is the fallback completion-token cap used when
// enrichment.query_augment.max_tokens is unset or non-positive. Mirrors the
// default registered in internal/storage/admin/settings_store.go so changes
// to one site cannot silently drift from the other.
const QueryAugmentDefaultMaxTokens = 2048

// queryAugmentResult is the in-memory product of one query-augmentation phase
// invocation. The worker stamps Queries and AugmentedContent onto the
// pendingJob so runEmbedBatch swaps the parent input and finalizeJob writes
// the marker columns.
type queryAugmentResult struct {
	enabled          bool
	queries          []string
	augmentedContent string // ready to feed straight into ep.Embed
	usage            *provider.TokenUsage
	model            string
	providerName     string
	llmLatency       time.Duration
	truncatedBytes   int // > 0 when the content tail was cut to fit the cap
}

// resolveQueryAugmentSettings snapshots the five admin-tunable knobs so the
// rest of the phase reads from a local struct rather than re-issuing settings
// cascades. Bad operator values fall back to documented defaults.
// QueryAugmentMaxCount mirrors the schema-declared upper bound on
// enrichment.query_augment.count (see internal/storage/admin/settings_store.go).
// Exported so API-layer handlers that re-resolve the same setting clamp to the
// same ceiling without redefining the constant and drifting over time.
const QueryAugmentMaxCount = 10

func (wp *WorkerPool) resolveQueryAugmentSettings(ctx context.Context) queryAugmentSettings {
	cfg := queryAugmentSettings{count: 4, maxTokens: QueryAugmentDefaultMaxTokens}
	cfg.enabled = wp.settings.ResolveBool(ctx, service.SettingQueryAugmentEnabled, "global")
	if v, err := wp.settings.ResolveInt(ctx, service.SettingQueryAugmentCount, "global"); err == nil && v > 0 {
		if v > QueryAugmentMaxCount {
			v = QueryAugmentMaxCount
		}
		cfg.count = v
	}
	cfg.model, _ = wp.settings.Resolve(ctx, service.SettingQueryAugmentModel, "global")
	cfg.prompt, _ = wp.settings.Resolve(ctx, service.SettingQueryAugmentPrompt, "global")
	if cfg.prompt == "" {
		cfg.prompt, _ = service.GetDefault(service.SettingQueryAugmentPrompt)
	}
	if v, err := wp.settings.ResolveInt(ctx, service.SettingQueryAugmentMaxInputChars, "global"); err == nil && v >= 0 {
		cfg.maxInputChars = v
	}
	if v, err := wp.settings.ResolveInt(ctx, service.SettingQueryAugmentMaxTokens, "global"); err == nil && v > 0 {
		cfg.maxTokens = v
	}
	return cfg
}

// RenderQueryAugmentPrompt substitutes the two named placeholders in the
// operator-editable prompt template. strings.Replace, not fmt.Sprintf, so a
// prompt body containing literal '%' characters is safe and a prompt that
// drops or duplicates a placeholder cannot crash the call site.
func RenderQueryAugmentPrompt(template, content string, n int) string {
	out := strings.ReplaceAll(template, "{content}", content)
	out = strings.ReplaceAll(out, "{N}", fmt.Sprintf("%d", n))
	return out
}

// ParseQueryAugmentResponse extracts a JSON array of strings from the LLM
// response. Tolerates five documented small-model failure modes before
// declaring parse failure, in order of preference:
//
//  1. Bare JSON array of strings (the contract). Strips markdown fences and
//     leading/trailing prose by clipping to the first '[' and last ']'.
//  2. JSON object wrapping the array under any single key
//     ({"queries": [...]}, {"questions": [...]}, etc). Used when the model
//     ignores "no envelope" prompt language. No key name is hardcoded.
//  3. Bare JSON array with mixed element types ([123, "x", true]); each
//     element is stringified and treated as a candidate query. Catches the
//     case where the model interpolates a number or boolean into one slot.
//  4. Bracketed list with missing or mixed quoting ([foo, bar, baz] or
//     ["foo", bar, 'baz']). Observed on qwen3:8b at higher temperatures: the
//     model emits the brackets but drops the per-element double quotes.
//     Split on commas (preferred) or newlines (fallback), strip stray quote
//     chars per token, drop empties.
//  5. Truncation-prefix recovery via json.Decoder.Token streaming. Catches
//     the case where the model emitted a well-formed prefix of a JSON array
//     of strings but ran out of tokens (or otherwise stopped) before the
//     closing ']'. Recovers every cleanly-decoded string element up to the
//     first decode error. Mirrors the longest-valid-prefix recovery in
//     internal/service/extraction_llm.go used by fact extraction.
//
// Empties and whitespace-only entries are dropped at the end regardless of
// which path succeeded.
func ParseQueryAugmentResponse(raw string) ([]string, error) {
	body := strings.TrimSpace(raw)
	if start, end := strings.Index(body, "["), strings.LastIndex(body, "]"); start >= 0 && end > start {
		body = body[start : end+1]
	} else if start, end := strings.Index(body, "{"), strings.LastIndex(body, "}"); start >= 0 && end > start {
		body = body[start : end+1]
	}

	candidates, err := decodeQueryCandidates([]byte(body))
	if err != nil {
		return nil, fmt.Errorf("query_augment parse: %w", err)
	}

	cleaned := make([]string, 0, len(candidates))
	for _, q := range candidates {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		cleaned = append(cleaned, q)
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("query_augment parse: empty array")
	}
	return cleaned, nil
}

// decodeQueryCandidates runs the four tolerant decode passes described on
// ParseQueryAugmentResponse and returns the raw candidate slice with no
// post-cleaning. Split out so the cleaning loop stays a single site.
func decodeQueryCandidates(body []byte) ([]string, error) {
	// Pass 1: bare []string. The contract path.
	var arr []string
	if err := json.Unmarshal(body, &arr); err == nil {
		return arr, nil
	}

	// Pass 2: object envelope. Pick the first value (any key) that decodes
	// as either []string or []any with all-string-coercible elements.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err == nil {
		// Map iteration is non-deterministic; in the common case the object
		// has exactly one key so this is fine. If it has multiple keys, the
		// first one that matches wins, which is acceptable for fail-soft.
		for _, v := range obj {
			var inner []string
			if err := json.Unmarshal(v, &inner); err == nil {
				return inner, nil
			}
			var innerAny []any
			if err := json.Unmarshal(v, &innerAny); err == nil {
				if out, ok := stringifyAnySlice(innerAny); ok {
					return out, nil
				}
			}
		}
	}

	// Pass 3: bare []any with mixed element types. Stringify each.
	var mixed []any
	if err := json.Unmarshal(body, &mixed); err == nil {
		if out, ok := stringifyAnySlice(mixed); ok {
			return out, nil
		}
	}

	// Pass 4: lenient — bracketed list with missing or mixed quoting. See
	// ParseQueryAugmentResponse docstring for the failure mode this rescues.
	if out, ok := lenientSplitArray(body); ok {
		return out, nil
	}

	// Pass 5: truncation-prefix recovery. The strict passes fail with
	// "unexpected end of JSON input" when the model emits the opening '['
	// plus some valid string elements but never writes the closing ']'
	// (token-budget exhaustion is the dominant cause; qwen3:8b in reasoning
	// mode is the canonical offender). Stream string tokens with json.Decoder
	// until the first decode error and return whatever cleanly decoded.
	if out, ok := recoverStringArrayPrefix(body); ok {
		return out, nil
	}

	// All passes failed; re-run the strict path to return its native error
	// for logging fidelity.
	return nil, json.Unmarshal(body, &arr)
}

// recoverStringArrayPrefix walks the body with a json.Decoder seeded at the
// first '[', returning every successfully-decoded string element before the
// first error. The decoder is lenient about trailing content past the last
// decoded token, so a truncated array (no closing ']') still yields its
// well-formed prefix. Returns (nil, false) when no '[' is present or zero
// string elements survive so the caller can preserve the strict-pass error
// for logs.
func recoverStringArrayPrefix(body []byte) ([]string, bool) {
	lb := bytes.IndexByte(body, '[')
	if lb < 0 {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(body[lb:]))

	tok, err := dec.Token()
	if err != nil {
		return nil, false
	}
	d, ok := tok.(json.Delim)
	if !ok || d != '[' {
		return nil, false
	}

	var out []string
	for dec.More() {
		var s string
		if err := dec.Decode(&s); err != nil {
			// Non-string element or truncation. Stop on first error and
			// return whatever survived.
			break
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// lenientSplitArray rescues a bracketed list whose elements are not
// (consistently) double-quoted JSON strings. Requires brackets — without them
// there is no list structure to extract. Splits the interior on commas
// (preferred) or newlines (fallback when no comma is present), trims
// whitespace, and strips a single layer of wrapping ASCII single, double, or
// backtick quote characters per token. Returns ok=false when no non-empty
// token survives so the caller can preserve the strict-pass error for logs.
func lenientSplitArray(body []byte) ([]string, bool) {
	s := strings.TrimSpace(string(body))
	l, r := strings.Index(s, "["), strings.LastIndex(s, "]")
	if l < 0 || r <= l {
		return nil, false
	}
	interior := s[l+1 : r]
	var raw []string
	if strings.Contains(interior, ",") {
		raw = strings.Split(interior, ",")
	} else {
		raw = strings.Split(interior, "\n")
	}
	out := make([]string, 0, len(raw))
	for _, tok := range raw {
		tok = strings.TrimSpace(tok)
		tok = strings.Trim(tok, "\"'`")
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		out = append(out, tok)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// stringifyAnySlice coerces each element of a []any to its string form,
// rejecting nested objects/arrays (which would round-trip to "map[...]" or
// "[...]" noise and never make a useful query). Returns (slice, true) when
// every element coerced; (nil, false) otherwise so the caller can fall
// through to the next pass.
func stringifyAnySlice(in []any) ([]string, bool) {
	out := make([]string, 0, len(in))
	for _, v := range in {
		switch t := v.(type) {
		case string:
			out = append(out, t)
		case float64, bool:
			out = append(out, fmt.Sprintf("%v", t))
		case json.Number:
			out = append(out, t.String())
		case nil:
			// Skip; the cleaning pass would drop an empty string anyway.
			continue
		default:
			return nil, false
		}
	}
	return out, true
}

// BuildAugmentedInput concatenates queries + separator + content with the
// queries-first ordering guaranteed by the byte cap. When maxChars > 0 and
// the result would exceed it, the content tail is truncated; queries are
// always preserved. Returns the assembled string and the number of bytes
// trimmed off the content (0 means no truncation).
func BuildAugmentedInput(queries []string, content string, maxChars int) (string, int) {
	queryBlock := strings.Join(queries, "\n")
	prefix := queryBlock + QueryAugmentSeparator
	full := prefix + content
	if maxChars <= 0 || len(full) <= maxChars {
		return full, 0
	}
	allowance := maxChars - len(prefix)
	if allowance <= 0 {
		return prefix, len(content)
	}
	return prefix + content[:allowance], len(content) - allowance
}

// runQueryAugment is the enrichment phase that runs between runPreEmbed and
// runEmbedBatch. Returns (result, "") on success and (nil, skipReason) when
// the phase did not produce augmented content. The caller persists skipReason
// onto the queue row so EnrichmentMonitor can render "skipped (cause)" instead
// of a bare "skipped" label.
//
// Fail-soft contract: settings missing, provider unavailable, LLM error, or
// parse error all degrade to a nil result with a documented reason and a
// slog.Warn. Augmentation must never block ingestion.
func (wp *WorkerPool) runQueryAugment(ctx context.Context, job *model.EnrichmentJob, mem *model.Memory) (*queryAugmentResult, string) {
	if wp.settings == nil {
		return nil, model.QueryAugmentSkipDisabled
	}
	cfg := wp.resolveQueryAugmentSettings(ctx)
	if !cfg.enabled {
		return nil, model.QueryAugmentSkipDisabled
	}
	if strings.TrimSpace(mem.Content) == "" {
		return nil, model.QueryAugmentSkipContentEmpty
	}

	// Prefer the dedicated query-augment provider; fall back to the fact
	// provider when no dedicated slot is wired (the production factory already
	// resolves this via Registry.GetQueryAugment, but tests and bare setups may
	// pass only factProvider).
	llmFactory := wp.queryAugmentProvider
	if llmFactory == nil {
		llmFactory = wp.factProvider
	}
	if llmFactory == nil {
		slog.Warn("enrichment: query_augment provider unavailable", "job", job.ID, "memory", mem.ID)
		return nil, model.QueryAugmentSkipProviderUnavailable
	}
	llm := llmFactory()
	if llm == nil {
		slog.Warn("enrichment: query_augment provider nil", "job", job.ID, "memory", mem.ID)
		return nil, model.QueryAugmentSkipProviderUnavailable
	}

	prompt := RenderQueryAugmentPrompt(cfg.prompt, mem.Content, cfg.count)
	// Deliberately NOT setting JSONMode. response_format=json_object on
	// OpenAI-compatible providers (including Ollama's compat shim) forces
	// the model to emit an object, not an array. qwen3:8b-extract observed
	// satisfying that constraint by emitting {"query 1":"", "query 2":"", ...}
	// — keys-as-queries — and degenerating into a loop until max_tokens
	// truncates. The prompt itself is already strict about array output;
	// ParseQueryAugmentResponse strips prose preambles via brackets.
	req := &provider.CompletionRequest{
		Messages:  []provider.Message{{Role: "user", Content: prompt}},
		Model:     cfg.model,
		MaxTokens: cfg.maxTokens,
	}

	start := time.Now()
	augmentCtx := provider.WithOperation(ctx, provider.OperationQueryAugment)
	resp, err := llm.Complete(augmentCtx, req)
	latency := time.Since(start)
	if err != nil {
		slog.Warn("enrichment: query_augment llm",
			"job", job.ID, "memory", mem.ID,
			"err", err, "llm_latency_ms", latency.Milliseconds())
		return nil, model.QueryAugmentSkipLLMError
	}

	queries, perr := ParseQueryAugmentResponse(resp.Content)
	if perr != nil {
		// Dump the raw LLM body alongside finish_reason so an operator can
		// diagnose without re-running the call. finish_reason="length" plus
		// "unexpected end of JSON input" is the canonical truncation signal
		// and tells the operator to raise enrichment.query_augment.max_tokens.
		slog.Warn("enrichment: query_augment parse",
			"job", job.ID, "memory", mem.ID,
			"err", perr, "raw_len", len(resp.Content),
			"finish_reason", resp.FinishReason,
			"model", resp.Model,
			"raw", resp.Content,
			"llm_latency_ms", latency.Milliseconds())
		return nil, model.QueryAugmentSkipParseError
	}

	augmented, trimmed := BuildAugmentedInput(queries, mem.Content, cfg.maxInputChars)
	if trimmed > 0 {
		slog.Info("enrichment: query_augment trimmed content tail",
			"job", job.ID, "memory", mem.ID,
			"trimmed_bytes", trimmed,
			"max_chars", cfg.maxInputChars,
			"final_len", len(augmented))
	}

	slog.Info("enrichment: query_augment",
		"job", job.ID,
		"memory", mem.ID,
		"queries", len(queries),
		"model", resp.Model,
		"prompt_tokens", resp.Usage.PromptTokens,
		"completion_tokens", resp.Usage.CompletionTokens,
		"llm_latency_ms", latency.Milliseconds())

	usage := resp.Usage
	return &queryAugmentResult{
		enabled:          true,
		queries:          queries,
		augmentedContent: augmented,
		usage:            &usage,
		model:            resp.Model,
		providerName:     llm.Name(),
		llmLatency:       latency,
		truncatedBytes:   trimmed,
	}, ""
}

// runQueryAugmentPreview runs the same phase the worker runs, but against
// arbitrary content with overrides for prompt/count, and never touches the DB
// or usage middleware (the preview endpoint records its own latency). Used by
// the Prompt Templates "Test" button and the memory-detail Preview button.
// When the LLM call or parse fails, the error is returned to the caller so the
// UI can surface it inline; the worker phase is fail-soft for the same paths.
type QueryAugmentPreview struct {
	Queries          []string
	AugmentedContent string
	RenderedPrompt   string
	Model            string
	LatencyMS        int64
	Truncated        int
}

func (wp *WorkerPool) RunQueryAugmentPreview(ctx context.Context, content, promptOverride, modelOverride string, count, maxInputChars int) (*QueryAugmentPreview, error) {
	cfg := wp.resolveQueryAugmentSettings(ctx)
	if promptOverride != "" {
		cfg.prompt = promptOverride
	}
	if modelOverride != "" {
		cfg.model = modelOverride
	}
	if count > 0 {
		cfg.count = count
	}
	if maxInputChars >= 0 {
		cfg.maxInputChars = maxInputChars
	}
	if wp.factProvider == nil {
		return nil, fmt.Errorf("fact provider not configured")
	}
	llm := wp.factProvider()
	if llm == nil {
		return nil, fmt.Errorf("fact provider returned nil")
	}
	prompt := RenderQueryAugmentPrompt(cfg.prompt, content, cfg.count)
	// JSONMode deliberately omitted; see runQueryAugment comment for why
	// response_format=json_object is harmful for the array-output contract.
	req := &provider.CompletionRequest{
		Messages:  []provider.Message{{Role: "user", Content: prompt}},
		Model:     cfg.model,
		MaxTokens: cfg.maxTokens,
	}
	start := time.Now()
	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationQueryAugment), req)
	latency := time.Since(start)
	if err != nil {
		return nil, fmt.Errorf("llm complete: %w", err)
	}
	queries, perr := ParseQueryAugmentResponse(resp.Content)
	if perr != nil {
		// Mirror the worker phase's diagnostic log; same finish_reason +
		// raw-body shape so operators triage live and preview parse failures
		// from a single grep target.
		slog.Warn("enrichment: query_augment preview parse",
			"err", perr,
			"raw_len", len(resp.Content),
			"finish_reason", resp.FinishReason,
			"model", resp.Model,
			"raw", resp.Content,
			"llm_latency_ms", latency.Milliseconds())
		return nil, perr
	}
	augmented, trimmed := BuildAugmentedInput(queries, content, cfg.maxInputChars)
	return &QueryAugmentPreview{
		Queries:          queries,
		AugmentedContent: augmented,
		RenderedPrompt:   prompt,
		Model:            resp.Model,
		LatencyMS:        latency.Milliseconds(),
		Truncated:        trimmed,
	}, nil
}
