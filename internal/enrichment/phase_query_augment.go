package enrichment

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
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
	systemPrompt  string // tunable static instruction (role, rules, output format)
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
	cfg.systemPrompt = service.ResolveOrDefault(ctx, wp.settings, service.SettingQueryAugmentSystemPrompt, "global")
	if v, err := wp.settings.ResolveInt(ctx, service.SettingQueryAugmentMaxInputChars, "global"); err == nil && v >= 0 {
		cfg.maxInputChars = v
	}
	if v, err := wp.settings.ResolveInt(ctx, service.SettingQueryAugmentMaxTokens, "global"); err == nil && v > 0 {
		cfg.maxTokens = v
	}
	return cfg
}

// queryAugmentUserWrapper is the hardcoded dynamic-half template for the
// query-augmentation phase. It carries the requested query count and the memory
// content; the tunable instruction (role, rules, output format) lives entirely
// in SettingQueryAugmentSystemPrompt, sent as the system message. The count and
// RenderQueryAugmentUser builds the user message for the query-augmentation
// phase from the memory content and the requested query count. The memory body
// is nonce-fenced as untrusted data (Fence handles a body containing literal
// delimiters by regenerating the nonce).
func RenderQueryAugmentUser(content string, n int) string {
	return "Generate " + strconv.Itoa(n) + " short, distinct retrieval queries for the memory below.\n\n" +
		provider.Fence("memory", content)
}

// ParseQueryAugmentResponse extracts a JSON array of strings from the LLM
// response, delegating to service.ParseLLMStringList for the actual parsing
// (the same tolerant five-pass parser the ask-decomposition path uses). This
// wrapper preserves the "query_augment parse:" error prefix that operators see
// via the admin enrichment test endpoint and the query-augment preview logs.
func ParseQueryAugmentResponse(raw string) ([]string, error) {
	queries, err := service.ParseLLMStringList(raw)
	if err != nil {
		return nil, fmt.Errorf("query_augment parse: %w", err)
	}
	return queries, nil
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

	system := cfg.systemPrompt
	user := RenderQueryAugmentUser(mem.Content, cfg.count)
	// Deliberately NOT setting JSONMode. response_format=json_object on
	// OpenAI-compatible providers (including Ollama's compat shim) forces
	// the model to emit an object, not an array. qwen3:8b-extract observed
	// satisfying that constraint by emitting {"query 1":"", "query 2":"", ...}
	// (keys-as-queries) and degenerating into a loop until max_tokens
	// truncates. The prompt itself is already strict about array output;
	// ParseQueryAugmentResponse strips prose preambles via brackets.
	// Model is left empty: the query-augment provider slot supplies the model
	// (falling back to the fact provider's model when no dedicated slot is set,
	// per Registry.GetQueryAugment).
	req := &provider.CompletionRequest{
		Messages:  provider.BuildMessages(provider.GuardedSystem(system), user),
		MaxTokens: cfg.maxTokens,
	}

	start := time.Now()
	// Stamp the memory/namespace so the recorded token_usage row attributes to
	// this memory (query augmentation runs from the batch-level ctx, which
	// otherwise carries no memory_id, so the query_augment phase would be
	// unattributable in per-memory views).
	augmentCtx := provider.WithOperation(
		provider.WithMemoryID(provider.WithNamespaceID(ctx, mem.NamespaceID), mem.ID),
		provider.OperationQueryAugment)
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
