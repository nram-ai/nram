package enrichment

import (
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
}

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
	cfg := queryAugmentSettings{count: 4}
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
// response. Tolerates a markdown fence and leading/trailing prose so a
// slightly chatty model does not push the phase into fail-soft.
func ParseQueryAugmentResponse(raw string) ([]string, error) {
	body := strings.TrimSpace(raw)
	if start, end := strings.Index(body, "["), strings.LastIndex(body, "]"); start >= 0 && end > start {
		body = body[start : end+1]
	}
	var out []string
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		return nil, fmt.Errorf("query_augment parse: %w", err)
	}
	// Drop empties and normalize whitespace; an LLM that emits a blank entry
	// would otherwise inflate the prepended block with a stray newline.
	cleaned := make([]string, 0, len(out))
	for _, q := range out {
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

	llmFactory := wp.factProvider
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
	req := &provider.CompletionRequest{
		Messages:  []provider.Message{{Role: "user", Content: prompt}},
		Model:     cfg.model,
		MaxTokens: 512,
		JSONMode:  true,
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
		slog.Warn("enrichment: query_augment parse",
			"job", job.ID, "memory", mem.ID,
			"err", perr, "raw_len", len(resp.Content),
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
	req := &provider.CompletionRequest{
		Messages:  []provider.Message{{Role: "user", Content: prompt}},
		Model:     cfg.model,
		MaxTokens: 512,
		JSONMode:  true,
	}
	start := time.Now()
	resp, err := llm.Complete(provider.WithOperation(ctx, provider.OperationQueryAugment), req)
	latency := time.Since(start)
	if err != nil {
		return nil, fmt.Errorf("llm complete: %w", err)
	}
	queries, perr := ParseQueryAugmentResponse(resp.Content)
	if perr != nil {
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
