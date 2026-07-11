package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Rerank method identifiers stored on SlotConfig.RerankMethod and used by
// createRerankProvider to pick an implementation. The admin save path probes the
// configured server (see ProbeRerankMethod) and persists the result, so the
// registry build is deterministic and never depends on the server being up at
// boot.
const (
	// RerankMethodCrossEncoder scores via a deterministic OpenAI-style
	// POST /v1/rerank endpoint (bge / MiniLM class on llama-server or SGLang).
	RerankMethodCrossEncoder = "cross_encoder"
	// RerankMethodJudge scores via a generative chat model prompted per
	// candidate. Non-deterministic; tolerated on ask, never gate-safe on recall.
	RerankMethodJudge = "judge"
)

// ---------- cross-encoder /v1/rerank wire types ----------

// openaiRerankRequest is the request body for POST /v1/rerank. Model is optional
// (single-model rerank servers ignore it); query and documents are required.
type openaiRerankRequest struct {
	Model     string   `json:"model,omitempty"`
	Query     string   `json:"query"`
	Documents []string `json:"documents"`
}

// openaiRerankResult is a single scored document in the rerank response. Results
// come back sorted by score with index back-referencing the input position.
type openaiRerankResult struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
}

// openaiRerankResponse is the response body from POST /v1/rerank. usage is
// present on the servers measured (total_tokens == prompt_tokens; the call does
// no generation).
type openaiRerankResponse struct {
	Results []openaiRerankResult `json:"results"`
	Usage   openaiUsage          `json:"usage"`
}

// sglangRerankItem is one element of the bare-array /v1/rerank response emitted by
// a stock SGLang launch server (no router). Score is a raw cross-encoder logit;
// per-item prompt_tokens under meta_info carry the prefill cost.
type sglangRerankItem struct {
	Index    int     `json:"index"`
	Score    float64 `json:"score"`
	MetaInfo struct {
		PromptTokens int `json:"prompt_tokens"`
	} `json:"meta_info"`
}

// parseRerankResponse decodes a /v1/rerank body of either wire shape: the
// OpenAI-style object {"results":[{"index","relevance_score"}],"usage":{...}}
// emitted by sglang_router / llama-server / Jina, or the bare JSON array
// [{"index","score","meta_info":{"prompt_tokens"}}] emitted by a stock SGLang
// launch server. It sniffs the first non-whitespace byte to pick the shape and
// returns results already normalized to []openaiRerankResult plus the token
// usage (summed from per-item prompt_tokens for the bare-array form, which has
// no top-level usage object).
func parseRerankResponse(raw []byte) ([]openaiRerankResult, TokenUsage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, TokenUsage{}, fmt.Errorf("empty rerank response")
	}
	if trimmed[0] == '[' { // bare SGLang launch server
		var items []sglangRerankItem
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return nil, TokenUsage{}, err
		}
		results := make([]openaiRerankResult, len(items))
		var usage TokenUsage
		for i, it := range items {
			results[i] = openaiRerankResult{Index: it.Index, RelevanceScore: it.Score}
			usage.PromptTokens += it.MetaInfo.PromptTokens
		}
		usage.TotalTokens = usage.PromptTokens
		return results, usage, nil
	}
	var rr openaiRerankResponse // sglang_router / llama-server / Jina object shape
	if err := json.Unmarshal(trimmed, &rr); err != nil {
		return nil, TokenUsage{}, err
	}
	return rr.Results, TokenUsage{
		PromptTokens:     rr.Usage.PromptTokens,
		CompletionTokens: rr.Usage.CompletionTokens,
		TotalTokens:      rr.Usage.TotalTokens,
	}, nil
}

// Rerank scores each document's relevance to query via the OpenAI-style
// /v1/rerank endpoint. Scores are remapped to input order and normalized to
// [0,1]: servers differ in score shape (bge XLM-RoBERTa returns raw, possibly
// negative logits; qwen3-reranker returns a [0,1] yes-probability), so when any
// returned score falls outside [0,1] a sigmoid is applied to all of them,
// otherwise they pass through. OpenAIProvider thus also satisfies RerankProvider
// for any cross-encoder server speaking the OpenAI wire protocol.
func (p *OpenAIProvider) Rerank(ctx context.Context, query string, docs []string) (*RerankResponse, error) {
	if len(docs) == 0 {
		return &RerankResponse{Scores: []float64{}, Model: p.config.DefaultModel}, nil
	}

	body := openaiRerankRequest{Model: p.config.DefaultModel, Query: query, Documents: docs}

	// Decode into raw bytes so parseRerankResponse can accept either the
	// object or bare-array wire shape; doRequest still enforces status-code and
	// streamed-response handling.
	var raw json.RawMessage
	if err := p.doRequest(ctx, http.MethodPost, "/v1/rerank", body, &raw); err != nil {
		return nil, fmt.Errorf("openai: rerank request failed: %w", err)
	}
	results, usage, err := parseRerankResponse(raw)
	if err != nil {
		return nil, fmt.Errorf("openai: parse rerank response: %w", err)
	}

	scores := make([]float64, len(docs))
	for _, res := range results {
		if res.Index < 0 || res.Index >= len(docs) {
			continue // ignore out-of-range indices rather than panic
		}
		scores[res.Index] = res.RelevanceScore
	}
	normalizeRerankScores(scores)

	// The /v1/rerank response carries no model field, so attribution uses the
	// configured model id.
	return &RerankResponse{
		Scores: scores,
		Model:  p.config.DefaultModel,
		Usage:  usage,
	}, nil
}

// normalizeRerankScores maps a score slice into [0,1] in place. If every score
// already sits in [0,1] (qwen-style yes-probability) it is left untouched; if any
// score falls outside [0,1] (bge raw logits) a logistic sigmoid is applied to all
// scores so a single monotonic transform preserves the ranking while bounding the
// values for use as an additive composite term.
func normalizeRerankScores(scores []float64) {
	needsSigmoid := false
	for _, s := range scores {
		if s < 0 || s > 1 {
			needsSigmoid = true
			break
		}
	}
	if !needsSigmoid {
		return
	}
	for i, s := range scores {
		scores[i] = 1.0 / (1.0 + math.Exp(-s))
	}
}

// ---------- LLM-judge reranker ----------

// RerankJudgeConfig carries the live, settings-resolved knobs for the LLM-judge
// rerank path. The service layer (which owns the settings registry) stamps it on
// the context before calling Rerank; the provider package stays settings-agnostic
// and reads it back here, mirroring how WithOperation carries the operation kind.
// The cross-encoder path ignores it entirely. Empty/zero fields fall back to the
// defaults below.
type RerankJudgeConfig struct {
	SystemPrompt string
	MaxTokens    int
	Temperature  float64
}

// Judge-path fallback defaults, used when the service did not stamp a config on
// the context (e.g. unit tests, or a caller that does not resolve settings). The
// prompt text mirrors the registered default of ranking.rerank.judge.system_prompt.
const (
	defaultRerankJudgeSystem    = "You are a relevance judge. Given a query and a document, output only a single number between 0 and 1 indicating how well the document answers the query (1 = perfectly answers it, 0 = irrelevant). Output the number and nothing else."
	defaultRerankJudgeMaxTokens = 16
)

// WithRerankJudgeConfig stamps the judge config onto ctx for the judge reranker.
func WithRerankJudgeConfig(ctx context.Context, cfg RerankJudgeConfig) context.Context {
	return context.WithValue(ctx, ctxKeyRerankJudge, cfg)
}

// rerankJudgeConfigFromContext returns the stamped judge config with defaults
// applied for any empty/zero field, so the judge always has a usable prompt and
// token cap even when the caller stamped nothing.
func rerankJudgeConfigFromContext(ctx context.Context) RerankJudgeConfig {
	cfg, _ := ctx.Value(ctxKeyRerankJudge).(RerankJudgeConfig)
	if cfg.SystemPrompt == "" {
		cfg.SystemPrompt = defaultRerankJudgeSystem
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = defaultRerankJudgeMaxTokens
	}
	return cfg
}

// judgeReranker scores each candidate with a generative chat model, one
// completion per document. Slow relative to a cross-encoder (N calls vs one
// batched call) and non-deterministic; used only where those costs are tolerable.
type judgeReranker struct {
	llm LLMProvider
}

// Name returns the wrapped LLM provider's identifier.
func (j *judgeReranker) Name() string { return j.llm.Name() }

// Rerank scores each document by prompting the chat model for a [0,1] relevance
// number. The prompt, token cap, and temperature come from the context-stamped
// RerankJudgeConfig (the service resolves them from settings). Token usage is
// summed across the per-document calls. A per-document completion error aborts the
// whole rerank (callers fail soft and keep the prior order).
func (j *judgeReranker) Rerank(ctx context.Context, query string, docs []string) (*RerankResponse, error) {
	jc := rerankJudgeConfigFromContext(ctx)
	scores := make([]float64, len(docs))
	var usage TokenUsage
	var modelName string
	for i, doc := range docs {
		user := fmt.Sprintf("Query: %s\n\nDocument: %s", query, doc)
		resp, err := j.llm.Complete(ctx, &CompletionRequest{
			Messages:    BuildMessages(jc.SystemPrompt, user),
			Temperature: jc.Temperature,
			MaxTokens:   jc.MaxTokens,
		})
		if err != nil {
			return nil, fmt.Errorf("judge rerank: scoring document %d: %w", i, err)
		}
		scores[i] = parseJudgeScore(resp.Content)
		usage.PromptTokens += resp.Usage.PromptTokens
		usage.CompletionTokens += resp.Usage.CompletionTokens
		usage.TotalTokens += resp.Usage.TotalTokens
		if modelName == "" {
			modelName = resp.Model
		}
	}
	return &RerankResponse{Scores: scores, Model: modelName, Usage: usage}, nil
}

// parseJudgeScore extracts the first numeric token from a judge completion and
// clamps it to [0,1]. Unparseable output scores 0 (treated as irrelevant) rather
// than erroring, so one malformed completion does not abort the rerank.
func parseJudgeScore(content string) float64 {
	fields := strings.FieldsFunc(content, func(r rune) bool {
		return (r < '0' || r > '9') && r != '.' && r != '-'
	})
	for _, f := range fields {
		if v, err := strconv.ParseFloat(f, 64); err == nil {
			if v < 0 {
				return 0
			}
			if v > 1 {
				return 1
			}
			return v
		}
	}
	return 0
}

// ---------- auto-detect ----------

// ProbeRerankMethod determines whether the configured server is a cross-encoder
// (answers POST /v1/rerank) or must be driven as an LLM judge. It posts a trivial
// rerank payload and inspects the status: 2xx => cross_encoder; a 4xx that means
// "no such route / bad request for this endpoint" (404/405/501/400) => judge
// (the server is a plain chat endpoint). Auth failures (401/403) and other 5xx
// are returned as errors rather than silently guessing a method, so a
// misconfigured slot surfaces instead of falling back to the slow path.
func ProbeRerankMethod(ctx context.Context, cfg SlotConfig) (string, error) {
	base := NormalizeBaseURL(cfg.BaseURL)
	payload, err := json.Marshal(openaiRerankRequest{
		Model:     cfg.Model,
		Query:     "ping",
		Documents: []string{"pong"},
	})
	if err != nil {
		return "", fmt.Errorf("rerank probe: marshal payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/v1/rerank", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("rerank probe: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	applyCustomHeaders(req, cfg.CustomHeaders, "Content-Type")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("rerank probe: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return RerankMethodCrossEncoder, nil
	case resp.StatusCode == http.StatusNotFound,
		resp.StatusCode == http.StatusMethodNotAllowed,
		resp.StatusCode == http.StatusNotImplemented,
		resp.StatusCode == http.StatusBadRequest:
		// The route is absent or rejects the rerank body: this is a generative
		// chat server, drive it as a judge.
		return RerankMethodJudge, nil
	default:
		return "", fmt.Errorf("rerank probe: unexpected status %d (configure auth/url, or set the method explicitly)", resp.StatusCode)
	}
}

// createRerankProvider builds the RerankProvider for a reranker slot from its
// configured method. An empty method defaults to cross_encoder (the admin save
// path normally fills it via ProbeRerankMethod; a config written without the
// probe falls back to the deterministic path). Cross-encoder requires an
// OpenAI-compatible wire type; judge accepts any LLM provider type.
func createRerankProvider(config SlotConfig) (RerankProvider, error) {
	method := config.RerankMethod
	if method == "" {
		method = RerankMethodCrossEncoder
	}
	switch method {
	case RerankMethodCrossEncoder:
		ptype := NormalizeProviderType(config.Type)
		if !isOpenAICompatibleType(ptype) {
			return nil, fmt.Errorf("reranker cross_encoder requires an openai-compatible provider type, got %q", config.Type)
		}
		return NewOpenAIProvider(OpenAIConfig{
			BaseURL:       config.BaseURL,
			APIKey:        config.APIKey,
			DefaultModel:  config.Model,
			Timeout:       slotTimeout(config.Timeout),
			ProviderType:  ptype,
			CustomHeaders: config.CustomHeaders,
			ExtraBody:     config.ExtraBody,
		}), nil
	case RerankMethodJudge:
		llm, err := createLLMProvider(config)
		if err != nil {
			return nil, err
		}
		return &judgeReranker{llm: llm}, nil
	default:
		return nil, fmt.Errorf("unknown rerank method %q", method)
	}
}

// ---------- usage-recording wrapper ----------

// UsageRecordingRerank wraps a RerankProvider and writes a token_usage row for
// every Rerank call (success or failure), mirroring UsageRecordingLLM/Embedding.
// Recording is best-effort: recorder errors are logged, never propagated.
type UsageRecordingRerank struct {
	inner    RerankProvider
	recorder UsageRecorder
	resolver UsageContextResolver
	counter  TokenCounter
}

// NewUsageRecordingRerank wraps inner so every Rerank call lands a token_usage row.
func NewUsageRecordingRerank(inner RerankProvider, recorder UsageRecorder, resolver UsageContextResolver) *UsageRecordingRerank {
	return &UsageRecordingRerank{inner: inner, recorder: recorder, resolver: resolver}
}

// WithTokenCounter attaches a Prometheus-style token counter fired on every
// Record. Returns the same wrapper for chaining at construction time.
func (u *UsageRecordingRerank) WithTokenCounter(c TokenCounter) *UsageRecordingRerank {
	u.counter = c
	return u
}

// Name returns the underlying provider's name.
func (u *UsageRecordingRerank) Name() string { return u.inner.Name() }

// Rerank delegates to the wrapped provider and records token usage.
func (u *UsageRecordingRerank) Rerank(ctx context.Context, query string, docs []string) (*RerankResponse, error) {
	start := time.Now()
	resp, err := u.inner.Rerank(ctx, query, docs)
	latency := int(time.Since(start).Milliseconds())
	u.record(ctx, query, docs, resp, err, latency)
	return resp, err
}

func (u *UsageRecordingRerank) record(ctx context.Context, query string, docs []string, resp *RerankResponse, callErr error, latencyMs int) {
	if u.recorder == nil {
		return
	}
	op := operationOrUnknown(ctx, u.inner.Name())

	var promptTokens, completionTokens int
	var modelName string
	if resp != nil {
		promptTokens = resp.Usage.PromptTokens
		completionTokens = resp.Usage.CompletionTokens
		modelName = resp.Model
	}

	// Tokenizer fallback: a cross-encoder /v1/rerank server may omit the usage
	// block (the measured llama-server reports it, but not every server does).
	// Estimate the prefill when the server reported nothing, mirroring the LLM
	// recorder. A cross-encoder scores (query, doc) pairs, prefilling the query
	// once per document, so count it per pair; reranking does no generation, so
	// completion stays 0.
	if resp != nil && promptTokens == 0 && completionTokens == 0 {
		promptTokens = EstimateTokens(modelName, query) * len(docs)
		for _, d := range docs {
			promptTokens += EstimateTokens(modelName, d)
		}
	}

	if u.counter != nil {
		u.counter(u.inner.Name(), string(op), float64(promptTokens+completionTokens))
	}

	// A reranker slot is not breaker-wrapped today (fail-soft read path), so
	// this guard is defensive/consistent with the LLM and embedding recorders.
	if skipUsageRecordErr(callErr) {
		return
	}

	recCtx, cancel := recordingContext(ctx)
	defer cancel()
	rec := buildUsageRow(recCtx, u.resolver, u.inner.Name(), modelName, op,
		promptTokens, completionTokens, latencyMs, callErr)

	if err := u.recorder.Record(recCtx, rec); err != nil {
		slog.Warn("usage_recorder: record failed",
			"provider", u.inner.Name(), "operation", op, "err", err)
	}
}
