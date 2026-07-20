package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Prompt-cache token accounting has one invariant that spans every adapter:
// CacheReadTokens and CacheWriteTokens are a SUBSET of PromptTokens, never an
// addition to it.
//
// Providers split into two camps on the wire and the adapters must normalize
// them to the same shape:
//
//   - OpenAI-compatible (incl. SGLang, vLLM, OpenRouter, DeepSeek, xAI, Groq,
//     Mistral, Azure) and Gemini already fold the cached counts inside
//     prompt_tokens / promptTokenCount. The adapter records them as-is.
//   - Anthropic reports them OUTSIDE input_tokens, so the adapter adds them in
//     before building TokenUsage.
//
// Getting that backwards on one adapter silently double-counts every cached
// call, inflating both the token_usage row and the dream budget with no error
// anywhere. These tests are the guard against that.

// rawJSONServer serves a fixed body for any request. Cache-token parsing is
// about wire shapes, including shapes the Go structs cannot express (an absent
// prompt_tokens_details object), so these tests post raw JSON rather than
// encoding a response struct.
func rawJSONServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func assertUsage(t *testing.T, got TokenUsage, wantPrompt, wantRead, wantWrite int) {
	t.Helper()
	if got.PromptTokens != wantPrompt {
		t.Errorf("PromptTokens = %d, want %d", got.PromptTokens, wantPrompt)
	}
	if got.CacheReadTokens != wantRead {
		t.Errorf("CacheReadTokens = %d, want %d", got.CacheReadTokens, wantRead)
	}
	if got.CacheWriteTokens != wantWrite {
		t.Errorf("CacheWriteTokens = %d, want %d", got.CacheWriteTokens, wantWrite)
	}
	// The subset invariant itself. Everything downstream depends on it:
	// TotalTokens stays PromptTokens+CompletionTokens, the dream budget keeps
	// charging the right number, and nram_tokens_used_total keeps its meaning.
	if got.CacheReadTokens+got.CacheWriteTokens > got.PromptTokens {
		t.Errorf("cache tokens (%d read + %d write) exceed PromptTokens (%d): they must be a subset, not additive",
			got.CacheReadTokens, got.CacheWriteTokens, got.PromptTokens)
	}
	if got.TotalTokens != got.PromptTokens+got.CompletionTokens {
		t.Errorf("TotalTokens = %d, want PromptTokens+CompletionTokens = %d",
			got.TotalTokens, got.PromptTokens+got.CompletionTokens)
	}
}

// TestOpenAICacheTokensAreSubsetOfPrompt is the double-count regression guard.
// prompt_tokens already includes cached_tokens on this wire shape, so a
// PromptTokens of 550 here would mean the adapter wrongly copied Anthropic's
// additive handling.
func TestOpenAICacheTokensAreSubsetOfPrompt(t *testing.T) {
	srv := rawJSONServer(t, `{
		"id": "chatcmpl-1",
		"model": "gpt-4",
		"choices": [{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
		"usage": {
			"prompt_tokens": 350,
			"completion_tokens": 10,
			"total_tokens": 360,
			"prompt_tokens_details": {"cached_tokens": 200, "cache_write_tokens": 50}
		}
	}`)

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, APIKey: "k", DefaultModel: "gpt-4"})
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	assertUsage(t, resp.Usage, 350, 200, 50)
}

// TestOpenAICacheTokensAbsentDetails covers the most common live shape: a
// server that reports usage but no prompt_tokens_details at all. A stock
// vLLM/SGLang launch without its reporting flag, and any provider with no
// prompt cache, look like this. It must parse cleanly to zeroes, never error.
func TestOpenAICacheTokensAbsentDetails(t *testing.T) {
	srv := rawJSONServer(t, `{
		"id": "chatcmpl-1",
		"model": "gpt-4",
		"choices": [{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
		"usage": {"prompt_tokens": 120, "completion_tokens": 5, "total_tokens": 125}
	}`)

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, APIKey: "k", DefaultModel: "gpt-4"})
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() with no prompt_tokens_details must not error: %v", err)
	}
	assertUsage(t, resp.Usage, 120, 0, 0)
}

// TestOpenAICacheTokensPartialDetails covers Azure, which reports cached reads
// but documents that it does not report cache writes separately.
func TestOpenAICacheTokensPartialDetails(t *testing.T) {
	srv := rawJSONServer(t, `{
		"id": "chatcmpl-1",
		"model": "gpt-4",
		"choices": [{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
		"usage": {
			"prompt_tokens": 1566,
			"completion_tokens": 20,
			"total_tokens": 1586,
			"prompt_tokens_details": {"cached_tokens": 1408}
		}
	}`)

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, APIKey: "k", DefaultModel: "gpt-4"})
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	assertUsage(t, resp.Usage, 1566, 1408, 0)
}

// TestAnthropicCacheTokensAreAddedToPrompt is the mirror-image guard. Anthropic
// reports cache tokens outside input_tokens, so the adapter must add them in:
// 100 + 50 + 200 = 350. A PromptTokens of 100 here would mean cached input was
// dropped from the count entirely and usage understated whenever caching is on.
func TestAnthropicCacheTokensAreAddedToPrompt(t *testing.T) {
	srv := rawJSONServer(t, `{
		"id": "msg_1",
		"type": "message",
		"role": "assistant",
		"model": "claude-sonnet-4-20250514",
		"stop_reason": "end_turn",
		"content": [{"type":"text","text":"ok"}],
		"usage": {
			"input_tokens": 100,
			"output_tokens": 8,
			"cache_creation_input_tokens": 50,
			"cache_read_input_tokens": 200
		}
	}`)

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL: srv.URL, APIKey: "k", DefaultModel: "claude-sonnet-4-20250514",
	})
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	assertUsage(t, resp.Usage, 350, 200, 50)
}

// TestAnthropicCacheTokensAbsent covers caching being off or below the minimum
// cacheable prefix, where Anthropic omits both fields.
func TestAnthropicCacheTokensAbsent(t *testing.T) {
	srv := rawJSONServer(t, `{
		"id": "msg_1",
		"type": "message",
		"role": "assistant",
		"model": "claude-sonnet-4-20250514",
		"stop_reason": "end_turn",
		"content": [{"type":"text","text":"ok"}],
		"usage": {"input_tokens": 100, "output_tokens": 8}
	}`)

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL: srv.URL, APIKey: "k", DefaultModel: "claude-sonnet-4-20250514",
	})
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	assertUsage(t, resp.Usage, 100, 0, 0)
}

// TestGeminiCacheTokensAreSubsetOfPrompt: the REST reference states
// promptTokenCount is "the total effective prompt size meaning this includes
// the number of tokens in the cached content", so this is a subset shape.
// Gemini has no per-call cache-write bucket.
func TestGeminiCacheTokensAreSubsetOfPrompt(t *testing.T) {
	srv := rawJSONServer(t, `{
		"candidates": [{
			"content": {"role":"model","parts":[{"text":"ok"}]},
			"finishReason": "STOP"
		}],
		"usageMetadata": {
			"promptTokenCount": 500,
			"candidatesTokenCount": 12,
			"totalTokenCount": 512,
			"cachedContentTokenCount": 300
		},
		"modelVersion": "gemini-2.0-flash"
	}`)

	p := NewGeminiProvider(GeminiConfig{
		BaseURL: srv.URL, APIKey: "k", DefaultModel: "gemini-2.0-flash",
	})
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	assertUsage(t, resp.Usage, 500, 300, 0)
}

// TestRerankCacheTokensFromObjectShape covers the /v1/rerank object shape,
// which decodes the same openaiUsage block as chat completions.
func TestRerankCacheTokensFromObjectShape(t *testing.T) {
	results, usage, err := parseRerankResponse([]byte(`{
		"results": [{"index":0,"relevance_score":0.9}],
		"usage": {
			"prompt_tokens": 400,
			"completion_tokens": 0,
			"total_tokens": 400,
			"prompt_tokens_details": {"cached_tokens": 380}
		}
	}`))
	if err != nil {
		t.Fatalf("parseRerankResponse() error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	if usage.PromptTokens != 400 {
		t.Errorf("PromptTokens = %d, want 400", usage.PromptTokens)
	}
	if usage.CacheReadTokens != 380 {
		t.Errorf("CacheReadTokens = %d, want 380", usage.CacheReadTokens)
	}
}

// TestRerankCacheTokensBareArrayShape: the stock-SGLang bare-array response
// carries only per-item meta_info.prompt_tokens and has nowhere to report
// cache data, so both buckets stay zero rather than being invented.
func TestRerankCacheTokensBareArrayShape(t *testing.T) {
	_, usage, err := parseRerankResponse([]byte(`[
		{"index":0,"score":1.5,"meta_info":{"prompt_tokens":120}},
		{"index":1,"score":0.2,"meta_info":{"prompt_tokens":130}}
	]`))
	if err != nil {
		t.Fatalf("parseRerankResponse() error: %v", err)
	}
	if usage.PromptTokens != 250 {
		t.Errorf("PromptTokens = %d, want 250", usage.PromptTokens)
	}
	if usage.CacheReadTokens != 0 || usage.CacheWriteTokens != 0 {
		t.Errorf("cache tokens = (%d, %d), want (0, 0): the bare-array shape reports none",
			usage.CacheReadTokens, usage.CacheWriteTokens)
	}
}

// TestCachingEmbedding_CarriesCacheTokensThroughMiss covers the singleflight
// missResult hop in cache_embedding.go: the inner response's usage is stashed
// in a struct, shared across collapsed callers, then re-emitted. Copying the
// whole TokenUsage is what makes the cache fields survive; a field-by-field
// rebuild there would drop them without failing anything else.
func TestCachingEmbedding_CarriesCacheTokensThroughMiss(t *testing.T) {
	inner := &fakeEmbedder{name: "fake", cacheReadPer: 6, cacheWritePer: 2}
	c := newTestCache(inner, &EmbedCacheConfig{Enabled: true, MaxEntries: 100})

	resp, err := c.Embed(context.Background(), &EmbeddingRequest{Input: []string{"a", "b"}})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	// fakeEmbedder reports 1 prompt token and cacheReadPer/cacheWritePer cache
	// tokens per input, so two inputs give 2 / 12 / 4.
	if resp.Usage.PromptTokens != 2 {
		t.Errorf("PromptTokens = %d, want 2", resp.Usage.PromptTokens)
	}
	if resp.Usage.CacheReadTokens != 12 {
		t.Errorf("CacheReadTokens = %d, want 12", resp.Usage.CacheReadTokens)
	}
	if resp.Usage.CacheWriteTokens != 4 {
		t.Errorf("CacheWriteTokens = %d, want 4", resp.Usage.CacheWriteTokens)
	}
}

// TestTokenCounterUnaffectedByCacheTokens pins the reason
// nram_tokens_used_total needed no change. The counter is fed
// promptTokens+completionTokens; because cache buckets are a subset of the
// prompt count rather than additional to it, adding them must not move the
// metric. If someone later "fixes" the counter by adding the cache fields in,
// every historical series silently shifts and this test says so.
func TestTokenCounterUnaffectedByCacheTokens(t *testing.T) {
	var got float64
	llm := &stubLLM{name: "openai", resp: &CompletionResponse{
		Content: "ok", Model: "m",
		Usage: TokenUsage{
			PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
			CacheReadTokens: 80, CacheWriteTokens: 15,
		},
	}}
	rec := &captureRecorder{}
	w := NewUsageRecordingLLM(llm, rec, nil).
		WithTokenCounter(func(_, _ string, n float64) { got = n })

	ctx := WithOperation(context.Background(), OperationFactExtraction)
	if _, err := w.Complete(ctx, &CompletionRequest{Model: "m"}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if got != 150 {
		t.Errorf("token counter got %v, want 150 (prompt+completion, cache buckets are a subset and must not be added)", got)
	}
	// And the row still carries them, so the metric staying flat is not because
	// the fields were dropped on the way through.
	row := rec.last()
	if row == nil || row.TokensCacheRead != 80 || row.TokensCacheWrite != 15 {
		t.Errorf("recorded row lost cache fields: %+v", row)
	}
}
