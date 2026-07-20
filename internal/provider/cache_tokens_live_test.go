package provider

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestPromptCacheTokens_Live drives the REAL production composition against a
// live OpenAI-compatible endpoint: OpenAIProvider parsing the server's actual
// usage block, wrapped in UsageRecordingLLM, producing an actual
// model.TokenUsage row. The stubbed tests prove each link separately (the
// adapter parses the shape, the middleware carries the fields); this proves the
// composition against a server that really prefix-caches.
//
// It sends the same long prefix twice. A prefix-caching server reports few or
// no cached tokens on the cold call and a large number on the warm one, so the
// assertion is that the second call's recorded tokens_cache_read is greater
// than the first — which no amount of struct wiring can fake.
//
// Gated on NRAM_LIVE_CACHE_URL + NRAM_LIVE_CACHE_MODEL so it never runs in CI.
// A server that does not report prompt_tokens_details (a stock vLLM/SGLang
// launch without its reporting flag) skips rather than fails: absence of the
// object is a server configuration, not a defect in this code. Run:
//
//	NRAM_LIVE_CACHE_URL=http://192.168.2.43:30000 \
//	NRAM_LIVE_CACHE_MODEL='Qwen/Qwen3-8B' \
//	go test ./internal/provider/ -run TestPromptCacheTokens_Live -v
func TestPromptCacheTokens_Live(t *testing.T) {
	url := os.Getenv("NRAM_LIVE_CACHE_URL")
	modelName := os.Getenv("NRAM_LIVE_CACHE_MODEL")
	if url == "" || modelName == "" {
		t.Skip("set NRAM_LIVE_CACHE_URL and NRAM_LIVE_CACHE_MODEL to run")
	}

	inner := NewOpenAIProvider(OpenAIConfig{
		BaseURL:         url,
		DefaultModel:    modelName,
		ProviderType:    ProviderTypeSGLang,
		DisableThinking: true,
	})
	rec := &captureRecorder{}
	wrapped := NewUsageRecordingLLM(inner, rec, nil)

	// Long enough to clear a prefix-cache block threshold, and identical across
	// both calls so the second can hit.
	//
	// The leading nonce makes the prefix unique per run, so the first call is
	// genuinely cold. Without it a rerun inherits the previous run's warm cache
	// and both calls report a full hit, which makes the warm-exceeds-cold
	// assertion below fail for a reason that has nothing to do with the code.
	prefix := uuid.NewString() + " " +
		strings.Repeat("The quick brown fox jumps over the lazy dog. ", 400)

	ctx := WithOperation(context.Background(), OperationFactExtraction)
	ctx = WithNamespaceID(ctx, uuid.New())

	for i := range 2 {
		resp, err := wrapped.Complete(ctx, &CompletionRequest{
			Messages:  []Message{{Role: "system", Content: prefix}, {Role: "user", Content: "say ok"}},
			MaxTokens: 4,
		})
		if err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
		t.Logf("call %d: prompt=%d cache_read=%d cache_write=%d",
			i+1, resp.Usage.PromptTokens, resp.Usage.CacheReadTokens, resp.Usage.CacheWriteTokens)
	}

	if len(rec.rows) != 2 {
		t.Fatalf("expected 2 recorded rows, got %d", len(rec.rows))
	}
	cold, warm := rec.rows[0], rec.rows[1]

	if cold.TokensInput == 0 {
		t.Fatal("server reported no prompt tokens; cannot evaluate caching")
	}
	if warm.TokensCacheRead == 0 {
		t.Skipf("server reported no cached tokens on a repeated prefix "+
			"(prompt=%d); it is likely not reporting prompt_tokens_details",
			warm.TokensInput)
	}

	if warm.TokensCacheRead <= cold.TokensCacheRead {
		t.Errorf("warm call cache_read (%d) should exceed cold call cache_read (%d)",
			warm.TokensCacheRead, cold.TokensCacheRead)
	}
	// The invariant, against real numbers from a real server.
	for i, row := range rec.rows {
		if row.TokensCacheRead+row.TokensCacheWrite > row.TokensInput {
			t.Errorf("row %d: cache tokens (%d+%d) exceed tokens_input (%d); they must be a subset",
				i, row.TokensCacheRead, row.TokensCacheWrite, row.TokensInput)
		}
	}
	t.Logf("recorded rows: cold cache_read=%d, warm cache_read=%d of %d input tokens",
		cold.TokensCacheRead, warm.TokensCacheRead, warm.TokensInput)
}
