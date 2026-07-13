package provider

import (
	"context"
	"math"
	"os"
	"testing"
)

// TestJudgeReranker_Live drives the REAL judge reranker code path against a live
// OpenAI-compatible endpoint, built through the production createRerankProvider
// factory: BuildGuardedMessages (guarded default system prompt + nonce-Fence()d
// query/document), the per-document chat completion, and parseJudgeScore. It is
// the end-to-end check for the fencing change on the judge path, which the
// stubbed unit tests can only approximate.
//
// Gated on NRAM_LIVE_JUDGE_RERANK_URL + NRAM_LIVE_JUDGE_RERANK_MODEL so it never
// runs in CI. Type defaults to ollama and thinking is disabled by default; set
// NRAM_LIVE_JUDGE_RERANK_TYPE / NRAM_LIVE_JUDGE_RERANK_THINKING=on to override.
// A generative chat model is required (a cross-encoder reranker fails the final
// assertion). Run:
//
//	NRAM_LIVE_JUDGE_RERANK_URL=http://192.168.2.43:30000 \
//	NRAM_LIVE_JUDGE_RERANK_MODEL='Qwen/Qwen3-8B' \
//	NRAM_LIVE_JUDGE_RERANK_TYPE=sglang \
//	go test ./internal/provider/ -run TestJudgeReranker_Live -v
func TestJudgeReranker_Live(t *testing.T) {
	url := os.Getenv("NRAM_LIVE_JUDGE_RERANK_URL")
	model := os.Getenv("NRAM_LIVE_JUDGE_RERANK_MODEL")
	if url == "" || model == "" {
		t.Skip("NRAM_LIVE_JUDGE_RERANK_URL/MODEL not set; skipping live judge reranker test")
	}
	provType := os.Getenv("NRAM_LIVE_JUDGE_RERANK_TYPE")
	if provType == "" {
		provType = ProviderTypeOllama
	}
	// A generative judge needs an answer, not a reasoning trace: disable thinking
	// by default (as a configured judge slot does; the live reranker slot sets
	// disable_thinking). One SlotConfig drives both the dispatch probe and the
	// provider, so the test wires exactly like production.
	disableThinking := os.Getenv("NRAM_LIVE_JUDGE_RERANK_THINKING") != "on"
	cfg := SlotConfig{
		Type:            provType,
		BaseURL:         url,
		Model:           model,
		Timeout:         60,
		DisableThinking: &disableThinking,
		RerankMethod:    RerankMethodJudge,
	}

	// A chat-only endpoint (no /v1/rerank) auto-selects the judge method in
	// production; confirm that dispatch too, from the same config.
	if method, err := ProbeRerankMethod(context.Background(), cfg); err != nil {
		t.Logf("ProbeRerankMethod error (non-fatal): %v", err)
	} else {
		t.Logf("ProbeRerankMethod => %q (want %q for a chat-only endpoint)", method, RerankMethodJudge)
		if method != RerankMethodJudge {
			t.Errorf("expected judge dispatch for a no-/v1/rerank endpoint, got %q", method)
		}
	}

	rp, err := createRerankProvider(cfg)
	if err != nil {
		t.Fatalf("createRerankProvider: %v", err)
	}

	// The default judge cap (16 tokens) is too tight for some models to finish a
	// number; stamp a slightly larger cap, as a resolved judge config would.
	ctx := WithRerankJudgeConfig(context.Background(), RerankJudgeConfig{MaxTokens: 32})

	query := "How do I rotate the instance signing key?"
	docs := []string{
		"To rotate the instance signing key, call the admin key-rotation endpoint; it generates a new keypair and re-signs issued tokens.", // most relevant
		"The cafeteria serves lunch from 11am to 2pm on weekdays.",                                                                         // irrelevant
		"Signing keys live in the database; key rotation is an administrative operation performed by an operator.",                         // partially relevant
	}

	resp, err := rp.Rerank(ctx, query, docs)
	if err != nil {
		t.Fatalf("live judge Rerank failed: %v", err)
	}
	if len(resp.Scores) != len(docs) {
		t.Fatalf("got %d scores, want %d", len(resp.Scores), len(docs))
	}

	for i, s := range resp.Scores {
		t.Logf("score[%d] = %.4f  doc=%q", i, s, docs[i])
		if math.IsNaN(s) || s < 0 || s > 1 {
			t.Errorf("score[%d] = %v out of [0,1]", i, s)
		}
	}
	t.Logf("usage: prompt=%d completion=%d total=%d model=%q",
		resp.Usage.PromptTokens, resp.Usage.CompletionTokens, resp.Usage.TotalTokens, resp.Model)

	// Ranking semantics: the on-topic doc must score strictly above the
	// clearly-irrelevant cafeteria doc. A model that returns a constant (a
	// cross-encoder reranker driven as a chat judge emits token noise that
	// parseJudgeScore salvages to a flat 1.0; a thinking model under a tight
	// token cap emits no number and flattens to 0.0) fails here, which is the
	// signal that the configured model is not a usable generative judge.
	if resp.Scores[0] <= resp.Scores[1] {
		t.Errorf("relevant doc scored %.4f, not above the irrelevant doc %.4f; the configured model is not producing a usable judge signal (needs a generative chat model with thinking disabled and enough max_tokens for a number)",
			resp.Scores[0], resp.Scores[1])
	}
}
