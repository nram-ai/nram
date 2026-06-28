package service

import (
	"context"
	"unicode/utf8"

	"github.com/nram-ai/nram/internal/provider"
)

// Shared mechanics for the rerank stage, used by both the recall path
// (rerankRecall, recall.go) and the ask path (rerankNeighborhood/mmrNeighborhood,
// ask.go). Kept here rather than in recall.go so the ask path does not carry an
// invisible dependency on recall internals.

// defaultRerankCandidates / defaultRerankMaxDocChars are the fallback values used
// only on the test path where the settings service is unwired. Production resolves
// these live from ranking.rerank.candidates / ranking.rerank.max_doc_chars, whose
// registered defaults (settingDefaults in settings.go) match these.
const (
	defaultRerankCandidates  = 25
	defaultRerankMaxDocChars = 1200
	defaultRerankJudgeTokens = 16
)

// withRerankJudgeConfig resolves the LLM-judge rerank knobs from settings and
// stamps them on the context so judgeReranker (provider package, settings-
// agnostic) can read them. Called before every Rerank; the cross-encoder path
// ignores the stamped config, so this is a no-op for the common case beyond a few
// cached settings reads. No-op when settings are unwired (provider applies its own
// defaults).
func withRerankJudgeConfig(ctx context.Context, settings *SettingsService) context.Context {
	if settings == nil {
		return ctx
	}
	cfg := provider.RerankJudgeConfig{
		SystemPrompt: settings.ResolveStringWithDefault(ctx, SettingRerankJudgeSystemPrompt, "global"),
		MaxTokens:    resolveRerankIntSetting(ctx, settings, SettingRerankJudgeMaxTokens, defaultRerankJudgeTokens),
		Temperature:  settings.ResolveFloatInRange(ctx, SettingRerankJudgeTemperature, "global", 0, 1, 0),
	}
	return provider.WithRerankJudgeConfig(ctx, cfg)
}

// rerankCallContext builds the context a rerank call runs under: the OperationRerank
// stamp (for token attribution) plus the judge config (settings-resolved, ignored
// by the cross-encoder). One seam shared by the recall and ask rerank stages.
func rerankCallContext(ctx context.Context, settings *SettingsService) context.Context {
	return withRerankJudgeConfig(provider.WithOperation(ctx, provider.OperationRerank), settings)
}

// resolveRerankIntSetting resolves a positive integer rerank setting, falling back
// to defaultVal when the settings service is unwired or the stored value is unset
// or below 1. One helper for both the window and doc-cap knobs.
func resolveRerankIntSetting(ctx context.Context, settings *SettingsService, key string, defaultVal int) int {
	if settings == nil {
		return defaultVal
	}
	n, err := settings.ResolveInt(ctx, key, "global")
	if err != nil || n < 1 {
		return defaultVal
	}
	return n
}

// truncateForRerank caps a document at maxChars on a UTF-8 boundary so the rerank
// request stays inside the reranker server's physical batch. A cross-encoder only
// attends to the first ~512 model tokens, so the tail is wasted; more importantly,
// a single (query, document) pair over the server's batch fails the WHOLE request
// (e.g. llama-server returns "input too large to process" for a stock 512-token
// --ubatch-size), which would silently disable reranking for that query. maxChars
// comes from ranking.rerank.max_doc_chars.
func truncateForRerank(s string, maxChars int) string {
	if maxChars <= 0 || len(s) <= maxChars {
		return s
	}
	// Trim back to a rune boundary so a multi-byte sequence is not split.
	cut := maxChars
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
