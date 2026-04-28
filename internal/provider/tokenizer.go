package provider

import (
	"strings"
	"sync"

	"github.com/pkoukk/tiktoken-go"
)

// EstimateTokens returns a best-effort token count for text under the given
// model's encoding. It is intended as a *fallback* for providers that do not
// return token usage in their response (notably Ollama embeddings, sometimes
// the Ollama OpenAI-compat endpoint). The provider's reported count is the
// source of truth — the estimator only runs when the provider returns 0/0.
//
// Encoding selection: o200k_base for GPT-4o / text-embedding-3-* / o1*; default
// cl100k_base for GPT-3.5/4. Non-OpenAI families (Anthropic, Gemini,
// Ollama-hosted Llama/Qwen/Mistral, etc.) are estimated under cl100k_base —
// numbers are approximate but sufficient for analytics rollups.
//
// On any tokenizer error, falls back to a len(text)/4 character heuristic.
// Never panics, never returns negative.
func EstimateTokens(model, text string) int {
	if text == "" {
		return 0
	}
	enc := encodingForModel(model)
	tk := getEncoding(enc)
	if tk == nil {
		return roughTokenCount(text)
	}
	return len(tk.Encode(text, nil, nil))
}

// encodingForModel maps a model identifier to its tiktoken encoding name.
func encodingForModel(model string) string {
	m := strings.ToLower(model)
	switch {
	case strings.HasPrefix(m, "gpt-4o"),
		strings.HasPrefix(m, "o1"),
		strings.HasPrefix(m, "o3"),
		strings.HasPrefix(m, "text-embedding-3"):
		return "o200k_base"
	default:
		return "cl100k_base"
	}
}

// encodingCache memoizes loaded *tiktoken.Tiktoken instances by encoding name.
var encodingCache sync.Map

// getEncoding returns a cached tiktoken instance for the named encoding, or
// nil if it cannot be loaded (which triggers the rough character fallback in
// EstimateTokens).
func getEncoding(name string) *tiktoken.Tiktoken {
	if cached, ok := encodingCache.Load(name); ok {
		if cached == nil {
			return nil
		}
		return cached.(*tiktoken.Tiktoken)
	}
	tk, err := tiktoken.GetEncoding(name)
	if err != nil {
		encodingCache.Store(name, (*tiktoken.Tiktoken)(nil))
		return nil
	}
	encodingCache.Store(name, tk)
	return tk
}

// roughTokenCount is a last-resort fallback that approximates ~4 characters
// per token for English-like text.
func roughTokenCount(text string) int {
	n := len(text) / 4
	if n < 1 {
		n = 1
	}
	return n
}

// PrewarmTokenizers loads every encoding the fallback estimator can target.
// tiktoken-go fetches BPE files lazily on first use; without pre-warming
// the very first provider call that hits the zero-token fallback would pay
// a synchronous network download. Calling this at registry construction
// pushes that cost outside the request hot path. Errors are silent — the
// estimator already falls back to len(text)/4 on encoding load failure.
func PrewarmTokenizers() {
	for _, enc := range []string{"cl100k_base", "o200k_base"} {
		_ = getEncoding(enc)
	}
}
