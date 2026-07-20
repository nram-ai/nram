// Package provider defines the core interfaces and types for LLM and embedding
// providers used throughout the nram enrichment pipeline.
package provider

import (
	"context"
	"net/url"
	"regexp"
	"strings"
)

// versionSegment matches a single trailing API version path segment such as
// "/v1", "/v1beta", "/v2", or "/v1alpha2". It is anchored to the end of the
// path so only the last segment is considered.
var versionSegment = regexp.MustCompile(`(?i)/v[0-9]+(?:alpha|beta)?[0-9]*$`)

// NormalizeBaseURL canonicalizes a provider base URL so callers may supply
// either a bare host (e.g. https://api.openai.com) or a vendor-documented URL
// that already includes a version segment (e.g. https://api.openai.com/v1).
// Each provider appends its own full versioned route path, so a version
// segment carried in the base URL would double-stack (.../v1/v1/embeddings).
//
// It trims a trailing slash, then strips one trailing version segment from the
// URL path. The host is never touched (a URL with no path is returned
// unchanged) and a real non-version prefix is preserved
// (https://openrouter.ai/api/v1 -> https://openrouter.ai/api). The function is
// idempotent.
func NormalizeBaseURL(raw string) string {
	trimmed := strings.TrimRight(raw, "/")
	u, err := url.Parse(trimmed)
	if err != nil || u.Path == "" {
		return trimmed
	}
	u.Path = versionSegment.ReplaceAllString(u.Path, "")
	return strings.TrimRight(u.String(), "/")
}

// Message represents a single message in a conversation with an LLM.
type Message struct {
	Role    string // "system", "user", or "assistant"
	Content string
}

// PromptSplitSeparator is the boundary between a static system instruction half
// and a dynamic user payload. It is the exact separator used when authoring the
// split prompt defaults, so a system half plus this separator plus a dynamic
// half reproduces the original combined template byte for byte. Token
// estimation reconstructs the combined length the same way.
const PromptSplitSeparator = "\n\n"

// buildMessages assembles the messages for an LLM completion from a static
// system instruction and a dynamic user payload. It is the unexported base
// primitive: production callers go through BuildGuardedMessages (which pairs
// GuardedSystem with the payload) so the untrusted-data directive on the system
// half can never be dropped. It stays unexported to keep that the only way.
//
// The two are sent as separate system and user messages so the system
// instruction forms a stable, cacheable prefix (KV-cache prefix reuse on
// Ollama/vLLM; prompt-prefix caching on hosted APIs when it exceeds the model's
// minimum cacheable size). An empty system collapses to a single user message.
func buildMessages(system, user string) []Message {
	if system == "" {
		return []Message{{Role: "user", Content: user}}
	}
	return []Message{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}
}

// JoinMessages concatenates message contents into the single string the token
// estimators measure. It joins with PromptSplitSeparator, so estimating over
// JoinMessages(BuildGuardedMessages(system, user)) reconstructs the combined
// template the split halves were authored from, exactly as the
// PromptSplitSeparator contract above describes.
//
// Estimate from the messages a call actually sends rather than rebuilding the
// text from the system and user halves a second time: a parallel
// reconstruction silently drifts from the wire the moment the message layout
// changes, and nothing would catch it. Joining also prevents word-boundary
// merges from skewing the count downward.
func JoinMessages(msgs []Message) string {
	parts := make([]string, len(msgs))
	for i, m := range msgs {
		parts[i] = m.Content
	}
	return strings.Join(parts, PromptSplitSeparator)
}

// CompletionRequest contains the parameters for an LLM completion call.
type CompletionRequest struct {
	Messages  []Message
	Model     string
	MaxTokens int
	// Temperature is the sampling temperature. nil leaves it unset so the
	// server applies its own default; a non-nil value is sent verbatim,
	// including 0 to request greedy/deterministic decoding. A bare float64
	// could not distinguish "unset" from an explicit 0, so a configured 0
	// (e.g. the ingestion-decision, rerank-judge, and ask-decomposition paths)
	// was silently dropped and sampled at the server default.
	Temperature *float64
	Stop        []string
	JSONMode    bool // request JSON-formatted output from the model
}

// Float64 returns a pointer to v, for setting optional pointer fields such as
// CompletionRequest.Temperature (where a non-nil 0 means greedy decoding).
func Float64(v float64) *float64 { return &v }

// CompletionResponse contains the result of an LLM completion call.
type CompletionResponse struct {
	Content      string
	Model        string
	FinishReason string
	Usage        TokenUsage
}

// IsTruncated reports whether a completion's finish reason indicates the model
// stopped because it hit the output token cap rather than finishing its
// response. Providers spell this differently: OpenAI and Ollama use "length",
// Anthropic "max_tokens", Gemini "MAX_TOKENS" (lowercased on ingest). A
// truncated response is structurally incomplete, and at temperature 0 a
// re-send of the same request with the same cap truncates identically, so
// callers use this to skip a deterministically-failing retry.
func IsTruncated(finishReason string) bool {
	switch strings.ToLower(strings.TrimSpace(finishReason)) {
	case "length", "max_tokens":
		return true
	default:
		return false
	}
}

// TokenUsage tracks the token consumption for a single LLM or embedding call.
//
// CacheReadTokens and CacheWriteTokens are a SUBSET of PromptTokens, never an
// addition to it, on every adapter. Providers split into two camps on the wire:
// OpenAI-compatible servers (including SGLang, vLLM, OpenRouter, DeepSeek,
// xAI, Groq, Mistral, Azure) and Gemini report cached counts already folded
// inside prompt_tokens/promptTokenCount, while Anthropic reports them outside
// input_tokens — so anthropic.go adds them in before constructing this struct.
// Keeping the subset invariant here is what lets TotalTokens stay
// PromptTokens+CompletionTokens and lets the dream budget and the
// nram_tokens_used_total counter keep their existing meaning unchanged.
type TokenUsage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	CacheReadTokens  int
	CacheWriteTokens int
}

// Add accumulates o into u, for callers that fan one logical operation out
// across several provider calls (the judge reranker scores one document per
// call). Summing every field in one place means a new bucket is added here
// rather than at each accumulation site, where a missed line silently reports
// zero for that bucket.
func (u *TokenUsage) Add(o TokenUsage) {
	u.PromptTokens += o.PromptTokens
	u.CompletionTokens += o.CompletionTokens
	u.TotalTokens += o.TotalTokens
	u.CacheReadTokens += o.CacheReadTokens
	u.CacheWriteTokens += o.CacheWriteTokens
}

// EmbeddingRequest contains the parameters for an embedding call.
type EmbeddingRequest struct {
	Input     []string
	Model     string
	Dimension int
}

// EmbeddingResponse contains the result of an embedding call.
type EmbeddingResponse struct {
	Embeddings [][]float32
	Model      string
	Usage      TokenUsage
}

// LLMProvider generates text completions from a large language model.
type LLMProvider interface {
	// Complete generates a completion for the given request.
	Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error)

	// Name returns the provider identifier (e.g., "openai", "anthropic").
	Name() string

	// Models returns the list of available model identifiers.
	Models() []string
}

// EmbeddingProvider generates vector embeddings from text.
type EmbeddingProvider interface {
	// Embed generates embeddings for the given request.
	Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error)

	// Name returns the provider identifier (e.g., "openai", "ollama").
	Name() string

	// Dimensions returns the supported embedding dimensions.
	Dimensions() []int
}

// RerankResponse is the result of a rerank scoring call. Scores are aligned to
// the input document order (Scores[i] scores docs[i]) and normalized to [0,1],
// higher being more relevant. Usage carries the (prefill-only) token cost for
// attribution.
type RerankResponse struct {
	Scores []float64
	Model  string
	Usage  TokenUsage
}

// RerankProvider scores the relevance of each candidate document to a query in
// one pass. It is the cross-encoder cousin of the bi-encoder EmbeddingProvider:
// query and candidate are read together rather than embedded separately, so the
// score reflects cross-attention a cosine cannot. Two implementations exist: a
// deterministic cross-encoder over an OpenAI-style /v1/rerank endpoint, and a
// generative LLM judge that prompts a chat model per candidate.
type RerankProvider interface {
	// Rerank returns one relevance score per document, aligned to docs order.
	Rerank(ctx context.Context, query string, docs []string) (*RerankResponse, error)

	// Name returns the provider identifier (e.g., "openai", "llama-server").
	Name() string
}

// Provider combines LLM and embedding capabilities for providers that support both.
type Provider interface {
	LLMProvider
	EmbeddingProvider
}

// ProviderHealth allows checking whether a provider is reachable and functional.
type ProviderHealth interface {
	// Ping verifies connectivity to the provider.
	Ping(ctx context.Context) error
}
