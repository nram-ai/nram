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

// BuildMessages assembles the messages for an LLM completion from a static
// system instruction and a dynamic user payload.
//
// The two are sent as separate system and user messages so the system
// instruction forms a stable, cacheable prefix (KV-cache prefix reuse on
// Ollama/vLLM; prompt-prefix caching on hosted APIs when it exceeds the model's
// minimum cacheable size). An empty system collapses to a single user message.
func BuildMessages(system, user string) []Message {
	if system == "" {
		return []Message{{Role: "user", Content: user}}
	}
	return []Message{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}
}

// CompletionRequest contains the parameters for an LLM completion call.
type CompletionRequest struct {
	Messages    []Message
	Model       string
	MaxTokens   int
	Temperature float64
	Stop        []string
	JSONMode    bool // request JSON-formatted output from the model
}

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
type TokenUsage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
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
