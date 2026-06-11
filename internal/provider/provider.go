// Package provider defines the core interfaces and types for LLM and embedding
// providers used throughout the nram enrichment pipeline.
package provider

import (
	"context"
	"strings"
)

// Message represents a single message in a conversation with an LLM.
type Message struct {
	Role    string // "system", "user", or "assistant"
	Content string
}

// CompletionRequest contains the parameters for an LLM completion call.
//
// RepeatPenalty, TopK, and MinP are Ollama-extension fields. Strict OpenAI
// rejects unknown fields, so OpenAIProvider gates them behind
// OpenAIConfig.ProviderType == ProviderTypeOllama. nil means "omit from the
// request"; providers that do not support these knobs see a clean request.
type CompletionRequest struct {
	Messages    []Message
	Model       string
	MaxTokens   int
	Temperature float64
	Stop        []string
	JSONMode    bool // request JSON-formatted output from the model

	RepeatPenalty *float64
	TopK          *int
	MinP          *float64
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
