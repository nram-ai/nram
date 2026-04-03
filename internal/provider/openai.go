package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenAIConfig holds the configuration for an OpenAI-compatible provider.
type OpenAIConfig struct {
	// BaseURL is the base URL of the OpenAI-compatible API (e.g., "https://api.openai.com").
	BaseURL string

	// APIKey is the API key for authentication.
	APIKey string

	// DefaultModel is the default model to use for completions when none is specified.
	DefaultModel string

	// DefaultEmbeddingModel is the default model to use for embeddings when none is specified.
	DefaultEmbeddingModel string

	// Organization is an optional organization identifier sent via the OpenAI-Organization header.
	Organization string

	// Timeout is the HTTP client timeout. Defaults to 30s if zero.
	Timeout time.Duration
}

// OpenAIProvider implements both LLMProvider and EmbeddingProvider using any
// OpenAI-compatible HTTP API. By changing BaseURL it works with OpenAI, Ollama,
// OpenRouter, vLLM, LiteLLM, Azure, and any other compatible endpoint.
type OpenAIProvider struct {
	config OpenAIConfig
	client *http.Client
}

// Compile-time interface checks.
var (
	_ LLMProvider       = (*OpenAIProvider)(nil)
	_ EmbeddingProvider = (*OpenAIProvider)(nil)
	_ ProviderHealth    = (*OpenAIProvider)(nil)
)

// NewOpenAIProvider creates a new OpenAIProvider with the given configuration.
func NewOpenAIProvider(config OpenAIConfig) *OpenAIProvider {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 120 * time.Second
	}

	// Normalize BaseURL: strip trailing slash.
	config.BaseURL = strings.TrimRight(config.BaseURL, "/")

	return &OpenAIProvider{
		config: config,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// ---------- OpenAI API request/response types ----------

// openaiChatMessage is a single message in the OpenAI chat completion format.
type openaiChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openaiResponseFormat specifies the output format for the model.
type openaiResponseFormat struct {
	Type string `json:"type"`
}

// openaiChatRequest is the request body for POST /v1/chat/completions.
type openaiChatRequest struct {
	Model          string                `json:"model"`
	Messages       []openaiChatMessage   `json:"messages"`
	MaxTokens      int                   `json:"max_tokens,omitempty"`
	Temperature    *float64              `json:"temperature,omitempty"`
	Stop           []string              `json:"stop,omitempty"`
	ResponseFormat *openaiResponseFormat `json:"response_format,omitempty"`
}

// openaiChatChoice is a single choice in a chat completion response.
type openaiChatChoice struct {
	Index        int               `json:"index"`
	Message      openaiChatMessage `json:"message"`
	FinishReason string            `json:"finish_reason"`
}

// openaiUsage is the token usage block returned by the OpenAI API.
type openaiUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// openaiChatResponse is the response body from POST /v1/chat/completions.
type openaiChatResponse struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Model   string             `json:"model"`
	Choices []openaiChatChoice `json:"choices"`
	Usage   openaiUsage        `json:"usage"`
}

// openaiEmbeddingRequest is the request body for POST /v1/embeddings.
type openaiEmbeddingRequest struct {
	Input      []string `json:"input"`
	Model      string   `json:"model"`
	Dimensions int      `json:"dimensions,omitempty"`
}

// openaiEmbeddingData is a single embedding in the response.
type openaiEmbeddingData struct {
	Object    string    `json:"object"`
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

// openaiEmbeddingResponse is the response body from POST /v1/embeddings.
type openaiEmbeddingResponse struct {
	Object string                `json:"object"`
	Data   []openaiEmbeddingData `json:"data"`
	Model  string                `json:"model"`
	Usage  openaiUsage           `json:"usage"`
}

// openaiErrorDetail captures the error object in an OpenAI error response.
type openaiErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

// openaiErrorResponse is the top-level error response from the OpenAI API.
type openaiErrorResponse struct {
	Error openaiErrorDetail `json:"error"`
}

// ---------- LLMProvider implementation ----------

// Complete sends a chat completion request to the OpenAI-compatible endpoint.
func (p *OpenAIProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	model := req.Model
	if model == "" {
		model = p.config.DefaultModel
	}
	if model == "" {
		return nil, fmt.Errorf("openai: no model specified and no default configured")
	}

	messages := make([]openaiChatMessage, len(req.Messages))
	for i, m := range req.Messages {
		messages[i] = openaiChatMessage{Role: m.Role, Content: m.Content}
	}

	body := openaiChatRequest{
		Model:    model,
		Messages: messages,
	}
	if req.MaxTokens > 0 {
		body.MaxTokens = req.MaxTokens
	}
	if req.Temperature != 0 {
		t := req.Temperature
		body.Temperature = &t
	}
	if len(req.Stop) > 0 {
		body.Stop = req.Stop
	}
	if req.JSONMode {
		body.ResponseFormat = &openaiResponseFormat{Type: "json_object"}
	}

	var chatResp openaiChatResponse
	if err := p.doRequest(ctx, http.MethodPost, "/v1/chat/completions", body, &chatResp); err != nil {
		return nil, fmt.Errorf("openai: completion request failed: %w", err)
	}

	if len(chatResp.Choices) == 0 {
		return nil, fmt.Errorf("openai: completion returned no choices")
	}

	choice := chatResp.Choices[0]
	return &CompletionResponse{
		Content:      choice.Message.Content,
		Model:        chatResp.Model,
		FinishReason: choice.FinishReason,
		Usage: TokenUsage{
			PromptTokens:     chatResp.Usage.PromptTokens,
			CompletionTokens: chatResp.Usage.CompletionTokens,
			TotalTokens:      chatResp.Usage.TotalTokens,
		},
	}, nil
}

// Name returns the provider identifier.
func (p *OpenAIProvider) Name() string {
	return "openai"
}

// Models returns the configured model identifiers.
func (p *OpenAIProvider) Models() []string {
	var models []string
	if p.config.DefaultModel != "" {
		models = append(models, p.config.DefaultModel)
	}
	if p.config.DefaultEmbeddingModel != "" && p.config.DefaultEmbeddingModel != p.config.DefaultModel {
		models = append(models, p.config.DefaultEmbeddingModel)
	}
	return models
}

// ---------- EmbeddingProvider implementation ----------

// Embed sends an embedding request to the OpenAI-compatible endpoint.
func (p *OpenAIProvider) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	model := req.Model
	if model == "" {
		model = p.config.DefaultEmbeddingModel
	}
	if model == "" {
		return nil, fmt.Errorf("openai: no embedding model specified and no default configured")
	}

	body := openaiEmbeddingRequest{
		Input: req.Input,
		Model: model,
	}
	if req.Dimension > 0 {
		body.Dimensions = req.Dimension
	}

	var embResp openaiEmbeddingResponse
	if err := p.doRequest(ctx, http.MethodPost, "/v1/embeddings", body, &embResp); err != nil {
		return nil, fmt.Errorf("openai: embedding request failed: %w", err)
	}

	embeddings := make([][]float32, len(embResp.Data))
	for _, d := range embResp.Data {
		if d.Index >= 0 && d.Index < len(embeddings) {
			embeddings[d.Index] = d.Embedding
		}
	}

	return &EmbeddingResponse{
		Embeddings: embeddings,
		Model:      embResp.Model,
		Usage: TokenUsage{
			PromptTokens:     embResp.Usage.PromptTokens,
			CompletionTokens: embResp.Usage.CompletionTokens,
			TotalTokens:      embResp.Usage.TotalTokens,
		},
	}, nil
}

// Dimensions returns common OpenAI embedding dimensions.
func (p *OpenAIProvider) Dimensions() []int {
	return []int{256, 512, 1024, 1536, 3072}
}

// ---------- ProviderHealth implementation ----------

// Ping verifies connectivity by hitting the GET /v1/models endpoint.
func (p *OpenAIProvider) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.config.BaseURL+"/v1/models", nil)
	if err != nil {
		return fmt.Errorf("openai: failed to create ping request: %w", err)
	}
	p.setHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("openai: ping failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("openai: ping returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ---------- Internal helpers ----------

// setHeaders sets the standard headers on an outbound request.
func (p *OpenAIProvider) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if p.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.config.APIKey)
	}
	if p.config.Organization != "" {
		req.Header.Set("OpenAI-Organization", p.config.Organization)
	}
}

// doRequest marshals the request body, sends it to the given path, and
// unmarshals the response into dest. Non-2xx responses are parsed as errors.
func (p *OpenAIProvider) doRequest(ctx context.Context, method, path string, body any, dest any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := p.config.BaseURL + path
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	p.setHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr openaiErrorResponse
		if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Error.Message != "" {
			return fmt.Errorf("API error (%d): %s [type=%s, code=%s]",
				resp.StatusCode, apiErr.Error.Message, apiErr.Error.Type, apiErr.Error.Code)
		}
		return fmt.Errorf("API error (%d): %s", resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, dest); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return nil
}
