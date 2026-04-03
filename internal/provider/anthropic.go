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

// AnthropicConfig holds the configuration for an Anthropic provider.
type AnthropicConfig struct {
	// APIKey is the API key for authentication (passed via x-api-key header).
	APIKey string

	// DefaultModel is the default model to use for completions when none is specified.
	DefaultModel string

	// BaseURL is the base URL of the Anthropic API. Defaults to "https://api.anthropic.com".
	BaseURL string

	// Timeout is the HTTP client timeout. Defaults to 30s if zero.
	Timeout time.Duration
}

// AnthropicProvider implements LLMProvider and ProviderHealth using the native
// Anthropic Messages API. Anthropic does not offer an embedding API, so
// EmbeddingProvider is not implemented.
type AnthropicProvider struct {
	config AnthropicConfig
	client *http.Client
}

// Compile-time interface checks.
var (
	_ LLMProvider    = (*AnthropicProvider)(nil)
	_ ProviderHealth = (*AnthropicProvider)(nil)
)

// NewAnthropicProvider creates a new AnthropicProvider with the given configuration.
func NewAnthropicProvider(config AnthropicConfig) *AnthropicProvider {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 120 * time.Second
	}

	if config.BaseURL == "" {
		config.BaseURL = "https://api.anthropic.com"
	}
	config.BaseURL = strings.TrimRight(config.BaseURL, "/")

	if config.DefaultModel == "" {
		config.DefaultModel = "claude-sonnet-4-20250514"
	}

	return &AnthropicProvider{
		config: config,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// ---------- Anthropic API request/response types ----------

// anthropicMessage is a single message in the Anthropic Messages API format.
type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// anthropicMessagesRequest is the request body for POST /v1/messages.
type anthropicMessagesRequest struct {
	Model         string             `json:"model"`
	MaxTokens     int                `json:"max_tokens"`
	System        string             `json:"system,omitempty"`
	Messages      []anthropicMessage `json:"messages"`
	Temperature   *float64           `json:"temperature,omitempty"`
	StopSequences []string           `json:"stop_sequences,omitempty"`
}

// anthropicContentBlock is a single block in the response content array.
type anthropicContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// anthropicUsage is the token usage block returned by the Anthropic API.
type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// anthropicMessagesResponse is the response body from POST /v1/messages.
type anthropicMessagesResponse struct {
	ID         string                  `json:"id"`
	Type       string                  `json:"type"`
	Role       string                  `json:"role"`
	Content    []anthropicContentBlock `json:"content"`
	Model      string                  `json:"model"`
	StopReason string                  `json:"stop_reason"`
	Usage      anthropicUsage          `json:"usage"`
}

// anthropicErrorDetail captures the error detail in an Anthropic error response.
type anthropicErrorDetail struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// anthropicErrorResponse is the top-level error response from the Anthropic API.
type anthropicErrorResponse struct {
	Type  string               `json:"type"`
	Error anthropicErrorDetail `json:"error"`
}

// ---------- LLMProvider implementation ----------

// Complete sends a messages request to the Anthropic API.
func (p *AnthropicProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	model := req.Model
	if model == "" {
		model = p.config.DefaultModel
	}
	if model == "" {
		return nil, fmt.Errorf("anthropic: no model specified and no default configured")
	}

	// Extract system messages and build the messages list.
	var systemText string
	var messages []anthropicMessage

	for _, m := range req.Messages {
		if m.Role == "system" {
			if systemText != "" {
				systemText += "\n"
			}
			systemText += m.Content
			continue
		}
		messages = append(messages, anthropicMessage{
			Role:    m.Role,
			Content: m.Content,
		})
	}

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}

	body := anthropicMessagesRequest{
		Model:     model,
		MaxTokens: maxTokens,
		Messages:  messages,
	}
	if systemText != "" {
		body.System = systemText
	}
	if req.Temperature != 0 {
		t := req.Temperature
		body.Temperature = &t
	}
	if len(req.Stop) > 0 {
		body.StopSequences = req.Stop
	}

	var msgResp anthropicMessagesResponse
	if err := p.doRequest(ctx, http.MethodPost, "/v1/messages", body, &msgResp); err != nil {
		return nil, fmt.Errorf("anthropic: completion request failed: %w", err)
	}

	// Extract text from content blocks.
	var content string
	for _, block := range msgResp.Content {
		if block.Type == "text" {
			content += block.Text
		}
	}

	return &CompletionResponse{
		Content:      content,
		Model:        msgResp.Model,
		FinishReason: msgResp.StopReason,
		Usage: TokenUsage{
			PromptTokens:     msgResp.Usage.InputTokens,
			CompletionTokens: msgResp.Usage.OutputTokens,
			TotalTokens:      msgResp.Usage.InputTokens + msgResp.Usage.OutputTokens,
		},
	}, nil
}

// Name returns the provider identifier.
func (p *AnthropicProvider) Name() string {
	return "anthropic"
}

// Models returns the configured model identifiers.
func (p *AnthropicProvider) Models() []string {
	var models []string
	if p.config.DefaultModel != "" {
		models = append(models, p.config.DefaultModel)
	}
	return models
}

// ---------- ProviderHealth implementation ----------

// Ping verifies connectivity by sending a minimal completion request.
func (p *AnthropicProvider) Ping(ctx context.Context) error {
	body := anthropicMessagesRequest{
		Model:     p.config.DefaultModel,
		MaxTokens: 1,
		Messages: []anthropicMessage{
			{Role: "user", Content: "hi"},
		},
	}

	var msgResp anthropicMessagesResponse
	if err := p.doRequest(ctx, http.MethodPost, "/v1/messages", body, &msgResp); err != nil {
		return fmt.Errorf("anthropic: ping failed: %w", err)
	}

	return nil
}

// ---------- Internal helpers ----------

// setHeaders sets the standard headers on an outbound request.
func (p *AnthropicProvider) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-version", "2023-06-01")
	if p.config.APIKey != "" {
		req.Header.Set("x-api-key", p.config.APIKey)
	}
}

// doRequest marshals the request body, sends it to the given path with
// Anthropic auth headers, and unmarshals the response into dest.
func (p *AnthropicProvider) doRequest(ctx context.Context, method, path string, body any, dest any) error {
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
		var apiErr anthropicErrorResponse
		if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Error.Message != "" {
			return fmt.Errorf("API error (%d): %s [type=%s]",
				resp.StatusCode, apiErr.Error.Message, apiErr.Error.Type)
		}
		return fmt.Errorf("API error (%d): %s", resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, dest); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return nil
}
