package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

	// Timeout is the HTTP client timeout. Defaults to 300s if zero.
	Timeout time.Duration

	// PromptCacheEnabled marks the system instruction prefix as cacheable via
	// cache_control: {type: "ephemeral"}. Below a model's minimum cacheable
	// prefix the hint is a no-op; above it, the prefix is cached.
	PromptCacheEnabled bool

	// JSONModeToolUse coerces JSONMode requests into a forced `emit_json`
	// tool_use call (Anthropic has no response_format flag). Off by default:
	// the native api.anthropic.com path returns extraction JSON fine without
	// it, so the request shape stays unchanged there. Enable it only for
	// Anthropic-compatible proxies (e.g. OAuth/Claude-Code passthroughs) that
	// drop response formatting and need the JSON pinned to a tool call.
	JSONModeToolUse bool

	// CustomHeaders are user-configured headers applied to every outbound
	// request. They override built-ins (including x-api-key) except the
	// reserved Content-Type and anthropic-version. Intended for proxies.
	CustomHeaders map[string]string
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
		timeout = 300 * time.Second
	}

	if config.BaseURL == "" {
		config.BaseURL = "https://api.anthropic.com"
	}
	config.BaseURL = NormalizeBaseURL(config.BaseURL)

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

// anthropicMessagesRequest is the request body for POST /v1/messages. System
// is `any` because the Anthropic API accepts either a plain string or an array
// of content blocks (used to attach cache_control to the system prefix).
type anthropicMessagesRequest struct {
	Model     string `json:"model"`
	MaxTokens int    `json:"max_tokens"`
	// Stream is always sent as false. The Messages API does not stream unless
	// asked (stream is an opt-in boolean), but every inference call here is
	// single-shot and parsed as one JSON body, so we state the intent explicitly
	// on the wire. This is the deciding signal for Anthropic-compatible relays,
	// gateways, and proxies that default to or force server-sent events: without
	// it they may return text/event-stream, which fails JSON parsing. No
	// omitempty, so the field is always present.
	Stream        bool                 `json:"stream"`
	System        any                  `json:"system,omitempty"`
	Messages      []anthropicMessage   `json:"messages"`
	Temperature   *float64             `json:"temperature,omitempty"`
	StopSequences []string             `json:"stop_sequences,omitempty"`
	Tools         []anthropicTool      `json:"tools,omitempty"`
	ToolChoice    *anthropicToolChoice `json:"tool_choice,omitempty"`
}

// anthropicTool describes a tool the model may call. nram uses this only to
// coerce structured JSON output via a forced tool_choice when JSONMode is
// requested and JSONModeToolUse is enabled; it does not expose general
// tool-use to callers.
type anthropicTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"input_schema"`
}

// anthropicToolChoice forces the model toward a specific tool. With Type="tool"
// and a Name, the model MUST emit a tool_use block for that tool — Anthropic's
// nearest equivalent of OpenAI's response_format:json_object.
type anthropicToolChoice struct {
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
}

// jsonEmitToolName is the synthetic tool used to coerce a JSON response. It is
// never executed; the tool_use input is read as the response body.
const jsonEmitToolName = "emit_json"

// Synthetic tool used to coerce a forced JSON response. Captured at package
// scope so each JSONMode call reuses one slice/map allocation; never mutated.
var (
	anthropicJSONEmitInputSchema = map[string]any{
		"type":                 "object",
		"properties":           map[string]any{},
		"additionalProperties": true,
	}
	anthropicJSONEmitTools = []anthropicTool{{
		Name:        jsonEmitToolName,
		Description: "Emit the structured response as a JSON object. The arguments to this tool ARE the response.",
		InputSchema: anthropicJSONEmitInputSchema,
	}}
	anthropicJSONEmitToolChoice = &anthropicToolChoice{
		Type: "tool",
		Name: jsonEmitToolName,
	}
)

// anthropicCacheControl marks a content block as cacheable.
type anthropicCacheControl struct {
	Type string `json:"type"` // "ephemeral"
}

// anthropicSystemBlock is one text block of the system prompt, optionally
// carrying a cache_control breakpoint.
type anthropicSystemBlock struct {
	Type         string                 `json:"type"` // "text"
	Text         string                 `json:"text"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

// anthropicContentBlock is a single block in the response content array.
// `tool_use` blocks carry the structured-output payload when JSONMode coercion
// is on; Name/ID are ignored for any tool other than jsonEmitToolName.
type anthropicContentBlock struct {
	Type  string          `json:"type"`
	Text  string          `json:"text,omitempty"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
}

// anthropicUsage is the token usage block returned by the Anthropic API.
// Cache tokens are reported separately from input_tokens: cache reads and
// writes are input the model processed but are billed at different rates.
type anthropicUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
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
		messages = append(messages, anthropicMessage(m))
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
		if p.config.PromptCacheEnabled {
			// Send the system prompt as a single cacheable text block. Below
			// the model's minimum cacheable prefix the breakpoint is ignored.
			body.System = []anthropicSystemBlock{{
				Type:         "text",
				Text:         systemText,
				CacheControl: &anthropicCacheControl{Type: "ephemeral"},
			}}
		} else {
			body.System = systemText
		}
	}
	if req.Temperature != nil {
		body.Temperature = req.Temperature
	}
	if len(req.Stop) > 0 {
		body.StopSequences = req.Stop
	}
	coerceJSON := req.JSONMode && p.config.JSONModeToolUse
	if coerceJSON {
		// Anthropic's Messages API has no response_format flag. Forcing a
		// single tool_use call is the documented way to coerce structured JSON:
		// the model MUST emit a tool_use block matching the schema, and we read
		// its `input` as the body. The schema is intentionally generic so the
		// extraction prompts decide the shape; the empty `properties: {}` map is
		// harmless on the canonical API and avoids a 400 on stricter
		// Anthropic-compatible relays that require the key to be present.
		body.Tools = anthropicJSONEmitTools
		body.ToolChoice = anthropicJSONEmitToolChoice
	}

	var msgResp anthropicMessagesResponse
	if err := p.doRequest(ctx, http.MethodPost, "/v1/messages", body, &msgResp); err != nil {
		return nil, fmt.Errorf("anthropic: completion request failed: %w", err)
	}

	// Extract content from response blocks. When JSON coercion is on the model
	// returns a tool_use block whose `input` is the JSON payload; we surface
	// that as the body (dropping any leading preamble text block — concatenating
	// it would defeat the point). If the forced tool_use is missing (a proxy
	// that ignores tool_choice, or a refusal that arrives as text) we fall back
	// to the text blocks so the JSON-as-text case still parses and a refusal's
	// text reaches the caller's parser; the shared extraction layer fails loudly
	// on a truly empty body. Plain text blocks are concatenated as before.
	var content strings.Builder
	usedToolUse := false
	if coerceJSON {
		for _, block := range msgResp.Content {
			if block.Type != "tool_use" || block.Name != jsonEmitToolName {
				continue
			}
			trimmed := bytes.TrimSpace(block.Input)
			if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
				continue
			}
			content.Write(trimmed)
			usedToolUse = true
			break
		}
	}
	if !usedToolUse {
		if coerceJSON {
			slog.Warn("anthropic: JSONMode response had no emit_json tool_use block; falling back to text",
				"model", msgResp.Model, "stop_reason", msgResp.StopReason, "blocks", len(msgResp.Content))
		}
		for _, block := range msgResp.Content {
			if block.Type == "text" {
				content.WriteString(block.Text)
			}
		}
	}

	// Prompt tokens include cached input (reads + writes): the model processed
	// all of it, so undercounting it would understate usage when caching is on.
	promptTokens := msgResp.Usage.InputTokens +
		msgResp.Usage.CacheCreationInputTokens +
		msgResp.Usage.CacheReadInputTokens
	return &CompletionResponse{
		Content:      content.String(),
		Model:        msgResp.Model,
		FinishReason: msgResp.StopReason,
		Usage: TokenUsage{
			PromptTokens:     promptTokens,
			CompletionTokens: msgResp.Usage.OutputTokens,
			TotalTokens:      promptTokens + msgResp.Usage.OutputTokens,
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
	applyCustomHeaders(req, p.config.CustomHeaders, "Content-Type", "anthropic-version")
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
	defer func() { _ = resp.Body.Close() }()

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

	if err := errIfStreamedResponse("anthropic", resp.Header.Get("Content-Type"), respBody); err != nil {
		return err
	}

	if err := json.Unmarshal(respBody, dest); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return nil
}
