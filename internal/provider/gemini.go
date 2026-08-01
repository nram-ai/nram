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

// GeminiConfig holds the configuration for a Google Gemini provider.
type GeminiConfig struct {
	// APIKey is the API key for authentication (passed as ?key= query parameter).
	APIKey string

	// DefaultModel is the default model to use for completions when none is specified.
	DefaultModel string

	// DefaultEmbeddingModel is the default model to use for embeddings when none is specified.
	DefaultEmbeddingModel string

	// BaseURL is the base URL of the Gemini API. Defaults to "https://generativelanguage.googleapis.com".
	BaseURL string

	// Timeout is the HTTP client timeout. Defaults to 300s if zero.
	Timeout time.Duration

	// CustomHeaders are user-configured headers applied to every outbound
	// request (overriding built-ins except Content-Type). Gemini carries auth
	// in a query parameter, so these are typically proxy headers. Intended for
	// proxies/gateways between nram and the provider.
	CustomHeaders map[string]string

	// DisableThinking, when true, sends thinkingConfig.thinkingBudget:0 to turn
	// off the reasoning pass on models that accept it (the Gemini 2.5 Flash
	// family; see geminiThinkingOffSupported). Already resolved from the slot's
	// pointer (nil defaults to true) by the registry. When false, no thinkingConfig
	// is sent and the model keeps its default behavior.
	DisableThinking bool
}

// setHeaders sets the standard headers on an outbound request, then applies any
// user-configured custom headers (Content-Type is reserved).
func (p *GeminiProvider) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	applyCustomHeaders(req, p.config.CustomHeaders, "Content-Type")
}

// GeminiProvider implements both LLMProvider and EmbeddingProvider using the
// Google Gemini REST API. It uses the native Gemini API format (not the
// OpenAI-compatible proxy).
type GeminiProvider struct {
	config GeminiConfig
	client *http.Client
}

// Compile-time interface checks.
var (
	_ LLMProvider       = (*GeminiProvider)(nil)
	_ EmbeddingProvider = (*GeminiProvider)(nil)
	_ ProviderHealth    = (*GeminiProvider)(nil)
)

// NewGeminiProvider creates a new GeminiProvider with the given configuration.
func NewGeminiProvider(config GeminiConfig) *GeminiProvider {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 300 * time.Second
	}

	if config.BaseURL == "" {
		config.BaseURL = "https://generativelanguage.googleapis.com"
	}
	config.BaseURL = NormalizeBaseURL(config.BaseURL)

	if config.DefaultModel == "" {
		config.DefaultModel = "gemini-2.0-flash"
	}
	if config.DefaultEmbeddingModel == "" {
		config.DefaultEmbeddingModel = "text-embedding-004"
	}

	return &GeminiProvider{
		config: config,
		client: newHTTPClient(timeout),
	}
}

// ---------- Gemini API request/response types ----------

// geminiPart is a single part of content in the Gemini API.
type geminiPart struct {
	Text string `json:"text"`
}

// geminiContent represents a content block with a role and parts.
type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

// geminiGenerationConfig holds generation parameters for Gemini.
type geminiGenerationConfig struct {
	MaxOutputTokens  int                   `json:"maxOutputTokens,omitempty"`
	Temperature      *float64              `json:"temperature,omitempty"`
	StopSequences    []string              `json:"stopSequences,omitempty"`
	ResponseMimeType string                `json:"responseMimeType,omitempty"`
	ThinkingConfig   *geminiThinkingConfig `json:"thinkingConfig,omitempty"`
}

// geminiThinkingConfig controls the model's thinking pass. ThinkingBudget:0
// disables thinking; see geminiThinkingOffSupported for which models accept it.
type geminiThinkingConfig struct {
	ThinkingBudget *int `json:"thinkingBudget,omitempty"`
}

// geminiThinkingOffSupported reports whether thinkingBudget:0 is a valid
// "disable thinking" value for the model. Only the Gemini 2.5 Flash family
// accepts 0: Gemini 2.5 Pro requires a budget >= 128, pre-2.5 models reject
// thinkingConfig outright, and 3.x uses a different control (thinkingLevel).
// Allowlisting by name means a model that cannot disable thinking is never sent
// an invalid value that would fail the call; it just keeps its default behavior.
func geminiThinkingOffSupported(model string) bool {
	return strings.Contains(strings.ToLower(model), "2.5-flash")
}

// geminiGenerateRequest is the request body for generateContent. There is
// deliberately no stream field (unlike the Anthropic and OpenAI request types):
// Gemini is non-streaming by endpoint, since streaming is the separate
// :streamGenerateContent method, so there is nothing to opt out of here.
type geminiGenerateRequest struct {
	Contents          []geminiContent         `json:"contents"`
	SystemInstruction *geminiContent          `json:"systemInstruction,omitempty"`
	GenerationConfig  *geminiGenerationConfig `json:"generationConfig,omitempty"`
}

// geminiCandidate is a single candidate in a generateContent response.
type geminiCandidate struct {
	Content      geminiContent `json:"content"`
	FinishReason string        `json:"finishReason"`
}

// geminiUsageMetadata is the token usage block returned by the Gemini API.
//
// CachedContentTokenCount is a subset of PromptTokenCount, not an addition to
// it: the REST reference states promptTokenCount is "the total effective
// prompt size meaning this includes the number of tokens in the cached
// content." Gemini has no cache-write bucket (explicit caching bills storage
// per token-hour instead, which is not a per-call token count).
type geminiUsageMetadata struct {
	PromptTokenCount        int `json:"promptTokenCount"`
	CandidatesTokenCount    int `json:"candidatesTokenCount"`
	TotalTokenCount         int `json:"totalTokenCount"`
	CachedContentTokenCount int `json:"cachedContentTokenCount"`
}

// geminiGenerateResponse is the response body from generateContent.
type geminiGenerateResponse struct {
	Candidates    []geminiCandidate   `json:"candidates"`
	UsageMetadata geminiUsageMetadata `json:"usageMetadata"`
	ModelVersion  string              `json:"modelVersion"`
}

// geminiEmbedRequest is the request body for embedContent.
type geminiEmbedRequest struct {
	Model   string        `json:"model"`
	Content geminiContent `json:"content"`
}

// geminiEmbedResponse is the response body from embedContent.
type geminiEmbedResponse struct {
	Embedding struct {
		Values []float32 `json:"values"`
	} `json:"embedding"`
}

// geminiBatchEmbedRequest is the request body for batchEmbedContents.
type geminiBatchEmbedRequest struct {
	Requests []geminiEmbedRequest `json:"requests"`
}

// geminiBatchEmbedResponse is the response body from batchEmbedContents.
type geminiBatchEmbedResponse struct {
	Embeddings []struct {
		Values []float32 `json:"values"`
	} `json:"embeddings"`
}

// geminiErrorDetail captures the error detail in a Gemini error response.
type geminiErrorDetail struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

// geminiErrorResponse is the top-level error response from the Gemini API.
type geminiErrorResponse struct {
	Error geminiErrorDetail `json:"error"`
}

// geminiModel represents a single model entry from the models list endpoint.
type geminiModel struct {
	Name string `json:"name"`
}

// geminiModelsResponse is the response from GET /v1beta/models.
type geminiModelsResponse struct {
	Models []geminiModel `json:"models"`
}

// ---------- LLMProvider implementation ----------

// Complete sends a generateContent request to the Gemini API.
func (p *GeminiProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	model := req.Model
	if model == "" {
		model = p.config.DefaultModel
	}
	if model == "" {
		return nil, fmt.Errorf("gemini: no model specified and no default configured")
	}

	// Build contents from messages; extract system instruction separately.
	var systemInstruction *geminiContent
	var contents []geminiContent

	for _, m := range req.Messages {
		if m.Role == "system" {
			systemInstruction = &geminiContent{
				Parts: []geminiPart{{Text: m.Content}},
			}
			continue
		}
		role := m.Role
		if role == "assistant" {
			role = "model"
		}
		contents = append(contents, geminiContent{
			Role:  role,
			Parts: []geminiPart{{Text: m.Content}},
		})
	}

	body := geminiGenerateRequest{
		Contents:          contents,
		SystemInstruction: systemInstruction,
	}

	disableThinking := p.config.DisableThinking && geminiThinkingOffSupported(model)
	if req.MaxTokens > 0 || req.Temperature != nil || len(req.Stop) > 0 || req.JSONMode || disableThinking {
		gc := &geminiGenerationConfig{}
		if req.MaxTokens > 0 {
			gc.MaxOutputTokens = req.MaxTokens
		}
		if req.Temperature != nil {
			gc.Temperature = req.Temperature
		}
		if len(req.Stop) > 0 {
			gc.StopSequences = req.Stop
		}
		if req.JSONMode {
			gc.ResponseMimeType = "application/json"
		}
		if disableThinking {
			// These are extraction/decision/synthesis calls; the thinking pass is
			// wasted tokens. Force it off where the model supports it.
			zero := 0
			gc.ThinkingConfig = &geminiThinkingConfig{ThinkingBudget: &zero}
		}
		body.GenerationConfig = gc
	}

	// The :generateContent endpoint returns a single JSON response. Gemini's
	// streaming variant is a separate method (:streamGenerateContent), so this
	// call is non-streaming by construction; there is no stream flag to set.
	path := fmt.Sprintf("/v1beta/models/%s:generateContent", model)

	var genResp geminiGenerateResponse
	if err := p.doRequest(ctx, http.MethodPost, path, body, &genResp); err != nil {
		return nil, fmt.Errorf("gemini: completion request failed: %w", err)
	}

	if len(genResp.Candidates) == 0 {
		return nil, fmt.Errorf("gemini: completion returned no candidates")
	}

	candidate := genResp.Candidates[0]
	var content strings.Builder
	for _, part := range candidate.Content.Parts {
		content.WriteString(part.Text)
	}

	return &CompletionResponse{
		Content:      content.String(),
		Model:        model,
		FinishReason: strings.ToLower(candidate.FinishReason),
		Usage: TokenUsage{
			PromptTokens:     genResp.UsageMetadata.PromptTokenCount,
			CompletionTokens: genResp.UsageMetadata.CandidatesTokenCount,
			TotalTokens:      genResp.UsageMetadata.TotalTokenCount,
			// Already inside PromptTokenCount; recorded, never added.
			CacheReadTokens: genResp.UsageMetadata.CachedContentTokenCount,
		},
	}, nil
}

// Name returns the provider identifier.
func (p *GeminiProvider) Name() string {
	return "gemini"
}

// Models returns the configured model identifiers.
func (p *GeminiProvider) Models() []string {
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

// Embed sends an embedding request to the Gemini API. For a single input it
// uses embedContent; for multiple inputs it uses batchEmbedContents.
func (p *GeminiProvider) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	model := req.Model
	if model == "" {
		model = p.config.DefaultEmbeddingModel
	}
	if model == "" {
		return nil, fmt.Errorf("gemini: no embedding model specified and no default configured")
	}

	if len(req.Input) == 1 {
		return p.embedSingle(ctx, model, req.Input[0])
	}
	return p.embedBatch(ctx, model, req.Input)
}

// embedSingle uses the embedContent endpoint for a single text input.
func (p *GeminiProvider) embedSingle(ctx context.Context, model, text string) (*EmbeddingResponse, error) {
	body := geminiEmbedRequest{
		Model: "models/" + model,
		Content: geminiContent{
			Parts: []geminiPart{{Text: text}},
		},
	}

	path := fmt.Sprintf("/v1beta/models/%s:embedContent", model)

	var embResp geminiEmbedResponse
	if err := p.doRequest(ctx, http.MethodPost, path, body, &embResp); err != nil {
		return nil, fmt.Errorf("gemini: embedding request failed: %w", err)
	}

	return &EmbeddingResponse{
		Embeddings: [][]float32{embResp.Embedding.Values},
		Model:      model,
		Usage: TokenUsage{
			PromptTokens: 1,
			TotalTokens:  1,
		},
	}, nil
}

// embedBatch uses the batchEmbedContents endpoint for multiple text inputs.
func (p *GeminiProvider) embedBatch(ctx context.Context, model string, texts []string) (*EmbeddingResponse, error) {
	requests := make([]geminiEmbedRequest, len(texts))
	for i, text := range texts {
		requests[i] = geminiEmbedRequest{
			Model: "models/" + model,
			Content: geminiContent{
				Parts: []geminiPart{{Text: text}},
			},
		}
	}

	body := geminiBatchEmbedRequest{Requests: requests}
	path := fmt.Sprintf("/v1beta/models/%s:batchEmbedContents", model)

	var batchResp geminiBatchEmbedResponse
	if err := p.doRequest(ctx, http.MethodPost, path, body, &batchResp); err != nil {
		return nil, fmt.Errorf("gemini: batch embedding request failed: %w", err)
	}

	embeddings := make([][]float32, len(batchResp.Embeddings))
	for i, emb := range batchResp.Embeddings {
		embeddings[i] = emb.Values
	}

	return &EmbeddingResponse{
		Embeddings: embeddings,
		Model:      model,
		Usage: TokenUsage{
			PromptTokens: len(texts),
			TotalTokens:  len(texts),
		},
	}, nil
}

// Dimensions returns common Gemini embedding dimensions.
func (p *GeminiProvider) Dimensions() []int {
	return []int{768}
}

// ---------- ProviderHealth implementation ----------

// Ping verifies connectivity by listing models via GET /v1beta/models.
func (p *GeminiProvider) Ping(ctx context.Context) error {
	url := p.config.BaseURL + "/v1beta/models"
	if p.config.APIKey != "" {
		url += "?key=" + p.config.APIKey
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("gemini: failed to create ping request: %w", err)
	}
	p.setHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("gemini: ping failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("gemini: ping returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ---------- Internal helpers ----------

// doRequest marshals the request body, sends it to the given path with the API
// key as a query parameter, and unmarshals the response into dest.
func (p *GeminiProvider) doRequest(ctx context.Context, method, path string, body any, dest any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := p.config.BaseURL + path
	if p.config.APIKey != "" {
		url += "?key=" + p.config.APIKey
	}

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
		var apiErr geminiErrorResponse
		if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Error.Message != "" {
			return fmt.Errorf("API error (%d): %s [status=%s]",
				resp.StatusCode, apiErr.Error.Message, apiErr.Error.Status)
		}
		return fmt.Errorf("API error (%d): %s", resp.StatusCode, string(respBody))
	}

	if err := errIfStreamedResponse("gemini", resp.Header.Get("Content-Type"), respBody); err != nil {
		return err
	}

	if err := json.Unmarshal(respBody, dest); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return nil
}
