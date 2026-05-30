package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// OllamaConfig holds the configuration for an Ollama discovery/management client.
type OllamaConfig struct {
	// BaseURL is the base URL of the Ollama API. Defaults to "http://localhost:11434".
	BaseURL string

	// Timeout is the HTTP client timeout for regular operations. Defaults to 300s if zero.
	Timeout time.Duration

	// PullTimeout is the HTTP client timeout for model pull operations. Defaults to 10 minutes if zero.
	PullTimeout time.Duration
}

// OllamaClient is a discovery and management client for Ollama. It is not a
// provider itself — actual LLM and embedding calls go through the OpenAI-compatible
// adapter pointed at the Ollama server. This client handles model listing,
// pulling, and health probing.
type OllamaClient struct {
	config OllamaConfig
	client *http.Client
}

// OllamaModel represents a single model available on the Ollama server.
type OllamaModel struct {
	Name       string    `json:"name"`
	Size       int64     `json:"size"`
	Digest     string    `json:"digest"`
	ModifiedAt time.Time `json:"modified_at"`
}

// PullProgress represents a single progress update during a model pull.
type PullProgress struct {
	Status    string `json:"status"`
	Completed int64  `json:"completed"`
	Total     int64  `json:"total"`
}

// ollamaTagsResponse is the response body from GET /api/tags.
type ollamaTagsResponse struct {
	Models []OllamaModel `json:"models"`
}

// ollamaPullRequest is the request body for POST /api/pull.
type ollamaPullRequest struct {
	Name   string `json:"name"`
	Stream bool   `json:"stream"`
}

// ollamaShowRequest is the request body for POST /api/show.
type ollamaShowRequest struct {
	Name string `json:"name"`
}

// ollamaShowResponse is the response body from POST /api/show. ModelInfo is
// a free-form map keyed by architecture-prefixed names (e.g.
// "llama.context_length", "qwen2.context_length", "bert.context_length")
// because Ollama exposes the raw GGUF metadata. We treat it as a generic
// map and pull `*.context_length` out lazily. Parameters is the Modelfile
// PARAMETER block as a single multi-line string (e.g.
// "num_ctx 8192\nstop \"<|im_start|>\"\n...") and we scan it for the
// num_ctx line as the second-tier signal of Ollama's configured context.
type ollamaShowResponse struct {
	ModelInfo  map[string]any `json:"model_info"`
	Parameters string         `json:"parameters"`
}

// ollamaPSResponse is the response body from GET /api/ps — the list of
// currently-loaded models. We use the per-model context_length field as
// the most authoritative signal of the context Ollama actually serves
// the model with (Modelfile defaults can be overridden at load time).
type ollamaPSResponse struct {
	Models []ollamaPSModel `json:"models"`
}

// ollamaPSModel is a single entry in /api/ps.models. Ollama populates
// both name and model with the same fully-qualified tag (e.g.
// "qwen2:7b"), but we match against either to be defensive against
// future divergence.
type ollamaPSModel struct {
	Name          string `json:"name"`
	Model         string `json:"model"`
	ContextLength int    `json:"context_length"`
}

// NewOllamaClient creates a new OllamaClient with the given configuration.
func NewOllamaClient(config OllamaConfig) *OllamaClient {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 300 * time.Second
	}

	if config.PullTimeout == 0 {
		config.PullTimeout = 10 * time.Minute
	}

	if config.BaseURL == "" {
		config.BaseURL = "http://localhost:11434"
	}
	config.BaseURL = strings.TrimRight(config.BaseURL, "/")

	return &OllamaClient{
		config: config,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// ListModels retrieves the list of models available on the Ollama server.
func (c *OllamaClient) ListModels(ctx context.Context) ([]OllamaModel, error) {
	url := c.config.BaseURL + "/api/tags"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("ollama: failed to create list request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama: list models request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ollama: failed to read list response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ollama: list models returned status %d: %s", resp.StatusCode, string(body))
	}

	var tagsResp ollamaTagsResponse
	if err := json.Unmarshal(body, &tagsResp); err != nil {
		return nil, fmt.Errorf("ollama: failed to unmarshal list response: %w", err)
	}

	if tagsResp.Models == nil {
		return []OllamaModel{}, nil
	}

	return tagsResp.Models, nil
}

// PullModel pulls a model from the Ollama registry. It streams progress updates
// and calls the optional progress callback for each update received.
func (c *OllamaClient) PullModel(ctx context.Context, name string, progress func(PullProgress)) error {
	pullBody := ollamaPullRequest{
		Name:   name,
		Stream: true,
	}

	jsonBody, err := json.Marshal(pullBody)
	if err != nil {
		return fmt.Errorf("ollama: failed to marshal pull request: %w", err)
	}

	url := c.config.BaseURL + "/api/pull"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("ollama: failed to create pull request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Use a separate client with PullTimeout for long-running pull operations.
	pullClient := &http.Client{
		Timeout: c.config.PullTimeout,
	}

	resp, err := pullClient.Do(req)
	if err != nil {
		return fmt.Errorf("ollama: pull request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ollama: pull returned status %d: %s", resp.StatusCode, string(body))
	}

	// Read streaming newline-delimited JSON progress updates.
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var p PullProgress
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			continue
		}

		// Check for error status in the stream.
		if strings.HasPrefix(p.Status, "error") {
			return fmt.Errorf("ollama: pull failed: %s", p.Status)
		}

		if progress != nil {
			progress(p)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("ollama: error reading pull stream: %w", err)
	}

	return nil
}

// ContextLength returns the effective and maximum context window in tokens
// for the named Ollama model.
//
// modelMax is the model's GGUF-declared ceiling, parsed from
// /api/show's model_info.<arch>.context_length. The architecture prefix
// varies per family (llama, qwen2, bert, gemma, ...) so we scan for any
// entry whose key ends in ".context_length" rather than hard-coding a
// per-arch list — keeps the surface working for new model families.
//
// effective is min(modelMax, configured) when both are known, otherwise
// whichever side is known. configured is resolved in priority order:
//  1. /api/ps — the actual context the runner allocated for the
//     currently-loaded model. Authoritative because the loader can
//     override the Modelfile default at load time.
//  2. /api/show parameters — the Modelfile PARAMETER num_ctx (the
//     default Ollama would use to load the model). Used as a fallback
//     when /api/ps has no entry for the model (model not loaded).
//
// Returns 0 for both values when Ollama responds but advertises nothing
// — the caller treats that as "unknown" and the UI shows the muted
// fallback. A non-nil error means the /api/show HTTP call itself
// failed; /api/ps failures are non-fatal and only suppress the
// runtime-context signal.
func (c *OllamaClient) ContextLength(ctx context.Context, modelName string) (effective, modelMax int, err error) {
	show, err := c.fetchShow(ctx, modelName)
	if err != nil {
		return 0, 0, err
	}

	modelMax = scanModelInfoContextLength(show.ModelInfo)
	paramNumCtx := parseNumCtxFromParameters(show.Parameters)

	psNumCtx := 0
	if running, perr := c.runningModels(ctx); perr == nil {
		psNumCtx = lookupRunningContext(running, modelName)
	}

	configured := psNumCtx
	if configured == 0 {
		configured = paramNumCtx
	}

	effective = modelMax
	if configured > 0 && (modelMax == 0 || configured < modelMax) {
		effective = configured
	}

	return effective, modelMax, nil
}

// fetchShow performs POST /api/show for the named model and returns the
// decoded response.
func (c *OllamaClient) fetchShow(ctx context.Context, modelName string) (ollamaShowResponse, error) {
	body, err := json.Marshal(ollamaShowRequest{Name: modelName})
	if err != nil {
		return ollamaShowResponse{}, fmt.Errorf("ollama: marshal show request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.BaseURL+"/api/show", bytes.NewReader(body))
	if err != nil {
		return ollamaShowResponse{}, fmt.Errorf("ollama: create show request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return ollamaShowResponse{}, fmt.Errorf("ollama: show request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return ollamaShowResponse{}, fmt.Errorf("ollama: read show response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ollamaShowResponse{}, fmt.Errorf("ollama: show returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var show ollamaShowResponse
	if err := json.Unmarshal(respBody, &show); err != nil {
		return ollamaShowResponse{}, fmt.Errorf("ollama: unmarshal show response: %w", err)
	}
	return show, nil
}

// runningModels performs GET /api/ps and returns the list of currently
// loaded models. Used to discover the actual context Ollama allocated
// for a model, which can differ from the Modelfile default when the
// loader was given an options.num_ctx override.
func (c *OllamaClient) runningModels(ctx context.Context) ([]ollamaPSModel, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.config.BaseURL+"/api/ps", nil)
	if err != nil {
		return nil, fmt.Errorf("ollama: create ps request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama: ps request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ollama: read ps response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ollama: ps returned status %d: %s", resp.StatusCode, string(body))
	}

	var ps ollamaPSResponse
	if err := json.Unmarshal(body, &ps); err != nil {
		return nil, fmt.Errorf("ollama: unmarshal ps response: %w", err)
	}
	return ps.Models, nil
}

// scanModelInfoContextLength pulls the architecture-specific
// context_length out of an Ollama /api/show model_info map. Returns 0
// when no matching key is present.
func scanModelInfoContextLength(modelInfo map[string]any) int {
	for k, v := range modelInfo {
		if !strings.HasSuffix(k, ".context_length") {
			continue
		}
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case int64:
			return int(n)
		}
	}
	return 0
}

// parseNumCtxFromParameters scans Ollama's /api/show parameters string
// (Modelfile PARAMETER lines, one per line, whitespace-separated) for a
// num_ctx entry and returns its integer value. Returns 0 when the
// parameter is absent or the value cannot be parsed — the caller treats
// 0 as "unknown" and continues down the fallback chain.
func parseNumCtxFromParameters(s string) int {
	if s == "" {
		return 0
	}
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 || fields[0] != "num_ctx" {
			continue
		}
		if n, err := strconv.Atoi(fields[1]); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// lookupRunningContext returns the loaded context_length for the named
// model from a /api/ps response, or 0 when no entry matches. Match is
// against either the name or the model field — Ollama populates both
// with the same tag today, but we accept either to stay defensive.
func lookupRunningContext(running []ollamaPSModel, modelName string) int {
	for _, m := range running {
		if m.Name == modelName || m.Model == modelName {
			if m.ContextLength > 0 {
				return m.ContextLength
			}
		}
	}
	return 0
}

// ProbeURL performs a simple health check against the Ollama server.
// It sends a GET request to the root URL and checks for a 200 status code.
func (c *OllamaClient) ProbeURL(ctx context.Context) error {
	url := c.config.BaseURL + "/"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("ollama: failed to create probe request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("ollama: probe failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ollama: probe returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
