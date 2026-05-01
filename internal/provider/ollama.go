package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OllamaConfig holds the configuration for an Ollama discovery/management client.
type OllamaConfig struct {
	// BaseURL is the base URL of the Ollama API. Defaults to "http://localhost:11434".
	BaseURL string

	// Timeout is the HTTP client timeout for regular operations. Defaults to 120s if zero.
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
// map and pull `*.context_length` out lazily.
type ollamaShowResponse struct {
	ModelInfo map[string]any `json:"model_info"`
}

// NewOllamaClient creates a new OllamaClient with the given configuration.
func NewOllamaClient(config OllamaConfig) *OllamaClient {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 120 * time.Second
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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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

// ContextLength queries POST /api/show for the named model and returns the
// architecture-specific context_length from the GGUF metadata. Returns 0
// (no error) when Ollama responds but the metadata does not advertise a
// context_length — the caller treats that as "unknown" and the UI shows
// the muted fallback. A non-nil error means the HTTP call itself failed.
//
// The architecture key in model_info varies per family (llama, qwen2,
// bert, gemma, etc.), so we scan for any entry whose key ends in
// ".context_length" rather than hard-coding a per-arch list. This keeps
// the surface working for new model families without code changes.
func (c *OllamaClient) ContextLength(ctx context.Context, modelName string) (int, error) {
	url := c.config.BaseURL + "/api/show"

	body, err := json.Marshal(ollamaShowRequest{Name: modelName})
	if err != nil {
		return 0, fmt.Errorf("ollama: marshal show request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("ollama: create show request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("ollama: show request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("ollama: read show response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("ollama: show returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var show ollamaShowResponse
	if err := json.Unmarshal(respBody, &show); err != nil {
		return 0, fmt.Errorf("ollama: unmarshal show response: %w", err)
	}

	for k, v := range show.ModelInfo {
		if !strings.HasSuffix(k, ".context_length") {
			continue
		}
		switch n := v.(type) {
		case float64:
			return int(n), nil
		case int:
			return n, nil
		case int64:
			return int(n), nil
		}
	}

	return 0, nil
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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ollama: probe returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
