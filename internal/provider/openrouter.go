package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenRouter routes inference through the OpenAI-compatible adapter, but its
// /api/v1/models endpoint exposes a richer schema than vanilla OpenAI:
// notably context_length per model, which lets the admin UI display the
// runtime limit alongside the model name.

const defaultOpenRouterBaseURL = "https://openrouter.ai/api/v1"

// openRouterModelsResponse mirrors the public schema of GET
// {base}/models. Only the fields used here are decoded.
type openRouterModelsResponse struct {
	Data []openRouterModel `json:"data"`
}

type openRouterModel struct {
	ID            string `json:"id"`
	ContextLength int    `json:"context_length"`
}

// OpenRouterContextLength looks up the configured model in OpenRouter's
// public /models catalog and returns its context_length. Returns 0 (no
// error) when the response decodes successfully but the model id is not
// present in the catalog; the caller treats that as "unknown."
//
// baseURL is the slot's configured URL; it is normalized to strip the
// trailing /chat/completions or similar so the /models endpoint is reachable
// even when the slot points at a sub-path.
func OpenRouterContextLength(ctx context.Context, baseURL, apiKey, modelID string, headers map[string]string) (int, error) {
	if modelID == "" {
		return 0, nil
	}

	url := normalizeOpenRouterModelsURL(baseURL)

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("openrouter: create models request: %w", err)
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	applyCustomHeaders(req, headers)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("openrouter: models request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("openrouter: read models response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("openrouter: models returned status %d: %s", resp.StatusCode, string(body))
	}

	var parsed openRouterModelsResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0, fmt.Errorf("openrouter: unmarshal models response: %w", err)
	}

	for _, m := range parsed.Data {
		if m.ID == modelID {
			return m.ContextLength, nil
		}
	}

	return 0, nil
}

// normalizeOpenRouterModelsURL returns "{base}/models" given any reasonable
// base URL the user might have entered: the canonical /api/v1, a trailing
// slash, or an unrelated sub-path. Falls back to OpenRouter's default base
// URL when baseURL is empty.
func normalizeOpenRouterModelsURL(baseURL string) string {
	base := strings.TrimRight(baseURL, "/")
	if base == "" {
		base = defaultOpenRouterBaseURL
	}
	// Strip common chat-completion suffixes the user may have pasted in.
	for _, suffix := range []string{"/chat/completions", "/completions", "/embeddings", "/models"} {
		base = strings.TrimSuffix(base, suffix)
	}
	return base + "/models"
}
