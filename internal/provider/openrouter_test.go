package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenRouterContextLength_FindsConfiguredModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/models" {
			t.Errorf("expected /api/v1/models, got %s", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q, want Bearer test-key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openRouterModelsResponse{
			Data: []openRouterModel{
				{ID: "anthropic/claude-sonnet-4", ContextLength: 200000},
				{ID: "openai/gpt-4o", ContextLength: 128000},
				{ID: "meta-llama/llama-3.1-70b", ContextLength: 131072},
			},
		})
	}))
	defer srv.Close()

	got, err := OpenRouterContextLength(context.Background(), srv.URL+"/api/v1", "test-key", "openai/gpt-4o")
	if err != nil {
		t.Fatalf("OpenRouterContextLength: %v", err)
	}
	if got != 128000 {
		t.Errorf("got %d, want 128000", got)
	}
}

func TestOpenRouterContextLength_UnknownModelReturnsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(openRouterModelsResponse{
			Data: []openRouterModel{{ID: "openai/gpt-4o", ContextLength: 128000}},
		})
	}))
	defer srv.Close()

	got, err := OpenRouterContextLength(context.Background(), srv.URL+"/api/v1", "", "missing/model")
	if err != nil {
		t.Fatalf("OpenRouterContextLength: %v", err)
	}
	if got != 0 {
		t.Errorf("got %d, want 0 for unknown model", got)
	}
}

func TestOpenRouterContextLength_EmptyModelReturnsZero(t *testing.T) {
	got, err := OpenRouterContextLength(context.Background(), "https://openrouter.ai/api/v1", "", "")
	if err != nil {
		t.Fatalf("OpenRouterContextLength: %v", err)
	}
	if got != 0 {
		t.Errorf("got %d, want 0 for empty model", got)
	}
}

func TestOpenRouterContextLength_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := OpenRouterContextLength(context.Background(), srv.URL+"/api/v1", "", "any/model")
	if err == nil {
		t.Fatal("expected error on 429, got nil")
	}
}

func TestNormalizeOpenRouterModelsURL(t *testing.T) {
	cases := map[string]string{
		"":                              "https://openrouter.ai/api/v1/models",
		"https://openrouter.ai/api/v1":  "https://openrouter.ai/api/v1/models",
		"https://openrouter.ai/api/v1/": "https://openrouter.ai/api/v1/models",
		"https://openrouter.ai/api/v1/chat/completions": "https://openrouter.ai/api/v1/models",
		"https://openrouter.ai/api/v1/completions":      "https://openrouter.ai/api/v1/models",
		"https://openrouter.ai/api/v1/embeddings":       "https://openrouter.ai/api/v1/models",
		"https://openrouter.ai/api/v1/models":           "https://openrouter.ai/api/v1/models",
	}
	for input, want := range cases {
		got := normalizeOpenRouterModelsURL(input)
		if got != want {
			t.Errorf("normalizeOpenRouterModelsURL(%q) = %q, want %q", input, got, want)
		}
	}
}
