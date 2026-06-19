package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestServer creates an httptest.Server that routes to the given handler map.
// Keys are "METHOD /path" strings.
func newTestServer(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		if h, ok := handlers[key]; ok {
			h(w, r)
			return
		}
		http.NotFound(w, r)
	}))
}

func TestOpenAIComplete(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			var req openaiChatRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			resp := openaiChatResponse{
				ID:    "chatcmpl-test123",
				Model: req.Model,
				Choices: []openaiChatChoice{
					{
						Index:        0,
						Message:      openaiChatMessage{Role: "assistant", Content: "Hello! How can I help you?"},
						FinishReason: "stop",
					},
				},
				Usage: openaiUsage{
					PromptTokens:     10,
					CompletionTokens: 8,
					TotalTokens:      18,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "gpt-4",
	})

	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages:    []Message{{Role: "user", Content: "Hello"}},
		MaxTokens:   100,
		Temperature: 0.7,
		Stop:        []string{"\n"},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if resp.Content != "Hello! How can I help you?" {
		t.Errorf("Content = %q, want %q", resp.Content, "Hello! How can I help you?")
	}
	if resp.Model != "gpt-4" {
		t.Errorf("Model = %q, want %q", resp.Model, "gpt-4")
	}
	if resp.FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want %q", resp.FinishReason, "stop")
	}
	if resp.Usage.TotalTokens != 18 {
		t.Errorf("TotalTokens = %d, want %d", resp.Usage.TotalTokens, 18)
	}
}

func TestOpenAICompleteAPIError(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(openaiErrorResponse{
				Error: openaiErrorDetail{
					Message: "Rate limit exceeded",
					Type:    "rate_limit_error",
					Code:    "rate_limit_exceeded",
				},
			})
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "gpt-4",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err == nil {
		t.Fatal("Complete() expected error, got nil")
	}
	if got := err.Error(); !contains(got, "Rate limit exceeded") {
		t.Errorf("error = %q, want it to contain %q", got, "Rate limit exceeded")
	}
}

func TestOpenAICompleteNoModel(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL: "http://localhost",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err == nil {
		t.Fatal("Complete() expected error for missing model, got nil")
	}
}

func TestOpenAIEmbed(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/embeddings": func(w http.ResponseWriter, r *http.Request) {
			var req openaiEmbeddingRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			data := make([]openaiEmbeddingData, len(req.Input))
			for i := range req.Input {
				data[i] = openaiEmbeddingData{
					Object:    "embedding",
					Embedding: []float32{0.1, 0.2, 0.3},
					Index:     i,
				}
			}

			resp := openaiEmbeddingResponse{
				Object: "list",
				Data:   data,
				Model:  req.Model,
				Usage: openaiUsage{
					PromptTokens: len(req.Input) * 5,
					TotalTokens:  len(req.Input) * 5,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:               srv.URL,
		APIKey:                "test-key",
		DefaultEmbeddingModel: "text-embedding-3-small",
	})

	resp, err := p.Embed(context.Background(), &EmbeddingRequest{
		Input: []string{"Hello world"},
	})
	if err != nil {
		t.Fatalf("Embed() error: %v", err)
	}
	if len(resp.Embeddings) != 1 {
		t.Fatalf("Embeddings length = %d, want 1", len(resp.Embeddings))
	}
	if len(resp.Embeddings[0]) != 3 {
		t.Errorf("Embedding dimension = %d, want 3", len(resp.Embeddings[0]))
	}
	if resp.Model != "text-embedding-3-small" {
		t.Errorf("Model = %q, want %q", resp.Model, "text-embedding-3-small")
	}
}

// TestOpenAIEmbedBaseURLWithVersionSuffix is a regression test for base-URL
// double-stacking: a BaseURL that already ends in "/v1" must not produce a
// "/v1/v1/embeddings" request. The constructor normalizes the version segment
// away, so the outbound path is exactly "/v1/embeddings".
func TestOpenAIEmbedBaseURLWithVersionSuffix(t *testing.T) {
	var gotPath string
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/embeddings": func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			resp := openaiEmbeddingResponse{
				Object: "list",
				Data:   []openaiEmbeddingData{{Object: "embedding", Embedding: []float32{0.1, 0.2, 0.3}, Index: 0}},
				Model:  "text-embedding-3-small",
				Usage:  openaiUsage{PromptTokens: 5, TotalTokens: 5},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	// Caller supplies a version-suffixed base URL (as a UI default or a pasted
	// vendor URL once did).
	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:               srv.URL + "/v1",
		APIKey:                "test-key",
		DefaultEmbeddingModel: "text-embedding-3-small",
	})

	resp, err := p.Embed(context.Background(), &EmbeddingRequest{Input: []string{"Hello world"}})
	if err != nil {
		t.Fatalf("Embed() error: %v", err)
	}
	if len(resp.Embeddings) != 1 {
		t.Fatalf("Embeddings length = %d, want 1", len(resp.Embeddings))
	}
	if gotPath != "/v1/embeddings" {
		t.Errorf("request path = %q, want %q (double-stacking regression)", gotPath, "/v1/embeddings")
	}
}

func TestOpenAIEmbedMultipleInputs(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/embeddings": func(w http.ResponseWriter, r *http.Request) {
			var req openaiEmbeddingRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			data := make([]openaiEmbeddingData, len(req.Input))
			for i := range req.Input {
				emb := make([]float32, 4)
				for j := range emb {
					emb[j] = float32(i)*0.1 + float32(j)*0.01
				}
				data[i] = openaiEmbeddingData{
					Object:    "embedding",
					Embedding: emb,
					Index:     i,
				}
			}

			resp := openaiEmbeddingResponse{
				Object: "list",
				Data:   data,
				Model:  req.Model,
				Usage: openaiUsage{
					PromptTokens: len(req.Input) * 5,
					TotalTokens:  len(req.Input) * 5,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:               srv.URL,
		APIKey:                "test-key",
		DefaultEmbeddingModel: "text-embedding-3-small",
	})

	inputs := []string{"first", "second", "third"}
	resp, err := p.Embed(context.Background(), &EmbeddingRequest{
		Input: inputs,
	})
	if err != nil {
		t.Fatalf("Embed() error: %v", err)
	}
	if len(resp.Embeddings) != 3 {
		t.Fatalf("Embeddings length = %d, want 3", len(resp.Embeddings))
	}
	for i, emb := range resp.Embeddings {
		if len(emb) != 4 {
			t.Errorf("Embedding[%d] dimension = %d, want 4", i, len(emb))
		}
	}
}

func TestOpenAIPingSuccess(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"GET /v1/models": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL: srv.URL,
		APIKey:  "test-key",
	})

	if err := p.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error: %v", err)
	}
}

func TestOpenAIPingFailure(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"GET /v1/models": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"invalid api key"}}`))
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL: srv.URL,
		APIKey:  "bad-key",
	})

	if err := p.Ping(context.Background()); err == nil {
		t.Fatal("Ping() expected error, got nil")
	}
}

func TestOpenAICustomBaseURL(t *testing.T) {
	var receivedHost string
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			receivedHost = r.Host
			resp := openaiChatResponse{
				Model: "local-model",
				Choices: []openaiChatChoice{
					{Message: openaiChatMessage{Role: "assistant", Content: "ok"}, FinishReason: "stop"},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		DefaultModel: "local-model",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if receivedHost == "" {
		t.Fatal("request was not received by the custom server")
	}
}

func TestOpenAIRequestHeaders(t *testing.T) {
	var (
		gotAuth         string
		gotContentType  string
		gotOrganization string
	)

	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			gotContentType = r.Header.Get("Content-Type")
			gotOrganization = r.Header.Get("OpenAI-Organization")

			resp := openaiChatResponse{
				Model: "gpt-4",
				Choices: []openaiChatChoice{
					{Message: openaiChatMessage{Role: "assistant", Content: "ok"}, FinishReason: "stop"},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		APIKey:       "sk-test-key-123",
		DefaultModel: "gpt-4",
		Organization: "org-abc",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	if gotAuth != "Bearer sk-test-key-123" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer sk-test-key-123")
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", gotContentType, "application/json")
	}
	if gotOrganization != "org-abc" {
		t.Errorf("OpenAI-Organization = %q, want %q", gotOrganization, "org-abc")
	}
}

func TestOpenAIName(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{})
	if got := p.Name(); got != "openai" {
		t.Errorf("Name() = %q, want %q", got, "openai")
	}
}

func TestOpenAIModels(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{
		DefaultModel:          "gpt-4",
		DefaultEmbeddingModel: "text-embedding-3-small",
	})
	models := p.Models()
	if len(models) != 2 {
		t.Fatalf("Models() length = %d, want 2", len(models))
	}
	if models[0] != "gpt-4" {
		t.Errorf("Models()[0] = %q, want %q", models[0], "gpt-4")
	}
	if models[1] != "text-embedding-3-small" {
		t.Errorf("Models()[1] = %q, want %q", models[1], "text-embedding-3-small")
	}
}

func TestOpenAIDimensions(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{})
	dims := p.Dimensions()
	if len(dims) == 0 {
		t.Fatal("Dimensions() returned empty slice")
	}
	// Should include standard OpenAI dimensions.
	found1536 := false
	for _, d := range dims {
		if d == 1536 {
			found1536 = true
		}
	}
	if !found1536 {
		t.Error("Dimensions() does not include 1536")
	}
}

func TestOpenAIDefaultTimeout(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{})
	if p.client.Timeout != 300*time.Second {
		t.Errorf("default timeout = %v, want %v", p.client.Timeout, 300*time.Second)
	}
}

func TestOpenAICustomTimeout(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{Timeout: 60 * time.Second})
	if p.client.Timeout != 60*time.Second {
		t.Errorf("timeout = %v, want %v", p.client.Timeout, 60*time.Second)
	}
}

// contains is a helper for substring checks in tests.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestOpenAIComplete_OllamaExtensionsGated_Ollama verifies that
// reasoning_effort lands in the request body when ProviderType is "ollama".
// This is the gating that disables the model's thinking pass on Ollama's
// OpenAI-compat endpoint without leaking the field to strict OpenAI.
func TestOpenAIComplete_OllamaExtensionsGated_Ollama(t *testing.T) {
	var receivedBody map[string]any
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&receivedBody)
			resp := openaiChatResponse{
				Model: "qwen3:8b-extract",
				Choices: []openaiChatChoice{{
					Message:      openaiChatMessage{Role: "assistant", Content: "[]"},
					FinishReason: "stop",
				}},
				Usage: openaiUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		DefaultModel: "qwen3:8b-extract",
		ProviderType: ProviderTypeOllama,
		Timeout:      5 * time.Second,
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages:    []Message{{Role: "user", Content: "ping"}},
		MaxTokens:   128,
		Temperature: 0.1,
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}

	if got, ok := receivedBody["reasoning_effort"]; !ok {
		t.Errorf("expected reasoning_effort in request body for Ollama provider")
	} else if got != "none" {
		t.Errorf("reasoning_effort = %v, want \"none\"", got)
	}
}

// TestOpenAIComplete_OllamaExtensionsGated_OpenAI verifies that
// reasoning_effort is stripped when ProviderType is "openai"; strict OpenAI
// endpoints reject unknown fields, so leaking it would break operator
// installs that point at api.openai.com.
func TestOpenAIComplete_OllamaExtensionsGated_OpenAI(t *testing.T) {
	var receivedBody map[string]any
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&receivedBody)
			resp := openaiChatResponse{
				Model: "gpt-4",
				Choices: []openaiChatChoice{{
					Message:      openaiChatMessage{Role: "assistant", Content: "[]"},
					FinishReason: "stop",
				}},
				Usage: openaiUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		DefaultModel: "gpt-4",
		ProviderType: ProviderTypeOpenAI,
		Timeout:      5 * time.Second,
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages:    []Message{{Role: "user", Content: "ping"}},
		MaxTokens:   128,
		Temperature: 0.1,
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}

	if _, present := receivedBody["reasoning_effort"]; present {
		t.Errorf("OpenAI provider must not send Ollama extension \"reasoning_effort\"; body had it")
	}
}

// chatCaptureServer returns a test server whose chat-completions handler decodes
// the request body into *dst and replies with a minimal valid completion.
func chatCaptureServer(t *testing.T, dst *map[string]any) *httptest.Server {
	t.Helper()
	return newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(dst)
			resp := openaiChatResponse{
				Model: "qwen3",
				Choices: []openaiChatChoice{{
					Message:      openaiChatMessage{Role: "assistant", Content: "[]"},
					FinishReason: "stop",
				}},
				Usage: openaiUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
}

// nestedBool reads body[outer][inner] as a bool, reporting whether the path
// existed and decoded to a bool. JSON objects decode to map[string]any.
func nestedBool(body map[string]any, outer, inner string) (val, ok bool) {
	m, isMap := body[outer].(map[string]any)
	if !isMap {
		return false, false
	}
	b, isBool := m[inner].(bool)
	return b, isBool
}

// vLLM and SGLang disable a Qwen3 thinking pass via top-level
// chat_template_kwargs:{enable_thinking:false} (reasoning_effort is ignored by
// those engines). This is the analog of the Ollama gating above.
func TestOpenAIComplete_ChatTemplateKwargsGated(t *testing.T) {
	for _, ptype := range []string{ProviderTypeVLLM, ProviderTypeSGLang} {
		t.Run(ptype, func(t *testing.T) {
			var got map[string]any
			srv := chatCaptureServer(t, &got)
			defer srv.Close()

			p := NewOpenAIProvider(OpenAIConfig{
				BaseURL:      srv.URL,
				DefaultModel: "qwen3",
				ProviderType: ptype,
				Timeout:      5 * time.Second,
			})
			if _, err := p.Complete(context.Background(), &CompletionRequest{
				Messages: []Message{{Role: "user", Content: "ping"}},
			}); err != nil {
				t.Fatalf("complete: %v", err)
			}

			if v, ok := nestedBool(got, "chat_template_kwargs", "enable_thinking"); !ok {
				t.Errorf("%s: expected chat_template_kwargs.enable_thinking in body, got %v", ptype, got["chat_template_kwargs"])
			} else if v {
				t.Errorf("%s: enable_thinking = true, want false", ptype)
			}
			if _, present := got["reasoning_effort"]; present {
				t.Errorf("%s: must not send Ollama-only reasoning_effort", ptype)
			}
		})
	}
}

// Strict OpenAI (and any non-vllm/sglang type) must not send the
// chat_template_kwargs extension, which those endpoints would reject.
func TestOpenAIComplete_ChatTemplateKwargsNotSentForOpenAI(t *testing.T) {
	var got map[string]any
	srv := chatCaptureServer(t, &got)
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		DefaultModel: "gpt-4o-mini",
		ProviderType: ProviderTypeOpenAI,
		Timeout:      5 * time.Second,
	})
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "ping"}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, present := got["chat_template_kwargs"]; present {
		t.Errorf("OpenAI provider must not send chat_template_kwargs; body had it")
	}
}

// A user-configured extra_body merges onto the top level of the request and its
// keys win, so a chat_template_kwargs override replaces the vllm/sglang default
// and arbitrary extra params (here a sampling field) reach the wire.
func TestOpenAIComplete_ExtraBodyOverlay(t *testing.T) {
	var got map[string]any
	srv := chatCaptureServer(t, &got)
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:      srv.URL,
		DefaultModel: "qwen3",
		ProviderType: ProviderTypeVLLM,
		Timeout:      5 * time.Second,
		ExtraBody: map[string]any{
			"chat_template_kwargs": map[string]any{"enable_thinking": true},
			"top_k":                20,
		},
	})
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "ping"}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// User override wins over the vllm enable_thinking=false default.
	if v, ok := nestedBool(got, "chat_template_kwargs", "enable_thinking"); !ok || !v {
		t.Errorf("extra_body chat_template_kwargs must override default; got %v", got["chat_template_kwargs"])
	}
	// Arbitrary extra key reaches the body.
	if got["top_k"] != float64(20) {
		t.Errorf("extra_body top_k = %v, want 20", got["top_k"])
	}
	// nram-set fields survive the merge.
	if got["model"] != "qwen3" {
		t.Errorf("merge dropped model field; got %v", got["model"])
	}
	if _, ok := got["stream"]; !ok {
		t.Errorf("merge dropped stream field")
	}
}

// extra_body also applies on the embeddings path (doRequest is shared).
func TestOpenAIEmbed_ExtraBodyOverlay(t *testing.T) {
	var got map[string]any
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/embeddings": func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&got)
			resp := openaiEmbeddingResponse{
				Data:  []openaiEmbeddingData{{Object: "embedding", Embedding: []float32{0.1}, Index: 0}},
				Model: "embed",
				Usage: openaiUsage{PromptTokens: 1, TotalTokens: 1},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:               srv.URL,
		DefaultEmbeddingModel: "embed",
		ProviderType:          ProviderTypeOpenAICompatible,
		Timeout:               5 * time.Second,
		ExtraBody:             map[string]any{"truncate_prompt_tokens": 512},
	})
	if _, err := p.Embed(context.Background(), &EmbeddingRequest{Input: []string{"hi"}}); err != nil {
		t.Fatalf("embed: %v", err)
	}
	if got["truncate_prompt_tokens"] != float64(512) {
		t.Errorf("extra_body not applied to embeddings; got %v", got["truncate_prompt_tokens"])
	}
	if got["model"] != "embed" {
		t.Errorf("merge dropped model field on embeddings; got %v", got["model"])
	}
}
