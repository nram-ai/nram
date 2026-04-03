package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newAnthropicTestServer creates an httptest.Server that routes based on exact
// path matching for the Anthropic Messages API.
func newAnthropicTestServer(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h, ok := handlers[r.URL.Path]; ok {
			h(w, r)
			return
		}
		http.NotFound(w, r)
	}))
}

func TestAnthropicComplete(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}

			// Verify Anthropic-specific headers.
			if got := r.Header.Get("x-api-key"); got != "test-key" {
				t.Errorf("x-api-key = %q, want %q", got, "test-key")
			}
			if got := r.Header.Get("anthropic-version"); got != "2023-06-01" {
				t.Errorf("anthropic-version = %q, want %q", got, "2023-06-01")
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want %q", got, "application/json")
			}

			var req anthropicMessagesRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if len(req.Messages) == 0 {
				http.Error(w, "no messages", http.StatusBadRequest)
				return
			}

			resp := anthropicMessagesResponse{
				ID:         "msg_test123",
				Type:       "message",
				Role:       "assistant",
				Model:      req.Model,
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "Hello! How can I help you?"},
				},
				Usage: anthropicUsage{
					InputTokens:  10,
					OutputTokens: 8,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
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
	if resp.Model != "claude-sonnet-4-20250514" {
		t.Errorf("Model = %q, want %q", resp.Model, "claude-sonnet-4-20250514")
	}
	if resp.FinishReason != "end_turn" {
		t.Errorf("FinishReason = %q, want %q", resp.FinishReason, "end_turn")
	}
	if resp.Usage.PromptTokens != 10 {
		t.Errorf("PromptTokens = %d, want %d", resp.Usage.PromptTokens, 10)
	}
	if resp.Usage.CompletionTokens != 8 {
		t.Errorf("CompletionTokens = %d, want %d", resp.Usage.CompletionTokens, 8)
	}
	if resp.Usage.TotalTokens != 18 {
		t.Errorf("TotalTokens = %d, want %d", resp.Usage.TotalTokens, 18)
	}
}

func TestAnthropicCompleteSystemMessageExtraction(t *testing.T) {
	var receivedReq anthropicMessagesRequest

	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := anthropicMessagesResponse{
				ID:         "msg_test456",
				Type:       "message",
				Role:       "assistant",
				Model:      receivedReq.Model,
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "I am a helpful assistant."},
				},
				Usage: anthropicUsage{
					InputTokens:  15,
					OutputTokens: 6,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{
			{Role: "system", Content: "You are helpful."},
			{Role: "user", Content: "Hello"},
		},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	// System message should be extracted as top-level system field.
	if receivedReq.System != "You are helpful." {
		t.Errorf("System = %q, want %q", receivedReq.System, "You are helpful.")
	}

	// Messages should only have the user message.
	if len(receivedReq.Messages) != 1 {
		t.Fatalf("Messages length = %d, want 1", len(receivedReq.Messages))
	}
	if receivedReq.Messages[0].Role != "user" {
		t.Errorf("Messages[0].Role = %q, want %q", receivedReq.Messages[0].Role, "user")
	}
	if receivedReq.Messages[0].Content != "Hello" {
		t.Errorf("Messages[0].Content = %q, want %q", receivedReq.Messages[0].Content, "Hello")
	}
}

func TestAnthropicCompleteDefaultModel(t *testing.T) {
	var receivedReq anthropicMessagesRequest

	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := anthropicMessagesResponse{
				ID:         "msg_test789",
				Type:       "message",
				Role:       "assistant",
				Model:      receivedReq.Model,
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "ok"},
				},
				Usage: anthropicUsage{InputTokens: 1, OutputTokens: 1},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL: srv.URL,
		APIKey:  "test-key",
		// DefaultModel left empty; constructor sets default.
	})

	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if resp.Model != "claude-sonnet-4-20250514" {
		t.Errorf("Model = %q, want %q", resp.Model, "claude-sonnet-4-20250514")
	}
	if receivedReq.Model != "claude-sonnet-4-20250514" {
		t.Errorf("request Model = %q, want %q", receivedReq.Model, "claude-sonnet-4-20250514")
	}
}

func TestAnthropicCompleteCustomModel(t *testing.T) {
	var receivedReq anthropicMessagesRequest

	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := anthropicMessagesResponse{
				ID:         "msg_custom",
				Type:       "message",
				Role:       "assistant",
				Model:      receivedReq.Model,
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "ok"},
				},
				Usage: anthropicUsage{InputTokens: 1, OutputTokens: 1},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
	})

	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
		Model:    "claude-opus-4-20250514",
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if resp.Model != "claude-opus-4-20250514" {
		t.Errorf("Model = %q, want %q", resp.Model, "claude-opus-4-20250514")
	}
	if receivedReq.Model != "claude-opus-4-20250514" {
		t.Errorf("request Model = %q, want %q", receivedReq.Model, "claude-opus-4-20250514")
	}
}

func TestAnthropicCompleteTemperatureAndStopSequences(t *testing.T) {
	var receivedReq anthropicMessagesRequest

	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := anthropicMessagesResponse{
				ID:         "msg_params",
				Type:       "message",
				Role:       "assistant",
				Model:      receivedReq.Model,
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "response"},
				},
				Usage: anthropicUsage{InputTokens: 5, OutputTokens: 2},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages:    []Message{{Role: "user", Content: "test"}},
		MaxTokens:   256,
		Temperature: 0.5,
		Stop:        []string{"END", "STOP"},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	if receivedReq.MaxTokens != 256 {
		t.Errorf("MaxTokens = %d, want 256", receivedReq.MaxTokens)
	}
	if receivedReq.Temperature == nil || *receivedReq.Temperature != 0.5 {
		t.Errorf("Temperature = %v, want 0.5", receivedReq.Temperature)
	}
	if len(receivedReq.StopSequences) != 2 || receivedReq.StopSequences[0] != "END" || receivedReq.StopSequences[1] != "STOP" {
		t.Errorf("StopSequences = %v, want [END STOP]", receivedReq.StopSequences)
	}
}

func TestAnthropicCompleteDefaultMaxTokens(t *testing.T) {
	var receivedReq anthropicMessagesRequest

	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := anthropicMessagesResponse{
				ID:         "msg_default_max",
				Type:       "message",
				Role:       "assistant",
				Model:      receivedReq.Model,
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "ok"},
				},
				Usage: anthropicUsage{InputTokens: 1, OutputTokens: 1},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
		// MaxTokens not specified — should default to 4096.
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	if receivedReq.MaxTokens != 4096 {
		t.Errorf("MaxTokens = %d, want 4096", receivedReq.MaxTokens)
	}
}

func TestAnthropicCompleteAPIError(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(anthropicErrorResponse{
				Type: "error",
				Error: anthropicErrorDetail{
					Type:    "rate_limit_error",
					Message: "Rate limit exceeded",
				},
			})
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
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

func TestAnthropicCompleteEmptyContentBlocks(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			resp := anthropicMessagesResponse{
				ID:         "msg_empty",
				Type:       "message",
				Role:       "assistant",
				Model:      "claude-sonnet-4-20250514",
				StopReason: "end_turn",
				Content:    []anthropicContentBlock{},
				Usage:      anthropicUsage{InputTokens: 5, OutputTokens: 0},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
	})

	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if resp.Content != "" {
		t.Errorf("Content = %q, want empty string", resp.Content)
	}
}

func TestAnthropicCompleteMultipleContentBlocks(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			resp := anthropicMessagesResponse{
				ID:         "msg_multi",
				Type:       "message",
				Role:       "assistant",
				Model:      "claude-sonnet-4-20250514",
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "Part one. "},
					{Type: "text", Text: "Part two."},
				},
				Usage: anthropicUsage{InputTokens: 5, OutputTokens: 6},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
	})

	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	if resp.Content != "Part one. Part two." {
		t.Errorf("Content = %q, want %q", resp.Content, "Part one. Part two.")
	}
}

func TestAnthropicPingSuccess(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			resp := anthropicMessagesResponse{
				ID:         "msg_ping",
				Type:       "message",
				Role:       "assistant",
				Model:      "claude-sonnet-4-20250514",
				StopReason: "end_turn",
				Content: []anthropicContentBlock{
					{Type: "text", Text: "hi"},
				},
				Usage: anthropicUsage{InputTokens: 1, OutputTokens: 1},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL: srv.URL,
		APIKey:  "test-key",
	})

	if err := p.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error: %v", err)
	}
}

func TestAnthropicPingFailure(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(anthropicErrorResponse{
				Type: "error",
				Error: anthropicErrorDetail{
					Type:    "authentication_error",
					Message: "Invalid API key",
				},
			})
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL: srv.URL,
		APIKey:  "bad-key",
	})

	if err := p.Ping(context.Background()); err == nil {
		t.Fatal("Ping() expected error, got nil")
	}
}

func TestAnthropicCompleteNoModel(t *testing.T) {
	p := &AnthropicProvider{
		config: AnthropicConfig{},
		client: &http.Client{Timeout: 30 * time.Second},
	}

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err == nil {
		t.Fatal("Complete() expected error for missing model, got nil")
	}
	if got := err.Error(); !contains(got, "no model specified") {
		t.Errorf("error = %q, want it to contain %q", got, "no model specified")
	}
}

func TestAnthropicContextCancellation(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			// Simulate slow response — the context should cancel before this completes.
			<-r.Context().Done()
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "claude-sonnet-4-20250514",
		Timeout:      5 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, err := p.Complete(ctx, &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err == nil {
		t.Fatal("Complete() expected error for cancelled context, got nil")
	}
}

func TestAnthropicName(t *testing.T) {
	p := NewAnthropicProvider(AnthropicConfig{})
	if got := p.Name(); got != "anthropic" {
		t.Errorf("Name() = %q, want %q", got, "anthropic")
	}
}

func TestAnthropicModels(t *testing.T) {
	p := NewAnthropicProvider(AnthropicConfig{
		DefaultModel: "claude-sonnet-4-20250514",
	})
	models := p.Models()
	if len(models) != 1 {
		t.Fatalf("Models() length = %d, want 1", len(models))
	}
	if models[0] != "claude-sonnet-4-20250514" {
		t.Errorf("Models()[0] = %q, want %q", models[0], "claude-sonnet-4-20250514")
	}
}

func TestAnthropicDefaultTimeout(t *testing.T) {
	p := NewAnthropicProvider(AnthropicConfig{})
	if p.client.Timeout != 120*time.Second {
		t.Errorf("default timeout = %v, want %v", p.client.Timeout, 120*time.Second)
	}
}

func TestAnthropicCustomTimeout(t *testing.T) {
	p := NewAnthropicProvider(AnthropicConfig{Timeout: 60 * time.Second})
	if p.client.Timeout != 60*time.Second {
		t.Errorf("timeout = %v, want %v", p.client.Timeout, 60*time.Second)
	}
}

func TestAnthropicDefaultBaseURL(t *testing.T) {
	p := NewAnthropicProvider(AnthropicConfig{})
	if p.config.BaseURL != "https://api.anthropic.com" {
		t.Errorf("BaseURL = %q, want %q", p.config.BaseURL, "https://api.anthropic.com")
	}
}

func TestAnthropicDefaultModel(t *testing.T) {
	p := NewAnthropicProvider(AnthropicConfig{})
	if p.config.DefaultModel != "claude-sonnet-4-20250514" {
		t.Errorf("DefaultModel = %q, want %q", p.config.DefaultModel, "claude-sonnet-4-20250514")
	}
}
