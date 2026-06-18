package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestErrIfStreamedResponse(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		body        string
		wantErr     bool
	}{
		{"plain json", "application/json", `{"id":"x"}`, false},
		{"json with charset", "application/json; charset=utf-8", `{"id":"x"}`, false},
		{"sse content-type", "text/event-stream; charset=utf-8", "event: message_start\ndata: {}\n\n", true},
		{"sse content-type uppercase", "Text/Event-Stream", "data: {}\n\n", true},
		{"mislabeled but event-framed body", "application/json", "event: message_start\ndata: {}\n\n", true},
		{"mislabeled but data-framed body", "", "  \n data: {\"x\":1}\n\n", true},
		{"empty body json type", "application/json", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := errIfStreamedResponse("anthropic", tc.contentType, []byte(tc.body))
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected nil, got %v", err)
			}
			if tc.wantErr && err != nil && !strings.Contains(err.Error(), "streaming") {
				t.Errorf("error message should mention streaming, got %q", err.Error())
			}
		})
	}
}

// TestAnthropicSendsStreamFalse verifies the request body carries an explicit
// "stream":false key (present, not omitted) on the wire.
func TestAnthropicSendsStreamFalse(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			var raw map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			v, ok := raw["stream"]
			if !ok {
				t.Errorf("request body missing \"stream\" key")
			} else if string(v) != "false" {
				t.Errorf("stream = %s, want false", v)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(anthropicMessagesResponse{
				Model:      "m",
				StopReason: "end_turn",
				Content:    []anthropicContentBlock{{Type: "text", Text: "ok"}},
				Usage:      anthropicUsage{InputTokens: 1, OutputTokens: 1},
			})
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{BaseURL: srv.URL, APIKey: "k", DefaultModel: "m"})
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}}, MaxTokens: 10,
	}); err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
}

// TestAnthropicStreamedResponseFailsLoudly verifies an SSE body yields the
// actionable streaming error rather than a generic unmarshal failure.
func TestAnthropicStreamedResponseFailsLoudly(t *testing.T) {
	srv := newAnthropicTestServer(t, map[string]http.HandlerFunc{
		"/v1/messages": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\"}\n\n"))
		},
	})
	defer srv.Close()

	p := NewAnthropicProvider(AnthropicConfig{BaseURL: srv.URL, APIKey: "k", DefaultModel: "m"})
	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}}, MaxTokens: 10,
	})
	if err == nil {
		t.Fatalf("expected error on streamed response, got nil")
	}
	if !strings.Contains(err.Error(), "streaming") {
		t.Errorf("error should mention streaming, got %q", err.Error())
	}
}

// TestOpenAISendsStreamFalse verifies the OpenAI-compatible request body carries
// an explicit "stream":false key on the wire.
func TestOpenAISendsStreamFalse(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			var raw map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			v, ok := raw["stream"]
			if !ok {
				t.Errorf("request body missing \"stream\" key")
			} else if string(v) != "false" {
				t.Errorf("stream = %s, want false", v)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(openaiChatResponse{
				Model: "m",
				Choices: []openaiChatChoice{
					{Message: openaiChatMessage{Role: "assistant", Content: "ok"}, FinishReason: "stop"},
				},
			})
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, APIKey: "k", DefaultModel: "m"})
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}}, MaxTokens: 10,
	}); err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
}

// TestOpenAIStreamedResponseFailsLoudly verifies an SSE body yields the
// actionable streaming error.
func TestOpenAIStreamedResponseFailsLoudly(t *testing.T) {
	srv := newTestServer(t, map[string]http.HandlerFunc{
		"POST /v1/chat/completions": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"id\":\"x\"}\n\ndata: [DONE]\n\n"))
		},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, APIKey: "k", DefaultModel: "m"})
	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}}, MaxTokens: 10,
	})
	if err == nil {
		t.Fatalf("expected error on streamed response, got nil")
	}
	if !strings.Contains(err.Error(), "streaming") {
		t.Errorf("error should mention streaming, got %q", err.Error())
	}
}
