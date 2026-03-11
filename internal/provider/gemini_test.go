package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newGeminiTestServer creates an httptest.Server that routes based on URL path
// suffix matching (since Gemini paths include model names). The handler
// receives the full request for query-parameter inspection.
func newGeminiTestServer(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for suffix, h := range handlers {
			if strings.HasSuffix(r.URL.Path, suffix) {
				h(w, r)
				return
			}
		}
		// Check exact path match (e.g. /v1beta/models).
		if h, ok := handlers[r.URL.Path]; ok {
			h(w, r)
			return
		}
		http.NotFound(w, r)
	}))
}

func TestGeminiComplete(t *testing.T) {
	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":generateContent": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}

			var req geminiGenerateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			// Verify request structure.
			if len(req.Contents) == 0 {
				http.Error(w, "no contents", http.StatusBadRequest)
				return
			}

			resp := geminiGenerateResponse{
				Candidates: []geminiCandidate{
					{
						Content: geminiContent{
							Role:  "model",
							Parts: []geminiPart{{Text: "Hello! How can I help you?"}},
						},
						FinishReason: "STOP",
					},
				},
				UsageMetadata: geminiUsageMetadata{
					PromptTokenCount:     10,
					CandidatesTokenCount: 8,
					TotalTokenCount:      18,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "gemini-2.0-flash",
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
	if resp.Model != "gemini-2.0-flash" {
		t.Errorf("Model = %q, want %q", resp.Model, "gemini-2.0-flash")
	}
	if resp.FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want %q", resp.FinishReason, "stop")
	}
	if resp.Usage.TotalTokens != 18 {
		t.Errorf("TotalTokens = %d, want %d", resp.Usage.TotalTokens, 18)
	}
	if resp.Usage.PromptTokens != 10 {
		t.Errorf("PromptTokens = %d, want %d", resp.Usage.PromptTokens, 10)
	}
	if resp.Usage.CompletionTokens != 8 {
		t.Errorf("CompletionTokens = %d, want %d", resp.Usage.CompletionTokens, 8)
	}
}

func TestGeminiCompleteWithSystemMessage(t *testing.T) {
	var receivedReq geminiGenerateRequest

	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":generateContent": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := geminiGenerateResponse{
				Candidates: []geminiCandidate{
					{
						Content: geminiContent{
							Role:  "model",
							Parts: []geminiPart{{Text: "I am a helpful assistant."}},
						},
						FinishReason: "STOP",
					},
				},
				UsageMetadata: geminiUsageMetadata{
					PromptTokenCount:     15,
					CandidatesTokenCount: 6,
					TotalTokenCount:      21,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "gemini-2.0-flash",
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

	// System message should be extracted as systemInstruction.
	if receivedReq.SystemInstruction == nil {
		t.Fatal("SystemInstruction should not be nil")
	}
	if receivedReq.SystemInstruction.Parts[0].Text != "You are helpful." {
		t.Errorf("SystemInstruction text = %q, want %q",
			receivedReq.SystemInstruction.Parts[0].Text, "You are helpful.")
	}

	// Contents should only have the user message.
	if len(receivedReq.Contents) != 1 {
		t.Fatalf("Contents length = %d, want 1", len(receivedReq.Contents))
	}
	if receivedReq.Contents[0].Role != "user" {
		t.Errorf("Contents[0].Role = %q, want %q", receivedReq.Contents[0].Role, "user")
	}
}

func TestGeminiCompleteAssistantRoleMapping(t *testing.T) {
	var receivedReq geminiGenerateRequest

	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":generateContent": func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := geminiGenerateResponse{
				Candidates: []geminiCandidate{
					{
						Content: geminiContent{
							Role:  "model",
							Parts: []geminiPart{{Text: "Sure!"}},
						},
						FinishReason: "STOP",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "gemini-2.0-flash",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{
			{Role: "user", Content: "Hello"},
			{Role: "assistant", Content: "Hi there!"},
			{Role: "user", Content: "Help me"},
		},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	// "assistant" should be mapped to "model" for Gemini.
	if len(receivedReq.Contents) != 3 {
		t.Fatalf("Contents length = %d, want 3", len(receivedReq.Contents))
	}
	if receivedReq.Contents[1].Role != "model" {
		t.Errorf("Contents[1].Role = %q, want %q", receivedReq.Contents[1].Role, "model")
	}
}

func TestGeminiCompleteAPIError(t *testing.T) {
	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":generateContent": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(geminiErrorResponse{
				Error: geminiErrorDetail{
					Code:    429,
					Message: "Resource exhausted",
					Status:  "RESOURCE_EXHAUSTED",
				},
			})
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "gemini-2.0-flash",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err == nil {
		t.Fatal("Complete() expected error, got nil")
	}
	if got := err.Error(); !contains(got, "Resource exhausted") {
		t.Errorf("error = %q, want it to contain %q", got, "Resource exhausted")
	}
}

func TestGeminiCompleteNoModel(t *testing.T) {
	p := &GeminiProvider{
		config: GeminiConfig{},
		client: &http.Client{Timeout: 30 * time.Second},
	}

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "Hello"}},
	})
	if err == nil {
		t.Fatal("Complete() expected error for missing model, got nil")
	}
}

func TestGeminiEmbedSingle(t *testing.T) {
	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":embedContent": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}

			var req geminiEmbedRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			resp := geminiEmbedResponse{}
			resp.Embedding.Values = []float32{0.1, 0.2, 0.3}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:               srv.URL,
		APIKey:                "test-key",
		DefaultEmbeddingModel: "text-embedding-004",
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
	if resp.Model != "text-embedding-004" {
		t.Errorf("Model = %q, want %q", resp.Model, "text-embedding-004")
	}
}

func TestGeminiEmbedBatch(t *testing.T) {
	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":batchEmbedContents": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}

			var req geminiBatchEmbedRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			resp := geminiBatchEmbedResponse{
				Embeddings: make([]struct {
					Values []float32 `json:"values"`
				}, len(req.Requests)),
			}
			for i := range req.Requests {
				resp.Embeddings[i].Values = []float32{
					float32(i)*0.1 + 0.01,
					float32(i)*0.1 + 0.02,
					float32(i)*0.1 + 0.03,
					float32(i)*0.1 + 0.04,
				}
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:               srv.URL,
		APIKey:                "test-key",
		DefaultEmbeddingModel: "text-embedding-004",
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

func TestGeminiPingSuccess(t *testing.T) {
	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		"/v1beta/models": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(geminiModelsResponse{
				Models: []geminiModel{{Name: "models/gemini-2.0-flash"}},
			})
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL: srv.URL,
		APIKey:  "test-key",
	})

	if err := p.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error: %v", err)
	}
}

func TestGeminiPingFailure(t *testing.T) {
	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		"/v1beta/models": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(geminiErrorResponse{
				Error: geminiErrorDetail{
					Code:    403,
					Message: "API key not valid",
					Status:  "PERMISSION_DENIED",
				},
			})
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL: srv.URL,
		APIKey:  "bad-key",
	})

	if err := p.Ping(context.Background()); err == nil {
		t.Fatal("Ping() expected error, got nil")
	}
}

func TestGeminiAPIKeyAsQueryParam(t *testing.T) {
	var receivedKey string

	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":generateContent": func(w http.ResponseWriter, r *http.Request) {
			receivedKey = r.URL.Query().Get("key")

			// Verify no Authorization header is set.
			if auth := r.Header.Get("Authorization"); auth != "" {
				t.Errorf("Authorization header should be empty, got %q", auth)
			}

			resp := geminiGenerateResponse{
				Candidates: []geminiCandidate{
					{
						Content: geminiContent{
							Role:  "model",
							Parts: []geminiPart{{Text: "ok"}},
						},
						FinishReason: "STOP",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:      srv.URL,
		APIKey:       "my-secret-api-key",
		DefaultModel: "gemini-2.0-flash",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	if receivedKey != "my-secret-api-key" {
		t.Errorf("API key = %q, want %q", receivedKey, "my-secret-api-key")
	}
}

func TestGeminiRequestFormat(t *testing.T) {
	var receivedReq geminiGenerateRequest
	var receivedPath string

	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":generateContent": func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := geminiGenerateResponse{
				Candidates: []geminiCandidate{
					{
						Content: geminiContent{
							Role:  "model",
							Parts: []geminiPart{{Text: "response"}},
						},
						FinishReason: "STOP",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:      srv.URL,
		APIKey:       "test-key",
		DefaultModel: "gemini-2.0-flash",
	})

	_, err := p.Complete(context.Background(), &CompletionRequest{
		Messages:    []Message{{Role: "user", Content: "Hello"}},
		MaxTokens:   256,
		Temperature: 0.5,
		Stop:        []string{"END"},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	// Verify path includes model name.
	if !strings.Contains(receivedPath, "gemini-2.0-flash") {
		t.Errorf("path = %q, want it to contain model name", receivedPath)
	}
	if !strings.HasSuffix(receivedPath, ":generateContent") {
		t.Errorf("path = %q, want it to end with :generateContent", receivedPath)
	}

	// Verify contents structure.
	if len(receivedReq.Contents) != 1 {
		t.Fatalf("Contents length = %d, want 1", len(receivedReq.Contents))
	}
	if receivedReq.Contents[0].Parts[0].Text != "Hello" {
		t.Errorf("Content text = %q, want %q", receivedReq.Contents[0].Parts[0].Text, "Hello")
	}

	// Verify generation config.
	if receivedReq.GenerationConfig == nil {
		t.Fatal("GenerationConfig should not be nil")
	}
	if receivedReq.GenerationConfig.MaxOutputTokens != 256 {
		t.Errorf("MaxOutputTokens = %d, want 256", receivedReq.GenerationConfig.MaxOutputTokens)
	}
	if receivedReq.GenerationConfig.Temperature == nil || *receivedReq.GenerationConfig.Temperature != 0.5 {
		t.Errorf("Temperature = %v, want 0.5", receivedReq.GenerationConfig.Temperature)
	}
	if len(receivedReq.GenerationConfig.StopSequences) != 1 || receivedReq.GenerationConfig.StopSequences[0] != "END" {
		t.Errorf("StopSequences = %v, want [END]", receivedReq.GenerationConfig.StopSequences)
	}
}

func TestGeminiEmbedRequestFormat(t *testing.T) {
	var receivedReq geminiEmbedRequest
	var receivedPath string

	srv := newGeminiTestServer(t, map[string]http.HandlerFunc{
		":embedContent": func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			json.NewDecoder(r.Body).Decode(&receivedReq)

			resp := geminiEmbedResponse{}
			resp.Embedding.Values = []float32{0.1, 0.2, 0.3}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	p := NewGeminiProvider(GeminiConfig{
		BaseURL:               srv.URL,
		APIKey:                "test-key",
		DefaultEmbeddingModel: "text-embedding-004",
	})

	_, err := p.Embed(context.Background(), &EmbeddingRequest{
		Input: []string{"Hello"},
	})
	if err != nil {
		t.Fatalf("Embed() error: %v", err)
	}

	// Verify path includes model name and endpoint.
	if !strings.Contains(receivedPath, "text-embedding-004") {
		t.Errorf("path = %q, want it to contain model name", receivedPath)
	}
	if !strings.HasSuffix(receivedPath, ":embedContent") {
		t.Errorf("path = %q, want it to end with :embedContent", receivedPath)
	}

	// Verify model field in request body.
	if receivedReq.Model != "models/text-embedding-004" {
		t.Errorf("Model = %q, want %q", receivedReq.Model, "models/text-embedding-004")
	}

	// Verify content structure.
	if len(receivedReq.Content.Parts) != 1 {
		t.Fatalf("Content.Parts length = %d, want 1", len(receivedReq.Content.Parts))
	}
	if receivedReq.Content.Parts[0].Text != "Hello" {
		t.Errorf("Content.Parts[0].Text = %q, want %q", receivedReq.Content.Parts[0].Text, "Hello")
	}
}

func TestGeminiName(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{})
	if got := p.Name(); got != "gemini" {
		t.Errorf("Name() = %q, want %q", got, "gemini")
	}
}

func TestGeminiModels(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{
		DefaultModel:          "gemini-2.0-flash",
		DefaultEmbeddingModel: "text-embedding-004",
	})
	models := p.Models()
	if len(models) != 2 {
		t.Fatalf("Models() length = %d, want 2", len(models))
	}
	if models[0] != "gemini-2.0-flash" {
		t.Errorf("Models()[0] = %q, want %q", models[0], "gemini-2.0-flash")
	}
	if models[1] != "text-embedding-004" {
		t.Errorf("Models()[1] = %q, want %q", models[1], "text-embedding-004")
	}
}

func TestGeminiDimensions(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{})
	dims := p.Dimensions()
	if len(dims) == 0 {
		t.Fatal("Dimensions() returned empty slice")
	}
	found768 := false
	for _, d := range dims {
		if d == 768 {
			found768 = true
		}
	}
	if !found768 {
		t.Error("Dimensions() does not include 768")
	}
}

func TestGeminiDefaultTimeout(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{})
	if p.client.Timeout != 30*time.Second {
		t.Errorf("default timeout = %v, want %v", p.client.Timeout, 30*time.Second)
	}
}

func TestGeminiCustomTimeout(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{Timeout: 60 * time.Second})
	if p.client.Timeout != 60*time.Second {
		t.Errorf("timeout = %v, want %v", p.client.Timeout, 60*time.Second)
	}
}

func TestGeminiDefaultBaseURL(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{})
	if p.config.BaseURL != "https://generativelanguage.googleapis.com" {
		t.Errorf("BaseURL = %q, want %q", p.config.BaseURL, "https://generativelanguage.googleapis.com")
	}
}

func TestGeminiDefaultModels(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{})
	if p.config.DefaultModel != "gemini-2.0-flash" {
		t.Errorf("DefaultModel = %q, want %q", p.config.DefaultModel, "gemini-2.0-flash")
	}
	if p.config.DefaultEmbeddingModel != "text-embedding-004" {
		t.Errorf("DefaultEmbeddingModel = %q, want %q", p.config.DefaultEmbeddingModel, "text-embedding-004")
	}
}
