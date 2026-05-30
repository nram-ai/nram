package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// newOllamaTestServer creates an httptest.Server that routes based on exact
// path matching for the Ollama API.
func newOllamaTestServer(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h, ok := handlers[r.URL.Path]; ok {
			h(w, r)
			return
		}
		http.NotFound(w, r)
	}))
}

func TestOllamaListModelsSuccess(t *testing.T) {
	now := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/tags": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			resp := ollamaTagsResponse{
				Models: []OllamaModel{
					{Name: "llama3:latest", Size: 4700000000, Digest: "abc123", ModifiedAt: now},
					{Name: "mistral:7b", Size: 3800000000, Digest: "def456", ModifiedAt: now.Add(-24 * time.Hour)},
					{Name: "nomic-embed-text:latest", Size: 274000000, Digest: "ghi789", ModifiedAt: now.Add(-48 * time.Hour)},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	models, err := c.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels() error: %v", err)
	}
	if len(models) != 3 {
		t.Fatalf("ListModels() returned %d models, want 3", len(models))
	}
	if models[0].Name != "llama3:latest" {
		t.Errorf("models[0].Name = %q, want %q", models[0].Name, "llama3:latest")
	}
	if models[0].Size != 4700000000 {
		t.Errorf("models[0].Size = %d, want %d", models[0].Size, int64(4700000000))
	}
	if models[0].Digest != "abc123" {
		t.Errorf("models[0].Digest = %q, want %q", models[0].Digest, "abc123")
	}
	if models[1].Name != "mistral:7b" {
		t.Errorf("models[1].Name = %q, want %q", models[1].Name, "mistral:7b")
	}
	if models[2].Name != "nomic-embed-text:latest" {
		t.Errorf("models[2].Name = %q, want %q", models[2].Name, "nomic-embed-text:latest")
	}
}

func TestOllamaListModelsEmpty(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/tags": func(w http.ResponseWriter, r *http.Request) {
			resp := ollamaTagsResponse{Models: nil}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	models, err := c.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels() error: %v", err)
	}
	if len(models) != 0 {
		t.Errorf("ListModels() returned %d models, want 0", len(models))
	}
}

func TestOllamaListModelsServerError(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/tags": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal server error", http.StatusInternalServerError)
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	_, err := c.ListModels(context.Background())
	if err == nil {
		t.Fatal("ListModels() expected error, got nil")
	}
	if got := err.Error(); !contains(got, "status 500") {
		t.Errorf("error = %q, want it to contain %q", got, "status 500")
	}
}

func TestOllamaPullModelSuccess(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/pull": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}

			var req ollamaPullRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.Name != "llama3:latest" {
				http.Error(w, "unexpected model name", http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/x-ndjson")
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "flushing not supported", http.StatusInternalServerError)
				return
			}

			updates := []PullProgress{
				{Status: "pulling manifest"},
				{Status: "downloading digestabc123", Completed: 0, Total: 5000},
				{Status: "downloading digestabc123", Completed: 2500, Total: 5000},
				{Status: "downloading digestabc123", Completed: 5000, Total: 5000},
				{Status: "verifying sha256 digest"},
				{Status: "success"},
			}

			for _, u := range updates {
				line, _ := json.Marshal(u)
				_, _ = fmt.Fprintf(w, "%s\n", line)
				flusher.Flush()
			}
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	err := c.PullModel(context.Background(), "llama3:latest", nil)
	if err != nil {
		t.Fatalf("PullModel() error: %v", err)
	}
}

func TestOllamaPullModelProgressCallback(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/pull": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-ndjson")
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "flushing not supported", http.StatusInternalServerError)
				return
			}

			updates := []PullProgress{
				{Status: "pulling manifest"},
				{Status: "downloading digestabc123", Completed: 1000, Total: 5000},
				{Status: "downloading digestabc123", Completed: 5000, Total: 5000},
				{Status: "success"},
			}

			for _, u := range updates {
				line, _ := json.Marshal(u)
				_, _ = fmt.Fprintf(w, "%s\n", line)
				flusher.Flush()
			}
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})

	var mu sync.Mutex
	var received []PullProgress

	err := c.PullModel(context.Background(), "llama3:latest", func(p PullProgress) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, p)
	})
	if err != nil {
		t.Fatalf("PullModel() error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 4 {
		t.Fatalf("received %d progress updates, want 4", len(received))
	}
	if received[0].Status != "pulling manifest" {
		t.Errorf("received[0].Status = %q, want %q", received[0].Status, "pulling manifest")
	}
	if received[1].Completed != 1000 {
		t.Errorf("received[1].Completed = %d, want %d", received[1].Completed, 1000)
	}
	if received[1].Total != 5000 {
		t.Errorf("received[1].Total = %d, want %d", received[1].Total, 5000)
	}
	if received[3].Status != "success" {
		t.Errorf("received[3].Status = %q, want %q", received[3].Status, "success")
	}
}

func TestOllamaPullModelServerError(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/pull": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	err := c.PullModel(context.Background(), "llama3:latest", nil)
	if err == nil {
		t.Fatal("PullModel() expected error, got nil")
	}
	if got := err.Error(); !contains(got, "status 503") {
		t.Errorf("error = %q, want it to contain %q", got, "status 503")
	}
}

func TestOllamaPullModelErrorInStream(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/pull": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-ndjson")
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "flushing not supported", http.StatusInternalServerError)
				return
			}

			updates := []PullProgress{
				{Status: "pulling manifest"},
				{Status: "error: model not found"},
			}

			for _, u := range updates {
				line, _ := json.Marshal(u)
				_, _ = fmt.Fprintf(w, "%s\n", line)
				flusher.Flush()
			}
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	err := c.PullModel(context.Background(), "nonexistent:latest", nil)
	if err == nil {
		t.Fatal("PullModel() expected error for error status in stream, got nil")
	}
	if got := err.Error(); !contains(got, "model not found") {
		t.Errorf("error = %q, want it to contain %q", got, "model not found")
	}
}

func TestOllamaProbeURLSuccess(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, "Ollama is running")
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	if err := c.ProbeURL(context.Background()); err != nil {
		t.Fatalf("ProbeURL() error: %v", err)
	}
}

func TestOllamaProbeURLFailure(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	err := c.ProbeURL(context.Background())
	if err == nil {
		t.Fatal("ProbeURL() expected error, got nil")
	}
	if got := err.Error(); !contains(got, "status 502") {
		t.Errorf("error = %q, want it to contain %q", got, "status 502")
	}
}

func TestOllamaProbeURLConnectionFailure(t *testing.T) {
	c := NewOllamaClient(OllamaConfig{
		BaseURL: "http://127.0.0.1:1", // Nothing listening on port 1.
		Timeout: 1 * time.Second,
	})

	err := c.ProbeURL(context.Background())
	if err == nil {
		t.Fatal("ProbeURL() expected error for connection failure, got nil")
	}
	if got := err.Error(); !contains(got, "probe failed") {
		t.Errorf("error = %q, want it to contain %q", got, "probe failed")
	}
}

func TestOllamaDefaultConfigValues(t *testing.T) {
	c := NewOllamaClient(OllamaConfig{})

	if c.config.BaseURL != "http://localhost:11434" {
		t.Errorf("BaseURL = %q, want %q", c.config.BaseURL, "http://localhost:11434")
	}
	if c.client.Timeout != 300*time.Second {
		t.Errorf("Timeout = %v, want %v", c.client.Timeout, 300*time.Second)
	}
	if c.config.PullTimeout != 10*time.Minute {
		t.Errorf("PullTimeout = %v, want %v", c.config.PullTimeout, 10*time.Minute)
	}
}

func TestOllamaCustomConfigValues(t *testing.T) {
	c := NewOllamaClient(OllamaConfig{
		BaseURL:     "http://my-ollama:9999",
		Timeout:     60 * time.Second,
		PullTimeout: 30 * time.Minute,
	})

	if c.config.BaseURL != "http://my-ollama:9999" {
		t.Errorf("BaseURL = %q, want %q", c.config.BaseURL, "http://my-ollama:9999")
	}
	if c.client.Timeout != 60*time.Second {
		t.Errorf("Timeout = %v, want %v", c.client.Timeout, 60*time.Second)
	}
	if c.config.PullTimeout != 30*time.Minute {
		t.Errorf("PullTimeout = %v, want %v", c.config.PullTimeout, 30*time.Minute)
	}
}

func TestOllamaContextLength_ParsesQwen2Family(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}
			var body ollamaShowRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Name != "qwen3:8b" {
				t.Errorf("expected name=qwen3:8b, got %q", body.Name)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"model_info":{"qwen2.context_length":40960,"qwen2.attention.head_count":32}}`))
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	effective, modelMax, err := c.ContextLength(context.Background(), "qwen3:8b")
	if err != nil {
		t.Fatalf("ContextLength: %v", err)
	}
	if modelMax != 40960 {
		t.Errorf("modelMax = %d, want 40960", modelMax)
	}
	if effective != 40960 {
		t.Errorf("effective = %d, want 40960 (no num_ctx configured -> equal to max)", effective)
	}
}

func TestOllamaContextLength_ParsesAnyArchKey(t *testing.T) {
	// Different model families use different architecture prefixes — the
	// detector must scan for any *.context_length key, not hard-code the
	// list.
	cases := map[string]int{
		`{"model_info":{"llama.context_length":8192}}`:          8192,
		`{"model_info":{"gemma.context_length":2048}}`:          2048,
		`{"model_info":{"bert.context_length":512}}`:            512,
		`{"model_info":{"unknown_arch.context_length":131072}}`: 131072,
	}
	for body, want := range cases {
		t.Run(body, func(t *testing.T) {
			srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
				"/api/show": func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(body))
				},
			})
			defer srv.Close()
			c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
			effective, modelMax, err := c.ContextLength(context.Background(), "any:tag")
			if err != nil {
				t.Fatalf("ContextLength: %v", err)
			}
			if modelMax != want {
				t.Errorf("modelMax = %d, want %d", modelMax, want)
			}
			if effective != want {
				t.Errorf("effective = %d, want %d", effective, want)
			}
		})
	}
}

func TestOllamaContextLength_MissingFieldReturnsZero(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			// Real Ollama responses sometimes lack model_info entirely; the
			// detector must treat this as "unknown" without surfacing an
			// error.
			_, _ = w.Write([]byte(`{"details":{"family":"llama"}}`))
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	effective, modelMax, err := c.ContextLength(context.Background(), "any:tag")
	if err != nil {
		t.Fatalf("ContextLength: %v", err)
	}
	if modelMax != 0 {
		t.Errorf("modelMax = %d, want 0", modelMax)
	}
	if effective != 0 {
		t.Errorf("effective = %d, want 0", effective)
	}
}

func TestOllamaContextLength_ServerError(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "model not found", http.StatusNotFound)
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	_, _, err := c.ContextLength(context.Background(), "missing:tag")
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if !contains(err.Error(), "status 404") {
		t.Errorf("error = %q, want substring %q", err.Error(), "status 404")
	}
}

// TestOllamaContextLength_PrefersPSOverShow verifies the priority chain:
// when /api/ps reports a context for the loaded model, that wins over the
// /api/show parameters num_ctx and over the model_info ceiling.
func TestOllamaContextLength_PrefersPSOverShow(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"model_info":{"qwen2.context_length":40960},
				"parameters":"num_ctx                        16384\nstop                          \"<|im_start|>\""
			}`))
		},
		"/api/ps": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("expected GET on /api/ps, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"models":[{"name":"qwen3:8b","model":"qwen3:8b","context_length":8192}]}`))
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	effective, modelMax, err := c.ContextLength(context.Background(), "qwen3:8b")
	if err != nil {
		t.Fatalf("ContextLength: %v", err)
	}
	if modelMax != 40960 {
		t.Errorf("modelMax = %d, want 40960", modelMax)
	}
	if effective != 8192 {
		t.Errorf("effective = %d, want 8192 (loaded num_ctx wins over Modelfile param and model max)", effective)
	}
}

// TestOllamaContextLength_FallsBackToParameters verifies that when /api/ps
// has no entry for the model (model unloaded), the Modelfile PARAMETER
// num_ctx is used.
func TestOllamaContextLength_FallsBackToParameters(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"model_info":{"qwen2.context_length":40960},
				"parameters":"num_ctx 8192\nstop \"<|im_start|>\""
			}`))
		},
		"/api/ps": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			// A different model is loaded; ours is not in the list.
			_, _ = w.Write([]byte(`{"models":[{"name":"llama3:latest","model":"llama3:latest","context_length":2048}]}`))
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	effective, modelMax, err := c.ContextLength(context.Background(), "qwen3:8b")
	if err != nil {
		t.Fatalf("ContextLength: %v", err)
	}
	if modelMax != 40960 {
		t.Errorf("modelMax = %d, want 40960", modelMax)
	}
	if effective != 8192 {
		t.Errorf("effective = %d, want 8192 (Modelfile num_ctx falls in when /api/ps has no entry)", effective)
	}
}

// TestOllamaContextLength_ParametersOnlyMatchesNumCtx verifies the
// parameters scanner picks num_ctx and ignores other lines that have
// numeric tokens in the same shape.
func TestOllamaContextLength_ParametersOnlyMatchesNumCtx(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			// Other parameter lines (stop strings, repeat_penalty, top_k)
			// must not be misread as num_ctx.
			_, _ = w.Write([]byte(`{
				"model_info":{"llama.context_length":8192},
				"parameters":"stop \"<|im_start|>\"\nstop \"<|im_end|>\"\nrepeat_penalty 1.1\ntop_k 40\nnum_ctx 4096\n"
			}`))
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	effective, modelMax, err := c.ContextLength(context.Background(), "any:tag")
	if err != nil {
		t.Fatalf("ContextLength: %v", err)
	}
	if modelMax != 8192 {
		t.Errorf("modelMax = %d, want 8192", modelMax)
	}
	if effective != 4096 {
		t.Errorf("effective = %d, want 4096 (num_ctx must be picked from a mixed parameters block)", effective)
	}
}

// TestOllamaContextLength_PSFailureIsNonFatal verifies that a /api/ps
// error doesn't fail the whole call — we still report what /api/show
// gave us.
func TestOllamaContextLength_PSFailureIsNonFatal(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"model_info":{"llama.context_length":8192}}`))
		},
		"/api/ps": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	effective, modelMax, err := c.ContextLength(context.Background(), "any:tag")
	if err != nil {
		t.Fatalf("ContextLength returned error despite /api/ps failure: %v", err)
	}
	if modelMax != 8192 {
		t.Errorf("modelMax = %d, want 8192", modelMax)
	}
	if effective != 8192 {
		t.Errorf("effective = %d, want 8192 (no num_ctx -> equal to max)", effective)
	}
}

// TestOllamaContextLength_ConfiguredAboveMaxIgnored verifies that a
// configured num_ctx larger than the model's hard ceiling does not
// inflate the effective value — the model's GGUF max is the final cap.
func TestOllamaContextLength_ConfiguredAboveMaxIgnored(t *testing.T) {
	srv := newOllamaTestServer(t, map[string]http.HandlerFunc{
		"/api/show": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"model_info":{"llama.context_length":8192},
				"parameters":"num_ctx 99999\n"
			}`))
		},
	})
	defer srv.Close()

	c := NewOllamaClient(OllamaConfig{BaseURL: srv.URL})
	effective, modelMax, err := c.ContextLength(context.Background(), "any:tag")
	if err != nil {
		t.Fatalf("ContextLength: %v", err)
	}
	if modelMax != 8192 {
		t.Errorf("modelMax = %d, want 8192", modelMax)
	}
	if effective != 8192 {
		t.Errorf("effective = %d, want 8192 (configured > max must clamp to max)", effective)
	}
}

// TestParseNumCtxFromParameters_EdgeCases exercises the parser directly
// for inputs the live tests don't naturally cover.
func TestParseNumCtxFromParameters_EdgeCases(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
	}{
		{"empty", "", 0},
		{"absent", "stop \"<|im_start|>\"\nrepeat_penalty 1.1\n", 0},
		{"trailing whitespace value", "num_ctx    8192   \n", 8192},
		{"leading spaces tolerated", "  num_ctx 8192\n", 8192},
		{"non-numeric value ignored", "num_ctx abc\n", 0},
		{"zero ignored", "num_ctx 0\n", 0},
		{"negative ignored", "num_ctx -1\n", 0},
		{"first match wins", "num_ctx 1024\nnum_ctx 8192\n", 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseNumCtxFromParameters(tc.in)
			if got != tc.want {
				t.Errorf("parseNumCtxFromParameters(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
