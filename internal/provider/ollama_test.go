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
			json.NewEncoder(w).Encode(resp)
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
			json.NewEncoder(w).Encode(resp)
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
				fmt.Fprintf(w, "%s\n", line)
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
				fmt.Fprintf(w, "%s\n", line)
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
				fmt.Fprintf(w, "%s\n", line)
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
			fmt.Fprint(w, "Ollama is running")
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
	if c.client.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want %v", c.client.Timeout, 30*time.Second)
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
