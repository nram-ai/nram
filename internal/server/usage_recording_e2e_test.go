package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// captureRecorder collects every TokenUsage row written through Record so
// the test can assert on the rows that landed in token_usage when an HTTP
// request flowed all the way through middleware → handler → provider.
type captureRecorder struct {
	mu   sync.Mutex
	rows []*model.TokenUsage
}

func (c *captureRecorder) Record(_ context.Context, u *model.TokenUsage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = append(c.rows, u)
	return nil
}

func (c *captureRecorder) snapshot() []*model.TokenUsage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*model.TokenUsage, len(c.rows))
	copy(out, c.rows)
	return out
}

// scriptedLLM is a minimal LLMProvider used to drive the e2e flow.
type scriptedLLM struct {
	resp *provider.CompletionResponse
	err  error
}

func (s *scriptedLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	return s.resp, s.err
}
func (s *scriptedLLM) Name() string     { return "scripted-llm" }
func (s *scriptedLLM) Models() []string { return []string{"scripted-model"} }

// scriptedEmbedder is a minimal EmbeddingProvider.
type scriptedEmbedder struct {
	resp *provider.EmbeddingResponse
	err  error
}

func (s *scriptedEmbedder) Embed(_ context.Context, _ *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return s.resp, s.err
}
func (s *scriptedEmbedder) Name() string      { return "scripted-embedder" }
func (s *scriptedEmbedder) Dimensions() []int { return []int{4} }

// buildE2ERouter wires a chi router with the global middleware stack used in
// production (panic recovery + RequestIDMiddleware) and mounts a tiny
// /v1/test-emit handler that calls the wrapped provider once. The handler
// stamps OperationFactExtraction so the middleware records the call.
func buildE2ERouter(llm provider.LLMProvider) http.Handler {
	r := chi.NewRouter()
	r.Use(RequestIDMiddleware)
	r.Post("/v1/test-emit", func(w http.ResponseWriter, req *http.Request) {
		ctx := provider.WithOperation(req.Context(), provider.OperationFactExtraction)
		ctx = provider.WithNamespaceID(ctx, uuid.New())
		_, _ = llm.Complete(ctx, &provider.CompletionRequest{
			Model:    "scripted-model",
			Messages: []provider.Message{{Role: "user", Content: "hello"}},
		})
		w.WriteHeader(http.StatusNoContent)
	})
	return r
}

// TestUsageRecordingE2E_RequestIDPropagatedToTokenUsage verifies that the
// RequestIDMiddleware stamps X-Request-ID into the request context and that
// a downstream provider call (wrapped by UsageRecordingProvider) writes a
// token_usage row whose request_id, operation, model, provider, success,
// and latency fields are all populated end-to-end. This is the integration
// proof that all the middleware pieces compose correctly.
func TestUsageRecordingE2E_RequestIDPropagatedToTokenUsage(t *testing.T) {
	rec := &captureRecorder{}
	inner := &scriptedLLM{
		resp: &provider.CompletionResponse{
			Content: "ok",
			Model:   "scripted-model",
			Usage:   provider.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
		},
	}
	wrapped := provider.NewUsageRecordingLLM(inner, rec, nil)
	router := buildE2ERouter(wrapped)

	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/test-emit", bytes.NewReader([]byte("{}")))
	req.Header.Set("X-Request-ID", "test-e2e-001")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("X-Request-ID") != "test-e2e-001" {
		t.Errorf("response did not echo X-Request-ID: got %q", resp.Header.Get("X-Request-ID"))
	}

	rows := rec.snapshot()
	if len(rows) != 1 {
		t.Fatalf("expected 1 token_usage row, got %d", len(rows))
	}
	row := rows[0]
	if row.Operation != string(provider.OperationFactExtraction) {
		t.Errorf("Operation: got %q want %q", row.Operation, provider.OperationFactExtraction)
	}
	if row.Provider != "scripted-llm" {
		t.Errorf("Provider: got %q", row.Provider)
	}
	if row.Model != "scripted-model" {
		t.Errorf("Model: got %q", row.Model)
	}
	if row.TokensInput != 100 || row.TokensOutput != 50 {
		t.Errorf("Tokens: got in=%d out=%d", row.TokensInput, row.TokensOutput)
	}
	if !row.Success {
		t.Error("expected Success=true")
	}
	if row.RequestID == nil || *row.RequestID != "test-e2e-001" {
		t.Errorf("RequestID: got %v want test-e2e-001", row.RequestID)
	}
	if row.LatencyMs == nil {
		t.Error("expected LatencyMs to be populated")
	}
}

// TestUsageRecordingE2E_ProviderErrorRecordsFailureRow verifies that when
// the underlying provider returns an error, the middleware still writes a
// token_usage row with success=false and a bounded error_code, and the
// request_id is still correlated even on the error path.
func TestUsageRecordingE2E_ProviderErrorRecordsFailureRow(t *testing.T) {
	rec := &captureRecorder{}
	inner := &scriptedLLM{
		err: errors.New("upstream 500"),
	}
	wrapped := provider.NewUsageRecordingLLM(inner, rec, nil)
	router := buildE2ERouter(wrapped)

	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/test-emit", bytes.NewReader([]byte("{}")))
	req.Header.Set("X-Request-ID", "test-e2e-err")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	rows := rec.snapshot()
	if len(rows) != 1 {
		t.Fatalf("expected 1 token_usage row, got %d", len(rows))
	}
	row := rows[0]
	if row.Success {
		t.Error("expected Success=false on provider error")
	}
	if row.ErrorCode == nil || *row.ErrorCode != "provider_error" {
		t.Errorf("ErrorCode: got %v want provider_error", row.ErrorCode)
	}
	if row.RequestID == nil || *row.RequestID != "test-e2e-err" {
		t.Errorf("RequestID: got %v want test-e2e-err", row.RequestID)
	}
	if row.LatencyMs == nil {
		t.Error("expected LatencyMs even on error path")
	}
}

// TestUsageRecordingE2E_NoIncomingRequestIDStampsGenerated verifies that
// when the client does not provide X-Request-ID, the middleware generates
// a UUID and that ID lands on the token_usage row — so every provider call
// is correlatable to an inbound request even for unmodified clients.
func TestUsageRecordingE2E_NoIncomingRequestIDStampsGenerated(t *testing.T) {
	rec := &captureRecorder{}
	inner := &scriptedEmbedder{
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{{0.1, 0.2}},
			Model:      "scripted-embed",
			Usage:      provider.TokenUsage{PromptTokens: 7},
		},
	}
	wrapped := provider.NewUsageRecordingEmbedding(inner, rec, nil)

	r := chi.NewRouter()
	r.Use(RequestIDMiddleware)
	r.Post("/v1/test-emit", func(w http.ResponseWriter, req *http.Request) {
		ctx := provider.WithOperation(req.Context(), provider.OperationEmbedding)
		_, _ = wrapped.Embed(ctx, &provider.EmbeddingRequest{Input: []string{"hi"}})
		w.WriteHeader(http.StatusNoContent)
	})

	srv := httptest.NewServer(r)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/test-emit", bytes.NewReader([]byte("{}")))
	// Deliberately no X-Request-ID set.
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	echoed := resp.Header.Get("X-Request-ID")
	if echoed == "" {
		t.Fatal("middleware did not generate or echo a request ID")
	}

	rows := rec.snapshot()
	if len(rows) != 1 {
		t.Fatalf("expected 1 token_usage row, got %d", len(rows))
	}
	row := rows[0]
	if row.RequestID == nil || *row.RequestID != echoed {
		t.Errorf("token_usage.request_id (%v) does not match echoed header (%q)", row.RequestID, echoed)
	}

	// JSON round-trip should still serialise cleanly.
	if _, err := json.Marshal(row); err != nil {
		t.Errorf("token_usage row failed to marshal: %v", err)
	}
}
