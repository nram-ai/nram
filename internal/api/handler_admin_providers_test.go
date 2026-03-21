package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- mock ProviderAdminStore ---

type mockProviderAdminStore struct {
	config     *ProviderConfigResponse
	configErr  error
	testResult *ProviderTestResult
	testErr    error
	updateErr  error
	models     []OllamaModel
	modelsErr  error
	pullErr    error

	// capture args
	updatedSlot string
	updatedCfg  ProviderSlotConfig
	pulledModel string
}

func (m *mockProviderAdminStore) GetProviderConfig(_ context.Context) (*ProviderConfigResponse, error) {
	return m.config, m.configErr
}

func (m *mockProviderAdminStore) TestProvider(_ context.Context, req ProviderTestRequest) (*ProviderTestResult, error) {
	return m.testResult, m.testErr
}

func (m *mockProviderAdminStore) UpdateProviderSlot(_ context.Context, slot string, cfg ProviderSlotConfig) error {
	m.updatedSlot = slot
	m.updatedCfg = cfg
	return m.updateErr
}

func (m *mockProviderAdminStore) ListOllamaModels(_ context.Context, _ string) ([]OllamaModel, error) {
	return m.models, m.modelsErr
}

func (m *mockProviderAdminStore) PullOllamaModel(_ context.Context, model string, _ string) error {
	m.pulledModel = model
	return m.pullErr
}

// --- tests ---

func TestAdminProvidersGetConfig(t *testing.T) {
	dims := 1536
	latency := int64(42)
	store := &mockProviderAdminStore{
		config: &ProviderConfigResponse{
			Embedding: ProviderSlotStatus{
				Configured: true,
				Type:       "openai",
				URL:        "https://api.openai.com",
				Model:      "text-embedding-3-small",
				Dimensions: &dims,
				Status:     "ok",
				LatencyMs:  &latency,
			},
			Fact: ProviderSlotStatus{
				Configured: false,
				Status:     "not_configured",
			},
			Entity: ProviderSlotStatus{
				Configured: true,
				Type:       "ollama",
				URL:        "http://localhost:11434",
				Model:      "llama3",
				Status:     "ok",
			},
		},
	}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/providers", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp ProviderConfigResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !resp.Embedding.Configured {
		t.Error("expected embedding configured")
	}
	if resp.Embedding.Type != "openai" {
		t.Errorf("expected type openai, got %q", resp.Embedding.Type)
	}
	if resp.Embedding.Dimensions == nil || *resp.Embedding.Dimensions != 1536 {
		t.Errorf("expected dimensions 1536")
	}
	if resp.Fact.Configured {
		t.Error("expected fact not configured")
	}
	if resp.Entity.Type != "ollama" {
		t.Errorf("expected entity type ollama, got %q", resp.Entity.Type)
	}
}

func TestAdminProvidersTestSuccess(t *testing.T) {
	store := &mockProviderAdminStore{
		testResult: &ProviderTestResult{
			Success:   true,
			Message:   "connection successful",
			LatencyMs: 150,
		},
	}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"slot":"embedding","config":{"type":"openai","url":"https://api.openai.com","model":"text-embedding-3-small"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/providers/test", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp ProviderTestResult
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !resp.Success {
		t.Error("expected success true")
	}
	if resp.LatencyMs != 150 {
		t.Errorf("expected latency 150, got %d", resp.LatencyMs)
	}
}

func TestAdminProvidersTestInvalidSlot(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"slot":"invalid","config":{"type":"openai","url":"https://api.openai.com","model":"gpt-4"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/providers/test", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminProvidersUpdateEmbedding(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"type":"openai","url":"https://api.openai.com","api_key":"sk-test","model":"text-embedding-3-small","dimensions":1536}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/providers/embedding", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %q", resp["status"])
	}
	if store.updatedSlot != "embedding" {
		t.Errorf("expected slot embedding, got %q", store.updatedSlot)
	}
	if store.updatedCfg.Type != "openai" {
		t.Errorf("expected type openai, got %q", store.updatedCfg.Type)
	}
}

func TestAdminProvidersUpdateFact(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"type":"anthropic","url":"https://api.anthropic.com","api_key":"sk-ant-test","model":"claude-3-haiku"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/providers/fact", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.updatedSlot != "fact" {
		t.Errorf("expected slot fact, got %q", store.updatedSlot)
	}
}

func TestAdminProvidersUpdateEntity(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"type":"ollama","url":"http://localhost:11434","model":"llama3"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/providers/entity", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.updatedSlot != "entity" {
		t.Errorf("expected slot entity, got %q", store.updatedSlot)
	}
}

func TestAdminProvidersUpdateSlotMissingType(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"type":"","url":"https://api.openai.com","model":"gpt-4"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/providers/embedding", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminProvidersListOllamaModels(t *testing.T) {
	store := &mockProviderAdminStore{
		models: []OllamaModel{
			{Name: "llama3:latest", Size: 4700000000, ModifiedAt: "2024-03-01T12:00:00Z"},
			{Name: "mistral:latest", Size: 3800000000, ModifiedAt: "2024-02-15T10:30:00Z"},
		},
	}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/providers/ollama/models", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp []OllamaModel
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp) != 2 {
		t.Fatalf("expected 2 models, got %d", len(resp))
	}
	if resp[0].Name != "llama3:latest" {
		t.Errorf("expected llama3:latest, got %q", resp[0].Name)
	}
	if resp[1].Name != "mistral:latest" {
		t.Errorf("expected mistral:latest, got %q", resp[1].Name)
	}
}

func TestAdminProvidersPullOllamaModel(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"model":"llama3:latest"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/providers/ollama/pull", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "accepted" {
		t.Errorf("expected status accepted, got %q", resp["status"])
	}
	if resp["model"] != "llama3:latest" {
		t.Errorf("expected model llama3:latest, got %q", resp["model"])
	}
	if store.pulledModel != "llama3:latest" {
		t.Errorf("expected pulled model llama3:latest, got %q", store.pulledModel)
	}
}

func TestAdminProvidersPullOllamaModelMissingName(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"model":""}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/providers/ollama/pull", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminProvidersGetConfigStoreError(t *testing.T) {
	store := &mockProviderAdminStore{
		configErr: errors.New("database failure"),
	}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/providers", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected code internal_error, got %q", resp.Error.Code)
	}
}

func TestAdminProvidersUnsupportedMethodOnRoot(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/providers", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}
