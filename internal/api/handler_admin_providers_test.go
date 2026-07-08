package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- mock ProviderAdminStore ---

type mockProviderAdminStore struct {
	config       ProviderConfigResponse
	configErr    error
	testResult   *ProviderTestResult
	testErr      error
	updateErr    error
	updateResult *UpdateProviderSlotResult
	models       []OllamaModel
	modelsErr    error
	pullErr      error

	providerModels    []string
	providerModelsErr error

	// capture args
	updatedSlot          string
	updatedCfg           ProviderSlotConfig
	updatedOpts          UpdateProviderSlotOpts
	pulledModel          string
	listHeaders          map[string]string
	pulledHeaders        map[string]string
	providerModelsURL    string
	providerModelsHeader map[string]string
}

func (m *mockProviderAdminStore) GetProviderConfig(_ context.Context) (ProviderConfigResponse, error) {
	return m.config, m.configErr
}

// findSlot returns the status for a slot name from a ProviderConfigResponse.
func findSlot(resp ProviderConfigResponse, slot string) (ProviderSlotStatus, bool) {
	for _, s := range resp {
		if s.Slot == slot {
			return s, true
		}
	}
	return ProviderSlotStatus{}, false
}

func (m *mockProviderAdminStore) TestProvider(_ context.Context, req ProviderTestRequest) (*ProviderTestResult, error) {
	return m.testResult, m.testErr
}

func (m *mockProviderAdminStore) UpdateProviderSlot(_ context.Context, slot string, cfg ProviderSlotConfig, opts UpdateProviderSlotOpts) (*UpdateProviderSlotResult, error) {
	m.updatedSlot = slot
	m.updatedCfg = cfg
	m.updatedOpts = opts
	return m.updateResult, m.updateErr
}

func (m *mockProviderAdminStore) ListOllamaModels(_ context.Context, _ string, headers map[string]string) ([]OllamaModel, error) {
	m.listHeaders = headers
	return m.models, m.modelsErr
}

func (m *mockProviderAdminStore) PullOllamaModel(_ context.Context, model string, _ string, headers map[string]string) error {
	m.pulledModel = model
	m.pulledHeaders = headers
	return m.pullErr
}

func (m *mockProviderAdminStore) ListProviderModels(_ context.Context, url string, headers map[string]string) ([]string, error) {
	m.providerModelsURL = url
	m.providerModelsHeader = headers
	return m.providerModels, m.providerModelsErr
}

// --- tests ---

func TestAdminProvidersGetConfig(t *testing.T) {
	dims := 1536
	latency := int64(42)
	store := &mockProviderAdminStore{
		config: ProviderConfigResponse{
			{
				Slot:       "embedding",
				Configured: true,
				Type:       "openai",
				URL:        "https://api.openai.com",
				Model:      "text-embedding-3-small",
				Dimensions: &dims,
				Status:     "ok",
				LatencyMs:  &latency,
			},
			{
				Slot:       "fact",
				Configured: false,
				Status:     "not_configured",
			},
			{
				Slot:       "entity",
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

	emb, ok := findSlot(resp, "embedding")
	if !ok || !emb.Configured {
		t.Error("expected embedding configured")
	}
	if emb.Type != "openai" {
		t.Errorf("expected type openai, got %q", emb.Type)
	}
	if emb.Dimensions == nil || *emb.Dimensions != 1536 {
		t.Errorf("expected dimensions 1536")
	}
	if fact, _ := findSlot(resp, "fact"); fact.Configured {
		t.Error("expected fact not configured")
	}
	if ent, _ := findSlot(resp, "entity"); ent.Type != "ollama" {
		t.Errorf("expected entity type ollama, got %q", ent.Type)
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

func TestAdminProvidersUpdateReturnsWarning(t *testing.T) {
	store := &mockProviderAdminStore{
		updateResult: &UpdateProviderSlotResult{
			Warning: `model "qwen3:4b" is not served by http://host:11434 (available: qwen3:8b); the slot was saved anyway`,
		},
	}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"type":"ollama","url":"http://host:11434","model":"qwen3:4b"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/providers/fact", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp UpdateProviderSlotResult
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Warning != store.updateResult.Warning {
		t.Errorf("expected warning passthrough %q, got %q", store.updateResult.Warning, resp.Warning)
	}
	if resp.NeedsConfirmation {
		t.Errorf("warning-only result must not set needs_confirmation")
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

func TestAdminProvidersUpdateCustomHeadersAndClearKey(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	body := `{"type":"openai","url":"https://proxy.example.com","model":"gpt-4","custom_headers":{"X-Proxy-Auth":"tok"},"clear_api_key":true}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/providers/fact", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := store.updatedCfg.CustomHeaders["X-Proxy-Auth"]; got != "tok" {
		t.Errorf("custom header not captured: %q", got)
	}
	if !store.updatedOpts.ClearAPIKey {
		t.Error("clear_api_key not threaded into UpdateProviderSlotOpts")
	}
}

func TestAdminProvidersGetMasksHeaderValues(t *testing.T) {
	store := &mockProviderAdminStore{
		config: ProviderConfigResponse{
			{
				Slot:             "fact",
				Configured:       true,
				Type:             "openai",
				Status:           "ok",
				APIKeySet:        true,
				CustomHeaderKeys: []string{"X-Proxy-Auth"},
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
	raw := w.Body.String()
	if !strings.Contains(raw, `"api_key_set":true`) {
		t.Errorf("api_key_set not surfaced: %s", raw)
	}
	if !strings.Contains(raw, `"custom_header_keys":["X-Proxy-Auth"]`) {
		t.Errorf("custom_header_keys not surfaced: %s", raw)
	}
	// A header value must never appear in the response.
	if strings.Contains(raw, "custom_headers") {
		t.Errorf("GET response must not include custom_headers values: %s", raw)
	}
}

func TestAdminProvidersOllamaModelsForwardsHeaders(t *testing.T) {
	store := &mockProviderAdminStore{}
	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/providers/ollama/models?url=http://proxy", nil)
	req.Header.Set("X-Nram-Provider-Header-X-Proxy-Auth", "tok")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := store.listHeaders["X-Proxy-Auth"]; got != "tok" {
		t.Errorf("forwarded header not extracted: %v", store.listHeaders)
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

func TestAdminProvidersListProviderModels(t *testing.T) {
	store := &mockProviderAdminStore{
		providerModels: []string{"Qwen/Qwen3-8B", "Qwen/Qwen3-Embedding-0.6B"},
	}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet,
		"/v1/admin/providers/models?url=http://host:8000", nil)
	req.Header.Set("X-Nram-Provider-Header-X-Proxy", "v1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp []string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 2 || resp[0] != "Qwen/Qwen3-8B" {
		t.Fatalf("unexpected models: %v", resp)
	}
	if store.providerModelsURL != "http://host:8000" {
		t.Errorf("url=%q, want http://host:8000", store.providerModelsURL)
	}
	if store.providerModelsHeader["X-Proxy"] != "v1" {
		t.Errorf("forwarded header X-Proxy=%q, want v1", store.providerModelsHeader["X-Proxy"])
	}
}

func TestAdminProvidersListProviderModelsMissingURL(t *testing.T) {
	store := &mockProviderAdminStore{}

	h := NewAdminProvidersHandler(ProviderAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/providers/models", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
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
