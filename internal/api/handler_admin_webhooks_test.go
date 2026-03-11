package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- mock WebhookAdminStore ---

type mockWebhookAdminStore struct {
	webhooks   []model.Webhook
	webhook    *model.Webhook
	testResult *WebhookTestResult
	listErr    error
	createErr  error
	getErr     error
	updateErr  error
	deleteErr  error
	testErr    error

	// capture args
	createdURL    string
	createdScope  string
	createdEvents []string
	createdSecret *string
	createdActive bool
	updatedID     uuid.UUID
	updatedURL    string
	updatedScope  string
	updatedEvents []string
	updatedSecret *string
	updatedActive bool
	deletedID     uuid.UUID
	testedID      uuid.UUID
	gotID         uuid.UUID
}

func (m *mockWebhookAdminStore) ListWebhooks(_ context.Context) ([]model.Webhook, error) {
	return m.webhooks, m.listErr
}

func (m *mockWebhookAdminStore) CreateWebhook(_ context.Context, url, scope string, events []string, secret *string, active bool) (*model.Webhook, error) {
	m.createdURL = url
	m.createdScope = scope
	m.createdEvents = events
	m.createdSecret = secret
	m.createdActive = active
	return m.webhook, m.createErr
}

func (m *mockWebhookAdminStore) GetWebhook(_ context.Context, id uuid.UUID) (*model.Webhook, error) {
	m.gotID = id
	return m.webhook, m.getErr
}

func (m *mockWebhookAdminStore) UpdateWebhook(_ context.Context, id uuid.UUID, url, scope string, events []string, secret *string, active bool) (*model.Webhook, error) {
	m.updatedID = id
	m.updatedURL = url
	m.updatedScope = scope
	m.updatedEvents = events
	m.updatedSecret = secret
	m.updatedActive = active
	return m.webhook, m.updateErr
}

func (m *mockWebhookAdminStore) DeleteWebhook(_ context.Context, id uuid.UUID) error {
	m.deletedID = id
	return m.deleteErr
}

func (m *mockWebhookAdminStore) TestWebhook(_ context.Context, id uuid.UUID) (*WebhookTestResult, error) {
	m.testedID = id
	return m.testResult, m.testErr
}

// --- tests ---

func TestAdminWebhooksListWebhooks(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	store := &mockWebhookAdminStore{
		webhooks: []model.Webhook{
			{
				ID:        id1,
				URL:       "https://example.com/hook1",
				Events:    []string{"memory.stored"},
				Scope:     "global",
				Active:    true,
				CreatedAt: now,
				UpdatedAt: now,
			},
			{
				ID:        id2,
				URL:       "https://example.com/hook2",
				Events:    []string{"memory.deleted", "entity.created"},
				Scope:     "project",
				Active:    false,
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
	}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/webhooks", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []model.Webhook `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 webhooks, got %d", len(resp.Data))
	}
	if resp.Data[0].URL != "https://example.com/hook1" {
		t.Errorf("expected URL https://example.com/hook1, got %q", resp.Data[0].URL)
	}
	if resp.Data[1].URL != "https://example.com/hook2" {
		t.Errorf("expected URL https://example.com/hook2, got %q", resp.Data[1].URL)
	}
}

func TestAdminWebhooksListWebhooksEmpty(t *testing.T) {
	store := &mockWebhookAdminStore{
		webhooks: []model.Webhook{},
	}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/webhooks", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []model.Webhook `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != 0 {
		t.Fatalf("expected 0 webhooks, got %d", len(resp.Data))
	}
}

func TestAdminWebhooksCreateWebhookSuccess(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	secret := "s3cret"
	store := &mockWebhookAdminStore{
		webhook: &model.Webhook{
			ID:        id,
			URL:       "https://example.com/hook",
			Events:    []string{"memory.stored", "memory.deleted"},
			Scope:     "global",
			Active:    true,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	body := `{"url":"https://example.com/hook","events":["memory.stored","memory.deleted"],"secret":"s3cret","active":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/webhooks", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp model.Webhook
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.ID != id {
		t.Errorf("expected id %s, got %s", id, resp.ID)
	}
	if resp.URL != "https://example.com/hook" {
		t.Errorf("expected URL https://example.com/hook, got %q", resp.URL)
	}

	if store.createdURL != "https://example.com/hook" {
		t.Errorf("expected created URL https://example.com/hook, got %q", store.createdURL)
	}
	if store.createdScope != "global" {
		t.Errorf("expected scope global, got %q", store.createdScope)
	}
	if len(store.createdEvents) != 2 {
		t.Errorf("expected 2 events, got %d", len(store.createdEvents))
	}
	if store.createdSecret == nil || *store.createdSecret != secret {
		t.Errorf("expected secret %q, got %v", secret, store.createdSecret)
	}
	if !store.createdActive {
		t.Error("expected active true")
	}
}

func TestAdminWebhooksCreateWebhookMissingURL(t *testing.T) {
	store := &mockWebhookAdminStore{}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	body := `{"events":["memory.stored"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/webhooks", bytes.NewBufferString(body))
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

func TestAdminWebhooksCreateWebhookEmptyEvents(t *testing.T) {
	store := &mockWebhookAdminStore{}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	body := `{"url":"https://example.com/hook","events":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/webhooks", bytes.NewBufferString(body))
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

func TestAdminWebhooksGetWebhookFound(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	store := &mockWebhookAdminStore{
		webhook: &model.Webhook{
			ID:        id,
			URL:       "https://example.com/hook",
			Events:    []string{"memory.stored"},
			Scope:     "global",
			Active:    true,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/webhooks/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp model.Webhook
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.ID != id {
		t.Errorf("expected id %s, got %s", id, resp.ID)
	}
	if store.gotID != id {
		t.Errorf("expected gotID %s, got %s", id, store.gotID)
	}
}

func TestAdminWebhooksGetWebhookNotFound(t *testing.T) {
	store := &mockWebhookAdminStore{
		getErr: errors.New("webhook not found"),
	}

	id := uuid.New()
	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/webhooks/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "not_found" {
		t.Errorf("expected code not_found, got %q", resp.Error.Code)
	}
}

func TestAdminWebhooksUpdateWebhookSuccess(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	store := &mockWebhookAdminStore{
		webhook: &model.Webhook{
			ID:        id,
			URL:       "https://example.com/updated",
			Events:    []string{"memory.stored"},
			Scope:     "project",
			Active:    false,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	body := `{"url":"https://example.com/updated","events":["memory.stored"],"scope":"project","active":false}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/webhooks/"+id.String(), bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp model.Webhook
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.URL != "https://example.com/updated" {
		t.Errorf("expected URL https://example.com/updated, got %q", resp.URL)
	}
	if store.updatedID != id {
		t.Errorf("expected updatedID %s, got %s", id, store.updatedID)
	}
	if store.updatedURL != "https://example.com/updated" {
		t.Errorf("expected updated URL https://example.com/updated, got %q", store.updatedURL)
	}
	if store.updatedScope != "project" {
		t.Errorf("expected scope project, got %q", store.updatedScope)
	}
	if store.updatedActive {
		t.Error("expected active false")
	}
}

func TestAdminWebhooksUpdateWebhookNotFound(t *testing.T) {
	store := &mockWebhookAdminStore{
		updateErr: errors.New("webhook not found"),
	}

	id := uuid.New()
	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	body := `{"url":"https://example.com/updated","events":["memory.stored"]}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/webhooks/"+id.String(), bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "not_found" {
		t.Errorf("expected code not_found, got %q", resp.Error.Code)
	}
}

func TestAdminWebhooksDeleteWebhookSuccess(t *testing.T) {
	store := &mockWebhookAdminStore{}

	id := uuid.New()
	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/webhooks/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}

	if store.deletedID != id {
		t.Errorf("expected deletedID %s, got %s", id, store.deletedID)
	}
}

func TestAdminWebhooksDeleteWebhookNotFound(t *testing.T) {
	store := &mockWebhookAdminStore{
		deleteErr: errors.New("webhook not found"),
	}

	id := uuid.New()
	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/webhooks/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "not_found" {
		t.Errorf("expected code not_found, got %q", resp.Error.Code)
	}
}

func TestAdminWebhooksTestWebhookSuccess(t *testing.T) {
	id := uuid.New()
	store := &mockWebhookAdminStore{
		testResult: &WebhookTestResult{
			Success:    true,
			StatusCode: 200,
			Message:    "OK",
			LatencyMs:  42,
		},
	}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/webhooks/"+id.String()+"/test", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp WebhookTestResult
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !resp.Success {
		t.Error("expected success true")
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected status_code 200, got %d", resp.StatusCode)
	}
	if resp.LatencyMs != 42 {
		t.Errorf("expected latency_ms 42, got %d", resp.LatencyMs)
	}
	if store.testedID != id {
		t.Errorf("expected testedID %s, got %s", id, store.testedID)
	}
}

func TestAdminWebhooksTestWebhookNotFound(t *testing.T) {
	store := &mockWebhookAdminStore{
		testErr: errors.New("webhook not found"),
	}

	id := uuid.New()
	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/webhooks/"+id.String()+"/test", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "not_found" {
		t.Errorf("expected code not_found, got %q", resp.Error.Code)
	}
}

func TestAdminWebhooksInvalidUUID(t *testing.T) {
	store := &mockWebhookAdminStore{}

	h := NewAdminWebhooksHandler(WebhookAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/webhooks/not-a-uuid", nil)
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
