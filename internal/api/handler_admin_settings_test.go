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
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// --- mock SettingsAdminStore ---

type mockSettingsAdminStore struct {
	settings    []model.Setting
	listErr     error
	updateErr   error
	schemas     []SettingSchema
	schemaErr   error

	// capture args
	listScope    string
	updatedKey   string
	updatedValue json.RawMessage
	updatedScope string
	updatedBy    *uuid.UUID
}

func (m *mockSettingsAdminStore) CountSettings(_ context.Context, scope string) (int, error) {
	return len(m.settings), m.listErr
}

func (m *mockSettingsAdminStore) ListSettings(_ context.Context, scope string, limit, offset int) ([]model.Setting, error) {
	m.listScope = scope
	return m.settings, m.listErr
}

func (m *mockSettingsAdminStore) UpdateSetting(_ context.Context, key string, value json.RawMessage, scope string, updatedBy *uuid.UUID) error {
	m.updatedKey = key
	m.updatedValue = value
	m.updatedScope = scope
	m.updatedBy = updatedBy
	return m.updateErr
}

func (m *mockSettingsAdminStore) GetSettingsSchema(_ context.Context) ([]SettingSchema, error) {
	return m.schemas, m.schemaErr
}

// --- tests ---

func TestAdminSettingsListSettings(t *testing.T) {
	userID := uuid.New()
	store := &mockSettingsAdminStore{
		settings: []model.Setting{
			{
				Key:       "memory.max_facts",
				Value:     json.RawMessage(`1000`),
				Scope:     "global",
				UpdatedBy: &userID,
				UpdatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			},
			{
				Key:       "enrichment.auto_extract",
				Value:     json.RawMessage(`true`),
				Scope:     "global",
				UpdatedBy: nil,
				UpdatedAt: time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC),
			},
		},
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp model.PaginatedResponse[model.Setting]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 settings, got %d", len(resp.Data))
	}
	if resp.Data[0].Key != "memory.max_facts" {
		t.Errorf("expected key memory.max_facts, got %q", resp.Data[0].Key)
	}
	if resp.Data[1].Key != "enrichment.auto_extract" {
		t.Errorf("expected key enrichment.auto_extract, got %q", resp.Data[1].Key)
	}
	if resp.Pagination.Total != 2 {
		t.Errorf("expected pagination.total=2, got %d", resp.Pagination.Total)
	}
}

func TestAdminSettingsListSettingsWithScope(t *testing.T) {
	store := &mockSettingsAdminStore{
		settings: []model.Setting{
			{
				Key:       "ranking.weight",
				Value:     json.RawMessage(`0.5`),
				Scope:     "project",
				UpdatedAt: time.Now(),
			},
		},
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings?scope=project", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.listScope != "project" {
		t.Errorf("expected scope project, got %q", store.listScope)
	}

	var resp model.PaginatedResponse[model.Setting]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 setting, got %d", len(resp.Data))
	}
	if resp.Data[0].Scope != "project" {
		t.Errorf("expected scope project, got %q", resp.Data[0].Scope)
	}
}

func TestAdminSettingsGetSchema(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{
				Key:          "memory.max_facts",
				Type:         "int",
				DefaultValue: json.RawMessage(`1000`),
				Description:  "Maximum number of facts per project",
				Category:     "memory",
			},
			{
				Key:          "enrichment.auto_extract",
				Type:         "bool",
				DefaultValue: json.RawMessage(`true`),
				Description:  "Automatically extract entities and facts",
				Category:     "enrichment",
			},
		},
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings?schema=true", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []SettingSchema `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(resp.Data))
	}
	if resp.Data[0].Key != "memory.max_facts" {
		t.Errorf("expected key memory.max_facts, got %q", resp.Data[0].Key)
	}
	if resp.Data[0].Type != "int" {
		t.Errorf("expected type int, got %q", resp.Data[0].Type)
	}
	if resp.Data[0].Category != "memory" {
		t.Errorf("expected category memory, got %q", resp.Data[0].Category)
	}
	if resp.Data[1].Key != "enrichment.auto_extract" {
		t.Errorf("expected key enrichment.auto_extract, got %q", resp.Data[1].Key)
	}
}

func TestAdminSettingsGetSchemaQdrantEntries(t *testing.T) {
	// Build schemas that include the 6 qdrant entries (matching production GetSettingsSchema).
	qdrantSchemas := []SettingSchema{
		{Key: "qdrant.addr", Type: "string", DefaultValue: json.RawMessage(`""`), Description: "Qdrant gRPC address", Category: "qdrant"},
		{Key: "qdrant.api_key", Type: "secret", DefaultValue: json.RawMessage(`""`), Description: "API key for Qdrant", Category: "qdrant"},
		{Key: "qdrant.use_tls", Type: "boolean", DefaultValue: json.RawMessage(`false`), Description: "Enable TLS", Category: "qdrant"},
		{Key: "qdrant.pool_size", Type: "number", DefaultValue: json.RawMessage(`3`), Description: "Pool size", Category: "qdrant"},
		{Key: "qdrant.keepalive_time", Type: "number", DefaultValue: json.RawMessage(`10`), Description: "Keepalive time", Category: "qdrant"},
		{Key: "qdrant.keepalive_timeout", Type: "number", DefaultValue: json.RawMessage(`2`), Description: "Keepalive timeout", Category: "qdrant"},
	}

	// Include a non-qdrant entry to verify filtering.
	allSchemas := append([]SettingSchema{
		{Key: "memory.default_confidence", Type: "number", DefaultValue: json.RawMessage(`0.9`), Description: "Default confidence", Category: "memory"},
	}, qdrantSchemas...)

	store := &mockSettingsAdminStore{schemas: allSchemas}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings?schema=true", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []SettingSchema `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Count qdrant entries in response.
	byKey := make(map[string]SettingSchema)
	qdrantCount := 0
	for _, s := range resp.Data {
		if s.Category == "qdrant" {
			qdrantCount++
			byKey[s.Key] = s
		}
	}

	if qdrantCount != 6 {
		t.Fatalf("expected 6 qdrant entries in response, got %d", qdrantCount)
	}

	// Verify specific types.
	if s, ok := byKey["qdrant.addr"]; !ok {
		t.Error("missing qdrant.addr in response")
	} else if s.Type != "string" {
		t.Errorf("qdrant.addr: expected type string, got %q", s.Type)
	}

	if s, ok := byKey["qdrant.api_key"]; !ok {
		t.Error("missing qdrant.api_key in response")
	} else if s.Type != "secret" {
		t.Errorf("qdrant.api_key: expected type secret, got %q", s.Type)
	}

	if s, ok := byKey["qdrant.use_tls"]; !ok {
		t.Error("missing qdrant.use_tls in response")
	} else if s.Type != "boolean" {
		t.Errorf("qdrant.use_tls: expected type boolean, got %q", s.Type)
	}
}

func TestAdminSettingsUpdateSetting(t *testing.T) {
	store := &mockSettingsAdminStore{}
	userID := uuid.New()

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"memory.max_facts","value":2000,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: userID,
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %q", resp["status"])
	}
	if store.updatedKey != "memory.max_facts" {
		t.Errorf("expected key memory.max_facts, got %q", store.updatedKey)
	}
	if store.updatedScope != "global" {
		t.Errorf("expected scope global, got %q", store.updatedScope)
	}
	if store.updatedBy == nil || *store.updatedBy != userID {
		t.Errorf("expected updatedBy %s, got %v", userID, store.updatedBy)
	}
}

func TestAdminSettingsUpdateSettingMissingKey(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"","value":100,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
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

func TestAdminSettingsListStoreError(t *testing.T) {
	store := &mockSettingsAdminStore{
		listErr: errors.New("database failure"),
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings", nil)
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

func TestAdminSettingsUpdateStoreError(t *testing.T) {
	store := &mockSettingsAdminStore{
		updateErr: errors.New("database failure"),
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"memory.max_facts","value":2000,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
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

func TestAdminSettingsUpdateNotFoundError(t *testing.T) {
	store := &mockSettingsAdminStore{
		updateErr: errors.New("setting not found"),
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"nonexistent.key","value":"abc","scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
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

func TestAdminSettingsSchemaStoreError(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemaErr: errors.New("database failure"),
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings?schema=true", nil)
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

func TestAdminSettingsUnsupportedMethod(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/settings", nil)
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

func TestAdminSettingsUpdateDefaultsToGlobalScope(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"memory.max_facts","value":500}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	if store.updatedScope != "global" {
		t.Errorf("expected scope global, got %q", store.updatedScope)
	}
}

func TestAdminSettingsUpdateNoAuthContext(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"memory.max_facts","value":500,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	if store.updatedBy != nil {
		t.Errorf("expected updatedBy nil, got %v", store.updatedBy)
	}
}

func TestAdminSettingsUpdateMissingValue(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"memory.max_facts","scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
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

func TestAdminSettingsUpdateInvalidJSON(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `not json`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
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
