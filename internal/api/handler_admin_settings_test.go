package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// --- mock SettingsAdminStore ---

type mockSettingsAdminStore struct {
	settings  []model.Setting
	listErr   error
	updateErr error
	schemas   []SettingSchema
	schemaErr error
	groups    []SettingGroup
	groupsErr error
	resetErr  error

	// capture args
	listScope    string
	listLimit    int
	listOffset   int
	updatedKey   string
	updatedValue json.RawMessage
	updatedScope string
	updatedBy    *uuid.UUID

	resetKey      string
	resetScope    string
	resetBy       *uuid.UUID
	resetAllScope string
	resetAllBy    *uuid.UUID
	resetAllCount int
}

func (m *mockSettingsAdminStore) CountSettings(_ context.Context, scope string) (int, error) {
	return len(m.settings), m.listErr
}

func (m *mockSettingsAdminStore) ListSettings(_ context.Context, scope string, limit, offset int) ([]model.Setting, error) {
	m.listScope = scope
	m.listLimit = limit
	m.listOffset = offset
	return m.settings, m.listErr
}

func (m *mockSettingsAdminStore) GetSetting(_ context.Context, key, _ string) (*model.Setting, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	for i := range m.settings {
		if m.settings[i].Key == key {
			return &m.settings[i], nil
		}
	}
	return nil, sql.ErrNoRows
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

func (m *mockSettingsAdminStore) GetSettingsGroups(_ context.Context) ([]SettingGroup, error) {
	return m.groups, m.groupsErr
}

func (m *mockSettingsAdminStore) ResetSetting(_ context.Context, key, scope string, updatedBy *uuid.UUID) error {
	m.resetKey = key
	m.resetScope = scope
	m.resetBy = updatedBy
	return m.resetErr
}

func (m *mockSettingsAdminStore) ResetAllSettings(_ context.Context, scope string, updatedBy *uuid.UUID) (int, error) {
	m.resetAllScope = scope
	m.resetAllBy = updatedBy
	if m.resetErr != nil {
		return 0, m.resetErr
	}
	if m.resetAllCount > 0 {
		return m.resetAllCount, nil
	}
	return len(m.schemas), nil
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

func TestAdminSettingsGetGroups(t *testing.T) {
	store := &mockSettingsAdminStore{
		groups: []SettingGroup{
			{
				ID:          "memory",
				Label:       "Memory",
				Description: "Defaults for new memories.",
				SubSections: []SettingSubSection{{Category: "memory"}},
			},
			{
				ID:                 "enrichment",
				Label:              "Enrichment",
				RequiresEnrichment: true,
				SubSections: []SettingSubSection{
					{Category: "enrichment", Label: "General"},
				},
			},
		},
	}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings?groups=true", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []SettingGroup `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resp.Data))
	}
	if resp.Data[0].ID != "memory" || resp.Data[0].SubSections[0].Category != "memory" {
		t.Errorf("unexpected first group: %+v", resp.Data[0])
	}
	if !resp.Data[1].RequiresEnrichment {
		t.Errorf("expected enrichment group to carry requires_enrichment")
	}
	if resp.Data[1].SubSections[0].Label != "General" {
		t.Errorf("expected sub-section label General, got %q", resp.Data[1].SubSections[0].Label)
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

func TestAdminSettingsUpdateSettingRejectsNonGlobalScope(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"memory.max_facts","value":2000,"scope":"project"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
	// The orphan-row write must not reach the store.
	if store.updatedKey != "" {
		t.Errorf("expected no store write, got updatedKey %q", store.updatedKey)
	}
}

func TestAdminSettingsUpdateSettingDefaultsScopeToGlobal(t *testing.T) {
	store := &mockSettingsAdminStore{}

	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	// Scope omitted entirely: the handler defaults it to global and accepts.
	body := `{"key":"memory.max_facts","value":2000}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedScope != "global" {
		t.Errorf("expected scope defaulted to global, got %q", store.updatedScope)
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

// rangeSchemas returns a fixture with a numeric setting bounded to [0, 1]
// step 0.05 — used by the range-enforcement tests below.
func rangeSchemas() []SettingSchema {
	min0 := 0.0
	max1 := 1.0
	step := 0.05
	return []SettingSchema{
		{
			Key:          "ranking.weight.similarity",
			Type:         "number",
			DefaultValue: json.RawMessage(`0.5`),
			Min:          &min0,
			Max:          &max1,
			Step:         &step,
		},
		{
			Key:          "memory.max_facts",
			Type:         "number",
			DefaultValue: json.RawMessage(`1000`),
			// No Min/Max — unbounded numeric, validation is a no-op.
		},
		{
			Key:          "enrichment.enabled",
			Type:         "boolean",
			DefaultValue: json.RawMessage(`true`),
			// Booleans bypass numeric validation entirely.
		},
	}
}

func TestAdminSettingsUpdate_RejectsBelowMinimum(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"ranking.weight.similarity","value":-0.1,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedKey != "" {
		t.Error("UpdateSetting should not be called when validation fails")
	}
	if !strings.Contains(w.Body.String(), "below schema minimum") {
		t.Errorf("error message should name the bound; got %s", w.Body.String())
	}
}

func TestAdminSettingsUpdate_RejectsAboveMaximum(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"ranking.weight.similarity","value":1.5,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedKey != "" {
		t.Error("UpdateSetting should not be called when validation fails")
	}
	if !strings.Contains(w.Body.String(), "above schema maximum") {
		t.Errorf("error message should name the bound; got %s", w.Body.String())
	}
}

func TestAdminSettingsUpdate_AcceptsBoundaryValues(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	for _, v := range []string{"0", "1", "0.5"} {
		store.updatedKey = ""
		body := `{"key":"ranking.weight.similarity","value":` + v + `,"scope":"global"}`
		req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
			UserID: uuid.New(),
			Role:   "admin",
		}))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("value %s: expected 200, got %d; body: %s", v, w.Code, w.Body.String())
		}
		if store.updatedKey != "ranking.weight.similarity" {
			t.Errorf("value %s: UpdateSetting should be called", v)
		}
	}
}

func TestAdminSettingsUpdate_AcceptsStringEncodedNumbers(t *testing.T) {
	// Some clients round-trip JSON numbers as strings; the validator
	// tolerates both shapes so it doesn't reject valid values on a
	// shape technicality.
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"ranking.weight.similarity","value":"0.75","scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
}

func TestAdminSettingsUpdate_RejectsNonNumericForNumericKey(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"ranking.weight.similarity","value":"not a number","scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
}

func TestAdminSettingsUpdate_SkipsValidationForUnboundedNumeric(t *testing.T) {
	// memory.max_facts in rangeSchemas() has no Min/Max — any numeric
	// value should pass through.
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"memory.max_facts","value":99999999,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
}

func TestAdminSettingsUpdate_SkipsValidationForUnknownKey(t *testing.T) {
	// Forward-compat: a key the schema hasn't been updated for should
	// still allow writes (otherwise rolling deploys break).
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"some.future.unregistered.key","value":42,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for unknown key (forward-compat), got %d; body: %s",
			w.Code, w.Body.String())
	}
}

func TestAdminSettingsUpdate_SkipsValidationForBooleanKey(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: rangeSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"enrichment.enabled","value":true,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
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

// TestAdminSettingsListSettingsDefaultLimitCoversRegistry pins the contract
// that GET /admin/settings with no ?limit= supplied must request enough rows
// to cover the whole schema registry. The bootstrap seeder writes one row
// per registered key (~170 today); a smaller default silently truncates the
// GET response and the UI re-renders the missing keys using the schema
// default, making operator toggles appear to do nothing even when the PUT
// succeeded. If the registry ever grows past the floor, both the handler
// default and this assertion must move together.
func TestAdminSettingsListSettingsDefaultLimitCoversRegistry(t *testing.T) {
	const minDefaultLimit = 500

	store := &mockSettingsAdminStore{}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if store.listLimit < minDefaultLimit {
		t.Fatalf("default page size regressed: handler asked store for limit=%d, "+
			"want >= %d. The seeder writes ~166 rows; a smaller default silently "+
			"truncates the GET response and breaks every setting whose key sorts "+
			"past the page boundary.", store.listLimit, minDefaultLimit)
	}
	if store.listOffset != 0 {
		t.Errorf("expected default offset 0, got %d", store.listOffset)
	}
}

// TestAdminSettingsListSettingsExplicitLimitHonored pins the contract that
// an explicit ?limit= is respected — the no-arg default is a floor, not a
// minimum, so external callers that paginate deliberately keep working.
func TestAdminSettingsListSettingsExplicitLimitHonored(t *testing.T) {
	store := &mockSettingsAdminStore{}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings?limit=25", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if store.listLimit != 25 {
		t.Fatalf("explicit ?limit=25 ignored: store received limit=%d", store.listLimit)
	}
}

// costRateSchemas registers usage.cost_rates as Type: "json" so the
// validator dispatches into validateCostRatesValue. Mirrors the layout
// of rangeSchemas() above.
func costRateSchemas() []SettingSchema {
	return []SettingSchema{
		{
			Key:          "usage.cost_rates",
			Type:         "json",
			DefaultValue: json.RawMessage(`[]`),
		},
	}
}

func TestAdminSettingsUpdate_AcceptsValidCostRates(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: costRateSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"usage.cost_rates","value":[{"key":"gpt-4o","inputCostPer1k":0.005,"outputCostPer1k":0.015}],"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedKey != "usage.cost_rates" {
		t.Errorf("UpdateSetting was not called with cost-rates key; got %q", store.updatedKey)
	}
}

func TestAdminSettingsUpdate_AcceptsEmptyCostRates(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: costRateSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"usage.cost_rates","value":[],"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{
		UserID: uuid.New(),
		Role:   "admin",
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
}

func TestAdminSettingsUpdate_RejectsCostRatesNonArray(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: costRateSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"usage.cost_rates","value":{"gpt-4o":{"in":0.005}},"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedKey != "" {
		t.Error("UpdateSetting should not be called for malformed cost-rates payload")
	}
}

func TestAdminSettingsUpdate_RejectsCostRatesNegative(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: costRateSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"usage.cost_rates","value":[{"key":"gpt-4o","inputCostPer1k":-0.1,"outputCostPer1k":0.015}],"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "negative inputCostPer1k") {
		t.Errorf("error should name the failing field; got %s", w.Body.String())
	}
}

func TestAdminSettingsUpdate_RejectsCostRatesEmptyKey(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: costRateSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"usage.cost_rates","value":[{"key":"","inputCostPer1k":0.005,"outputCostPer1k":0.015}],"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
}

// --- cost-rates GET handler ---

type mockCostRatesStore struct {
	value json.RawMessage
	err   error
}

func (m *mockCostRatesStore) GetCostRates(_ context.Context) (json.RawMessage, error) {
	return m.value, m.err
}

func TestUsageCostRates_ReturnsStoredBlob(t *testing.T) {
	store := &mockCostRatesStore{value: json.RawMessage(`[{"key":"gpt-4o","inputCostPer1k":0.005,"outputCostPer1k":0.015}]`)}
	h := NewUsageCostRatesHandler(CostRatesConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/usage/cost_rates", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "gpt-4o") {
		t.Errorf("response should contain stored rate; got %s", w.Body.String())
	}
}

func TestUsageCostRates_NoRowsReturnsEmpty(t *testing.T) {
	store := &mockCostRatesStore{err: sql.ErrNoRows}
	h := NewUsageCostRatesHandler(CostRatesConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/usage/cost_rates", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (empty fallback), got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"data":[]`) {
		t.Errorf("missing-row response should fall back to empty list; got %s", w.Body.String())
	}
}

func TestUsageCostRates_StoreErrorReturns500(t *testing.T) {
	store := &mockCostRatesStore{err: errors.New("db down")}
	h := NewUsageCostRatesHandler(CostRatesConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/usage/cost_rates", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d; body: %s", w.Code, w.Body.String())
	}
}

func TestUsageCostRates_RejectsNonGet(t *testing.T) {
	store := &mockCostRatesStore{}
	h := NewUsageCostRatesHandler(CostRatesConfig{Store: store})
	req := httptest.NewRequest(http.MethodPost, "/v1/usage/cost_rates", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminSettingsUpdate_RejectsCostRatesDuplicate(t *testing.T) {
	store := &mockSettingsAdminStore{schemas: costRateSchemas()}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"usage.cost_rates","value":[{"key":"gpt-4o","inputCostPer1k":0.005,"outputCostPer1k":0.015},{"key":"gpt-4o","inputCostPer1k":0.01,"outputCostPer1k":0.02}],"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "duplicate") {
		t.Errorf("error should mention duplicate; got %s", w.Body.String())
	}
}

// transitiveWaterSchemas is the in-process schema slice the cross-key
// validator needs to find both transitive water-mark keys for type-checking.
func transitiveWaterSchemas() []SettingSchema {
	min0 := 0.0
	max1 := 1.0
	step := 0.01
	return []SettingSchema{
		{Key: service.SettingDreamTransitiveNamespaceHighWater, Type: "number", DefaultValue: json.RawMessage(`0.95`), Min: &min0, Max: &max1, Step: &step},
		{Key: service.SettingDreamTransitiveNamespaceLowWater, Type: "number", DefaultValue: json.RawMessage(`0.80`), Min: &min0, Max: &max1, Step: &step},
	}
}

// --- reset endpoint ---

func TestAdminSettingsResetSingleKey(t *testing.T) {
	userID := uuid.New()
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
		},
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	body := `{"key":"memory.max_facts","scope":"global"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{UserID: userID, Role: "admin"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.resetKey != "memory.max_facts" {
		t.Errorf("expected resetKey memory.max_facts, got %q", store.resetKey)
	}
	if store.resetScope != "global" {
		t.Errorf("expected scope global, got %q", store.resetScope)
	}
	if store.resetBy == nil || *store.resetBy != userID {
		t.Errorf("expected resetBy %s, got %v", userID, store.resetBy)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %v", resp["status"])
	}
	if n, _ := resp["reset"].(float64); n != 1 {
		t.Errorf("expected reset=1, got %v", resp["reset"])
	}
}

func TestAdminSettingsResetRejectsNonGlobalScope(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
		},
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	body := `{"key":"memory.max_facts","scope":"project"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{UserID: uuid.New(), Role: "admin"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
	// The reset must not reach the store for a non-global scope.
	if store.resetKey != "" {
		t.Errorf("expected no store reset, got resetKey %q", store.resetKey)
	}
}

func TestAdminSettingsResetAllEmptyBody(t *testing.T) {
	userID := uuid.New()
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
			{Key: "enrichment.enabled", Type: "boolean", DefaultValue: json.RawMessage(`true`)},
		},
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", nil)
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{UserID: userID, Role: "admin"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.resetAllScope != "global" {
		t.Errorf("expected scope global default, got %q", store.resetAllScope)
	}
	if store.resetKey != "" {
		t.Errorf("expected ResetSetting not called, but resetKey=%q", store.resetKey)
	}
	if store.resetAllBy == nil || *store.resetAllBy != userID {
		t.Errorf("expected resetAllBy %s, got %v", userID, store.resetAllBy)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n, _ := resp["reset"].(float64); n != 2 {
		t.Errorf("expected reset=2 (registry size), got %v", resp["reset"])
	}
}

func TestAdminSettingsResetUnknownKey(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
		},
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	body := `{"key":"not.a.real.key","scope":"global"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.resetKey != "" {
		t.Error("ResetSetting must not be called for unknown key")
	}
	if !strings.Contains(w.Body.String(), "not registered") {
		t.Errorf("error should name the issue; got %s", w.Body.String())
	}
}

func TestAdminSettingsResetDefaultsScopeToGlobal(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
		},
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	// Body omits scope; handler must default to "global" so a bare POST is
	// the "reset everything to factory state" affordance.
	body := `{"key":"memory.max_facts"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.resetScope != "global" {
		t.Errorf("expected scope global, got %q", store.resetScope)
	}
}

// TestAdminSettingsReset_RejectsDefaultThatBreaksCrossKeyInvariant pins the
// fix for the cross-key invariant gap: resetting only one half of a
// (high_water, low_water) pair must not write a default that violates
// low_water < high_water. The PUT path enforces this; the reset path now
// also routes through validateValueAgainstSchema.
func TestAdminSettingsReset_RejectsDefaultThatBreaksCrossKeyInvariant(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: transitiveWaterSchemas(),
		// Operator has overridden high_water to 0.50; resetting low_water
		// to its default 0.80 would produce low_water (0.80) >= high_water (0.50).
		settings: []model.Setting{
			{Key: service.SettingDreamTransitiveNamespaceHighWater, Value: json.RawMessage(`0.50`), Scope: "global"},
		},
	}
	// Override the default to a value that breaks the invariant.
	for i := range store.schemas {
		if store.schemas[i].Key == service.SettingDreamTransitiveNamespaceLowWater {
			store.schemas[i].DefaultValue = json.RawMessage(`0.80`)
		}
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	body := `{"key":"dreaming.transitive.namespace_low_water","scope":"global"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.resetKey != "" {
		t.Error("ResetSetting must not be called when default would break invariant")
	}
}

// TestAdminSettingsReset_ChunkedEmptyBody pins the fix for the
// ContentLength=-1 edge case: an empty body (chunked transfer or otherwise)
// must be treated as "reset all at global", not 400.
func TestAdminSettingsReset_ChunkedEmptyBody(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
		},
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", http.NoBody)
	req.ContentLength = -1 // simulate chunked transfer
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.resetAllScope != "global" {
		t.Errorf("expected fallback to global, got %q", store.resetAllScope)
	}
}

// TestAdminSettingsReset_TrimsWhitespaceScope pins the fix for
// whitespace-only scope: it must normalize to "global", not pass through as
// a literal " " scope that silently no-ops.
func TestAdminSettingsReset_TrimsWhitespaceScope(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
		},
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	body := `{"key":"memory.max_facts","scope":"   "}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.resetScope != "global" {
		t.Errorf("expected scope global after trim, got %q", store.resetScope)
	}
}

func TestAdminSettingsResetRejectsNonPost(t *testing.T) {
	store := &mockSettingsAdminStore{}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/settings/reset", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminSettingsResetStoreError(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: []SettingSchema{
			{Key: "memory.max_facts", Type: "number", DefaultValue: json.RawMessage(`1000`)},
		},
		resetErr: errors.New("database failure"),
	}
	h := NewAdminSettingsResetHandler(SettingsAdminConfig{Store: store})

	body := `{"key":"memory.max_facts","scope":"global"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/settings/reset", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// TestAdminSettingsUpdate_RejectsHighWaterAtOrBelowLowWater proves the
// cross-key invariant: high_water must be strictly above low_water or the
// pressure-driven transitive prune cannot converge.
func TestAdminSettingsUpdate_RejectsHighWaterAtOrBelowLowWater(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: transitiveWaterSchemas(),
		settings: []model.Setting{
			{Key: service.SettingDreamTransitiveNamespaceLowWater, Value: json.RawMessage(`0.80`), Scope: "global"},
		},
	}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	// Try to set high_water to 0.75, which is below the stored low_water 0.80.
	body := `{"key":"dreaming.transitive.namespace_high_water","value":0.75,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedKey != "" {
		t.Error("UpdateSetting should not be called when cross-key invariant fails")
	}
	if !strings.Contains(w.Body.String(), "strictly less than") {
		t.Errorf("error should explain the invariant; got %s", w.Body.String())
	}
}

// TestAdminSettingsUpdate_RejectsLowWaterAtOrAboveHighWater is the mirror
// of the above, asserted on the low_water side so a PUT on either key
// catches misconfiguration.
func TestAdminSettingsUpdate_RejectsLowWaterAtOrAboveHighWater(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: transitiveWaterSchemas(),
		settings: []model.Setting{
			{Key: service.SettingDreamTransitiveNamespaceHighWater, Value: json.RawMessage(`0.95`), Scope: "global"},
		},
	}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"dreaming.transitive.namespace_low_water","value":0.95,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedKey != "" {
		t.Error("UpdateSetting should not be called when cross-key invariant fails")
	}
}

// TestAdminSettingsUpdate_AcceptsValidWaterMarks confirms the happy path
// still passes through to the store.
func TestAdminSettingsUpdate_AcceptsValidWaterMarks(t *testing.T) {
	store := &mockSettingsAdminStore{
		schemas: transitiveWaterSchemas(),
		settings: []model.Setting{
			{Key: service.SettingDreamTransitiveNamespaceLowWater, Value: json.RawMessage(`0.70`), Scope: "global"},
		},
	}
	h := NewAdminSettingsHandler(SettingsAdminConfig{Store: store})
	body := `{"key":"dreaming.transitive.namespace_high_water","value":0.90,"scope":"global"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/settings", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), &auth.AuthContext{UserID: uuid.New(), Role: "admin"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.updatedKey != service.SettingDreamTransitiveNamespaceHighWater {
		t.Errorf("UpdateSetting should fire on valid pair, key=%q", store.updatedKey)
	}
}
