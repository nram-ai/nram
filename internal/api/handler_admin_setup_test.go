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

// --- mock SetupStore ---

type mockSetupStore struct {
	complete    bool
	completeErr error
	setupUser   *model.User
	setupKey    string
	setupErr    error
	backend     string
}

func (m *mockSetupStore) IsSetupComplete(_ context.Context) (bool, error) {
	return m.complete, m.completeErr
}

func (m *mockSetupStore) CompleteSetup(_ context.Context, _, _ string) (*model.User, string, error) {
	return m.setupUser, m.setupKey, m.setupErr
}

func (m *mockSetupStore) Backend() string { return m.backend }

// --- tests ---

func TestSetupStatusNotComplete(t *testing.T) {
	h := NewAdminSetupStatusHandler(SetupConfig{
		Store: &mockSetupStore{
			complete: false,
			backend:  "sqlite",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/setup/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp setupStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.SetupComplete {
		t.Error("expected setup_complete to be false")
	}
	if resp.Backend != "sqlite" {
		t.Errorf("expected backend sqlite, got %q", resp.Backend)
	}
}

func TestSetupStatusComplete(t *testing.T) {
	h := NewAdminSetupStatusHandler(SetupConfig{
		Store: &mockSetupStore{
			complete: true,
			backend:  "postgres",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/setup/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp setupStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !resp.SetupComplete {
		t.Error("expected setup_complete to be true")
	}
	if resp.Backend != "postgres" {
		t.Errorf("expected backend postgres, got %q", resp.Backend)
	}
}

func TestSetupSuccess(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	userID := uuid.New()
	orgID := uuid.New()
	nsID := uuid.New()

	jwtSecret := []byte("test-secret-key-for-setup")

	h := NewAdminSetupHandler(SetupConfig{
		Store: &mockSetupStore{
			complete: false,
			backend:  "sqlite",
			setupUser: &model.User{
				ID:          userID,
				Email:       "admin@example.com",
				DisplayName: "admin@example.com",
				OrgID:       orgID,
				NamespaceID: nsID,
				Role:        "administrator",
				Settings:    json.RawMessage(`{}`),
				CreatedAt:   now,
				UpdatedAt:   now,
			},
			setupKey: "nram_k_testkey123",
		},
		JWTSecret: jwtSecret,
	})

	body := `{"email":"admin@example.com","password":"supersecret"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/setup", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	var resp setupResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.User == nil {
		t.Fatal("expected user in response")
	}
	if resp.User.ID != userID {
		t.Errorf("expected user ID %s, got %s", userID, resp.User.ID)
	}
	if resp.User.Email != "admin@example.com" {
		t.Errorf("expected email admin@example.com, got %q", resp.User.Email)
	}
	if resp.User.Role != "administrator" {
		t.Errorf("expected role administrator, got %q", resp.User.Role)
	}
	if resp.APIKey != "nram_k_testkey123" {
		t.Errorf("expected api_key nram_k_testkey123, got %q", resp.APIKey)
	}
	if resp.Token == "" {
		t.Error("expected non-empty token in response")
	}
	if resp.Message == "" {
		t.Error("expected non-empty message")
	}
}

func TestSetupAlreadyComplete(t *testing.T) {
	h := NewAdminSetupHandler(SetupConfig{
		Store: &mockSetupStore{
			complete: true,
			backend:  "sqlite",
		},
	})

	body := `{"email":"admin@example.com","password":"supersecret"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/setup", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Error.Code != "conflict" {
		t.Errorf("expected code conflict, got %q", resp.Error.Code)
	}
}

func TestSetupMissingEmail(t *testing.T) {
	h := NewAdminSetupHandler(SetupConfig{
		Store: &mockSetupStore{
			complete: false,
			backend:  "sqlite",
		},
	})

	body := `{"email":"","password":"supersecret"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/setup", bytes.NewBufferString(body))
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

func TestSetupPasswordTooShort(t *testing.T) {
	h := NewAdminSetupHandler(SetupConfig{
		Store: &mockSetupStore{
			complete: false,
			backend:  "sqlite",
		},
	})

	body := `{"email":"admin@example.com","password":"short"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/setup", bytes.NewBufferString(body))
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

func TestSetupStoreError(t *testing.T) {
	h := NewAdminSetupHandler(SetupConfig{
		Store: &mockSetupStore{
			complete: false,
			backend:  "sqlite",
			setupErr: errors.New("database write failed"),
		},
	})

	body := `{"email":"admin@example.com","password":"supersecret"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/setup", bytes.NewBufferString(body))
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

func TestSetupStatusStoreError(t *testing.T) {
	h := NewAdminSetupStatusHandler(SetupConfig{
		Store: &mockSetupStore{
			completeErr: errors.New("db down"),
			backend:     "sqlite",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/setup/status", nil)
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
