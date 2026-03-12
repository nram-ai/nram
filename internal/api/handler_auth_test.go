package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock AuthUserRepo ---

type mockAuthUserRepo struct {
	user    *model.User
	getErr  error
	updated bool
}

func (m *mockAuthUserRepo) GetByEmail(_ context.Context, _ string) (*model.User, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.user, nil
}

func (m *mockAuthUserRepo) UpdateLastLogin(_ context.Context, _ uuid.UUID) error {
	m.updated = true
	return nil
}

// helper to create a hashed password for tests.
func mustHash(t *testing.T, pw string) *string {
	t.Helper()
	h, err := storage.HashPassword(pw)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return &h
}

var testJWTSecret = []byte("test-secret-key-for-auth-tests!!")

// --- Login handler tests ---

func TestLoginSuccess(t *testing.T) {
	hash := mustHash(t, "correct-password")
	userID := uuid.New()
	orgID := uuid.New()

	repo := &mockAuthUserRepo{
		user: &model.User{
			ID:           userID,
			Email:        "admin@example.com",
			DisplayName:  "Admin",
			PasswordHash: hash,
			OrgID:        orgID,
			Role:         "administrator",
		},
	}

	h := NewLoginHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(loginRequest{Email: "admin@example.com", Password: "correct-password"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp loginResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Token == "" {
		t.Fatal("expected non-empty token")
	}
	if resp.User.ID != userID {
		t.Fatalf("expected user ID %s, got %s", userID, resp.User.ID)
	}
	if resp.User.Email != "admin@example.com" {
		t.Fatalf("expected email admin@example.com, got %s", resp.User.Email)
	}
	if resp.User.Role != "administrator" {
		t.Fatalf("expected role administrator, got %s", resp.User.Role)
	}
	if !repo.updated {
		t.Fatal("expected UpdateLastLogin to be called")
	}
}

func TestLoginWrongPassword(t *testing.T) {
	hash := mustHash(t, "correct-password")
	repo := &mockAuthUserRepo{
		user: &model.User{
			ID:           uuid.New(),
			Email:        "admin@example.com",
			PasswordHash: hash,
			OrgID:        uuid.New(),
			Role:         "administrator",
		},
	}

	h := NewLoginHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(loginRequest{Email: "admin@example.com", Password: "wrong-password"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestLoginUserNotFound(t *testing.T) {
	repo := &mockAuthUserRepo{getErr: sql.ErrNoRows}

	h := NewLoginHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(loginRequest{Email: "nobody@example.com", Password: "password"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestLoginAccountDisabled(t *testing.T) {
	hash := mustHash(t, "password")
	now := time.Now()
	repo := &mockAuthUserRepo{
		user: &model.User{
			ID:           uuid.New(),
			Email:        "disabled@example.com",
			PasswordHash: hash,
			OrgID:        uuid.New(),
			Role:         "member",
			DisabledAt:   &now,
		},
	}

	h := NewLoginHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(loginRequest{Email: "disabled@example.com", Password: "password"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestLoginNoLocalCredentials(t *testing.T) {
	repo := &mockAuthUserRepo{
		user: &model.User{
			ID:           uuid.New(),
			Email:        "idp@example.com",
			PasswordHash: nil,
			OrgID:        uuid.New(),
			Role:         "member",
		},
	}

	h := NewLoginHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(loginRequest{Email: "idp@example.com", Password: "password"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestLoginEmptyFields(t *testing.T) {
	h := NewLoginHandler(AuthConfig{UserRepo: &mockAuthUserRepo{}, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(loginRequest{Email: "", Password: ""})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestLoginInvalidBody(t *testing.T) {
	h := NewLoginHandler(AuthConfig{UserRepo: &mockAuthUserRepo{}, JWTSecret: testJWTSecret})

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestLoginMethodNotAllowed(t *testing.T) {
	h := NewLoginHandler(AuthConfig{UserRepo: &mockAuthUserRepo{}, JWTSecret: testJWTSecret})

	req := httptest.NewRequest(http.MethodGet, "/v1/auth/login", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- Lookup handler tests ---

func TestLookupLocal(t *testing.T) {
	hash := mustHash(t, "password")
	repo := &mockAuthUserRepo{
		user: &model.User{
			ID:           uuid.New(),
			Email:        "local@example.com",
			PasswordHash: hash,
		},
	}

	h := NewLookupHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(lookupRequest{Email: "local@example.com"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/lookup", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp lookupResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Method != "local" {
		t.Fatalf("expected method local, got %s", resp.Method)
	}
}

func TestLookupIdP(t *testing.T) {
	repo := &mockAuthUserRepo{
		user: &model.User{
			ID:           uuid.New(),
			Email:        "idp@example.com",
			PasswordHash: nil,
		},
	}

	h := NewLookupHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(lookupRequest{Email: "idp@example.com"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/lookup", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp lookupResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Method != "idp" {
		t.Fatalf("expected method idp, got %s", resp.Method)
	}
}

func TestLookupUnknown(t *testing.T) {
	repo := &mockAuthUserRepo{getErr: sql.ErrNoRows}

	h := NewLookupHandler(AuthConfig{UserRepo: repo, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(lookupRequest{Email: "nobody@example.com"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/lookup", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp lookupResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Method != "unknown" {
		t.Fatalf("expected method unknown, got %s", resp.Method)
	}
}

func TestLookupEmptyEmail(t *testing.T) {
	h := NewLookupHandler(AuthConfig{UserRepo: &mockAuthUserRepo{}, JWTSecret: testJWTSecret})

	body, _ := json.Marshal(lookupRequest{Email: ""})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/lookup", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestLookupMethodNotAllowed(t *testing.T) {
	h := NewLookupHandler(AuthConfig{UserRepo: &mockAuthUserRepo{}, JWTSecret: testJWTSecret})

	req := httptest.NewRequest(http.MethodGet, "/v1/auth/lookup", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}
