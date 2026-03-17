package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// mockAPIKeyValidator implements APIKeyValidator for testing.
type mockAPIKeyValidator struct {
	key *model.APIKey
	err error
}

func (m *mockAPIKeyValidator) Validate(_ context.Context, _ string) (*model.APIKey, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.key, nil
}

// mockUserIdentityLookup implements UserIdentityLookup for testing.
// It maps user IDs to roles; if the ID is not found it returns an error.
type mockUserIdentityLookup struct {
	roles map[uuid.UUID]string
	// fixedRole is returned for any user ID when roles is nil.
	fixedRole string
}

func (m *mockUserIdentityLookup) GetIdentityByID(_ context.Context, id uuid.UUID) (string, uuid.UUID, error) {
	if m.roles != nil {
		role, ok := m.roles[id]
		if !ok {
			return "", uuid.Nil, fmt.Errorf("user not found")
		}
		return role, uuid.Nil, nil
	}
	return m.fixedRole, uuid.Nil, nil
}

var testSecret = []byte("test-secret-key-for-jwt-signing!")

// okHandler is a simple handler that returns 200 and the user ID from context.
func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ac := FromContext(r.Context())
		if ac == nil {
			http.Error(w, "no auth context", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(ac.UserID.String()))
	})
}

func TestHandler_NoAuthorizationHeader(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandler_InvalidBearerFormat(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	for _, header := range []string{"Basic abc", "Bearer ", "Token xyz"} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", header)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("header %q: expected 401, got %d", header, rec.Code)
		}
	}
}

func TestHandler_ValidAPIKey(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()
	validator := &mockAPIKeyValidator{
		key: &model.APIKey{
			ID:     keyID,
			UserID: userID,
			Scopes: []uuid.UUID{uuid.New()},
		},
	}
	roleLookup := &mockUserIdentityLookup{roles: map[uuid.UUID]string{userID: "member"}}

	mw := NewAuthMiddleware(validator, roleLookup, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer nram_k_abc123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != userID.String() {
		t.Fatalf("expected user ID %s, got %s", userID, rec.Body.String())
	}
}

func TestHandler_InvalidAPIKey(t *testing.T) {
	validator := &mockAPIKeyValidator{
		err: storage.ErrAPIKeyNotFound,
	}

	mw := NewAuthMiddleware(validator, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer nram_k_invalid")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandler_ExpiredAPIKey(t *testing.T) {
	validator := &mockAPIKeyValidator{
		err: storage.ErrAPIKeyExpired,
	}

	mw := NewAuthMiddleware(validator, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer nram_k_expired")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandler_ValidJWT(t *testing.T) {
	userID := uuid.New()
	tokenStr, err := GenerateJWT(userID, uuid.Nil, "admin", testSecret, time.Hour)
	if err != nil {
		t.Fatalf("generate jwt: %v", err)
	}

	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != userID.String() {
		t.Fatalf("expected user ID %s, got %s", userID, rec.Body.String())
	}
}

func TestHandler_ExpiredJWT(t *testing.T) {
	userID := uuid.New()
	// Generate a JWT that expired 1 hour ago
	tokenStr, err := GenerateJWT(userID, uuid.Nil, "user", testSecret, -time.Hour)
	if err != nil {
		t.Fatalf("generate jwt: %v", err)
	}

	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandler_InvalidJWTSignature(t *testing.T) {
	userID := uuid.New()
	// Sign with a different secret
	tokenStr, err := GenerateJWT(userID, uuid.Nil, "user", []byte("wrong-secret"), time.Hour)
	if err != nil {
		t.Fatalf("generate jwt: %v", err)
	}

	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandler_GarbageJWT(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt-at-all")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestContextHelpers(t *testing.T) {
	// FromContext on empty context returns nil
	if ac := FromContext(context.Background()); ac != nil {
		t.Fatal("expected nil AuthContext from empty context")
	}

	// WithContext + FromContext round-trip
	userID := uuid.New()
	keyID := uuid.New()
	original := &AuthContext{
		UserID:   userID,
		Role:     "admin",
		APIKeyID: &keyID,
		Scopes:   []uuid.UUID{uuid.New()},
	}

	ctx := WithContext(context.Background(), original)
	recovered := FromContext(ctx)
	if recovered == nil {
		t.Fatal("expected non-nil AuthContext")
	}
	if recovered.UserID != original.UserID {
		t.Fatalf("UserID mismatch: %s != %s", recovered.UserID, original.UserID)
	}
	if recovered.Role != original.Role {
		t.Fatalf("Role mismatch: %s != %s", recovered.Role, original.Role)
	}
	if recovered.APIKeyID == nil || *recovered.APIKeyID != *original.APIKeyID {
		t.Fatal("APIKeyID mismatch")
	}
	if len(recovered.Scopes) != len(original.Scopes) {
		t.Fatalf("Scopes length mismatch: %d != %d", len(recovered.Scopes), len(original.Scopes))
	}
}

func TestHandler_APIKeyContextHasAPIKeyID(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()
	validator := &mockAPIKeyValidator{
		key: &model.APIKey{
			ID:     keyID,
			UserID: userID,
			Scopes: []uuid.UUID{},
		},
	}
	roleLookup := &mockUserIdentityLookup{roles: map[uuid.UUID]string{userID: "member"}}

	mw := NewAuthMiddleware(validator, roleLookup, testSecret)

	var captured *AuthContext
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = FromContext(r.Context())
	})

	handler := mw.Handler(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer nram_k_test")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if captured == nil {
		t.Fatal("expected AuthContext to be set")
	}
	if captured.APIKeyID == nil || *captured.APIKeyID != keyID {
		t.Fatalf("expected APIKeyID %s, got %v", keyID, captured.APIKeyID)
	}
}

func TestHandler_JWTContextHasNoAPIKeyID(t *testing.T) {
	userID := uuid.New()
	tokenStr, err := GenerateJWT(userID, uuid.Nil, "editor", testSecret, time.Hour)
	if err != nil {
		t.Fatalf("generate jwt: %v", err)
	}

	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)

	var captured *AuthContext
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = FromContext(r.Context())
	})

	handler := mw.Handler(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if captured == nil {
		t.Fatal("expected AuthContext to be set")
	}
	if captured.APIKeyID != nil {
		t.Fatal("expected nil APIKeyID for JWT auth")
	}
	if captured.Role != "editor" {
		t.Fatalf("expected role editor, got %s", captured.Role)
	}
}

func TestGenerateJWT_RoundTrip(t *testing.T) {
	userID := uuid.New()
	tokenStr, err := GenerateJWT(userID, uuid.Nil, "admin", testSecret, time.Hour)
	if err != nil {
		t.Fatalf("generate jwt: %v", err)
	}

	if tokenStr == "" {
		t.Fatal("expected non-empty token")
	}

	// Verify the token can be parsed back
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(tokenStr, claims, func(_ *jwt.Token) (interface{}, error) {
		return testSecret, nil
	})
	if err != nil {
		t.Fatalf("parse jwt: %v", err)
	}
	if !tok.Valid {
		t.Fatal("token should be valid")
	}

	sub, _ := claims.GetSubject()
	if sub != userID.String() {
		t.Fatalf("subject mismatch: %s != %s", sub, userID)
	}
	if claims.Role != "admin" {
		t.Fatalf("role mismatch: %s != admin", claims.Role)
	}
	if claims.Issuer != "nram" {
		t.Fatalf("issuer mismatch: %s != nram", claims.Issuer)
	}
}

func TestGenerateJWT_ErrorOnEmptySecret(t *testing.T) {
	// jwt.SignedString with an empty key still works with HS256 (it's valid bytes),
	// so we just verify it doesn't panic and produces a token.
	userID := uuid.New()
	tokenStr, err := GenerateJWT(userID, uuid.Nil, "user", []byte{}, time.Hour)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if tokenStr == "" {
		t.Fatal("expected non-empty token")
	}
}

// Verify that 401 responses include WWW-Authenticate header derived from the
// request Host header (base URL is always derived from the request, never configured).
func TestHandler_WWWAuthenticate_WithIssuerURL(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	// No auth header → 401 with WWW-Authenticate.
	// httptest.NewRequest sets Host: example.com so base is http://example.com.
	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	wwwAuth := rec.Header().Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Fatal("expected WWW-Authenticate header, got empty")
	}

	expected := `Bearer resource_metadata="http://example.com/.well-known/oauth-protected-resource"`
	if wwwAuth != expected {
		t.Errorf("WWW-Authenticate mismatch\n  got:  %s\n  want: %s", wwwAuth, expected)
	}
}

// Verify WWW-Authenticate also appears on invalid token 401s.
func TestHandler_WWWAuthenticate_InvalidToken(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer bad-jwt-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	wwwAuth := rec.Header().Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Fatal("expected WWW-Authenticate header on invalid token, got empty")
	}
}

// Verify JWT with wrong audience is rejected (RFC 8707 audience validation).
// httptest.NewRequest sets Host: example.com so expected audience is http://example.com/mcp.
func TestHandler_JWT_WrongAudience_Rejected(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	userID := uuid.New()
	// Generate a token with audience for a DIFFERENT server.
	wrongAudToken, err := generateJWTWithAudience(userID, uuid.Nil, "member", testSecret, time.Hour, "https://other-server.example.com/mcp")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+wrongAudToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong-audience token, got %d", rec.Code)
	}
}

// Verify JWT with correct audience passes.
// httptest.NewRequest sets Host: example.com so the correct audience is http://example.com/mcp.
func TestHandler_JWT_CorrectAudience_Accepted(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	userID := uuid.New()
	// Audience must match what baseURLFromRequest returns for Host: example.com.
	correctAudToken, err := generateJWTWithAudience(userID, uuid.Nil, "member", testSecret, time.Hour, "http://example.com/mcp")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+correctAudToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for correct-audience token, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// Verify JWT without audience claim still works (backwards compat with tokens
// issued before resource parameter was sent).
func TestHandler_JWT_NoAudience_Accepted(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	userID := uuid.New()
	noAudToken, err := GenerateJWT(userID, uuid.Nil, "member", testSecret, time.Hour)
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+noAudToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for no-audience token (backwards compat), got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// Verify that a non-ErrAPIKeyNotFound error from the validator also results in 401.
func TestHandler_APIKeyValidatorArbitraryError(t *testing.T) {
	validator := &mockAPIKeyValidator{
		err: errors.New("database connection lost"),
	}

	mw := NewAuthMiddleware(validator, &mockUserIdentityLookup{fixedRole: "member"}, testSecret)
	handler := mw.Handler(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer nram_k_whatever")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}
