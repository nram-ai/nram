package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// fixedTimings is a deterministic SessionTimings for refresh-window tests.
type fixedTimings struct {
	ttl       time.Duration
	threshold time.Duration
}

func (f fixedTimings) TokenTTL(_ context.Context) time.Duration         { return f.ttl }
func (f fixedTimings) RefreshThreshold(_ context.Context) time.Duration { return f.threshold }

// signSessionJWTAged mints a session JWT whose IssuedAt is `age` ago and
// whose ExpiresAt is `ttl - age` from now. Uses the jwt library directly
// because GenerateSessionJWT always stamps IssuedAt = time.Now.
func signSessionJWTAged(t *testing.T, secret []byte, age, ttl time.Duration) string {
	t.Helper()
	now := time.Now().UTC()
	issued := now.Add(-age)
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uuid.New().String(),
			IssuedAt:  jwt.NewNumericDate(issued),
			ExpiresAt: jwt.NewNumericDate(issued.Add(ttl)),
			Issuer:    "nram",
		},
		Role:        RoleOrgOwner,
		OrgID:       uuid.New().String(),
		Email:       "alice@example.com",
		DisplayName: "Alice",
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(secret)
	if err != nil {
		t.Fatalf("sign session jwt: %v", err)
	}
	return signed
}

// signAudienceBoundJWTAged mints an audience-bound (OAuth-style) JWT
// missing the Email/DisplayName claims. Used to verify refresh ignores
// non-SPA tokens.
func signAudienceBoundJWTAged(t *testing.T, secret []byte, age, ttl time.Duration, audience string) string {
	t.Helper()
	now := time.Now().UTC()
	issued := now.Add(-age)
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uuid.New().String(),
			IssuedAt:  jwt.NewNumericDate(issued),
			ExpiresAt: jwt.NewNumericDate(issued.Add(ttl)),
			Issuer:    "nram",
			Audience:  jwt.ClaimStrings{audience},
		},
		Role:  RoleOrgOwner,
		OrgID: uuid.New().String(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(secret)
	if err != nil {
		t.Fatalf("sign audience-bound jwt: %v", err)
	}
	return signed
}

func TestMiddleware_RefreshesAgedSessionJWT(t *testing.T) {
	timings := fixedTimings{ttl: time.Hour, threshold: 30 * time.Minute}
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "org_owner"}, testSecret, timings)

	// Token that is 45 min old → past the 30 min threshold.
	token := signSessionJWTAged(t, testSecret, 45*time.Minute, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mw.Handler(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	refreshed := rec.Header().Get(SessionRefreshHeader)
	if refreshed == "" {
		t.Fatal("expected X-Refreshed-Token header to be set on aged session JWT")
	}
	if refreshed == token {
		t.Fatal("refreshed token must differ from the original")
	}

	// Confirm the refreshed token is itself valid: feed it back through the
	// middleware; the inner handler should see it.
	req2 := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
	req2.Header.Set("Authorization", "Bearer "+refreshed)
	rec2 := httptest.NewRecorder()
	mw.Handler(okHandler()).ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("refreshed token failed validation: %d %s", rec2.Code, rec2.Body.String())
	}
}

func TestMiddleware_DoesNotRefreshFreshSessionJWT(t *testing.T) {
	timings := fixedTimings{ttl: time.Hour, threshold: 30 * time.Minute}
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "org_owner"}, testSecret, timings)

	// Just-issued token; nowhere near the threshold.
	token := signSessionJWTAged(t, testSecret, 0, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mw.Handler(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get(SessionRefreshHeader); got != "" {
		t.Errorf("fresh JWT must not trigger refresh, got header %q", got)
	}
}

func TestMiddleware_DoesNotRefreshAudienceBoundJWT(t *testing.T) {
	timings := fixedTimings{ttl: time.Hour, threshold: 30 * time.Minute}
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "org_owner"}, testSecret, timings)

	// OAuth/MCP access token: audience-bound, no Email/DisplayName claims.
	// Aged 45 min → would trigger refresh if it were a session JWT.
	aged := signAudienceBoundJWTAged(t, testSecret, 45*time.Minute, time.Hour, "https://example.com/mcp")

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
	req.Header.Set("Authorization", "Bearer "+aged)
	req.Host = "example.com"
	rec := httptest.NewRecorder()
	mw.Handler(okHandler()).ServeHTTP(rec, req)

	// We don't care about the response code (audience may mismatch the
	// request Host); only that no refresh header was emitted.
	if got := rec.Header().Get(SessionRefreshHeader); got != "" {
		t.Errorf("audience-bound JWT must not trigger session refresh, got header %q", got)
	}
}

func TestMiddleware_NilTimingsSkipsRefresh(t *testing.T) {
	// nil SessionTimings → opt-out of refresh. Middleware still authenticates.
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "org_owner"}, testSecret, nil)

	token := signSessionJWTAged(t, testSecret, 23*time.Hour, 24*time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mw.Handler(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get(SessionRefreshHeader); got != "" {
		t.Errorf("nil timings must opt out of refresh, got header %q", got)
	}
}
