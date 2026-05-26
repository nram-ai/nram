package auth

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// SessionRefreshHeader is the response header that carries a freshly-issued
// session JWT when the auth middleware decides to slide the session forward.
// The SPA client reads it and rotates localStorage.
const SessionRefreshHeader = "X-Refreshed-Token"

// DefaultSessionTokenTTL is the fallback session-JWT lifetime applied when
// no SessionTimings is wired (tests, ad-hoc callers). Production wiring
// supplies a SessionTimings backed by the settings registry, which
// overrides this. The default of 24h matches the long-standing behavior of
// the password-login flow before the registry was introduced.
const DefaultSessionTokenTTL = 24 * time.Hour

// DefaultSessionRefreshThreshold is the fallback refresh threshold applied
// when no SessionTimings is wired. Half DefaultSessionTokenTTL, so an
// active user (making requests far more often than this) always carries a
// token nowhere near expiry.
const DefaultSessionRefreshThreshold = DefaultSessionTokenTTL / 2

// SessionTimings is the runtime source of session-JWT TTL and refresh
// threshold. Implementations resolve through the settings registry so
// operators can hot-tune session lifetime without a redeploy. See keys
// service.SettingAuthSessionTokenTTLSeconds and
// service.SettingAuthSessionRefreshThresholdSeconds.
//
// Both methods are called per request — implementations should cache reads
// (the settings service has its own short-TTL cache, so a thin pass-through
// is fine).
type SessionTimings interface {
	TokenTTL(ctx context.Context) time.Duration
	RefreshThreshold(ctx context.Context) time.Duration
}

// ResolveTokenTTL reads the configured session-JWT lifetime, falling back
// to DefaultSessionTokenTTL when timings is nil or returns a non-positive
// value (a misconfigured registry must never collapse the session to zero
// lifetime).
func ResolveTokenTTL(ctx context.Context, timings SessionTimings) time.Duration {
	if timings == nil {
		return DefaultSessionTokenTTL
	}
	if ttl := timings.TokenTTL(ctx); ttl > 0 {
		return ttl
	}
	return DefaultSessionTokenTTL
}

// ResolveRefreshThreshold reads the configured refresh threshold, falling
// back to DefaultSessionRefreshThreshold under the same conditions as
// ResolveTokenTTL.
func ResolveRefreshThreshold(ctx context.Context, timings SessionTimings) time.Duration {
	if timings == nil {
		return DefaultSessionRefreshThreshold
	}
	if t := timings.RefreshThreshold(ctx); t > 0 {
		return t
	}
	return DefaultSessionRefreshThreshold
}

// APIKeyValidator defines the interface for validating API keys.
// This allows the middleware to be tested with mock implementations.
type APIKeyValidator interface {
	Validate(ctx context.Context, rawKey string) (*model.APIKey, error)
}

// UserIdentityLookup resolves the role and org ID for an active user by ID.
// It must return an error if the user is disabled or not found.
type UserIdentityLookup interface {
	GetIdentityByID(ctx context.Context, id uuid.UUID) (role string, orgID uuid.UUID, err error)
}

// AuthContext holds the authenticated identity extracted from a request.
type AuthContext struct {
	UserID   uuid.UUID
	OrgID    uuid.UUID
	Role     string
	APIKeyID *uuid.UUID // non-nil when authenticated via API key
	Scopes   []uuid.UUID
}

type contextKey int

const authContextKey contextKey = 0

// FromContext extracts the AuthContext from a request context.
// Returns nil if no authentication info is present.
func FromContext(ctx context.Context) *AuthContext {
	ac, _ := ctx.Value(authContextKey).(*AuthContext)
	return ac
}

// WithContext stores an AuthContext in the given context.
func WithContext(ctx context.Context, ac *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, ac)
}

// Claims defines the JWT claims used by nram.
type Claims struct {
	jwt.RegisteredClaims
	Role        string `json:"role"`
	OrgID       string `json:"org_id,omitempty"`
	Email       string `json:"email,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

// AuthMiddleware validates Bearer tokens from the Authorization header.
// Tokens with the "nram_k_" prefix are validated as API keys; all others
// are parsed as JWTs signed with HMAC-SHA256.
type AuthMiddleware struct {
	apiKeyValidator    APIKeyValidator
	userIdentityLookup UserIdentityLookup
	jwtSecret          []byte
	timings            SessionTimings
}

// NewAuthMiddleware creates a new AuthMiddleware with the given dependencies.
// timings drives the sliding-refresh logic for SPA session JWTs; pass nil to
// disable refresh entirely (audience-bound and API-key flows are unaffected).
func NewAuthMiddleware(apiKeyValidator APIKeyValidator, userIdentityLookup UserIdentityLookup, jwtSecret []byte, timings SessionTimings) *AuthMiddleware {
	return &AuthMiddleware{
		apiKeyValidator:    apiKeyValidator,
		userIdentityLookup: userIdentityLookup,
		jwtSecret:          jwtSecret,
		timings:            timings,
	}
}

// Handler returns an http.Handler middleware that authenticates requests.
func (m *AuthMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeUnauthorized(w, r, "missing authorization header")
			return
		}

		token, ok := strings.CutPrefix(authHeader, "Bearer ")
		if !ok || token == "" {
			writeUnauthorized(w, r, "invalid authorization header format")
			return
		}

		var ac *AuthContext
		var refreshed string
		var err error

		if strings.HasPrefix(token, "nram_k_") {
			ac, err = m.validateAPIKey(r.Context(), token)
		} else {
			ac, refreshed, err = m.validateJWT(r, token)
		}

		if err != nil {
			writeUnauthorized(w, r, err.Error())
			return
		}

		if refreshed != "" {
			w.Header().Set(SessionRefreshHeader, refreshed)
		}

		next.ServeHTTP(w, r.WithContext(WithContext(r.Context(), ac)))
	})
}

// writeUnauthorized writes a 401 response with a WWW-Authenticate header
// that points MCP clients to the OAuth protected resource metadata endpoint
// for auto-discovery per the MCP auth specification.
func writeUnauthorized(w http.ResponseWriter, r *http.Request, msg string) {
	base := baseURLFromRequest(r)
	if base != "" {
		w.Header().Set("WWW-Authenticate",
			fmt.Sprintf(`Bearer resource_metadata="%s/.well-known/oauth-protected-resource"`, base))
	}
	http.Error(w, msg, http.StatusUnauthorized)
}

// requestIsSecure returns true if the request is over TLS or behind an HTTPS proxy.
func requestIsSecure(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
}

// baseURL derives the external base URL from the request's Host header.
// The Host header includes the port when non-standard (e.g. "localhost:8674").
// X-Forwarded-Proto is respected for TLS detection behind reverse proxies.
func baseURLFromRequest(r *http.Request) string {
	if r == nil || r.Host == "" {
		return ""
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
		scheme = fwd
	}
	return scheme + "://" + r.Host
}

func (m *AuthMiddleware) validateAPIKey(ctx context.Context, rawKey string) (*AuthContext, error) {
	key, err := m.apiKeyValidator.Validate(ctx, rawKey)
	if err != nil {
		return nil, fmt.Errorf("invalid api key: %w", err)
	}

	role, orgID, err := m.userIdentityLookup.GetIdentityByID(ctx, key.UserID)
	if err != nil {
		// User is disabled or not found — treat as unauthorized.
		return nil, fmt.Errorf("api key user unavailable: %w", err)
	}

	keyID := key.ID
	return &AuthContext{
		UserID:   key.UserID,
		OrgID:    orgID,
		Role:     role,
		APIKeyID: &keyID,
		Scopes:   key.Scopes,
	}, nil
}

func (m *AuthMiddleware) validateJWT(r *http.Request, tokenStr string) (*AuthContext, string, error) {
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.jwtSecret, nil
	})
	if err != nil {
		return nil, "", fmt.Errorf("invalid jwt: %w", err)
	}
	if !tok.Valid {
		return nil, "", fmt.Errorf("invalid jwt token")
	}

	// RFC 8707 / MCP spec: "MCP servers MUST validate that access tokens were
	// issued specifically for them as the intended audience."
	// Derive expected audience from the request Host header.
	base := baseURLFromRequest(r)
	if base != "" {
		expectedAudience := base + "/mcp"
		aud, _ := claims.GetAudience()
		if len(aud) > 0 && !containsAudience(aud, expectedAudience) {
			return nil, "", fmt.Errorf("token audience %v does not include this server (%s)", aud, expectedAudience)
		}
	}

	sub, err := claims.GetSubject()
	if err != nil || sub == "" {
		return nil, "", fmt.Errorf("jwt missing subject")
	}

	userID, err := uuid.Parse(sub)
	if err != nil {
		return nil, "", fmt.Errorf("jwt subject is not a valid uuid: %w", err)
	}

	var orgID uuid.UUID
	if claims.OrgID != "" {
		orgID, err = uuid.Parse(claims.OrgID)
		if err != nil {
			return nil, "", fmt.Errorf("jwt org_id is not a valid uuid: %w", err)
		}
	}

	ac := &AuthContext{
		UserID: userID,
		OrgID:  orgID,
		Role:   claims.Role,
	}

	refreshed, err := m.maybeRefreshSessionJWT(r.Context(), claims, userID, orgID)
	if err != nil {
		// A refresh-side error never invalidates the request — return an
		// empty hint. The original token is still valid; the next request
		// will simply try to refresh again.
		return ac, "", nil
	}
	return ac, refreshed, nil
}

// maybeRefreshSessionJWT decides whether the in-flight token is a SPA
// session JWT past its refresh threshold and, if so, mints a fresh one.
// Returns the new token (or empty string when no refresh is warranted).
//
// A SPA session JWT is identified by: no audience claim (audience-bound
// tokens are OAuth/MCP access tokens with their own refresh-token grant)
// AND populated Email + DisplayName claims (only GenerateSessionJWT writes
// these — GenerateJWT does not).
func (m *AuthMiddleware) maybeRefreshSessionJWT(ctx context.Context, claims *Claims, userID, orgID uuid.UUID) (string, error) {
	// Refresh is opt-in: callers that don't wire SessionTimings (tests,
	// ad-hoc handlers) get the legacy fixed-TTL behavior.
	if m.timings == nil {
		return "", nil
	}
	if aud, _ := claims.GetAudience(); len(aud) > 0 {
		return "", nil
	}
	if claims.Email == "" || claims.DisplayName == "" {
		return "", nil
	}
	if claims.IssuedAt == nil {
		return "", nil
	}
	threshold := ResolveRefreshThreshold(ctx, m.timings)
	if time.Since(claims.IssuedAt.Time) < threshold {
		return "", nil
	}
	ttl := ResolveTokenTTL(ctx, m.timings)
	return GenerateSessionJWT(userID, orgID, claims.Role, claims.Email, claims.DisplayName, m.jwtSecret, ttl)
}

// containsAudience checks if an audience list contains the expected value.
func containsAudience(aud jwt.ClaimStrings, expected string) bool {
	return slices.Contains(aud, expected)
}

// GenerateJWT creates a signed JWT for the given user without an audience claim.
// Use generateJWTWithAudience when an RFC 8707 resource indicator must be bound.
func GenerateJWT(userID uuid.UUID, orgID uuid.UUID, role string, secret []byte, expiry time.Duration) (string, error) {
	return generateJWTWithAudience(userID, orgID, role, secret, expiry, "")
}

// GenerateSessionJWT creates a signed JWT that includes user profile claims
// (email, display_name) so the SPA can bootstrap session info from the token
// alone (e.g. after an IdP callback redirect).
func GenerateSessionJWT(userID uuid.UUID, orgID uuid.UUID, role, email, displayName string, secret []byte, expiry time.Duration) (string, error) {
	now := time.Now().UTC()
	var orgIDStr string
	if orgID != uuid.Nil {
		orgIDStr = orgID.String()
	}
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(expiry)),
			Issuer:    "nram",
		},
		Role:        role,
		OrgID:       orgIDStr,
		Email:       email,
		DisplayName: displayName,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

// generateJWTWithAudience creates a signed JWT. When resource is non-empty it
// is set as the sole audience claim (RFC 8707 §2).
func generateJWTWithAudience(userID uuid.UUID, orgID uuid.UUID, role string, secret []byte, expiry time.Duration, resource string) (string, error) {
	now := time.Now().UTC()
	reg := jwt.RegisteredClaims{
		Subject:   userID.String(),
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(expiry)),
		Issuer:    "nram",
	}
	if resource != "" {
		reg.Audience = jwt.ClaimStrings{resource}
	}
	var orgIDStr string
	if orgID != uuid.Nil {
		orgIDStr = orgID.String()
	}
	claims := Claims{
		RegisteredClaims: reg,
		Role:             role,
		OrgID:            orgIDStr,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
