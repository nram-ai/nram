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

// APIKeyBearerPrefix and ShareTokenBearerPrefix are the wire-format prefixes
// the middleware uses to dispatch Bearer credentials. The share-token storage
// layer exposes ShareTokenWirePrefix; both must stay in lockstep — if the
// wire format changes, update both constants.
const (
	APIKeyBearerPrefix     = "nram_k_"
	ShareTokenBearerPrefix = "nram_s_"
)

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

// ShareTokenValidator validates a raw nram_s_<secret> credential.
// Implementations reject one-shot shares whose ConsumedAt is set; the
// middleware and the consent flow both rely on that guard.
type ShareTokenValidator interface {
	Resolve(ctx context.Context, rawSecret string) (*model.ShareToken, []model.ShareTokenGrant, error)
}

// ShareTokenLookup hydrates a share + grants from the DB by ID. Used on the
// OAuth-issued JWT path, where the access token carries share_token_id and
// the middleware must re-resolve the current grant state on every request
// (so owner edits take effect immediately without a token refresh).
type ShareTokenLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.ShareToken, error)
	ListGrants(ctx context.Context, shareID uuid.UUID) ([]model.ShareTokenGrant, error)
}

// UserIdentityLookup resolves the role and org ID for an active user by ID.
// It must return an error if the user is disabled or not found.
type UserIdentityLookup interface {
	GetIdentityByID(ctx context.Context, id uuid.UUID) (role string, orgID uuid.UUID, err error)
}

// AuthContext holds the authenticated identity extracted from a request.
type AuthContext struct {
	UserID       uuid.UUID
	OrgID        uuid.UUID
	Role         string
	APIKeyID     *uuid.UUID // non-nil when authenticated via API key
	ShareTokenID *uuid.UUID // non-nil when authenticated via share token (bearer-direct OR OAuth-bound)
	Scopes       []uuid.UUID
	// ShareGrants is the current project-permission set when ShareTokenID is
	// non-nil. Empty for non-share-token authentication. Populated per request
	// from share_token_grants so owner edits take effect on the recipient's
	// next call.
	ShareGrants []model.ProjectGrant
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
//
// ShareTokenID is set on access tokens issued through the share-paste consent
// flow. The wire format is the share's UUID as a hex string; the middleware
// looks up share_tokens + share_token_grants on every request so owner edits
// take effect immediately without a token refresh.
type Claims struct {
	jwt.RegisteredClaims
	Role         string `json:"role"`
	OrgID        string `json:"org_id,omitempty"`
	Email        string `json:"email,omitempty"`
	DisplayName  string `json:"display_name,omitempty"`
	ShareTokenID string `json:"stid,omitempty"`
}

// AuthMiddleware validates Bearer tokens from the Authorization header.
// Tokens with the "nram_k_" prefix are validated as API keys; tokens with the
// "nram_s_" prefix are validated as share tokens (bearer-direct path); all
// others are parsed as JWTs signed with HMAC-SHA256.
//
// shareTokenValidator and shareTokenLookup are optional. When nil the
// nram_s_ branch is rejected as an unknown credential format and the
// share_token_id claim on JWTs is silently ignored (the request authenticates
// as the underlying user without scoped grants). Tests can pass nil; production
// wiring supplies both.
type AuthMiddleware struct {
	apiKeyValidator     APIKeyValidator
	shareTokenValidator ShareTokenValidator
	shareTokenLookup    ShareTokenLookup
	userIdentityLookup  UserIdentityLookup
	jwtSecret           []byte
	timings             SessionTimings
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

// WithShareTokens wires share-token validation paths onto the middleware.
// Returns the receiver to support fluent construction in the application
// bootstrap (which builds the middleware before the share-token service
// exists due to the existing user/api-key wiring order).
func (m *AuthMiddleware) WithShareTokens(validator ShareTokenValidator, lookup ShareTokenLookup) *AuthMiddleware {
	m.shareTokenValidator = validator
	m.shareTokenLookup = lookup
	return m
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

		switch {
		case strings.HasPrefix(token, APIKeyBearerPrefix):
			ac, err = m.validateAPIKey(r.Context(), token)
		case strings.HasPrefix(token, ShareTokenBearerPrefix):
			ac, err = m.validateShareToken(r.Context(), token)
		default:
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

// validateShareToken handles the bearer-direct path for nram_s_<secret>. The
// recipient identity collapses to the owner's identity (share-bearer activity
// logs as the owner with share_token_id annotation); the AuthContext carries
// the share id + grant set so downstream MCP handlers can enforce the
// per-tier matrix and project allowlist.
//
// Returns an unauthorized error if the share-token validator is unwired,
// the secret does not parse, the share is revoked/expired, or the share is
// one-shot and already consumed (bearer-direct on a consumed one-shot is a
// hard reject per the 2026-05-27 design decision; the recipient must obtain
// a fresh share or use the OAuth flow).
func (m *AuthMiddleware) validateShareToken(ctx context.Context, rawSecret string) (*AuthContext, error) {
	if m.shareTokenValidator == nil {
		return nil, fmt.Errorf("share-token authentication not configured")
	}

	share, grants, err := m.shareTokenValidator.Resolve(ctx, rawSecret)
	if err != nil {
		return nil, fmt.Errorf("invalid share token: %w", err)
	}
	if len(grants) == 0 {
		return nil, fmt.Errorf("share token has no active project grants")
	}

	role, orgID, err := m.userIdentityLookup.GetIdentityByID(ctx, share.OwnerUserID)
	if err != nil {
		return nil, fmt.Errorf("share-token owner unavailable: %w", err)
	}

	shareID := share.ID
	projectGrants := make([]model.ProjectGrant, 0, len(grants))
	for _, g := range grants {
		projectGrants = append(projectGrants, model.ProjectGrant{
			ProjectID:  g.ProjectID,
			Permission: g.Permission,
		})
	}

	return &AuthContext{
		UserID:       share.OwnerUserID,
		OrgID:        orgID,
		Role:         role,
		ShareTokenID: &shareID,
		ShareGrants:  projectGrants,
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

	// OAuth-issued access tokens carrying a share_token_id claim re-resolve
	// the share + grants from the DB on every request so owner edits take
	// effect without a refresh, and so revocation kills the session at the
	// next call (instead of waiting for JWT expiry).
	//
	// If the JWT carries stid but the lookup is unwired, REJECT the token
	// rather than silently authenticating as the unscoped owner. A misconfigured
	// deployment must never collapse a share-scoped credential into full
	// account access.
	if claims.ShareTokenID != "" {
		if m.shareTokenLookup == nil {
			return nil, "", fmt.Errorf("share-scoped jwt presented but share-token lookup is not configured")
		}
		shareID, parseErr := uuid.Parse(claims.ShareTokenID)
		if parseErr != nil {
			return nil, "", fmt.Errorf("jwt share_token_id is not a valid uuid: %w", parseErr)
		}
		share, lookupErr := m.shareTokenLookup.GetByID(r.Context(), shareID)
		if lookupErr != nil {
			return nil, "", fmt.Errorf("share token unavailable: %w", lookupErr)
		}
		if !share.Active(time.Now().UTC()) {
			return nil, "", fmt.Errorf("share token no longer active")
		}
		grants, grantErr := m.shareTokenLookup.ListGrants(r.Context(), shareID)
		if grantErr != nil {
			return nil, "", fmt.Errorf("share token grants lookup: %w", grantErr)
		}
		// A share whose project grants have all been cascade-deleted (every
		// project the share covered was deleted) is effectively dead. Reject
		// the request so the recipient sees an unambiguous 401 instead of
		// per-tool "not authorized" errors on every call, and so the share
		// stops appearing usable.
		if len(grants) == 0 {
			return nil, "", fmt.Errorf("share token has no active project grants")
		}
		projectGrants := make([]model.ProjectGrant, 0, len(grants))
		for _, g := range grants {
			projectGrants = append(projectGrants, model.ProjectGrant{
				ProjectID:  g.ProjectID,
				Permission: g.Permission,
			})
		}
		ac.ShareTokenID = &shareID
		ac.ShareGrants = projectGrants
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
	return generateJWTWithAudience(userID, orgID, role, secret, expiry, "", nil)
}

// GenerateShareScopedJWT creates a signed JWT that carries a share_token_id
// claim. The middleware uses this claim to re-resolve the share + grants on
// every request, so owner edits and revocation take effect without a token
// refresh. Use only on the share-paste OAuth mint path; the account-holder
// path passes shareTokenID=nil to GenerateJWT or generateJWTWithAudience.
func GenerateShareScopedJWT(userID, orgID uuid.UUID, role string, secret []byte, expiry time.Duration, resource string, shareTokenID *uuid.UUID) (string, error) {
	return generateJWTWithAudience(userID, orgID, role, secret, expiry, resource, shareTokenID)
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
// is set as the sole audience claim (RFC 8707 §2). When shareTokenID is
// non-nil it is set as the stid claim so the middleware can scope the
// caller's identity to the share's grant set.
func generateJWTWithAudience(userID uuid.UUID, orgID uuid.UUID, role string, secret []byte, expiry time.Duration, resource string, shareTokenID *uuid.UUID) (string, error) {
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
	var stid string
	if shareTokenID != nil {
		stid = shareTokenID.String()
	}
	claims := Claims{
		RegisteredClaims: reg,
		Role:             role,
		OrgID:            orgIDStr,
		ShareTokenID:     stid,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
