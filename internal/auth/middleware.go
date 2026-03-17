package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// APIKeyValidator defines the interface for validating API keys.
// This allows the middleware to be tested with mock implementations.
type APIKeyValidator interface {
	Validate(ctx context.Context, rawKey string) (*model.APIKey, error)
}

// AuthContext holds the authenticated identity extracted from a request.
type AuthContext struct {
	UserID   uuid.UUID
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
	Role string `json:"role"`
}

// AuthMiddleware validates Bearer tokens from the Authorization header.
// Tokens with the "nram_k_" prefix are validated as API keys; all others
// are parsed as JWTs signed with HMAC-SHA256.
type AuthMiddleware struct {
	apiKeyValidator APIKeyValidator
	jwtSecret       []byte
	issuerURL       string // OAuth issuer URL for WWW-Authenticate header
}

// NewAuthMiddleware creates a new AuthMiddleware with the given dependencies.
// issuerURL is optional — when set, 401 responses include a WWW-Authenticate
// header with the resource_metadata URI so MCP clients can auto-discover OAuth.
func NewAuthMiddleware(apiKeyValidator APIKeyValidator, jwtSecret []byte, opts ...AuthMiddlewareOption) *AuthMiddleware {
	m := &AuthMiddleware{
		apiKeyValidator: apiKeyValidator,
		jwtSecret:       jwtSecret,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// AuthMiddlewareOption configures optional AuthMiddleware behaviour.
type AuthMiddlewareOption func(*AuthMiddleware)

// WithIssuerURL sets the OAuth issuer URL used to build WWW-Authenticate headers.
func WithIssuerURL(url string) AuthMiddlewareOption {
	return func(m *AuthMiddleware) { m.issuerURL = url }
}

// Handler returns an http.Handler middleware that authenticates requests.
func (m *AuthMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			m.writeUnauthorized(w, "missing authorization header")
			return
		}

		token, ok := strings.CutPrefix(authHeader, "Bearer ")
		if !ok || token == "" {
			m.writeUnauthorized(w, "invalid authorization header format")
			return
		}

		var ac *AuthContext
		var err error

		if strings.HasPrefix(token, "nram_k_") {
			ac, err = m.validateAPIKey(r.Context(), token)
		} else {
			ac, err = m.validateJWT(token)
		}

		if err != nil {
			m.writeUnauthorized(w, err.Error())
			return
		}

		next.ServeHTTP(w, r.WithContext(WithContext(r.Context(), ac)))
	})
}

// writeUnauthorized writes a 401 response with a WWW-Authenticate header
// that points MCP clients to the OAuth protected resource metadata endpoint
// for auto-discovery per the MCP auth specification.
func (m *AuthMiddleware) writeUnauthorized(w http.ResponseWriter, msg string) {
	if m.issuerURL != "" {
		w.Header().Set("WWW-Authenticate",
			fmt.Sprintf(`Bearer resource_metadata="%s/.well-known/oauth-protected-resource"`, m.issuerURL))
	}
	http.Error(w, msg, http.StatusUnauthorized)
}

func (m *AuthMiddleware) validateAPIKey(ctx context.Context, rawKey string) (*AuthContext, error) {
	key, err := m.apiKeyValidator.Validate(ctx, rawKey)
	if err != nil {
		return nil, fmt.Errorf("invalid api key: %w", err)
	}

	keyID := key.ID
	return &AuthContext{
		UserID:   key.UserID,
		Role:     "", // API keys do not carry a role; callers should resolve from user record if needed
		APIKeyID: &keyID,
		Scopes:   key.Scopes,
	}, nil
}

func (m *AuthMiddleware) validateJWT(tokenStr string) (*AuthContext, error) {
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.jwtSecret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid jwt: %w", err)
	}
	if !tok.Valid {
		return nil, fmt.Errorf("invalid jwt token")
	}

	// RFC 8707 / MCP spec: "MCP servers MUST validate that access tokens were
	// issued specifically for them as the intended audience."
	// When the token carries an audience claim AND this server knows its own
	// resource URI, verify the audience includes this server.
	if m.issuerURL != "" {
		expectedAudience := m.issuerURL + "/mcp"
		aud, _ := claims.GetAudience()
		if len(aud) > 0 && !containsAudience(aud, expectedAudience) {
			return nil, fmt.Errorf("token audience %v does not include this server (%s)", aud, expectedAudience)
		}
	}

	sub, err := claims.GetSubject()
	if err != nil || sub == "" {
		return nil, fmt.Errorf("jwt missing subject")
	}

	userID, err := uuid.Parse(sub)
	if err != nil {
		return nil, fmt.Errorf("jwt subject is not a valid uuid: %w", err)
	}

	return &AuthContext{
		UserID: userID,
		Role:   claims.Role,
	}, nil
}

// containsAudience checks if an audience list contains the expected value.
func containsAudience(aud jwt.ClaimStrings, expected string) bool {
	for _, a := range aud {
		if a == expected {
			return true
		}
	}
	return false
}

// GenerateJWT creates a signed JWT for the given user without an audience claim.
// Use generateJWTWithAudience when an RFC 8707 resource indicator must be bound.
func GenerateJWT(userID uuid.UUID, role string, secret []byte, expiry time.Duration) (string, error) {
	return generateJWTWithAudience(userID, role, secret, expiry, "")
}

// generateJWTWithAudience creates a signed JWT. When resource is non-empty it
// is set as the sole audience claim (RFC 8707 §2).
func generateJWTWithAudience(userID uuid.UUID, role string, secret []byte, expiry time.Duration, resource string) (string, error) {
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
	claims := Claims{
		RegisteredClaims: reg,
		Role:             role,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
