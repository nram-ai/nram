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

// GenerateJWT creates a signed JWT for the given user.
func GenerateJWT(userID uuid.UUID, role string, secret []byte, expiry time.Duration) (string, error) {
	now := time.Now().UTC()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(expiry)),
			Issuer:    "nram",
		},
		Role: role,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
