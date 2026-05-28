package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

const (
	authCodeExpiry          = 10 * time.Minute
	accessTokenExpiry       = 1 * time.Hour
	refreshTokenExpiry      = 30 * 24 * time.Hour
	codeChallengeMethodS256 = "S256"
)

// ShareTokenResolver is the subset of *service.ShareTokenService needed by
// the consent flow and the token-refresh re-validation. Defined as an
// interface so the auth package does not depend on internal/service (which
// depends on storage, which depends on auth — a direct import would cycle).
type ShareTokenResolver interface {
	Resolve(ctx context.Context, rawSecret string) (*model.ShareToken, []model.ShareTokenGrant, error)
	MarkConsumed(ctx context.Context, shareID uuid.UUID) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.ShareToken, error)
}

// ProjectByIDLookup resolves a project for the consent screen's grants
// display. Returning ErrProjectNotFound (or any error) collapses to a
// generic "project unavailable" row so the consent screen still renders.
type ProjectByIDLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// OAuthServer implements OAuth 2.0 Authorization Code + PKCE (RFC 7636),
// Dynamic Client Registration (RFC 7591), and Server Metadata (RFC 8414).
type OAuthServer struct {
	oauthRepo     *storage.OAuthRepo
	userRepo      *storage.UserRepo
	jwtSecret     []byte
	shareTokens   ShareTokenResolver // optional; nil disables the share-paste consent path
	projectLookup ProjectByIDLookup  // optional; nil collapses grant rows to "(project unavailable)"
}

// NewOAuthServer creates a new OAuthServer with the given dependencies.
func NewOAuthServer(oauthRepo *storage.OAuthRepo, userRepo *storage.UserRepo, jwtSecret []byte) *OAuthServer {
	return &OAuthServer{
		oauthRepo: oauthRepo,
		userRepo:  userRepo,
		jwtSecret: jwtSecret,
	}
}

// WithShareTokens wires the share-paste consent path. Returns the receiver
// for fluent construction.
func (s *OAuthServer) WithShareTokens(resolver ShareTokenResolver, projects ProjectByIDLookup) *OAuthServer {
	s.shareTokens = resolver
	s.projectLookup = projects
	return s
}

// serverMetadata is the response for RFC 8414 server metadata.
type serverMetadata struct {
	Issuer                            string   `json:"issuer"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	RegistrationEndpoint              string   `json:"registration_endpoint"`
	UserinfoEndpoint                  string   `json:"userinfo_endpoint"`
	ResponseTypesSupported            []string `json:"response_types_supported"`
	GrantTypesSupported               []string `json:"grant_types_supported"`
	CodeChallengeMethodsSupported     []string `json:"code_challenge_methods_supported"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported"`
}

// MetadataHandler returns the RFC 8414 OAuth Authorization Server Metadata.
func (s *OAuthServer) MetadataHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		base := baseURLFromRequest(r)
		meta := serverMetadata{
			Issuer:                            base,
			AuthorizationEndpoint:             base + "/authorize",
			TokenEndpoint:                     base + "/token",
			RegistrationEndpoint:              base + "/register",
			UserinfoEndpoint:                  base + "/userinfo",
			ResponseTypesSupported:            []string{"code"},
			GrantTypesSupported:               []string{"authorization_code", "refresh_token"},
			CodeChallengeMethodsSupported:     []string{codeChallengeMethodS256},
			TokenEndpointAuthMethodsSupported: []string{"client_secret_post", "none"},
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(meta)
	}
}

// clientRegistrationRequest is the request body for RFC 7591 dynamic client registration.
type clientRegistrationRequest struct {
	ClientName   string   `json:"client_name"`
	RedirectURIs []string `json:"redirect_uris"`
	GrantTypes   []string `json:"grant_types"`
}

// clientRegistrationResponse is the response body for RFC 7591 dynamic client registration.
type clientRegistrationResponse struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	ClientName   string   `json:"client_name"`
	RedirectURIs []string `json:"redirect_uris"`
	GrantTypes   []string `json:"grant_types"`
}

// RegisterClientHandler handles POST requests for RFC 7591 dynamic client registration.
func (s *OAuthServer) RegisterClientHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeOAuthError(w, http.StatusMethodNotAllowed, "invalid_request", "method not allowed")
			return
		}

		var req clientRegistrationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
			return
		}

		if req.ClientName == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "client_name is required")
			return
		}

		if len(req.RedirectURIs) == 0 {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "redirect_uris is required")
			return
		}

		if len(req.GrantTypes) == 0 {
			req.GrantTypes = []string{"authorization_code"}
		}

		clientID := uuid.New().String()
		rawSecret := generateClientSecret()
		hashedSecret := hashSecret(rawSecret)

		client := &model.OAuthClient{
			ClientID:       clientID,
			ClientSecret:   &hashedSecret,
			Name:           req.ClientName,
			RedirectURIs:   req.RedirectURIs,
			GrantTypes:     req.GrantTypes,
			AutoRegistered: true,
		}

		if err := s.oauthRepo.CreateClient(r.Context(), client); err != nil {
			writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to register client")
			return
		}

		resp := clientRegistrationResponse{
			ClientID:     clientID,
			ClientSecret: rawSecret,
			ClientName:   req.ClientName,
			RedirectURIs: req.RedirectURIs,
			GrantTypes:   req.GrantTypes,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(resp)
	}
}

// AuthorizeHandler handles POST submissions to the OAuth authorization
// endpoint. The consent screen itself is now a React SPA page served by
// the UI fallback at GET /authorize; the page calls
// /v1/oauth/authorize/context to validate the OAuth request and learn its
// rendering context, then form-POSTs back to /authorize with the user's
// decision. This handler processes that submission and mints the
// authorization code. The router wires this only on POST, so no method
// guard is needed here.
func (s *OAuthServer) AuthorizeHandler() http.HandlerFunc {
	return s.handleAuthorizePOST
}

// tokenRequest represents the POST body for the /token endpoint.
type tokenRequest struct {
	GrantType    string `json:"grant_type"`
	Code         string `json:"code"`
	RedirectURI  string `json:"redirect_uri"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	CodeVerifier string `json:"code_verifier"`
	RefreshToken string `json:"refresh_token"`
	// Resource is the RFC 8707 resource indicator sent by the client during
	// token exchange. When present it must match the resource recorded in the
	// authorization code.
	Resource string `json:"resource"`
}

// tokenResponse is the response for the /token endpoint.
type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// TokenHandler handles POST requests for the OAuth token endpoint.
// Supports grant_type=authorization_code (with PKCE) and grant_type=refresh_token.
func (s *OAuthServer) TokenHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeOAuthError(w, http.StatusMethodNotAllowed, "invalid_request", "method not allowed")
			return
		}

		// Support both form-encoded and JSON
		var req tokenRequest
		contentType := r.Header.Get("Content-Type")
		if strings.HasPrefix(contentType, "application/x-www-form-urlencoded") || contentType == "" {
			if err := r.ParseForm(); err != nil {
				writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid form body")
				return
			}
			req = tokenRequest{
				GrantType:    r.FormValue("grant_type"),
				Code:         r.FormValue("code"),
				RedirectURI:  r.FormValue("redirect_uri"),
				ClientID:     r.FormValue("client_id"),
				ClientSecret: r.FormValue("client_secret"),
				CodeVerifier: r.FormValue("code_verifier"),
				RefreshToken: r.FormValue("refresh_token"),
				Resource:     r.FormValue("resource"),
			}
		} else {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
				return
			}
		}

		switch req.GrantType {
		case "authorization_code":
			s.handleAuthorizationCodeGrant(w, r, &req)
		case "refresh_token":
			s.handleRefreshTokenGrant(w, r, &req)
		default:
			writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "only authorization_code and refresh_token are supported")
		}
	}
}

func (s *OAuthServer) handleAuthorizationCodeGrant(w http.ResponseWriter, r *http.Request, req *tokenRequest) {
	if req.Code == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "code is required")
		return
	}
	if req.CodeVerifier == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "code_verifier is required (PKCE)")
		return
	}

	authCode, err := s.oauthRepo.GetAuthCode(r.Context(), req.Code)
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "invalid or expired authorization code")
		return
	}

	// Validate client_secret if provided (client_secret_post auth method)
	if req.ClientSecret != "" {
		client, err := s.oauthRepo.GetClientByID(r.Context(), authCode.ClientID)
		if err != nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_client", "unknown client")
			return
		}
		if client.ClientSecret == nil || hashSecret(req.ClientSecret) != *client.ClientSecret {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "invalid client_secret")
			return
		}
	}

	// Check expiration
	if time.Now().UTC().After(authCode.ExpiresAt) {
		// Consume the expired code
		s.oauthRepo.ConsumeAuthCode(r.Context(), req.Code)
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "authorization code expired")
		return
	}

	// Verify redirect_uri matches
	if req.RedirectURI != "" && req.RedirectURI != authCode.RedirectURI {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "redirect_uri mismatch")
		return
	}

	// Verify PKCE code_challenge
	if authCode.CodeChallenge == nil || *authCode.CodeChallenge == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "no code_challenge found for this code")
		return
	}
	if !verifyCodeChallenge(req.CodeVerifier, *authCode.CodeChallenge, authCode.CodeChallengeMethod) {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "PKCE code_verifier validation failed")
		return
	}

	// RFC 8707: Validate resource parameter against what was stored in the
	// auth code. We trust the auth code's resource value (already validated
	// at authorize time) rather than re-deriving from this request's Host,
	// which may differ when the token exchange comes from a different origin.
	if req.Resource != "" && authCode.Resource != "" && req.Resource != authCode.Resource {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "resource parameter mismatch")
		return
	}

	// Determine effective resource for JWT audience
	effectiveResource := authCode.Resource
	if effectiveResource == "" {
		effectiveResource = req.Resource
	}

	// Consume the authorization code (single-use)
	if err := s.oauthRepo.ConsumeAuthCode(r.Context(), req.Code); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "authorization code already consumed")
		return
	}

	// Look up the user to get their role and org ID
	role := "member"
	var userOrgID uuid.UUID
	user, err := s.userRepo.GetByID(r.Context(), authCode.UserID)
	if err == nil {
		role = user.Role
		userOrgID = user.OrgID
	}

	// Generate access token (JWT), including the audience claim when a resource
	// indicator is present (RFC 8707 §2). If the auth code was minted under a
	// share, the share_token_id propagates to the JWT (stid claim) and the
	// refresh token (share_token_id column) so the middleware can scope every
	// downstream request against the share's grant set and so share revocation
	// cascades through the entire derived chain.
	accessToken, err := generateJWTWithAudience(authCode.UserID, userOrgID, role, s.jwtSecret, accessTokenExpiry, effectiveResource, authCode.ShareTokenID)
	if err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to generate access token")
		return
	}

	// Generate refresh token
	rawRefreshToken := generateRefreshToken()
	refreshHash := hashSecret(rawRefreshToken)
	refreshExpiry := time.Now().UTC().Add(refreshTokenExpiry)

	refreshToken := &model.OAuthRefreshToken{
		TokenHash:    refreshHash,
		ClientID:     authCode.ClientID,
		UserID:       authCode.UserID,
		Scope:        authCode.Scope,
		ShareTokenID: authCode.ShareTokenID,
		ExpiresAt:    &refreshExpiry,
	}
	if err := s.oauthRepo.CreateRefreshToken(r.Context(), refreshToken); err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to create refresh token")
		return
	}

	resp := tokenResponse{
		AccessToken:  accessToken,
		TokenType:    "Bearer",
		ExpiresIn:    int(accessTokenExpiry.Seconds()),
		RefreshToken: rawRefreshToken,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *OAuthServer) handleRefreshTokenGrant(w http.ResponseWriter, r *http.Request, req *tokenRequest) {
	if req.RefreshToken == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "refresh_token is required")
		return
	}

	tokenHash := hashSecret(req.RefreshToken)

	storedToken, err := s.oauthRepo.GetRefreshToken(r.Context(), tokenHash)
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "invalid refresh token")
		return
	}

	// Check if revoked
	if storedToken.RevokedAt != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "refresh token has been revoked")
		return
	}

	// Check if expired
	if storedToken.ExpiresAt != nil && time.Now().UTC().After(*storedToken.ExpiresAt) {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "refresh token expired")
		return
	}

	// Share-scoped refresh tokens must re-validate the parent share on every
	// rotation. The cascade-revoke on share revocation is best-effort; if it
	// raced or errored, the refresh row could still be live while the share
	// itself is dead. Fail at /token rather than minting a fresh access JWT
	// that the middleware will then reject on every /mcp call.
	if storedToken.ShareTokenID != nil {
		if s.shareTokens == nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "share-token validation is not configured")
			return
		}
		share, err := s.shareTokens.GetByID(r.Context(), *storedToken.ShareTokenID)
		if err != nil || share == nil || !share.Active(time.Now().UTC()) {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "share token no longer active")
			return
		}
	}

	// Look up user role and org ID
	role := "member"
	var userOrgID uuid.UUID
	user, err := s.userRepo.GetByID(r.Context(), storedToken.UserID)
	if err == nil {
		role = user.Role
		userOrgID = user.OrgID
	}

	// Generate new access token. Share-scoped tokens propagate share_token_id
	// across the refresh boundary so the middleware continues to apply the
	// share's grant set after rotation. Plain account-holder refreshes pass
	// nil and behave as before.
	accessToken, err := GenerateShareScopedJWT(storedToken.UserID, userOrgID, role, s.jwtSecret, accessTokenExpiry, "", storedToken.ShareTokenID)
	if err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to generate access token")
		return
	}

	// Rotate refresh token: revoke old, create new
	if err := s.oauthRepo.RevokeRefreshToken(r.Context(), tokenHash); err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to rotate refresh token")
		return
	}

	newRawRefreshToken := generateRefreshToken()
	newRefreshHash := hashSecret(newRawRefreshToken)
	newRefreshExpiry := time.Now().UTC().Add(refreshTokenExpiry)

	newRefreshToken := &model.OAuthRefreshToken{
		TokenHash:    newRefreshHash,
		ClientID:     storedToken.ClientID,
		UserID:       storedToken.UserID,
		Scope:        storedToken.Scope,
		ShareTokenID: storedToken.ShareTokenID,
		ExpiresAt:    &newRefreshExpiry,
	}
	if err := s.oauthRepo.CreateRefreshToken(r.Context(), newRefreshToken); err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to create new refresh token")
		return
	}

	resp := tokenResponse{
		AccessToken:  accessToken,
		TokenType:    "Bearer",
		ExpiresIn:    int(accessTokenExpiry.Seconds()),
		RefreshToken: newRawRefreshToken,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// userInfoResponse is the response for the UserInfo endpoint.
type userInfoResponse struct {
	Sub   string `json:"sub"`
	Email string `json:"email"`
	Name  string `json:"name"`
	Role  string `json:"role"`
	OrgID string `json:"org_id"`
}

// UserInfoHandler returns user information for the authenticated Bearer token holder.
// Implements a subset of OpenID Connect UserInfo (GET /userinfo).
func (s *OAuthServer) UserInfoHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "missing authorization header")
			return
		}

		tokenStr, ok := strings.CutPrefix(authHeader, "Bearer ")
		if !ok || tokenStr == "" {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "invalid authorization header format")
			return
		}

		claims := &Claims{}
		tok, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return s.jwtSecret, nil
		})
		if err != nil || !tok.Valid {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "invalid or expired token")
			return
		}

		sub, err := claims.GetSubject()
		if err != nil || sub == "" {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "token missing subject")
			return
		}

		userID, err := uuid.Parse(sub)
		if err != nil {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "invalid user id in token")
			return
		}

		user, err := s.userRepo.GetByID(r.Context(), userID)
		if err != nil {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "user not found")
			return
		}

		resp := userInfoResponse{
			Sub:   user.ID.String(),
			Email: user.Email,
			Name:  user.DisplayName,
			Role:  user.Role,
			OrgID: user.OrgID.String(),
		}

		// Share-scoped tokens authenticate as the owner but the recipient is
		// a third party. Re-validate the share is still active (mirrors the
		// per-request middleware check; without this, /userinfo keeps
		// returning identity for the JWT TTL after the owner revokes), and
		// scrub every owner-identifying field so the recipient cannot learn
		// the owner's name, email, stable user UUID, or org assignment.
		// Sub is rewritten to the share id so the response stays OIDC-valid
		// without leaking the owner.
		if claims.ShareTokenID != "" {
			if s.shareTokens == nil {
				writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "share-token validation is not configured")
				return
			}
			shareID, parseErr := uuid.Parse(claims.ShareTokenID)
			if parseErr != nil {
				writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "share_token_id is not a valid uuid")
				return
			}
			share, lookupErr := s.shareTokens.GetByID(r.Context(), shareID)
			if lookupErr != nil || share == nil || !share.Active(time.Now().UTC()) {
				writeOAuthError(w, http.StatusUnauthorized, "invalid_token", "share token no longer active")
				return
			}
			resp.Sub = shareID.String()
			resp.Email = ""
			resp.Name = ""
			resp.Role = "share_bearer"
			resp.OrgID = ""
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}

// protectedResourceMetadata is the response for RFC 9728 protected resource metadata.
type protectedResourceMetadata struct {
	Resource               string   `json:"resource"`
	AuthorizationServers   []string `json:"authorization_servers"`
	BearerMethodsSupported []string `json:"bearer_methods_supported"`
	ScopesSupported        []string `json:"scopes_supported,omitempty"`
}

// ProtectedResourceHandler returns RFC 9728 OAuth Protected Resource Metadata.
// Served at GET /.well-known/oauth-protected-resource.
// The `resource` field is the canonical MCP server URI (the endpoint the
// client uses tokens against), not the authorization server URL.
func (s *OAuthServer) ProtectedResourceHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		base := baseURLFromRequest(r)
		meta := protectedResourceMetadata{
			Resource:               base + "/mcp",
			AuthorizationServers:   []string{base},
			BearerMethodsSupported: []string{"header"},
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(meta)
	}
}

// verifyCodeChallenge verifies a PKCE code_verifier against a stored code_challenge.
// Only the S256 method is supported per RFC 7636.
func verifyCodeChallenge(verifier, challenge, method string) bool {
	if method != codeChallengeMethodS256 {
		return false
	}
	return generateCodeChallenge(verifier) == challenge
}

// generateCodeChallenge computes the S256 PKCE code challenge from a code verifier.
// challenge = BASE64URL(SHA256(code_verifier))
func generateCodeChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// generateClientSecret generates a cryptographically random client secret.
func generateClientSecret() string {
	return generateRandomString(32)
}

// generateAuthCode generates a cryptographically random authorization code.
func generateAuthCode() string {
	return generateRandomString(32)
}

// generateRefreshToken generates a cryptographically random refresh token.
func generateRefreshToken() string {
	return generateRandomString(32)
}

// generateRandomString produces a hex-encoded random string of the given byte length.
func generateRandomString(byteLen int) string {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand.Read failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// hashSecret returns the SHA-256 hex digest of a secret string.
func hashSecret(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}

// containsString checks if a string slice contains a specific value.
func containsString(slice []string, val string) bool {
	return slices.Contains(slice, val)
}

// oauthError represents an OAuth 2.0 error response.
type oauthError struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// writeOAuthError writes a JSON error response per RFC 6749 Section 5.2.
func writeOAuthError(w http.ResponseWriter, statusCode int, errCode, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(oauthError{
		Error:            errCode,
		ErrorDescription: description,
	})
}

// redirectWithError redirects to the redirect_uri with error parameters per RFC 6749 Section 4.1.2.1.
func redirectWithError(w http.ResponseWriter, r *http.Request, redirectURI, errCode, description, state string) {
	u, err := url.Parse(redirectURI)
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid redirect_uri")
		return
	}

	q := u.Query()
	q.Set("error", errCode)
	q.Set("error_description", description)
	if state != "" {
		q.Set("state", state)
	}
	u.RawQuery = q.Encode()

	http.Redirect(w, r, u.String(), http.StatusFound)
}
