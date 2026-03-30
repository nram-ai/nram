package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

const (
	idpStateExpiry   = 10 * time.Minute
	idpSessionExpiry = 1 * time.Hour
	idpCallbackPath  = "/auth/idp/callback"
	idpMaxStates     = 10000
	discoveryCacheTTL = 15 * time.Minute
)

// IdPUserRepo defines the user repository methods needed by the IdP handler.
type IdPUserRepo interface {
	GetByEmail(ctx context.Context, email string) (*model.User, error)
	UpdateLastLogin(ctx context.Context, userID uuid.UUID) error
}

// IdPUserCreator provisions new users during SSO auto-provisioning.
type IdPUserCreator interface {
	CreateUser(ctx context.Context, email, displayName, password, role string, orgID uuid.UUID) (*model.User, error)
}

// IdPConfigRepo reads identity provider configurations.
type IdPConfigRepo interface {
	GetIdPByID(ctx context.Context, id uuid.UUID) (*model.OAuthIdPConfig, error)
}

type oidcDiscovery struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
	JwksURI               string `json:"jwks_uri"`
}

type cachedDiscovery struct {
	disco     *oidcDiscovery
	expiresAt time.Time
}

// idpState tracks an in-flight IdP login to prevent CSRF.
type idpState struct {
	IdPID       uuid.UUID
	RedirectURL string
	Nonce       string
	ExpiresAt   time.Time
}

type idpStateStore struct {
	mu     sync.Mutex
	states map[string]*idpState
	stop   chan struct{}
}

func newIdPStateStore() *idpStateStore {
	s := &idpStateStore{
		states: make(map[string]*idpState),
		stop:   make(chan struct{}),
	}
	go s.cleanup()
	return s
}

func (s *idpStateStore) Close() {
	close(s.stop)
}

// Set stores a state entry. Returns false if the store is at capacity.
func (s *idpStateStore) Set(key string, state *idpState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.states) >= idpMaxStates {
		return false
	}
	s.states[key] = state
	return true
}

func (s *idpStateStore) Get(key string) (*idpState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(st.ExpiresAt) {
		delete(s.states, key)
		return nil, false
	}
	delete(s.states, key)
	return st, true
}

func (s *idpStateStore) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for k, v := range s.states {
				if now.After(v.ExpiresAt) {
					delete(s.states, k)
				}
			}
			s.mu.Unlock()
		}
	}
}

type IdPHandler struct {
	idpRepo    IdPConfigRepo
	userRepo   IdPUserRepo
	userCreate IdPUserCreator
	jwtSecret  []byte
	stateStore *idpStateStore
	httpClient *http.Client

	discoMu    sync.RWMutex
	discoCache map[string]*cachedDiscovery
}

type IdPHandlerConfig struct {
	IdPRepo    IdPConfigRepo
	UserRepo   IdPUserRepo
	UserCreate IdPUserCreator
	JWTSecret  []byte
}

func NewIdPHandler(cfg IdPHandlerConfig) *IdPHandler {
	return &IdPHandler{
		idpRepo:    cfg.IdPRepo,
		userRepo:   cfg.UserRepo,
		userCreate: cfg.UserCreate,
		jwtSecret:  cfg.JWTSecret,
		stateStore: newIdPStateStore(),
		httpClient: &http.Client{Timeout: 15 * time.Second},
		discoCache: make(map[string]*cachedDiscovery),
	}
}

// Close releases resources held by the IdP handler.
func (h *IdPHandler) Close() {
	h.stateStore.Close()
}

// LoginHandler initiates an SSO login by redirecting to the external IdP.
// GET /auth/idp/login?idp_id=<uuid>&redirect=<url>
func (h *IdPHandler) LoginHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		idpIDStr := r.URL.Query().Get("idp_id")
		if idpIDStr == "" {
			http.Error(w, "idp_id query parameter is required", http.StatusBadRequest)
			return
		}

		idpID, err := uuid.Parse(idpIDStr)
		if err != nil {
			http.Error(w, "invalid idp_id", http.StatusBadRequest)
			return
		}

		idpCfg, err := h.idpRepo.GetIdPByID(r.Context(), idpID)
		if err != nil {
			http.Error(w, "identity provider not found", http.StatusNotFound)
			return
		}

		disco, err := h.resolveEndpoints(r.Context(), idpCfg)
		if err != nil {
			http.Error(w, "failed to resolve IdP endpoints", http.StatusBadGateway)
			return
		}

		stateKey := generateRandomString(16)
		nonce := generateRandomString(16)

		if !h.stateStore.Set(stateKey, &idpState{
			IdPID:       idpID,
			RedirectURL: r.URL.Query().Get("redirect"),
			Nonce:       nonce,
			ExpiresAt:   time.Now().Add(idpStateExpiry),
		}) {
			http.Error(w, "too many pending login attempts", http.StatusServiceUnavailable)
			return
		}

		callbackURL := baseURLFromRequest(r) + idpCallbackPath

		authURL, err := url.Parse(disco.AuthorizationEndpoint)
		if err != nil {
			http.Error(w, "invalid authorization endpoint from IdP", http.StatusBadGateway)
			return
		}

		q := authURL.Query()
		q.Set("client_id", idpCfg.ClientID)
		q.Set("redirect_uri", callbackURL)
		q.Set("response_type", "code")
		q.Set("scope", scopesForProvider(idpCfg.ProviderType, disco.AuthorizationEndpoint))
		q.Set("state", stateKey)
		q.Set("nonce", nonce)
		// Colons in scope values (e.g. "read:user") must stay literal.
		// url.Values.Encode() percent-encodes them, which GitHub rejects.
		authURL.RawQuery = strings.ReplaceAll(q.Encode(), "%3A", ":")

		http.Redirect(w, r, authURL.String(), http.StatusFound)
	}
}

// CallbackHandler handles the redirect from the external IdP after authentication.
// GET /auth/idp/callback?code=<code>&state=<state>
func (h *IdPHandler) CallbackHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if errCode := r.URL.Query().Get("error"); errCode != "" {
			desc := r.URL.Query().Get("error_description")
			http.Error(w, fmt.Sprintf("IdP error: %s - %s", errCode, desc), http.StatusBadRequest)
			return
		}

		code := r.URL.Query().Get("code")
		stateKey := r.URL.Query().Get("state")

		if code == "" || stateKey == "" {
			http.Error(w, "missing code or state parameter", http.StatusBadRequest)
			return
		}

		state, ok := h.stateStore.Get(stateKey)
		if !ok {
			http.Error(w, "invalid or expired state parameter", http.StatusBadRequest)
			return
		}

		idpCfg, err := h.idpRepo.GetIdPByID(r.Context(), state.IdPID)
		if err != nil {
			http.Error(w, "identity provider not found", http.StatusNotFound)
			return
		}

		disco, err := h.resolveEndpoints(r.Context(), idpCfg)
		if err != nil {
			http.Error(w, "failed to resolve IdP endpoints", http.StatusBadGateway)
			return
		}

		callbackURL := baseURLFromRequest(r) + idpCallbackPath

		tokenResp, err := h.exchangeCode(r.Context(), disco.TokenEndpoint, code, callbackURL, idpCfg.ClientID, idpCfg.ClientSecret)
		if err != nil {
			http.Error(w, "token exchange failed", http.StatusBadGateway)
			return
		}

		userInfo, err := h.getUserInfo(r.Context(), tokenResp, disco.UserinfoEndpoint)
		if err != nil {
			http.Error(w, "failed to get user info", http.StatusBadGateway)
			return
		}

		if userInfo.Email == "" {
			http.Error(w, "IdP did not return an email address", http.StatusBadRequest)
			return
		}

		if len(idpCfg.AllowedDomains) > 0 {
			emailDomain := emailDomainOf(userInfo.Email)
			if !containsString(idpCfg.AllowedDomains, emailDomain) {
				http.Error(w, fmt.Sprintf("email domain %q is not allowed for this identity provider", emailDomain), http.StatusForbidden)
				return
			}
		}

		user, err := h.findOrCreateUser(r.Context(), idpCfg, userInfo)
		if err != nil {
			http.Error(w, "user provisioning failed", http.StatusInternalServerError)
			return
		}

		if user.DisabledAt != nil {
			http.Error(w, "account disabled", http.StatusForbidden)
			return
		}

		sessionToken, err := GenerateSessionJWT(user.ID, user.OrgID, user.Role, user.Email, user.DisplayName, h.jwtSecret, idpSessionExpiry)
		if err != nil {
			http.Error(w, "failed to create session", http.StatusInternalServerError)
			return
		}

		http.SetCookie(w, &http.Cookie{
			Name:     "nram_session",
			Value:    sessionToken,
			Path:     "/",
			MaxAge:   300, // Short-lived: SPA reads it on load and moves to localStorage.
			SameSite: http.SameSiteLaxMode,
			Secure:   requestIsSecure(r),
		})

		_ = h.userRepo.UpdateLastLogin(r.Context(), user.ID)

		redirectTarget := "/"
		if state.RedirectURL != "" {
			redirectTarget = state.RedirectURL
		}
		http.Redirect(w, r, redirectTarget, http.StatusFound)
	}
}

// resolveEndpoints returns OIDC endpoints by using explicit URLs from the config
// when available, falling back to OIDC discovery via issuer_url.
func (h *IdPHandler) resolveEndpoints(ctx context.Context, cfg *model.OAuthIdPConfig) (*oidcDiscovery, error) {
	// If explicit endpoint URLs are configured, use them directly.
	if cfg.AuthorizeURL != nil && *cfg.AuthorizeURL != "" &&
		cfg.TokenURL != nil && *cfg.TokenURL != "" {
		disco := &oidcDiscovery{
			AuthorizationEndpoint: *cfg.AuthorizeURL,
			TokenEndpoint:         *cfg.TokenURL,
		}
		if cfg.UserinfoURL != nil {
			disco.UserinfoEndpoint = *cfg.UserinfoURL
		}
		return disco, nil
	}

	// Fall back to OIDC discovery.
	if cfg.IssuerURL == nil || *cfg.IssuerURL == "" {
		return nil, fmt.Errorf("identity provider has no issuer URL or explicit endpoint URLs configured")
	}

	return h.discoverOIDC(ctx, *cfg.IssuerURL)
}

// discoverOIDC fetches and caches the OIDC discovery document from the issuer.
func (h *IdPHandler) discoverOIDC(ctx context.Context, issuerURL string) (*oidcDiscovery, error) {
	key := strings.TrimRight(issuerURL, "/")

	// Check cache.
	h.discoMu.RLock()
	if cached, ok := h.discoCache[key]; ok && time.Now().Before(cached.expiresAt) {
		h.discoMu.RUnlock()
		return cached.disco, nil
	}
	h.discoMu.RUnlock()

	discoveryURL := key + "/.well-known/openid-configuration"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create discovery request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("discovery returned status %d: %s", resp.StatusCode, string(body))
	}

	var disco oidcDiscovery
	if err := json.NewDecoder(resp.Body).Decode(&disco); err != nil {
		return nil, fmt.Errorf("decode discovery document: %w", err)
	}

	if disco.AuthorizationEndpoint == "" || disco.TokenEndpoint == "" {
		return nil, fmt.Errorf("discovery document missing required endpoints")
	}

	// Cache the result.
	h.discoMu.Lock()
	h.discoCache[key] = &cachedDiscovery{
		disco:     &disco,
		expiresAt: time.Now().Add(discoveryCacheTTL),
	}
	h.discoMu.Unlock()

	return &disco, nil
}

type idpTokenResponse struct {
	AccessToken  string `json:"access_token"`
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

func (h *IdPHandler) exchangeCode(ctx context.Context, tokenEndpoint, code, redirectURI, clientID, clientSecret string) (*idpTokenResponse, error) {
	data := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {redirectURI},
		"client_id":    {clientID},
	}
	if clientSecret != "" {
		data.Set("client_secret", clientSecret)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, fmt.Errorf("read token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp idpTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("token response missing access_token")
	}

	return &tokenResp, nil
}

type idpUserInfo struct {
	Email string
	Name  string
}

func (h *IdPHandler) getUserInfo(ctx context.Context, tokenResp *idpTokenResponse, userinfoEndpoint string) (*idpUserInfo, error) {
	// Try the ID token first (safe: received directly from IdP token endpoint over TLS).
	if tokenResp.IDToken != "" {
		info, err := h.parseIDTokenClaims(tokenResp.IDToken)
		if err == nil && info.Email != "" {
			return info, nil
		}
	}

	if userinfoEndpoint == "" {
		return nil, fmt.Errorf("no id_token and no userinfo endpoint available")
	}

	info, err := h.fetchUserInfo(ctx, userinfoEndpoint, tokenResp.AccessToken)
	if err != nil {
		return nil, err
	}

	// Some providers (e.g. GitHub) return a null email when the user's email
	// is private. Try fetching from a /emails endpoint as a fallback.
	if info.Email == "" {
		if email, err := h.fetchPrimaryEmail(ctx, userinfoEndpoint, tokenResp.AccessToken); err == nil && email != "" {
			info.Email = email
		}
	}

	return info, nil
}

func (h *IdPHandler) parseIDTokenClaims(idToken string) (*idpUserInfo, error) {
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(idToken, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("parse id_token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}

	return extractUserInfoFromClaims(claims), nil
}

func (h *IdPHandler) fetchUserInfo(ctx context.Context, userinfoEndpoint, accessToken string) (*idpUserInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, userinfoEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("userinfo request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("userinfo returned status %d: %s", resp.StatusCode, string(body))
	}

	var claims map[string]interface{}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 256*1024)).Decode(&claims); err != nil {
		return nil, fmt.Errorf("decode userinfo response: %w", err)
	}

	return extractUserInfoFromClaims(claims), nil
}

// fetchPrimaryEmail tries to get the user's primary email from a provider's
// email list endpoint. Derives the URL by appending "/emails" to the userinfo
// endpoint path (e.g. https://api.github.com/user -> /user/emails). Returns
// the first primary+verified email, or the first verified email, or the first
// email in the list.
func (h *IdPHandler) fetchPrimaryEmail(ctx context.Context, userinfoEndpoint, accessToken string) (string, error) {
	u, err := url.Parse(userinfoEndpoint)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/emails"
	emailsURL := u.String()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, emailsURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("emails endpoint returned %d", resp.StatusCode)
	}

	var emails []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 256*1024)).Decode(&emails); err != nil {
		return "", err
	}

	// Prefer primary+verified, then verified, then any.
	var verified, fallback string
	for _, e := range emails {
		if e.Email == "" {
			continue
		}
		if fallback == "" {
			fallback = e.Email
		}
		if e.Verified && verified == "" {
			verified = e.Email
		}
		if e.Primary && e.Verified {
			return e.Email, nil
		}
	}
	if verified != "" {
		return verified, nil
	}
	return fallback, nil
}

// extractUserInfoFromClaims extracts email and display name from OIDC claims.
func extractUserInfoFromClaims(claims map[string]interface{}) *idpUserInfo {
	info := &idpUserInfo{}

	if email, ok := claims["email"].(string); ok {
		info.Email = email
	}

	if name, ok := claims["name"].(string); ok {
		info.Name = name
	} else if givenName, ok := claims["given_name"].(string); ok {
		info.Name = givenName
		if familyName, ok := claims["family_name"].(string); ok {
			info.Name += " " + familyName
		}
	}

	return info
}

func (h *IdPHandler) findOrCreateUser(ctx context.Context, idpCfg *model.OAuthIdPConfig, info *idpUserInfo) (*model.User, error) {
	user, err := h.userRepo.GetByEmail(ctx, info.Email)
	if err == nil {
		return user, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("look up user: %w", err)
	}

	if !idpCfg.AutoProvision {
		return nil, fmt.Errorf("user %q does not exist and auto-provisioning is disabled", info.Email)
	}

	if idpCfg.OrgID == nil {
		return nil, fmt.Errorf("cannot auto-provision user: IdP has no organization configured")
	}

	displayName := info.Name
	if displayName == "" {
		displayName = strings.Split(info.Email, "@")[0]
	}

	role := idpCfg.DefaultRole
	if role == "" {
		role = "member"
	}

	// Random password for IdP-only user (256-bit entropy, login path effectively unusable).
	randomPW := generateRandomString(32)

	user, err = h.userCreate.CreateUser(ctx, info.Email, displayName, randomPW, role, *idpCfg.OrgID)
	if err != nil {
		return nil, fmt.Errorf("auto-provision user: %w", err)
	}

	return user, nil
}

// scopesForProvider returns the OAuth scopes appropriate for the given
// provider. Checks provider_type first, then falls back to detecting the
// provider from the authorize URL hostname for configs that were saved
// with a generic type.
func scopesForProvider(providerType string, authorizeURL string) string {
	provider := strings.ToLower(providerType)

	// Fall back to hostname detection when provider_type is generic.
	if provider == "oidc" || provider == "oauth" || provider == "" {
		if strings.Contains(authorizeURL, "github.com") {
			provider = "github"
		} else if strings.Contains(authorizeURL, "gitlab.com") || strings.Contains(authorizeURL, "gitlab") {
			provider = "gitlab"
		}
	}

	switch provider {
	case "github":
		return "read:user user:email"
	case "gitlab":
		return "openid email profile read_user"
	default:
		return "openid email profile"
	}
}

func emailDomainOf(email string) string {
	parts := strings.SplitN(email, "@", 2)
	if len(parts) != 2 {
		return ""
	}
	return strings.ToLower(parts[1])
}
