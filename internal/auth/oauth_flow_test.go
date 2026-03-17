package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Test DB setup helpers
// ---------------------------------------------------------------------------

// oauthTestDB opens a fresh in-memory SQLite database and runs all migrations.
// It is isolated per test and cleaned up automatically.
func oauthTestDB(t *testing.T) storage.DB {
	t.Helper()

	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("oauthTestDB: getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("oauthTestDB: chdir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("oauthTestDB: open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("oauthTestDB: migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("oauthTestDB: migrate up: %v", err)
	}

	return db
}

// oauthTestUser creates a full org+namespace+user hierarchy in the DB and
// returns the resulting *model.User.
func oauthTestUser(t *testing.T, db storage.DB) *model.User {
	t.Helper()
	ctx := context.Background()

	nsRepo := storage.NewNamespaceRepo(db)
	orgRepo := storage.NewOrganizationRepo(db)
	userRepo := storage.NewUserRepo(db)

	// Root namespace ID used by migrations/seed
	rootID := uuid.MustParse("00000000-0000-0000-0000-000000000000")

	orgNSID := uuid.New()
	orgNS := &model.Namespace{
		ID:       orgNSID,
		Name:     "Test Org NS",
		Slug:     orgNSID.String(),
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgNSID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, orgNS); err != nil {
		t.Fatalf("oauthTestUser: create org namespace: %v", err)
	}

	org := &model.Organization{
		NamespaceID: orgNSID,
		Name:        "Test Org",
		Slug:        "test-org-" + orgNSID.String()[:8],
	}
	if err := orgRepo.Create(ctx, org); err != nil {
		t.Fatalf("oauthTestUser: create org: %v", err)
	}

	user := &model.User{
		Email:       "oauthtest-" + uuid.New().String()[:8] + "@example.com",
		DisplayName: "OAuth Test User",
		OrgID:       org.ID,
		Role:        "admin",
	}
	if err := userRepo.Create(ctx, user, nsRepo, orgNS.Path); err != nil {
		t.Fatalf("oauthTestUser: create user: %v", err)
	}
	return user
}

// ---------------------------------------------------------------------------
// PKCE helper
// ---------------------------------------------------------------------------

func computeCodeChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// ---------------------------------------------------------------------------
// Router builder
// ---------------------------------------------------------------------------

// buildOAuthRouter constructs a chi router with:
//   - GET  /.well-known/oauth-protected-resource
//   - GET  /.well-known/oauth-authorization-server
//   - GET  /authorize
//   - POST /token
//   - POST /register
//   - GET  /userinfo
//   - GET  /mcp  (protected by AuthMiddleware with issuerURL set)
func buildOAuthRouter(oauthSrv *OAuthServer, issuerURL string, secret []byte) http.Handler {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, secret, WithIssuerURL(issuerURL))

	r := chi.NewRouter()

	// Well-known discovery endpoints (no auth)
	r.Get("/.well-known/oauth-protected-resource", oauthSrv.ProtectedResourceHandler())
	r.Get("/.well-known/oauth-authorization-server", oauthSrv.MetadataHandler())

	// OAuth endpoints at MCP spec fallback paths (no auth middleware)
	r.Get("/authorize", oauthSrv.AuthorizeHandler())
	r.Post("/token", oauthSrv.TokenHandler())
	r.Post("/register", oauthSrv.RegisterClientHandler())
	r.Get("/userinfo", oauthSrv.UserInfoHandler())

	// Protected MCP stub — returns 200 when authenticated
	r.With(mw.Handler).Get("/mcp", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	})

	return r
}

// ---------------------------------------------------------------------------
// Test 1: Full MCP OAuth discovery + auth code + token flow
// ---------------------------------------------------------------------------

func TestOAuthFlow_MCPDiscovery_FullFlow(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)

	secret := []byte("test-oauth-secret-32-bytes-long!!")
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)

	srv := httptest.NewServer(nil) // start just to get URL
	srv.Close()

	// Use a fixed issuer URL for the test server
	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)

	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse // do not follow redirects automatically
		},
	}

	// -----------------------------------------------------------------------
	// Step 1: GET /mcp with no auth → 401 + WWW-Authenticate header
	// -----------------------------------------------------------------------
	resp, err := client.Get(ts.URL + "/mcp")
	if err != nil {
		t.Fatalf("step 1 GET /mcp: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("step 1: expected 401, got %d", resp.StatusCode)
	}
	wwwAuth := resp.Header.Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Fatal("step 1: expected WWW-Authenticate header, got empty")
	}
	t.Logf("step 1 WWW-Authenticate: %s", wwwAuth)

	// -----------------------------------------------------------------------
	// Step 2: Parse resource_metadata URL from WWW-Authenticate
	// -----------------------------------------------------------------------
	// Format: Bearer resource_metadata="<url>"
	const prefix = `Bearer resource_metadata="`
	if !strings.HasPrefix(wwwAuth, prefix) {
		t.Fatalf("step 2: WWW-Authenticate does not start with expected prefix, got: %s", wwwAuth)
	}
	resourceMetaURL := strings.TrimSuffix(strings.TrimPrefix(wwwAuth, prefix), `"`)
	t.Logf("step 2 resource_metadata URL: %s", resourceMetaURL)

	// Replace the issuerURL host with the test server host for actual requests
	resourceMetaURL = strings.Replace(resourceMetaURL, issuerURL, ts.URL, 1)

	// -----------------------------------------------------------------------
	// Step 3: GET /.well-known/oauth-protected-resource → parse authorization_servers
	// -----------------------------------------------------------------------
	resp, err = client.Get(resourceMetaURL)
	if err != nil {
		t.Fatalf("step 3 GET oauth-protected-resource: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 3: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var protectedMeta struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		BearerMethods        []string `json:"bearer_methods_supported"`
	}
	if err := json.Unmarshal(body, &protectedMeta); err != nil {
		t.Fatalf("step 3: unmarshal: %v; body: %s", err, body)
	}
	if len(protectedMeta.AuthorizationServers) == 0 {
		t.Fatal("step 3: no authorization_servers in response")
	}
	t.Logf("step 3 authorization_servers[0]: %s", protectedMeta.AuthorizationServers[0])

	// -----------------------------------------------------------------------
	// Step 4: GET /.well-known/oauth-authorization-server → parse endpoints
	// -----------------------------------------------------------------------
	authServerMetaURL := ts.URL + "/.well-known/oauth-authorization-server"
	resp, err = client.Get(authServerMetaURL)
	if err != nil {
		t.Fatalf("step 4 GET oauth-authorization-server: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 4: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var authServerMeta struct {
		Issuer                string   `json:"issuer"`
		AuthorizationEndpoint string   `json:"authorization_endpoint"`
		TokenEndpoint         string   `json:"token_endpoint"`
		RegistrationEndpoint  string   `json:"registration_endpoint"`
		CCMethods             []string `json:"code_challenge_methods_supported"`
	}
	if err := json.Unmarshal(body, &authServerMeta); err != nil {
		t.Fatalf("step 4: unmarshal: %v; body: %s", err, body)
	}
	if authServerMeta.RegistrationEndpoint == "" {
		t.Fatal("step 4: missing registration_endpoint")
	}
	if authServerMeta.AuthorizationEndpoint == "" {
		t.Fatal("step 4: missing authorization_endpoint")
	}
	if authServerMeta.TokenEndpoint == "" {
		t.Fatal("step 4: missing token_endpoint")
	}
	t.Logf("step 4 registration_endpoint: %s", authServerMeta.RegistrationEndpoint)

	// -----------------------------------------------------------------------
	// Step 5: POST /register → get client_id
	// -----------------------------------------------------------------------
	regBody := `{"client_name":"Claude Code","redirect_uris":["http://localhost/callback"],"grant_types":["authorization_code","refresh_token"]}`
	regURL := ts.URL + "/register"
	resp, err = client.Post(regURL, "application/json", strings.NewReader(regBody))
	if err != nil {
		t.Fatalf("step 5 POST /register: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("step 5: expected 200/201, got %d; body: %s", resp.StatusCode, body)
	}

	var regResp struct {
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		RedirectURIs []string `json:"redirect_uris"`
		GrantTypes   []string `json:"grant_types"`
	}
	if err := json.Unmarshal(body, &regResp); err != nil {
		t.Fatalf("step 5: unmarshal: %v; body: %s", err, body)
	}
	if regResp.ClientID == "" {
		t.Fatal("step 5: empty client_id in registration response")
	}
	t.Logf("step 5 client_id: %s", regResp.ClientID)

	// -----------------------------------------------------------------------
	// Step 6: Build authorization URL with PKCE
	// -----------------------------------------------------------------------
	codeVerifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk" // 43-char RFC 7636 verifier
	codeChallenge := computeCodeChallenge(codeVerifier)
	state := "test-state-" + uuid.New().String()[:8]
	redirectURI := "http://localhost/callback"

	authParams := url.Values{}
	authParams.Set("client_id", regResp.ClientID)
	authParams.Set("redirect_uri", redirectURI)
	authParams.Set("response_type", "code")
	authParams.Set("code_challenge", codeChallenge)
	authParams.Set("code_challenge_method", "S256")
	authParams.Set("state", state)

	authURL := ts.URL + "/authorize?" + authParams.Encode()
	t.Logf("step 6 authorization URL (params): %s", authParams.Encode())

	// -----------------------------------------------------------------------
	// Step 7: GET /authorize with nram_session cookie → expect redirect to callback
	// -----------------------------------------------------------------------
	// Generate a valid session JWT for the test user
	sessionToken, err := GenerateJWT(user.ID, user.Role, secret, time.Hour)
	if err != nil {
		t.Fatalf("step 7 generate session JWT: %v", err)
	}

	authReq, err := http.NewRequest(http.MethodGet, authURL, nil)
	if err != nil {
		t.Fatalf("step 7 new request: %v", err)
	}
	authReq.AddCookie(&http.Cookie{
		Name:  "nram_session",
		Value: sessionToken,
	})

	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("step 7 GET /authorize: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("step 7: expected 302 redirect, got %d; body: %s", resp.StatusCode, body)
	}

	location := resp.Header.Get("Location")
	if location == "" {
		t.Fatal("step 7: no Location header in redirect")
	}
	t.Logf("step 7 redirect Location: %s", location)

	// -----------------------------------------------------------------------
	// Step 8: Extract authorization code from redirect URL
	// -----------------------------------------------------------------------
	redirected, err := url.Parse(location)
	if err != nil {
		t.Fatalf("step 8: parse redirect URL: %v", err)
	}
	authCode := redirected.Query().Get("code")
	if authCode == "" {
		t.Fatalf("step 8: no 'code' param in redirect URL: %s", location)
	}
	if errParam := redirected.Query().Get("error"); errParam != "" {
		t.Fatalf("step 8: redirect contains error: %s (%s)", errParam, redirected.Query().Get("error_description"))
	}
	returnedState := redirected.Query().Get("state")
	if returnedState != state {
		t.Fatalf("step 8: state mismatch: got %q, want %q", returnedState, state)
	}
	t.Logf("step 8 authorization code: %s...", authCode[:8])

	// -----------------------------------------------------------------------
	// Step 9: POST /token with authorization_code grant
	// -----------------------------------------------------------------------
	tokenParams := url.Values{}
	tokenParams.Set("grant_type", "authorization_code")
	tokenParams.Set("code", authCode)
	tokenParams.Set("redirect_uri", redirectURI)
	tokenParams.Set("client_id", regResp.ClientID)
	tokenParams.Set("code_verifier", codeVerifier)

	resp, err = client.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(tokenParams.Encode()))
	if err != nil {
		t.Fatalf("step 9 POST /token: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 9: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		t.Fatalf("step 9: unmarshal: %v; body: %s", err, body)
	}
	if tokenResp.AccessToken == "" {
		t.Fatal("step 9: empty access_token")
	}
	if tokenResp.RefreshToken == "" {
		t.Fatal("step 9: empty refresh_token")
	}
	if tokenResp.TokenType != "Bearer" {
		t.Fatalf("step 9: expected token_type Bearer, got %q", tokenResp.TokenType)
	}
	if tokenResp.ExpiresIn <= 0 {
		t.Fatalf("step 9: expected positive expires_in, got %d", tokenResp.ExpiresIn)
	}
	t.Logf("step 9 access_token obtained (len=%d)", len(tokenResp.AccessToken))

	// -----------------------------------------------------------------------
	// Step 10: GET /mcp with Bearer access_token → expect 200
	// -----------------------------------------------------------------------
	mcpReq, err := http.NewRequest(http.MethodGet, ts.URL+"/mcp", nil)
	if err != nil {
		t.Fatalf("step 10 new request: %v", err)
	}
	mcpReq.Header.Set("Authorization", "Bearer "+tokenResp.AccessToken)

	resp, err = client.Do(mcpReq)
	if err != nil {
		t.Fatalf("step 10 GET /mcp: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 10: expected 200 with valid token, got %d; body: %s", resp.StatusCode, body)
	}
	t.Logf("step 10 /mcp response: %s", body)

	// -----------------------------------------------------------------------
	// Step 11: POST /token with refresh_token grant → new access token
	// -----------------------------------------------------------------------
	refreshParams := url.Values{}
	refreshParams.Set("grant_type", "refresh_token")
	refreshParams.Set("refresh_token", tokenResp.RefreshToken)
	refreshParams.Set("client_id", regResp.ClientID)

	resp, err = client.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(refreshParams.Encode()))
	if err != nil {
		t.Fatalf("step 11 POST /token refresh: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 11: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var refreshResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &refreshResp); err != nil {
		t.Fatalf("step 11: unmarshal: %v; body: %s", err, body)
	}
	if refreshResp.AccessToken == "" {
		t.Fatal("step 11: empty new access_token")
	}
	if refreshResp.RefreshToken == "" {
		t.Fatal("step 11: empty new refresh_token")
	}
	if refreshResp.RefreshToken == tokenResp.RefreshToken {
		t.Fatal("step 11: new refresh_token should differ from old (rotation)")
	}
	t.Logf("step 11 refresh token rotated successfully")
}

// ---------------------------------------------------------------------------
// Test 2: Protected Resource Metadata (RFC 9728)
// ---------------------------------------------------------------------------

func TestOAuthFlow_ProtectedResourceMetadata(t *testing.T) {
	db := oauthTestDB(t)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(storage.NewOAuthRepo(db), storage.NewUserRepo(db), secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/.well-known/oauth-protected-resource")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json content-type, got: %s", ct)
	}

	var meta struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		BearerMethods        []string `json:"bearer_methods_supported"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, body)
	}

	if meta.Resource == "" {
		t.Error("RFC 9728: 'resource' field is required but empty")
	}
	if len(meta.AuthorizationServers) == 0 {
		t.Error("RFC 9728: 'authorization_servers' field is required but empty")
	}
	if len(meta.BearerMethods) == 0 {
		t.Error("RFC 9728: 'bearer_methods_supported' field is required but empty")
	}
	expectedResource := issuerURL + "/mcp"
	if meta.Resource != expectedResource {
		t.Errorf("resource mismatch: got %q, want %q (must be MCP endpoint per RFC 9728)", meta.Resource, expectedResource)
	}
	if len(meta.AuthorizationServers) > 0 && meta.AuthorizationServers[0] != issuerURL {
		t.Errorf("authorization_servers[0] mismatch: got %q, want %q", meta.AuthorizationServers[0], issuerURL)
	}
	t.Logf("RFC 9728 metadata valid: resource=%s, auth_servers=%v", meta.Resource, meta.AuthorizationServers)
}

// ---------------------------------------------------------------------------
// Test 3: Authorization Server Metadata (RFC 8414)
// ---------------------------------------------------------------------------

func TestOAuthFlow_AuthServerMetadata(t *testing.T) {
	db := oauthTestDB(t)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(storage.NewOAuthRepo(db), storage.NewUserRepo(db), secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var meta struct {
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
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, body)
	}

	// RFC 8414 required fields
	if meta.Issuer == "" {
		t.Error("RFC 8414: 'issuer' is required")
	}
	if meta.AuthorizationEndpoint == "" {
		t.Error("RFC 8414: 'authorization_endpoint' is required")
	}
	if meta.TokenEndpoint == "" {
		t.Error("RFC 8414: 'token_endpoint' is required")
	}
	if meta.RegistrationEndpoint == "" {
		t.Error("RFC 7591/8414: 'registration_endpoint' is required for dynamic registration")
	}
	if len(meta.ResponseTypesSupported) == 0 {
		t.Error("RFC 8414: 'response_types_supported' is required")
	}
	if len(meta.CodeChallengeMethodsSupported) == 0 {
		t.Error("PKCE: 'code_challenge_methods_supported' must be present")
	}

	// Verify S256 is in code_challenge_methods_supported
	hasS256 := false
	for _, m := range meta.CodeChallengeMethodsSupported {
		if m == "S256" {
			hasS256 = true
		}
	}
	if !hasS256 {
		t.Errorf("PKCE S256 method missing from code_challenge_methods_supported: %v", meta.CodeChallengeMethodsSupported)
	}

	// Verify issuer matches
	if meta.Issuer != issuerURL {
		t.Errorf("issuer mismatch: got %q, want %q", meta.Issuer, issuerURL)
	}

	// Verify endpoints contain the issuer URL base
	for _, ep := range []struct{ name, val string }{
		{"authorization_endpoint", meta.AuthorizationEndpoint},
		{"token_endpoint", meta.TokenEndpoint},
		{"registration_endpoint", meta.RegistrationEndpoint},
	} {
		if !strings.HasPrefix(ep.val, issuerURL) {
			t.Errorf("%s should start with issuer URL %q, got %q", ep.name, issuerURL, ep.val)
		}
	}

	t.Logf("RFC 8414 metadata valid: issuer=%s, S256 supported", meta.Issuer)
}

// ---------------------------------------------------------------------------
// Test 4: Dynamic Client Registration (RFC 7591)
// ---------------------------------------------------------------------------

func TestOAuthFlow_DynamicRegistration(t *testing.T) {
	db := oauthTestDB(t)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(storage.NewOAuthRepo(db), storage.NewUserRepo(db), secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{}

	t.Run("successful registration", func(t *testing.T) {
		body := `{"client_name":"My Test App","redirect_uris":["https://myapp.example.com/callback"],"grant_types":["authorization_code","refresh_token"]}`
		resp, err := client.Post(ts.URL+"/register", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201, got %d; body: %s", resp.StatusCode, respBody)
		}

		var reg struct {
			ClientID     string   `json:"client_id"`
			ClientSecret string   `json:"client_secret"`
			ClientName   string   `json:"client_name"`
			RedirectURIs []string `json:"redirect_uris"`
			GrantTypes   []string `json:"grant_types"`
		}
		if err := json.Unmarshal(respBody, &reg); err != nil {
			t.Fatalf("unmarshal: %v; body: %s", err, respBody)
		}

		if reg.ClientID == "" {
			t.Fatal("expected non-empty client_id")
		}
		if reg.ClientSecret == "" {
			t.Fatal("expected non-empty client_secret")
		}
		if reg.ClientName != "My Test App" {
			t.Fatalf("expected client_name 'My Test App', got %q", reg.ClientName)
		}
		if len(reg.RedirectURIs) != 1 || reg.RedirectURIs[0] != "https://myapp.example.com/callback" {
			t.Fatalf("unexpected redirect_uris: %v", reg.RedirectURIs)
		}

		// Verify client can be retrieved from DB
		ctx := context.Background()
		oauthRepo := storage.NewOAuthRepo(db)
		storedClient, err := oauthRepo.GetClientByID(ctx, reg.ClientID)
		if err != nil {
			t.Fatalf("GetClientByID after registration: %v", err)
		}
		if storedClient.ClientID != reg.ClientID {
			t.Fatalf("stored client_id mismatch: got %q, want %q", storedClient.ClientID, reg.ClientID)
		}
		if !storedClient.AutoRegistered {
			t.Fatal("expected auto_registered=true for dynamically registered client")
		}
		t.Logf("registered client_id: %s", reg.ClientID)
	})

	t.Run("missing client_name", func(t *testing.T) {
		body := `{"redirect_uris":["https://myapp.example.com/callback"]}`
		resp, err := client.Post(ts.URL+"/register", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", resp.StatusCode)
		}
	})

	t.Run("missing redirect_uris", func(t *testing.T) {
		body := `{"client_name":"No Redirect"}`
		resp, err := client.Post(ts.URL+"/register", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", resp.StatusCode)
		}
	})

	t.Run("default grant_types when omitted", func(t *testing.T) {
		body := `{"client_name":"Default Grants","redirect_uris":["https://example.com/cb"]}`
		resp, err := client.Post(ts.URL+"/register", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201, got %d; body: %s", resp.StatusCode, respBody)
		}

		var reg struct {
			GrantTypes []string `json:"grant_types"`
		}
		if err := json.Unmarshal(respBody, &reg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(reg.GrantTypes) == 0 {
			t.Fatal("expected default grant_types to be set")
		}
		t.Logf("default grant_types: %v", reg.GrantTypes)
	})
}

// ---------------------------------------------------------------------------
// Test 5: PKCE Required
// ---------------------------------------------------------------------------

func TestOAuthFlow_PKCE_Required(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)
	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Register a client first
	regBody := `{"client_name":"PKCE Test","redirect_uris":["http://localhost/cb"],"grant_types":["authorization_code"]}`
	regResp, err := client.Post(ts.URL+"/register", "application/json", strings.NewReader(regBody))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	regData, _ := io.ReadAll(regResp.Body)
	regResp.Body.Close()

	var reg struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(regData, &reg)

	// Create session token
	sessionToken, _ := GenerateJWT(user.ID, user.Role, secret, time.Hour)

	// Try authorization WITHOUT code_challenge
	params := url.Values{}
	params.Set("client_id", reg.ClientID)
	params.Set("redirect_uri", "http://localhost/cb")
	params.Set("response_type", "code")
	params.Set("state", "test-state")
	// Intentionally omitting code_challenge and code_challenge_method

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/authorize?"+params.Encode(), nil)
	req.AddCookie(&http.Cookie{Name: "nram_session", Value: sessionToken})

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /authorize: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Should redirect with error since PKCE is required
	if resp.StatusCode == http.StatusOK {
		t.Fatal("expected redirect or error, got 200")
	}

	if resp.StatusCode == http.StatusFound {
		location := resp.Header.Get("Location")
		redirected, _ := url.Parse(location)
		errParam := redirected.Query().Get("error")
		if errParam == "" {
			t.Fatalf("expected error in redirect, got location: %s", location)
		}
		t.Logf("PKCE required correctly rejected: error=%s", errParam)
	} else {
		// Some servers return 400 directly
		t.Logf("PKCE required rejected with status %d: %s", resp.StatusCode, body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 or redirect with error, got %d", resp.StatusCode)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 6: PKCE Wrong Verifier
// ---------------------------------------------------------------------------

func TestOAuthFlow_PKCE_WrongVerifier(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)
	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Register a client
	regBody := `{"client_name":"Wrong Verifier Test","redirect_uris":["http://localhost/cb"],"grant_types":["authorization_code"]}`
	regResp, _ := client.Post(ts.URL+"/register", "application/json", strings.NewReader(regBody))
	regData, _ := io.ReadAll(regResp.Body)
	regResp.Body.Close()

	var reg struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(regData, &reg)

	// PKCE with correct verifier for authorization
	codeVerifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	codeChallenge := computeCodeChallenge(codeVerifier)

	sessionToken, _ := GenerateJWT(user.ID, user.Role, secret, time.Hour)

	params := url.Values{}
	params.Set("client_id", reg.ClientID)
	params.Set("redirect_uri", "http://localhost/cb")
	params.Set("response_type", "code")
	params.Set("code_challenge", codeChallenge)
	params.Set("code_challenge_method", "S256")
	params.Set("state", "test-state")

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/authorize?"+params.Encode(), nil)
	req.AddCookie(&http.Cookie{Name: "nram_session", Value: sessionToken})

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /authorize: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302 redirect, got %d; body: %s", resp.StatusCode, body)
	}

	location := resp.Header.Get("Location")
	redirected, _ := url.Parse(location)
	authCode := redirected.Query().Get("code")
	if authCode == "" {
		t.Fatalf("expected authorization code in redirect, location: %s", location)
	}

	// Now try token exchange with WRONG verifier
	wrongVerifier := "WR0NG-V3R1F13R-TH1S-1S-NOT-R1GHT-AT-ALL!!"
	tokenParams := url.Values{}
	tokenParams.Set("grant_type", "authorization_code")
	tokenParams.Set("code", authCode)
	tokenParams.Set("redirect_uri", "http://localhost/cb")
	tokenParams.Set("client_id", reg.ClientID)
	tokenParams.Set("code_verifier", wrongVerifier)

	tokenResp, err := client.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(tokenParams.Encode()))
	if err != nil {
		t.Fatalf("POST /token: %v", err)
	}
	tokenBody, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body.Close()

	if tokenResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for wrong verifier, got %d; body: %s", tokenResp.StatusCode, tokenBody)
	}

	var errResp struct {
		Error string `json:"error"`
	}
	json.Unmarshal(tokenBody, &errResp)
	if errResp.Error == "" {
		t.Fatal("expected error in JSON response")
	}
	t.Logf("wrong verifier correctly rejected: error=%s", errResp.Error)
}

// ---------------------------------------------------------------------------
// Test 7: Expired Authorization Code
// ---------------------------------------------------------------------------

func TestOAuthFlow_ExpiredCode(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)

	ctx := context.Background()

	// Register a client directly in the DB
	clientSecret := "test-client-secret"
	oauthClient := &model.OAuthClient{
		ClientID:       "expired-code-test-" + uuid.New().String()[:8],
		ClientSecret:   &clientSecret,
		Name:           "Expired Code Test",
		RedirectURIs:   []string{"http://localhost/cb"},
		GrantTypes:     []string{"authorization_code"},
		AutoRegistered: false,
	}
	if err := oauthRepo.CreateClient(ctx, oauthClient); err != nil {
		t.Fatalf("create oauth client: %v", err)
	}

	// Insert an expired authorization code directly
	codeVerifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	codeChallenge := computeCodeChallenge(codeVerifier)
	expiredCode := &model.OAuthAuthorizationCode{
		Code:                "expired-code-" + uuid.New().String()[:8],
		ClientID:            oauthClient.ClientID,
		UserID:              user.ID,
		RedirectURI:         "http://localhost/cb",
		Scope:               "",
		CodeChallenge:       &codeChallenge,
		CodeChallengeMethod: "S256",
		ExpiresAt:           time.Now().UTC().Add(-1 * time.Hour), // already expired
	}
	if err := oauthRepo.CreateAuthCode(ctx, expiredCode); err != nil {
		t.Fatalf("create expired auth code: %v", err)
	}

	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{}

	// Try to exchange the expired code
	tokenParams := url.Values{}
	tokenParams.Set("grant_type", "authorization_code")
	tokenParams.Set("code", expiredCode.Code)
	tokenParams.Set("redirect_uri", "http://localhost/cb")
	tokenParams.Set("client_id", oauthClient.ClientID)
	tokenParams.Set("code_verifier", codeVerifier)

	resp, err := client.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(tokenParams.Encode()))
	if err != nil {
		t.Fatalf("POST /token: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for expired code, got %d; body: %s", resp.StatusCode, body)
	}

	var errResp struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	json.Unmarshal(body, &errResp)

	if errResp.Error != "invalid_grant" {
		t.Errorf("expected error=invalid_grant, got %q", errResp.Error)
	}
	t.Logf("expired code correctly rejected: %s - %s", errResp.Error, errResp.ErrorDescription)
}

// ---------------------------------------------------------------------------
// Test 8: Refresh Token Rotation
// ---------------------------------------------------------------------------

func TestOAuthFlow_RefreshToken(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)
	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	httpClient := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Register client
	regBody := `{"client_name":"Refresh Test","redirect_uris":["http://localhost/cb"],"grant_types":["authorization_code","refresh_token"]}`
	regResp, _ := httpClient.Post(ts.URL+"/register", "application/json", strings.NewReader(regBody))
	regData, _ := io.ReadAll(regResp.Body)
	regResp.Body.Close()
	var reg struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(regData, &reg)

	// Complete auth code flow
	codeVerifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	codeChallenge := computeCodeChallenge(codeVerifier)
	sessionToken, _ := GenerateJWT(user.ID, user.Role, secret, time.Hour)

	params := url.Values{}
	params.Set("client_id", reg.ClientID)
	params.Set("redirect_uri", "http://localhost/cb")
	params.Set("response_type", "code")
	params.Set("code_challenge", codeChallenge)
	params.Set("code_challenge_method", "S256")
	params.Set("state", "test-state")

	authReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/authorize?"+params.Encode(), nil)
	authReq.AddCookie(&http.Cookie{Name: "nram_session", Value: sessionToken})
	authResp, _ := httpClient.Do(authReq)
	authResp.Body.Close()

	location := authResp.Header.Get("Location")
	redirected, _ := url.Parse(location)
	authCode := redirected.Query().Get("code")

	tokenParams := url.Values{}
	tokenParams.Set("grant_type", "authorization_code")
	tokenParams.Set("code", authCode)
	tokenParams.Set("redirect_uri", "http://localhost/cb")
	tokenParams.Set("client_id", reg.ClientID)
	tokenParams.Set("code_verifier", codeVerifier)

	tokenResp, _ := httpClient.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(tokenParams.Encode()))
	tokenData, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body.Close()

	var firstTokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	json.Unmarshal(tokenData, &firstTokens)

	if firstTokens.RefreshToken == "" {
		t.Fatal("no refresh token after initial authorization")
	}

	// First refresh
	refreshParams := url.Values{}
	refreshParams.Set("grant_type", "refresh_token")
	refreshParams.Set("refresh_token", firstTokens.RefreshToken)
	refreshParams.Set("client_id", reg.ClientID)

	refresh1Resp, _ := httpClient.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(refreshParams.Encode()))
	refresh1Data, _ := io.ReadAll(refresh1Resp.Body)
	refresh1Resp.Body.Close()

	if refresh1Resp.StatusCode != http.StatusOK {
		t.Fatalf("first refresh: expected 200, got %d; body: %s", refresh1Resp.StatusCode, refresh1Data)
	}

	var secondTokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	json.Unmarshal(refresh1Data, &secondTokens)

	if secondTokens.AccessToken == "" {
		t.Fatal("no access_token after first refresh")
	}
	if secondTokens.RefreshToken == "" {
		t.Fatal("no refresh_token after first refresh")
	}
	if secondTokens.RefreshToken == firstTokens.RefreshToken {
		t.Fatal("refresh token was not rotated: old and new tokens are identical")
	}

	// Attempt to reuse the OLD refresh token — must be rejected (it was revoked)
	oldRefreshParams := url.Values{}
	oldRefreshParams.Set("grant_type", "refresh_token")
	oldRefreshParams.Set("refresh_token", firstTokens.RefreshToken) // the old one
	oldRefreshParams.Set("client_id", reg.ClientID)

	reuseResp, _ := httpClient.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(oldRefreshParams.Encode()))
	reuseData, _ := io.ReadAll(reuseResp.Body)
	reuseResp.Body.Close()

	if reuseResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("reuse of revoked refresh token: expected 400, got %d; body: %s", reuseResp.StatusCode, reuseData)
	}

	var reuseErr struct {
		Error string `json:"error"`
	}
	json.Unmarshal(reuseData, &reuseErr)
	if reuseErr.Error != "invalid_grant" {
		t.Errorf("expected error=invalid_grant for revoked token, got %q", reuseErr.Error)
	}

	// New refresh token still works
	newRefreshParams := url.Values{}
	newRefreshParams.Set("grant_type", "refresh_token")
	newRefreshParams.Set("refresh_token", secondTokens.RefreshToken)
	newRefreshParams.Set("client_id", reg.ClientID)

	refresh2Resp, _ := httpClient.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(newRefreshParams.Encode()))
	refresh2Data, _ := io.ReadAll(refresh2Resp.Body)
	refresh2Resp.Body.Close()

	if refresh2Resp.StatusCode != http.StatusOK {
		t.Fatalf("second refresh with new token: expected 200, got %d; body: %s", refresh2Resp.StatusCode, refresh2Data)
	}
	t.Logf("refresh token rotation verified: old revoked, new usable, chain continues")
}

// ---------------------------------------------------------------------------
// Test 9: WWW-Authenticate Header Format
// ---------------------------------------------------------------------------

func TestOAuthFlow_WWWAuthenticate_Header(t *testing.T) {
	db := oauthTestDB(t)
	secret := []byte("test-secret-32-bytes-xxxxxxxxxx!!")
	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(storage.NewOAuthRepo(db), storage.NewUserRepo(db), secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{}

	t.Run("no auth header produces WWW-Authenticate", func(t *testing.T) {
		resp, err := client.Get(ts.URL + "/mcp")
		if err != nil {
			t.Fatalf("GET /mcp: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}

		wwwAuth := resp.Header.Get("WWW-Authenticate")
		if wwwAuth == "" {
			t.Fatal("expected WWW-Authenticate header, got empty")
		}

		// Verify exact MCP spec format: Bearer resource_metadata="<url>"
		expectedPrefix := `Bearer resource_metadata="`
		if !strings.HasPrefix(wwwAuth, expectedPrefix) {
			t.Errorf("WWW-Authenticate does not match MCP spec format\n  got:  %s\n  want: starts with %s", wwwAuth, expectedPrefix)
		}
		if !strings.HasSuffix(wwwAuth, `"`) {
			t.Errorf("WWW-Authenticate should end with quote, got: %s", wwwAuth)
		}

		// Extract and verify the URL
		urlPart := strings.TrimSuffix(strings.TrimPrefix(wwwAuth, expectedPrefix), `"`)
		if !strings.HasSuffix(urlPart, "/.well-known/oauth-protected-resource") {
			t.Errorf("resource_metadata URL should end with /.well-known/oauth-protected-resource, got: %s", urlPart)
		}
		t.Logf("WWW-Authenticate: %s", wwwAuth)
	})

	t.Run("invalid bearer token also produces WWW-Authenticate", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/mcp", nil)
		req.Header.Set("Authorization", "Bearer invalid-token-garbage")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET /mcp: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401 for invalid token, got %d", resp.StatusCode)
		}

		wwwAuth := resp.Header.Get("WWW-Authenticate")
		if wwwAuth == "" {
			t.Fatal("expected WWW-Authenticate on invalid token, got empty")
		}
		t.Logf("WWW-Authenticate on invalid token: %s", wwwAuth)
	})

	t.Run("valid token does not produce 401", func(t *testing.T) {
		// Generate a valid JWT for this test
		userID := uuid.New()
		tokenStr, err := GenerateJWT(userID, "admin", secret, time.Hour)
		if err != nil {
			t.Fatalf("GenerateJWT: %v", err)
		}

		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/mcp", nil)
		req.Header.Set("Authorization", "Bearer "+tokenStr)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET /mcp: %v", err)
		}
		resp.Body.Close()

		// Should not be 401 — either 200 (found in DB) or some other status but not 401
		if resp.StatusCode == http.StatusUnauthorized {
			t.Fatal("expected valid JWT to not get 401")
		}
		t.Logf("valid JWT resulted in status %d (not 401)", resp.StatusCode)
	})
}

// ---------------------------------------------------------------------------
// RFC 8707 resource parameter tests
// ---------------------------------------------------------------------------

// TestOAuthFlow_ResourceParameter_Mismatch verifies that a token request whose
// resource parameter differs from the one stored in the authorization code is
// rejected with an invalid_grant error.
func TestOAuthFlow_ResourceParameter_Mismatch(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)

	secret := []byte("test-oauth-secret-32-bytes-long!!")
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)

	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Register a client
	regBody := `{"client_name":"Resource Test Client","redirect_uris":["http://localhost/cb"],"grant_types":["authorization_code"]}`
	resp, err := client.Post(ts.URL+"/register", "application/json", strings.NewReader(regBody))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.NewDecoder(resp.Body).Decode(&regResp)
	resp.Body.Close()

	if regResp.ClientID == "" {
		t.Fatal("expected non-empty client_id from registration")
	}

	// Generate PKCE pair
	codeVerifier := "mismatch_resource_test_verifier_1234567890ab"
	codeChallenge := computeCodeChallenge(codeVerifier)

	// Step 1: Authorize with resource=https://resource-a.example.com
	authParams := url.Values{}
	authParams.Set("client_id", regResp.ClientID)
	authParams.Set("redirect_uri", "http://localhost/cb")
	authParams.Set("response_type", "code")
	authParams.Set("code_challenge", codeChallenge)
	authParams.Set("code_challenge_method", "S256")
	authParams.Set("state", "s1")
	authParams.Set("resource", "https://resource-a.example.com")

	sessionToken, err := GenerateJWT(user.ID, user.Role, secret, time.Hour)
	if err != nil {
		t.Fatalf("generate session JWT: %v", err)
	}
	authReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/authorize?"+authParams.Encode(), nil)
	authReq.AddCookie(&http.Cookie{Name: "nram_session", Value: sessionToken})

	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("authorize: expected 302, got %d", resp.StatusCode)
	}

	loc, _ := url.Parse(resp.Header.Get("Location"))
	authCode := loc.Query().Get("code")
	if authCode == "" {
		t.Fatalf("no code in redirect: %s", resp.Header.Get("Location"))
	}

	// Step 2: Token exchange with a DIFFERENT resource — must be rejected
	tokenParams := url.Values{}
	tokenParams.Set("grant_type", "authorization_code")
	tokenParams.Set("code", authCode)
	tokenParams.Set("redirect_uri", "http://localhost/cb")
	tokenParams.Set("client_id", regResp.ClientID)
	tokenParams.Set("code_verifier", codeVerifier)
	tokenParams.Set("resource", "https://resource-b.example.com") // intentional mismatch

	tokenResp, err := client.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(tokenParams.Encode()))
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	body, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body.Close()

	if tokenResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for resource mismatch, got %d; body: %s", tokenResp.StatusCode, body)
	}

	var errResp struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("unmarshal error response: %v; body: %s", err, body)
	}
	if errResp.Error != "invalid_grant" {
		t.Fatalf("expected error=invalid_grant, got %q", errResp.Error)
	}
	t.Logf("resource mismatch correctly rejected with: %s — %s", errResp.Error, errResp.ErrorDescription)
}

// TestOAuthFlow_ResourceParameter_InJWT verifies that when a resource parameter
// is present in the authorization request, the issued access token carries a
// matching audience (aud) claim per RFC 8707 §2.
func TestOAuthFlow_ResourceParameter_InJWT(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)

	secret := []byte("test-oauth-secret-32-bytes-long!!")
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)

	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	httpClient := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Register a client
	regBody := `{"client_name":"JWT Audience Test","redirect_uris":["http://localhost/cb"],"grant_types":["authorization_code"]}`
	resp, err := httpClient.Post(ts.URL+"/register", "application/json", strings.NewReader(regBody))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.NewDecoder(resp.Body).Decode(&regResp)
	resp.Body.Close()

	if regResp.ClientID == "" {
		t.Fatal("expected non-empty client_id from registration")
	}

	targetResource := "https://mcp.example.com/server"
	codeVerifier := "audience_test_verifier_string_1234567890abcd"
	codeChallenge := computeCodeChallenge(codeVerifier)

	// Authorize with resource indicator
	authParams := url.Values{}
	authParams.Set("client_id", regResp.ClientID)
	authParams.Set("redirect_uri", "http://localhost/cb")
	authParams.Set("response_type", "code")
	authParams.Set("code_challenge", codeChallenge)
	authParams.Set("code_challenge_method", "S256")
	authParams.Set("state", "s2")
	authParams.Set("resource", targetResource)

	sessionToken, err := GenerateJWT(user.ID, user.Role, secret, time.Hour)
	if err != nil {
		t.Fatalf("generate session JWT: %v", err)
	}
	authReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/authorize?"+authParams.Encode(), nil)
	authReq.AddCookie(&http.Cookie{Name: "nram_session", Value: sessionToken})

	resp, err = httpClient.Do(authReq)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("authorize: expected 302, got %d", resp.StatusCode)
	}

	loc, _ := url.Parse(resp.Header.Get("Location"))
	authCode := loc.Query().Get("code")
	if authCode == "" {
		t.Fatalf("no code in redirect: %s", resp.Header.Get("Location"))
	}

	// Token exchange — supply matching resource
	tokenParams := url.Values{}
	tokenParams.Set("grant_type", "authorization_code")
	tokenParams.Set("code", authCode)
	tokenParams.Set("redirect_uri", "http://localhost/cb")
	tokenParams.Set("client_id", regResp.ClientID)
	tokenParams.Set("code_verifier", codeVerifier)
	tokenParams.Set("resource", targetResource)

	tokenResp, err := httpClient.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(tokenParams.Encode()))
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	body, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body.Close()

	if tokenResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", tokenResp.StatusCode, body)
	}

	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		t.Fatalf("unmarshal token response: %v; body: %s", err, body)
	}
	if tok.AccessToken == "" {
		t.Fatal("expected non-empty access_token")
	}

	// Parse JWT and verify audience claim contains the resource indicator
	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(tok.AccessToken, claims, func(t *jwt.Token) (interface{}, error) {
		return secret, nil
	})
	if err != nil {
		t.Fatalf("parse access token: %v", err)
	}
	if !parsed.Valid {
		t.Fatal("access token is not valid")
	}

	aud, err := claims.GetAudience()
	if err != nil || len(aud) == 0 {
		t.Fatalf("expected audience claim in JWT, got: %v (err=%v)", aud, err)
	}
	found := false
	for _, a := range aud {
		if a == targetResource {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected audience to contain %q, got %v", targetResource, aud)
	}
	t.Logf("JWT audience correctly set to: %v", aud)
}

// TestOAuthFlow_MCPDiscovery_FullFlow_WithResource re-runs the complete MCP
// discovery and auth code flow with the RFC 8707 resource parameter included in
// both the authorization request and the token exchange.
func TestOAuthFlow_MCPDiscovery_FullFlow_WithResource(t *testing.T) {
	db := oauthTestDB(t)
	user := oauthTestUser(t, db)

	secret := []byte("test-oauth-secret-32-bytes-long!!")
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)

	issuerURL := "http://localhost:8674"
	oauthSrv := NewOAuthServer(oauthRepo, userRepo, secret, issuerURL)
	router := buildOAuthRouter(oauthSrv, issuerURL, secret)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Register client
	regBody := `{"client_name":"Full Flow Resource Client","redirect_uris":["http://localhost/callback"],"grant_types":["authorization_code","refresh_token"]}`
	resp, err := client.Post(ts.URL+"/register", "application/json", strings.NewReader(regBody))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.NewDecoder(resp.Body).Decode(&regResp)
	resp.Body.Close()

	if regResp.ClientID == "" {
		t.Fatal("empty client_id")
	}

	resourceIndicator := issuerURL + "/mcp"
	codeVerifier := "full_flow_resource_verifier_abcdefghijklmno"
	codeChallenge := computeCodeChallenge(codeVerifier)
	state := "rf-state-" + uuid.New().String()[:8]
	redirectURI := "http://localhost/callback"

	// Build authorization URL with resource parameter
	authParams := url.Values{}
	authParams.Set("client_id", regResp.ClientID)
	authParams.Set("redirect_uri", redirectURI)
	authParams.Set("response_type", "code")
	authParams.Set("code_challenge", codeChallenge)
	authParams.Set("code_challenge_method", "S256")
	authParams.Set("state", state)
	authParams.Set("resource", resourceIndicator)

	sessionToken, err := GenerateJWT(user.ID, user.Role, secret, time.Hour)
	if err != nil {
		t.Fatalf("generate session JWT: %v", err)
	}
	authReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/authorize?"+authParams.Encode(), nil)
	authReq.AddCookie(&http.Cookie{Name: "nram_session", Value: sessionToken})

	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("authorize: expected 302, got %d", resp.StatusCode)
	}

	loc, _ := url.Parse(resp.Header.Get("Location"))
	authCode := loc.Query().Get("code")
	if authCode == "" {
		t.Fatalf("no code in redirect URL: %s", resp.Header.Get("Location"))
	}
	if loc.Query().Get("state") != state {
		t.Fatalf("state mismatch: got %q, want %q", loc.Query().Get("state"), state)
	}

	// Token exchange with matching resource
	tokenParams := url.Values{}
	tokenParams.Set("grant_type", "authorization_code")
	tokenParams.Set("code", authCode)
	tokenParams.Set("redirect_uri", redirectURI)
	tokenParams.Set("client_id", regResp.ClientID)
	tokenParams.Set("code_verifier", codeVerifier)
	tokenParams.Set("resource", resourceIndicator)

	tokenHTTPResp, err := client.Post(ts.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader(tokenParams.Encode()))
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	body, _ := io.ReadAll(tokenHTTPResp.Body)
	tokenHTTPResp.Body.Close()

	if tokenHTTPResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", tokenHTTPResp.StatusCode, body)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, body)
	}
	if tokenResp.AccessToken == "" {
		t.Fatal("empty access_token")
	}
	if tokenResp.RefreshToken == "" {
		t.Fatal("empty refresh_token")
	}

	// Parse JWT and verify audience
	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(tokenResp.AccessToken, claims, func(t *jwt.Token) (interface{}, error) {
		return secret, nil
	})
	if err != nil {
		t.Fatalf("parse access token: %v", err)
	}
	if !parsed.Valid {
		t.Fatal("access token invalid")
	}

	aud, err := claims.GetAudience()
	if err != nil || len(aud) == 0 {
		t.Fatalf("expected audience claim, got: %v (err=%v)", aud, err)
	}
	found := false
	for _, a := range aud {
		if a == resourceIndicator {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("JWT audience should contain %q, got %v", resourceIndicator, aud)
	}

	// Verify the token works against the protected /mcp endpoint
	mcpReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/mcp", nil)
	mcpReq.Header.Set("Authorization", "Bearer "+tokenResp.AccessToken)
	mcpResp, err := client.Do(mcpReq)
	if err != nil {
		t.Fatalf("GET /mcp: %v", err)
	}
	mcpBody, _ := io.ReadAll(mcpResp.Body)
	mcpResp.Body.Close()

	if mcpResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /mcp with resource-bound token, got %d; body: %s", mcpResp.StatusCode, mcpBody)
	}
	t.Logf("full flow with resource parameter succeeded; JWT aud=%v", aud)
}
