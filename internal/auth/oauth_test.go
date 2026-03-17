package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// testOAuthDB creates a temporary SQLite DB with all migrations applied.
func testOAuthDB(t *testing.T) storage.DB {
	t.Helper()
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.Chdir(origDir)
	})

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	return db
}

// testOAuthSetup creates a full test environment: DB, repos, user, client, and OAuthServer.
type testOAuthEnv struct {
	db        storage.DB
	oauthRepo *storage.OAuthRepo
	userRepo  *storage.UserRepo
	nsRepo    *storage.NamespaceRepo
	server    *OAuthServer
	user      *model.User
	client    *model.OAuthClient
}

var rootID = uuid.MustParse("00000000-0000-0000-0000-000000000000")

func setupOAuthEnv(t *testing.T) *testOAuthEnv {
	t.Helper()
	ctx := context.Background()
	db := testOAuthDB(t)

	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)
	nsRepo := storage.NewNamespaceRepo(db)
	orgRepo := storage.NewOrganizationRepo(db)

	// Create org namespace
	orgNSID := uuid.New()
	ns := &model.Namespace{
		ID:       orgNSID,
		Name:     "Org " + orgNSID.String()[:8],
		Slug:     orgNSID.String(),
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgNSID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("failed to create org namespace: %v", err)
	}

	// Create organization
	org := &model.Organization{
		NamespaceID: orgNSID,
		Name:        "Test Org",
		Slug:        "test-org-" + orgNSID.String()[:8],
	}
	if err := orgRepo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create org: %v", err)
	}

	// Create user
	user := &model.User{
		Email:       "oauth-test-" + uuid.New().String()[:8] + "@example.com",
		DisplayName: "OAuth Test User",
		OrgID:       org.ID,
		Role:        "member",
	}
	if err := userRepo.Create(ctx, user, nsRepo, ns.Path); err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	// Create OAuth client
	secret := hashSecret("client-secret-raw")
	client := &model.OAuthClient{
		ClientID:       "test-client-" + uuid.New().String()[:8],
		ClientSecret:   &secret,
		Name:           "Test OAuth Client",
		RedirectURIs:   []string{"https://example.com/callback"},
		GrantTypes:     []string{"authorization_code", "refresh_token"},
		OrgID:          &user.OrgID,
		AutoRegistered: false,
	}
	if err := oauthRepo.CreateClient(ctx, client); err != nil {
		t.Fatalf("failed to create test oauth client: %v", err)
	}

	srv := NewOAuthServer(oauthRepo, userRepo, testSecret)

	return &testOAuthEnv{
		db:        db,
		oauthRepo: oauthRepo,
		userRepo:  userRepo,
		nsRepo:    nsRepo,
		server:    srv,
		user:      user,
		client:    client,
	}
}

// ---------------------------------------------------------------------------
// Metadata endpoint tests
// ---------------------------------------------------------------------------

func TestMetadataHandler_ReturnsValidJSON(t *testing.T) {
	env := setupOAuthEnv(t)

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	rec := httptest.NewRecorder()

	env.server.MetadataHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var meta serverMetadata
	if err := json.NewDecoder(rec.Body).Decode(&meta); err != nil {
		t.Fatalf("failed to decode metadata: %v", err)
	}

	// httptest.NewRequest sets Host: example.com; baseURLFromRequest returns "http://example.com"
	expectedBase := "http://example.com"
	if meta.Issuer != expectedBase {
		t.Fatalf("unexpected issuer: %q", meta.Issuer)
	}
	if meta.AuthorizationEndpoint != expectedBase+"/authorize" {
		t.Fatalf("unexpected authorization_endpoint: %q", meta.AuthorizationEndpoint)
	}
	if meta.TokenEndpoint != expectedBase+"/token" {
		t.Fatalf("unexpected token_endpoint: %q", meta.TokenEndpoint)
	}
	if meta.RegistrationEndpoint != expectedBase+"/register" {
		t.Fatalf("unexpected registration_endpoint: %q", meta.RegistrationEndpoint)
	}
	if meta.UserinfoEndpoint != expectedBase+"/userinfo" {
		t.Fatalf("unexpected userinfo_endpoint: %q", meta.UserinfoEndpoint)
	}
	if len(meta.ResponseTypesSupported) != 1 || meta.ResponseTypesSupported[0] != "code" {
		t.Fatalf("unexpected response_types_supported: %v", meta.ResponseTypesSupported)
	}
	if len(meta.CodeChallengeMethodsSupported) != 1 || meta.CodeChallengeMethodsSupported[0] != "S256" {
		t.Fatalf("unexpected code_challenge_methods_supported: %v", meta.CodeChallengeMethodsSupported)
	}
}

// ---------------------------------------------------------------------------
// Client registration tests
// ---------------------------------------------------------------------------

func TestRegisterClientHandler_CreatesClient(t *testing.T) {
	env := setupOAuthEnv(t)

	body := `{"client_name":"My New App","redirect_uris":["https://myapp.com/cb"],"grant_types":["authorization_code"]}`
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	env.server.RegisterClientHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp clientRegistrationResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ClientID == "" {
		t.Fatal("expected non-empty client_id")
	}
	if resp.ClientSecret == "" {
		t.Fatal("expected non-empty client_secret")
	}
	if resp.ClientName != "My New App" {
		t.Fatalf("unexpected client_name: %q", resp.ClientName)
	}
	if len(resp.RedirectURIs) != 1 || resp.RedirectURIs[0] != "https://myapp.com/cb" {
		t.Fatalf("unexpected redirect_uris: %v", resp.RedirectURIs)
	}

	// Verify client exists in DB
	stored, err := env.oauthRepo.GetClientByID(context.Background(), resp.ClientID)
	if err != nil {
		t.Fatalf("client not found in DB: %v", err)
	}
	if stored.Name != "My New App" {
		t.Fatalf("stored name mismatch: %q", stored.Name)
	}
	if !stored.AutoRegistered {
		t.Fatal("expected auto_registered to be true")
	}
}

func TestRegisterClientHandler_MissingName(t *testing.T) {
	env := setupOAuthEnv(t)

	body := `{"redirect_uris":["https://myapp.com/cb"]}`
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	env.server.RegisterClientHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestRegisterClientHandler_MissingRedirectURIs(t *testing.T) {
	env := setupOAuthEnv(t)

	body := `{"client_name":"No Redirects"}`
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	env.server.RegisterClientHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Authorize endpoint tests
// ---------------------------------------------------------------------------

func TestAuthorizeHandler_ValidPKCE_RedirectsWithCode(t *testing.T) {
	env := setupOAuthEnv(t)

	codeVerifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	codeChallenge := generateCodeChallenge(codeVerifier)

	q := url.Values{}
	q.Set("client_id", env.client.ClientID)
	q.Set("redirect_uri", "https://example.com/callback")
	q.Set("response_type", "code")
	q.Set("code_challenge", codeChallenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", "xyz123")

	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	req = req.WithContext(WithContext(req.Context(), &AuthContext{UserID: env.user.ID, Role: env.user.Role}))
	rec := httptest.NewRecorder()

	env.server.AuthorizeHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d; body: %s", rec.Code, rec.Body.String())
	}

	loc, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("invalid Location header: %v", err)
	}

	code := loc.Query().Get("code")
	if code == "" {
		t.Fatal("expected code in redirect URL")
	}
	if loc.Query().Get("state") != "xyz123" {
		t.Fatalf("expected state=xyz123, got %q", loc.Query().Get("state"))
	}
}

func TestAuthorizeHandler_Unauthenticated(t *testing.T) {
	env := setupOAuthEnv(t)

	q := url.Values{}
	q.Set("client_id", env.client.ClientID)
	q.Set("redirect_uri", "https://example.com/callback")
	q.Set("response_type", "code")
	q.Set("code_challenge", "abc")
	q.Set("code_challenge_method", "S256")

	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	// No auth context set
	rec := httptest.NewRecorder()

	env.server.AuthorizeHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}

	loc := rec.Header().Get("Location")
	if loc == "" {
		t.Fatal("expected Location header on redirect")
	}
	if !strings.HasPrefix(loc, "/login?redirect=") {
		t.Fatalf("expected redirect to /login, got %q", loc)
	}
}

func TestAuthorizeHandler_MissingClientID(t *testing.T) {
	env := setupOAuthEnv(t)

	req := httptest.NewRequest(http.MethodGet, "/authorize?response_type=code", nil)
	rec := httptest.NewRecorder()

	env.server.AuthorizeHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAuthorizeHandler_MissingCodeChallenge(t *testing.T) {
	env := setupOAuthEnv(t)

	q := url.Values{}
	q.Set("client_id", env.client.ClientID)
	q.Set("redirect_uri", "https://example.com/callback")
	q.Set("response_type", "code")

	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	env.server.AuthorizeHandler().ServeHTTP(rec, req)

	// Should redirect with error since redirect_uri is present
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
	loc, _ := url.Parse(rec.Header().Get("Location"))
	if loc.Query().Get("error") != "invalid_request" {
		t.Fatalf("expected error=invalid_request, got %q", loc.Query().Get("error"))
	}
}

func TestAuthorizeHandler_UnknownClientID(t *testing.T) {
	env := setupOAuthEnv(t)

	q := url.Values{}
	q.Set("client_id", "unknown-client")
	q.Set("redirect_uri", "https://example.com/callback")
	q.Set("response_type", "code")
	q.Set("code_challenge", "abc")
	q.Set("code_challenge_method", "S256")

	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	env.server.AuthorizeHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAuthorizeHandler_UnregisteredRedirectURI(t *testing.T) {
	env := setupOAuthEnv(t)

	q := url.Values{}
	q.Set("client_id", env.client.ClientID)
	q.Set("redirect_uri", "https://evil.com/callback")
	q.Set("response_type", "code")
	q.Set("code_challenge", "abc")
	q.Set("code_challenge_method", "S256")

	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	env.server.AuthorizeHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Token endpoint tests
// ---------------------------------------------------------------------------

// createAuthCodeForTokenTest creates a valid authorization code via the authorize endpoint.
func createAuthCodeForTokenTest(t *testing.T, env *testOAuthEnv, codeVerifier string) string {
	t.Helper()
	codeChallenge := generateCodeChallenge(codeVerifier)

	q := url.Values{}
	q.Set("client_id", env.client.ClientID)
	q.Set("redirect_uri", "https://example.com/callback")
	q.Set("response_type", "code")
	q.Set("code_challenge", codeChallenge)
	q.Set("code_challenge_method", "S256")
	q.Set("scope", "read write")

	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	req = req.WithContext(WithContext(req.Context(), &AuthContext{UserID: env.user.ID, Role: env.user.Role}))
	rec := httptest.NewRecorder()
	env.server.AuthorizeHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("authorize: expected 302, got %d; body: %s", rec.Code, rec.Body.String())
	}

	loc, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("invalid Location: %v", err)
	}

	code := loc.Query().Get("code")
	if code == "" {
		t.Fatal("expected code in redirect URL")
	}
	return code
}

func TestTokenHandler_AuthCodeGrant_ValidPKCE(t *testing.T) {
	env := setupOAuthEnv(t)
	codeVerifier := "a]very}secure_code!verifier-with+various.chars~12345678"
	code := createAuthCodeForTokenTest(t, env, codeVerifier)

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	form.Set("code_verifier", codeVerifier)

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp tokenResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode token response: %v", err)
	}

	if resp.AccessToken == "" {
		t.Fatal("expected non-empty access_token")
	}
	if resp.TokenType != "Bearer" {
		t.Fatalf("expected token_type=Bearer, got %q", resp.TokenType)
	}
	if resp.ExpiresIn != 3600 {
		t.Fatalf("expected expires_in=3600, got %d", resp.ExpiresIn)
	}
	if resp.RefreshToken == "" {
		t.Fatal("expected non-empty refresh_token")
	}

	// Verify the access token is a valid JWT
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(resp.AccessToken, claims, func(t *jwt.Token) (interface{}, error) {
		return testSecret, nil
	})
	if err != nil {
		t.Fatalf("failed to parse access token JWT: %v", err)
	}
	if !tok.Valid {
		t.Fatal("access token JWT is invalid")
	}

	// Verify cache-control headers
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control=no-store, got %q", rec.Header().Get("Cache-Control"))
	}
}

func TestTokenHandler_AuthCodeGrant_InvalidPKCE(t *testing.T) {
	env := setupOAuthEnv(t)
	codeVerifier := "correct_verifier_string_for_challenge"
	code := createAuthCodeForTokenTest(t, env, codeVerifier)

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	form.Set("code_verifier", "wrong_verifier_will_not_match")

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var errResp oauthError
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.Error != "invalid_grant" {
		t.Fatalf("expected error=invalid_grant, got %q", errResp.Error)
	}
}

func TestTokenHandler_AuthCodeGrant_ConsumedCode(t *testing.T) {
	env := setupOAuthEnv(t)
	codeVerifier := "verifier_for_consumed_test"
	code := createAuthCodeForTokenTest(t, env, codeVerifier)

	// First exchange succeeds
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	form.Set("code_verifier", codeVerifier)

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("first exchange: expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	// Second exchange with same code should fail
	req2 := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec2 := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("second exchange: expected 400, got %d; body: %s", rec2.Code, rec2.Body.String())
	}
}

func TestTokenHandler_AuthCodeGrant_InvalidCode(t *testing.T) {
	env := setupOAuthEnv(t)

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", "nonexistent-code")
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	form.Set("code_verifier", "some_verifier")

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var errResp oauthError
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.Error != "invalid_grant" {
		t.Fatalf("expected error=invalid_grant, got %q", errResp.Error)
	}
}

func TestTokenHandler_AuthCodeGrant_MissingCodeVerifier(t *testing.T) {
	env := setupOAuthEnv(t)
	code := createAuthCodeForTokenTest(t, env, "some_verifier")

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	// No code_verifier

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestTokenHandler_AuthCodeGrant_JSONBody(t *testing.T) {
	env := setupOAuthEnv(t)
	codeVerifier := "json_body_verifier_test"
	code := createAuthCodeForTokenTest(t, env, codeVerifier)

	body := `{"grant_type":"authorization_code","code":"` + code + `","redirect_uri":"https://example.com/callback","client_id":"` + env.client.ClientID + `","code_verifier":"` + codeVerifier + `"}`
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Refresh token grant tests
// ---------------------------------------------------------------------------

func TestTokenHandler_RefreshTokenGrant(t *testing.T) {
	env := setupOAuthEnv(t)
	codeVerifier := "verifier_for_refresh_test"
	code := createAuthCodeForTokenTest(t, env, codeVerifier)

	// Exchange code for tokens
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	form.Set("code_verifier", codeVerifier)

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code exchange: expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var tokenResp tokenResponse
	json.NewDecoder(rec.Body).Decode(&tokenResp)

	// Use refresh token to get new access token
	refreshForm := url.Values{}
	refreshForm.Set("grant_type", "refresh_token")
	refreshForm.Set("refresh_token", tokenResp.RefreshToken)

	req2 := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(refreshForm.Encode()))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec2 := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("refresh: expected 200, got %d; body: %s", rec2.Code, rec2.Body.String())
	}

	var refreshResp tokenResponse
	json.NewDecoder(rec2.Body).Decode(&refreshResp)

	if refreshResp.AccessToken == "" {
		t.Fatal("expected non-empty access_token from refresh")
	}
	if refreshResp.RefreshToken == "" {
		t.Fatal("expected non-empty new refresh_token (token rotation)")
	}
	if refreshResp.RefreshToken == tokenResp.RefreshToken {
		t.Fatal("expected new refresh_token to differ from old one (rotation)")
	}
	if refreshResp.TokenType != "Bearer" {
		t.Fatalf("expected token_type=Bearer, got %q", refreshResp.TokenType)
	}

	// Verify the new access token is a valid JWT
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(refreshResp.AccessToken, claims, func(t *jwt.Token) (interface{}, error) {
		return testSecret, nil
	})
	if err != nil {
		t.Fatalf("failed to parse refreshed access token: %v", err)
	}
	if !tok.Valid {
		t.Fatal("refreshed access token is invalid")
	}
}

func TestTokenHandler_RefreshTokenGrant_OldTokenRevoked(t *testing.T) {
	env := setupOAuthEnv(t)
	codeVerifier := "verifier_for_rotation_revoke"
	code := createAuthCodeForTokenTest(t, env, codeVerifier)

	// Exchange code for tokens
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	form.Set("code_verifier", codeVerifier)

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	var tokenResp tokenResponse
	json.NewDecoder(rec.Body).Decode(&tokenResp)

	// Refresh once
	refreshForm := url.Values{}
	refreshForm.Set("grant_type", "refresh_token")
	refreshForm.Set("refresh_token", tokenResp.RefreshToken)

	req2 := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(refreshForm.Encode()))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec2 := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("refresh: expected 200, got %d", rec2.Code)
	}

	// Try to use the old refresh token again — should fail because it was revoked
	req3 := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(refreshForm.Encode()))
	req3.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec3 := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusBadRequest {
		t.Fatalf("old refresh token: expected 400, got %d; body: %s", rec3.Code, rec3.Body.String())
	}
}

func TestTokenHandler_RefreshTokenGrant_InvalidToken(t *testing.T) {
	env := setupOAuthEnv(t)

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", "totally-invalid-token")

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestTokenHandler_UnsupportedGrantType(t *testing.T) {
	env := setupOAuthEnv(t)

	form := url.Values{}
	form.Set("grant_type", "client_credentials")

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// PKCE helper tests
// ---------------------------------------------------------------------------

func TestPKCE_S256_RoundTrip(t *testing.T) {
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	challenge := generateCodeChallenge(verifier)

	if !verifyCodeChallenge(verifier, challenge, "S256") {
		t.Fatal("expected PKCE verification to succeed")
	}
}

func TestPKCE_S256_WrongVerifier(t *testing.T) {
	verifier := "correct_verifier"
	challenge := generateCodeChallenge(verifier)

	if verifyCodeChallenge("wrong_verifier", challenge, "S256") {
		t.Fatal("expected PKCE verification to fail with wrong verifier")
	}
}

func TestPKCE_UnsupportedMethod(t *testing.T) {
	if verifyCodeChallenge("anything", "anything", "plain") {
		t.Fatal("expected PKCE verification to fail with unsupported method")
	}
}

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestGenerateClientSecret_NonEmpty(t *testing.T) {
	s := generateClientSecret()
	if len(s) == 0 {
		t.Fatal("expected non-empty client secret")
	}
	// Should be hex-encoded 32 bytes = 64 chars
	if len(s) != 64 {
		t.Fatalf("expected 64 char hex string, got %d chars", len(s))
	}
}

func TestGenerateAuthCode_Unique(t *testing.T) {
	a := generateAuthCode()
	b := generateAuthCode()
	if a == b {
		t.Fatal("expected unique auth codes")
	}
}

func TestGenerateRefreshToken_NonEmpty(t *testing.T) {
	tok := generateRefreshToken()
	if len(tok) == 0 {
		t.Fatal("expected non-empty refresh token")
	}
}

func TestHashSecret_Deterministic(t *testing.T) {
	secret := "my-secret-value"
	h1 := hashSecret(secret)
	h2 := hashSecret(secret)
	if h1 != h2 {
		t.Fatal("expected identical hashes for same input")
	}
}

func TestHashSecret_DifferentInputs(t *testing.T) {
	h1 := hashSecret("secret-a")
	h2 := hashSecret("secret-b")
	if h1 == h2 {
		t.Fatal("expected different hashes for different inputs")
	}
}

// ---------------------------------------------------------------------------
// Auth code expiry test
// ---------------------------------------------------------------------------

func TestTokenHandler_AuthCodeGrant_ExpiredCode(t *testing.T) {
	env := setupOAuthEnv(t)
	ctx := context.Background()

	codeVerifier := "verifier_for_expired_test"
	codeChallenge := generateCodeChallenge(codeVerifier)

	// Create an expired auth code directly in the DB
	codeVal := generateAuthCode()
	authCode := &model.OAuthAuthorizationCode{
		Code:                codeVal,
		ClientID:            env.client.ClientID,
		UserID:              env.user.ID,
		RedirectURI:         "https://example.com/callback",
		Scope:               "read",
		CodeChallenge:       &codeChallenge,
		CodeChallengeMethod: codeChallengeMethodS256,
		ExpiresAt:           time.Now().UTC().Add(-1 * time.Hour), // already expired
	}
	if err := env.oauthRepo.CreateAuthCode(ctx, authCode); err != nil {
		t.Fatalf("failed to create expired auth code: %v", err)
	}

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", codeVal)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("client_id", env.client.ClientID)
	form.Set("code_verifier", codeVerifier)

	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	env.server.TokenHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var errResp oauthError
	json.NewDecoder(rec.Body).Decode(&errResp)
	if errResp.Error != "invalid_grant" {
		t.Fatalf("expected error=invalid_grant, got %q", errResp.Error)
	}
}
