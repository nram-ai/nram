package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// Guard the invariant that share-paste OAuth flows propagate share_token_id
// from authorization code → access JWT (stid claim) → refresh token row →
// next access JWT after refresh. A regression silently strips share scoping
// from OAuth-bound recipients, granting them full owner-equivalent access.

type shareSvcStub struct {
	share         *model.ShareToken
	grants        []model.ShareTokenGrant
	secret        string
	consumeCalled bool
}

func (s *shareSvcStub) Resolve(_ context.Context, rawSecret string) (*model.ShareToken, []model.ShareTokenGrant, error) {
	if rawSecret != s.secret || s.share.RevokedAt != nil {
		return nil, nil, errShareStubMismatch
	}
	if s.share.IsOneShot && s.share.ConsumedAt != nil {
		return nil, nil, errShareStubMismatch
	}
	return s.share, s.grants, nil
}

func (s *shareSvcStub) MarkConsumed(_ context.Context, _ uuid.UUID) error {
	s.consumeCalled = true
	now := time.Now().UTC()
	s.share.ConsumedAt = &now
	return nil
}

func (s *shareSvcStub) GetByID(_ context.Context, id uuid.UUID) (*model.ShareToken, error) {
	if s.share == nil || s.share.ID != id {
		return nil, errShareStubMismatch
	}
	return s.share, nil
}

type shareTestFixtures struct {
	env       *testOAuthEnv
	shareSvc  *shareSvcStub
	projectA  uuid.UUID
	rawSecret string
	share     *model.ShareToken
}

func setupShareConsent(t *testing.T) *shareTestFixtures {
	t.Helper()
	env := setupOAuthEnv(t)
	ctx := context.Background()

	projectA := uuid.New()
	shareID := uuid.New()
	expiresAt := time.Now().UTC().Add(24 * time.Hour)
	share := &model.ShareToken{
		ID:          shareID,
		OwnerUserID: env.user.ID,
		Name:        "Test share",
		IsOneShot:   false,
		ExpiresAt:   expiresAt,
		TokenPrefix: "abcd1234",
		TokenHash:   "fake-hash-for-fixture-only",
	}

	// Seed the share_tokens row directly so the FK constraints on
	// oauth_clients.share_token_id and oauth_refresh_tokens.share_token_id
	// are satisfied when BindClientToShare and CreateRefreshToken run. The
	// stub returns the share+grants in memory; this row exists only to make
	// the FK checks pass.
	insertShare := `INSERT INTO share_tokens (id, owner_user_id, token_hash, token_prefix, name, is_one_shot, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
	if _, err := env.db.Exec(ctx, insertShare,
		share.ID.String(), share.OwnerUserID.String(), share.TokenHash, share.TokenPrefix,
		share.Name, 0, share.ExpiresAt.Format(time.RFC3339),
	); err != nil {
		t.Fatalf("seed share_tokens: %v", err)
	}

	grants := []model.ShareTokenGrant{{
		ShareTokenID: share.ID,
		ProjectID:    projectA,
		Permission:   model.SharePermissionRead,
	}}
	secret := "nram_s_fixturesecretfixturesecretfixturesecretfixturesec"
	stub := &shareSvcStub{share: share, grants: grants, secret: secret}

	env.server.WithShareTokens(stub, nil)

	return &shareTestFixtures{
		env:       env,
		shareSvc:  stub,
		projectA:  projectA,
		rawSecret: secret,
		share:     share,
	}
}

var errShareStubMismatch = &shareStubErr{msg: "share secret mismatch"}

type shareStubErr struct{ msg string }

func (e *shareStubErr) Error() string { return e.msg }

func TestSharePasteConsent_MintsCodeWithShareID(t *testing.T) {
	f := setupShareConsent(t)
	ctx := context.Background()

	codeVerifier := "share-paste-consent-verifier-1234567890abcdefghij"
	codeChallenge := generateCodeChallenge(codeVerifier)

	form := url.Values{}
	form.Set("client_id", f.env.client.ClientID)
	form.Set("redirect_uri", "https://example.com/callback")
	form.Set("response_type", "code")
	form.Set("code_challenge", codeChallenge)
	form.Set("code_challenge_method", "S256")
	form.Set("share_token", f.rawSecret)
	form.Set("auth_mode", "share")
	form.Set("decision", "approve")

	req := httptest.NewRequest(http.MethodPost, "/authorize", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	f.env.server.AuthorizeHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("share consent: expected 302 redirect, got %d; body: %s", rec.Code, rec.Body.String())
	}

	loc, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse redirect: %v", err)
	}
	codeValue := loc.Query().Get("code")
	if codeValue == "" {
		t.Fatal("share consent: no code in redirect URL")
	}

	authCode, err := f.env.oauthRepo.GetAuthCode(ctx, codeValue)
	if err != nil {
		t.Fatalf("load auth code: %v", err)
	}
	if authCode.ShareTokenID == nil {
		t.Fatal("auth code missing share_token_id; consent flow did not propagate the share id")
	}
	if *authCode.ShareTokenID != f.share.ID {
		t.Fatalf("auth code share_token_id = %s, want %s", *authCode.ShareTokenID, f.share.ID)
	}
}

// Load-bearing for the 2026-05-27 fix that closed the silent share-scoping
// bypass: the access JWT must carry stid, the refresh row must carry
// share_token_id, and the OAuth client must be bound to the share.
func TestTokenHandler_SharePastedCode_PropagatesShareID(t *testing.T) {
	f := setupShareConsent(t)
	ctx := context.Background()

	codeVerifier := "share-token-mint-verifier-abcdefghijklmnopqrst"
	codeChallenge := generateCodeChallenge(codeVerifier)

	consentForm := url.Values{}
	consentForm.Set("client_id", f.env.client.ClientID)
	consentForm.Set("redirect_uri", "https://example.com/callback")
	consentForm.Set("response_type", "code")
	consentForm.Set("code_challenge", codeChallenge)
	consentForm.Set("code_challenge_method", "S256")
	consentForm.Set("share_token", f.rawSecret)
	consentForm.Set("auth_mode", "share")
	consentForm.Set("decision", "approve")

	req := httptest.NewRequest(http.MethodPost, "/authorize", strings.NewReader(consentForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	f.env.server.AuthorizeHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("consent: expected 302, got %d", rec.Code)
	}
	loc, _ := url.Parse(rec.Header().Get("Location"))
	codeValue := loc.Query().Get("code")
	if codeValue == "" {
		t.Fatal("no code in consent redirect")
	}

	boundClient, err := f.env.oauthRepo.GetClientByID(ctx, f.env.client.ClientID)
	if err != nil {
		t.Fatalf("reload bound client: %v", err)
	}
	if boundClient.ShareTokenID == nil || *boundClient.ShareTokenID != f.share.ID {
		t.Fatalf("oauth client not bound to share after consent: %+v", boundClient.ShareTokenID)
	}

	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "authorization_code")
	tokenForm.Set("code", codeValue)
	tokenForm.Set("redirect_uri", "https://example.com/callback")
	tokenForm.Set("client_id", f.env.client.ClientID)
	tokenForm.Set("client_secret", "client-secret-raw")
	tokenForm.Set("code_verifier", codeVerifier)

	tokenReq := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(tokenForm.Encode()))
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenRec := httptest.NewRecorder()
	f.env.server.TokenHandler().ServeHTTP(tokenRec, tokenReq)
	if tokenRec.Code != http.StatusOK {
		t.Fatalf("token exchange: expected 200, got %d; body: %s", tokenRec.Code, tokenRec.Body.String())
	}

	var tokResp tokenResponse
	if err := json.NewDecoder(tokenRec.Body).Decode(&tokResp); err != nil {
		t.Fatalf("decode token response: %v", err)
	}

	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(tokResp.AccessToken, claims, func(t *jwt.Token) (any, error) {
		return testSecret, nil
	})
	if err != nil || !parsed.Valid {
		t.Fatalf("parse access JWT: %v", err)
	}
	if claims.ShareTokenID == "" {
		t.Fatal("access JWT missing stid claim; share-paste OAuth chain is unscoped (regression of 2026-05-27 fix)")
	}
	if claims.ShareTokenID != f.share.ID.String() {
		t.Fatalf("stid claim = %s, want %s", claims.ShareTokenID, f.share.ID)
	}

	refreshHash := hashSecret(tokResp.RefreshToken)
	stored, err := f.env.oauthRepo.GetRefreshToken(ctx, refreshHash)
	if err != nil {
		t.Fatalf("load refresh token row: %v", err)
	}
	if stored.ShareTokenID == nil {
		t.Fatal("refresh token missing share_token_id; rotation would strip share scoping")
	}
	if *stored.ShareTokenID != f.share.ID {
		t.Fatalf("refresh token share_token_id = %s, want %s", *stored.ShareTokenID, f.share.ID)
	}
}

// Refresh-token rotation must carry share_token_id forward on both the new
// access JWT and the new refresh row.
// TestShareTokenRepo_RevokeZeroGrantSharesForOwner verifies the sweep that
// the project-delete cascade calls post-commit: any non-revoked share whose
// grants table is empty gets its revoked_at stamped. Active shares with
// grants are left alone; already-revoked shares are not touched.
func TestShareTokenRepo_RevokeZeroGrantSharesForOwner(t *testing.T) {
	env := setupOAuthEnv(t)
	ctx := context.Background()

	repo := storage.NewShareTokenRepo(env.db)
	expiresAt := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)

	// Three shares: one with no grants (sweep target), one with a grant
	// (must remain active), one already revoked (must remain revoked).
	zeroGrantsID := uuid.New()
	activeID := uuid.New()
	preRevokedID := uuid.New()
	for _, s := range []struct {
		id   uuid.UUID
		hash string
		rev  bool
	}{
		{zeroGrantsID, "hash-zero", false},
		{activeID, "hash-active", false},
		{preRevokedID, "hash-revoked", true},
	} {
		insert := `INSERT INTO share_tokens (id, owner_user_id, token_hash, token_prefix, name, is_one_shot, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
		if _, err := env.db.Exec(ctx, insert,
			s.id.String(), env.user.ID.String(), s.hash, s.hash[:8], "share", 0, expiresAt,
		); err != nil {
			t.Fatalf("seed share %s: %v", s.id, err)
		}
		if s.rev {
			now := time.Now().UTC().Format(time.RFC3339)
			_, _ = env.db.Exec(ctx, `UPDATE share_tokens SET revoked_at = ? WHERE id = ?`, now, s.id.String())
		}
	}

	// Insert a project + grant for the "active" share.
	projectID := uuid.New()
	if _, err := env.db.Exec(ctx,
		`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, default_tags, settings) VALUES (?, ?, ?, ?, ?, '[]', '{}')`,
		projectID.String(), env.user.NamespaceID.String(), env.user.NamespaceID.String(), "P", "p-"+projectID.String()[:8],
	); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	if _, err := env.db.Exec(ctx,
		`INSERT INTO share_token_grants (share_token_id, project_id, permission) VALUES (?, ?, ?)`,
		activeID.String(), projectID.String(), "read",
	); err != nil {
		t.Fatalf("seed grant: %v", err)
	}

	revoked, err := repo.RevokeZeroGrantSharesForOwner(ctx, env.user.ID)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(revoked) != 1 || revoked[0] != zeroGrantsID {
		t.Fatalf("expected 1 freshly-revoked share (%s), got %v", zeroGrantsID, revoked)
	}

	// Verify the right share was revoked.
	checkRevoked := func(id uuid.UUID, want bool) {
		s, err := repo.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		got := s.RevokedAt != nil
		if got != want {
			t.Fatalf("share %s revoked=%v, want %v", id, got, want)
		}
	}
	checkRevoked(zeroGrantsID, true)
	checkRevoked(activeID, false)
	checkRevoked(preRevokedID, true)
}

func TestRefreshTokenGrant_PreservesShareID(t *testing.T) {
	f := setupShareConsent(t)
	ctx := context.Background()

	rawRefresh := generateRefreshToken()
	refreshHash := hashSecret(rawRefresh)
	expires := time.Now().UTC().Add(24 * time.Hour)
	stored := &model.OAuthRefreshToken{
		TokenHash:    refreshHash,
		ClientID:     f.env.client.ClientID,
		UserID:       f.env.user.ID,
		ShareTokenID: &f.share.ID,
		ExpiresAt:    &expires,
	}
	if err := f.env.oauthRepo.CreateRefreshToken(ctx, stored); err != nil {
		t.Fatalf("seed refresh token: %v", err)
	}

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", rawRefresh)
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	f.env.server.TokenHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh grant: expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var tokResp tokenResponse
	if err := json.NewDecoder(rec.Body).Decode(&tokResp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(tokResp.AccessToken, claims, func(t *jwt.Token) (any, error) {
		return testSecret, nil
	})
	if err != nil || !parsed.Valid {
		t.Fatalf("parse access JWT: %v", err)
	}
	if claims.ShareTokenID != f.share.ID.String() {
		t.Fatalf("rotated JWT stid = %q, want %q (refresh path strips share scoping)", claims.ShareTokenID, f.share.ID)
	}

	newHash := hashSecret(tokResp.RefreshToken)
	newRow, err := f.env.oauthRepo.GetRefreshToken(ctx, newHash)
	if err != nil {
		t.Fatalf("load rotated refresh: %v", err)
	}
	if newRow.ShareTokenID == nil || *newRow.ShareTokenID != f.share.ID {
		t.Fatalf("rotated refresh row missing share_token_id; got %v", newRow.ShareTokenID)
	}
}

// UserInfoHandler must NOT leak owner identity to a share-bearer caller. The
// recipient is a third party who must not learn the owner's display name,
// email, stable user UUID, or org assignment from /userinfo. Sub is rewritten
// to the share id so the response stays OIDC-valid without leaking the owner.
// In addition, the handler must re-resolve share.Active so a revoked or
// expired share fails closed within the JWT TTL rather than continuing to
// authenticate /userinfo for up to access_token_expiry after revoke.
func TestUserInfoHandler_ShareBearer_RedactsOwnerAndRevalidatesShare(t *testing.T) {
	postUserInfo := func(t *testing.T, env *testOAuthEnv, jwtStr string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/userinfo", nil)
		req.Header.Set("Authorization", "Bearer "+jwtStr)
		rec := httptest.NewRecorder()
		env.server.UserInfoHandler().ServeHTTP(rec, req)
		return rec
	}

	t.Run("redacts owner pii and rewrites sub", func(t *testing.T) {
		f := setupShareConsent(t)
		jwtStr, err := GenerateShareScopedJWT(f.env.user.ID, f.env.user.OrgID, f.env.user.Role, testSecret, time.Hour, "", &f.share.ID, "")
		if err != nil {
			t.Fatalf("mint share-scoped jwt: %v", err)
		}

		rec := postUserInfo(t, f.env, jwtStr)
		if rec.Code != http.StatusOK {
			t.Fatalf("share-scoped /userinfo: expected 200, got %d; body: %s", rec.Code, rec.Body.String())
		}

		var resp userInfoResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp.Sub != f.share.ID.String() {
			t.Fatalf("sub = %q, want share id %q (owner UUID must not leak)", resp.Sub, f.share.ID)
		}
		if resp.Sub == f.env.user.ID.String() {
			t.Fatalf("sub = owner UUID %q; recipient gets a stable re-identification key for the owner", resp.Sub)
		}
		if resp.Name != "" {
			t.Fatalf("name = %q, want empty (owner display name must not leak to share recipient)", resp.Name)
		}
		if resp.Email != "" {
			t.Fatalf("email = %q, want empty", resp.Email)
		}
		if resp.OrgID != "" {
			t.Fatalf("org_id = %q, want empty", resp.OrgID)
		}
		if resp.Role != "share_bearer" {
			t.Fatalf("role = %q, want share_bearer", resp.Role)
		}
	})

	t.Run("revoked share rejected", func(t *testing.T) {
		f := setupShareConsent(t)
		now := time.Now().UTC()
		f.share.RevokedAt = &now
		jwtStr, err := GenerateShareScopedJWT(f.env.user.ID, f.env.user.OrgID, f.env.user.Role, testSecret, time.Hour, "", &f.share.ID, "")
		if err != nil {
			t.Fatalf("mint share-scoped jwt: %v", err)
		}

		rec := postUserInfo(t, f.env, jwtStr)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("revoked share /userinfo: expected 401, got %d; body: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("expired share rejected", func(t *testing.T) {
		f := setupShareConsent(t)
		// Backdate expiry so share.Active(now) is false. The JWT itself is
		// still well within its own expiry; this proves the handler re-checks
		// share state, not just the access JWT.
		f.share.ExpiresAt = time.Now().UTC().Add(-time.Minute)
		jwtStr, err := GenerateShareScopedJWT(f.env.user.ID, f.env.user.OrgID, f.env.user.Role, testSecret, time.Hour, "", &f.share.ID, "")
		if err != nil {
			t.Fatalf("mint share-scoped jwt: %v", err)
		}

		rec := postUserInfo(t, f.env, jwtStr)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expired share /userinfo: expected 401, got %d; body: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("account jwt unaffected", func(t *testing.T) {
		// Sanity check: non-share JWTs continue to return full identity.
		// Without this guard, a refactor that gates ALL /userinfo through
		// shareTokens.GetByID would silently 401 every account-flow caller.
		env := setupOAuthEnv(t)
		jwtStr, err := GenerateJWT(env.user.ID, env.user.OrgID, env.user.Role, testSecret, time.Hour)
		if err != nil {
			t.Fatalf("mint account jwt: %v", err)
		}
		req := httptest.NewRequest(http.MethodGet, "/userinfo", nil)
		req.Header.Set("Authorization", "Bearer "+jwtStr)
		rec := httptest.NewRecorder()
		env.server.UserInfoHandler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("account /userinfo: expected 200, got %d; body: %s", rec.Code, rec.Body.String())
		}
		var resp userInfoResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp.Sub != env.user.ID.String() {
			t.Fatalf("account /userinfo sub = %q, want owner id %q", resp.Sub, env.user.ID)
		}
		if resp.Email != env.user.Email {
			t.Fatalf("account /userinfo email redacted unexpectedly: got %q, want %q", resp.Email, env.user.Email)
		}
		if resp.Name != env.user.DisplayName {
			t.Fatalf("account /userinfo name redacted unexpectedly: got %q, want %q", resp.Name, env.user.DisplayName)
		}
	})
}

func TestSharePreviewHandler_ReturnsGrantsWithoutConsuming(t *testing.T) {
	f := setupShareConsent(t)

	codeVerifier := "share-preview-verifier-0123456789abcdefghijkl"
	codeChallenge := generateCodeChallenge(codeVerifier)
	reqBody := map[string]string{
		"client_id":             f.env.client.ClientID,
		"redirect_uri":          "https://example.com/callback",
		"response_type":         "code",
		"code_challenge":        codeChallenge,
		"code_challenge_method": "S256",
		"share_token":           f.rawSecret,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/share/preview", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	f.env.server.SharePreviewHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OwnerName string `json:"owner_name"`
		ShareName string `json:"share_name"`
		IsOneShot bool   `json:"is_one_shot"`
		Grants    []struct {
			ProjectName string `json:"project_name"`
			Permission  string `json:"permission"`
		} `json:"grants"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ShareName != f.share.Name {
		t.Fatalf("share_name: got %q, want %q", resp.ShareName, f.share.Name)
	}
	if len(resp.Grants) != 1 {
		t.Fatalf("grants len: got %d, want 1", len(resp.Grants))
	}
	if f.shareSvc.consumeCalled {
		t.Fatalf("preview must not consume the share; MarkConsumed was called")
	}
}

func TestSharePreviewHandler_RejectsInvalidSecret(t *testing.T) {
	f := setupShareConsent(t)

	codeVerifier := "share-preview-bad-verifier-0123456789abcdefghi"
	codeChallenge := generateCodeChallenge(codeVerifier)
	reqBody := map[string]string{
		"client_id":             f.env.client.ClientID,
		"redirect_uri":          "https://example.com/callback",
		"response_type":         "code",
		"code_challenge":        codeChallenge,
		"code_challenge_method": "S256",
		"share_token":           "nram_s_obviouslynotarealsecret",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/share/preview", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	f.env.server.SharePreviewHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on bad secret, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestShareAcceptHandler_JSONResponse(t *testing.T) {
	f := setupShareConsent(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/share/accept?token="+f.rawSecret, nil)
	rec := httptest.NewRecorder()
	f.env.server.ShareAcceptHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected JSON content type, got %q", ct)
	}
	var resp struct {
		ShareName    string `json:"share_name"`
		MCPServerURL string `json:"mcp_server_url"`
		ShareToken   string `json:"share_token"`
		Error        string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.ShareName != f.share.Name {
		t.Fatalf("share_name: got %q, want %q", resp.ShareName, f.share.Name)
	}
	if resp.ShareToken != f.rawSecret {
		t.Fatalf("share_token not echoed; got %q", resp.ShareToken)
	}
	if !strings.HasSuffix(resp.MCPServerURL, "/mcp") {
		t.Fatalf("mcp_server_url should end in /mcp, got %q", resp.MCPServerURL)
	}
}

func TestShareAcceptHandler_RevokedShareFriendlyError(t *testing.T) {
	f := setupShareConsent(t)
	now := time.Now().UTC()
	f.share.RevokedAt = &now

	req := httptest.NewRequest(http.MethodGet, "/v1/share/accept?token="+f.rawSecret, nil)
	rec := httptest.NewRecorder()
	f.env.server.ShareAcceptHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (friendly error), got %d", rec.Code)
	}
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == "" {
		t.Fatalf("expected error message for revoked share; got body: %s", rec.Body.String())
	}
}
