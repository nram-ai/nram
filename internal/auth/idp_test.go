package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// makeIDToken builds an id_token from the given claims. The signature is
// irrelevant: the IdP handler verifies claims, not the signature (OIDC Core
// 3.1.3.7 direct-TLS exception), and parses with ParseUnverified.
func makeIDToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("test-signing-key"))
	if err != nil {
		t.Fatalf("sign id_token: %v", err)
	}
	return s
}

func strptr(s string) *string { return &s }

// signAndParse round-trips claims through a signed JWT and ParseUnverified so
// the resulting MapClaims carry the same JSON-decoded types (numbers as
// float64) the handler sees in production. Building a MapClaims literal by hand
// would leave int64s that the jwt typed accessors reject.
func signAndParse(t *testing.T, claims jwt.MapClaims) jwt.MapClaims {
	t.Helper()
	tok := makeIDToken(t, claims)
	parsed := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(tok, parsed); err != nil {
		t.Fatalf("parse: %v", err)
	}
	return parsed
}

// --- validateIDTokenClaims -------------------------------------------------

func TestValidateIDTokenClaims(t *testing.T) {
	issuer := "https://idp.example.com"
	discoveryCfg := &model.OAuthIdPConfig{ClientID: "client-abc", IssuerURL: strptr(issuer)}
	explicitCfg := &model.OAuthIdPConfig{ClientID: "client-abc"} // no issuer (explicit-endpoint mode)
	const nonce = "the-expected-nonce"

	future := time.Now().Add(time.Hour).Unix()

	base := func() jwt.MapClaims {
		return jwt.MapClaims{
			"iss":   issuer,
			"aud":   "client-abc",
			"exp":   future,
			"nonce": nonce,
			"email": "user@idp.example.com",
		}
	}

	tests := []struct {
		name    string
		cfg     *model.OAuthIdPConfig
		nonce   string
		mutate  func(jwt.MapClaims)
		wantErr bool
	}{
		{name: "valid discovery", cfg: discoveryCfg, nonce: nonce, mutate: func(jwt.MapClaims) {}},
		{name: "valid aud as array", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			c["aud"] = []string{"someone-else", "client-abc"}
		}},
		{name: "wrong aud", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			c["aud"] = "another-client"
		}, wantErr: true},
		{name: "missing aud", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			delete(c, "aud")
		}, wantErr: true},
		{name: "wrong iss", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			c["iss"] = "https://evil.example.com"
		}, wantErr: true},
		{name: "expired exp", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			c["exp"] = time.Now().Add(-time.Hour).Unix()
		}, wantErr: true},
		{name: "missing exp", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			delete(c, "exp")
		}, wantErr: true},
		{name: "nbf in the future", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			c["nbf"] = time.Now().Add(time.Hour).Unix()
		}, wantErr: true},
		{name: "missing nonce", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			delete(c, "nonce")
		}, wantErr: true},
		{name: "mismatched nonce", cfg: discoveryCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			c["nonce"] = "attacker-nonce"
		}, wantErr: true},
		{name: "explicit mode skips iss check", cfg: explicitCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			c["iss"] = "anything-goes"
		}},
		{name: "explicit mode with no iss claim", cfg: explicitCfg, nonce: nonce, mutate: func(c jwt.MapClaims) {
			delete(c, "iss")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claims := base()
			tc.mutate(claims)
			err := validateIDTokenClaims(signAndParse(t, claims), tc.cfg, tc.nonce)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

// parseIDTokenClaims wires ParseUnverified to validateIDTokenClaims and
// extractUserInfoFromClaims; confirm the round-trip on a good and a bad token.
func TestParseIDTokenClaims(t *testing.T) {
	cfg := &model.OAuthIdPConfig{ClientID: "client-abc", IssuerURL: strptr("https://idp.example.com")}
	h := &IdPHandler{}

	good := makeIDToken(t, jwt.MapClaims{
		"iss": "https://idp.example.com", "aud": "client-abc",
		"exp": time.Now().Add(time.Hour).Unix(), "nonce": "n1",
		"email": "user@idp.example.com", "email_verified": true, "name": "Test User",
	})
	info, err := h.parseIDTokenClaims(good, cfg, "n1")
	if err != nil {
		t.Fatalf("good token rejected: %v", err)
	}
	if info.Email != "user@idp.example.com" || !info.EmailVerified || info.Name != "Test User" {
		t.Fatalf("unexpected info: %+v", info)
	}

	bad := makeIDToken(t, jwt.MapClaims{
		"iss": "https://idp.example.com", "aud": "wrong-audience",
		"exp": time.Now().Add(time.Hour).Unix(), "nonce": "n1",
	})
	if _, err := h.parseIDTokenClaims(bad, cfg, "n1"); err == nil {
		t.Fatalf("expected wrong-aud token to be rejected")
	}
}

// --- extractUserInfoFromClaims (email_verified handling) -------------------

func TestExtractUserInfoEmailVerified(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{"bool true", true, true},
		{"bool false", false, false},
		{"string true", "true", true},
		{"string True", "True", true},
		{"string false", "false", false},
		{"absent", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claims := map[string]any{"email": "a@b.c", "name": "A B"}
			if tc.value != nil {
				claims["email_verified"] = tc.value
			}
			info := extractUserInfoFromClaims(claims)
			if info.EmailVerified != tc.want {
				t.Fatalf("email_verified=%v -> EmailVerified=%v, want %v", tc.value, info.EmailVerified, tc.want)
			}
			if info.Email != "a@b.c" {
				t.Fatalf("email not extracted: %q", info.Email)
			}
		})
	}
}

// --- CallbackHandler end-to-end --------------------------------------------

type fakeIdPConfigRepo struct{ cfg *model.OAuthIdPConfig }

func (f *fakeIdPConfigRepo) GetIdPByID(_ context.Context, _ uuid.UUID) (*model.OAuthIdPConfig, error) {
	return f.cfg, nil
}

type fakeUserRepo struct {
	user            *model.User
	getErr          error
	lastLoginCalled bool
}

func (f *fakeUserRepo) GetByEmail(_ context.Context, _ string) (*model.User, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.user, nil
}

func (f *fakeUserRepo) UpdateLastLogin(_ context.Context, _ uuid.UUID) error {
	f.lastLoginCalled = true
	return nil
}

type fakeUserCreator struct{ created *model.User }

func (f *fakeUserCreator) CreateUser(_ context.Context, email, displayName, _ string, role string, orgID uuid.UUID) (*model.User, error) {
	u := &model.User{ID: uuid.New(), Email: email, DisplayName: displayName, Role: role, OrgID: orgID}
	f.created = u
	return u, nil
}

// callbackFixture spins up a fake IdP token endpoint returning the supplied
// id_token, wires an IdPHandler with explicit endpoint URLs (no discovery), and
// seeds a pending state so CallbackHandler can be driven directly.
func callbackFixture(t *testing.T, idToken string, userRepo *fakeUserRepo) (*IdPHandler, *model.OAuthIdPConfig, string) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "fake-access-token",
				"token_type":   "Bearer",
				"id_token":     idToken,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	orgID := uuid.New()
	cfg := &model.OAuthIdPConfig{
		ID:            uuid.New(),
		OrgID:         &orgID,
		ProviderType:  "oidc",
		ClientID:      "client-abc",
		ClientSecret:  "shhh",
		AuthorizeURL:  strptr(srv.URL + "/authorize"),
		TokenURL:      strptr(srv.URL + "/token"),
		AutoProvision: true,
		DefaultRole:   "member",
	}

	h := NewIdPHandler(IdPHandlerConfig{
		IdPRepo:    &fakeIdPConfigRepo{cfg: cfg},
		UserRepo:   userRepo,
		UserCreate: &fakeUserCreator{},
		JWTSecret:  []byte("0123456789abcdef0123456789abcdef"),
	})
	t.Cleanup(h.Close)

	stateKey := "state-key-123"
	if ok := h.stateStore.Set(stateKey, &idpState{
		IdPID:     cfg.ID,
		Nonce:     "the-expected-nonce",
		ExpiresAt: time.Now().Add(idpStateExpiry),
	}); !ok {
		t.Fatal("failed to seed state")
	}

	return h, cfg, stateKey
}

func driveCallback(t *testing.T, h *IdPHandler, stateKey string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/auth/idp/callback?code=authcode&state="+stateKey, nil)
	rec := httptest.NewRecorder()
	h.CallbackHandler()(rec, req)
	return rec.Result()
}

func sessionCookie(resp *http.Response) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == "nram_session" {
			return c
		}
	}
	return nil
}

func existingUser() *model.User {
	return &model.User{ID: uuid.New(), Email: "user@idp.example.com", DisplayName: "User", Role: "member", OrgID: uuid.New()}
}

// TestCallbackIDTokenPath drives CallbackHandler for the OIDC id_token flow: a
// verified identity logs in and sets the session cookie, while every claim
// failure (unverified email, wrong aud, mismatched nonce) is rejected with no
// cookie. Unverified emails must be refused whether or not the account already
// exists (auto-provisioning must not create one either).
func TestCallbackIDTokenPath(t *testing.T) {
	goodClaims := func() jwt.MapClaims {
		return jwt.MapClaims{
			"aud": "client-abc", "exp": time.Now().Add(time.Hour).Unix(),
			"nonce": "the-expected-nonce", "email": "user@idp.example.com", "email_verified": true,
		}
	}

	tests := []struct {
		name       string
		mutate     func(jwt.MapClaims)
		userRepo   *fakeUserRepo
		wantStatus int
		wantCookie bool
	}{
		{"verified email logs in", nil, &fakeUserRepo{user: existingUser()}, http.StatusFound, true},
		{"unverified email rejected", func(c jwt.MapClaims) { c["email_verified"] = false }, &fakeUserRepo{user: existingUser()}, http.StatusForbidden, false},
		{"unverified email is not auto-provisioned", func(c jwt.MapClaims) { c["email"] = "new@idp.example.com"; c["email_verified"] = false }, &fakeUserRepo{getErr: sql.ErrNoRows}, http.StatusForbidden, false},
		{"nonce mismatch rejected", func(c jwt.MapClaims) { c["nonce"] = "attacker-controlled-nonce" }, &fakeUserRepo{user: existingUser()}, http.StatusBadGateway, false},
		{"wrong audience rejected", func(c jwt.MapClaims) { c["aud"] = "some-other-client" }, &fakeUserRepo{user: existingUser()}, http.StatusBadGateway, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claims := goodClaims()
			if tc.mutate != nil {
				tc.mutate(claims)
			}
			h, _, stateKey := callbackFixture(t, makeIDToken(t, claims), tc.userRepo)

			resp := driveCallback(t, h, stateKey)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if gotCookie := sessionCookie(resp) != nil; gotCookie != tc.wantCookie {
				t.Fatalf("session cookie present = %v, want %v", gotCookie, tc.wantCookie)
			}
			if tc.userRepo.lastLoginCalled != tc.wantCookie {
				t.Fatalf("UpdateLastLogin called = %v, want %v", tc.userRepo.lastLoginCalled, tc.wantCookie)
			}
		})
	}
}

// TestCallbackRedirectTarget verifies the post-login 302 target is constrained
// to a same-origin root-relative path: an off-origin ?redirect= is dropped to
// "/", while a legitimate relative path is preserved. The successful login must
// still emit the 302 and the session cookie so the guard cannot regress the
// happy path. The evil cases fail against the pre-fix code, which passed the
// stored value straight to http.Redirect.
func TestCallbackRedirectTarget(t *testing.T) {
	tests := []struct {
		name     string
		redirect string
		want     string
	}{
		{"absolute off-origin dropped", "https://evil.example", "/"},
		{"protocol-relative dropped", "//evil.example", "/"},
		{"empty defaults to root", "", "/"},
		{"relative path preserved", "/dashboard", "/dashboard"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claims := jwt.MapClaims{
				"aud": "client-abc", "exp": time.Now().Add(time.Hour).Unix(),
				"nonce": "the-expected-nonce", "email": "user@idp.example.com", "email_verified": true,
			}
			h, cfg, stateKey := callbackFixture(t, makeIDToken(t, claims), &fakeUserRepo{user: existingUser()})

			// Re-seed the consumed-once state with the redirect under test.
			if ok := h.stateStore.Set(stateKey, &idpState{
				IdPID:       cfg.ID,
				RedirectURL: tc.redirect,
				Nonce:       "the-expected-nonce",
				ExpiresAt:   time.Now().Add(idpStateExpiry),
			}); !ok {
				t.Fatal("failed to seed state")
			}

			resp := driveCallback(t, h, stateKey)

			if resp.StatusCode != http.StatusFound {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusFound)
			}
			if got := resp.Header.Get("Location"); got != tc.want {
				t.Fatalf("Location = %q, want %q", got, tc.want)
			}
			if sessionCookie(resp) == nil {
				t.Fatal("expected session cookie on successful login")
			}
		})
	}
}

// callbackFixtureUserinfo wires an IdPHandler for the OAuth/userinfo flow (no
// id_token): the token endpoint returns only an access token, and the userinfo
// and /emails endpoints return the supplied bodies. Auto-provisioning is on so a
// verified email logs in without a pre-existing account.
func callbackFixtureUserinfo(t *testing.T, userinfo map[string]any, emails []map[string]any) (*IdPHandler, string) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "fake-access-token", "token_type": "Bearer"})
		case "/userinfo":
			_ = json.NewEncoder(w).Encode(userinfo)
		case "/userinfo/emails":
			_ = json.NewEncoder(w).Encode(emails)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	orgID := uuid.New()
	cfg := &model.OAuthIdPConfig{
		ID:            uuid.New(),
		OrgID:         &orgID,
		ProviderType:  "oidc",
		ClientID:      "client-abc",
		ClientSecret:  "shhh",
		AuthorizeURL:  strptr(srv.URL + "/authorize"),
		TokenURL:      strptr(srv.URL + "/token"),
		UserinfoURL:   strptr(srv.URL + "/userinfo"),
		AutoProvision: true,
		DefaultRole:   "member",
	}

	h := NewIdPHandler(IdPHandlerConfig{
		IdPRepo:    &fakeIdPConfigRepo{cfg: cfg},
		UserRepo:   &fakeUserRepo{getErr: sql.ErrNoRows},
		UserCreate: &fakeUserCreator{},
		JWTSecret:  []byte("0123456789abcdef0123456789abcdef"),
	})
	t.Cleanup(h.Close)

	stateKey := "state-key-123"
	if ok := h.stateStore.Set(stateKey, &idpState{IdPID: cfg.ID, Nonce: "the-expected-nonce", ExpiresAt: time.Now().Add(idpStateExpiry)}); !ok {
		t.Fatal("failed to seed state")
	}
	return h, stateKey
}

// TestCallbackUserinfoPath covers the OAuth/userinfo flow where verification
// comes from the /emails endpoint. The verified-email gate must stay
// authoritative: an address confirmed verified logs in, but an /emails response
// whose only address is unverified must be refused (the altitude fix — the
// verified flag from /emails is propagated instead of blindly assumed true).
func TestCallbackUserinfoPath(t *testing.T) {
	tests := []struct {
		name       string
		userinfo   map[string]any
		emails     []map[string]any
		wantStatus int
		wantCookie bool
	}{
		{
			name:       "private userinfo email, /emails confirms verified",
			userinfo:   map[string]any{"email": "", "name": "GH User"},
			emails:     []map[string]any{{"email": "gh@example.com", "primary": true, "verified": true}},
			wantStatus: http.StatusFound,
			wantCookie: true,
		},
		{
			name:       "userinfo email without email_verified, /emails confirms verified",
			userinfo:   map[string]any{"email": "gh@example.com", "name": "GH User"},
			emails:     []map[string]any{{"email": "gh@example.com", "primary": true, "verified": true}},
			wantStatus: http.StatusFound,
			wantCookie: true,
		},
		{
			name:       "only unverified address is refused",
			userinfo:   map[string]any{"email": "", "name": "GH User"},
			emails:     []map[string]any{{"email": "gh@example.com", "primary": true, "verified": false}},
			wantStatus: http.StatusForbidden,
			wantCookie: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, stateKey := callbackFixtureUserinfo(t, tc.userinfo, tc.emails)
			resp := driveCallback(t, h, stateKey)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if gotCookie := sessionCookie(resp) != nil; gotCookie != tc.wantCookie {
				t.Fatalf("session cookie present = %v, want %v", gotCookie, tc.wantCookie)
			}
		})
	}
}
