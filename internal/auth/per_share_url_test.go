package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// Tests for per-share connector URLs (/mcp/{share_id}). The path segment is a
// cosmetic discriminator that lets a recipient add multiple shares (or a share
// alongside their own account) as distinct connectors in clients that key a
// connector by URL. These tests pin the four seams that make the path
// meaningful to auth: audience derivation, the bearer/credential path guard,
// resource validation, and path-scoped discovery.

func TestMcpAudiencePath(t *testing.T) {
	id := uuid.New()
	cases := []struct{ path, want string }{
		{"/mcp", "/mcp"},
		{"/mcp/", "/mcp"},
		{"/mcp/" + id.String(), "/mcp/" + id.String()},
		{"/v1/projects/x/memories", "/mcp"},
		{"/", "/mcp"},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, c.path, nil)
		if got := mcpAudiencePath(req); got != c.want {
			t.Errorf("mcpAudiencePath(%q) = %q, want %q", c.path, got, c.want)
		}
	}
	if got := mcpAudiencePath(nil); got != "/mcp" {
		t.Errorf("mcpAudiencePath(nil) = %q, want /mcp", got)
	}
}

func TestMcpPathShareID(t *testing.T) {
	id := uuid.New()
	okReq := httptest.NewRequest(http.MethodGet, "/mcp/"+id.String(), nil)
	if got, ok := mcpPathShareID(okReq); !ok || got != id {
		t.Fatalf("mcpPathShareID(/mcp/%s) = (%s,%v), want (%s,true)", id, got, ok, id)
	}
	for _, p := range []string{"/mcp", "/mcp/", "/mcp/not-a-uuid", "/mcp/" + id.String() + "/extra", "/other"} {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		if _, ok := mcpPathShareID(req); ok {
			t.Errorf("mcpPathShareID(%q) ok = true, want false", p)
		}
	}
}

// doAuthReq runs a request through the middleware handler and returns the
// status code. The handler under test is okHandler() (200 when auth passes).
func doAuthReq(h http.Handler, path, token string) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// A share-scoped OAuth token (aud + stid bound to one share) must authorize at
// that share's own /mcp/{id} URL and nowhere else.
func TestHandler_PerShareURL_TokenBoundToPath(t *testing.T) {
	owner := uuid.New()
	shareID := uuid.New()
	share := &model.ShareToken{ID: shareID, OwnerUserID: owner, ExpiresAt: time.Now().Add(24 * time.Hour)}
	grants := []model.ShareTokenGrant{{ShareTokenID: shareID, ProjectID: uuid.New(), Permission: model.SharePermissionRead}}
	lookup := &mockShareTokenLookup{share: share, grants: grants}

	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret, nil).
		WithShareTokens(nil, lookup)
	handler := mw.Handler(okHandler())

	resource := "http://example.com/mcp/" + shareID.String()
	token, err := GenerateShareScopedJWT(owner, uuid.Nil, "member", testSecret, time.Hour, resource, &shareID, "")
	if err != nil {
		t.Fatalf("generate share-scoped JWT: %v", err)
	}

	if code := doAuthReq(handler, "/mcp/"+shareID.String(), token); code != http.StatusOK {
		t.Fatalf("per-share token at its own URL: got %d, want 200", code)
	}
	if code := doAuthReq(handler, "/mcp/"+uuid.New().String(), token); code != http.StatusUnauthorized {
		t.Fatalf("per-share token replayed at another share URL: got %d, want 401", code)
	}
	if code := doAuthReq(handler, "/mcp", token); code != http.StatusUnauthorized {
		t.Fatalf("per-share token at bare /mcp: got %d, want 401", code)
	}
}

// A non-share credential (own-account JWT) must be rejected on a per-share URL
// even when it carries no audience claim, but still works at the bare /mcp.
func TestHandler_NonShareCredentialOnPerShareURL_Rejected(t *testing.T) {
	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret, nil)
	handler := mw.Handler(okHandler())

	token, err := GenerateJWT(uuid.New(), uuid.Nil, "member", testSecret, time.Hour)
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}

	if code := doAuthReq(handler, "/mcp/"+uuid.New().String(), token); code != http.StatusUnauthorized {
		t.Fatalf("own-account token on per-share URL: got %d, want 401", code)
	}
	if code := doAuthReq(handler, "/mcp", token); code != http.StatusOK {
		t.Fatalf("own-account token at bare /mcp: got %d, want 200", code)
	}
}

// The bearer-direct path (nram_s_<secret>) skips audience validation, so the
// explicit path guard is what binds it to the share's URL.
func TestHandler_NramSBearer_PerShareURLGuard(t *testing.T) {
	owner := uuid.New()
	shareID := uuid.New()
	share := &model.ShareToken{ID: shareID, OwnerUserID: owner, ExpiresAt: time.Now().Add(24 * time.Hour)}
	grants := []model.ShareTokenGrant{{ShareTokenID: shareID, ProjectID: uuid.New(), Permission: model.SharePermissionRead}}
	secret := ShareTokenBearerPrefix + strings.Repeat("a", 48)
	validator := &mockShareTokenValidator{share: share, grants: grants, secret: secret}

	mw := NewAuthMiddleware(&mockAPIKeyValidator{}, &mockUserIdentityLookup{fixedRole: "member"}, testSecret, nil).
		WithShareTokens(validator, nil)
	handler := mw.Handler(okHandler())

	if code := doAuthReq(handler, "/mcp/"+shareID.String(), secret); code != http.StatusOK {
		t.Fatalf("nram_s_ at matching per-share URL: got %d, want 200", code)
	}
	if code := doAuthReq(handler, "/mcp/"+uuid.New().String(), secret); code != http.StatusUnauthorized {
		t.Fatalf("nram_s_ at mismatched per-share URL: got %d, want 401", code)
	}
	if code := doAuthReq(handler, "/mcp", secret); code != http.StatusOK {
		t.Fatalf("nram_s_ at bare /mcp: got %d, want 200", code)
	}
}

func TestValidateResource_AcceptsBareAndPerShare(t *testing.T) {
	f := setupShareConsent(t)
	req := httptest.NewRequest(http.MethodGet, "/authorize", nil)
	const base = "http://example.com"

	check := func(resource string, want bool) {
		t.Helper()
		if _, got := f.env.server.validateResource(req, resource); got != want {
			t.Errorf("validateResource(%q) ok=%v, want %v", resource, got, want)
		}
	}
	check("", true)
	check(base+"/mcp", true)
	check(base+"/mcp/"+f.share.ID.String(), true)
	check(base+"/mcp/"+uuid.New().String(), false) // unknown share
	check(base+"/mcp/not-a-uuid", false)
	check(base+"/evil", false)
}

func TestShareAcceptHandler_ReturnsPerShareURL(t *testing.T) {
	f := setupShareConsent(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/share/accept?token="+f.rawSecret, nil)
	rec := httptest.NewRecorder()
	f.env.server.ShareAcceptHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("accept handler: got %d", rec.Code)
	}
	var resp shareAcceptResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := "http://example.com/mcp/" + f.share.ID.String()
	if resp.MCPServerURL != want {
		t.Fatalf("mcp_server_url = %q, want %q", resp.MCPServerURL, want)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestProtectedResourceHandler_PathScoped(t *testing.T) {
	f := setupShareConsent(t)
	h := f.env.server.ProtectedResourceHandler()
	const base = "http://example.com"

	get := func(path string) (int, string) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var m protectedResourceMetadata
		_ = json.Unmarshal(rec.Body.Bytes(), &m)
		return rec.Code, m.Resource
	}

	if code, res := get("/.well-known/oauth-protected-resource"); code != http.StatusOK || res != base+"/mcp" {
		t.Fatalf("bare: code=%d res=%q", code, res)
	}
	if code, res := get("/.well-known/oauth-protected-resource/mcp/" + f.share.ID.String()); code != http.StatusOK || res != base+"/mcp/"+f.share.ID.String() {
		t.Fatalf("per-share: code=%d res=%q", code, res)
	}
	if code, _ := get("/.well-known/oauth-protected-resource/mcp/" + uuid.New().String()); code != http.StatusNotFound {
		t.Fatalf("unknown share: code=%d, want 404", code)
	}
	if code, _ := get("/.well-known/oauth-protected-resource/mcp/not-a-uuid"); code != http.StatusNotFound {
		t.Fatalf("non-uuid: code=%d, want 404", code)
	}
}

// completeShareAuthorize must reject a resource indicator that names a share
// other than the one whose secret was pasted (URL/credential cross-wiring).
func TestCompleteShareAuthorize_RejectsResourceForDifferentShare(t *testing.T) {
	f := setupShareConsent(t)
	req := httptest.NewRequest(http.MethodPost, "/authorize", nil)
	params := authorizeRequestParams{
		ClientID:    f.env.client.ClientID,
		RedirectURI: "https://example.com/callback",
		Resource:    "http://example.com/mcp/" + uuid.New().String(),
	}
	_, err := f.env.server.completeShareAuthorize(req, params, f.rawSecret)
	var re *redirectError
	if !errors.As(err, &re) || re.Code != "invalid_target" {
		t.Fatalf("err = %v, want invalid_target redirectError", err)
	}
}

// completeAccountAuthorize must reject a per-share resource (account tokens
// target the bare /mcp), but allow the bare resource through to the session
// check.
func TestCompleteAccountAuthorize_ResourceGuard(t *testing.T) {
	f := setupShareConsent(t)
	req := httptest.NewRequest(http.MethodGet, "/authorize", nil)

	perShare := authorizeRequestParams{Resource: "http://example.com/mcp/" + f.share.ID.String()}
	_, err := f.env.server.completeAccountAuthorize(req, perShare)
	var re *redirectError
	if !errors.As(err, &re) || re.Code != "invalid_target" {
		t.Fatalf("per-share resource: err = %v, want invalid_target redirectError", err)
	}

	bare := authorizeRequestParams{Resource: "http://example.com/mcp"}
	_, err = f.env.server.completeAccountAuthorize(req, bare)
	if !errors.Is(err, errAuthorizeNeedsLogin) {
		t.Fatalf("bare resource: err = %v, want errAuthorizeNeedsLogin (resource allowed, no session)", err)
	}
}
