package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// mockOrgIdPStore implements OrgIdPStore, recording which mutating operations
// were reached so a test can prove the write-time URL gate short-circuits before
// the store is touched.
type mockOrgIdPStore struct {
	createCalled bool
	updateCalled bool
	getCalled    bool
	getRes       *model.OAuthIdPConfig
}

func (m *mockOrgIdPStore) ListIdPsByOrg(context.Context, uuid.UUID) ([]model.OAuthIdPConfig, error) {
	return nil, nil
}

func (m *mockOrgIdPStore) CreateIdP(context.Context, *model.OAuthIdPConfig) error {
	m.createCalled = true
	return nil
}

func (m *mockOrgIdPStore) UpdateIdPByOrg(context.Context, *model.OAuthIdPConfig, uuid.UUID) error {
	m.updateCalled = true
	return nil
}

func (m *mockOrgIdPStore) GetIdPByID(context.Context, uuid.UUID) (*model.OAuthIdPConfig, error) {
	m.getCalled = true
	return m.getRes, nil
}

func (m *mockOrgIdPStore) DeleteIdPByOrg(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}

func TestOrgIdPCreateRejectsNonHTTPIssuer(t *testing.T) {
	org := uuid.New()
	store := &mockOrgIdPStore{}
	h := NewOrgIdPHandler(store)

	body := map[string]any{"provider_type": "oidc", "client_id": "cid", "client_secret": "sec", "issuer_url": "ftp://internal.example"}
	ac := &auth.AuthContext{UserID: uuid.New(), OrgID: org, Role: auth.RoleOrgOwner}
	w := doChiRequest(h, http.MethodPost, "/v1/orgs/"+org.String()+"/idp", map[string]string{"org_id": org.String()}, body, ac)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-http issuer_url, got %d; body: %s", w.Code, w.Body.String())
	}
	if store.createCalled {
		t.Error("CreateIdP must not be reached for an invalid issuer_url")
	}
}

func TestOrgIdPCreateAcceptsHTTPSIssuer(t *testing.T) {
	org := uuid.New()
	store := &mockOrgIdPStore{}
	h := NewOrgIdPHandler(store)

	body := map[string]any{"provider_type": "oidc", "client_id": "cid", "client_secret": "sec", "issuer_url": "https://idp.example.com"}
	ac := &auth.AuthContext{UserID: uuid.New(), OrgID: org, Role: auth.RoleOrgOwner}
	w := doChiRequest(h, http.MethodPost, "/v1/orgs/"+org.String()+"/idp", map[string]string{"org_id": org.String()}, body, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for a valid https issuer_url, got %d; body: %s", w.Code, w.Body.String())
	}
	if !store.createCalled {
		t.Error("CreateIdP should be reached for a valid config")
	}
}

func TestOrgIdPUpdateRejectsNonHTTPTokenURL(t *testing.T) {
	org := uuid.New()
	id := uuid.New()
	store := &mockOrgIdPStore{}
	h := NewOrgIdPHandler(store)

	body := map[string]any{"token_url": "ftp://internal.example/token"}
	ac := &auth.AuthContext{UserID: uuid.New(), OrgID: org, Role: auth.RoleOrgOwner}
	w := doChiRequest(h, http.MethodPut, "/v1/orgs/"+org.String()+"/idp/"+id.String(), map[string]string{"org_id": org.String()}, body, ac)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-http token_url, got %d; body: %s", w.Code, w.Body.String())
	}
	// The gate runs before the store is consulted at all.
	if store.getCalled {
		t.Error("GetIdPByID must not be reached for an invalid token_url")
	}
	if store.updateCalled {
		t.Error("UpdateIdPByOrg must not be reached for an invalid token_url")
	}
}
