package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- mock OAuthAdminStore ---

type mockOAuthAdminStore struct {
	clients      []model.OAuthClient
	client       *model.OAuthClient
	clientSecret string
	idps         []model.OAuthIdPConfig
	idp          *model.OAuthIdPConfig

	listClientsErr  error
	createClientErr error
	deleteClientErr error
	listIdPsErr     error
	createIdPErr    error
	deleteIdPErr    error

	// capture args
	createdName         string
	createdRedirectURIs []string
	createdClientType   string
	deletedClientID     uuid.UUID
	createdIdPReq       CreateIdPRequest
	deletedIdPID        uuid.UUID
}

func (m *mockOAuthAdminStore) ListAllClients(_ context.Context) ([]model.OAuthClient, error) {
	return m.clients, m.listClientsErr
}

func (m *mockOAuthAdminStore) CreateClient(_ context.Context, name string, redirectURIs []string, clientType string) (*model.OAuthClient, string, error) {
	m.createdName = name
	m.createdRedirectURIs = redirectURIs
	m.createdClientType = clientType
	return m.client, m.clientSecret, m.createClientErr
}

func (m *mockOAuthAdminStore) DeleteClient(_ context.Context, id uuid.UUID) error {
	m.deletedClientID = id
	return m.deleteClientErr
}

func (m *mockOAuthAdminStore) ListIdPs(_ context.Context) ([]model.OAuthIdPConfig, error) {
	return m.idps, m.listIdPsErr
}

func (m *mockOAuthAdminStore) CreateIdP(_ context.Context, req CreateIdPRequest) (*model.OAuthIdPConfig, error) {
	m.createdIdPReq = req
	return m.idp, m.createIdPErr
}

func (m *mockOAuthAdminStore) DeleteIdP(_ context.Context, id uuid.UUID) error {
	m.deletedIdPID = id
	return m.deleteIdPErr
}

// --- tests ---

func TestAdminOAuthListClients(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	secret := "supersecret"
	store := &mockOAuthAdminStore{
		clients: []model.OAuthClient{
			{
				ID:             id1,
				ClientID:       "client-abc",
				ClientSecret:   &secret,
				Name:           "Test App",
				RedirectURIs:   []string{"http://localhost/callback"},
				GrantTypes:     []string{"authorization_code"},
				AutoRegistered: false,
				CreatedAt:      now,
			},
			{
				ID:             id2,
				ClientID:       "client-xyz",
				ClientSecret:   nil,
				Name:           "Public App",
				RedirectURIs:   []string{},
				GrantTypes:     []string{"authorization_code"},
				AutoRegistered: true,
				CreatedAt:      now,
			},
		},
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/oauth/clients", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp []oauthClientResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp) != 2 {
		t.Fatalf("expected 2 clients, got %d", len(resp))
	}
	if resp[0].Name != "Test App" {
		t.Errorf("expected name Test App, got %q", resp[0].Name)
	}
	if resp[0].ClientType != "confidential" {
		t.Errorf("expected client_type confidential, got %q", resp[0].ClientType)
	}
	if resp[0].Type != "manual" {
		t.Errorf("expected type manual, got %q", resp[0].Type)
	}
	if resp[1].ClientType != "public" {
		t.Errorf("expected client_type public, got %q", resp[1].ClientType)
	}
	if resp[1].Type != "auto" {
		t.Errorf("expected type auto, got %q", resp[1].Type)
	}
}

func TestAdminOAuthListClientsEmpty(t *testing.T) {
	store := &mockOAuthAdminStore{
		clients: []model.OAuthClient{},
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/oauth/clients", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp []oauthClientResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("expected 0 clients, got %d", len(resp))
	}
}

func TestAdminOAuthListClientsError(t *testing.T) {
	store := &mockOAuthAdminStore{
		listClientsErr: errors.New("database error"),
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/oauth/clients", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestAdminOAuthCreateClientConfidential(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	store := &mockOAuthAdminStore{
		client: &model.OAuthClient{
			ID:             id,
			ClientID:       "generated-client-id",
			Name:           "My App",
			RedirectURIs:   []string{"http://localhost/callback"},
			GrantTypes:     []string{"authorization_code", "refresh_token"},
			AutoRegistered: false,
			CreatedAt:      now,
		},
		clientSecret: "generated-secret",
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"name":"My App","redirect_uris":["http://localhost/callback"],"client_type":"confidential"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/clients", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp oauthClientCreatedResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.ID != id {
		t.Errorf("expected id %s, got %s", id, resp.ID)
	}
	if resp.Name != "My App" {
		t.Errorf("expected name My App, got %q", resp.Name)
	}
	if resp.ClientSecret == nil || *resp.ClientSecret != "generated-secret" {
		t.Errorf("expected client_secret generated-secret, got %v", resp.ClientSecret)
	}
	if resp.ClientType != "confidential" {
		t.Errorf("expected client_type confidential, got %q", resp.ClientType)
	}
	if store.createdName != "My App" {
		t.Errorf("expected created name My App, got %q", store.createdName)
	}
	if store.createdClientType != "confidential" {
		t.Errorf("expected created client_type confidential, got %q", store.createdClientType)
	}
}

func TestAdminOAuthCreateClientPublic(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	store := &mockOAuthAdminStore{
		client: &model.OAuthClient{
			ID:             id,
			ClientID:       "generated-client-id",
			Name:           "Public App",
			RedirectURIs:   []string{},
			GrantTypes:     []string{"authorization_code"},
			AutoRegistered: false,
			CreatedAt:      now,
		},
		clientSecret: "",
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"name":"Public App","redirect_uris":[],"client_type":"public"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/clients", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp oauthClientCreatedResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.ClientSecret != nil {
		t.Errorf("expected no client_secret for public client, got %v", resp.ClientSecret)
	}
	if resp.ClientType != "public" {
		t.Errorf("expected client_type public, got %q", resp.ClientType)
	}
	if store.createdClientType != "public" {
		t.Errorf("expected created client_type public, got %q", store.createdClientType)
	}
}

func TestAdminOAuthCreateClientMissingName(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"redirect_uris":["http://localhost/callback"],"client_type":"confidential"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/clients", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminOAuthCreateClientInvalidType(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"name":"App","client_type":"invalid"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/clients", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminOAuthDeleteClientSuccess(t *testing.T) {
	store := &mockOAuthAdminStore{}

	id := uuid.New()
	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/oauth/clients/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if store.deletedClientID != id {
		t.Errorf("expected deletedClientID %s, got %s", id, store.deletedClientID)
	}
}

func TestAdminOAuthDeleteClientNotFound(t *testing.T) {
	store := &mockOAuthAdminStore{
		deleteClientErr: errors.New("oauth client not found"),
	}

	id := uuid.New()
	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/oauth/clients/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestAdminOAuthDeleteClientInvalidUUID(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/oauth/clients/not-a-uuid", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminOAuthListIdPs(t *testing.T) {
	id1 := uuid.New()
	orgID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	issuer := "https://accounts.google.com"
	store := &mockOAuthAdminStore{
		idps: []model.OAuthIdPConfig{
			{
				ID:             id1,
				OrgID:          &orgID,
				ProviderType:   "google",
				ClientID:       "goog-client-id",
				IssuerURL:      &issuer,
				AllowedDomains: []string{"example.com"},
				AutoProvision:  true,
				DefaultRole:    "member",
				CreatedAt:      now,
				UpdatedAt:      now,
			},
		},
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/oauth/idp", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp []model.OAuthIdPConfig
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp) != 1 {
		t.Fatalf("expected 1 idp, got %d", len(resp))
	}
	if resp[0].ProviderType != "google" {
		t.Errorf("expected provider_type google, got %q", resp[0].ProviderType)
	}
}

func TestAdminOAuthListIdPsEmpty(t *testing.T) {
	store := &mockOAuthAdminStore{
		idps: []model.OAuthIdPConfig{},
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/oauth/idp", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp []model.OAuthIdPConfig
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("expected 0 idps, got %d", len(resp))
	}
}

func TestAdminOAuthCreateIdPSuccess(t *testing.T) {
	id := uuid.New()
	orgID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	issuer := "https://accounts.google.com"
	store := &mockOAuthAdminStore{
		idp: &model.OAuthIdPConfig{
			ID:             id,
			OrgID:          &orgID,
			ProviderType:   "google",
			ClientID:       "goog-client-id",
			IssuerURL:      &issuer,
			AllowedDomains: []string{"example.com"},
			AutoProvision:  true,
			DefaultRole:    "member",
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"org_id":"` + orgID.String() + `","provider_type":"google","client_id":"goog-client-id","client_secret":"goog-secret","issuer_url":"https://accounts.google.com","allowed_domains":["example.com"],"auto_provision":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/idp", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp model.OAuthIdPConfig
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.ID != id {
		t.Errorf("expected id %s, got %s", id, resp.ID)
	}
	if resp.ProviderType != "google" {
		t.Errorf("expected provider_type google, got %q", resp.ProviderType)
	}
	if store.createdIdPReq.ProviderType != "google" {
		t.Errorf("expected created provider_type google, got %q", store.createdIdPReq.ProviderType)
	}
	if store.createdIdPReq.ClientSecret != "goog-secret" {
		t.Errorf("expected created client_secret goog-secret, got %q", store.createdIdPReq.ClientSecret)
	}
}

func TestAdminOAuthCreateIdPMissingProviderType(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"client_id":"abc","client_secret":"xyz"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/idp", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminOAuthCreateIdPMissingClientID(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"provider_type":"google","client_secret":"xyz"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/idp", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminOAuthCreateIdPMissingClientSecret(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	body := `{"provider_type":"google","client_id":"abc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/oauth/idp", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminOAuthDeleteIdPSuccess(t *testing.T) {
	store := &mockOAuthAdminStore{}

	id := uuid.New()
	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/oauth/idp/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if store.deletedIdPID != id {
		t.Errorf("expected deletedIdPID %s, got %s", id, store.deletedIdPID)
	}
}

func TestAdminOAuthDeleteIdPNotFound(t *testing.T) {
	store := &mockOAuthAdminStore{
		deleteIdPErr: errors.New("idp not found"),
	}

	id := uuid.New()
	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/oauth/idp/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestAdminOAuthDeleteIdPInvalidUUID(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/oauth/idp/not-a-uuid", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminOAuthUnknownSubPath(t *testing.T) {
	store := &mockOAuthAdminStore{}

	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/oauth/unknown", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminOAuthMethodNotAllowed(t *testing.T) {
	store := &mockOAuthAdminStore{}
	h := NewAdminOAuthHandler(OAuthAdminConfig{Store: store})

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/v1/admin/oauth/clients"},
		{http.MethodDelete, "/v1/admin/oauth/clients"},
		{http.MethodPut, "/v1/admin/oauth/idp"},
		{http.MethodDelete, "/v1/admin/oauth/idp"},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, tt.path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("%s %s: expected 400, got %d", tt.method, tt.path, w.Code)
		}
	}
}
