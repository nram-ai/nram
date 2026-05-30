package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// --- mock implementations ---

type mockAPIKeyManager struct {
	createFn     func(ctx context.Context, key *model.APIKey) (string, error)
	listByUserFn func(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error)
	revokeFn     func(ctx context.Context, id uuid.UUID) error
	getByIDFn    func(ctx context.Context, id uuid.UUID) (*model.APIKey, error)
}

func (m *mockAPIKeyManager) Create(ctx context.Context, key *model.APIKey) (string, error) {
	if m.createFn != nil {
		return m.createFn(ctx, key)
	}
	key.ID = uuid.New()
	key.KeyPrefix = "nram_k_abcdef"
	key.CreatedAt = time.Now().UTC()
	return "nram_k_abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678", nil
}

func (m *mockAPIKeyManager) ListByUser(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error) {
	if m.listByUserFn != nil {
		return m.listByUserFn(ctx, userID)
	}
	return nil, nil
}

func (m *mockAPIKeyManager) Revoke(ctx context.Context, id uuid.UUID) error {
	if m.revokeFn != nil {
		return m.revokeFn(ctx, id)
	}
	return nil
}

func (m *mockAPIKeyManager) GetByID(ctx context.Context, id uuid.UUID) (*model.APIKey, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	return nil, fmt.Errorf("not found")
}

type mockOAuthClientManager struct {
	createClientFn      func(ctx context.Context, client *model.OAuthClient) error
	listClientsByUserFn func(ctx context.Context, userID uuid.UUID) ([]model.OAuthClient, error)
	deleteClientFn      func(ctx context.Context, clientID string) error
	getClientByIDFn     func(ctx context.Context, clientID string) (*model.OAuthClient, error)
}

func (m *mockOAuthClientManager) CreateClient(ctx context.Context, client *model.OAuthClient) error {
	if m.createClientFn != nil {
		return m.createClientFn(ctx, client)
	}
	client.ID = uuid.New()
	client.CreatedAt = time.Now().UTC()
	return nil
}

func (m *mockOAuthClientManager) ListClientsByUser(ctx context.Context, userID uuid.UUID) ([]model.OAuthClient, error) {
	if m.listClientsByUserFn != nil {
		return m.listClientsByUserFn(ctx, userID)
	}
	return nil, nil
}

func (m *mockOAuthClientManager) DeleteClient(ctx context.Context, clientID string) error {
	if m.deleteClientFn != nil {
		return m.deleteClientFn(ctx, clientID)
	}
	return nil
}

func (m *mockOAuthClientManager) GetClientByID(ctx context.Context, clientID string) (*model.OAuthClient, error) {
	if m.getClientByIDFn != nil {
		return m.getClientByIDFn(ctx, clientID)
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockOAuthClientManager) RevokeTokensForUserClient(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

// --- helpers ---

func doSelfServiceRequest(handler http.HandlerFunc, method, target string, body any, ac *auth.AuthContext) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}

	req := httptest.NewRequest(method, target, &buf)
	req.Header.Set("Content-Type", "application/json")

	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func doChiRequest(handler http.HandlerFunc, method, path string, params map[string]string, body any, ac *auth.AuthContext) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}

	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")

	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}

	// Set up chi route context for URL params.
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

// --- API Key tests ---

func TestMeAPIKeys_ListSuccess(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()
	now := time.Now().UTC()

	keys := &mockAPIKeyManager{
		listByUserFn: func(ctx context.Context, uid uuid.UUID) ([]model.APIKey, error) {
			if uid != userID {
				t.Errorf("expected userID %s, got %s", userID, uid)
			}
			return []model.APIKey{
				{
					ID:        keyID,
					UserID:    userID,
					KeyPrefix: "nram_k_abcdef",
					Name:      "Test Key",
					Scopes:    []uuid.UUID{},
					CreatedAt: now,
				},
			}, nil
		},
	}

	handler := NewMeAPIKeysHandler(keys)
	ac := &auth.AuthContext{UserID: userID, Role: "user"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/api-keys", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string][]model.APIKey
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	data := resp["data"]
	if len(data) != 1 {
		t.Fatalf("expected 1 key, got %d", len(data))
	}
	if data[0].Name != "Test Key" {
		t.Errorf("expected name 'Test Key', got %q", data[0].Name)
	}
}

func TestMeAPIKeys_CreateSuccess(t *testing.T) {
	userID := uuid.New()
	rawKey := "nram_k_test_raw_key_value_here_1234567890"

	keys := &mockAPIKeyManager{
		createFn: func(ctx context.Context, key *model.APIKey) (string, error) {
			if key.UserID != userID {
				t.Errorf("expected userID %s, got %s", userID, key.UserID)
			}
			if key.Name != "My API Key" {
				t.Errorf("expected name 'My API Key', got %q", key.Name)
			}
			key.ID = uuid.New()
			key.KeyPrefix = "nram_k_test_ra"
			key.CreatedAt = time.Now().UTC()
			return rawKey, nil
		},
	}

	handler := NewMeAPIKeysHandler(keys)
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]any{
		"name":   "My API Key",
		"scopes": []string{},
	}

	w := doSelfServiceRequest(handler, http.MethodPost, "/v1/me/api-keys", body, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp createAPIKeyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if resp.Key != rawKey {
		t.Errorf("expected key %q, got %q", rawKey, resp.Key)
	}
	if resp.Name != "My API Key" {
		t.Errorf("expected name 'My API Key', got %q", resp.Name)
	}
}

func TestMeAPIKeys_CreateMissingName(t *testing.T) {
	handler := NewMeAPIKeysHandler(&mockAPIKeyManager{})
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	body := map[string]any{
		"scopes": []string{},
	}

	w := doSelfServiceRequest(handler, http.MethodPost, "/v1/me/api-keys", body, ac)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request, got %+v", envelope.Error)
	}
}

func TestMeAPIKeys_RevokeSuccess(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()
	revoked := false

	keys := &mockAPIKeyManager{
		getByIDFn: func(ctx context.Context, id uuid.UUID) (*model.APIKey, error) {
			if id != keyID {
				return nil, fmt.Errorf("not found")
			}
			return &model.APIKey{
				ID:     keyID,
				UserID: userID,
				Name:   "Key to revoke",
			}, nil
		},
		revokeFn: func(ctx context.Context, id uuid.UUID) error {
			if id != keyID {
				t.Errorf("expected keyID %s, got %s", keyID, id)
			}
			revoked = true
			return nil
		},
	}

	handler := NewMeAPIKeyRevokeHandler(keys)
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	w := doChiRequest(handler, http.MethodDelete, "/v1/me/api-keys/"+keyID.String(), map[string]string{"id": keyID.String()}, nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]bool
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !resp["revoked"] {
		t.Error("expected revoked to be true")
	}
	if !revoked {
		t.Error("expected revoke function to be called")
	}
}

func TestMeAPIKeys_RevokeWrongUser(t *testing.T) {
	ownerID := uuid.New()
	attackerID := uuid.New()
	keyID := uuid.New()

	keys := &mockAPIKeyManager{
		getByIDFn: func(ctx context.Context, id uuid.UUID) (*model.APIKey, error) {
			return &model.APIKey{
				ID:     keyID,
				UserID: ownerID,
				Name:   "Not yours",
			}, nil
		},
	}

	handler := NewMeAPIKeyRevokeHandler(keys)
	ac := &auth.AuthContext{UserID: attackerID, Role: "user"}

	w := doChiRequest(handler, http.MethodDelete, "/v1/me/api-keys/"+keyID.String(), map[string]string{"id": keyID.String()}, nil, ac)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "forbidden" {
		t.Errorf("expected forbidden, got %+v", envelope.Error)
	}
}

// --- OAuth Client tests ---

func TestMeOAuthClients_ListSuccess(t *testing.T) {
	userID := uuid.New()
	clientUUID := uuid.New()
	now := time.Now().UTC()

	clients := &mockOAuthClientManager{
		listClientsByUserFn: func(ctx context.Context, uid uuid.UUID) ([]model.OAuthClient, error) {
			if uid != userID {
				t.Errorf("expected userID %s, got %s", userID, uid)
			}
			return []model.OAuthClient{
				{
					ID:           clientUUID,
					ClientID:     "nram_abc123",
					Name:         "My App",
					RedirectURIs: []string{"http://localhost:3000/callback"},
					GrantTypes:   []string{"authorization_code"},
					CreatedAt:    now,
				},
			}, nil
		},
	}

	handler := NewMeOAuthClientsHandler(clients)
	ac := &auth.AuthContext{UserID: userID, Role: "user"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/oauth-clients", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string][]model.OAuthClient
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	data := resp["data"]
	if len(data) != 1 {
		t.Fatalf("expected 1 client, got %d", len(data))
	}
	if data[0].Name != "My App" {
		t.Errorf("expected name 'My App', got %q", data[0].Name)
	}
}

func TestMeOAuthClients_CreateSuccess(t *testing.T) {
	userID := uuid.New()

	clients := &mockOAuthClientManager{
		createClientFn: func(ctx context.Context, client *model.OAuthClient) error {
			if client.Name != "My App" {
				t.Errorf("expected name 'My App', got %q", client.Name)
			}
			if client.ClientID == "" {
				t.Error("expected client_id to be generated")
			}
			if client.ClientSecret == nil {
				t.Error("expected client_secret to be set")
			}
			client.ID = uuid.New()
			client.CreatedAt = time.Now().UTC()
			return nil
		},
	}

	handler := NewMeOAuthClientsHandler(clients)
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]any{
		"name":          "My App",
		"redirect_uris": []string{"http://localhost:3000/callback"},
		"grant_types":   []string{"authorization_code"},
	}

	w := doSelfServiceRequest(handler, http.MethodPost, "/v1/me/oauth-clients", body, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp createOAuthClientResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if resp.Name != "My App" {
		t.Errorf("expected name 'My App', got %q", resp.Name)
	}
	if resp.ClientSecret == nil {
		t.Error("expected client_secret in response")
	}
	if resp.ClientID == "" {
		t.Error("expected client_id in response")
	}
}

func TestMeOAuthClients_DeleteSuccess(t *testing.T) {
	userID := uuid.New()
	clientUUID := uuid.New()
	clientIDStr := "nram_abc123"
	deleted := false

	clients := &mockOAuthClientManager{
		listClientsByUserFn: func(ctx context.Context, uid uuid.UUID) ([]model.OAuthClient, error) {
			return []model.OAuthClient{
				{
					ID:       clientUUID,
					ClientID: clientIDStr,
					Name:     "My App",
					UserID:   &userID,
				},
			}, nil
		},
		deleteClientFn: func(ctx context.Context, cid string) error {
			if cid != clientIDStr {
				t.Errorf("expected clientID %q, got %q", clientIDStr, cid)
			}
			deleted = true
			return nil
		},
	}

	handler := NewMeOAuthClientRevokeHandler(clients)
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	w := doChiRequest(handler, http.MethodDelete, "/v1/me/oauth-clients/"+clientUUID.String(), map[string]string{"id": clientUUID.String()}, nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]bool
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !resp["deleted"] {
		t.Error("expected deleted to be true")
	}
	if !deleted {
		t.Error("expected delete function to be called")
	}
}

func TestMeAPIKeys_Unauthenticated(t *testing.T) {
	handler := NewMeAPIKeysHandler(&mockAPIKeyManager{})

	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/api-keys", nil, nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "unauthorized" {
		t.Errorf("expected unauthorized, got %+v", envelope.Error)
	}
}

// --- Capabilities tests ---

type fakeCapabilityResolver struct {
	resolveFn func(ctx context.Context, key, scope string) bool
}

func (f *fakeCapabilityResolver) ResolveBool(ctx context.Context, key, scope string) bool {
	if f.resolveFn != nil {
		return f.resolveFn(ctx, key, scope)
	}
	return false
}

func TestMeCapabilities_ReturnsBothFlags(t *testing.T) {
	handler := NewMeCapabilitiesHandler(MeCapabilitiesConfig{
		EnrichmentAvailable: func() bool { return true },
		Settings: &fakeCapabilityResolver{
			resolveFn: func(_ context.Context, key, scope string) bool {
				if key != "dreaming.enabled" {
					t.Errorf("expected key dreaming.enabled, got %q", key)
				}
				if scope != "global" {
					t.Errorf("expected scope global, got %q", scope)
				}
				return true
			},
		},
	})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "member"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/capabilities", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp MeCapabilitiesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !resp.EnrichmentAvailable {
		t.Error("expected enrichment_available=true")
	}
	if !resp.DreamingEnabled {
		t.Error("expected dreaming_enabled=true")
	}
}

func TestMeCapabilities_AvailableClosed(t *testing.T) {
	handler := NewMeCapabilitiesHandler(MeCapabilitiesConfig{
		EnrichmentAvailable: func() bool { return false },
		Settings: &fakeCapabilityResolver{
			resolveFn: func(context.Context, string, string) bool { return false },
		},
	})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "readonly"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/capabilities", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp MeCapabilitiesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.EnrichmentAvailable || resp.DreamingEnabled {
		t.Errorf("expected both flags false, got %+v", resp)
	}
}

func TestMeCapabilities_Unauthenticated(t *testing.T) {
	handler := NewMeCapabilitiesHandler(MeCapabilitiesConfig{
		EnrichmentAvailable: func() bool { return true },
	})

	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/capabilities", nil, nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMeCapabilities_MethodNotAllowed(t *testing.T) {
	handler := NewMeCapabilitiesHandler(MeCapabilitiesConfig{
		EnrichmentAvailable: func() bool { return true },
	})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "member"}
	w := doSelfServiceRequest(handler, http.MethodPost, "/v1/me/capabilities", nil, ac)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMeCapabilities_NilDependenciesAreSafe(t *testing.T) {
	// A misconfigured handler (nil EnrichmentAvailable, nil Settings) should
	// still return 200 with both flags false rather than panic — the SPA
	// renders the closed-gate state gracefully.
	handler := NewMeCapabilitiesHandler(MeCapabilitiesConfig{})

	ac := &auth.AuthContext{UserID: uuid.New(), Role: "member"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/capabilities", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp MeCapabilitiesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.EnrichmentAvailable || resp.DreamingEnabled {
		t.Errorf("expected both flags false, got %+v", resp)
	}
}

// --- MeProfile / MeProfilePatch tests ---

type mockMeProfileRepo struct {
	user      *model.User
	updateErr error
	updated   *model.User
}

func (m *mockMeProfileRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	if m.user == nil {
		return nil, fmt.Errorf("not found")
	}
	cp := *m.user
	return &cp, nil
}

func (m *mockMeProfileRepo) Update(ctx context.Context, user *model.User) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	cp := *user
	m.updated = &cp
	m.user = &cp
	return nil
}

func TestMeProfile_GetReturnsThemeFromSettings(t *testing.T) {
	userID := uuid.New()
	repo := &mockMeProfileRepo{
		user: &model.User{
			ID:          userID,
			Email:       "u@example.com",
			DisplayName: "U",
			Role:        "member",
			OrgID:       uuid.New(),
			Settings:    json.RawMessage(`{"theme":"light"}`),
		},
	}

	handler := NewMeProfileHandler(repo)
	ac := &auth.AuthContext{UserID: userID, Role: "member"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/profile", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp SessionUser
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Theme != "light" {
		t.Errorf("expected theme=light, got %q", resp.Theme)
	}
}

func TestMeProfile_GetReturnsEmptyThemeWhenUnset(t *testing.T) {
	userID := uuid.New()
	repo := &mockMeProfileRepo{
		user: &model.User{
			ID:       userID,
			Email:    "u@example.com",
			Role:     "member",
			OrgID:    uuid.New(),
			Settings: json.RawMessage(`{}`),
		},
	}

	handler := NewMeProfileHandler(repo)
	ac := &auth.AuthContext{UserID: userID, Role: "member"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/profile", nil, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp SessionUser
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Theme != "" {
		t.Errorf("expected empty theme, got %q", resp.Theme)
	}
}

func TestMeProfilePatch_SetsTheme(t *testing.T) {
	userID := uuid.New()
	repo := &mockMeProfileRepo{
		user: &model.User{
			ID:       userID,
			Email:    "u@example.com",
			Role:     "member",
			OrgID:    uuid.New(),
			Settings: json.RawMessage(`{}`),
		},
	}

	handler := NewMeProfilePatchHandler(repo)
	ac := &auth.AuthContext{UserID: userID, Role: "member"}
	body := map[string]string{"theme": "dark"}
	w := doSelfServiceRequest(handler, http.MethodPatch, "/v1/me/profile", body, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp SessionUser
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Theme != "dark" {
		t.Errorf("expected theme=dark in response, got %q", resp.Theme)
	}
	if repo.updated == nil {
		t.Fatal("expected Update to be called")
	}
	if got := repo.updated.GetSettingString("theme"); got != "dark" {
		t.Errorf("expected persisted theme=dark, got %q", got)
	}
}

func TestMeProfilePatch_PreservesOtherSettings(t *testing.T) {
	userID := uuid.New()
	repo := &mockMeProfileRepo{
		user: &model.User{
			ID:       userID,
			Email:    "u@example.com",
			Role:     "member",
			OrgID:    uuid.New(),
			Settings: json.RawMessage(`{"locale":"en-GB","other":42}`),
		},
	}

	handler := NewMeProfilePatchHandler(repo)
	ac := &auth.AuthContext{UserID: userID, Role: "member"}
	body := map[string]string{"theme": "light"}
	w := doSelfServiceRequest(handler, http.MethodPatch, "/v1/me/profile", body, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var persisted map[string]any
	if err := json.Unmarshal(repo.updated.Settings, &persisted); err != nil {
		t.Fatalf("decode persisted settings: %v", err)
	}
	if persisted["theme"] != "light" {
		t.Errorf("expected theme=light, got %v", persisted["theme"])
	}
	if persisted["locale"] != "en-GB" {
		t.Errorf("expected locale=en-GB preserved, got %v", persisted["locale"])
	}
	if persisted["other"] != float64(42) {
		t.Errorf("expected other=42 preserved, got %v", persisted["other"])
	}
}

func TestMeProfilePatch_RejectsInvalidTheme(t *testing.T) {
	userID := uuid.New()
	repo := &mockMeProfileRepo{
		user: &model.User{ID: userID, Settings: json.RawMessage(`{}`)},
	}

	handler := NewMeProfilePatchHandler(repo)
	ac := &auth.AuthContext{UserID: userID, Role: "member"}
	body := map[string]string{"theme": "neon"}
	w := doSelfServiceRequest(handler, http.MethodPatch, "/v1/me/profile", body, ac)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if repo.updated != nil {
		t.Error("expected Update NOT to be called for invalid theme")
	}
}

func TestMeProfilePatch_NoOpSkipsUpdate(t *testing.T) {
	userID := uuid.New()
	repo := &mockMeProfileRepo{
		user: &model.User{
			ID:       userID,
			Settings: json.RawMessage(`{"theme":"dark"}`),
		},
	}

	handler := NewMeProfilePatchHandler(repo)
	ac := &auth.AuthContext{UserID: userID, Role: "member"}
	w := doSelfServiceRequest(handler, http.MethodPatch, "/v1/me/profile", map[string]any{}, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if repo.updated != nil {
		t.Error("expected Update NOT to be called when no fields supplied")
	}
	var resp SessionUser
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Theme != "dark" {
		t.Errorf("expected response theme unchanged at dark, got %q", resp.Theme)
	}
}

func TestMeProfilePatch_Unauthenticated(t *testing.T) {
	handler := NewMeProfilePatchHandler(&mockMeProfileRepo{})
	w := doSelfServiceRequest(handler, http.MethodPatch, "/v1/me/profile", map[string]string{"theme": "dark"}, nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMeProfilePatch_MethodNotAllowed(t *testing.T) {
	repo := &mockMeProfileRepo{user: &model.User{ID: uuid.New(), Settings: json.RawMessage(`{}`)}}
	handler := NewMeProfilePatchHandler(repo)
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "member"}
	w := doSelfServiceRequest(handler, http.MethodGet, "/v1/me/profile", nil, ac)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}
