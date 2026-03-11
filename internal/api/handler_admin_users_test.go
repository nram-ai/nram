package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- mock UserAdminStore ---

type mockUserAdminStore struct {
	listUsersFn     func(ctx context.Context) ([]model.User, error)
	createUserFn    func(ctx context.Context, email, displayName, password, role string, orgID uuid.UUID) (*model.User, error)
	getUserFn       func(ctx context.Context, id uuid.UUID) (*model.User, error)
	updateUserFn    func(ctx context.Context, id uuid.UUID, displayName, role string, settings json.RawMessage) (*model.User, error)
	deleteUserFn    func(ctx context.Context, id uuid.UUID) error
	countAdminsFn   func(ctx context.Context) (int, error)
	listAPIKeysFn   func(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error)
	generateAPIKeyFn func(ctx context.Context, userID uuid.UUID, name string, scopes []uuid.UUID, expiresAt *time.Time) (*model.APIKey, string, error)
	revokeAPIKeyFn  func(ctx context.Context, keyID uuid.UUID) error
}

func (m *mockUserAdminStore) ListUsers(ctx context.Context) ([]model.User, error) {
	return m.listUsersFn(ctx)
}

func (m *mockUserAdminStore) CreateUser(ctx context.Context, email, displayName, password, role string, orgID uuid.UUID) (*model.User, error) {
	return m.createUserFn(ctx, email, displayName, password, role, orgID)
}

func (m *mockUserAdminStore) GetUser(ctx context.Context, id uuid.UUID) (*model.User, error) {
	return m.getUserFn(ctx, id)
}

func (m *mockUserAdminStore) UpdateUser(ctx context.Context, id uuid.UUID, displayName, role string, settings json.RawMessage) (*model.User, error) {
	return m.updateUserFn(ctx, id, displayName, role, settings)
}

func (m *mockUserAdminStore) DeleteUser(ctx context.Context, id uuid.UUID) error {
	return m.deleteUserFn(ctx, id)
}

func (m *mockUserAdminStore) CountAdmins(ctx context.Context) (int, error) {
	return m.countAdminsFn(ctx)
}

func (m *mockUserAdminStore) ListAPIKeys(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error) {
	return m.listAPIKeysFn(ctx, userID)
}

func (m *mockUserAdminStore) GenerateAPIKey(ctx context.Context, userID uuid.UUID, name string, scopes []uuid.UUID, expiresAt *time.Time) (*model.APIKey, string, error) {
	return m.generateAPIKeyFn(ctx, userID, name, scopes, expiresAt)
}

func (m *mockUserAdminStore) RevokeAPIKey(ctx context.Context, keyID uuid.UUID) error {
	return m.revokeAPIKeyFn(ctx, keyID)
}

// --- helpers ---

func adminUsersHandler(store UserAdminStore) http.HandlerFunc {
	return NewAdminUsersHandler(UserAdminConfig{Store: store})
}

func newTestUser(email, role string) model.User {
	now := time.Now().UTC()
	return model.User{
		ID:          uuid.New(),
		Email:       email,
		DisplayName: strings.Split(email, "@")[0],
		OrgID:       uuid.New(),
		NamespaceID: uuid.New(),
		Role:        role,
		Settings:    json.RawMessage(`{}`),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// --- tests ---

func TestAdminUsers_ListUsers(t *testing.T) {
	u1 := newTestUser("alice@example.com", "administrator")
	u2 := newTestUser("bob@example.com", "member")

	store := &mockUserAdminStore{
		listUsersFn: func(_ context.Context) ([]model.User, error) {
			return []model.User{u1, u2}, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/users", nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data []model.User `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 users, got %d", len(resp.Data))
	}
	if resp.Data[0].Email != "alice@example.com" {
		t.Errorf("expected first user email 'alice@example.com', got %q", resp.Data[0].Email)
	}
}

func TestAdminUsers_CreateUser_Success(t *testing.T) {
	orgID := uuid.New()
	created := newTestUser("new@example.com", "member")

	store := &mockUserAdminStore{
		createUserFn: func(_ context.Context, email, displayName, password, role string, oid uuid.UUID) (*model.User, error) {
			if email != "new@example.com" {
				t.Errorf("unexpected email: %q", email)
			}
			if password != "securepassword123" {
				t.Errorf("unexpected password: %q", password)
			}
			if role != "member" {
				t.Errorf("unexpected role: %q", role)
			}
			if oid != orgID {
				t.Errorf("unexpected orgID: %v", oid)
			}
			return &created, nil
		},
	}

	body := `{"email":"new@example.com","display_name":"New User","password":"securepassword123","role":"member","org_id":"` + orgID.String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp model.User
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Email != "new@example.com" {
		t.Errorf("expected email 'new@example.com', got %q", resp.Email)
	}
}

func TestAdminUsers_CreateUser_MissingEmail(t *testing.T) {
	store := &mockUserAdminStore{}

	body := `{"password":"securepassword123","role":"member"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request, got %+v", envelope.Error)
	}
}

func TestAdminUsers_CreateUser_InvalidRole(t *testing.T) {
	store := &mockUserAdminStore{}

	body := `{"email":"test@example.com","password":"securepassword123","role":"superuser"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request, got %+v", envelope.Error)
	}
}

func TestAdminUsers_GetUser_Found(t *testing.T) {
	user := newTestUser("alice@example.com", "administrator")

	store := &mockUserAdminStore{
		getUserFn: func(_ context.Context, id uuid.UUID) (*model.User, error) {
			if id != user.ID {
				t.Errorf("unexpected id: %v", id)
			}
			return &user, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/users/"+user.ID.String(), nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp model.User
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != user.ID {
		t.Errorf("expected id %v, got %v", user.ID, resp.ID)
	}
}

func TestAdminUsers_GetUser_NotFound(t *testing.T) {
	store := &mockUserAdminStore{
		getUserFn: func(_ context.Context, _ uuid.UUID) (*model.User, error) {
			return nil, sql.ErrNoRows
		},
	}

	id := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/users/"+id.String(), nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAdminUsers_UpdateUser_Success(t *testing.T) {
	userID := uuid.New()
	updated := newTestUser("alice@example.com", "org_owner")
	updated.ID = userID
	updated.DisplayName = "Alice Updated"

	store := &mockUserAdminStore{
		updateUserFn: func(_ context.Context, id uuid.UUID, displayName, role string, settings json.RawMessage) (*model.User, error) {
			if id != userID {
				t.Errorf("unexpected id: %v", id)
			}
			if displayName != "Alice Updated" {
				t.Errorf("unexpected displayName: %q", displayName)
			}
			if role != "org_owner" {
				t.Errorf("unexpected role: %q", role)
			}
			return &updated, nil
		},
	}

	body := `{"display_name":"Alice Updated","role":"org_owner","settings":{"theme":"dark"}}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/users/"+userID.String(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp model.User
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.DisplayName != "Alice Updated" {
		t.Errorf("expected display_name 'Alice Updated', got %q", resp.DisplayName)
	}
}

func TestAdminUsers_DeleteUser_Success(t *testing.T) {
	userID := uuid.New()
	user := newTestUser("bob@example.com", "member")
	user.ID = userID

	store := &mockUserAdminStore{
		getUserFn: func(_ context.Context, id uuid.UUID) (*model.User, error) {
			return &user, nil
		},
		deleteUserFn: func(_ context.Context, id uuid.UUID) error {
			if id != userID {
				t.Errorf("unexpected id: %v", id)
			}
			return nil
		},
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/users/"+userID.String(), nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAdminUsers_DeleteUser_LastAdmin(t *testing.T) {
	userID := uuid.New()
	user := newTestUser("admin@example.com", "administrator")
	user.ID = userID

	store := &mockUserAdminStore{
		getUserFn: func(_ context.Context, id uuid.UUID) (*model.User, error) {
			return &user, nil
		},
		countAdminsFn: func(_ context.Context) (int, error) {
			return 1, nil
		},
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/users/"+userID.String(), nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "conflict" {
		t.Errorf("expected conflict, got %+v", envelope.Error)
	}
	if !strings.Contains(envelope.Error.Message, "last administrator") {
		t.Errorf("expected last administrator message, got %q", envelope.Error.Message)
	}
}

func TestAdminUsers_ListAPIKeys_Success(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()
	now := time.Now().UTC()

	store := &mockUserAdminStore{
		listAPIKeysFn: func(_ context.Context, uid uuid.UUID) ([]model.APIKey, error) {
			if uid != userID {
				t.Errorf("expected userID %s, got %s", userID, uid)
			}
			return []model.APIKey{
				{
					ID:        keyID,
					UserID:    userID,
					KeyPrefix: "nram_k_abc",
					Name:      "Test Key",
					Scopes:    []uuid.UUID{},
					CreatedAt: now,
				},
			}, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/users/"+userID.String()+"/api-keys", nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data []model.APIKey `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 key, got %d", len(resp.Data))
	}
	if resp.Data[0].Name != "Test Key" {
		t.Errorf("expected name 'Test Key', got %q", resp.Data[0].Name)
	}
}

func TestAdminUsers_GenerateAPIKey_Success(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()
	now := time.Now().UTC()
	rawKey := "nram_k_generated_raw_key_value_1234567890"

	store := &mockUserAdminStore{
		generateAPIKeyFn: func(_ context.Context, uid uuid.UUID, name string, scopes []uuid.UUID, expiresAt *time.Time) (*model.APIKey, string, error) {
			if uid != userID {
				t.Errorf("expected userID %s, got %s", userID, uid)
			}
			if name != "Admin Generated Key" {
				t.Errorf("expected name 'Admin Generated Key', got %q", name)
			}
			return &model.APIKey{
				ID:        keyID,
				UserID:    userID,
				KeyPrefix: "nram_k_gen",
				Name:      name,
				Scopes:    scopes,
				CreatedAt: now,
			}, rawKey, nil
		},
	}

	body := `{"name":"Admin Generated Key","scopes":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/users/"+userID.String()+"/api-keys", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp generateAPIKeyResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.RawKey != rawKey {
		t.Errorf("expected raw_key %q, got %q", rawKey, resp.RawKey)
	}
	if resp.APIKey.Name != "Admin Generated Key" {
		t.Errorf("expected api_key name 'Admin Generated Key', got %q", resp.APIKey.Name)
	}
}

func TestAdminUsers_GenerateAPIKey_MissingName(t *testing.T) {
	userID := uuid.New()
	store := &mockUserAdminStore{}

	body := `{"scopes":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/users/"+userID.String()+"/api-keys", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request, got %+v", envelope.Error)
	}
}

func TestAdminUsers_RevokeAPIKey_Success(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()
	revoked := false

	store := &mockUserAdminStore{
		revokeAPIKeyFn: func(_ context.Context, kid uuid.UUID) error {
			if kid != keyID {
				t.Errorf("expected keyID %s, got %s", keyID, kid)
			}
			revoked = true
			return nil
		},
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/users/"+userID.String()+"/api-keys/"+keyID.String(), nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
	if !revoked {
		t.Error("expected revoke function to be called")
	}
}

func TestAdminUsers_RevokeAPIKey_NotFound(t *testing.T) {
	userID := uuid.New()
	keyID := uuid.New()

	store := &mockUserAdminStore{
		revokeAPIKeyFn: func(_ context.Context, _ uuid.UUID) error {
			return sql.ErrNoRows
		},
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/users/"+userID.String()+"/api-keys/"+keyID.String(), nil)
	rec := httptest.NewRecorder()
	adminUsersHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "not_found" {
		t.Errorf("expected not_found, got %+v", envelope.Error)
	}
}
