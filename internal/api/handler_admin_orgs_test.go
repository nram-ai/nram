package api

import (
	"context"
	"database/sql"
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

// --- mock OrgStore ---

type mockOrgStore struct {
	listOrgsFunc  func(ctx context.Context) ([]model.Organization, error)
	createOrgFunc func(ctx context.Context, name, slug string) (*model.Organization, error)
	getOrgFunc    func(ctx context.Context, id uuid.UUID) (*model.Organization, error)
	updateOrgFunc func(ctx context.Context, id uuid.UUID, name, slug string, settings json.RawMessage) (*model.Organization, error)
	deleteOrgFunc func(ctx context.Context, id uuid.UUID) error
}

func (m *mockOrgStore) ListOrgs(ctx context.Context) ([]model.Organization, error) {
	return m.listOrgsFunc(ctx)
}

func (m *mockOrgStore) CreateOrg(ctx context.Context, name, slug string) (*model.Organization, error) {
	return m.createOrgFunc(ctx, name, slug)
}

func (m *mockOrgStore) GetOrg(ctx context.Context, id uuid.UUID) (*model.Organization, error) {
	return m.getOrgFunc(ctx, id)
}

func (m *mockOrgStore) UpdateOrg(ctx context.Context, id uuid.UUID, name, slug string, settings json.RawMessage) (*model.Organization, error) {
	return m.updateOrgFunc(ctx, id, name, slug, settings)
}

func (m *mockOrgStore) DeleteOrg(ctx context.Context, id uuid.UUID) error {
	return m.deleteOrgFunc(ctx, id)
}

// --- helpers ---

func newTestOrg(name, slug string) model.Organization {
	return model.Organization{
		ID:          uuid.New(),
		NamespaceID: uuid.New(),
		Name:        name,
		Slug:        slug,
		Settings:    json.RawMessage(`{}`),
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
}

func adminOrgsHandler(store OrgStore) http.HandlerFunc {
	return NewAdminOrgsHandler(OrgAdminConfig{Store: store})
}

// --- tests ---

func TestAdminOrgs_ListOrgs(t *testing.T) {
	org1 := newTestOrg("Acme", "acme")
	org2 := newTestOrg("Globex", "globex")

	store := &mockOrgStore{
		listOrgsFunc: func(_ context.Context) ([]model.Organization, error) {
			return []model.Organization{org1, org2}, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/orgs", nil)
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp struct {
		Data []model.Organization `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 orgs, got %d", len(resp.Data))
	}
	if resp.Data[0].Name != "Acme" {
		t.Errorf("expected first org name 'Acme', got %q", resp.Data[0].Name)
	}
}

func TestAdminOrgs_ListOrgs_Empty(t *testing.T) {
	store := &mockOrgStore{
		listOrgsFunc: func(_ context.Context) ([]model.Organization, error) {
			return []model.Organization{}, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/orgs", nil)
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp struct {
		Data []model.Organization `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Fatalf("expected 0 orgs, got %d", len(resp.Data))
	}
}

func TestAdminOrgs_CreateOrg_Success(t *testing.T) {
	created := newTestOrg("NewOrg", "new-org")

	store := &mockOrgStore{
		createOrgFunc: func(_ context.Context, name, slug string) (*model.Organization, error) {
			if name != "NewOrg" || slug != "new-org" {
				t.Errorf("unexpected args: name=%q slug=%q", name, slug)
			}
			return &created, nil
		},
	}

	body := `{"name":"NewOrg","slug":"new-org"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/orgs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	var resp model.Organization
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Name != "NewOrg" {
		t.Errorf("expected name 'NewOrg', got %q", resp.Name)
	}
}

func TestAdminOrgs_CreateOrg_MissingName(t *testing.T) {
	store := &mockOrgStore{}

	body := `{"slug":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/orgs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAdminOrgs_CreateOrg_MissingSlug(t *testing.T) {
	store := &mockOrgStore{}

	body := `{"name":"Test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/orgs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAdminOrgs_GetOrg_Found(t *testing.T) {
	org := newTestOrg("Acme", "acme")

	store := &mockOrgStore{
		getOrgFunc: func(_ context.Context, id uuid.UUID) (*model.Organization, error) {
			if id != org.ID {
				t.Errorf("unexpected id: %v", id)
			}
			return &org, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/orgs/"+org.ID.String(), nil)
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp model.Organization
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != org.ID {
		t.Errorf("expected id %v, got %v", org.ID, resp.ID)
	}
}

func TestAdminOrgs_GetOrg_NotFound(t *testing.T) {
	store := &mockOrgStore{
		getOrgFunc: func(_ context.Context, _ uuid.UUID) (*model.Organization, error) {
			return nil, sql.ErrNoRows
		},
	}

	id := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/orgs/"+id.String(), nil)
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAdminOrgs_GetOrg_InvalidUUID(t *testing.T) {
	store := &mockOrgStore{}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/orgs/not-a-uuid", nil)
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAdminOrgs_UpdateOrg_Success(t *testing.T) {
	orgID := uuid.New()
	updated := newTestOrg("Updated", "updated")
	updated.ID = orgID

	store := &mockOrgStore{
		updateOrgFunc: func(_ context.Context, id uuid.UUID, name, slug string, settings json.RawMessage) (*model.Organization, error) {
			if id != orgID {
				t.Errorf("unexpected id: %v", id)
			}
			return &updated, nil
		},
	}

	body := `{"name":"Updated","slug":"updated","settings":{"foo":"bar"}}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/orgs/"+orgID.String(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp model.Organization
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Name != "Updated" {
		t.Errorf("expected name 'Updated', got %q", resp.Name)
	}
}

func TestAdminOrgs_UpdateOrg_NotFound(t *testing.T) {
	store := &mockOrgStore{
		updateOrgFunc: func(_ context.Context, _ uuid.UUID, _, _ string, _ json.RawMessage) (*model.Organization, error) {
			return nil, sql.ErrNoRows
		},
	}

	id := uuid.New()
	body := `{"name":"X","slug":"x"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/orgs/"+id.String(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAdminOrgs_DeleteOrg_Success(t *testing.T) {
	orgID := uuid.New()

	store := &mockOrgStore{
		deleteOrgFunc: func(_ context.Context, id uuid.UUID) error {
			if id != orgID {
				t.Errorf("unexpected id: %v", id)
			}
			return nil
		},
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/orgs/"+orgID.String(), nil)
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestAdminOrgs_DeleteOrg_NotFound(t *testing.T) {
	store := &mockOrgStore{
		deleteOrgFunc: func(_ context.Context, _ uuid.UUID) error {
			return sql.ErrNoRows
		},
	}

	id := uuid.New()
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/orgs/"+id.String(), nil)
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAdminOrgs_CreateOrg_StoreError(t *testing.T) {
	store := &mockOrgStore{
		createOrgFunc: func(_ context.Context, _, _ string) (*model.Organization, error) {
			return nil, errors.New("database connection lost")
		},
	}

	body := `{"name":"Fail","slug":"fail"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/orgs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminOrgsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
