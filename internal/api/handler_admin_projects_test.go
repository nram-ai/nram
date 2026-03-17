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

// --- mock ProjectAdminStore ---

type mockProjectAdminStore struct {
	countProjectsFunc func(ctx context.Context) (int, error)
	listProjectsFunc  func(ctx context.Context, limit, offset int) ([]model.Project, error)
	createProjectFunc func(ctx context.Context, name, slug, description string, ownerNamespaceID uuid.UUID, defaultTags []string, settings json.RawMessage) (*model.Project, error)
	getProjectFunc    func(ctx context.Context, id uuid.UUID) (*model.Project, error)
	updateProjectFunc func(ctx context.Context, id uuid.UUID, name, slug, description string, defaultTags []string, settings json.RawMessage) (*model.Project, error)
	deleteProjectFunc func(ctx context.Context, id uuid.UUID) error
}

func (m *mockProjectAdminStore) CountProjects(ctx context.Context) (int, error) {
	if m.countProjectsFunc != nil {
		return m.countProjectsFunc(ctx)
	}
	return 0, nil
}

func (m *mockProjectAdminStore) ListProjects(ctx context.Context, limit, offset int) ([]model.Project, error) {
	return m.listProjectsFunc(ctx, limit, offset)
}

func (m *mockProjectAdminStore) CreateProject(ctx context.Context, name, slug, description string, ownerNamespaceID uuid.UUID, defaultTags []string, settings json.RawMessage) (*model.Project, error) {
	return m.createProjectFunc(ctx, name, slug, description, ownerNamespaceID, defaultTags, settings)
}

func (m *mockProjectAdminStore) GetProject(ctx context.Context, id uuid.UUID) (*model.Project, error) {
	return m.getProjectFunc(ctx, id)
}

func (m *mockProjectAdminStore) UpdateProject(ctx context.Context, id uuid.UUID, name, slug, description string, defaultTags []string, settings json.RawMessage) (*model.Project, error) {
	return m.updateProjectFunc(ctx, id, name, slug, description, defaultTags, settings)
}

func (m *mockProjectAdminStore) DeleteProject(ctx context.Context, id uuid.UUID) error {
	return m.deleteProjectFunc(ctx, id)
}

// --- helpers ---

func newTestProject(name, slug string) model.Project {
	return model.Project{
		ID:               uuid.New(),
		NamespaceID:      uuid.New(),
		OwnerNamespaceID: uuid.New(),
		Name:             name,
		Slug:             slug,
		Description:      "test project",
		DefaultTags:      []string{"test"},
		Settings:         json.RawMessage(`{}`),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
}

func adminProjectsHandler(store ProjectAdminStore) http.HandlerFunc {
	return NewAdminProjectsHandler(ProjectAdminConfig{Store: store})
}

// --- tests ---

func TestAdminProjects_ListProjects(t *testing.T) {
	p1 := newTestProject("Alpha", "alpha")
	p2 := newTestProject("Beta", "beta")

	store := &mockProjectAdminStore{
		countProjectsFunc: func(_ context.Context) (int, error) {
			return 2, nil
		},
		listProjectsFunc: func(_ context.Context, limit, offset int) ([]model.Project, error) {
			return []model.Project{p1, p2}, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/projects", nil)
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp model.PaginatedResponse[model.Project]
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(resp.Data))
	}
	if resp.Data[0].Name != "Alpha" {
		t.Errorf("expected first project name 'Alpha', got %q", resp.Data[0].Name)
	}
	if resp.Pagination.Total != 2 {
		t.Errorf("expected pagination.total=2, got %d", resp.Pagination.Total)
	}
}

func TestAdminProjects_ListProjects_Empty(t *testing.T) {
	store := &mockProjectAdminStore{
		countProjectsFunc: func(_ context.Context) (int, error) {
			return 0, nil
		},
		listProjectsFunc: func(_ context.Context, limit, offset int) ([]model.Project, error) {
			return []model.Project{}, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/projects", nil)
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp model.PaginatedResponse[model.Project]
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Fatalf("expected 0 projects, got %d", len(resp.Data))
	}
	if resp.Pagination.Total != 0 {
		t.Errorf("expected pagination.total=0, got %d", resp.Pagination.Total)
	}
}

func TestAdminProjects_CreateProject_Success(t *testing.T) {
	ownerNSID := uuid.New()
	created := newTestProject("NewProject", "new-project")
	created.OwnerNamespaceID = ownerNSID

	store := &mockProjectAdminStore{
		createProjectFunc: func(_ context.Context, name, slug, description string, ownerNamespaceID uuid.UUID, defaultTags []string, settings json.RawMessage) (*model.Project, error) {
			if name != "NewProject" || slug != "new-project" {
				t.Errorf("unexpected args: name=%q slug=%q", name, slug)
			}
			if ownerNamespaceID != ownerNSID {
				t.Errorf("unexpected owner_namespace_id: %v", ownerNamespaceID)
			}
			return &created, nil
		},
	}

	body := `{"name":"NewProject","slug":"new-project","description":"a project","owner_namespace_id":"` + ownerNSID.String() + `","default_tags":["go"],"settings":{"k":"v"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/projects", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	var resp model.Project
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Name != "NewProject" {
		t.Errorf("expected name 'NewProject', got %q", resp.Name)
	}
}

func TestAdminProjects_CreateProject_MissingName(t *testing.T) {
	store := &mockProjectAdminStore{}

	body := `{"slug":"test","owner_namespace_id":"` + uuid.New().String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/projects", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAdminProjects_CreateProject_MissingSlug(t *testing.T) {
	store := &mockProjectAdminStore{}

	body := `{"name":"Test","owner_namespace_id":"` + uuid.New().String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/projects", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAdminProjects_CreateProject_MissingOwnerNamespaceID(t *testing.T) {
	store := &mockProjectAdminStore{}

	body := `{"name":"Test","slug":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/projects", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAdminProjects_GetProject_Found(t *testing.T) {
	project := newTestProject("Alpha", "alpha")

	store := &mockProjectAdminStore{
		getProjectFunc: func(_ context.Context, id uuid.UUID) (*model.Project, error) {
			if id != project.ID {
				t.Errorf("unexpected id: %v", id)
			}
			return &project, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/projects/"+project.ID.String(), nil)
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp model.Project
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != project.ID {
		t.Errorf("expected id %v, got %v", project.ID, resp.ID)
	}
}

func TestAdminProjects_GetProject_NotFound(t *testing.T) {
	store := &mockProjectAdminStore{
		getProjectFunc: func(_ context.Context, _ uuid.UUID) (*model.Project, error) {
			return nil, sql.ErrNoRows
		},
	}

	id := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/projects/"+id.String(), nil)
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAdminProjects_GetProject_InvalidUUID(t *testing.T) {
	store := &mockProjectAdminStore{}

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/projects/not-a-uuid", nil)
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAdminProjects_UpdateProject_Success(t *testing.T) {
	projectID := uuid.New()
	updated := newTestProject("Updated", "updated")
	updated.ID = projectID

	store := &mockProjectAdminStore{
		updateProjectFunc: func(_ context.Context, id uuid.UUID, name, slug, description string, defaultTags []string, settings json.RawMessage) (*model.Project, error) {
			if id != projectID {
				t.Errorf("unexpected id: %v", id)
			}
			return &updated, nil
		},
	}

	body := `{"name":"Updated","slug":"updated","description":"updated desc","default_tags":["go"],"settings":{"foo":"bar"}}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/projects/"+projectID.String(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp model.Project
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Name != "Updated" {
		t.Errorf("expected name 'Updated', got %q", resp.Name)
	}
}

func TestAdminProjects_UpdateProject_NotFound(t *testing.T) {
	store := &mockProjectAdminStore{
		updateProjectFunc: func(_ context.Context, _ uuid.UUID, _, _, _ string, _ []string, _ json.RawMessage) (*model.Project, error) {
			return nil, sql.ErrNoRows
		},
	}

	id := uuid.New()
	body := `{"name":"X","slug":"x"}`
	req := httptest.NewRequest(http.MethodPut, "/v1/admin/projects/"+id.String(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAdminProjects_DeleteProject_Success(t *testing.T) {
	projectID := uuid.New()

	store := &mockProjectAdminStore{
		deleteProjectFunc: func(_ context.Context, id uuid.UUID) error {
			if id != projectID {
				t.Errorf("unexpected id: %v", id)
			}
			return nil
		},
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/projects/"+projectID.String(), nil)
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestAdminProjects_DeleteProject_NotFound(t *testing.T) {
	store := &mockProjectAdminStore{
		deleteProjectFunc: func(_ context.Context, _ uuid.UUID) error {
			return sql.ErrNoRows
		},
	}

	id := uuid.New()
	req := httptest.NewRequest(http.MethodDelete, "/v1/admin/projects/"+id.String(), nil)
	rec := httptest.NewRecorder()
	adminProjectsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
