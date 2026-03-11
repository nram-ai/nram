package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// --- mock implementations for projects handler ---

type mockProjectLister struct {
	listFn    func(ctx context.Context, ownerNSID uuid.UUID) ([]model.Project, error)
	createFn  func(ctx context.Context, project *model.Project) error
	getSlugFn func(ctx context.Context, ownerNSID uuid.UUID, slug string) (*model.Project, error)
}

func (m *mockProjectLister) ListByUser(ctx context.Context, ownerNSID uuid.UUID) ([]model.Project, error) {
	if m.listFn != nil {
		return m.listFn(ctx, ownerNSID)
	}
	return nil, nil
}

func (m *mockProjectLister) Create(ctx context.Context, project *model.Project) error {
	if m.createFn != nil {
		return m.createFn(ctx, project)
	}
	project.ID = uuid.New()
	return nil
}

func (m *mockProjectLister) GetBySlug(ctx context.Context, ownerNSID uuid.UUID, slug string) (*model.Project, error) {
	if m.getSlugFn != nil {
		return m.getSlugFn(ctx, ownerNSID, slug)
	}
	return nil, fmt.Errorf("not found")
}

type mockUserGetter struct {
	user *model.User
	err  error
}

func (m *mockUserGetter) GetByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.user != nil {
		return m.user, nil
	}
	return &model.User{
		ID:          id,
		NamespaceID: uuid.New(),
	}, nil
}

type mockNamespaceCreator struct {
	createFn func(ctx context.Context, ns *model.Namespace) error
	getFn    func(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
}

func (m *mockNamespaceCreator) Create(ctx context.Context, ns *model.Namespace) error {
	if m.createFn != nil {
		return m.createFn(ctx, ns)
	}
	return nil
}

func (m *mockNamespaceCreator) GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error) {
	if m.getFn != nil {
		return m.getFn(ctx, id)
	}
	return &model.Namespace{
		ID:    id,
		Path:  "/orgs/acme/users/alice",
		Depth: 2,
	}, nil
}

// --- helpers ---

func doProjectsRequest(handler http.HandlerFunc, method string, body interface{}, ac *auth.AuthContext, query string) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}

	target := "/v1/me/projects"
	if query != "" {
		target += "?" + query
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

// --- tests ---

func TestMeProjects_ListSuccess(t *testing.T) {
	userNSID := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: userNSID}

	projects := &mockProjectLister{
		listFn: func(ctx context.Context, ownerNSID uuid.UUID) ([]model.Project, error) {
			if ownerNSID != userNSID {
				t.Errorf("expected ownerNSID %s, got %s", userNSID, ownerNSID)
			}
			return []model.Project{
				{ID: uuid.New(), Name: "Alpha", Slug: "alpha"},
				{ID: uuid.New(), Name: "Beta", Slug: "beta"},
			}, nil
		},
	}

	handler := NewMeProjectsHandler(projects, &mockUserGetter{user: user}, &mockNamespaceCreator{})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}
	w := doProjectsRequest(handler, http.MethodGet, nil, ac, "limit=50&offset=0")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp model.PaginatedResponse[model.Project]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Errorf("expected 2 projects, got %d", len(resp.Data))
	}
	if resp.Pagination.Total != 2 {
		t.Errorf("expected total 2, got %d", resp.Pagination.Total)
	}
	if resp.Pagination.Limit != 50 {
		t.Errorf("expected limit 50, got %d", resp.Pagination.Limit)
	}
	if resp.Pagination.Offset != 0 {
		t.Errorf("expected offset 0, got %d", resp.Pagination.Offset)
	}
}

func TestMeProjects_CreateSuccess(t *testing.T) {
	userNSID := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: userNSID}

	var createdProject *model.Project
	projects := &mockProjectLister{
		createFn: func(ctx context.Context, project *model.Project) error {
			project.ID = uuid.New()
			createdProject = project
			return nil
		},
	}

	handler := NewMeProjectsHandler(projects, &mockUserGetter{user: user}, &mockNamespaceCreator{})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}

	body := map[string]interface{}{
		"name":        "Project Mayhem",
		"slug":        "project-mayhem",
		"description": "Distributed systems redesign",
		"default_tags": []string{"architecture"},
	}

	w := doProjectsRequest(handler, http.MethodPost, body, ac, "")

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp model.Project
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if resp.Name != "Project Mayhem" {
		t.Errorf("expected name 'Project Mayhem', got %q", resp.Name)
	}
	if resp.Slug != "project-mayhem" {
		t.Errorf("expected slug 'project-mayhem', got %q", resp.Slug)
	}
	if createdProject == nil {
		t.Fatal("expected project to be created")
	}
	if createdProject.OwnerNamespaceID != userNSID {
		t.Errorf("expected owner_namespace_id %s, got %s", userNSID, createdProject.OwnerNamespaceID)
	}
}

func TestMeProjects_CreateMissingName(t *testing.T) {
	handler := NewMeProjectsHandler(&mockProjectLister{}, &mockUserGetter{}, &mockNamespaceCreator{})
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	body := map[string]interface{}{
		"description": "no name provided",
	}

	w := doProjectsRequest(handler, http.MethodPost, body, ac, "")

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

func TestMeProjects_CreateSlugConflict(t *testing.T) {
	userNSID := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: userNSID}

	projects := &mockProjectLister{
		getSlugFn: func(ctx context.Context, ownerNSID uuid.UUID, slug string) (*model.Project, error) {
			return &model.Project{ID: uuid.New(), Slug: slug}, nil
		},
	}

	handler := NewMeProjectsHandler(projects, &mockUserGetter{user: user}, &mockNamespaceCreator{})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}

	body := map[string]interface{}{
		"name": "Duplicate",
		"slug": "existing-slug",
	}

	w := doProjectsRequest(handler, http.MethodPost, body, ac, "")

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "conflict" {
		t.Errorf("expected conflict, got %+v", envelope.Error)
	}
}

func TestMeProjects_CreateAutoSlug(t *testing.T) {
	userNSID := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: userNSID}

	var createdProject *model.Project
	projects := &mockProjectLister{
		createFn: func(ctx context.Context, project *model.Project) error {
			project.ID = uuid.New()
			createdProject = project
			return nil
		},
	}

	handler := NewMeProjectsHandler(projects, &mockUserGetter{user: user}, &mockNamespaceCreator{})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}

	body := map[string]interface{}{
		"name": "My Cool Project!",
	}

	w := doProjectsRequest(handler, http.MethodPost, body, ac, "")

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	if createdProject == nil {
		t.Fatal("expected project to be created")
	}
	if createdProject.Slug != "my-cool-project" {
		t.Errorf("expected auto-slug 'my-cool-project', got %q", createdProject.Slug)
	}
}

func TestMeProjects_GetUnauthenticated(t *testing.T) {
	handler := NewMeProjectsHandler(&mockProjectLister{}, &mockUserGetter{}, &mockNamespaceCreator{})

	w := doProjectsRequest(handler, http.MethodGet, nil, nil, "")

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

func TestMeProjects_PostInvalidJSON(t *testing.T) {
	handler := NewMeProjectsHandler(&mockProjectLister{}, &mockUserGetter{}, &mockNamespaceCreator{})
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	req := httptest.NewRequest(http.MethodPost, "/v1/me/projects", bytes.NewBufferString("{invalid"))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithContext(req.Context(), ac))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

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

func TestMeProjects_MethodNotAllowed(t *testing.T) {
	handler := NewMeProjectsHandler(&mockProjectLister{}, &mockUserGetter{}, &mockNamespaceCreator{})
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	w := doProjectsRequest(handler, http.MethodDelete, nil, ac, "")

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSlugify(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Project Mayhem", "project-mayhem"},
		{"My Cool Project!", "my-cool-project"},
		{"  spaces  everywhere  ", "spaces-everywhere"},
		{"UPPERCASE", "uppercase"},
		{"special@#$chars", "special-chars"},
		{"already-slug", "already-slug"},
		{"multiple---hyphens", "multiple-hyphens"},
	}

	for _, tt := range tests {
		got := slugify(tt.input)
		if got != tt.expected {
			t.Errorf("slugify(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
