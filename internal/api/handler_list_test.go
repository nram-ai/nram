package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- mock implementations for list/detail tests ---

type mockMemoryLister struct {
	listFn  func(ctx context.Context, nsID uuid.UUID, limit, offset int) ([]model.Memory, error)
	countFn func(ctx context.Context, nsID uuid.UUID) (int, error)
	getFn   func(ctx context.Context, id uuid.UUID) (*model.Memory, error)
}

func (m *mockMemoryLister) ListByNamespace(ctx context.Context, nsID uuid.UUID, limit, offset int) ([]model.Memory, error) {
	if m.listFn != nil {
		return m.listFn(ctx, nsID, limit, offset)
	}
	return nil, nil
}

func (m *mockMemoryLister) CountByNamespace(ctx context.Context, nsID uuid.UUID) (int, error) {
	if m.countFn != nil {
		return m.countFn(ctx, nsID)
	}
	return 0, nil
}

func (m *mockMemoryLister) GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error) {
	if m.getFn != nil {
		return m.getFn(ctx, id)
	}
	return nil, sql.ErrNoRows
}

type mockProjectGetter struct {
	project *model.Project
	err     error
}

func (m *mockProjectGetter) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.project != nil {
		return m.project, nil
	}
	return &model.Project{
		ID:          id,
		Slug:        "test-project",
		NamespaceID: uuid.New(),
	}, nil
}

// --- helpers ---

func newListRouter(memRepo MemoryLister, projRepo ProjectGetter) *chi.Mux {
	r := chi.NewRouter()
	r.Get("/v1/projects/{project_id}/memories", NewListHandler(memRepo, projRepo, nil))
	r.Get("/v1/projects/{project_id}/memories/{id}", NewDetailHandler(memRepo, projRepo, nil))
	return r
}

func doListRequest(router http.Handler, projectID, query string) *httptest.ResponseRecorder {
	path := "/v1/projects/" + projectID + "/memories"
	if query != "" {
		path += "?" + query
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func doDetailRequest(router http.Handler, projectID, memoryID string) *httptest.ResponseRecorder {
	path := "/v1/projects/" + projectID + "/memories/" + memoryID
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// --- list tests ---

func TestListHandler_Success(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	mems := []model.Memory{
		{
			ID:          uuid.New(),
			NamespaceID: nsID,
			Content:     "memory one",
			Tags:        []string{"a"},
			Confidence:  1.0,
			Importance:  0.5,
			Metadata:    json.RawMessage(`{}`),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			ID:          uuid.New(),
			NamespaceID: nsID,
			Content:     "memory two",
			Tags:        []string{"b"},
			Confidence:  0.9,
			Importance:  0.3,
			Metadata:    json.RawMessage(`{}`),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, gotNS uuid.UUID, limit, offset int) ([]model.Memory, error) {
			if gotNS != nsID {
				t.Errorf("expected namespace %s, got %s", nsID, gotNS)
			}
			if limit != 50 {
				t.Errorf("expected default limit 50, got %d", limit)
			}
			if offset != 0 {
				t.Errorf("expected default offset 0, got %d", offset)
			}
			return mems, nil
		},
		countFn: func(_ context.Context, gotNS uuid.UUID) (int, error) {
			return 2, nil
		},
	}

	projRepo := &mockProjectGetter{project: proj}
	router := newListRouter(memRepo, projRepo)

	w := doListRequest(router, projectID.String(), "")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp model.PaginatedResponse[model.Memory]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Data))
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

func TestListHandler_CustomLimitOffset(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
			if limit != 10 {
				t.Errorf("expected limit 10, got %d", limit)
			}
			if offset != 20 {
				t.Errorf("expected offset 20, got %d", offset)
			}
			return []model.Memory{}, nil
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) {
			return 100, nil
		},
	}

	projRepo := &mockProjectGetter{project: proj}
	router := newListRouter(memRepo, projRepo)

	w := doListRequest(router, projectID.String(), "limit=10&offset=20")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp model.PaginatedResponse[model.Memory]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Pagination.Limit != 10 {
		t.Errorf("expected limit 10, got %d", resp.Pagination.Limit)
	}
	if resp.Pagination.Offset != 20 {
		t.Errorf("expected offset 20, got %d", resp.Pagination.Offset)
	}
	if resp.Pagination.Total != 100 {
		t.Errorf("expected total 100, got %d", resp.Pagination.Total)
	}
}

func TestListHandler_InvalidProjectID(t *testing.T) {
	memRepo := &mockMemoryLister{}
	projRepo := &mockProjectGetter{}
	router := newListRouter(memRepo, projRepo)

	w := doListRequest(router, "not-a-uuid", "")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	var env errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error == nil || env.Error.Code != "bad_request" {
		t.Errorf("expected bad_request, got %+v", env.Error)
	}
}

func TestListHandler_ClampsLimitToMax(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, _ uuid.UUID, limit, _ int) ([]model.Memory, error) {
			if limit != 200 {
				t.Errorf("expected clamped limit 200, got %d", limit)
			}
			return []model.Memory{}, nil
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) {
			return 0, nil
		},
	}

	projRepo := &mockProjectGetter{project: proj}
	router := newListRouter(memRepo, projRepo)

	w := doListRequest(router, projectID.String(), "limit=999")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp model.PaginatedResponse[model.Memory]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Pagination.Limit != 200 {
		t.Errorf("expected limit 200, got %d", resp.Pagination.Limit)
	}
}

// --- detail tests ---

func TestDetailHandler_Success(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	memoryID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	mem := &model.Memory{
		ID:          memoryID,
		NamespaceID: nsID,
		Content:     "test memory",
		Tags:        []string{"x"},
		Confidence:  1.0,
		Importance:  0.5,
		Metadata:    json.RawMessage(`{}`),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	memRepo := &mockMemoryLister{
		getFn: func(_ context.Context, id uuid.UUID) (*model.Memory, error) {
			if id != memoryID {
				return nil, sql.ErrNoRows
			}
			return mem, nil
		},
	}

	projRepo := &mockProjectGetter{project: proj}
	router := newListRouter(memRepo, projRepo)

	w := doDetailRequest(router, projectID.String(), memoryID.String())

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var got model.Memory
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.ID != memoryID {
		t.Errorf("expected id %s, got %s", memoryID, got.ID)
	}
	if got.Content != "test memory" {
		t.Errorf("expected content 'test memory', got %q", got.Content)
	}
}

func TestDetailHandler_NotFound(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		getFn: func(_ context.Context, _ uuid.UUID) (*model.Memory, error) {
			return nil, sql.ErrNoRows
		},
	}

	projRepo := &mockProjectGetter{project: proj}
	router := newListRouter(memRepo, projRepo)

	w := doDetailRequest(router, projectID.String(), uuid.New().String())

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}

	var env errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error == nil || env.Error.Code != "not_found" {
		t.Errorf("expected not_found, got %+v", env.Error)
	}
}

func TestDetailHandler_WrongNamespace(t *testing.T) {
	projectNS := uuid.New()
	otherNS := uuid.New()
	projectID := uuid.New()
	memoryID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: projectNS}
	mem := &model.Memory{
		ID:          memoryID,
		NamespaceID: otherNS,
		Content:     "wrong namespace memory",
		Tags:        []string{},
		Metadata:    json.RawMessage(`{}`),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	memRepo := &mockMemoryLister{
		getFn: func(_ context.Context, id uuid.UUID) (*model.Memory, error) {
			if id == memoryID {
				return mem, nil
			}
			return nil, sql.ErrNoRows
		},
	}

	projRepo := &mockProjectGetter{project: proj}
	router := newListRouter(memRepo, projRepo)

	w := doDetailRequest(router, projectID.String(), memoryID.String())

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}

	var env errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error == nil || env.Error.Code != "not_found" {
		t.Errorf("expected not_found, got %+v", env.Error)
	}
}

func TestDetailHandler_InvalidID(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{}
	projRepo := &mockProjectGetter{project: proj}
	router := newListRouter(memRepo, projRepo)

	w := doDetailRequest(router, projectID.String(), "not-a-uuid")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	var env errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error == nil || env.Error.Code != "bad_request" {
		t.Errorf("expected bad_request, got %+v", env.Error)
	}
}
