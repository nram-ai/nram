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

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock implementations for list/detail tests ---

type mockMemoryLister struct {
	listFn        func(ctx context.Context, nsID uuid.UUID, limit, offset int) ([]model.Memory, error)
	countFn       func(ctx context.Context, nsID uuid.UUID) (int, error)
	listIDsFn     func(ctx context.Context, nsID uuid.UUID, filters storage.MemoryListFilters, max int) ([]uuid.UUID, error)
	getFn         func(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	listParentsFn func(ctx context.Context, nsID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error)
	countParentsFn func(ctx context.Context, nsID uuid.UUID, filters storage.MemoryListFilters) (int, error)
	findChildrenFn func(ctx context.Context, nsID uuid.UUID, parentIDs []uuid.UUID, relations []string) (map[uuid.UUID][]model.Memory, error)
	lastFilters   storage.MemoryListFilters
	filtersSeen   bool
}

func (m *mockMemoryLister) ListByNamespace(ctx context.Context, nsID uuid.UUID, limit, offset int) ([]model.Memory, error) {
	if m.listFn != nil {
		return m.listFn(ctx, nsID, limit, offset)
	}
	return nil, nil
}

func (m *mockMemoryLister) ListByNamespaceFiltered(ctx context.Context, nsID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error) {
	m.lastFilters = filters
	m.filtersSeen = true
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

func (m *mockMemoryLister) CountByNamespaceFiltered(ctx context.Context, nsID uuid.UUID, _ storage.MemoryListFilters) (int, error) {
	if m.countFn != nil {
		return m.countFn(ctx, nsID)
	}
	return 0, nil
}

func (m *mockMemoryLister) ListIDsByNamespaceFiltered(ctx context.Context, nsID uuid.UUID, filters storage.MemoryListFilters, max int) ([]uuid.UUID, error) {
	m.lastFilters = filters
	m.filtersSeen = true
	if m.listIDsFn != nil {
		return m.listIDsFn(ctx, nsID, filters, max)
	}
	return nil, nil
}

func (m *mockMemoryLister) GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error) {
	if m.getFn != nil {
		return m.getFn(ctx, id)
	}
	return nil, sql.ErrNoRows
}

func (m *mockMemoryLister) ListParentsByNamespaceFiltered(ctx context.Context, nsID uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error) {
	m.lastFilters = filters
	m.filtersSeen = true
	if m.listParentsFn != nil {
		return m.listParentsFn(ctx, nsID, filters, limit, offset)
	}
	return nil, nil
}

func (m *mockMemoryLister) CountParentsByNamespaceFiltered(ctx context.Context, nsID uuid.UUID, filters storage.MemoryListFilters) (int, error) {
	if m.countParentsFn != nil {
		return m.countParentsFn(ctx, nsID, filters)
	}
	return 0, nil
}

func (m *mockMemoryLister) FindChildrenByParents(ctx context.Context, nsID uuid.UUID, parentIDs []uuid.UUID, relations []string) (map[uuid.UUID][]model.Memory, error) {
	if m.findChildrenFn != nil {
		return m.findChildrenFn(ctx, nsID, parentIDs, relations)
	}
	return map[uuid.UUID][]model.Memory{}, nil
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

// --- filter parsing + IDs handler ---

func TestListHandler_FiltersForwardedToRepo(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) { return 0, nil },
		listFn: func(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
			return nil, nil
		},
	}
	router := newListRouter(memRepo, &mockProjectGetter{project: proj})

	w := doListRequest(router, projectID.String(),
		"tag=alpha&tag=beta&date_from=2026-01-01&date_to=2026-12-31&enriched=true&source=ingest&search=hello")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if !memRepo.filtersSeen {
		t.Fatal("expected filtered repo methods to be called")
	}
	got := memRepo.lastFilters
	if len(got.Tags) != 2 || got.Tags[0] != "alpha" || got.Tags[1] != "beta" {
		t.Errorf("tags = %v", got.Tags)
	}
	if got.DateFrom == nil || got.DateFrom.Year() != 2026 || got.DateFrom.Month() != 1 {
		t.Errorf("date_from = %v", got.DateFrom)
	}
	if got.DateTo == nil {
		t.Errorf("date_to = nil")
	}
	if got.Enriched == nil || !*got.Enriched {
		t.Errorf("expected Enriched=*true, got %+v", got.Enriched)
	}
	if got.Source != "ingest" {
		t.Errorf("source = %q", got.Source)
	}
	if got.Search != "hello" {
		t.Errorf("search = %q", got.Search)
	}
}

func TestListHandler_InvalidDateFilter(t *testing.T) {
	projectID := uuid.New()
	router := newListRouter(&mockMemoryLister{}, &mockProjectGetter{
		project: &model.Project{ID: projectID, NamespaceID: uuid.New()},
	})

	w := doListRequest(router, projectID.String(), "date_from=not-a-date")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestListIDsHandler_Success(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	wantIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	memRepo := &mockMemoryLister{
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) { return 5, nil },
		listIDsFn: func(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, max int) ([]uuid.UUID, error) {
			if max != defaultMaxListIDs {
				t.Errorf("expected default max %d, got %d", defaultMaxListIDs, max)
			}
			return wantIDs, nil
		},
	}

	r := chi.NewRouter()
	r.Get("/v1/projects/{project_id}/memories/ids", NewListIDsHandler(memRepo, &mockProjectGetter{project: proj}))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/"+projectID.String()+"/memories/ids?tag=foo", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp ListIDsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.IDs) != 3 {
		t.Errorf("expected 3 ids, got %d", len(resp.IDs))
	}
	if !resp.Truncated {
		t.Errorf("expected truncated=true (3 ids of 5 total)")
	}
	if resp.TotalMatching != 5 {
		t.Errorf("expected total_matching=5, got %d", resp.TotalMatching)
	}
	if !memRepo.filtersSeen || len(memRepo.lastFilters.Tags) != 1 {
		t.Errorf("expected tag filter forwarded; got %+v", memRepo.lastFilters)
	}
}

func TestListIDsHandler_HonorsHardCap(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) { return 0, nil },
		listIDsFn: func(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, max int) ([]uuid.UUID, error) {
			if max != hardCapListIDs {
				t.Errorf("expected hard cap %d, got %d", hardCapListIDs, max)
			}
			return nil, nil
		},
	}

	r := chi.NewRouter()
	r.Get("/v1/projects/{project_id}/memories/ids", NewListIDsHandler(memRepo, &mockProjectGetter{project: proj}))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/"+projectID.String()+"/memories/ids?max=999999", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestListHandler_HidesSupersededByDefault(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) { return 0, nil },
		listFn: func(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
			return nil, nil
		},
	}
	router := newListRouter(memRepo, &mockProjectGetter{project: proj})

	if w := doListRequest(router, projectID.String(), ""); w.Code != http.StatusOK {
		t.Fatalf("default request: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !memRepo.lastFilters.HideSuperseded {
		t.Errorf("default should set HideSuperseded=true; got %+v", memRepo.lastFilters)
	}

	memRepo.filtersSeen = false
	if w := doListRequest(router, projectID.String(), "include_superseded=true"); w.Code != http.StatusOK {
		t.Fatalf("include request: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if memRepo.lastFilters.HideSuperseded {
		t.Errorf("include_superseded=true should clear HideSuperseded; got %+v", memRepo.lastFilters)
	}
}

func TestDetailHandler_HidesSupersededByDefault(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	memoryID := uuid.New()
	winnerID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	now := time.Now()

	memRepo := &mockMemoryLister{
		getFn: func(_ context.Context, id uuid.UUID) (*model.Memory, error) {
			return &model.Memory{
				ID: id, NamespaceID: nsID, Content: "loser",
				SupersededBy: &winnerID,
				CreatedAt:    now, UpdatedAt: now,
			}, nil
		},
	}
	router := newListRouter(memRepo, &mockProjectGetter{project: proj})

	w := doDetailRequest(router, projectID.String(), memoryID.String())
	if w.Code != http.StatusNotFound {
		t.Fatalf("default detail of superseded should 404; got %d: %s", w.Code, w.Body.String())
	}

	path := "/v1/projects/" + projectID.String() + "/memories/" + memoryID.String() + "?include_superseded=true"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("include_superseded=true should surface loser; got %d: %s", w.Code, w.Body.String())
	}
}

func TestListHandler_GroupByParent_EmbedsChildren(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	parentID := uuid.New()
	child1ID := uuid.New()
	child2ID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	now := time.Now()

	listParentsCalled := false
	findChildrenCalled := false

	memRepo := &mockMemoryLister{
		listParentsFn: func(_ context.Context, gotNS uuid.UUID, _ storage.MemoryListFilters, _, _ int) ([]model.Memory, error) {
			listParentsCalled = true
			if gotNS != nsID {
				t.Errorf("expected namespace %s, got %s", nsID, gotNS)
			}
			return []model.Memory{{
				ID: parentID, NamespaceID: nsID, Content: "parent",
				CreatedAt: now, UpdatedAt: now,
			}}, nil
		},
		countParentsFn: func(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters) (int, error) {
			return 1, nil
		},
		findChildrenFn: func(_ context.Context, gotNS uuid.UUID, parentIDs []uuid.UUID, relations []string) (map[uuid.UUID][]model.Memory, error) {
			findChildrenCalled = true
			if gotNS != nsID {
				t.Errorf("findChildren expected namespace %s, got %s", nsID, gotNS)
			}
			if len(parentIDs) != 1 || parentIDs[0] != parentID {
				t.Errorf("findChildren expected [%s], got %v", parentID, parentIDs)
			}
			if len(relations) == 0 {
				t.Errorf("findChildren expected non-empty relations")
			}
			return map[uuid.UUID][]model.Memory{
				parentID: {
					{ID: child1ID, NamespaceID: nsID, Content: "fact 1", Enriched: true, ParentID: &parentID, CreatedAt: now, UpdatedAt: now},
					{ID: child2ID, NamespaceID: nsID, Content: "fact 2", Enriched: true, ParentID: &parentID, CreatedAt: now, UpdatedAt: now},
				},
			}, nil
		},
		// Flat-mode functions should NOT be called.
		listFn: func(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
			t.Errorf("flat list should not be called when group_by_parent=true")
			return nil, nil
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) {
			t.Errorf("flat count should not be called when group_by_parent=true")
			return 0, nil
		},
	}

	router := newListRouter(memRepo, &mockProjectGetter{project: proj})
	w := doListRequest(router, projectID.String(), "group_by_parent=true")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !listParentsCalled {
		t.Errorf("expected ListParentsByNamespaceFiltered to be called")
	}
	if !findChildrenCalled {
		t.Errorf("expected FindChildrenByParents to be called")
	}

	var resp model.PaginatedResponse[model.Memory]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 parent, got %d", len(resp.Data))
	}
	if resp.Pagination.Total != 1 {
		t.Errorf("expected total=1 (parents), got %d", resp.Pagination.Total)
	}
	parent := resp.Data[0]
	if parent.ID != parentID {
		t.Fatalf("expected parent id %s, got %s", parentID, parent.ID)
	}
	if len(parent.Children) != 2 {
		t.Fatalf("expected 2 embedded children, got %d", len(parent.Children))
	}
	if parent.Children[0].ID != child1ID || parent.Children[1].ID != child2ID {
		t.Fatalf("unexpected child ids: %s, %s", parent.Children[0].ID, parent.Children[1].ID)
	}
}

func TestListHandler_GroupByParent_NoChildrenReturnsEmptySlice(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	parentID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	now := time.Now()

	memRepo := &mockMemoryLister{
		listParentsFn: func(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, _, _ int) ([]model.Memory, error) {
			return []model.Memory{{ID: parentID, NamespaceID: nsID, Content: "lone parent", CreatedAt: now, UpdatedAt: now}}, nil
		},
		countParentsFn: func(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters) (int, error) {
			return 1, nil
		},
		findChildrenFn: func(_ context.Context, _ uuid.UUID, _ []uuid.UUID, _ []string) (map[uuid.UUID][]model.Memory, error) {
			return map[uuid.UUID][]model.Memory{}, nil
		},
	}

	router := newListRouter(memRepo, &mockProjectGetter{project: proj})
	w := doListRequest(router, projectID.String(), "group_by_parent=true")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Children field may be absent or empty for parents with no enrichment
	// children — both forms are equivalent on the client.
	var resp model.PaginatedResponse[model.Memory]
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 parent, got %d", len(resp.Data))
	}
	if len(resp.Data[0].Children) != 0 {
		t.Errorf("expected 0 children, got %d", len(resp.Data[0].Children))
	}
}

func TestListHandler_DefaultMode_OmitsChildren(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	memID := uuid.New()
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	now := time.Now()

	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
			return []model.Memory{{ID: memID, NamespaceID: nsID, Content: "x", CreatedAt: now, UpdatedAt: now}}, nil
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) {
			return 1, nil
		},
		listParentsFn: func(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, _, _ int) ([]model.Memory, error) {
			t.Errorf("parent list should not be called without group_by_parent")
			return nil, nil
		},
		findChildrenFn: func(_ context.Context, _ uuid.UUID, _ []uuid.UUID, _ []string) (map[uuid.UUID][]model.Memory, error) {
			t.Errorf("findChildren should not be called without group_by_parent")
			return nil, nil
		},
	}

	router := newListRouter(memRepo, &mockProjectGetter{project: proj})
	w := doListRequest(router, projectID.String(), "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()
	if strings.Contains(body, `"children"`) {
		t.Errorf("default flat-mode response should omit the children field; body: %s", body)
	}
}
