package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// --- mock move service ---

type mockMoveService struct {
	lastReq *service.MoveRequest
	moveFn  func(ctx context.Context, req *service.MoveRequest) (*service.MoveResponse, error)
}

func (m *mockMoveService) Move(ctx context.Context, req *service.MoveRequest) (*service.MoveResponse, error) {
	m.lastReq = req
	if m.moveFn != nil {
		return m.moveFn(ctx, req)
	}
	results := make([]service.MoveResult, 0, len(req.MemoryIDs))
	for _, id := range req.MemoryIDs {
		results = append(results, service.MoveResult{OldID: id, NewID: uuid.New()})
	}
	return &service.MoveResponse{Moved: len(results), Results: results, LatencyMs: 1}, nil
}

// --- access-config mocks ---

type fakeProjLookup struct{ projects map[uuid.UUID]*model.Project }

func (f fakeProjLookup) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	p, ok := f.projects[id]
	if !ok {
		return nil, fmt.Errorf("project not found")
	}
	return p, nil
}

type fakeNSLookup struct {
	namespaces map[uuid.UUID]*model.Namespace
}

func (f fakeNSLookup) GetByID(_ context.Context, id uuid.UUID) (*model.Namespace, error) {
	n, ok := f.namespaces[id]
	if !ok {
		return nil, fmt.Errorf("namespace not found")
	}
	return n, nil
}

type fakeOrgLookup struct {
	orgs map[uuid.UUID]*model.Organization
}

func (f fakeOrgLookup) GetByID(_ context.Context, id uuid.UUID) (*model.Organization, error) {
	o, ok := f.orgs[id]
	if !ok {
		return nil, fmt.Errorf("org not found")
	}
	return o, nil
}

type fakeUserLookup struct{ users map[uuid.UUID]*model.User }

func (f fakeUserLookup) GetByID(_ context.Context, id uuid.UUID) (*model.User, error) {
	u, ok := f.users[id]
	if !ok {
		return nil, fmt.Errorf("user not found")
	}
	return u, nil
}

// adminCtx returns an auth context for an administrator (bypasses org checks).
func adminCtx() *auth.AuthContext {
	return &auth.AuthContext{UserID: uuid.New(), OrgID: uuid.New(), Role: auth.RoleAdministrator}
}

func newSingleMoveRouter(h http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/{id}/move", h)
	return r
}

func newBulkMoveRouter(h http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/move", h)
	return r
}

func doMoveRequest(router http.Handler, method, path string, body any, ac *auth.AuthContext) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// emptyAccessCfg is sufficient for admin-context tests because admins bypass the
// namespace lookups entirely.
func emptyAccessCfg() ProjectAccessConfig {
	return ProjectAccessConfig{
		Projects:   fakeProjLookup{projects: map[uuid.UUID]*model.Project{}},
		Namespaces: fakeNSLookup{namespaces: map[uuid.UUID]*model.Namespace{}},
		Orgs:       fakeOrgLookup{orgs: map[uuid.UUID]*model.Organization{}},
		Users:      fakeUserLookup{users: map[uuid.UUID]*model.User{}},
	}
}

// --- tests ---

func TestMoveHandler_SingleHappyPath(t *testing.T) {
	svc := &mockMoveService{}
	h := NewMoveHandler(svc, emptyAccessCfg(), nil)
	router := newSingleMoveRouter(h)

	sourceID, memID, targetID := uuid.New(), uuid.New(), uuid.New()
	path := fmt.Sprintf("/v1/projects/%s/memories/%s/move", sourceID, memID)
	w := doMoveRequest(router, http.MethodPost, path, moveRequestBody{TargetProjectID: targetID.String()}, adminCtx())

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if svc.lastReq == nil {
		t.Fatal("move service not called")
	}
	if svc.lastReq.SourceProjectID != sourceID || svc.lastReq.TargetProjectID != targetID {
		t.Errorf("wrong projects: src=%v dst=%v", svc.lastReq.SourceProjectID, svc.lastReq.TargetProjectID)
	}
	if len(svc.lastReq.MemoryIDs) != 1 || svc.lastReq.MemoryIDs[0] != memID {
		t.Errorf("wrong memory ids: %v", svc.lastReq.MemoryIDs)
	}
}

func TestMoveHandler_BulkHappyPath(t *testing.T) {
	svc := &mockMoveService{}
	h := NewBulkMoveHandler(svc, emptyAccessCfg(), nil)
	router := newBulkMoveRouter(h)

	sourceID, targetID := uuid.New(), uuid.New()
	id1, id2 := uuid.New(), uuid.New()
	path := fmt.Sprintf("/v1/projects/%s/memories/move", sourceID)
	body := moveRequestBody{IDs: []uuid.UUID{id1, id2}, TargetProjectID: targetID.String()}
	w := doMoveRequest(router, http.MethodPost, path, body, adminCtx())

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if svc.lastReq == nil || len(svc.lastReq.MemoryIDs) != 2 {
		t.Fatalf("expected 2 memory ids forwarded, got %+v", svc.lastReq)
	}
}

func TestMoveHandler_DestinationNotOwnedForbidden(t *testing.T) {
	// Non-admin user in org A; destination project lives in org B → 403, and the
	// move service must never be invoked.
	userID := uuid.New()
	orgAID, orgANS := uuid.New(), uuid.New()
	targetID, targetNS := uuid.New(), uuid.New()

	cfg := ProjectAccessConfig{
		Projects: fakeProjLookup{projects: map[uuid.UUID]*model.Project{
			targetID: {ID: targetID, NamespaceID: targetNS},
		}},
		Namespaces: fakeNSLookup{namespaces: map[uuid.UUID]*model.Namespace{
			orgANS:   {ID: orgANS, Path: "/org-a/"},
			targetNS: {ID: targetNS, Path: "/org-b/proj/"},
		}},
		Orgs:  fakeOrgLookup{orgs: map[uuid.UUID]*model.Organization{orgAID: {ID: orgAID, NamespaceID: orgANS}}},
		Users: fakeUserLookup{users: map[uuid.UUID]*model.User{userID: {ID: userID, OrgID: orgAID}}},
	}

	svc := &mockMoveService{}
	h := NewMoveHandler(svc, cfg, nil)
	router := newSingleMoveRouter(h)

	ac := &auth.AuthContext{UserID: userID, OrgID: orgAID, Role: "member"}
	sourceID, memID := uuid.New(), uuid.New()
	path := fmt.Sprintf("/v1/projects/%s/memories/%s/move", sourceID, memID)
	w := doMoveRequest(router, http.MethodPost, path, moveRequestBody{TargetProjectID: targetID.String()}, ac)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
	if svc.lastReq != nil {
		t.Error("move service must not be called when destination is not owned")
	}
}

func TestMoveHandler_InvalidTargetProjectID(t *testing.T) {
	svc := &mockMoveService{}
	h := NewMoveHandler(svc, emptyAccessCfg(), nil)
	router := newSingleMoveRouter(h)

	sourceID, memID := uuid.New(), uuid.New()
	path := fmt.Sprintf("/v1/projects/%s/memories/%s/move", sourceID, memID)
	w := doMoveRequest(router, http.MethodPost, path, moveRequestBody{TargetProjectID: "not-a-uuid"}, adminCtx())

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if svc.lastReq != nil {
		t.Error("move service must not be called on invalid target id")
	}
}

func TestMoveHandler_BulkEmptyIDs(t *testing.T) {
	svc := &mockMoveService{}
	h := NewBulkMoveHandler(svc, emptyAccessCfg(), nil)
	router := newBulkMoveRouter(h)

	sourceID, targetID := uuid.New(), uuid.New()
	path := fmt.Sprintf("/v1/projects/%s/memories/move", sourceID)
	w := doMoveRequest(router, http.MethodPost, path, moveRequestBody{IDs: nil, TargetProjectID: targetID.String()}, adminCtx())

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMoveHandler_ServiceErrorMapped(t *testing.T) {
	svc := &mockMoveService{moveFn: func(_ context.Context, _ *service.MoveRequest) (*service.MoveResponse, error) {
		return nil, fmt.Errorf("target_project_id must differ from the source project")
	}}
	h := NewMoveHandler(svc, emptyAccessCfg(), nil)
	router := newSingleMoveRouter(h)

	sourceID, memID, targetID := uuid.New(), uuid.New(), uuid.New()
	path := fmt.Sprintf("/v1/projects/%s/memories/%s/move", sourceID, memID)
	w := doMoveRequest(router, http.MethodPost, path, moveRequestBody{TargetProjectID: targetID.String()}, adminCtx())

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for 'must differ' error, got %d: %s", w.Code, w.Body.String())
	}
}
