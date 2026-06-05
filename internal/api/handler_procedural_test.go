package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// mockProceduralServicer is an in-memory ProceduralServicer for handler tests.
type mockProceduralServicer struct {
	rows map[uuid.UUID]*model.ProceduralEntry
}

func newMockProceduralServicer() *mockProceduralServicer {
	return &mockProceduralServicer{rows: map[uuid.UUID]*model.ProceduralEntry{}}
}

func (m *mockProceduralServicer) List(_ context.Context, ns uuid.UUID) ([]model.ProceduralEntry, error) {
	out := []model.ProceduralEntry{}
	for _, e := range m.rows {
		if e.NamespaceID == ns {
			out = append(out, *e)
		}
	}
	return out, nil
}

func (m *mockProceduralServicer) Get(_ context.Context, id, ns uuid.UUID) (*model.ProceduralEntry, error) {
	e, ok := m.rows[id]
	if !ok || e.NamespaceID != ns {
		return nil, sql.ErrNoRows
	}
	cp := *e
	return &cp, nil
}

func (m *mockProceduralServicer) Create(_ context.Context, e *model.ProceduralEntry) (*model.ProceduralEntry, error) {
	e.ID = uuid.New()
	cp := *e
	m.rows[e.ID] = &cp
	return e, nil
}

func (m *mockProceduralServicer) Update(_ context.Context, e *model.ProceduralEntry) (*model.ProceduralEntry, error) {
	m.rows[e.ID] = e
	return e, nil
}

func (m *mockProceduralServicer) Delete(_ context.Context, id, ns uuid.UUID) error {
	e, ok := m.rows[id]
	if !ok || e.NamespaceID != ns {
		return sql.ErrNoRows
	}
	delete(m.rows, id)
	return nil
}

func doProceduralRequest(handler http.HandlerFunc, method, target string, body any, ac *auth.AuthContext) *httptest.ResponseRecorder {
	return doProceduralRequestID(handler, method, target, body, ac, "")
}

// doProceduralRequestID serves the request, optionally injecting a chi route
// context so {id}-bearing handlers can resolve the path parameter.
func doProceduralRequestID(handler http.HandlerFunc, method, target string, body any, ac *auth.AuthContext, id string) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, target, &buf)
	req.Header.Set("Content-Type", "application/json")
	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}
	if id != "" {
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", id)
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func TestMeProcedural_CreateListGet(t *testing.T) {
	userNSID := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: userNSID}
	svc := newMockProceduralServicer()
	users := &mockUserGetter{user: user}
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}

	// Create.
	createH := NewMeProceduralHandler(svc, users)
	w := doProceduralRequest(createH, http.MethodPost, "/v1/me/procedural",
		map[string]any{"content": "Verify before claiming done.", "priority": 7}, ac)
	if w.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d (%s)", w.Code, w.Body.String())
	}
	var created model.ProceduralEntry
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	if created.ID == uuid.Nil || !created.Enabled || created.Priority != 7 {
		t.Fatalf("unexpected created entry: %+v", created)
	}

	// List.
	w = doProceduralRequest(createH, http.MethodGet, "/v1/me/procedural", nil, ac)
	if w.Code != http.StatusOK {
		t.Fatalf("list: expected 200, got %d", w.Code)
	}
	var list model.PaginatedResponse[model.ProceduralEntry]
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if list.Pagination.Total != 1 || len(list.Data) != 1 {
		t.Fatalf("expected 1 entry, got total=%d len=%d", list.Pagination.Total, len(list.Data))
	}
}

func TestMeProcedural_CreateRequiresContent(t *testing.T) {
	user := &model.User{ID: uuid.New(), NamespaceID: uuid.New()}
	svc := newMockProceduralServicer()
	h := NewMeProceduralHandler(svc, &mockUserGetter{user: user})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}

	w := doProceduralRequest(h, http.MethodPost, "/v1/me/procedural", map[string]any{"content": "  "}, ac)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for blank content, got %d", w.Code)
	}
}

func TestMeProcedural_DeleteForeignReturns404(t *testing.T) {
	user := &model.User{ID: uuid.New(), NamespaceID: uuid.New()}
	svc := newMockProceduralServicer()
	// Seed an entry in a different namespace.
	foreign := &model.ProceduralEntry{ID: uuid.New(), NamespaceID: uuid.New(), Content: "x"}
	svc.rows[foreign.ID] = foreign

	h := NewMeProceduralDeleteHandler(svc, &mockUserGetter{user: user})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}
	w := doProceduralRequestID(h, http.MethodDelete, "/v1/me/procedural/"+foreign.ID.String(), nil, ac, foreign.ID.String())
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 deleting foreign entry, got %d", w.Code)
	}
}
