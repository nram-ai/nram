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
	"github.com/nram-ai/nram/internal/service"
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

func (m *mockProceduralServicer) Export(_ context.Context, ns uuid.UUID) (*service.ProceduralExportData, error) {
	entries := []service.ProceduralExportEntry{}
	for _, e := range m.rows {
		if e.NamespaceID != ns {
			continue
		}
		entries = append(entries, service.ProceduralExportEntry{
			ID:       e.ID,
			Content:  e.Content,
			Title:    e.Title,
			Category: e.Category,
			Tags:     e.Tags,
			Priority: e.Priority,
			Enabled:  e.Enabled,
		})
	}
	return &service.ProceduralExportData{
		Version: "1.0",
		Entries: entries,
		Stats:   service.ProceduralExportStats{Count: len(entries)},
	}, nil
}

func (m *mockProceduralServicer) Import(_ context.Context, ns uuid.UUID, entries []service.ProceduralExportEntry) (*service.ProceduralImportResult, error) {
	res := &service.ProceduralImportResult{Errors: []service.ProceduralImportErr{}}
	for i, in := range entries {
		if in.Content == "" {
			res.Skipped++
			res.Errors = append(res.Errors, service.ProceduralImportErr{Index: i, Message: "content is required"})
			continue
		}
		if in.ID != uuid.Nil {
			if e, ok := m.rows[in.ID]; ok && e.NamespaceID == ns {
				e.Content = in.Content
				e.Title = in.Title
				e.Category = in.Category
				e.Tags = in.Tags
				e.Priority = in.Priority
				e.Enabled = in.Enabled
				res.Updated++
				continue
			}
		}
		id := uuid.New()
		m.rows[id] = &model.ProceduralEntry{
			ID: id, NamespaceID: ns, Content: in.Content, Title: in.Title,
			Category: in.Category, Tags: in.Tags, Priority: in.Priority, Enabled: in.Enabled,
		}
		res.Imported++
	}
	return res, nil
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

func TestMeProcedural_Export(t *testing.T) {
	ns := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: ns}
	svc := newMockProceduralServicer()
	svc.rows[uuid.New()] = &model.ProceduralEntry{ID: uuid.New(), NamespaceID: ns, Content: "a", Enabled: true}
	svc.rows[uuid.New()] = &model.ProceduralEntry{ID: uuid.New(), NamespaceID: ns, Content: "b", Enabled: false}

	h := NewMeProceduralExportHandler(svc, &mockUserGetter{user: user})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}
	w := doProceduralRequest(h, http.MethodGet, "/v1/me/procedural/export", nil, ac)
	if w.Code != http.StatusOK {
		t.Fatalf("export: expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var data service.ProceduralExportData
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
		t.Fatalf("unmarshal export: %v", err)
	}
	if data.Stats.Count != 2 || len(data.Entries) != 2 {
		t.Fatalf("expected 2 entries, got count=%d len=%d", data.Stats.Count, len(data.Entries))
	}
}

func TestMeProcedural_ImportEnvelopeAndBareArray(t *testing.T) {
	ns := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: ns}
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}

	// Envelope form.
	svc := newMockProceduralServicer()
	h := NewMeProceduralImportHandler(svc, &mockUserGetter{user: user})
	envelope := service.ProceduralExportData{
		Version: "1.0",
		Entries: []service.ProceduralExportEntry{{Content: "one"}, {Content: "two"}},
	}
	w := doProceduralRequest(h, http.MethodPost, "/v1/me/procedural/import", envelope, ac)
	if w.Code != http.StatusOK {
		t.Fatalf("import envelope: expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var res service.ProceduralImportResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if res.Imported != 2 {
		t.Fatalf("envelope import: expected 2 imported, got %d", res.Imported)
	}

	// Bare array form.
	svc2 := newMockProceduralServicer()
	h2 := NewMeProceduralImportHandler(svc2, &mockUserGetter{user: user})
	bare := []service.ProceduralExportEntry{{Content: "x"}}
	w2 := doProceduralRequest(h2, http.MethodPost, "/v1/me/procedural/import", bare, ac)
	if w2.Code != http.StatusOK {
		t.Fatalf("import bare: expected 200, got %d (%s)", w2.Code, w2.Body.String())
	}
	var res2 service.ProceduralImportResult
	if err := json.Unmarshal(w2.Body.Bytes(), &res2); err != nil {
		t.Fatalf("unmarshal bare result: %v", err)
	}
	if res2.Imported != 1 {
		t.Fatalf("bare import: expected 1 imported, got %d", res2.Imported)
	}
}

func TestMeProcedural_ImportMalformedReturns400(t *testing.T) {
	user := &model.User{ID: uuid.New(), NamespaceID: uuid.New()}
	svc := newMockProceduralServicer()
	h := NewMeProceduralImportHandler(svc, &mockUserGetter{user: user})
	ac := &auth.AuthContext{UserID: user.ID, Role: "user"}

	req := httptest.NewRequest(http.MethodPost, "/v1/me/procedural/import", bytes.NewBufferString("{not json"))
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed body, got %d", w.Code)
	}
}
