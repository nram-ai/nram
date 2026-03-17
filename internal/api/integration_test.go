package api

// integration_test.go exercises the full HTTP API through a real chi router wired with
// AuthMiddleware, RequireRole, and actual handler instances.  It does NOT trust the
// implementation: every response field is asserted individually and every nil-safety
// guarantee is explicitly verified.
//
// The tests are self-contained in package api so they can reuse the mock types that
// are already declared in the other _test.go files of this package.  No new mock types
// are defined here.

import (
	"bytes"
	"context"
	"database/sql"
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
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
)

// ---------------------------------------------------------------------------
// JWT helpers (mirror router_test.go — cannot import server package from here)
// ---------------------------------------------------------------------------

var integrationJWTSecret = []byte("integration-test-jwt-secret-key-32b")

func integrationJWT(t *testing.T, userID uuid.UUID, role string) string {
	t.Helper()
	tok, err := auth.GenerateJWT(userID, role, integrationJWTSecret, time.Hour)
	if err != nil {
		t.Fatalf("GenerateJWT: %v", err)
	}
	return tok
}

// integrationAPIKeyValidator always rejects API keys so only JWT auth is used.
type integrationAPIKeyValidator struct{}

func (v *integrationAPIKeyValidator) Validate(_ context.Context, _ string) (*model.APIKey, error) {
	return nil, fmt.Errorf("invalid key")
}

// ---------------------------------------------------------------------------
// Full chi router factory
// ---------------------------------------------------------------------------

// integrationRouterConfig holds all dependencies needed to build the full router.
type integrationRouterConfig struct {
	store     http.HandlerFunc
	list      http.HandlerFunc
	detail    http.HandlerFunc
	update    http.HandlerFunc
	delete_   http.HandlerFunc
	batchGet  http.HandlerFunc
	recall    http.HandlerFunc
	bulkForget http.HandlerFunc
	meRecall  http.HandlerFunc
	meProjects http.HandlerFunc
	// admin
	adminDashboard   http.HandlerFunc
	adminSetupStatus http.HandlerFunc
	adminSetup       http.HandlerFunc
	// health
	health http.HandlerFunc
}

func newIntegrationRouter(t *testing.T, cfg integrationRouterConfig) http.Handler {
	t.Helper()

	mw := auth.NewAuthMiddleware(&integrationAPIKeyValidator{}, integrationJWTSecret)

	r := chi.NewRouter()
	r.Use(ErrorMiddleware)

	// Public
	r.Get("/v1/health", nullableHandler(cfg.health))
	r.Get("/v1/admin/setup/status", nullableHandler(cfg.adminSetupStatus))
	r.Post("/v1/admin/setup", nullableHandler(cfg.adminSetup))

	// Authenticated routes
	r.Group(func(r chi.Router) {
		r.Use(mw.Handler)

		r.Route("/v1/projects/{project_id}/memories", func(r chi.Router) {
			r.Post("/", nullableHandler(cfg.store))
			r.Get("/", nullableHandler(cfg.list))
			r.Get("/{id}", nullableHandler(cfg.detail))
			r.Put("/{id}", nullableHandler(cfg.update))
			r.Delete("/{id}", nullableHandler(cfg.delete_))
			r.Post("/get", nullableHandler(cfg.batchGet))
			r.Post("/recall", nullableHandler(cfg.recall))
			r.Post("/forget", nullableHandler(cfg.bulkForget))
		})

		r.Route("/v1/me", func(r chi.Router) {
			r.Post("/memories/recall", nullableHandler(cfg.meRecall))
			r.HandleFunc("/projects", nullableHandler(cfg.meProjects))
		})

		r.Route("/v1/admin", func(r chi.Router) {
			r.Use(auth.RequireRole(auth.RoleAdministrator))
			r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			})
		})
	})

	return r
}

// nullableHandler returns a 501 if h is nil, otherwise h.
func nullableHandler(h http.HandlerFunc) http.HandlerFunc {
	if h == nil {
		return func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotImplemented)
		}
	}
	return h
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

func integrationRequest(t *testing.T, router http.Handler, method, path string, body interface{}, token string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, dest interface{}) {
	t.Helper()
	if err := json.NewDecoder(w.Body).Decode(dest); err != nil {
		t.Fatalf("decode JSON (status %d, body %q): %v", w.Code, w.Body.String(), err)
	}
}

func assertContentType(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	ct := w.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: got %q, want \"application/json; charset=utf-8\" (body: %s)", ct, w.Body.String())
	}
}

func assertErrorEnvelope(t *testing.T, w *httptest.ResponseRecorder, wantCode string, wantStatus int) {
	t.Helper()
	if w.Code != wantStatus {
		t.Errorf("status: got %d, want %d (body: %s)", w.Code, wantStatus, w.Body.String())
	}
	assertContentType(t, w)
	var env errorEnvelope
	decodeJSON(t, w, &env)
	if env.Error == nil {
		t.Fatalf("error envelope missing .error field (body: %s)", w.Body.String())
	}
	if env.Error.Code != wantCode {
		t.Errorf(".error.code: got %q, want %q (message: %s)", env.Error.Code, wantCode, env.Error.Message)
	}
	if env.Error.Message == "" {
		t.Errorf(".error.message must not be empty")
	}
}

// ---------------------------------------------------------------------------
// Store → Recall → Forget lifecycle
// ---------------------------------------------------------------------------

// TestHTTP_StoreMemory_Success verifies the happy path for POST store and
// asserts every documented response field.
func TestHTTP_StoreMemory_Success(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := newTestStoreService(&mockProjectRepo{})
	router := newIntegrationRouter(t, integrationRouterConfig{
		store: NewStoreHandler(svc, nil),
	})

	body := map[string]interface{}{
		"content": "The user always prefers dark mode.",
		"source":  "conversation",
		"tags":    []string{"ui", "preference"},
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories", body, token)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp service.StoreResponse
	decodeJSON(t, w, &resp)

	if resp.ID == uuid.Nil {
		t.Error("response.id must not be nil UUID")
	}
	if resp.ProjectID != projectID {
		t.Errorf("response.project_id: got %s, want %s", resp.ProjectID, projectID)
	}
	if resp.ProjectSlug == "" {
		t.Error("response.project_slug must not be empty")
	}
	if resp.Content != "The user always prefers dark mode." {
		t.Errorf("response.content: got %q", resp.Content)
	}
	if resp.Tags == nil {
		t.Error("response.tags must not be nil (nil-safety)")
	}
	// Tags slice was provided so must be present.
	if len(resp.Tags) != 2 {
		t.Errorf("response.tags: got %v, want [ui preference]", resp.Tags)
	}
	// enriched is false on a plain store without an embedding provider.
	if resp.Enriched {
		t.Error("response.enriched should be false for plain store")
	}
	// latency_ms is non-negative.
	if resp.LatencyMs < 0 {
		t.Errorf("response.latency_ms must be >= 0, got %d", resp.LatencyMs)
	}
}

// TestHTTP_StoreMemory_MissingContent verifies that a 400 with proper error
// envelope is returned when content is absent.
func TestHTTP_StoreMemory_MissingContent(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := newTestStoreService(&mockProjectRepo{})
	router := newIntegrationRouter(t, integrationRouterConfig{
		store: NewStoreHandler(svc, nil),
	})

	body := map[string]interface{}{
		"source": "test",
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories", body, token)
	assertErrorEnvelope(t, w, "bad_request", http.StatusBadRequest)
}

// TestHTTP_StoreMemory_InvalidProjectID verifies that a non-UUID project_id
// produces a 400.
func TestHTTP_StoreMemory_InvalidProjectID(t *testing.T) {
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := newTestStoreService(&mockProjectRepo{})
	router := newIntegrationRouter(t, integrationRouterConfig{
		store: NewStoreHandler(svc, nil),
	})

	body := map[string]interface{}{
		"content": "some content",
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/not-a-uuid/memories", body, token)
	assertErrorEnvelope(t, w, "bad_request", http.StatusBadRequest)
}

// TestHTTP_RecallMemory_Success verifies recall response shape.
func TestHTTP_RecallMemory_Success(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockRecallService{
		recallFn: func(_ context.Context, _ *service.RecallRequest) (*service.RecallResponse, error) {
			return &service.RecallResponse{
				Memories: []service.RecallResult{
					{
						ID:        memoryID,
						ProjectID: projectID,
						Content:   "dark mode preference",
						Tags:      []string{"ui"},
						Score:     0.9,
						CreatedAt: time.Now(),
					},
				},
				Graph: service.RecallGraph{
					Entities:      []service.RecallEntity{},
					Relationships: []service.RecallRelationship{},
				},
				TotalSearched: 5,
				LatencyMs:     3,
			}, nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		recall: NewRecallHandler(svc),
	})

	body := map[string]interface{}{
		"query": "dark mode",
		"limit": 10,
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/recall", body, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp service.RecallResponse
	decodeJSON(t, w, &resp)

	if resp.Memories == nil {
		t.Error("response.memories must not be nil")
	}
	if len(resp.Memories) != 1 {
		t.Fatalf("response.memories: got %d, want 1", len(resp.Memories))
	}
	if resp.Memories[0].ID != memoryID {
		t.Errorf("response.memories[0].id: got %s, want %s", resp.Memories[0].ID, memoryID)
	}
	if resp.Graph.Entities == nil {
		t.Error("response.graph.entities must not be nil")
	}
	if resp.Graph.Relationships == nil {
		t.Error("response.graph.relationships must not be nil")
	}
	if resp.TotalSearched != 5 {
		t.Errorf("response.total_searched: got %d, want 5", resp.TotalSearched)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("response.latency_ms must be >= 0, got %d", resp.LatencyMs)
	}
}

// TestHTTP_RecallMemory_MissingQuery verifies that an empty/absent query returns 400.
func TestHTTP_RecallMemory_MissingQuery(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockRecallService{}
	router := newIntegrationRouter(t, integrationRouterConfig{
		recall: NewRecallHandler(svc),
	})

	body := map[string]interface{}{
		"limit": 10,
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/recall", body, token)
	assertErrorEnvelope(t, w, "bad_request", http.StatusBadRequest)
}

// TestHTTP_RecallMemory_EmptyResults verifies that when recall returns no
// matches the response contains memories:[] (not null) and
// graph.entities:[] (not null).
func TestHTTP_RecallMemory_EmptyResults(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockRecallService{
		recallFn: func(_ context.Context, _ *service.RecallRequest) (*service.RecallResponse, error) {
			return &service.RecallResponse{
				Memories: []service.RecallResult{},
				Graph: service.RecallGraph{
					Entities:      []service.RecallEntity{},
					Relationships: []service.RecallRelationship{},
				},
				TotalSearched: 0,
				LatencyMs:     1,
			}, nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		recall: NewRecallHandler(svc),
	})

	body := map[string]interface{}{"query": "something obscure"}
	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/recall", body, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	// Decode into a raw map so we can inspect the actual JSON values.
	var raw map[string]json.RawMessage
	decodeJSON(t, w, &raw)

	memoriesRaw, ok := raw["memories"]
	if !ok {
		t.Fatal("response must have a 'memories' key")
	}
	if string(memoriesRaw) == "null" {
		t.Error("memories must be [] not null when there are no results")
	}

	// Decode graph sub-object.
	var graphRaw map[string]json.RawMessage
	if err := json.Unmarshal(raw["graph"], &graphRaw); err != nil {
		t.Fatalf("unmarshal graph: %v", err)
	}
	if string(graphRaw["entities"]) == "null" {
		t.Error("graph.entities must be [] not null")
	}
	if string(graphRaw["relationships"]) == "null" {
		t.Error("graph.relationships must be [] not null")
	}
}

// TestHTTP_MeRecall_Unauthenticated verifies that /v1/me/memories/recall returns
// 401 when no Authorization header is supplied.
func TestHTTP_MeRecall_Unauthenticated(t *testing.T) {
	svc := &mockRecallService{}
	users := &mockUserReader{}
	router := newIntegrationRouter(t, integrationRouterConfig{
		meRecall: NewMeRecallHandler(svc, users),
	})

	body := map[string]interface{}{"query": "anything"}
	// No token.
	w := integrationRequest(t, router, http.MethodPost, "/v1/me/memories/recall", body, "")

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want 401 (body: %s)", w.Code, w.Body.String())
	}
}

// TestHTTP_BulkForget_Success verifies the 200 response with deleted count.
func TestHTTP_BulkForget_Success(t *testing.T) {
	projectID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockForgetService{
		forgetFn: func(_ context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
			if req.ProjectID != projectID {
				return nil, fmt.Errorf("unexpected project_id")
			}
			return &service.ForgetResponse{Deleted: 2, LatencyMs: 7}, nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		bulkForget: NewBulkForgetHandler(svc, nil),
	})

	body := map[string]interface{}{
		"ids": []string{id1.String(), id2.String()},
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/forget", body, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp service.ForgetResponse
	decodeJSON(t, w, &resp)

	if resp.Deleted != 2 {
		t.Errorf("response.deleted: got %d, want 2", resp.Deleted)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("response.latency_ms must be >= 0, got %d", resp.LatencyMs)
	}
}

// TestHTTP_SingleDelete_Success verifies DELETE /v1/projects/{id}/memories/{id}
// returns 200 with a response body (not 204).
func TestHTTP_SingleDelete_Success(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockForgetService{
		forgetFn: func(_ context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
			if req.MemoryID == nil || *req.MemoryID != memoryID {
				return nil, fmt.Errorf("unexpected memory_id")
			}
			return &service.ForgetResponse{Deleted: 1, LatencyMs: 3}, nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		delete_: NewDeleteHandler(svc, nil),
	})

	w := integrationRequest(t, router, http.MethodDelete, "/v1/projects/"+projectID.String()+"/memories/"+memoryID.String(), nil, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp service.ForgetResponse
	decodeJSON(t, w, &resp)

	if resp.Deleted != 1 {
		t.Errorf("response.deleted: got %d, want 1", resp.Deleted)
	}
}

// ---------------------------------------------------------------------------
// Update and Get
// ---------------------------------------------------------------------------

// TestHTTP_UpdateMemory_Success verifies PUT update returns 200 with
// previous_content field populated.
func TestHTTP_UpdateMemory_Success(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockUpdateService{
		updateFn: func(_ context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error) {
			if req.Content == nil {
				return nil, fmt.Errorf("content expected")
			}
			return &service.UpdateResponse{
				ID:              memoryID,
				ProjectID:       projectID,
				Content:         *req.Content,
				Tags:            []string{"existing"},
				PreviousContent: "old content",
				ReEmbedded:      false,
				LatencyMs:       12,
			}, nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		update: NewUpdateHandler(svc, nil),
	})

	body := map[string]interface{}{
		"content": "new content",
	}

	w := integrationRequest(t, router, http.MethodPut, "/v1/projects/"+projectID.String()+"/memories/"+memoryID.String(), body, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp service.UpdateResponse
	decodeJSON(t, w, &resp)

	if resp.ID != memoryID {
		t.Errorf("response.id: got %s, want %s", resp.ID, memoryID)
	}
	if resp.Content != "new content" {
		t.Errorf("response.content: got %q, want \"new content\"", resp.Content)
	}
	if resp.PreviousContent != "old content" {
		t.Errorf("response.previous_content: got %q, want \"old content\"", resp.PreviousContent)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("response.latency_ms must be >= 0, got %d", resp.LatencyMs)
	}
}

// TestHTTP_BatchGet_Success verifies that batch get with a mix of found and
// not_found IDs produces the correct response shape.
func TestHTTP_BatchGet_Success(t *testing.T) {
	projectID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()
	idMissing := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockBatchGetServicer{
		resp: &service.BatchGetResponse{
			Found: []service.MemoryDetail{
				{ID: id1, Content: "first memory", Tags: []string{"a"}, CreatedAt: time.Now(), UpdatedAt: time.Now()},
				{ID: id2, Content: "second memory", Tags: []string{"b"}, CreatedAt: time.Now(), UpdatedAt: time.Now()},
			},
			NotFound:  []uuid.UUID{idMissing},
			LatencyMs: 4,
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		batchGet: NewBatchGetHandler(svc),
	})

	body := map[string]interface{}{
		"ids": []string{id1.String(), id2.String(), idMissing.String()},
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/get", body, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp service.BatchGetResponse
	decodeJSON(t, w, &resp)

	if len(resp.Found) != 2 {
		t.Errorf("response.found: got %d items, want 2", len(resp.Found))
	}
	if len(resp.NotFound) != 1 {
		t.Errorf("response.not_found: got %d items, want 1", len(resp.NotFound))
	}
	if resp.NotFound[0] != idMissing {
		t.Errorf("response.not_found[0]: got %s, want %s", resp.NotFound[0], idMissing)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("response.latency_ms must be >= 0, got %d", resp.LatencyMs)
	}
}

// TestHTTP_BatchGet_EmptyIDs verifies that posting an empty ids array returns 400.
func TestHTTP_BatchGet_EmptyIDs(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	svc := &mockBatchGetServicer{}
	router := newIntegrationRouter(t, integrationRouterConfig{
		batchGet: NewBatchGetHandler(svc),
	})

	body := map[string]interface{}{
		"ids": []string{},
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/get", body, token)
	assertErrorEnvelope(t, w, "bad_request", http.StatusBadRequest)
}

// ---------------------------------------------------------------------------
// List and Detail
// ---------------------------------------------------------------------------

// TestHTTP_ListMemories_Pagination verifies custom limit/offset are reflected
// in the pagination metadata.
func TestHTTP_ListMemories_Pagination(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
			if limit != 5 {
				return nil, fmt.Errorf("unexpected limit %d", limit)
			}
			if offset != 10 {
				return nil, fmt.Errorf("unexpected offset %d", offset)
			}
			return []model.Memory{
				{ID: uuid.New(), NamespaceID: nsID, Content: "mem", Tags: []string{}, Metadata: json.RawMessage(`{}`), CreatedAt: time.Now(), UpdatedAt: time.Now()},
			}, nil
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) {
			return 42, nil
		},
	}
	projRepo := &mockProjectGetter{project: proj}

	router := newIntegrationRouter(t, integrationRouterConfig{
		list: NewListHandler(memRepo, projRepo),
	})

	w := integrationRequest(t, router, http.MethodGet, "/v1/projects/"+projectID.String()+"/memories?limit=5&offset=10", nil, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp model.PaginatedResponse[model.Memory]
	decodeJSON(t, w, &resp)

	if resp.Pagination.Limit != 5 {
		t.Errorf("pagination.limit: got %d, want 5", resp.Pagination.Limit)
	}
	if resp.Pagination.Offset != 10 {
		t.Errorf("pagination.offset: got %d, want 10", resp.Pagination.Offset)
	}
	if resp.Pagination.Total != 42 {
		t.Errorf("pagination.total: got %d, want 42", resp.Pagination.Total)
	}
	if len(resp.Data) != 1 {
		t.Errorf("data: got %d items, want 1", len(resp.Data))
	}
}

// TestHTTP_ListMemories_DefaultPagination verifies that omitting limit/offset
// causes the handler to use defaults (limit=50, offset=0).
func TestHTTP_ListMemories_DefaultPagination(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	var capturedLimit, capturedOffset int
	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
			capturedLimit = limit
			capturedOffset = offset
			return []model.Memory{}, nil
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) { return 0, nil },
	}
	projRepo := &mockProjectGetter{project: proj}

	router := newIntegrationRouter(t, integrationRouterConfig{
		list: NewListHandler(memRepo, projRepo),
	})

	w := integrationRequest(t, router, http.MethodGet, "/v1/projects/"+projectID.String()+"/memories", nil, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	if capturedLimit != 50 {
		t.Errorf("default limit: got %d, want 50", capturedLimit)
	}
	if capturedOffset != 0 {
		t.Errorf("default offset: got %d, want 0", capturedOffset)
	}

	var resp model.PaginatedResponse[model.Memory]
	decodeJSON(t, w, &resp)

	if resp.Pagination.Limit != 50 {
		t.Errorf("pagination.limit in response: got %d, want 50", resp.Pagination.Limit)
	}
	if resp.Pagination.Offset != 0 {
		t.Errorf("pagination.offset in response: got %d, want 0", resp.Pagination.Offset)
	}
}

// TestHTTP_DetailMemory_Success verifies GET single memory returns the full
// memory object.
func TestHTTP_DetailMemory_Success(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	memoryID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	now := time.Now().UTC().Truncate(time.Second)
	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	mem := &model.Memory{
		ID:          memoryID,
		NamespaceID: nsID,
		Content:     "the remembered fact",
		Tags:        []string{"fact"},
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

	router := newIntegrationRouter(t, integrationRouterConfig{
		detail: NewDetailHandler(memRepo, projRepo),
	})

	w := integrationRequest(t, router, http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/"+memoryID.String(), nil, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var got model.Memory
	decodeJSON(t, w, &got)

	if got.ID != memoryID {
		t.Errorf("response.id: got %s, want %s", got.ID, memoryID)
	}
	if got.Content != "the remembered fact" {
		t.Errorf("response.content: got %q", got.Content)
	}
}

// TestHTTP_DetailMemory_NotFound verifies GET non-existent memory returns 404.
func TestHTTP_DetailMemory_NotFound(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}
	memRepo := &mockMemoryLister{
		getFn: func(_ context.Context, _ uuid.UUID) (*model.Memory, error) {
			return nil, sql.ErrNoRows
		},
	}
	projRepo := &mockProjectGetter{project: proj}

	router := newIntegrationRouter(t, integrationRouterConfig{
		detail: NewDetailHandler(memRepo, projRepo),
	})

	w := integrationRequest(t, router, http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/"+uuid.New().String(), nil, token)
	assertErrorEnvelope(t, w, "not_found", http.StatusNotFound)
}

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

// TestHTTP_ListProjects_Success verifies GET /v1/me/projects returns a
// paginated response.
func TestHTTP_ListProjects_Success(t *testing.T) {
	userNSID := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: userNSID}
	token := integrationJWT(t, user.ID, auth.RoleMember)

	projects := &mockProjectLister{
		listFn: func(_ context.Context, _ uuid.UUID) ([]model.Project, error) {
			return []model.Project{
				{ID: uuid.New(), Name: "Alpha", Slug: "alpha"},
				{ID: uuid.New(), Name: "Beta", Slug: "beta"},
			}, nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		meProjects: NewMeProjectsHandler(projects, &mockUserGetter{user: user}, &mockNamespaceCreator{}),
	})

	w := integrationRequest(t, router, http.MethodGet, "/v1/me/projects", nil, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp model.PaginatedResponse[model.Project]
	decodeJSON(t, w, &resp)

	if resp.Pagination.Total != 2 {
		t.Errorf("pagination.total: got %d, want 2", resp.Pagination.Total)
	}
	if len(resp.Data) != 2 {
		t.Errorf("data: got %d items, want 2", len(resp.Data))
	}
}

// TestHTTP_CreateProject_Success verifies POST /v1/me/projects returns 201
// with the created project.
func TestHTTP_CreateProject_Success(t *testing.T) {
	userNSID := uuid.New()
	user := &model.User{ID: uuid.New(), NamespaceID: userNSID}
	token := integrationJWT(t, user.ID, auth.RoleMember)

	projects := &mockProjectLister{
		createFn: func(_ context.Context, p *model.Project) error {
			p.ID = uuid.New()
			return nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		meProjects: NewMeProjectsHandler(projects, &mockUserGetter{user: user}, &mockNamespaceCreator{}),
	})

	body := map[string]interface{}{
		"name":        "Integration Test Project",
		"slug":        "integration-test",
		"description": "Created from integration test",
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/me/projects", body, token)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp model.Project
	decodeJSON(t, w, &resp)

	if resp.Name != "Integration Test Project" {
		t.Errorf("response.name: got %q", resp.Name)
	}
	if resp.Slug != "integration-test" {
		t.Errorf("response.slug: got %q", resp.Slug)
	}
}

// ---------------------------------------------------------------------------
// Admin RBAC
// ---------------------------------------------------------------------------

// TestHTTP_AdminRoute_RequiresAuth verifies that hitting an admin endpoint
// without any Authorization header yields 401.
func TestHTTP_AdminRoute_RequiresAuth(t *testing.T) {
	router := newIntegrationRouter(t, integrationRouterConfig{})

	// No token.
	w := integrationRequest(t, router, http.MethodGet, "/v1/admin/dashboard", nil, "")

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want 401 (body: %s)", w.Code, w.Body.String())
	}
}

// TestHTTP_AdminRoute_RequiresAdmin verifies that a member-role JWT is
// rejected by the admin route with 403.
func TestHTTP_AdminRoute_RequiresAdmin(t *testing.T) {
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	router := newIntegrationRouter(t, integrationRouterConfig{})

	w := integrationRequest(t, router, http.MethodGet, "/v1/admin/dashboard", nil, token)

	if w.Code != http.StatusForbidden {
		t.Errorf("status: got %d, want 403 (body: %s)", w.Code, w.Body.String())
	}
}

// TestHTTP_AdminRoute_AdminSucceeds verifies that an administrator JWT
// reaches the admin handler (200 from the inline handler above).
func TestHTTP_AdminRoute_AdminSucceeds(t *testing.T) {
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleAdministrator)

	router := newIntegrationRouter(t, integrationRouterConfig{})

	w := integrationRequest(t, router, http.MethodGet, "/v1/admin/dashboard", nil, token)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
}

// TestHTTP_SetupEndpoint_NoAuth verifies that /v1/admin/setup/status is
// accessible without any authentication.
func TestHTTP_SetupEndpoint_NoAuth(t *testing.T) {
	setupHandler := NewAdminSetupStatusHandler(SetupConfig{
		Store: &mockSetupStore{complete: false, backend: "sqlite"},
	})

	router := newIntegrationRouter(t, integrationRouterConfig{
		adminSetupStatus: setupHandler,
	})

	// No token.
	w := integrationRequest(t, router, http.MethodGet, "/v1/admin/setup/status", nil, "")

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	assertContentType(t, w)

	var resp setupStatusResponse
	decodeJSON(t, w, &resp)
	if resp.Backend != "sqlite" {
		t.Errorf("response.backend: got %q, want \"sqlite\"", resp.Backend)
	}
}

// ---------------------------------------------------------------------------
// Error Format
// ---------------------------------------------------------------------------

// TestHTTP_ErrorFormat_Consistent is a table-driven test that verifies every
// error condition produces a response matching
// {"error":{"code":"...","message":"..."}} regardless of handler or condition.
func TestHTTP_ErrorFormat_Consistent(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	memberToken := integrationJWT(t, userID, auth.RoleMember)

	type errorCase struct {
		name       string
		method     string
		path       string
		body       interface{}
		token      string
		wantStatus int
		wantCode   string
		// handler overrides (lazily built per subtest)
		store     http.HandlerFunc
		recall    http.HandlerFunc
		batchGet  http.HandlerFunc
		bulkForget http.HandlerFunc
		update    http.HandlerFunc
		list      http.HandlerFunc
		detail    http.HandlerFunc
	}

	// Shared mocks.
	alwaysBadRecall := &mockRecallService{}
	alwaysBadBatch := &mockBatchGetServicer{}
	alwaysBadForget := &mockForgetService{}

	cases := []errorCase{
		{
			name:       "store no content 400",
			method:     http.MethodPost,
			path:       "/v1/projects/" + projectID.String() + "/memories",
			body:       map[string]interface{}{"source": "x"},
			token:      memberToken,
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_request",
			store:      NewStoreHandler(newTestStoreService(&mockProjectRepo{}), nil),
		},
		{
			name:       "store invalid project uuid 400",
			method:     http.MethodPost,
			path:       "/v1/projects/not-uuid/memories",
			body:       map[string]interface{}{"content": "x"},
			token:      memberToken,
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_request",
			store:      NewStoreHandler(newTestStoreService(&mockProjectRepo{}), nil),
		},
		{
			name:       "recall no query 400",
			method:     http.MethodPost,
			path:       "/v1/projects/" + projectID.String() + "/memories/recall",
			body:       map[string]interface{}{},
			token:      memberToken,
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_request",
			recall:     NewRecallHandler(alwaysBadRecall),
		},
		{
			name:       "batch get empty ids 400",
			method:     http.MethodPost,
			path:       "/v1/projects/" + projectID.String() + "/memories/get",
			body:       map[string]interface{}{"ids": []string{}},
			token:      memberToken,
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_request",
			batchGet:   NewBatchGetHandler(alwaysBadBatch),
		},
		{
			name:       "bulk forget no filter 400",
			method:     http.MethodPost,
			path:       "/v1/projects/" + projectID.String() + "/memories/forget",
			body:       map[string]interface{}{},
			token:      memberToken,
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_request",
			bulkForget: NewBulkForgetHandler(alwaysBadForget, nil),
		},
		{
			name:   "update missing fields 400",
			method: http.MethodPut,
			path:   "/v1/projects/" + projectID.String() + "/memories/" + uuid.New().String(),
			body:   map[string]interface{}{},
			token:  memberToken,
			// service returns "at least one of" error => mapped to 400
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_request",
			update: NewUpdateHandler(&mockUpdateService{
				updateFn: func(_ context.Context, _ *service.UpdateRequest) (*service.UpdateResponse, error) {
					return nil, fmt.Errorf("at least one of content, tags, or metadata must be provided")
				},
			}, nil),
		},
		{
			name:       "list no auth 401",
			method:     http.MethodGet,
			path:       "/v1/projects/" + projectID.String() + "/memories",
			body:       nil,
			token:      "", // no token
			wantStatus: http.StatusUnauthorized,
			wantCode:   "", // auth middleware uses http.Error, not JSON — skip code check
			list:       NewListHandler(&mockMemoryLister{}, &mockProjectGetter{}),
		},
		{
			name:       "detail not found 404",
			method:     http.MethodGet,
			path:       "/v1/projects/" + projectID.String() + "/memories/" + uuid.New().String(),
			body:       nil,
			token:      memberToken,
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
			detail: NewDetailHandler(&mockMemoryLister{
				getFn: func(_ context.Context, _ uuid.UUID) (*model.Memory, error) {
					return nil, sql.ErrNoRows
				},
			}, &mockProjectGetter{project: &model.Project{ID: projectID, Slug: "t", NamespaceID: uuid.New()}}),
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			router := newIntegrationRouter(t, integrationRouterConfig{
				store:      tc.store,
				recall:     tc.recall,
				batchGet:   tc.batchGet,
				bulkForget: tc.bulkForget,
				update:     tc.update,
				list:       tc.list,
				detail:     tc.detail,
			})

			w := integrationRequest(t, router, tc.method, tc.path, tc.body, tc.token)

			if w.Code != tc.wantStatus {
				t.Errorf("status: got %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}

			// Auth middleware returns plain text errors, not JSON — only assert
			// envelope shape for cases that go through a handler.
			if tc.wantCode == "" {
				return
			}

			assertContentType(t, w)

			var env errorEnvelope
			if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
				t.Fatalf("decode error envelope: %v (body: %s)", err, w.Body.String())
			}
			if env.Error == nil {
				t.Fatalf("error envelope missing .error field (body: %s)", w.Body.String())
			}
			if env.Error.Code != tc.wantCode {
				t.Errorf(".error.code: got %q, want %q (message: %s)", env.Error.Code, tc.wantCode, env.Error.Message)
			}
			if env.Error.Message == "" {
				t.Error(".error.message must not be empty")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Nil Safety
// ---------------------------------------------------------------------------

// TestHTTP_NilTags_BecomeEmptyArray verifies that storing a memory without
// specifying tags returns tags:[] in the response JSON (not null).
func TestHTTP_NilTags_BecomeEmptyArray(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	// Use a custom memory repo that returns nil tags to simulate a DB row with
	// no tags.  The store service normalises this before returning.
	svc := newTestStoreService(&mockProjectRepo{})

	router := newIntegrationRouter(t, integrationRouterConfig{
		store: NewStoreHandler(svc, nil),
	})

	// Do not provide tags in the request.
	body := map[string]interface{}{
		"content": "A memory without any tags.",
	}

	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories", body, token)

	if w.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (body: %s)", w.Code, w.Body.String())
	}

	// Decode into a raw map so we can inspect the literal JSON value.
	var raw map[string]json.RawMessage
	decodeJSON(t, w, &raw)

	tagsRaw, ok := raw["tags"]
	if !ok {
		t.Fatal("response must contain a 'tags' key")
	}
	if string(tagsRaw) == "null" {
		t.Error("tags must be [] (empty array) not null when no tags are stored")
	}
	// Must be a JSON array (starts with '[').
	if len(tagsRaw) == 0 || tagsRaw[0] != '[' {
		t.Errorf("tags must be a JSON array, got: %s", tagsRaw)
	}
}

// TestHTTP_NilMemories_BecomeEmptyArray verifies that recall returning no
// matches serialises memories as [] not null.
func TestHTTP_NilMemories_BecomeEmptyArray(t *testing.T) {
	projectID := uuid.New()
	userID := uuid.New()
	token := integrationJWT(t, userID, auth.RoleMember)

	// Service returns nil slice for Memories to simulate a zero-result DB query.
	svc := &mockRecallService{
		recallFn: func(_ context.Context, _ *service.RecallRequest) (*service.RecallResponse, error) {
			// Return a nil Memories slice deliberately.
			return &service.RecallResponse{
				Memories:      nil,
				Graph:         service.RecallGraph{},
				TotalSearched: 0,
				LatencyMs:     1,
			}, nil
		},
	}

	router := newIntegrationRouter(t, integrationRouterConfig{
		recall: NewRecallHandler(svc),
	})

	body := map[string]interface{}{"query": "anything"}
	w := integrationRequest(t, router, http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/recall", body, token)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	var raw map[string]json.RawMessage
	decodeJSON(t, w, &raw)

	memoriesRaw, ok := raw["memories"]
	if !ok {
		t.Fatal("response must contain a 'memories' key")
	}
	if string(memoriesRaw) == "null" {
		t.Error("memories must be [] (empty array) not null")
	}
	if len(memoriesRaw) == 0 || memoriesRaw[0] != '[' {
		t.Errorf("memories must be a JSON array, got: %s", memoriesRaw)
	}
}

// ---------------------------------------------------------------------------
// Compile-time import sink: ensure provider import is used via newTestStoreService
// which calls service.NewStoreService with a func() provider.EmbeddingProvider.
// ---------------------------------------------------------------------------
var _ = (*provider.EmbeddingProvider)(nil)
