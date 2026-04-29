package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock repositories ---

type mockMemoryRepo struct {
	createFn func(ctx context.Context, mem *model.Memory) error
	getFn    func(ctx context.Context, id uuid.UUID) (*model.Memory, error)
}

func (m *mockMemoryRepo) Create(ctx context.Context, mem *model.Memory) error {
	if m.createFn != nil {
		return m.createFn(ctx, mem)
	}
	return nil
}

func (m *mockMemoryRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error) {
	if m.getFn != nil {
		return m.getFn(ctx, id)
	}
	return &model.Memory{ID: id}, nil
}

func (m *mockMemoryRepo) LookupByContentHash(_ context.Context, _ uuid.UUID, _ string) (*model.Memory, error) {
	return nil, sql.ErrNoRows
}

var _ = storage.HashContent // ensure storage import is referenced even if dedup hit never asserted in this file

type mockProjectRepo struct {
	project *model.Project
	err     error
}

func (m *mockProjectRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error) {
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

func (m *mockProjectRepo) GetByNamespaceID(ctx context.Context, namespaceID uuid.UUID) (*model.Project, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.project != nil && m.project.NamespaceID == namespaceID {
		return m.project, nil
	}
	return &model.Project{
		ID:          uuid.New(),
		Slug:        "test-project",
		NamespaceID: namespaceID,
	}, nil
}

type mockNamespaceRepo struct{}

func (m *mockNamespaceRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error) {
	return &model.Namespace{ID: id}, nil
}

type mockIngestionLogRepo struct{}

func (m *mockIngestionLogRepo) Create(ctx context.Context, log *model.IngestionLog) error {
	return nil
}

type mockTokenUsageRepo struct{}

func (m *mockTokenUsageRepo) Record(ctx context.Context, usage *model.TokenUsage) error {
	return nil
}

type mockEnrichmentQueueRepo struct{}

func (m *mockEnrichmentQueueRepo) Enqueue(ctx context.Context, item *model.EnrichmentJob) error {
	return nil
}

type mockVectorStore struct{}

func (m *mockVectorStore) Upsert(ctx context.Context, id uuid.UUID, nsID uuid.UUID, embedding []float32, dimension int) error {
	return nil
}

// --- helpers ---

func newTestStoreService(projectRepo service.ProjectRepository) *service.StoreService {
	return service.NewStoreService(
		&mockMemoryRepo{},
		projectRepo,
		&mockNamespaceRepo{},
		&mockIngestionLogRepo{},
		&mockEnrichmentQueueRepo{},
	)
}

func newTestRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories", handler)
	return r
}

func doStoreRequest(router http.Handler, projectID string, body interface{}, ac *auth.AuthContext) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(body)

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories", &buf)
	req.Header.Set("Content-Type", "application/json")

	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// --- tests ---

func TestStoreHandler_Success(t *testing.T) {
	svc := newTestStoreService(&mockProjectRepo{})
	router := newTestRouter(NewStoreHandler(svc, nil))

	projectID := uuid.New()
	userID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"content": "Remember that the user prefers dark mode.",
		"source":  "conversation",
		"tags":    []string{"preference", "ui"},
	}

	w := doStoreRequest(router, projectID.String(), body, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.StoreResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ID == uuid.Nil {
		t.Error("expected non-nil memory ID")
	}
	if resp.ProjectID != projectID {
		t.Errorf("expected project_id %s, got %s", projectID, resp.ProjectID)
	}
	if resp.Content != "Remember that the user prefers dark mode." {
		t.Errorf("unexpected content: %s", resp.Content)
	}
}

func TestStoreHandler_MissingContent(t *testing.T) {
	svc := newTestStoreService(&mockProjectRepo{})
	router := newTestRouter(NewStoreHandler(svc, nil))

	projectID := uuid.New()
	body := map[string]interface{}{
		"source": "test",
	}

	w := doStoreRequest(router, projectID.String(), body, nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request error code, got %+v", envelope.Error)
	}
}

func TestStoreHandler_InvalidProjectID(t *testing.T) {
	svc := newTestStoreService(&mockProjectRepo{})
	router := newTestRouter(NewStoreHandler(svc, nil))

	body := map[string]interface{}{
		"content": "test content",
	}

	w := doStoreRequest(router, "not-a-uuid", body, nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request error, got %+v", envelope.Error)
	}
}

func TestStoreHandler_ServiceError_ProjectNotFound(t *testing.T) {
	projectRepo := &mockProjectRepo{
		err: fmt.Errorf("record not found"),
	}
	svc := newTestStoreService(projectRepo)
	router := newTestRouter(NewStoreHandler(svc, nil))

	projectID := uuid.New()
	body := map[string]interface{}{
		"content": "test content",
	}

	w := doStoreRequest(router, projectID.String(), body, nil)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "not_found" {
		t.Errorf("expected not_found error, got %+v", envelope.Error)
	}
}

func TestStoreHandler_ServiceError_Internal(t *testing.T) {
	memRepo := &mockMemoryRepo{
		createFn: func(ctx context.Context, mem *model.Memory) error {
			return fmt.Errorf("database connection lost")
		},
	}
	svc := service.NewStoreService(
		memRepo,
		&mockProjectRepo{},
		&mockNamespaceRepo{},
		&mockIngestionLogRepo{},
		&mockEnrichmentQueueRepo{},
	)
	router := newTestRouter(NewStoreHandler(svc, nil))

	projectID := uuid.New()
	body := map[string]interface{}{
		"content": "test content",
	}

	w := doStoreRequest(router, projectID.String(), body, nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "internal_error" {
		t.Errorf("expected internal_error, got %+v", envelope.Error)
	}
}

func TestStoreHandler_InvalidJSON(t *testing.T) {
	svc := newTestStoreService(&mockProjectRepo{})
	router := newTestRouter(NewStoreHandler(svc, nil))

	projectID := uuid.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID.String()+"/memories",
		bytes.NewBufferString("{invalid json"))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestStoreHandler_EmitsMemoryCreatedEvent(t *testing.T) {
	bus := events.NewMemoryBus(0, 0)
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	svc := newTestStoreService(&mockProjectRepo{})
	router := newTestRouter(NewStoreHandler(svc, bus))

	projectID := uuid.New()
	userID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"content": "test event emission",
	}

	w := doStoreRequest(router, projectID.String(), body, ac)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	select {
	case ev := <-ch:
		if ev.Type != events.MemoryCreated {
			t.Errorf("expected event type %s, got %s", events.MemoryCreated, ev.Type)
		}
		if ev.Scope != "project:"+projectID.String() {
			t.Errorf("expected scope project:%s, got %s", projectID, ev.Scope)
		}
	default:
		t.Fatal("expected memory.created event to be emitted")
	}
}
