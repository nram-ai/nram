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
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/service"
)

// mockBatchStoreService implements BatchStoreServicer for testing.
type mockBatchStoreService struct {
	batchStoreFn func(ctx context.Context, req *service.BatchStoreRequest) (*service.BatchStoreResponse, error)
}

func (m *mockBatchStoreService) BatchStore(ctx context.Context, req *service.BatchStoreRequest) (*service.BatchStoreResponse, error) {
	if m.batchStoreFn != nil {
		return m.batchStoreFn(ctx, req)
	}
	return &service.BatchStoreResponse{
		Processed:       len(req.Items),
		MemoriesCreated: len(req.Items),
		Errors:          nil,
		LatencyMs:       42,
	}, nil
}

func newBatchStoreRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/batch", handler)
	return r
}

func doBatchStoreRequest(router http.Handler, projectID string, body interface{}, ac *auth.AuthContext) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(body)

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories/batch", &buf)
	req.Header.Set("Content-Type", "application/json")

	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestBatchStoreHandler_Success(t *testing.T) {
	svc := &mockBatchStoreService{}
	router := newBatchStoreRouter(NewBatchStoreHandler(svc, nil))

	projectID := uuid.New()
	userID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"items": []map[string]interface{}{
			{"content": "first memory", "tags": []string{"tag1"}, "source": "test"},
			{"content": "second memory", "tags": []string{"tag2"}},
		},
		"options": map[string]interface{}{
			"enrich": false,
			"ttl":    "30d",
		},
	}

	w := doBatchStoreRequest(router, projectID.String(), body, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.BatchStoreResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Processed != 2 {
		t.Errorf("expected processed=2, got %d", resp.Processed)
	}
	if resp.MemoriesCreated != 2 {
		t.Errorf("expected memories_created=2, got %d", resp.MemoriesCreated)
	}
	if len(resp.Errors) != 0 {
		t.Errorf("expected no errors, got %d", len(resp.Errors))
	}
}

func TestBatchStoreHandler_EmptyItems(t *testing.T) {
	svc := &mockBatchStoreService{}
	router := newBatchStoreRouter(NewBatchStoreHandler(svc, nil))

	projectID := uuid.New()
	body := map[string]interface{}{
		"items": []map[string]interface{}{},
	}

	w := doBatchStoreRequest(router, projectID.String(), body, nil)

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

func TestBatchStoreHandler_InvalidProjectID(t *testing.T) {
	svc := &mockBatchStoreService{}
	router := newBatchStoreRouter(NewBatchStoreHandler(svc, nil))

	body := map[string]interface{}{
		"items": []map[string]interface{}{
			{"content": "test content"},
		},
	}

	w := doBatchStoreRequest(router, "not-a-uuid", body, nil)

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

func TestBatchStoreHandler_ServiceError_ProjectNotFound(t *testing.T) {
	svc := &mockBatchStoreService{
		batchStoreFn: func(ctx context.Context, req *service.BatchStoreRequest) (*service.BatchStoreResponse, error) {
			return nil, fmt.Errorf("project not found: record not found")
		},
	}
	router := newBatchStoreRouter(NewBatchStoreHandler(svc, nil))

	projectID := uuid.New()
	body := map[string]interface{}{
		"items": []map[string]interface{}{
			{"content": "test content"},
		},
	}

	w := doBatchStoreRequest(router, projectID.String(), body, nil)

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

func TestBatchStoreHandler_PartialSuccess(t *testing.T) {
	svc := &mockBatchStoreService{
		batchStoreFn: func(ctx context.Context, req *service.BatchStoreRequest) (*service.BatchStoreResponse, error) {
			return &service.BatchStoreResponse{
				Processed:       3,
				MemoriesCreated: 2,
				Errors: []service.BatchStoreError{
					{Index: 1, Message: "failed to create memory: duplicate content"},
				},
				LatencyMs: 55,
			}, nil
		},
	}
	router := newBatchStoreRouter(NewBatchStoreHandler(svc, nil))

	projectID := uuid.New()
	userID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"items": []map[string]interface{}{
			{"content": "first memory"},
			{"content": "bad memory"},
			{"content": "third memory"},
		},
	}

	w := doBatchStoreRequest(router, projectID.String(), body, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.BatchStoreResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Processed != 3 {
		t.Errorf("expected processed=3, got %d", resp.Processed)
	}
	if resp.MemoriesCreated != 2 {
		t.Errorf("expected memories_created=2, got %d", resp.MemoriesCreated)
	}
	if len(resp.Errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(resp.Errors))
	}
	if resp.Errors[0].Index != 1 {
		t.Errorf("expected error index=1, got %d", resp.Errors[0].Index)
	}
}

func TestBatchStoreHandler_EmitsMemoryCreatedEvents(t *testing.T) {
	bus := events.NewMemoryBus(0, 0)
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	svc := &mockBatchStoreService{}
	router := newBatchStoreRouter(NewBatchStoreHandler(svc, bus))

	projectID := uuid.New()
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	body := map[string]interface{}{
		"items": []map[string]interface{}{
			{"content": "first memory"},
			{"content": "second memory"},
		},
	}

	w := doBatchStoreRequest(router, projectID.String(), body, ac)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	// The mock returns MemoriesCreated=2, so we expect 2 events.
	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			if ev.Type != events.MemoryCreated {
				t.Errorf("event %d: expected type %s, got %s", i, events.MemoryCreated, ev.Type)
			}
			if ev.Scope != "project:"+projectID.String() {
				t.Errorf("event %d: expected scope project:%s, got %s", i, projectID, ev.Scope)
			}
		default:
			t.Fatalf("expected event %d to be emitted", i)
		}
	}
}
