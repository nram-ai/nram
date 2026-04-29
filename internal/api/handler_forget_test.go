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

// --- mock forget service ---

type mockForgetService struct {
	forgetFn func(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error)
}

func (m *mockForgetService) Forget(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
	if m.forgetFn != nil {
		return m.forgetFn(ctx, req)
	}
	return &service.ForgetResponse{Deleted: 0, LatencyMs: 1}, nil
}

// --- test helpers ---

func newBulkForgetRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/forget", handler)
	return r
}

func newDeleteRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Delete("/v1/projects/{project_id}/memories/{id}", handler)
	return r
}

func doBulkForgetRequest(router http.Handler, path string, body interface{}, ac *auth.AuthContext) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(body)

	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", "application/json")

	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func doDeleteRequest(router http.Handler, path string, ac *auth.AuthContext) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, path, nil)

	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// --- tests ---

func TestBulkForgetHandler_Success(t *testing.T) {
	projectID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()

	svc := &mockForgetService{
		forgetFn: func(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
			if req.ProjectID != projectID {
				t.Errorf("expected project_id %s, got %s", projectID, req.ProjectID)
			}
			if len(req.MemoryIDs) != 2 {
				t.Errorf("expected 2 memory IDs, got %d", len(req.MemoryIDs))
			}
			if req.HardDelete != true {
				t.Error("expected hard_delete to be true")
			}
			return &service.ForgetResponse{Deleted: 2, LatencyMs: 15}, nil
		},
	}

	router := newBulkForgetRouter(NewBulkForgetHandler(svc, nil))
	userID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"ids":  []string{id1.String(), id2.String()},
		"hard": true,
	}

	w := doBulkForgetRequest(router, "/v1/projects/"+projectID.String()+"/memories/forget", body, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.ForgetResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Deleted != 2 {
		t.Errorf("expected deleted=2, got %d", resp.Deleted)
	}
	if resp.LatencyMs != 15 {
		t.Errorf("expected latency_ms=15, got %d", resp.LatencyMs)
	}
}

func TestBulkForgetHandler_MissingFilters(t *testing.T) {
	svc := &mockForgetService{}
	router := newBulkForgetRouter(NewBulkForgetHandler(svc, nil))
	projectID := uuid.New()

	// Body with no ids and no tags.
	body := map[string]interface{}{
		"hard": false,
	}

	w := doBulkForgetRequest(router, "/v1/projects/"+projectID.String()+"/memories/forget", body, nil)

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

func TestBulkForgetHandler_InvalidProjectID(t *testing.T) {
	svc := &mockForgetService{}
	router := newBulkForgetRouter(NewBulkForgetHandler(svc, nil))

	body := map[string]interface{}{
		"ids": []string{uuid.New().String()},
	}

	w := doBulkForgetRequest(router, "/v1/projects/not-a-uuid/memories/forget", body, nil)

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

func TestDeleteHandler_Success(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()

	svc := &mockForgetService{
		forgetFn: func(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
			if req.ProjectID != projectID {
				t.Errorf("expected project_id %s, got %s", projectID, req.ProjectID)
			}
			if req.MemoryID == nil {
				t.Fatal("expected memory_id to be set")
			}
			if *req.MemoryID != memoryID {
				t.Errorf("expected memory_id %s, got %s", memoryID, *req.MemoryID)
			}
			if req.HardDelete {
				t.Error("expected hard_delete to be false for single delete")
			}
			return &service.ForgetResponse{Deleted: 1, LatencyMs: 5}, nil
		},
	}

	router := newDeleteRouter(NewDeleteHandler(svc, nil))
	userID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	w := doDeleteRequest(router, "/v1/projects/"+projectID.String()+"/memories/"+memoryID.String(), ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.ForgetResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("expected deleted=1, got %d", resp.Deleted)
	}
	if resp.LatencyMs != 5 {
		t.Errorf("expected latency_ms=5, got %d", resp.LatencyMs)
	}
}

func TestDeleteHandler_InvalidMemoryID(t *testing.T) {
	svc := &mockForgetService{}
	router := newDeleteRouter(NewDeleteHandler(svc, nil))
	projectID := uuid.New()

	w := doDeleteRequest(router, "/v1/projects/"+projectID.String()+"/memories/not-a-uuid", nil)

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

func TestForgetHandler_ServiceErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr error
		wantCode   int
		wantError  string
	}{
		{
			name:       "not found error",
			serviceErr: fmt.Errorf("project not found"),
			wantCode:   http.StatusNotFound,
			wantError:  "not_found",
		},
		{
			name:       "bad request error",
			serviceErr: fmt.Errorf("at least one of memory_id, memory_ids, or tags must be provided"),
			wantCode:   http.StatusBadRequest,
			wantError:  "bad_request",
		},
		{
			name:       "internal error",
			serviceErr: fmt.Errorf("database connection lost"),
			wantCode:   http.StatusInternalServerError,
			wantError:  "internal_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &mockForgetService{
				forgetFn: func(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
					return nil, tt.serviceErr
				},
			}

			router := newBulkForgetRouter(NewBulkForgetHandler(svc, nil))
			projectID := uuid.New()
			body := map[string]interface{}{
				"ids": []string{uuid.New().String()},
			}

			w := doBulkForgetRequest(router, "/v1/projects/"+projectID.String()+"/memories/forget", body, nil)

			if w.Code != tt.wantCode {
				t.Fatalf("expected status %d, got %d: %s", tt.wantCode, w.Code, w.Body.String())
			}

			var envelope errorEnvelope
			if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
				t.Fatalf("failed to decode error: %v", err)
			}
			if envelope.Error == nil || envelope.Error.Code != tt.wantError {
				t.Errorf("expected %s error, got %+v", tt.wantError, envelope.Error)
			}
		})
	}
}

func TestBulkForgetHandler_TagsOnly(t *testing.T) {
	projectID := uuid.New()

	svc := &mockForgetService{
		forgetFn: func(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
			if len(req.Tags) != 2 {
				t.Errorf("expected 2 tags, got %d", len(req.Tags))
			}
			if len(req.MemoryIDs) != 0 {
				t.Errorf("expected 0 memory IDs, got %d", len(req.MemoryIDs))
			}
			return &service.ForgetResponse{Deleted: 3, LatencyMs: 10}, nil
		},
	}

	router := newBulkForgetRouter(NewBulkForgetHandler(svc, nil))

	body := map[string]interface{}{
		"tags": []string{"obsolete", "temp"},
	}

	w := doBulkForgetRequest(router, "/v1/projects/"+projectID.String()+"/memories/forget", body, nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteHandler_EmitsMemoryDeletedEvent(t *testing.T) {
	bus := events.NewMemoryBus(0, 0)
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	projectID := uuid.New()
	memoryID := uuid.New()

	svc := &mockForgetService{
		forgetFn: func(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
			return &service.ForgetResponse{Deleted: 1, LatencyMs: 5}, nil
		},
	}

	router := newDeleteRouter(NewDeleteHandler(svc, bus))
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	w := doDeleteRequest(router, "/v1/projects/"+projectID.String()+"/memories/"+memoryID.String(), ac)
	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	select {
	case ev := <-ch:
		if ev.Type != events.MemoryDeleted {
			t.Errorf("expected event type %s, got %s", events.MemoryDeleted, ev.Type)
		}
		if ev.Scope != "project:"+projectID.String() {
			t.Errorf("expected scope project:%s, got %s", projectID, ev.Scope)
		}
	default:
		t.Fatal("expected memory.deleted event to be emitted")
	}
}

func TestBulkForgetHandler_EmitsMemoryDeletedEvents(t *testing.T) {
	bus := events.NewMemoryBus(0, 0)
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	projectID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()

	svc := &mockForgetService{
		forgetFn: func(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error) {
			return &service.ForgetResponse{Deleted: 2, LatencyMs: 10}, nil
		},
	}

	router := newBulkForgetRouter(NewBulkForgetHandler(svc, bus))

	body := map[string]interface{}{
		"ids": []string{id1.String(), id2.String()},
	}

	w := doBulkForgetRequest(router, "/v1/projects/"+projectID.String()+"/memories/forget", body, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			if ev.Type != events.MemoryDeleted {
				t.Errorf("event %d: expected type %s, got %s", i, events.MemoryDeleted, ev.Type)
			}
		default:
			t.Fatalf("expected event %d to be emitted", i)
		}
	}
}
