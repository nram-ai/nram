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
	"github.com/nram-ai/nram/internal/service"
)

// --- mock enrichment service ---

type mockEnrichService struct {
	enrichFn func(ctx context.Context, req *service.EnrichRequest) (*service.EnrichResponse, error)
}

func (m *mockEnrichService) Enrich(ctx context.Context, req *service.EnrichRequest) (*service.EnrichResponse, error) {
	if m.enrichFn != nil {
		return m.enrichFn(ctx, req)
	}
	return &service.EnrichResponse{Queued: 0, Skipped: 0, LatencyMs: 1}, nil
}

// --- helpers ---

func newEnrichRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/enrich", handler)
	return r
}

func doEnrichRequest(router http.Handler, projectID string, body interface{}) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(body)

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories/enrich", &buf)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// --- tests ---

func TestEnrichHandler_ByIDs_Success(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	projectID := uuid.New()

	svc := &mockEnrichService{
		enrichFn: func(ctx context.Context, req *service.EnrichRequest) (*service.EnrichResponse, error) {
			if req.ProjectID != projectID {
				t.Errorf("expected project_id %s, got %s", projectID, req.ProjectID)
			}
			if len(req.MemoryIDs) != 2 {
				t.Errorf("expected 2 memory IDs, got %d", len(req.MemoryIDs))
			}
			if req.MemoryIDs[0] != id1 || req.MemoryIDs[1] != id2 {
				t.Errorf("unexpected memory IDs: %v", req.MemoryIDs)
			}
			if req.Priority != 5 {
				t.Errorf("expected priority 5, got %d", req.Priority)
			}
			return &service.EnrichResponse{Queued: 2, Skipped: 0, LatencyMs: 10}, nil
		},
	}

	router := newEnrichRouter(NewEnrichHandler(svc))
	body := map[string]interface{}{
		"ids":      []string{id1.String(), id2.String()},
		"priority": 5,
	}

	w := doEnrichRequest(router, projectID.String(), body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.EnrichResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Queued != 2 {
		t.Errorf("expected queued=2, got %d", resp.Queued)
	}
	if resp.Skipped != 0 {
		t.Errorf("expected skipped=0, got %d", resp.Skipped)
	}
	if resp.LatencyMs != 10 {
		t.Errorf("expected latency_ms=10, got %d", resp.LatencyMs)
	}
}

func TestEnrichHandler_All_Success(t *testing.T) {
	projectID := uuid.New()

	svc := &mockEnrichService{
		enrichFn: func(ctx context.Context, req *service.EnrichRequest) (*service.EnrichResponse, error) {
			if req.ProjectID != projectID {
				t.Errorf("expected project_id %s, got %s", projectID, req.ProjectID)
			}
			if !req.All {
				t.Error("expected all=true")
			}
			if len(req.MemoryIDs) != 0 {
				t.Errorf("expected no memory IDs, got %d", len(req.MemoryIDs))
			}
			return &service.EnrichResponse{Queued: 47, Skipped: 3, LatencyMs: 12}, nil
		},
	}

	router := newEnrichRouter(NewEnrichHandler(svc))
	body := map[string]interface{}{
		"all": true,
	}

	w := doEnrichRequest(router, projectID.String(), body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.EnrichResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Queued != 47 {
		t.Errorf("expected queued=47, got %d", resp.Queued)
	}
	if resp.Skipped != 3 {
		t.Errorf("expected skipped=3, got %d", resp.Skipped)
	}
}

func TestEnrichHandler_MissingIDsAndAll(t *testing.T) {
	svc := &mockEnrichService{}
	router := newEnrichRouter(NewEnrichHandler(svc))

	projectID := uuid.New()
	body := map[string]interface{}{
		"priority": 3,
	}

	w := doEnrichRequest(router, projectID.String(), body)

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

func TestEnrichHandler_InvalidProjectID(t *testing.T) {
	svc := &mockEnrichService{}
	router := newEnrichRouter(NewEnrichHandler(svc))

	body := map[string]interface{}{
		"all": true,
	}

	w := doEnrichRequest(router, "not-a-uuid", body)

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

func TestEnrichHandler_ServiceError_NotFound(t *testing.T) {
	svc := &mockEnrichService{
		enrichFn: func(ctx context.Context, req *service.EnrichRequest) (*service.EnrichResponse, error) {
			return nil, fmt.Errorf("project not found: record does not exist")
		},
	}
	router := newEnrichRouter(NewEnrichHandler(svc))

	projectID := uuid.New()
	body := map[string]interface{}{
		"all": true,
	}

	w := doEnrichRequest(router, projectID.String(), body)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "not_found" {
		t.Errorf("expected not_found error code, got %+v", envelope.Error)
	}
}

func TestEnrichHandler_ServiceError_Internal(t *testing.T) {
	svc := &mockEnrichService{
		enrichFn: func(ctx context.Context, req *service.EnrichRequest) (*service.EnrichResponse, error) {
			return nil, fmt.Errorf("database connection lost")
		},
	}
	router := newEnrichRouter(NewEnrichHandler(svc))

	projectID := uuid.New()
	body := map[string]interface{}{
		"ids": []string{uuid.New().String()},
	}

	w := doEnrichRequest(router, projectID.String(), body)

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
