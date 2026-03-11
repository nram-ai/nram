package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/service"
)

// --- mock BatchGetServicer ---

type mockBatchGetServicer struct {
	resp *service.BatchGetResponse
	err  error
}

func (m *mockBatchGetServicer) BatchGet(_ context.Context, req *service.BatchGetRequest) (*service.BatchGetResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

// --- helpers ---

func newBatchGetRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/get", handler)
	return r
}

func doBatchGetRequest(router http.Handler, projectID string, body interface{}) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(body)

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories/get", &buf)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// --- tests ---

func TestBatchGetHandler_Success(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	svc := &mockBatchGetServicer{
		resp: &service.BatchGetResponse{
			Found: []service.MemoryDetail{
				{
					ID:        id1,
					Content:   "memory one",
					Tags:      []string{"tag1"},
					Enriched:  false,
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				{
					ID:        id2,
					Content:   "memory two",
					Tags:      []string{"tag2"},
					Enriched:  true,
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
			},
			NotFound:  []uuid.UUID{id3},
			LatencyMs: 5,
		},
	}

	router := newBatchGetRouter(NewBatchGetHandler(svc))
	projectID := uuid.New()

	body := map[string]interface{}{
		"ids": []string{id1.String(), id2.String(), id3.String()},
	}

	w := doBatchGetRequest(router, projectID.String(), body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.BatchGetResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Found) != 2 {
		t.Errorf("expected 2 found, got %d", len(resp.Found))
	}
	if len(resp.NotFound) != 1 {
		t.Errorf("expected 1 not_found, got %d", len(resp.NotFound))
	}
	if resp.NotFound[0] != id3 {
		t.Errorf("expected not_found to contain %s, got %s", id3, resp.NotFound[0])
	}
	if resp.LatencyMs != 5 {
		t.Errorf("expected latency_ms 5, got %d", resp.LatencyMs)
	}
}

func TestBatchGetHandler_EmptyIDs(t *testing.T) {
	svc := &mockBatchGetServicer{}
	router := newBatchGetRouter(NewBatchGetHandler(svc))
	projectID := uuid.New()

	body := map[string]interface{}{
		"ids": []string{},
	}

	w := doBatchGetRequest(router, projectID.String(), body)

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

func TestBatchGetHandler_InvalidProjectID(t *testing.T) {
	svc := &mockBatchGetServicer{}
	router := newBatchGetRouter(NewBatchGetHandler(svc))

	body := map[string]interface{}{
		"ids": []string{uuid.New().String()},
	}

	w := doBatchGetRequest(router, "not-a-uuid", body)

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

func TestBatchGetHandler_ServiceError_NotFound(t *testing.T) {
	svc := &mockBatchGetServicer{
		err: fmt.Errorf("project not found: record not found"),
	}
	router := newBatchGetRouter(NewBatchGetHandler(svc))
	projectID := uuid.New()

	body := map[string]interface{}{
		"ids": []string{uuid.New().String()},
	}

	w := doBatchGetRequest(router, projectID.String(), body)

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

func TestBatchGetHandler_ServiceError_Internal(t *testing.T) {
	svc := &mockBatchGetServicer{
		err: fmt.Errorf("batch get failed: database connection lost"),
	}
	router := newBatchGetRouter(NewBatchGetHandler(svc))
	projectID := uuid.New()

	body := map[string]interface{}{
		"ids": []string{uuid.New().String()},
	}

	w := doBatchGetRequest(router, projectID.String(), body)

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
