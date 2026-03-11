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
	"github.com/nram-ai/nram/internal/service"
)

// mockUpdateService implements UpdateServicer for testing.
type mockUpdateService struct {
	updateFn func(ctx context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error)
}

func (m *mockUpdateService) Update(ctx context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, req)
	}
	return &service.UpdateResponse{
		ID:        req.MemoryID,
		ProjectID: req.ProjectID,
	}, nil
}

func newUpdateTestRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Put("/v1/projects/{project_id}/memories/{id}", handler)
	return r
}

func doUpdateRequest(router http.Handler, projectID, memoryID string, body interface{}, ac *auth.AuthContext) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(body)

	req := httptest.NewRequest(http.MethodPut, "/v1/projects/"+projectID+"/memories/"+memoryID, &buf)
	req.Header.Set("Content-Type", "application/json")

	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestUpdateHandler_ContentSuccess(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()
	userID := uuid.New()

	svc := &mockUpdateService{
		updateFn: func(ctx context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error) {
			if req.ProjectID != projectID {
				t.Errorf("expected project_id %s, got %s", projectID, req.ProjectID)
			}
			if req.MemoryID != memoryID {
				t.Errorf("expected memory_id %s, got %s", memoryID, req.MemoryID)
			}
			if req.Content == nil || *req.Content != "updated content" {
				t.Errorf("expected content 'updated content', got %v", req.Content)
			}
			return &service.UpdateResponse{
				ID:              memoryID,
				ProjectID:       projectID,
				Content:         *req.Content,
				Tags:            []string{"existing"},
				PreviousContent: "old content",
				ReEmbedded:      true,
				LatencyMs:       42,
			}, nil
		},
	}

	router := newUpdateTestRouter(NewUpdateHandler(svc))
	ac := &auth.AuthContext{UserID: userID, Role: "user"}
	body := map[string]interface{}{
		"content": "updated content",
	}

	w := doUpdateRequest(router, projectID.String(), memoryID.String(), body, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.UpdateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != memoryID {
		t.Errorf("expected id %s, got %s", memoryID, resp.ID)
	}
	if resp.Content != "updated content" {
		t.Errorf("expected content 'updated content', got %s", resp.Content)
	}
	if resp.PreviousContent != "old content" {
		t.Errorf("expected previous_content 'old content', got %s", resp.PreviousContent)
	}
	if !resp.ReEmbedded {
		t.Error("expected re_embedded to be true")
	}
}

func TestUpdateHandler_TagsOnlySuccess(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()

	svc := &mockUpdateService{
		updateFn: func(ctx context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error) {
			if req.Content != nil {
				t.Error("expected content to be nil")
			}
			if req.Tags == nil {
				t.Fatal("expected tags to be non-nil")
			}
			if len(*req.Tags) != 2 || (*req.Tags)[0] != "new" || (*req.Tags)[1] != "tags" {
				t.Errorf("unexpected tags: %v", *req.Tags)
			}
			return &service.UpdateResponse{
				ID:              memoryID,
				ProjectID:       projectID,
				Content:         "unchanged content",
				Tags:            *req.Tags,
				PreviousContent: "unchanged content",
				ReEmbedded:      false,
				LatencyMs:       10,
			}, nil
		},
	}

	router := newUpdateTestRouter(NewUpdateHandler(svc))
	body := map[string]interface{}{
		"tags": []string{"new", "tags"},
	}

	w := doUpdateRequest(router, projectID.String(), memoryID.String(), body, nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.UpdateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ReEmbedded {
		t.Error("expected re_embedded to be false for tags-only update")
	}
}

func TestUpdateHandler_MissingAllFields(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()

	svc := &mockUpdateService{
		updateFn: func(ctx context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error) {
			return nil, fmt.Errorf("at least one of content, tags, or metadata must be provided")
		},
	}

	router := newUpdateTestRouter(NewUpdateHandler(svc))
	body := map[string]interface{}{}

	w := doUpdateRequest(router, projectID.String(), memoryID.String(), body, nil)

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

func TestUpdateHandler_InvalidProjectID(t *testing.T) {
	memoryID := uuid.New()
	svc := &mockUpdateService{}

	router := newUpdateTestRouter(NewUpdateHandler(svc))
	body := map[string]interface{}{
		"content": "test",
	}

	w := doUpdateRequest(router, "not-a-uuid", memoryID.String(), body, nil)

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

func TestUpdateHandler_InvalidMemoryID(t *testing.T) {
	projectID := uuid.New()
	svc := &mockUpdateService{}

	router := newUpdateTestRouter(NewUpdateHandler(svc))
	body := map[string]interface{}{
		"content": "test",
	}

	w := doUpdateRequest(router, projectID.String(), "not-a-uuid", body, nil)

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

func TestUpdateHandler_ServiceError_NotFound(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()

	svc := &mockUpdateService{
		updateFn: func(ctx context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error) {
			return nil, fmt.Errorf("memory not found: record does not exist")
		},
	}

	router := newUpdateTestRouter(NewUpdateHandler(svc))
	body := map[string]interface{}{
		"content": "test",
	}

	w := doUpdateRequest(router, projectID.String(), memoryID.String(), body, nil)

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
