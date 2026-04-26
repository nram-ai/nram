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
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// --- mock recall service ---

type mockRecallService struct {
	recallFn func(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error)
}

func (m *mockRecallService) Recall(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
	if m.recallFn != nil {
		return m.recallFn(ctx, req)
	}
	return &service.RecallResponse{
		Memories:  []service.RecallResult{},
		LatencyMs: 1,
	}, nil
}

// --- mock user reader ---

type mockUserReader struct {
	user *model.User
	err  error
}

func (m *mockUserReader) GetByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.user != nil {
		return m.user, nil
	}
	return &model.User{
		ID:          id,
		NamespaceID: uuid.New(),
	}, nil
}

// --- test helpers ---

func newRecallRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/recall", handler)
	return r
}

func newMeRecallRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/me/memories/recall", handler)
	return r
}

func doRecallRequest(router http.Handler, path string, body interface{}, ac *auth.AuthContext) *httptest.ResponseRecorder {
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

// --- tests ---

func TestRecallHandler_Success(t *testing.T) {
	memoryID := uuid.New()
	projectID := uuid.New()

	svc := &mockRecallService{
		recallFn: func(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
			if req.ProjectID != projectID {
				t.Errorf("expected project_id %s, got %s", projectID, req.ProjectID)
			}
			if req.Query != "dark mode preferences" {
				t.Errorf("unexpected query: %s", req.Query)
			}
			return &service.RecallResponse{
				Memories: []service.RecallResult{
					{
						ID:        memoryID,
						ProjectID: projectID,
						Content:   "User prefers dark mode",
						Tags:      []string{"preference"},
						Score:     0.95,
						CreatedAt: time.Now(),
					},
				},
				LatencyMs: 5,
			}, nil
		},
	}

	router := newRecallRouter(NewRecallHandler(svc))
	userID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"query":         "dark mode preferences",
		"limit":         5,
		"threshold":     0.5,
		"tags":          []string{"preference"},
		"include_graph": true,
		"graph_depth":   2,
	}

	w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall", body, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.RecallResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(resp.Memories))
	}
	if resp.Memories[0].ID != memoryID {
		t.Errorf("expected memory ID %s, got %s", memoryID, resp.Memories[0].ID)
	}
	if resp.Memories[0].Content != "User prefers dark mode" {
		t.Errorf("unexpected content: %s", resp.Memories[0].Content)
	}
}

func TestRecallHandler_MissingQuery(t *testing.T) {
	svc := &mockRecallService{}
	router := newRecallRouter(NewRecallHandler(svc))

	projectID := uuid.New()
	body := map[string]interface{}{
		"limit": 10,
	}

	w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall", body, nil)

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

func TestRecallHandler_InvalidProjectID(t *testing.T) {
	svc := &mockRecallService{}
	router := newRecallRouter(NewRecallHandler(svc))

	body := map[string]interface{}{
		"query": "test query",
	}

	w := doRecallRequest(router, "/v1/projects/not-a-uuid/memories/recall", body, nil)

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

func TestMeRecallHandler_Success(t *testing.T) {
	namespaceID := uuid.New()
	userID := uuid.New()

	users := &mockUserReader{
		user: &model.User{
			ID:          userID,
			NamespaceID: namespaceID,
		},
	}

	svc := &mockRecallService{
		recallFn: func(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
			if req.NamespaceID == nil {
				t.Error("expected NamespaceID to be set")
			} else if *req.NamespaceID != namespaceID {
				t.Errorf("expected namespace_id %s, got %s", namespaceID, *req.NamespaceID)
			}
			if req.UserID == nil || *req.UserID != userID {
				t.Error("expected UserID to match authenticated user")
			}
			return &service.RecallResponse{
				Memories:  []service.RecallResult{},
				LatencyMs: 2,
			}, nil
		},
	}

	router := newMeRecallRouter(NewMeRecallHandler(svc, users))
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"query": "my preferences",
	}

	w := doRecallRequest(router, "/v1/me/memories/recall", body, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp service.RecallResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("unexpected latency: %d", resp.LatencyMs)
	}
}

func TestRecallHandler_ServiceError(t *testing.T) {
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
			name:       "internal error",
			serviceErr: fmt.Errorf("database connection lost"),
			wantCode:   http.StatusInternalServerError,
			wantError:  "internal_error",
		},
		{
			name:       "bad request query required",
			serviceErr: fmt.Errorf("query is required"),
			wantCode:   http.StatusBadRequest,
			wantError:  "bad_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &mockRecallService{
				recallFn: func(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
					return nil, tt.serviceErr
				},
			}

			router := newRecallRouter(NewRecallHandler(svc))
			projectID := uuid.New()
			body := map[string]interface{}{
				"query": "test query",
			}

			w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall", body, nil)

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

func TestRecallHandler_DiversifyByTagPrefix_Forwarded(t *testing.T) {
	projectID := uuid.New()

	var captured *service.RecallRequest
	svc := &mockRecallService{
		recallFn: func(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
			captured = req
			return &service.RecallResponse{
				Memories: []service.RecallResult{},
				CoverageGaps: []service.CoverageGap{
					{GroupKey: "category-c", Cause: "limit"},
				},
				LatencyMs: 1,
			}, nil
		},
	}

	router := newRecallRouter(NewRecallHandler(svc))
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	body := map[string]interface{}{
		"query":                   "q",
		"limit":                   2,
		"diversify_by_tag_prefix": "category-",
	}

	w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall", body, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if captured == nil {
		t.Fatal("service not called")
	}
	if captured.DiversifyByTagPrefix != "category-" {
		t.Errorf("expected DiversifyByTagPrefix=category-, got %q", captured.DiversifyByTagPrefix)
	}

	var resp service.RecallResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.CoverageGaps) != 1 || resp.CoverageGaps[0].GroupKey != "category-c" || resp.CoverageGaps[0].Cause != "limit" {
		t.Errorf("coverage_gaps not round-tripped: %+v", resp.CoverageGaps)
	}
}

func TestMeRecallHandler_DiversifyByTagPrefix_Forwarded(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	users := &mockUserReader{user: &model.User{ID: userID, NamespaceID: nsID}}

	var captured *service.RecallRequest
	svc := &mockRecallService{
		recallFn: func(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
			captured = req
			return &service.RecallResponse{Memories: []service.RecallResult{}, LatencyMs: 1}, nil
		},
	}

	router := newMeRecallRouter(NewMeRecallHandler(svc, users))
	ac := &auth.AuthContext{UserID: userID, Role: "user"}

	body := map[string]interface{}{
		"query":                   "q",
		"diversify_by_tag_prefix": "category-",
	}

	w := doRecallRequest(router, "/v1/me/memories/recall", body, ac)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if captured == nil || captured.DiversifyByTagPrefix != "category-" {
		t.Errorf("DiversifyByTagPrefix not forwarded from /me handler: %+v", captured)
	}
}

// TestRecallHandler_DiversifyByTagPrefix_OmittedFromJSON confirms that an
// empty CoverageGaps slice is elided from the wire format via omitempty, so
// existing clients see no schema drift when the feature is unused.
func TestRecallHandler_DiversifyByTagPrefix_OmittedFromJSON(t *testing.T) {
	projectID := uuid.New()
	svc := &mockRecallService{
		recallFn: func(_ context.Context, _ *service.RecallRequest) (*service.RecallResponse, error) {
			return &service.RecallResponse{
				Memories:  []service.RecallResult{},
				LatencyMs: 1,
			}, nil
		},
	}
	router := newRecallRouter(NewRecallHandler(svc))
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}
	body := map[string]interface{}{"query": "q"}

	w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall", body, ac)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if bytes.Contains(w.Body.Bytes(), []byte("coverage_gaps")) {
		t.Errorf("coverage_gaps should be omitted when empty, body=%s", w.Body.String())
	}
}

func TestMeRecallHandler_NoAuth(t *testing.T) {
	svc := &mockRecallService{}
	users := &mockUserReader{}
	router := newMeRecallRouter(NewMeRecallHandler(svc, users))

	body := map[string]interface{}{
		"query": "test query",
	}

	w := doRecallRequest(router, "/v1/me/memories/recall", body, nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d: %s", w.Code, w.Body.String())
	}
}

// TestRecallHandler_PassesIncludeLowNoveltyFlag confirms include_low_novelty
// in the JSON body propagates to service.RecallRequest.IncludeLowNovelty.
// Default false (omitted from body) preserves the standard recall behavior.
func TestRecallHandler_PassesIncludeLowNoveltyFlag(t *testing.T) {
	projectID := uuid.New()

	var captured *service.RecallRequest
	svc := &mockRecallService{
		recallFn: func(_ context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
			captured = req
			return &service.RecallResponse{Memories: []service.RecallResult{}, LatencyMs: 1}, nil
		},
	}
	router := newRecallRouter(NewRecallHandler(svc))
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	// Default: omitted → false.
	if w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall",
		map[string]interface{}{"query": "q"}, ac); w.Code != http.StatusOK {
		t.Fatalf("default request: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if captured == nil || captured.IncludeLowNovelty {
		t.Errorf("default should keep IncludeLowNovelty=false; got %+v", captured)
	}

	// Opt-in: body field → true.
	if w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall",
		map[string]interface{}{"query": "q", "include_low_novelty": true}, ac); w.Code != http.StatusOK {
		t.Fatalf("opt-in request: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if captured == nil || !captured.IncludeLowNovelty {
		t.Errorf("include_low_novelty=true should set IncludeLowNovelty; got %+v", captured)
	}
}
