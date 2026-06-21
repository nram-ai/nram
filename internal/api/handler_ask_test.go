package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/service"
)

type mockAskService struct {
	resp *service.AskResponse
	err  error
	last *service.AskRequest
}

func (m *mockAskService) Ask(_ context.Context, req *service.AskRequest) (*service.AskResponse, error) {
	m.last = req
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &service.AskResponse{Answer: "synthesized", Confidence: 0.5}, nil
}

func newMeAskRouter(h http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/me/memories/ask", h)
	return r
}

func TestMeAskHandler_Success(t *testing.T) {
	svc := &mockAskService{resp: &service.AskResponse{Answer: "the answer", Confidence: 0.7}}
	router := newMeAskRouter(NewMeAskHandler(svc, &mockUserReader{}))
	ac := &auth.AuthContext{UserID: uuid.New()}
	w := doRecallRequest(router, "/v1/me/memories/ask", map[string]any{"query": "what?"}, ac)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if svc.last == nil || svc.last.Query != "what?" {
		t.Errorf("ask service not called with the query: %+v", svc.last)
	}
}

func TestMeAskHandler_ForwardsProjectSlug(t *testing.T) {
	svc := &mockAskService{}
	router := newMeAskRouter(NewMeAskHandler(svc, &mockUserReader{}))
	ac := &auth.AuthContext{UserID: uuid.New()}
	w := doRecallRequest(router, "/v1/me/memories/ask", map[string]any{"query": "q", "project": "work"}, ac)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if svc.last.ProjectSlug != "work" {
		t.Errorf("expected project slug forwarded, got %q", svc.last.ProjectSlug)
	}
}

func TestMeAskHandler_EmptyQuery(t *testing.T) {
	router := newMeAskRouter(NewMeAskHandler(&mockAskService{}, &mockUserReader{}))
	ac := &auth.AuthContext{UserID: uuid.New()}
	w := doRecallRequest(router, "/v1/me/memories/ask", map[string]any{"query": "  "}, ac)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty query, got %d", w.Code)
	}
}

func TestMeAskHandler_NoAuth(t *testing.T) {
	router := newMeAskRouter(NewMeAskHandler(&mockAskService{}, &mockUserReader{}))
	w := doRecallRequest(router, "/v1/me/memories/ask", map[string]any{"query": "q"}, nil)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth, got %d", w.Code)
	}
}

func TestMeAskHandler_ProviderUnconfigured(t *testing.T) {
	svc := &mockAskService{err: service.ErrAskProviderUnconfigured}
	router := newMeAskRouter(NewMeAskHandler(svc, &mockUserReader{}))
	ac := &auth.AuthContext{UserID: uuid.New()}
	w := doRecallRequest(router, "/v1/me/memories/ask", map[string]any{"query": "q"}, ac)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when provider unconfigured, got %d", w.Code)
	}
}

func TestAskGateMiddleware(t *testing.T) {
	hit := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	})

	// Disabled → 404, handler not reached.
	gate := AskGateMiddleware(func(context.Context) bool { return false })
	w := httptest.NewRecorder()
	gate(next).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/me/memories/ask", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 when ask disabled, got %d", w.Code)
	}
	if hit {
		t.Error("handler should not run when gate is closed")
	}

	// Enabled → passes through.
	hit = false
	gate = AskGateMiddleware(func(context.Context) bool { return true })
	w = httptest.NewRecorder()
	gate(next).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/me/memories/ask", nil))
	if w.Code != http.StatusOK || !hit {
		t.Errorf("expected pass-through when ask enabled, got %d hit=%v", w.Code, hit)
	}
}
