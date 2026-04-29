package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestEnrichmentGateMiddleware_Open(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	mw := EnrichmentGateMiddleware(func() bool { return true })

	rec := httptest.NewRecorder()
	mw(next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/dreaming", nil))

	if !called {
		t.Fatal("next handler should run when gate is open")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestEnrichmentGateMiddleware_Closed(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	mw := EnrichmentGateMiddleware(func() bool { return false })

	rec := httptest.NewRecorder()
	mw(next).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/abc/memories/enrich", nil))

	if called {
		t.Fatal("next handler must not run when gate is closed")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body must be JSON envelope: %v; body=%s", err, rec.Body.String())
	}
	if body.Error.Code != "enrichment_unavailable" {
		t.Errorf("error.code = %q, want %q", body.Error.Code, "enrichment_unavailable")
	}
	if body.Error.Message == "" {
		t.Error("error.message should not be empty")
	}
}

func TestEnrichmentGateMiddleware_NilAvailableFunc(t *testing.T) {
	// A nil available func is treated as "gate closed" — defensive default.
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	})
	mw := EnrichmentGateMiddleware(nil)

	rec := httptest.NewRecorder()
	mw(next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/dreaming", nil))

	if called {
		t.Error("next handler must not run when available func is nil")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestEnrichmentGateMiddleware_LiveReBind(t *testing.T) {
	// The available func is read per request, so flipping the gate state
	// affects subsequent calls without needing to rebuild the middleware.
	var available atomic.Bool
	mw := EnrichmentGateMiddleware(func() bool { return available.Load() })

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	// Closed → 503.
	rec1 := httptest.NewRecorder()
	mw(next).ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/v1/dreaming", nil))
	if rec1.Code != http.StatusServiceUnavailable {
		t.Fatalf("first call: status = %d, want 503", rec1.Code)
	}

	// Live re-bind.
	available.Store(true)

	// Open → 200.
	rec2 := httptest.NewRecorder()
	mw(next).ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/v1/dreaming", nil))
	if rec2.Code != http.StatusOK {
		t.Fatalf("second call: status = %d, want 200", rec2.Code)
	}

	// Live re-bind back to closed.
	available.Store(false)

	rec3 := httptest.NewRecorder()
	mw(next).ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/v1/dreaming", nil))
	if rec3.Code != http.StatusServiceUnavailable {
		t.Fatalf("third call: status = %d, want 503", rec3.Code)
	}
}
