package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSetupGuardBlocks(t *testing.T) {
	guard := SetupGuardMiddleware(func(_ context.Context) bool { return false })

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := guard(inner)

	req := httptest.NewRequest(http.MethodGet, "/v1/memories", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "setup_required" {
		t.Errorf("expected code setup_required, got %q", resp.Error.Code)
	}
	if resp.Error.Message != "Complete setup at the admin UI" {
		t.Errorf("unexpected message: %q", resp.Error.Message)
	}
}

func TestSetupGuardPassesThrough(t *testing.T) {
	guard := SetupGuardMiddleware(func(_ context.Context) bool { return true })

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	handler := guard(inner)

	req := httptest.NewRequest(http.MethodGet, "/v1/memories", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "ok" {
		t.Errorf("expected body 'ok', got %q", w.Body.String())
	}
}

func TestSetupCheckerMarkComplete(t *testing.T) {
	sc := &SetupChecker{}

	if sc.IsComplete(context.Background()) {
		t.Fatal("expected not complete before MarkComplete")
	}

	sc.MarkComplete()

	if !sc.IsComplete(context.Background()) {
		t.Fatal("expected complete after MarkComplete")
	}
}
