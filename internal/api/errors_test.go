package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIErrorImplementsError(t *testing.T) {
	var err error = ErrNotFound("test resource")
	if err.Error() != "not_found: test resource" {
		t.Fatalf("unexpected error string: %s", err.Error())
	}
}

func TestErrBadRequest(t *testing.T) {
	e := ErrBadRequest("invalid input")
	assertAPIError(t, e, "bad_request", "invalid input", http.StatusBadRequest)
}

func TestErrUnauthorized(t *testing.T) {
	e := ErrUnauthorized("missing token")
	assertAPIError(t, e, "unauthorized", "missing token", http.StatusUnauthorized)
}

func TestErrForbidden(t *testing.T) {
	e := ErrForbidden("access denied")
	assertAPIError(t, e, "forbidden", "access denied", http.StatusForbidden)
}

func TestErrNotFound(t *testing.T) {
	e := ErrNotFound("resource not found")
	assertAPIError(t, e, "not_found", "resource not found", http.StatusNotFound)
}

func TestErrConflict(t *testing.T) {
	e := ErrConflict("already exists")
	assertAPIError(t, e, "conflict", "already exists", http.StatusConflict)
}

func TestErrRateLimited(t *testing.T) {
	e := ErrRateLimited("too many requests")
	assertAPIError(t, e, "rate_limited", "too many requests", http.StatusTooManyRequests)
}

func TestErrInternal(t *testing.T) {
	e := ErrInternal("something broke")
	assertAPIError(t, e, "internal_error", "something broke", http.StatusInternalServerError)
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()
	apiErr := ErrNotFound("user not found")
	WriteError(w, apiErr)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Fatalf("unexpected content-type: %s", ct)
	}

	var envelope struct {
		Error struct {
			Code    string      `json:"code"`
			Message string      `json:"message"`
			Details interface{} `json:"details"`
		} `json:"error"`
	}
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if envelope.Error.Code != "not_found" {
		t.Fatalf("expected code not_found, got %s", envelope.Error.Code)
	}
	if envelope.Error.Message != "user not found" {
		t.Fatalf("expected message 'user not found', got %s", envelope.Error.Message)
	}
	if envelope.Error.Details != nil {
		t.Fatalf("expected nil details, got %v", envelope.Error.Details)
	}
}

func TestWriteErrorWithDetails(t *testing.T) {
	w := httptest.NewRecorder()
	apiErr := ErrBadRequest("validation failed")
	apiErr.Details = map[string]string{"field": "email", "reason": "invalid format"}
	WriteError(w, apiErr)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}

	var envelope struct {
		Error struct {
			Code    string                 `json:"code"`
			Message string                 `json:"message"`
			Details map[string]interface{} `json:"details"`
		} `json:"error"`
	}
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if envelope.Error.Details["field"] != "email" {
		t.Fatalf("expected field=email, got %v", envelope.Error.Details["field"])
	}
}

func TestErrorMiddlewareRecoversPanic(t *testing.T) {
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	handler := ErrorMiddleware(panicHandler)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}

	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if envelope.Error.Code != "internal_error" {
		t.Fatalf("expected code internal_error, got %s", envelope.Error.Code)
	}
}

func TestErrorMiddlewarePassesThrough(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := ErrorMiddleware(okHandler)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func assertAPIError(t *testing.T, e *APIError, code, message string, status int) {
	t.Helper()
	if e.Code != code {
		t.Errorf("expected code %q, got %q", code, e.Code)
	}
	if e.Message != message {
		t.Errorf("expected message %q, got %q", message, e.Message)
	}
	if e.Status != status {
		t.Errorf("expected status %d, got %d", status, e.Status)
	}
}
