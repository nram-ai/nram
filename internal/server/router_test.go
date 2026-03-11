package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// mockAPIKeyValidator implements auth.APIKeyValidator for testing.
type mockAPIKeyValidator struct{}

func (m *mockAPIKeyValidator) Validate(_ context.Context, _ string) (*model.APIKey, error) {
	return nil, fmt.Errorf("invalid key")
}

var testJWTSecret = []byte("test-secret-key-for-router-tests")

func generateTestJWT(t *testing.T, userID uuid.UUID, role string) string {
	t.Helper()
	token, err := auth.GenerateJWT(userID, role, testJWTSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to generate test JWT: %v", err)
	}
	return token
}

func TestHealthEndpointNoAuth(t *testing.T) {
	healthCalled := false
	handlers := Handlers{
		Health: func(w http.ResponseWriter, r *http.Request) {
			healthCalled = true
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		},
	}

	r := newTestRouter(t, handlers)

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if !healthCalled {
		t.Error("health handler was not called")
	}
}

func TestMetricsEndpointNoAuth(t *testing.T) {
	r := newTestRouter(t, Handlers{})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAuthenticatedProjectRoute(t *testing.T) {
	storeCalled := false
	handlers := Handlers{
		Store: func(w http.ResponseWriter, r *http.Request) {
			storeCalled = true
			w.WriteHeader(http.StatusCreated)
		},
	}

	r := newTestRouter(t, handlers)
	userID := uuid.New()
	token := generateTestJWT(t, userID, auth.RoleMember)

	projectID := uuid.New().String()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if !storeCalled {
		t.Error("store handler was not called")
	}
}

func TestUnauthenticatedRequestReturns401(t *testing.T) {
	r := newTestRouter(t, Handlers{})

	projectID := uuid.New().String()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+projectID+"/memories", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", rec.Code)
	}
}

func TestAdminRouteNonAdminReturns403(t *testing.T) {
	r := newTestRouter(t, Handlers{})
	userID := uuid.New()
	token := generateTestJWT(t, userID, auth.RoleMember)

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/dashboard", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected status 403, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestAdminRouteAdminReturns200(t *testing.T) {
	dashboardCalled := false
	handlers := Handlers{
		AdminDashboard: func(w http.ResponseWriter, r *http.Request) {
			dashboardCalled = true
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"data":"dashboard"}`))
		},
	}

	r := newTestRouter(t, handlers)
	userID := uuid.New()
	token := generateTestJWT(t, userID, auth.RoleAdministrator)

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/dashboard", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if !dashboardCalled {
		t.Error("admin dashboard handler was not called")
	}
}

func TestNotImplementedHandler(t *testing.T) {
	// Leave all handlers nil — they should return 501.
	r := newTestRouter(t, Handlers{})
	userID := uuid.New()
	token := generateTestJWT(t, userID, auth.RoleMember)

	projectID := uuid.New().String()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Errorf("expected status 501, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestNotImplementedHandlerResponse(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	notImplemented(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Errorf("expected status 501, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Errorf("expected content-type application/json; charset=utf-8, got %q", ct)
	}
}

// newTestRouter creates a chi.Mux with full middleware for testing.
func newTestRouter(t *testing.T, handlers Handlers) http.Handler {
	t.Helper()

	validator := &mockAPIKeyValidator{}
	authMw := auth.NewAuthMiddleware(validator, testJWTSecret)
	rl := auth.NewRateLimiter(100, 200)
	t.Cleanup(rl.Stop)
	metrics := api.NewMetrics()

	cfg := RouterConfig{
		AuthMiddleware: authMw,
		RateLimiter:    rl,
		Metrics:        metrics,
	}

	return NewRouter(cfg, handlers)
}

// Ensure unused import suppression is not needed — verify jwt and model are used.
var _ jwt.Claims
var _ *model.APIKey
