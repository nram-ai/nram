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
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/observability/metrics"
)

// mockAPIKeyValidator implements auth.APIKeyValidator for testing.
type mockAPIKeyValidator struct{}

func (m *mockAPIKeyValidator) Validate(_ context.Context, _ string) (*model.APIKey, error) {
	return nil, fmt.Errorf("invalid key")
}

// mockUserIdentityLookup implements auth.UserIdentityLookup for testing.
// It always returns "member" as the role.
type mockUserIdentityLookup struct{}

func (m *mockUserIdentityLookup) GetIdentityByID(_ context.Context, _ uuid.UUID) (string, uuid.UUID, error) {
	return "member", uuid.Nil, nil
}

var testJWTSecret = []byte("test-secret-key-for-router-tests")

func generateTestJWT(t *testing.T, userID uuid.UUID, role string) string {
	t.Helper()
	token, err := auth.GenerateJWT(userID, uuid.Nil, role, testJWTSecret, 1*time.Hour)
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
			_, _ = w.Write([]byte(`{"status":"ok"}`))
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

// TestMoveRoutesDispatch proves the two move routes registered under the same
// memories subrouter resolve to distinct handlers without colliding: the
// single-segment POST .../memories/move hits BulkMove, while the two-segment
// POST .../memories/{id}/move hits Move.
func TestMoveRoutesDispatch(t *testing.T) {
	moveCalled, bulkMoveCalled := false, false
	handlers := Handlers{
		Move: func(w http.ResponseWriter, _ *http.Request) {
			moveCalled = true
			w.WriteHeader(http.StatusOK)
		},
		BulkMove: func(w http.ResponseWriter, _ *http.Request) {
			bulkMoveCalled = true
			w.WriteHeader(http.StatusOK)
		},
	}

	r := newTestRouter(t, handlers)
	token := generateTestJWT(t, uuid.New(), auth.RoleMember)
	projectID := uuid.New().String()

	// Single move: two segments after /memories.
	memoryID := uuid.New().String()
	reqSingle := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories/"+memoryID+"/move", nil)
	reqSingle.Header.Set("Authorization", "Bearer "+token)
	recSingle := httptest.NewRecorder()
	r.ServeHTTP(recSingle, reqSingle)
	if recSingle.Code != http.StatusOK || !moveCalled {
		t.Errorf("single move route did not dispatch to Move (code=%d, called=%v)", recSingle.Code, moveCalled)
	}
	if bulkMoveCalled {
		t.Error("single move route incorrectly hit BulkMove")
	}

	// Bulk move: one segment after /memories.
	bulkMoveCalled = false
	reqBulk := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID+"/memories/move", nil)
	reqBulk.Header.Set("Authorization", "Bearer "+token)
	recBulk := httptest.NewRecorder()
	r.ServeHTTP(recBulk, reqBulk)
	if recBulk.Code != http.StatusOK || !bulkMoveCalled {
		t.Errorf("bulk move route did not dispatch to BulkMove (code=%d, called=%v)", recBulk.Code, bulkMoveCalled)
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
			_, _ = w.Write([]byte(`{"data":"dashboard"}`))
		},
	}

	r := newTestRouter(t, handlers)
	userID := uuid.New()
	token := generateTestJWT(t, userID, auth.RoleAdministrator)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
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
	// Leave all handlers nil; they should return 501.
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
	authMw := auth.NewAuthMiddleware(validator, &mockUserIdentityLookup{}, testJWTSecret, nil)
	rl := auth.NewRateLimiter(100, 200, 0, 0)
	t.Cleanup(rl.Stop)
	m := metrics.New()

	cfg := RouterConfig{
		AuthMiddleware: authMw,
		RateLimiter:    rl,
		Metrics:        m,
	}

	return NewRouter(cfg, handlers)
}

// Ensure unused import suppression is not needed: verify jwt and model are used.
var _ jwt.Claims
var _ *model.APIKey
