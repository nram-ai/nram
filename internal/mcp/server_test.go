package mcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nram-ai/nram/internal/storage"
)

func TestNewServer_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestHandler_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)
	h := srv.Handler()
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestBackend_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)
	if got := srv.Backend(); got != storage.BackendSQLite {
		t.Fatalf("expected backend %q, got %q", storage.BackendSQLite, got)
	}
}

func TestBackend_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)
	if got := srv.Backend(); got != storage.BackendPostgres {
		t.Fatalf("expected backend %q, got %q", storage.BackendPostgres, got)
	}
}

func TestMCPServer_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)
	if srv.MCPServer() == nil {
		t.Fatal("expected non-nil MCPServer")
	}
}

func TestDeps_ReturnsSameBackend(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)
	if got := srv.Deps().Backend; got != storage.BackendPostgres {
		t.Fatalf("expected deps backend %q, got %q", storage.BackendPostgres, got)
	}
}

func TestHTTPRequestFromContext_Present(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	ctx := context.WithValue(context.Background(), httpRequestKey, req)
	got := HTTPRequestFromContext(ctx)
	if got != req {
		t.Fatal("expected to retrieve the stored request")
	}
}

func TestHTTPRequestFromContext_Absent(t *testing.T) {
	got := HTTPRequestFromContext(context.Background())
	if got != nil {
		t.Fatal("expected nil when no request in context")
	}
}

// --- Origin validation tests (MCP spec security requirement) ---

func TestOriginValidation_NoOrigin_Allowed(t *testing.T) {
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	// No Origin header — should pass through to the MCP handler.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// The MCP SDK may return various codes for an uninitialized POST, but
	// it MUST NOT be 403 (that's the Origin rejection code).
	if rec.Code == http.StatusForbidden {
		t.Fatal("request without Origin should not be rejected with 403")
	}
}

func TestOriginValidation_MatchingOrigin_Allowed(t *testing.T) {
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Host = "localhost:8674"
	req.Header.Set("Origin", "http://localhost:8674")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusForbidden {
		t.Fatalf("matching Origin should not be rejected; got 403: %s", rec.Body.String())
	}
}

func TestOriginValidation_MismatchedOrigin_Rejected(t *testing.T) {
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Host = "localhost:8674"
	req.Header.Set("Origin", "http://evil.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("mismatched Origin should be rejected with 403; got %d", rec.Code)
	}
}

func TestOriginValidation_HTTPSOrigin_Matches(t *testing.T) {
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Host = "nram.example.com"
	req.Header.Set("Origin", "https://nram.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusForbidden {
		t.Fatalf("https Origin matching Host should not be rejected; got 403")
	}
}

func TestIsAllowedOrigin(t *testing.T) {
	tests := []struct {
		origin string
		host   string
		want   bool
	}{
		{"http://localhost:8674", "localhost:8674", true},
		{"https://nram.example.com", "nram.example.com", true},
		{"http://evil.com", "localhost:8674", false},
		{"https://evil.com:8674", "localhost:8674", false},
		{"http://localhost:9999", "localhost:8674", false},
		{"http://localhost:8674", "localhost:8674", true},
	}
	for _, tt := range tests {
		got := isAllowedOrigin(tt.origin, tt.host)
		if got != tt.want {
			t.Errorf("isAllowedOrigin(%q, %q) = %v, want %v", tt.origin, tt.host, got, tt.want)
		}
	}
}
