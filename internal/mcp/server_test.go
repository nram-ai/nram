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
