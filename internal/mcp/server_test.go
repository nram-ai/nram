package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

func TestNewServer_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
}

// TestNewServerPanicsOnNilMetrics pins the construction-time enforcement.
// Production wiring drift (e.g. cmd/server/main.go forgetting to populate
// Dependencies.Metrics) must fail at startup, not silently in production.
// The MCP wrappers depend on a non-nil recorder; recordTier has no nil-guard
// since the panic invariant makes it unreachable.
func TestNewServerPanicsOnNilMetrics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil Metrics; got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value is %T, want string", r)
		}
		if !strings.Contains(msg, "Dependencies.Metrics is required") {
			t.Errorf("panic message %q must name the required field", msg)
		}
	}()
	_ = NewServer(Dependencies{Metrics: nil})
}

func TestHandler_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	h := srv.Handler()
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestBackend_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	if got := srv.Backend(); got != storage.BackendSQLite {
		t.Fatalf("expected backend %q, got %q", storage.BackendSQLite, got)
	}
}

func TestBackend_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)
	if got := srv.Backend(); got != storage.BackendPostgres {
		t.Fatalf("expected backend %q, got %q", storage.BackendPostgres, got)
	}
}

func TestMCPServer_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	if srv.MCPServer() == nil {
		t.Fatal("expected non-nil MCPServer")
	}
}

func TestDeps_ReturnsSameBackend(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)
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
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	// No Origin header; should pass through to the MCP handler.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// The MCP SDK may return various codes for an uninitialized POST, but
	// it MUST NOT be 403 (that's the Origin rejection code).
	if rec.Code == http.StatusForbidden {
		t.Fatal("request without Origin should not be rejected with 403")
	}
}

func TestOriginValidation_MatchingOrigin_Allowed(t *testing.T) {
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
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
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
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
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
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

func TestBuildInstructions_UnderSizeLimit(t *testing.T) {
	// Claude Code truncates server instructions at 2048 characters.
	// The full variant (both providers) is the longest and must fit.
	const maxChars = 2048

	full := buildInstructions(true, true)
	if len(full) > maxChars {
		t.Errorf("full instructions are %d chars, must be under %d (over by %d)",
			len(full), maxChars, len(full)-maxChars)
	}
}

func TestToolDescriptions_UnderSizeLimit(t *testing.T) {
	// Claude Desktop truncates individual tool descriptions at 2048 bytes.
	// Test both backends since Postgres registers additional tools.
	const maxDescBytes = 2048

	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		deps := Dependencies{Backend: backend}
		srv := newTestServer(deps)
		tools := srv.MCPServer().ListTools()

		for name, st := range tools {
			desc := st.Tool.Description
			if len(desc) > maxDescBytes {
				t.Errorf("[%s] tool %q description is %d bytes, must be under %d",
					backend, name, len(desc), maxDescBytes)
			}
		}
	}
}

// TestEveryToolHasOutputSchema pins the wire contract that every MCP tool
// nram exposes advertises an outputSchema with type=object. Clients (Claude,
// Cursor, MCP Inspector) key off outputSchema to reason about tool results
// without parsing opaque JSON text. Per the MCP spec
// (modelcontextprotocol.io/specification/2025-06-18/server/tools), the
// top-level type MUST be "object".
//
// `export` is exempt because it has two output shapes (json structured,
// ndjson text) that a single schema cannot honestly describe; the tool
// advertises no outputSchema and consumers branch on the requested format.
//
// The expected tool-name set is pinned explicitly so a future registration
// regression (e.g. a Register* call removed, or a tool gated on a non-nil
// dep that this test does not provide) fails loudly instead of silently
// shrinking coverage.
func TestEveryToolHasOutputSchema(t *testing.T) {
	expected := []string{
		"list", "store", "store_batch", "recall", "forget",
		"update", "get", "graph", "list_projects",
		"delete_project", "update_project",
		"procedural_fetch", "procedural_store", "procedural_update", "procedural_forget",
		"about_me",
	}
	exempt := map[string]bool{} // export was the sole exemption; tool withdrawn 2026-05-27

	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		// delete_project is gated on a non-nil ProjectDelete service
		// (RegisterProjectDeleteTool early-returns otherwise). Inject a
		// non-nil sentinel so the test exercises the fully-configured
		// surface; the handler is never invoked in this test.
		deps := Dependencies{
			Backend:       backend,
			ProjectDelete: &service.ProjectDeleteService{},
		}
		srv := newTestServer(deps)
		tools := srv.MCPServer().ListTools()

		// Pin the exact registered set so a silent drop or rename surfaces.
		for _, name := range expected {
			if _, ok := tools[name]; !ok {
				t.Errorf("[%s] expected tool %q to be registered", backend, name)
			}
		}
		if len(tools) != len(expected) {
			got := make([]string, 0, len(tools))
			for n := range tools {
				got = append(got, n)
			}
			t.Errorf("[%s] tool-name set drifted: want %v, got %v", backend, expected, got)
		}

		for name, st := range tools {
			if exempt[name] {
				continue
			}
			schema := st.Tool.OutputSchema
			rawSchemaPresent := len(st.Tool.RawOutputSchema) > 0
			if !rawSchemaPresent && schema.Type == "" && schema.Properties == nil {
				t.Errorf("[%s] tool %q has no outputSchema", backend, name)
				continue
			}
			if rawSchemaPresent {
				var parsed struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal(st.Tool.RawOutputSchema, &parsed); err != nil {
					t.Errorf("[%s] tool %q outputSchema is not valid JSON: %v", backend, name, err)
					continue
				}
				if parsed.Type != "object" {
					t.Errorf("[%s] tool %q outputSchema.type = %q, want \"object\"",
						backend, name, parsed.Type)
				}
				continue
			}
			if schema.Type != "object" {
				t.Errorf("[%s] tool %q outputSchema.type = %q, want \"object\"",
					backend, name, schema.Type)
			}
		}
	}
}

func TestBuildInstructions_AllVariants(t *testing.T) {
	tests := []struct {
		name           string
		hasEmbedding   bool
		hasEnrichment  bool
		mustContain    []string
		mustNotContain []string
	}{
		{
			name:          "both providers",
			hasEmbedding:  true,
			hasEnrichment: true,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"graph",
				"ALWAYS query first",
				"recall",
				"list",
				"Enrichment is fully server-managed",
			},
			mustNotContain: []string{
				"No embedding provider",
				"no embedding provider",
				"specific tags",
				"enrich: true",
			},
		},
		{
			name:          "embedding only",
			hasEmbedding:  true,
			hasEnrichment: false,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"recall",
				"list",
				"semantic search",
			},
			mustNotContain: []string{
				"graph",
				"enrich: true",
			},
		},
		{
			name:          "enrichment only",
			hasEmbedding:  false,
			hasEnrichment: true,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"graph",
				"ALWAYS query first",
				"recall",
				"Enrichment is fully server-managed",
				"specific tags",
			},
			mustNotContain: []string{
				"semantic search",
			},
		},
		{
			name:          "neither provider",
			hasEmbedding:  false,
			hasEnrichment: false,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"recall",
				"list",
				"specific tags",
			},
			mustNotContain: []string{
				"graph",
				"enrich: true",
				"semantic search",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildInstructions(tt.hasEmbedding, tt.hasEnrichment)
			for _, s := range tt.mustContain {
				if !strings.Contains(result, s) {
					t.Errorf("expected instructions to contain %q", s)
				}
			}
			for _, s := range tt.mustNotContain {
				if strings.Contains(result, s) {
					t.Errorf("expected instructions NOT to contain %q", s)
				}
			}
			// The procedural session-start guidance is provider-independent and
			// must appear in every variant.
			if !strings.Contains(result, "procedural_fetch") {
				t.Errorf("expected instructions to contain procedural_fetch session-start guidance")
			}
			if !strings.Contains(result, "SESSION START") {
				t.Errorf("expected instructions to contain SESSION START guidance")
			}
			if !strings.Contains(result, "your first action MUST be to call procedural_fetch") {
				t.Errorf("expected instructions to contain the blocking session-start directive")
			}
		})
	}
}

func TestBuildInstructions_RetrievalPrecedence(t *testing.T) {
	// With enrichment: graph must come before recall, recall before list.
	full := buildInstructions(true, true)
	graphIdx := strings.Index(full, "graph")
	recallIdx := strings.Index(full, "recall")
	listIdx := strings.Index(full, "list")

	if graphIdx < 0 || recallIdx < 0 || listIdx < 0 {
		t.Fatal("expected all three retrieval tools to be mentioned")
	}
	if graphIdx >= recallIdx {
		t.Error("graph must appear before recall in retrieval order")
	}
	if recallIdx >= listIdx {
		t.Error("recall must appear before list in retrieval order")
	}
}

func TestBuildInstructions_BehavioralTriggers(t *testing.T) {
	full := buildInstructions(true, true)
	triggers := []string{
		"preference",
		"decision",
		"each task start",
		"before storing",
		"avoid duplicates",
		"store immediately",
	}
	for _, trigger := range triggers {
		if !strings.Contains(full, trigger) {
			t.Errorf("expected behavioral trigger %q in instructions", trigger)
		}
	}
}
