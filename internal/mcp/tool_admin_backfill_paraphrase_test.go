package mcp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// buildAdminCtx wraps buildAuthCtx with administrator role so the
// admin-gated tool handler proceeds past its role check.
func buildAdminCtx(userID uuid.UUID) context.Context {
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	ac := &auth.AuthContext{UserID: userID, Role: auth.RoleAdministrator}
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	return context.WithValue(context.Background(), httpRequestKey, req)
}

// buildNonAdminCtx is the same shape but with a non-admin role.
func buildNonAdminCtx(userID uuid.UUID) context.Context {
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	ac := &auth.AuthContext{UserID: userID, Role: auth.RoleMember}
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	return context.WithValue(context.Background(), httpRequestKey, req)
}

// errProjectNotFound is the canned project-lookup error used by the
// project-not-found path test below.
var errProjectNotFound = errors.New("project not found")

func TestMemoryBackfillExtractedFactParaphrase_Registered(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_backfill_extracted_fact_paraphrase"]; !ok {
		t.Fatal("memory_backfill_extracted_fact_paraphrase tool not registered")
	}
}

func TestHandleAdminBackfillParaphrase_NoHTTPRequest(t *testing.T) {
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite})

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_backfill_extracted_fact_paraphrase"
	req.Params.Arguments = map[string]interface{}{}

	result, err := handleAdminBackfillParaphrase(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleAdminBackfillParaphrase_NoAuth(t *testing.T) {
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite})

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_backfill_extracted_fact_paraphrase"
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildNoAuthCtx()
	result, err := handleAdminBackfillParaphrase(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleAdminBackfillParaphrase_NonAdmin_Forbidden(t *testing.T) {
	// A non-administrator caller must be rejected before any service
	// lookup. Without this gate any authenticated user could trigger a
	// deployment-wide sweep.
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite})

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_backfill_extracted_fact_paraphrase"
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildNonAdminCtx(uuid.New())
	result, err := handleAdminBackfillParaphrase(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "administrator required")
}

func TestHandleAdminBackfillParaphrase_NoService_Configured(t *testing.T) {
	// Deployment without the EnrichService wired must report a clear
	// "not configured" error rather than nil-deref.
	srv := NewServer(Dependencies{Backend: storage.BackendSQLite}) // Enrich is nil

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_backfill_extracted_fact_paraphrase"
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAdminCtx(uuid.New())
	result, err := handleAdminBackfillParaphrase(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "backfill service not configured")
}

// stubParaphraseLister returns a fixed candidate list for MCP-level tests
// where we want to verify the tool round-trips through EnrichService into
// the candidate-listing path. Independent of the service package's
// stubParaphraseLister so the MCP test file remains self-contained.
type mcpStubParaphraseLister struct {
	ids []uuid.UUID
}

func (m *mcpStubParaphraseLister) ListEnrichedParentsWithExtractedChildren(_ context.Context, _ []uuid.UUID, _ int) ([]uuid.UUID, error) {
	return m.ids, nil
}

func newEnrichSvcWithParaphraseLister(nsID uuid.UUID, candidates []uuid.UUID) *service.EnrichService {
	memMap := make([]model.Memory, 0, len(candidates))
	for _, id := range candidates {
		memMap = append(memMap, model.Memory{ID: id, NamespaceID: nsID, Enriched: true})
	}
	svc := newMockEnrichService(nsID, memMap)
	svc.AttachParaphraseCandidateLister(&mcpStubParaphraseLister{ids: candidates})
	return svc
}

func TestHandleAdminBackfillParaphrase_DryRun_Counts(t *testing.T) {
	nsID := uuid.New()
	cand := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	enrichSvc := newEnrichSvcWithParaphraseLister(nsID, cand)

	srv := NewServer(Dependencies{
		Backend: storage.BackendSQLite,
		Enrich:  enrichSvc,
	})

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_backfill_extracted_fact_paraphrase"
	req.Params.Arguments = map[string]interface{}{"dry_run": true}

	ctx := buildAdminCtx(uuid.New())
	result, err := handleAdminBackfillParaphrase(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got tool error: %+v", result)
	}
	text := extractText(result)
	if !strings.Contains(text, "candidate_count=3") {
		t.Errorf("expected candidate_count=3 in result, got %q", text)
	}
	if !strings.Contains(text, "enqueued=0") {
		t.Errorf("dry_run should not enqueue; got %q", text)
	}
	if !strings.Contains(text, "dry_run=true") {
		t.Errorf("dry_run flag not echoed; got %q", text)
	}
}

func TestHandleAdminBackfillParaphrase_Execute_Enqueues(t *testing.T) {
	nsID := uuid.New()
	cand := []uuid.UUID{uuid.New(), uuid.New()}
	enrichSvc := newEnrichSvcWithParaphraseLister(nsID, cand)

	srv := NewServer(Dependencies{
		Backend: storage.BackendSQLite,
		Enrich:  enrichSvc,
	})

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_backfill_extracted_fact_paraphrase"
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAdminCtx(uuid.New())
	result, err := handleAdminBackfillParaphrase(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got tool error: %+v", result)
	}
	text := extractText(result)
	if !strings.Contains(text, "candidate_count=2") || !strings.Contains(text, "enqueued=2") {
		t.Errorf("expected candidate_count=2 enqueued=2, got %q", text)
	}
}

func TestHandleAdminBackfillParaphrase_ProjectNotFound(t *testing.T) {
	// When a project slug is supplied but the project does not exist (or the
	// user has no access), the tool must surface a clear "project not found"
	// error rather than silently sweeping the wrong namespace.
	srv := NewServer(Dependencies{
		Backend: storage.BackendSQLite,
		Enrich:  newMockEnrichService(uuid.New(), nil),
		UserRepo: &mockUserRepoStore{
			user: &model.User{ID: uuid.New(), NamespaceID: uuid.New()},
		},
		ProjectRepo: &mockProjectRepoStore{getErr: errProjectNotFound},
	})

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_backfill_extracted_fact_paraphrase"
	req.Params.Arguments = map[string]interface{}{"project": "nonexistent"}

	ctx := buildAdminCtx(uuid.New())
	result, err := handleAdminBackfillParaphrase(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "not found")
}
