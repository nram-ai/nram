package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// TestMemoryRecallTool_EmptyResults_NoNull verifies that the recall tool returns
// JSON with "memories":[] and "entities":[] (not null) when there are no matches.
func TestMemoryRecallTool_EmptyResults_NoNull(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}

	recallSvc := newMockRecallSvc()

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{},
		NamespaceRepo: &mockNamespaceRepoStore{ns: &model.Namespace{ID: nsID, Path: "/user"}},
		Recall:        recallSvc,
	}
	srv := newTestServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "recall"
	callReq.Params.Arguments = map[string]any{
		"query": "nonexistent topic that matches nothing",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryRecall(ctx, srv, callReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)

	// Raw JSON string check: no null values where arrays should be.
	if strings.Contains(text, `"memories":null`) {
		t.Error("raw JSON contains \"memories\":null; expected \"memories\":[]")
	}
	if strings.Contains(text, `"entities":null`) {
		t.Error("raw JSON contains \"entities\":null; expected \"entities\":[]")
	}

	// Structural check: unmarshal and verify fields are non-nil empty slices.
	var resp service.RecallResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Memories == nil {
		t.Error("expected non-nil Memories slice, got nil")
	}
	if len(resp.Memories) != 0 {
		t.Errorf("expected 0 memories, got %d", len(resp.Memories))
	}
	if resp.Graph.Entities == nil {
		t.Error("expected non-nil Graph.Entities slice, got nil")
	}
	if len(resp.Graph.Entities) != 0 {
		t.Errorf("expected 0 graph entities, got %d", len(resp.Graph.Entities))
	}
}

// TestMemoryStoreTool_NilTags_NoTagsField confirms that when no tags are
// supplied the slim MCP store response does not surface a tags field at all
// (it was dropped — caller already has the input). The legacy guarantee
// (no `tags:null`) carries over by virtue of the field being absent.
func TestMemoryStoreTool_NilTags_NoTagsField(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	memoryID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	storeSvc := newMockStoreService(memoryID, projectID, "test")

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: &model.Namespace{ID: nsID, Path: "/user"}},
		Store:         storeSvc,
	}
	srv := newTestServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "store"
	// Intentionally omit "tags" to exercise the nil tags path.
	callReq.Params.Arguments = map[string]any{
		"project": "test",
		"content": "hello world with no tags",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStore(ctx, srv, callReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)

	// Raw JSON: no tags field at all in the slim MCP store response.
	if strings.Contains(text, `"tags"`) {
		t.Errorf("expected slim MCP store response to omit tags, got %s", text)
	}

	// Structural check on the slim response: id + project_slug present.
	var resp mcpStoreResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.ID == uuid.Nil {
		t.Error("expected non-nil id in store response")
	}
	if resp.ProjectSlug == "" {
		t.Error("expected non-empty project_slug in store response")
	}
}

// TestMemoryProjectsTool_EmptyList_ReturnsEmptyArray verifies that the projects
// tool returns [] (not null) when the user has no projects.
func TestMemoryProjectsTool_EmptyList_ReturnsEmptyArray(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{
			listResult: []model.Project{},
		},
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	ctx := buildAuthCtx(userID)
	result, err := handleMemoryProjects(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)

	// Pin the canonical empty form including pagination metadata. Any extra
	// field or null-valued optional shifts the exact match and fails the
	// check immediately.
	want := `{"projects":[],"pagination":{"total":0,"limit":50,"offset":0}}`
	if strings.TrimSpace(text) != want {
		t.Errorf("expected exact %q, got %q", want, text)
	}

	// Structural check.
	var resp listProjectsResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Projects == nil {
		t.Error("expected non-nil Projects slice after unmarshal, got nil")
	}
	if len(resp.Projects) != 0 {
		t.Errorf("expected 0 projects, got %d", len(resp.Projects))
	}
}

// TestMemoryProjectsTool_NilListResult_ReturnsEmptyArray verifies that even when
// the underlying repo returns a nil slice (not an empty slice), the tool still
// returns [] in JSON.
func TestMemoryProjectsTool_NilListResult_ReturnsEmptyArray(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{
			listResult: nil, // explicitly nil
		},
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	ctx := buildAuthCtx(userID)
	result, err := handleMemoryProjects(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)

	// The Projects field must be a non-null array even when the underlying repo
	// returned nil.
	if strings.Contains(text, `"projects":null`) {
		t.Errorf("projects field is null; expected []")
	}

	var resp listProjectsResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Projects == nil {
		t.Error("expected non-nil Projects slice after unmarshal, got nil")
	}
}

// TestMemoryExportTool_* removed 2026-05-27 along with the MCP export tool.
// The nil-to-empty conversion contract on service.ExportData is still
// exercised by internal/service/export_test.go and the REST handler tests
// in internal/api/handler_export_import_test.go.
