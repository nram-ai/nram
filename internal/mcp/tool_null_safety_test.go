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
	srv := NewServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "memory_recall"
	callReq.Params.Arguments = map[string]interface{}{
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
	srv := NewServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "memory_store"
	// Intentionally omit "tags" to exercise the nil tags path.
	callReq.Params.Arguments = map[string]interface{}{
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
	srv := NewServer(deps)

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

	// The response should be "[]" not "null".
	trimmed := strings.TrimSpace(text)
	if trimmed == "null" {
		t.Error("raw JSON is \"null\"; expected \"[]\"")
	}
	if trimmed != "[]" {
		t.Errorf("expected \"[]\", got %q", trimmed)
	}

	// Structural check.
	var items []projectItem
	if err := json.Unmarshal([]byte(text), &items); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if items == nil {
		t.Error("expected non-nil slice after unmarshal, got nil")
	}
	if len(items) != 0 {
		t.Errorf("expected 0 projects, got %d", len(items))
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
	srv := NewServer(deps)

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

	// The response should be "[]" not "null".
	trimmed := strings.TrimSpace(text)
	if trimmed == "null" {
		t.Error("raw JSON is \"null\"; expected \"[]\"")
	}

	var items []projectItem
	if err := json.Unmarshal([]byte(text), &items); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if items == nil {
		t.Error("expected non-nil slice after unmarshal, got nil")
	}
}

// TestMemoryExportTool_EmptyExport_NoNull verifies that the export tool returns
// empty arrays (not null) for memories, entities, and relationships when the
// project has no data.
func TestMemoryExportTool_EmptyExport_NoNull(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "Empty", Slug: "empty"}

	exportSvc := service.NewExportService(
		&mockExportMemoryReader{memories: []model.Memory{}},
		&mockExportEntityLister{entities: []model.Entity{}},
		&mockExportRelLister{rels: []model.Relationship{}},
		&mockExportLineageReader{},
		&mockExportProjectRepo{project: project},
		nil,
	)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Export:      exportSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "empty",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryExport(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)

	// Raw JSON string checks: no null arrays.
	if strings.Contains(text, `"memories":null`) {
		t.Error("raw JSON contains \"memories\":null; expected \"memories\":[]")
	}
	if strings.Contains(text, `"entities":null`) {
		t.Error("raw JSON contains \"entities\":null; expected \"entities\":[]")
	}
	if strings.Contains(text, `"relationships":null`) {
		t.Error("raw JSON contains \"relationships\":null; expected \"relationships\":[]")
	}

	// Structural check.
	var data service.ExportData
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if data.Memories == nil {
		t.Error("expected non-nil Memories slice, got nil")
	}
	if len(data.Memories) != 0 {
		t.Errorf("expected 0 memories, got %d", len(data.Memories))
	}
	if data.Entities == nil {
		t.Error("expected non-nil Entities slice, got nil")
	}
	if len(data.Entities) != 0 {
		t.Errorf("expected 0 entities, got %d", len(data.Entities))
	}
	if data.Relationships == nil {
		t.Error("expected non-nil Relationships slice, got nil")
	}
	if len(data.Relationships) != 0 {
		t.Errorf("expected 0 relationships, got %d", len(data.Relationships))
	}
	if data.Version != "1.0" {
		t.Errorf("expected version %q, got %q", "1.0", data.Version)
	}
	if data.Stats.MemoryCount != 0 {
		t.Errorf("expected memory count 0, got %d", data.Stats.MemoryCount)
	}
	if data.Stats.EntityCount != 0 {
		t.Errorf("expected entity count 0, got %d", data.Stats.EntityCount)
	}
	if data.Stats.RelationshipCount != 0 {
		t.Errorf("expected relationship count 0, got %d", data.Stats.RelationshipCount)
	}
}

// TestMemoryExportTool_NilRepoResults_NoNull verifies that the export tool
// still produces empty arrays even when the underlying repos return nil slices.
func TestMemoryExportTool_NilRepoResults_NoNull(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "NilData", Slug: "nildata"}

	// Pass nil slices (not empty slices) to exercise nil-to-empty conversion.
	exportSvc := service.NewExportService(
		&mockExportMemoryReader{memories: nil},
		&mockExportEntityLister{entities: nil},
		&mockExportRelLister{rels: nil},
		&mockExportLineageReader{},
		&mockExportProjectRepo{project: project},
		nil,
	)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Export:      exportSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "nildata",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryExport(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)

	// Raw JSON string checks.
	if strings.Contains(text, `"memories":null`) {
		t.Error("raw JSON contains \"memories\":null; expected \"memories\":[]")
	}
	if strings.Contains(text, `"entities":null`) {
		t.Error("raw JSON contains \"entities\":null; expected \"entities\":[]")
	}
	if strings.Contains(text, `"relationships":null`) {
		t.Error("raw JSON contains \"relationships\":null; expected \"relationships\":[]")
	}

	// Structural check.
	var data service.ExportData
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if data.Memories == nil {
		t.Error("expected non-nil Memories slice, got nil")
	}
	if data.Entities == nil {
		t.Error("expected non-nil Entities slice, got nil")
	}
	if data.Relationships == nil {
		t.Error("expected non-nil Relationships slice, got nil")
	}
}
