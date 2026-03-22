package mcp

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock types for graph/projects/export tests ---

type mockEntityReader struct {
	entities []model.Entity
	err      error
}

func (m *mockEntityReader) FindBySimilarity(_ context.Context, _ uuid.UUID, _ string, _ string, _ int) ([]model.Entity, error) {
	return m.entities, m.err
}

func (m *mockEntityReader) FindByAlias(_ context.Context, _ uuid.UUID, _ string) ([]model.Entity, error) {
	return m.entities, m.err
}

func (m *mockEntityReader) ListByNamespace(_ context.Context, _ uuid.UUID) ([]model.Entity, error) {
	return m.entities, m.err
}

type mockTraverser struct {
	rels []model.Relationship
	err  error
}

func (m *mockTraverser) TraverseFromEntity(_ context.Context, _ uuid.UUID, _ int) ([]model.Relationship, error) {
	return m.rels, m.err
}

type mockExportMemoryReader struct {
	memories []model.Memory
}

func (m *mockExportMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for _, mem := range m.memories {
		if mem.ID == id {
			return &mem, nil
		}
	}
	return nil, nil
}

func (m *mockExportMemoryReader) GetBatch(_ context.Context, _ []uuid.UUID) ([]model.Memory, error) {
	return m.memories, nil
}

func (m *mockExportMemoryReader) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return m.memories, nil
}

type mockExportEntityLister struct {
	entities []model.Entity
}

func (m *mockExportEntityLister) ListByNamespace(_ context.Context, _ uuid.UUID) ([]model.Entity, error) {
	return m.entities, nil
}

type mockExportRelLister struct {
	rels []model.Relationship
}

func (m *mockExportRelLister) ListByEntity(_ context.Context, _ uuid.UUID) ([]model.Relationship, error) {
	return m.rels, nil
}

type mockExportLineageReader struct{}

func (m *mockExportLineageReader) ListByMemory(_ context.Context, _ uuid.UUID) ([]model.MemoryLineage, error) {
	return nil, nil
}

type mockExportProjectRepo struct {
	project *model.Project
}

func (m *mockExportProjectRepo) GetByID(_ context.Context, _ uuid.UUID) (*model.Project, error) {
	if m.project != nil {
		return m.project, nil
	}
	return nil, io.ErrUnexpectedEOF
}

// --- memory_graph schema tests ---

func TestMemoryGraph_Registered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_graph"]; !ok {
		t.Error("expected memory_graph to be registered on Postgres")
	}
}

// --- memory_projects schema tests ---

func TestMemoryProjects_Registered_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_projects"]; !ok {
		t.Error("expected memory_projects to be registered on SQLite")
	}
}

func TestMemoryProjects_Registered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_projects"]; !ok {
		t.Error("expected memory_projects to be registered on Postgres")
	}
}

// --- memory_export schema tests ---

func TestMemoryExport_Registered_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_export"]; !ok {
		t.Error("expected memory_export to be registered on SQLite")
	}
}

func TestMemoryExport_Registered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_export"]; !ok {
		t.Error("expected memory_export to be registered on Postgres")
	}
}

// --- memory_graph handler tests ---

func TestHandleMemoryGraph_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{"entity": "test"}

	result, err := handleMemoryGraph(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryGraph_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{"entity": "test"}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryGraph_MissingEntity(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "entity is required")
}

func TestHandleMemoryGraph_EntitySearch(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	entityID := uuid.New()
	relID := uuid.New()
	targetID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	entities := []model.Entity{
		{ID: entityID, NamespaceID: nsID, Name: "Alice", EntityType: "person", Canonical: "alice"},
	}
	rels := []model.Relationship{
		{
			ID:       relID,
			SourceID: entityID,
			TargetID: targetID,
			Relation: "knows",
			Weight:   1.0,
			ValidFrom: time.Now(),
		},
	}

	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{getErr: nil, project: nil},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    &mockTraverser{rels: rels},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"entity": "Alice",
		"depth":  float64(3),
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp graphResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Query != "Alice" {
		t.Errorf("expected query %q, got %q", "Alice", resp.Query)
	}
	if resp.Depth != 3 {
		t.Errorf("expected depth 3, got %d", resp.Depth)
	}
	if len(resp.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(resp.Entities))
	}
	if len(resp.Relationships) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(resp.Relationships))
	}
}

func TestHandleMemoryGraph_DefaultDepth(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		EntityReader: &mockEntityReader{entities: nil},
		Traverser:    &mockTraverser{},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"entity": "something",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp graphResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Depth != 2 {
		t.Errorf("expected default depth 2, got %d", resp.Depth)
	}
}

// --- memory_projects handler tests ---

func TestHandleMemoryProjects_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	result, err := handleMemoryProjects(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryProjects_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	ctx := buildNoAuthCtx()
	result, err := handleMemoryProjects(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryProjects_ListSuccess(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	p1ID := uuid.New()
	p2ID := uuid.New()
	projects := []model.Project{
		{ID: p1ID, Name: "Project One", Slug: "project-one", Description: "First project"},
		{ID: p2ID, Name: "Project Two", Slug: "project-two", Description: "Second project"},
	}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{
			listResult: projects,
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
	var items []projectItem
	if err := json.Unmarshal([]byte(text), &items); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(items))
	}
	if items[0].Slug != "project-one" {
		t.Errorf("expected slug %q, got %q", "project-one", items[0].Slug)
	}
	if items[1].Description != "Second project" {
		t.Errorf("expected description %q, got %q", "Second project", items[1].Description)
	}
}

func TestHandleMemoryProjects_EmptyList(t *testing.T) {
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
	var items []projectItem
	if err := json.Unmarshal([]byte(text), &items); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected 0 projects, got %d", len(items))
	}
}

// --- memory_export handler tests ---

func TestHandleMemoryExport_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{"project": "test"}

	result, err := handleMemoryExport(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryExport_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{"project": "test"}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryExport(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryExport_InvalidFormat(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"format":  "csv",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryExport(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "unsupported format")
}

func TestHandleMemoryExport_JSONSuccess(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "Test", Slug: "test"}

	exportSvc := service.NewExportService(
		&mockExportMemoryReader{memories: []model.Memory{}},
		&mockExportEntityLister{entities: []model.Entity{}},
		&mockExportRelLister{rels: []model.Relationship{}},
		&mockExportLineageReader{},
		&mockExportProjectRepo{project: project},
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
		"project": "test",
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
	var data service.ExportData
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if data.Version != "1.0" {
		t.Errorf("expected version %q, got %q", "1.0", data.Version)
	}
	if data.Project.Slug != "test" {
		t.Errorf("expected project slug %q, got %q", "test", data.Project.Slug)
	}
}

func TestHandleMemoryExport_NDJSONSuccess(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "Test", Slug: "test"}

	exportSvc := service.NewExportService(
		&mockExportMemoryReader{memories: []model.Memory{}},
		&mockExportEntityLister{entities: []model.Entity{}},
		&mockExportRelLister{rels: []model.Relationship{}},
		&mockExportLineageReader{},
		&mockExportProjectRepo{project: project},
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
		"project": "test",
		"format":  "ndjson",
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
	if text == "" {
		t.Error("expected non-empty NDJSON output")
	}

	// First line should be a project record.
	lines := splitNDJSON(text)
	if len(lines) == 0 {
		t.Fatal("expected at least one NDJSON line")
	}

	var first map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("failed to unmarshal first NDJSON line: %v", err)
	}
	if first["type"] != "project" {
		t.Errorf("expected first line type %q, got %q", "project", first["type"])
	}
}

func TestHandleMemoryExport_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{getErr: io.ErrUnexpectedEOF},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "nonexistent",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryExport(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project not found")
}

// splitNDJSON splits NDJSON text into individual lines, skipping empty lines.
func splitNDJSON(text string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			line := text[start:i]
			if len(line) > 0 {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	if start < len(text) {
		line := text[start:]
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}
	return lines
}
