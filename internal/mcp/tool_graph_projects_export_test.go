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

func (m *mockEntityReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Entity, error) {
	if m.err != nil {
		return nil, m.err
	}
	want := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	out := make([]model.Entity, 0, len(ids))
	for _, e := range m.entities {
		if _, ok := want[e.ID]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

type mockTraverser struct {
	rels []model.Relationship
	err  error
	// lastDepth records the depth argument from the most recent call so tests
	// can verify the handler propagates default and explicit depths correctly.
	lastDepth int
}

func (m *mockTraverser) TraverseFromEntity(_ context.Context, _ uuid.UUID, depth int) ([]model.Relationship, error) {
	m.lastDepth = depth
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

func (m *mockExportMemoryReader) ListByNamespaceFiltered(_ context.Context, _ uuid.UUID, filters storage.MemoryListFilters, _, _ int) ([]model.Memory, error) {
	if !filters.HideSuperseded {
		return m.memories, nil
	}
	out := make([]model.Memory, 0, len(m.memories))
	for _, mem := range m.memories {
		if mem.SupersededBy != nil {
			continue
		}
		out = append(out, mem)
	}
	return out, nil
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

func (m *mockExportLineageReader) ListByMemory(_ context.Context, _ uuid.UUID, _ uuid.UUID) ([]model.MemoryLineage, error) {
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

func (m *mockExportProjectRepo) GetByNamespaceID(_ context.Context, namespaceID uuid.UUID) (*model.Project, error) {
	if m.project != nil && m.project.NamespaceID == namespaceID {
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
		// Target lives in the same namespace so orphan resolution can fold it
		// into entities[] when the relationship references it.
		{ID: targetID, NamespaceID: nsID, Name: "Bob", EntityType: "person", Canonical: "bob"},
	}
	rels := []model.Relationship{
		{
			ID:        relID,
			SourceID:  entityID,
			TargetID:  targetID,
			Relation:  "knows",
			Weight:    1.0,
			ValidFrom: time.Now(),
		},
	}

	traverser := &mockTraverser{rels: rels}
	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{getErr: nil, project: nil},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    traverser,
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

	if traverser.lastDepth != 3 {
		t.Errorf("expected depth 3 propagated to traverser, got %d", traverser.lastDepth)
	}

	text := extractText(result)
	var resp graphResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	// Anchor entity (Alice) plus the orphan-resolved target (Bob).
	if len(resp.Entities) != 2 {
		t.Errorf("expected 2 entities (anchor + resolved target), got %d", len(resp.Entities))
	}
	if len(resp.Relationships) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(resp.Relationships))
	}
	assertNoOrphanRelationships(t, resp)
}

func TestHandleMemoryGraph_DefaultDepth(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	entityID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	// One anchor entity so traversal actually runs (without it, the handler
	// short-circuits before invoking the traverser).
	entities := []model.Entity{
		{ID: entityID, NamespaceID: nsID, Name: "something", EntityType: "concept", Canonical: "something"},
	}
	traverser := &mockTraverser{}

	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    traverser,
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
	if traverser.lastDepth != 2 {
		t.Errorf("expected default depth 2 propagated to traverser, got %d", traverser.lastDepth)
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

func TestHandleMemoryGraph_FiltersSupersededSourceMemory(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	// Two memories — one alive, one superseded — used as source memories for
	// two relationships. The superseded one's relationship is dropped by
	// default but reappears with include_superseded=true.
	winnerID := uuid.New()
	loserID := uuid.New()
	winnerMemSrc := winnerID
	loserMemSrc := loserID
	now := time.Now().UTC()
	memories := map[uuid.UUID][]model.Memory{
		nsID: {
			{ID: winnerID, NamespaceID: nsID, Content: "winner", Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: loserID, NamespaceID: nsID, Content: "loser", Tags: []string{}, CreatedAt: now, UpdatedAt: now, SupersededBy: &winnerID},
		},
	}

	entityID := uuid.New()
	aliveTargetID := uuid.New()
	supersededTargetID := uuid.New()
	relAliveID := uuid.New()
	relSupersededID := uuid.New()
	entities := []model.Entity{
		{ID: entityID, NamespaceID: nsID, Name: "Alice", EntityType: "person", Canonical: "alice"},
		// Target entities live in the same namespace so orphan resolution can
		// fold them into entities[] instead of pruning the relationships.
		{ID: aliveTargetID, NamespaceID: nsID, Name: "Bob", EntityType: "person", Canonical: "bob"},
		{ID: supersededTargetID, NamespaceID: nsID, Name: "Carol", EntityType: "person", Canonical: "carol"},
	}
	rels := []model.Relationship{
		{ID: relAliveID, SourceID: entityID, TargetID: aliveTargetID, Relation: "knows", Weight: 1, ValidFrom: now, SourceMemory: &winnerMemSrc},
		{ID: relSupersededID, SourceID: entityID, TargetID: supersededTargetID, Relation: "knows", Weight: 1, ValidFrom: now, SourceMemory: &loserMemSrc},
	}

	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{project: project},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    &mockTraverser{rels: rels},
		MemoryLister: &mockMemoryListerByNs{memoriesByNs: memories},
	}
	srv := NewServer(deps)

	// Default: superseded relationship dropped.
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{"entity": "Alice"}
	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("default graph: %v", err)
	}
	if result.IsError {
		t.Fatalf("default graph errored: %v", result.Content)
	}
	var resp graphResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("decode default: %v", err)
	}
	if len(resp.Relationships) != 1 {
		t.Fatalf("expected only the live relationship; got %d", len(resp.Relationships))
	}
	// Identity comes from source_memory now that per-edge IDs are dropped.
	if resp.Relationships[0].SourceMemory == nil || *resp.Relationships[0].SourceMemory != winnerID {
		t.Errorf("expected source_memory=%s on surviving rel; got %+v", winnerID, resp.Relationships[0].SourceMemory)
	}
	assertNoOrphanRelationships(t, resp)

	// include_superseded=true: both relationships present.
	reqIncl := mcp.CallToolRequest{}
	reqIncl.Params.Arguments = map[string]interface{}{
		"entity":             "Alice",
		"include_superseded": true,
	}
	resultIncl, err := handleMemoryGraph(ctx, srv, reqIncl)
	if err != nil {
		t.Fatalf("include graph: %v", err)
	}
	if resultIncl.IsError {
		t.Fatalf("include graph errored: %v", resultIncl.Content)
	}
	var respIncl graphResponse
	if err := json.Unmarshal([]byte(extractText(resultIncl)), &respIncl); err != nil {
		t.Fatalf("decode include: %v", err)
	}
	if len(respIncl.Relationships) != 2 {
		t.Fatalf("expected both relationships with include_superseded; got %d", len(respIncl.Relationships))
	}
}

func TestHandleMemoryExport_HidesSupersededByDefault(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	winnerID := uuid.New()
	loserID := uuid.New()
	now := time.Now().UTC()
	mems := []model.Memory{
		{ID: winnerID, NamespaceID: nsID, Content: "winner", Tags: []string{}, CreatedAt: now, UpdatedAt: now},
		{ID: loserID, NamespaceID: nsID, Content: "loser", Tags: []string{}, CreatedAt: now, UpdatedAt: now, SupersededBy: &winnerID},
	}

	exportSvc := service.NewExportService(
		&mockExportMemoryReader{memories: mems},
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
	req.Params.Arguments = map[string]interface{}{"project": "test"}
	ctx := buildAuthCtx(userID)
	result, err := handleMemoryExport(ctx, srv, req)
	if err != nil {
		t.Fatalf("default export: %v", err)
	}
	if result.IsError {
		t.Fatalf("default export errored: %v", result.Content)
	}
	var data service.ExportData
	if err := json.Unmarshal([]byte(extractText(result)), &data); err != nil {
		t.Fatalf("decode default: %v", err)
	}
	if len(data.Memories) != 1 || data.Memories[0].ID != winnerID {
		t.Fatalf("expected only winner in default export; got %+v", data.Memories)
	}

	reqIncl := mcp.CallToolRequest{}
	reqIncl.Params.Arguments = map[string]interface{}{
		"project":            "test",
		"include_superseded": true,
	}
	resultIncl, err := handleMemoryExport(ctx, srv, reqIncl)
	if err != nil {
		t.Fatalf("include export: %v", err)
	}
	if resultIncl.IsError {
		t.Fatalf("include export errored: %v", resultIncl.Content)
	}
	var dataIncl service.ExportData
	if err := json.Unmarshal([]byte(extractText(resultIncl)), &dataIncl); err != nil {
		t.Fatalf("decode include: %v", err)
	}
	if len(dataIncl.Memories) != 2 {
		t.Fatalf("expected both rows with include_superseded; got %d", len(dataIncl.Memories))
	}
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
