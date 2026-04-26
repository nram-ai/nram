package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock types for list tests ---

// mockMemoryListerByNs returns different results per namespace.
type mockMemoryListerByNs struct {
	memoriesByNs map[uuid.UUID][]model.Memory
	countByNs    map[uuid.UUID]int
	listErr      error
	countErr     error
}

func (m *mockMemoryListerByNs) supersededInNs(nsID uuid.UUID) int {
	n := 0
	for _, mem := range m.memoriesByNs[nsID] {
		if mem.SupersededBy != nil {
			n++
		}
	}
	return n
}

func (m *mockMemoryListerByNs) ListByNamespaceFiltered(_ context.Context, nsID uuid.UUID, filters storage.MemoryListFilters, _, _ int) ([]model.Memory, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	rows := m.memoriesByNs[nsID]
	if !filters.HideSuperseded {
		return rows, nil
	}
	out := make([]model.Memory, 0, len(rows))
	for _, mem := range rows {
		if mem.SupersededBy != nil {
			continue
		}
		out = append(out, mem)
	}
	return out, nil
}

func (m *mockMemoryListerByNs) CountByNamespaceFiltered(_ context.Context, nsID uuid.UUID, filters storage.MemoryListFilters) (int, error) {
	if m.countErr != nil {
		return 0, m.countErr
	}
	c := m.countByNs[nsID]
	if filters.HideSuperseded {
		c -= m.supersededInNs(nsID)
	}
	return c, nil
}

func (m *mockMemoryListerByNs) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	idSet := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	var out []model.Memory
	for _, rows := range m.memoriesByNs {
		for _, mem := range rows {
			if _, ok := idSet[mem.ID]; ok {
				out = append(out, mem)
			}
		}
	}
	return out, nil
}

// slugProjectRepo returns different projects per slug.
type slugProjectRepo struct {
	projects   map[string]*model.Project
	listResult []model.Project
}

func (m *slugProjectRepo) GetBySlug(_ context.Context, _ uuid.UUID, slug string) (*model.Project, error) {
	if p, ok := m.projects[slug]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("not found")
}

func (m *slugProjectRepo) ListByUser(_ context.Context, _ uuid.UUID) ([]model.Project, error) {
	return m.listResult, nil
}

func (m *slugProjectRepo) Create(_ context.Context, p *model.Project) error {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	return nil
}

func (m *slugProjectRepo) UpdateDescription(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

// --- memory_list schema tests ---

func TestMemoryList_Registered_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_list"]; !ok {
		t.Error("expected memory_list to be registered on SQLite")
	}
}

func TestMemoryList_Registered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_list"]; !ok {
		t.Error("expected memory_list to be registered on Postgres")
	}
}

func TestMemoryList_Schema_HasExpectedFields(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	st, ok := tools["memory_list"]
	if !ok {
		t.Fatal("memory_list not registered")
	}

	schema, err := json.Marshal(st.Tool.InputSchema)
	if err != nil {
		t.Fatalf("failed to marshal schema: %v", err)
	}
	schemaStr := string(schema)

	for _, field := range []string{"project", "limit", "offset"} {
		if !containsField(schemaStr, field) {
			t.Errorf("expected schema to contain field %q", field)
		}
	}
}

// --- memory_list handler tests ---

func TestHandleMemoryList_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	result, err := handleMemoryList(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryList_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	ctx := buildNoAuthCtx()
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryList_ProjectNotFound(t *testing.T) {
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
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project not found")
}

func TestHandleMemoryList_DefaultsToGlobal(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	globalProject := &model.Project{ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"global": globalProject,
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{globalNsID: {}},
			countByNs:    map[uuid.UUID]int{globalNsID: 0},
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("expected 0 memories, got %d", len(resp.Data))
	}
	if resp.Pagination.Limit != listDefaultLimit {
		t.Errorf("expected default limit %d, got %d", listDefaultLimit, resp.Pagination.Limit)
	}
	if resp.Pagination.Offset != 0 {
		t.Errorf("expected default offset 0, got %d", resp.Pagination.Offset)
	}
}

func TestHandleMemoryList_NonGlobalIncludesGlobal(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	now := time.Now().UTC()
	projectMem := model.Memory{
		ID: uuid.New(), Content: "project memory",
		Tags: []string{"p"}, CreatedAt: now, UpdatedAt: now,
	}
	globalMem := model.Memory{
		ID: uuid.New(), Content: "global memory",
		Tags: []string{"g"}, CreatedAt: now, UpdatedAt: now,
	}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"myproj": {ID: uuid.New(), NamespaceID: projectNsID, OwnerNamespaceID: nsID, Slug: "myproj"},
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{
				projectNsID: {projectMem},
				globalNsID:  {globalMem},
			},
			countByNs: map[uuid.UUID]int{
				projectNsID: 1,
				globalNsID:  1,
			},
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "myproj",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 memories (project + global), got %d", len(resp.Data))
	}
	if resp.Pagination.Total != 2 {
		t.Errorf("expected total 2, got %d", resp.Pagination.Total)
	}
	if resp.Data[0].Content != "project memory" {
		t.Errorf("expected first item to be project memory, got %q", resp.Data[0].Content)
	}
	if resp.Data[1].Content != "global memory" {
		t.Errorf("expected second item to be global memory, got %q", resp.Data[1].Content)
	}
}

func TestHandleMemoryList_GlobalProjectDoesNotDoubleInclude(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	now := time.Now().UTC()
	mem := model.Memory{
		ID: uuid.New(), Content: "global only",
		Tags: []string{}, CreatedAt: now, UpdatedAt: now,
	}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{globalNsID: {mem}},
			countByNs:    map[uuid.UUID]int{globalNsID: 1},
		},
	}
	srv := NewServer(deps)

	// Explicitly request "global" — should NOT double-include global.
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "global",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 memory (no double global), got %d", len(resp.Data))
	}
	if resp.Pagination.Total != 1 {
		t.Errorf("expected total 1, got %d", resp.Pagination.Total)
	}
}

func TestHandleMemoryList_WithMemories(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	now := time.Now().UTC()
	src := "test-source"
	mem1 := model.Memory{
		ID: uuid.New(), Content: "first memory",
		Source: &src, Tags: []string{"tag1"},
		CreatedAt: now, UpdatedAt: now,
	}
	mem2 := model.Memory{
		ID: uuid.New(), Content: "second memory",
		Tags: []string{"tag2", "tag3"},
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"test":   {ID: uuid.New(), NamespaceID: projectNsID, OwnerNamespaceID: nsID, Slug: "test"},
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{
				projectNsID: {mem1, mem2},
				globalNsID:  {},
			},
			countByNs: map[uuid.UUID]int{
				projectNsID: 2,
				globalNsID:  0,
			},
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"limit":   float64(10),
		"offset":  float64(0),
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(resp.Data))
	}
	if resp.Data[0].Content != "first memory" {
		t.Errorf("expected first memory content %q, got %q", "first memory", resp.Data[0].Content)
	}
	if resp.Data[0].Source == nil || *resp.Data[0].Source != "test-source" {
		t.Errorf("expected source %q", "test-source")
	}
	if resp.Data[1].Content != "second memory" {
		t.Errorf("expected second memory content %q, got %q", "second memory", resp.Data[1].Content)
	}
	if resp.Pagination.Total != 2 {
		t.Errorf("expected total 2, got %d", resp.Pagination.Total)
	}
	if resp.Pagination.Limit != 10 {
		t.Errorf("expected limit 10, got %d", resp.Pagination.Limit)
	}
}

func TestHandleMemoryList_LimitClamped(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{globalNsID: {}},
			countByNs:    map[uuid.UUID]int{globalNsID: 0},
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"limit": float64(500),
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Pagination.Limit != listMaxLimit {
		t.Errorf("expected limit clamped to %d, got %d", listMaxLimit, resp.Pagination.Limit)
	}
}

func TestHandleMemoryList_CountError(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			countErr: fmt.Errorf("db error"),
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "failed to count memories")
}

func TestHandleMemoryList_ListError(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			countByNs: map[uuid.UUID]int{globalNsID: 5},
			listErr:   fmt.Errorf("db error"),
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "failed to list memories")
}

func TestHandleMemoryList_EmptyList(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{globalNsID: {}},
			countByNs:    map[uuid.UUID]int{globalNsID: 0},
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("expected 0 memories, got %d", len(resp.Data))
	}
	if resp.Pagination.Total != 0 {
		t.Errorf("expected total 0, got %d", resp.Pagination.Total)
	}
}

func TestHandleMemoryList_Pagination(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	globalNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	now := time.Now().UTC()
	mem := model.Memory{
		ID: uuid.New(), Content: "page two memory",
		Tags: []string{}, CreatedAt: now, UpdatedAt: now,
	}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"test":   {ID: uuid.New(), NamespaceID: projectNsID, OwnerNamespaceID: nsID, Slug: "test"},
			"global": {ID: uuid.New(), NamespaceID: globalNsID, OwnerNamespaceID: nsID, Slug: "global"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{
				projectNsID: {mem},
				globalNsID:  {},
			},
			countByNs: map[uuid.UUID]int{
				projectNsID: 20,
				globalNsID:  5,
			},
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"limit":   float64(10),
		"offset":  float64(10),
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Pagination.Total != 25 {
		t.Errorf("expected total 25 (20 + 5), got %d", resp.Pagination.Total)
	}
	if resp.Pagination.Limit != 10 {
		t.Errorf("expected limit 10, got %d", resp.Pagination.Limit)
	}
	if resp.Pagination.Offset != 10 {
		t.Errorf("expected offset 10, got %d", resp.Pagination.Offset)
	}
}

func TestHandleMemoryList_NoGlobalProjectGraceful(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	now := time.Now().UTC()
	mem := model.Memory{
		ID: uuid.New(), Content: "only project",
		Tags: []string{}, CreatedAt: now, UpdatedAt: now,
	}

	// No "global" project exists — should still work with just the project.
	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"myproj": {ID: uuid.New(), NamespaceID: projectNsID, OwnerNamespaceID: nsID, Slug: "myproj"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{projectNsID: {mem}},
			countByNs:    map[uuid.UUID]int{projectNsID: 1},
		},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "myproj",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(resp.Data))
	}
	if resp.Pagination.Total != 1 {
		t.Errorf("expected total 1, got %d", resp.Pagination.Total)
	}
}

func TestHandleMemoryList_HidesSupersededByDefault(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projNsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	now := time.Now().UTC()
	winnerID := uuid.New()
	winner := model.Memory{ID: winnerID, NamespaceID: projNsID, Content: "winner", Tags: []string{}, CreatedAt: now, UpdatedAt: now}
	loser := model.Memory{ID: uuid.New(), NamespaceID: projNsID, Content: "loser", Tags: []string{}, CreatedAt: now, UpdatedAt: now, SupersededBy: &winnerID}

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{user: user},
		ProjectRepo: &slugProjectRepo{projects: map[string]*model.Project{
			"myproj": {ID: uuid.New(), NamespaceID: projNsID, OwnerNamespaceID: nsID, Slug: "myproj"},
		}},
		MemoryLister: &mockMemoryListerByNs{
			memoriesByNs: map[uuid.UUID][]model.Memory{projNsID: {winner, loser}},
			countByNs:    map[uuid.UUID]int{projNsID: 2},
		},
	}
	srv := NewServer(deps)

	// Default — supersede hidden.
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{"project": "myproj"}
	ctx := buildAuthCtx(userID)
	result, err := handleMemoryList(ctx, srv, req)
	if err != nil {
		t.Fatalf("default list: %v", err)
	}
	if result.IsError {
		t.Fatalf("default list errored: %v", result.Content)
	}
	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("decode default: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 (winner only); got %d", len(resp.Data))
	}
	if resp.Data[0].ID != winnerID {
		t.Errorf("expected winner; got %s", resp.Data[0].ID)
	}
	if resp.Pagination.Total != 1 {
		t.Errorf("expected pagination.total=1; got %d", resp.Pagination.Total)
	}

	// include_superseded=true — both surface, total matches row count.
	reqIncl := mcp.CallToolRequest{}
	reqIncl.Params.Arguments = map[string]interface{}{
		"project":            "myproj",
		"include_superseded": true,
	}
	resultIncl, err := handleMemoryList(ctx, srv, reqIncl)
	if err != nil {
		t.Fatalf("include list: %v", err)
	}
	if resultIncl.IsError {
		t.Fatalf("include list errored: %v", resultIncl.Content)
	}
	var respIncl listMemoryResponse
	if err := json.Unmarshal([]byte(extractText(resultIncl)), &respIncl); err != nil {
		t.Fatalf("decode include: %v", err)
	}
	if len(respIncl.Data) != 2 {
		t.Fatalf("include_superseded should return both rows; got %d", len(respIncl.Data))
	}
	if respIncl.Pagination.Total != 2 {
		t.Errorf("expected pagination.total=2 with include; got %d", respIncl.Pagination.Total)
	}
}
