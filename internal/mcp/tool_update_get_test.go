package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

var errNotFound = errors.New("not found")

// --- mock types specific to update/get tests ---

type mockMemoryUpdater struct {
	mem       *model.Memory
	getErr    error
	updateErr error
}

func (m *mockMemoryUpdater) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.mem, nil
}

func (m *mockMemoryUpdater) Update(_ context.Context, mem *model.Memory) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.mem = mem
	return nil
}

type mockLineageCreator struct{}

func (m *mockLineageCreator) Create(_ context.Context, _ *model.MemoryLineage) error {
	return nil
}

type mockMemoryBatchReader struct {
	memories []model.Memory
	err      error
}

func (m *mockMemoryBatchReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for _, mem := range m.memories {
		if mem.ID == id {
			return &mem, nil
		}
	}
	return nil, nil
}

func (m *mockMemoryBatchReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.memories, nil
}

func (m *mockMemoryBatchReader) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return nil, nil
}

// newMockUpdateService creates a mock UpdateService for testing.
func newMockUpdateService(mem *model.Memory) *service.UpdateService {
	nsID := mem.NamespaceID
	projectID := uuid.New()

	return service.NewUpdateService(
		&mockMemoryUpdater{mem: mem},
		&mockProjectLookup{project: &model.Project{ID: projectID, NamespaceID: nsID}},
		&mockLineageCreator{},
		nil, // no vector store
		&mockTokenUsageRepo{},
		nil, // no embed provider
	)
}

// newMockBatchGetService creates a mock BatchGetService for testing.
func newMockBatchGetService(nsID uuid.UUID, memories []model.Memory) *service.BatchGetService {
	projectID := uuid.New()

	return service.NewBatchGetService(
		&mockMemoryBatchReader{memories: memories},
		&mockProjectLookup{project: &model.Project{ID: projectID, NamespaceID: nsID}},
	)
}

// --- memory_update handler tests ---

func TestHandleMemoryUpdate_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      uuid.New().String(),
		"project": "test",
		"content": "new content",
	}

	result, err := handleMemoryUpdate(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryUpdate_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      uuid.New().String(),
		"project": "test",
		"content": "new content",
	}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryUpdate_MissingID(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"content": "new content",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "id is required")
}

func TestHandleMemoryUpdate_InvalidID(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      "not-a-uuid",
		"project": "test",
		"content": "new content",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "invalid memory id")
}

func TestHandleMemoryUpdate_MissingProject(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      uuid.New().String(),
		"content": "new content",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project is required")
}

func TestHandleMemoryUpdate_NoFieldsProvided(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      uuid.New().String(),
		"project": "test",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "at least one of content, tags, or metadata must be provided")
}

func TestHandleMemoryUpdate_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{getErr: errNotFound},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      uuid.New().String(),
		"project": "nonexistent",
		"content": "new content",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project not found")
}

func TestHandleMemoryUpdate_Success(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	memoryID := uuid.New()
	projectID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	mem := &model.Memory{
		ID:          memoryID,
		NamespaceID: nsID,
		Content:     "old content",
		Tags:        []string{"old"},
		Metadata:    json.RawMessage(`{}`),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	updateSvc := newMockUpdateService(mem)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Update:      updateSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":       memoryID.String(),
		"project":  "test",
		"content":  "new content",
		"tags":     []interface{}{"new-tag"},
		"metadata": map[string]interface{}{"key": "value"},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.UpdateResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.ID != memoryID {
		t.Errorf("expected memory ID %s, got %s", memoryID, resp.ID)
	}
	if resp.Content != "new content" {
		t.Errorf("expected content %q, got %q", "new content", resp.Content)
	}
	if resp.PreviousContent != "old content" {
		t.Errorf("expected previous content %q, got %q", "old content", resp.PreviousContent)
	}
}

func TestHandleMemoryUpdate_ReEmbedIndicator(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	memoryID := uuid.New()
	projectID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	mem := &model.Memory{
		ID:          memoryID,
		NamespaceID: nsID,
		Content:     "old content",
		Tags:        []string{},
		Metadata:    json.RawMessage(`{}`),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	// No embed provider, so re_embedded should be false.
	updateSvc := newMockUpdateService(mem)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Update:      updateSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      memoryID.String(),
		"project": "test",
		"content": "changed content",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.UpdateResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	// Without an embed provider, re_embedded should be false.
	if resp.ReEmbedded {
		t.Error("expected re_embedded to be false without embed provider")
	}
}

// --- memory_get handler tests ---

func TestHandleMemoryGet_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"ids":     []interface{}{uuid.New().String()},
		"project": "test",
	}

	result, err := handleMemoryGet(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryGet_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"ids":     []interface{}{uuid.New().String()},
		"project": "test",
	}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryGet_MissingIDs(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "ids is required")
}

func TestHandleMemoryGet_EmptyIDs(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"ids":     []interface{}{},
		"project": "test",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "ids is required")
}

func TestHandleMemoryGet_InvalidUUID(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"ids":     []interface{}{"not-a-uuid"},
		"project": "test",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "not a valid UUID")
}

func TestHandleMemoryGet_MissingProject(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"ids": []interface{}{uuid.New().String()},
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project is required")
}

func TestHandleMemoryGet_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{getErr: errNotFound},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"ids":     []interface{}{uuid.New().String()},
		"project": "nonexistent",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project not found")
}

func TestHandleMemoryGet_SuccessWithFoundAndNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	memID1 := uuid.New()
	memID2 := uuid.New() // This one won't be in the store.

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	now := time.Now()
	memories := []model.Memory{
		{
			ID:          memID1,
			NamespaceID: nsID,
			Content:     "found memory",
			Tags:        []string{"tag1"},
			Metadata:    json.RawMessage(`{"k":"v"}`),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	batchGetSvc := newMockBatchGetService(nsID, memories)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		BatchGet:    batchGetSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_get"
	req.Params.Arguments = map[string]interface{}{
		"ids":     []interface{}{memID1.String(), memID2.String()},
		"project": "test",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.BatchGetResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if len(resp.Found) != 1 {
		t.Fatalf("expected 1 found memory, got %d", len(resp.Found))
	}
	if resp.Found[0].ID != memID1 {
		t.Errorf("expected found memory ID %s, got %s", memID1, resp.Found[0].ID)
	}
	if resp.Found[0].Content != "found memory" {
		t.Errorf("expected content %q, got %q", "found memory", resp.Found[0].Content)
	}
	if len(resp.NotFound) != 1 {
		t.Fatalf("expected 1 not_found ID, got %d", len(resp.NotFound))
	}
	if resp.NotFound[0] != memID2 {
		t.Errorf("expected not_found ID %s, got %s", memID2, resp.NotFound[0])
	}
}

// --- schema tests ---

func TestMemoryUpdate_SchemaRegistered(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_update"]; !ok {
		t.Fatal("memory_update tool not registered")
	}
}

func TestMemoryGet_SchemaRegistered(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_get"]; !ok {
		t.Fatal("memory_get tool not registered")
	}
}
