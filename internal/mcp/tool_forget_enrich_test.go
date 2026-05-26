package mcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock types for forget tests ---

type mockMemoryDeleter struct {
	memories map[uuid.UUID]*model.Memory
}

func (m *mockMemoryDeleter) SoftDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	delete(m.memories, id)
	return nil
}

func (m *mockMemoryDeleter) HardDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	delete(m.memories, id)
	return nil
}

func (m *mockMemoryDeleter) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, errNotFound
	}
	return mem, nil
}

func (m *mockMemoryDeleter) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return nil, nil
}

func (m *mockMemoryDeleter) FindBySupersededBy(_ context.Context, _ uuid.UUID, id uuid.UUID) ([]uuid.UUID, error) {
	var out []uuid.UUID
	for ancestorID, mem := range m.memories {
		if mem.SupersededBy != nil && *mem.SupersededBy == id && mem.DeletedAt == nil {
			out = append(out, ancestorID)
		}
	}
	return out, nil
}

func newMockForgetService(nsID uuid.UUID, memories map[uuid.UUID]*model.Memory) *service.ForgetService {
	projectID := uuid.New()
	return service.NewForgetService(
		&mockMemoryDeleter{memories: memories},
		&mockProjectLookup{project: &model.Project{ID: projectID, NamespaceID: nsID}},
		nil, nil,
	)
}

// --- memory_forget schema tests ---

func TestMemoryForget_SchemaRegistered_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["forget"]; !ok {
		t.Fatal("memory_forget tool not registered on SQLite backend")
	}
}

func TestMemoryForget_SchemaRegistered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["forget"]; !ok {
		t.Fatal("memory_forget tool not registered on Postgres backend")
	}
}

// --- memory_forget handler tests ---

func TestHandleMemoryForget_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{uuid.New().String()},
	}

	result, err := handleMemoryForget(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryForget_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{uuid.New().String()},
	}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryForget_MissingIDs(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "ids is required")
}

func TestHandleMemoryForget_EmptyIDs(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{},
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "ids is required")
}

func TestHandleMemoryForget_InvalidUUID(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{"not-a-uuid"},
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "not a valid UUID")
}

func TestHandleMemoryForget_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{getErr: errNotFound},
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "nonexistent",
		"ids":     []interface{}{uuid.New().String()},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project not found")
}

func TestHandleMemoryForget_Success_SoftDelete(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	memID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	memories := map[uuid.UUID]*model.Memory{
		memID: {
			ID:          memID,
			NamespaceID: nsID,
			Content:     "to delete",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		},
	}

	forgetSvc := newMockForgetService(nsID, memories)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Forget:      forgetSvc,
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{memID.String()},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.ForgetResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", resp.Deleted)
	}
}

func TestHandleMemoryForget_Success_HardDelete(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	memID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	memories := map[uuid.UUID]*model.Memory{
		memID: {
			ID:          memID,
			NamespaceID: nsID,
			Content:     "to hard delete",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		},
	}

	forgetSvc := newMockForgetService(nsID, memories)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Forget:      forgetSvc,
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{memID.String()},
		"hard":    true,
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.ForgetResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", resp.Deleted)
	}
}
