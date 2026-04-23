package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock types for recall tool tests ---

type mockRecallService struct {
	resp *service.RecallResponse
	err  error
	req  *service.RecallRequest // captures the last request
}

func (m *mockRecallService) Recall(_ context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
	m.req = req
	return m.resp, m.err
}

// newMockRecallSvc creates a mock RecallService that returns a fixed response.
// Since RecallService is a concrete type with unexported fields, we create a
// real instance with minimal mocks for the recall path.
func newMockRecallSvc() *service.RecallService {
	nsID := uuid.New()
	return service.NewRecallService(
		&mockMemoryReaderRecall{},
		&mockProjectLookup{project: &model.Project{ID: uuid.New(), NamespaceID: nsID}},
		&mockNamespaceLookup{ns: &model.Namespace{ID: nsID}},
		&mockTokenUsageRepo{},
		nil, // no vector search
		nil, // no entity reader
		nil, // no traverser
		nil, // no shares
		nil, // no embed provider
	)
}

type mockMemoryReaderRecall struct{}

func (m *mockMemoryReaderRecall) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	return &model.Memory{ID: id}, nil
}

func (m *mockMemoryReaderRecall) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	var mems []model.Memory
	for _, id := range ids {
		mems = append(mems, model.Memory{ID: id})
	}
	return mems, nil
}

func (m *mockMemoryReaderRecall) ListByNamespace(_ context.Context, _ uuid.UUID, _ int, _ int) ([]model.Memory, error) {
	return []model.Memory{}, nil
}

// --- schema tests ---

func TestMemoryRecall_Schema_Postgres_HasGraphParams(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	st, ok := tools["memory_recall"]
	if !ok {
		t.Fatal("memory_recall tool not registered")
	}

	raw, _ := json.Marshal(st.Tool.InputSchema)
	schema := string(raw)

	if !containsField(schema, "include_graph") {
		t.Error("expected include_graph param to be present on Postgres backend")
	}
	if !containsField(schema, "graph_depth") {
		t.Error("expected graph_depth param to be present on Postgres backend")
	}
}

func TestMemoryRecall_Schema_HasDiversifyByTagPrefix(t *testing.T) {
	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		deps := Dependencies{Backend: backend}
		srv := NewServer(deps)
		tools := srv.MCPServer().ListTools()
		st, ok := tools["memory_recall"]
		if !ok {
			t.Fatalf("backend %s: memory_recall tool not registered", backend)
		}
		raw, _ := json.Marshal(st.Tool.InputSchema)
		if !containsField(string(raw), "diversify_by_tag_prefix") {
			t.Errorf("backend %s: expected diversify_by_tag_prefix param in schema, got %s", backend, string(raw))
		}
	}
}

// --- handler tests ---

func TestHandleMemoryRecall_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_recall"
	req.Params.Arguments = map[string]interface{}{
		"query": "test query",
	}

	result, err := handleMemoryRecall(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryRecall_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_recall"
	req.Params.Arguments = map[string]interface{}{
		"query": "test query",
	}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryRecall(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryRecall_MissingQuery(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_recall"
	req.Params.Arguments = map[string]interface{}{}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryRecall(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "query is required")
}

func TestHandleMemoryRecall_ProjectScoped(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "myproj"}

	recallSvc := newMockRecallSvc()

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: &model.Namespace{ID: nsID, Path: "/user"}},
		Recall:        recallSvc,
	}
	srv := NewServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "memory_recall"
	callReq.Params.Arguments = map[string]interface{}{
		"query":   "find something",
		"project": "myproj",
		"limit":   float64(5),
		"tags":    []interface{}{"important"},
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
	var resp service.RecallResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Memories == nil {
		t.Error("expected non-nil memories array")
	}
}

func TestHandleMemoryRecall_UserScoped(t *testing.T) {
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
		"query": "search everything",
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
	var resp service.RecallResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
}

func TestHandleMemoryRecall_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{getErr: fmt.Errorf("not found")},
		NamespaceRepo: &mockNamespaceRepoStore{ns: &model.Namespace{ID: nsID, Path: "/user"}},
		Recall:        newMockRecallSvc(),
	}
	srv := NewServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "memory_recall"
	callReq.Params.Arguments = map[string]interface{}{
		"query":   "search",
		"project": "nonexistent",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryRecall(ctx, srv, callReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project not found")
}

