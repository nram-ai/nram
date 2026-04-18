package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock types for store tool tests ---

type mockUserRepoStore struct {
	user *model.User
	err  error
}

func (m *mockUserRepoStore) GetByID(_ context.Context, _ uuid.UUID) (*model.User, error) {
	return m.user, m.err
}

type mockProjectRepoStore struct {
	project     *model.Project
	getErr      error
	created     *model.Project
	createErr   error
	listResult  []model.Project
	listErr     error
}

func (m *mockProjectRepoStore) GetBySlug(_ context.Context, _ uuid.UUID, _ string) (*model.Project, error) {
	return m.project, m.getErr
}

func (m *mockProjectRepoStore) ListByUser(_ context.Context, _ uuid.UUID) ([]model.Project, error) {
	return m.listResult, m.listErr
}

func (m *mockProjectRepoStore) Create(_ context.Context, p *model.Project) error {
	if m.createErr != nil {
		return m.createErr
	}
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	m.created = p
	return nil
}

func (m *mockProjectRepoStore) UpdateDescription(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

type mockNamespaceRepoStore struct {
	ns        *model.Namespace
	getErr    error
	createErr error
}

func (m *mockNamespaceRepoStore) GetByID(_ context.Context, _ uuid.UUID) (*model.Namespace, error) {
	return m.ns, m.getErr
}

func (m *mockNamespaceRepoStore) Create(_ context.Context, _ *model.Namespace) error {
	return m.createErr
}

// buildAuthCtx creates a context with an HTTP request carrying auth info.
func buildAuthCtx(userID uuid.UUID) context.Context {
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	ac := &auth.AuthContext{UserID: userID}
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	return context.WithValue(context.Background(), httpRequestKey, req)
}

// buildNoAuthCtx creates a context with an HTTP request but no auth.
func buildNoAuthCtx() context.Context {
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	return context.WithValue(context.Background(), httpRequestKey, req)
}

// --- schema tests ---

func TestMemoryStore_Schema_Postgres_HasEnrich(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	st, ok := tools["memory_store"]
	if !ok {
		t.Fatal("memory_store tool not registered")
	}

	raw, _ := json.Marshal(st.Tool.InputSchema)
	schema := string(raw)
	if !containsField(schema, "enrich") {
		t.Error("expected enrich param to be present on Postgres backend")
	}
}

func TestMemoryStoreBatch_Schema_Postgres_HasEnrich(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	st, ok := tools["memory_store_batch"]
	if !ok {
		t.Fatal("memory_store_batch tool not registered")
	}

	raw, _ := json.Marshal(st.Tool.InputSchema)
	schema := string(raw)
	if !containsField(schema, "enrich") {
		t.Error("expected enrich param to be present on Postgres backend")
	}
}

// --- handler tests ---

func TestHandleMemoryStore_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"content": "hello",
	}

	result, err := handleMemoryStore(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryStore_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"content": "hello",
	}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryStore_MissingContent(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "content is required")
}

func TestHandleMemoryStore_UserNotFound(t *testing.T) {
	userID := uuid.New()

	deps := Dependencies{
		Backend:  storage.BackendSQLite,
		UserRepo: &mockUserRepoStore{err: errors.New("user not found")},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"content": "hello",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "failed to resolve project")
}

func TestHandleMemoryStore_ExistingProject(t *testing.T) {
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
	callReq.Params.Arguments = map[string]interface{}{
		"project":  "test",
		"content":  "hello world",
		"source":   "test-source",
		"tags":     []interface{}{"tag1", "tag2"},
		"metadata": map[string]interface{}{"key": "value"},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStore(ctx, srv, callReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	// Verify response is valid JSON with expected fields.
	text := extractText(result)
	var resp service.StoreResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.ID == uuid.Nil {
		t.Error("expected non-nil memory ID")
	}
	if resp.ProjectSlug != "test" {
		t.Errorf("expected project slug %q, got %q", "test", resp.ProjectSlug)
	}
}

func TestHandleMemoryStore_AutoCreateProject(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	memoryID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	ns := &model.Namespace{ID: nsID, Path: "/user", Depth: 1}

	// Project not found triggers auto-create
	projectRepo := &mockProjectRepoStore{getErr: errors.New("not found")}

	storeSvc := newMockStoreService(memoryID, uuid.Nil, "new-project")

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   projectRepo,
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
		Store:         storeSvc,
	}
	srv := NewServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "memory_store"
	callReq.Params.Arguments = map[string]interface{}{
		"project": "new-project",
		"content": "hello",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStore(ctx, srv, callReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	// Verify project was created.
	if projectRepo.created == nil {
		t.Fatal("expected project to be auto-created")
	}
	if projectRepo.created.Slug != "new-project" {
		t.Errorf("expected slug %q, got %q", "new-project", projectRepo.created.Slug)
	}
}

// --- batch handler tests ---

func TestHandleMemoryStoreBatch_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"items":   []interface{}{map[string]interface{}{"content": "a"}},
	}

	result, err := handleMemoryStoreBatch(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryStoreBatch_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"items":   []interface{}{map[string]interface{}{"content": "a"}},
	}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryStoreBatch(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryStoreBatch_EmptyItems(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"items":   []interface{}{},
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryStoreBatch(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "items is required and must be a non-empty array")
}

func TestHandleMemoryStoreBatch_ItemMissingContent(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: &model.Namespace{ID: nsID, Path: "/user"}},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"items":   []interface{}{map[string]interface{}{"source": "no-content"}},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStoreBatch(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "content is required")
}

func TestHandleMemoryStoreBatch_Success(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	batchSvc := newMockBatchStoreService(2)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: &model.Namespace{ID: nsID, Path: "/user"}},
		BatchStore:    batchSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"items": []interface{}{
			map[string]interface{}{"content": "memory 1"},
			map[string]interface{}{"content": "memory 2", "tags": []interface{}{"t1"}},
		},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStoreBatch(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.BatchStoreResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.MemoriesCreated != 2 {
		t.Errorf("expected 2 memories created, got %d", resp.MemoriesCreated)
	}
}

// --- helper extraction tests ---

func TestExtractStringSlice(t *testing.T) {
	tests := []struct {
		name string
		in   interface{}
		want int
	}{
		{"nil", nil, 0},
		{"empty", []interface{}{}, 0},
		{"strings", []interface{}{"a", "b"}, 2},
		{"mixed", []interface{}{"a", 42}, 1},
		{"not_slice", "hello", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractStringSlice(tt.in)
			if tt.want == 0 && got != nil && len(got) != 0 {
				t.Errorf("expected nil or empty, got %v", got)
			} else if tt.want > 0 && len(got) != tt.want {
				t.Errorf("expected %d items, got %d", tt.want, len(got))
			}
		})
	}
}

func TestExtractRawJSON(t *testing.T) {
	result := extractRawJSON(map[string]interface{}{"key": "val"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	var m map[string]string
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if m["key"] != "val" {
		t.Errorf("expected key=val, got %v", m)
	}

	if extractRawJSON(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestExtractBatchItems_Valid(t *testing.T) {
	raw := []interface{}{
		map[string]interface{}{
			"content": "hello",
			"source":  "src",
			"tags":    []interface{}{"t1"},
		},
		map[string]interface{}{
			"content": "world",
		},
	}

	items, err := extractBatchItems(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].Content != "hello" {
		t.Errorf("expected content 'hello', got %q", items[0].Content)
	}
	if items[0].Source != "src" {
		t.Errorf("expected source 'src', got %q", items[0].Source)
	}
	if len(items[0].Tags) != 1 || items[0].Tags[0] != "t1" {
		t.Errorf("expected tags [t1], got %v", items[0].Tags)
	}
}

func TestExtractBatchItems_MissingContent(t *testing.T) {
	raw := []interface{}{
		map[string]interface{}{"source": "only-source"},
	}
	_, err := extractBatchItems(raw)
	if err == nil {
		t.Fatal("expected error for missing content")
	}
}

func TestExtractBatchItems_InvalidType(t *testing.T) {
	raw := []interface{}{"not-a-map"}
	_, err := extractBatchItems(raw)
	if err == nil {
		t.Fatal("expected error for non-object item")
	}
}

// --- test helpers ---

func containsField(schema string, field string) bool {
	var s map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &s); err != nil {
		return false
	}
	props, ok := s["properties"].(map[string]interface{})
	if !ok {
		return false
	}
	_, exists := props[field]
	return exists
}

func assertToolError(t *testing.T, result *mcp.CallToolResult, substring string) {
	t.Helper()
	if !result.IsError {
		t.Fatal("expected tool error result")
	}
	text := extractText(result)
	if text == "" || !contains(text, substring) {
		t.Errorf("expected error containing %q, got %q", substring, text)
	}
}

func extractText(result *mcp.CallToolResult) string {
	for _, c := range result.Content {
		if tc, ok := c.(mcp.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstring(s, sub))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// newMockStoreService creates a mock *service.StoreService. Since StoreService
// is a concrete type with unexported fields, we need a real instance with
// mock repositories that will return our desired results.
func newMockStoreService(memID, projectID uuid.UUID, slug string) *service.StoreService {
	return service.NewStoreService(
		&mockMemoryRepo{id: memID},
		&mockProjectLookup{project: &model.Project{ID: projectID, Slug: slug, NamespaceID: uuid.New()}},
		&mockNamespaceLookup{ns: &model.Namespace{ID: uuid.New()}},
		&mockIngestionLogRepo{},
		&mockEnrichmentQueueRepo{},
	)
}

func newMockBatchStoreService(memoriesCreated int) *service.BatchStoreService {
	nsID := uuid.New()
	return service.NewBatchStoreService(
		&mockMemoryRepo{id: uuid.New()},
		&mockProjectLookup{project: &model.Project{ID: uuid.New(), Slug: "test", NamespaceID: nsID}},
		&mockNamespaceLookup{ns: &model.Namespace{ID: nsID}},
		&mockIngestionLogRepo{},
		&mockEnrichmentQueueRepo{},
	)
}

type mockMemoryRepo struct {
	id uuid.UUID
}

func (m *mockMemoryRepo) Create(_ context.Context, mem *model.Memory) error {
	if m.id != uuid.Nil {
		mem.ID = m.id
	}
	return nil
}

func (m *mockMemoryRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	return &model.Memory{ID: id}, nil
}

type mockProjectLookup struct {
	project *model.Project
}

func (m *mockProjectLookup) GetByID(_ context.Context, _ uuid.UUID) (*model.Project, error) {
	return m.project, nil
}

type mockNamespaceLookup struct {
	ns *model.Namespace
}

func (m *mockNamespaceLookup) GetByID(_ context.Context, _ uuid.UUID) (*model.Namespace, error) {
	return m.ns, nil
}

type mockIngestionLogRepo struct{}

func (m *mockIngestionLogRepo) Create(_ context.Context, _ *model.IngestionLog) error {
	return nil
}

type mockTokenUsageRepo struct{}

func (m *mockTokenUsageRepo) Record(_ context.Context, _ *model.TokenUsage) error {
	return nil
}

type mockEnrichmentQueueRepo struct{}

func (m *mockEnrichmentQueueRepo) Enqueue(_ context.Context, _ *model.EnrichmentJob) error {
	return nil
}
