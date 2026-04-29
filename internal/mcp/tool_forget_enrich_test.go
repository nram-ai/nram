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

// --- mock types for forget/enrich tests ---

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

type mockEnrichMemoryReader struct {
	memories []model.Memory
}

func (m *mockEnrichMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for _, mem := range m.memories {
		if mem.ID == id {
			return &mem, nil
		}
	}
	return nil, errNotFound
}

func (m *mockEnrichMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	var result []model.Memory
	idSet := make(map[uuid.UUID]bool)
	for _, id := range ids {
		idSet[id] = true
	}
	for _, mem := range m.memories {
		if idSet[mem.ID] {
			result = append(result, mem)
		}
	}
	return result, nil
}

func (m *mockEnrichMemoryReader) ListByNamespaceFiltered(_ context.Context, nsID uuid.UUID, filters storage.MemoryListFilters, limit, _ int) ([]model.Memory, error) {
	mems := m.memories
	if filters.HideSuperseded {
		filtered := make([]model.Memory, 0, len(mems))
		for _, mem := range mems {
			if mem.SupersededBy != nil {
				continue
			}
			filtered = append(filtered, mem)
		}
		mems = filtered
	}
	if limit > len(mems) {
		return mems, nil
	}
	return mems[:limit], nil
}

func (m *mockEnrichMemoryReader) ListByNamespace(_ context.Context, nsID uuid.UUID, limit, _ int) ([]model.Memory, error) {
	var result []model.Memory
	for _, mem := range m.memories {
		if mem.NamespaceID == nsID {
			result = append(result, mem)
		}
	}
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func newMockForgetService(nsID uuid.UUID, memories map[uuid.UUID]*model.Memory) *service.ForgetService {
	projectID := uuid.New()
	return service.NewForgetService(
		&mockMemoryDeleter{memories: memories},
		&mockProjectLookup{project: &model.Project{ID: projectID, NamespaceID: nsID}},
		nil, nil,
	)
}

type mockLineageQuerier struct{}

func (m *mockLineageQuerier) FindParentIDs(_ context.Context, _ uuid.UUID, _ []uuid.UUID) (map[uuid.UUID]uuid.UUID, error) {
	return nil, nil
}

func (m *mockLineageQuerier) FindChildIDsByRelation(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ []string) ([]uuid.UUID, error) {
	return nil, nil
}

func newMockEnrichService(nsID uuid.UUID, memories []model.Memory) *service.EnrichService {
	projectID := uuid.New()
	return service.NewEnrichService(
		&mockEnrichMemoryReader{memories: memories},
		&mockProjectLookup{project: &model.Project{ID: projectID, NamespaceID: nsID}},
		&mockEnrichmentQueueRepo{},
		&mockLineageQuerier{},
	)
}

// --- memory_forget schema tests ---

func TestMemoryForget_SchemaRegistered_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_forget"]; !ok {
		t.Fatal("memory_forget tool not registered on SQLite backend")
	}
}

func TestMemoryForget_SchemaRegistered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_forget"]; !ok {
		t.Fatal("memory_forget tool not registered on Postgres backend")
	}
}

// --- memory_enrich schema tests ---

func TestMemoryEnrich_Registered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_enrich"]; !ok {
		t.Fatal("memory_enrich tool not registered on Postgres backend")
	}
}

// TestHandleMemoryEnrich_GateClosed verifies the enrichment-available gate
// short-circuits the handler with a clear "enrichment_unavailable" tool
// error before any project resolution happens. The gate is evaluated live
// each call so a provider reload reopens the surface without restart.
func TestHandleMemoryEnrich_GateClosed(t *testing.T) {
	deps := Dependencies{
		Backend:             storage.BackendSQLite,
		EnrichmentAvailable: func() bool { return false },
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{"project": "test"}

	// Use an authenticated context so we know the gate runs before the
	// auth check (matches the handler's defensive ordering).
	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "enrichment_unavailable")
}

// TestHandleMemoryEnrich_GateOpenStillRequiresAuth verifies the gate does
// not bypass downstream auth/project checks — closing the gate is the only
// effect of a missing provider, not a free pass.
func TestHandleMemoryEnrich_GateOpenStillRequiresAuth(t *testing.T) {
	deps := Dependencies{
		Backend:             storage.BackendSQLite,
		EnrichmentAvailable: func() bool { return true },
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{"project": "test"}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

// TestHandleMemoryStore_EnrichTrueGateClosed verifies opting in to async
// enrichment via memory_store(enrich: true) is rejected with the same
// gate-closed error. Calls without enrich:true are not gated here — that
// path queues a job either way today and the worker drops it onto pending
// when the gate is closed (see internal/enrichment.TestProcessJob_NoProviders).
func TestHandleMemoryStore_EnrichTrueGateClosed(t *testing.T) {
	deps := Dependencies{
		Backend:             storage.BackendSQLite,
		EnrichmentAvailable: func() bool { return false },
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store"
	req.Params.Arguments = map[string]interface{}{
		"content": "test content",
		"enrich":  true,
	}
	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "enrichment_unavailable")
}

// TestHandleMemoryStoreBatch_EnrichTrueGateClosed mirrors the single-store
// case for the batch tool.
func TestHandleMemoryStoreBatch_EnrichTrueGateClosed(t *testing.T) {
	deps := Dependencies{
		Backend:             storage.BackendSQLite,
		EnrichmentAvailable: func() bool { return false },
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"items":  []interface{}{map[string]interface{}{"content": "a"}},
		"enrich": true,
	}
	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryStoreBatch(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "enrichment_unavailable")
}

// --- memory_forget handler tests ---

func TestHandleMemoryForget_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
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

// --- memory_enrich handler tests ---

func TestHandleMemoryEnrich_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
	}

	result, err := handleMemoryEnrich(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryEnrich_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
	}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryEnrich_ProjectNotFound(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	deps := Dependencies{
		Backend:     storage.BackendPostgres,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{getErr: errNotFound},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{
		"project": "nonexistent",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "project not found")
}

func TestHandleMemoryEnrich_Success_AllUnEnriched(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	now := time.Now()
	memories := []model.Memory{
		{ID: uuid.New(), NamespaceID: nsID, Content: "mem1", Enriched: false, CreatedAt: now, UpdatedAt: now},
		{ID: uuid.New(), NamespaceID: nsID, Content: "mem2", Enriched: true, CreatedAt: now, UpdatedAt: now},
	}

	enrichSvc := newMockEnrichService(nsID, memories)

	deps := Dependencies{
		Backend:     storage.BackendPostgres,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Enrich:      enrichSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.EnrichResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Queued != 1 {
		t.Errorf("expected 1 queued, got %d", resp.Queued)
	}
	if resp.Skipped != 1 {
		t.Errorf("expected 1 skipped, got %d", resp.Skipped)
	}
}

func TestHandleMemoryEnrich_Success_SpecificIDs(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()
	memID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	now := time.Now()
	memories := []model.Memory{
		{ID: memID, NamespaceID: nsID, Content: "target", Enriched: false, CreatedAt: now, UpdatedAt: now},
	}

	enrichSvc := newMockEnrichService(nsID, memories)

	deps := Dependencies{
		Backend:     storage.BackendPostgres,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Enrich:      enrichSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{memID.String()},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	text := extractText(result)
	var resp service.EnrichResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Queued != 1 {
		t.Errorf("expected 1 queued, got %d", resp.Queued)
	}
}

func TestHandleMemoryEnrich_InvalidUUID(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	deps := Dependencies{
		Backend:     storage.BackendPostgres,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{
		"project": "test",
		"ids":     []interface{}{"bad-uuid"},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "not a valid UUID")
}

func TestHandleMemoryEnrich_SkipsSupersededByDefault(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: projectID, NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

	now := time.Now()
	winnerID := uuid.New()
	loserID := uuid.New()
	memories := []model.Memory{
		{ID: winnerID, NamespaceID: nsID, Content: "winner", Enriched: false, CreatedAt: now, UpdatedAt: now},
		{ID: loserID, NamespaceID: nsID, Content: "loser", Enriched: false, CreatedAt: now, UpdatedAt: now, SupersededBy: &winnerID},
	}

	deps := Dependencies{
		Backend:     storage.BackendPostgres,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Enrich:      newMockEnrichService(nsID, memories),
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_enrich"
	req.Params.Arguments = map[string]interface{}{"project": "test"}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryEnrich(ctx, srv, req)
	if err != nil {
		t.Fatalf("default enrich: %v", err)
	}
	if result.IsError {
		t.Fatalf("default enrich errored: %v", result.Content)
	}
	var resp service.EnrichResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("decode default: %v", err)
	}
	if resp.Queued != 1 {
		t.Fatalf("expected only the winner queued; got %d", resp.Queued)
	}

	deps.Enrich = newMockEnrichService(nsID, memories)
	srv = NewServer(deps)
	reqIncl := mcp.CallToolRequest{}
	reqIncl.Params.Name = "memory_enrich"
	reqIncl.Params.Arguments = map[string]interface{}{
		"project":            "test",
		"include_superseded": true,
	}
	resultIncl, err := handleMemoryEnrich(ctx, srv, reqIncl)
	if err != nil {
		t.Fatalf("include enrich: %v", err)
	}
	if resultIncl.IsError {
		t.Fatalf("include enrich errored: %v", resultIncl.Content)
	}
	var respIncl service.EnrichResponse
	if err := json.Unmarshal([]byte(extractText(resultIncl)), &respIncl); err != nil {
		t.Fatalf("decode include: %v", err)
	}
	if respIncl.Queued != 2 {
		t.Fatalf("expected both rows queued with include_superseded; got %d", respIncl.Queued)
	}
}
