package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// New mock types used only in integration tests.
// Types that already exist in other _test.go files in this package are reused
// directly — they are accessible because all files share the package "mcp".
// ---------------------------------------------------------------------------

// mockMemoryRepoWithContent stores memories in a map so tests can verify
// content after store operations. It also supports per-call failure injection.
type mockMemoryRepoWithContent struct {
	memories  map[uuid.UUID]*model.Memory
	callCount int
	failIndex int // if > 0, fail on the Nth Create call (1-based)
}

func newMockMemoryRepoWithContent() *mockMemoryRepoWithContent {
	return &mockMemoryRepoWithContent{
		memories: make(map[uuid.UUID]*model.Memory),
	}
}

func (m *mockMemoryRepoWithContent) Create(_ context.Context, mem *model.Memory) error {
	m.callCount++
	if m.failIndex > 0 && m.callCount == m.failIndex {
		return errors.New("simulated create failure")
	}
	if mem.ID == uuid.Nil {
		mem.ID = uuid.New()
	}
	clone := *mem
	m.memories[mem.ID] = &clone
	return nil
}

func (m *mockMemoryRepoWithContent) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return mem, nil
}

// trackingMemoryDeleter wraps the real deletion operations but records which
// IDs were soft-deleted and which were hard-deleted, so tests can assert on
// the distinction. The existing mockMemoryDeleter (in tool_forget_enrich_test.go)
// uses delete(m.memories, id) for both paths which makes the distinction
// unobservable — hence this separate type.
type trackingMemoryDeleter struct {
	memories    map[uuid.UUID]*model.Memory
	hardDeleted []uuid.UUID
	softDeleted []uuid.UUID
}

func newTrackingMemoryDeleter(memories map[uuid.UUID]*model.Memory) *trackingMemoryDeleter {
	if memories == nil {
		memories = make(map[uuid.UUID]*model.Memory)
	}
	return &trackingMemoryDeleter{memories: memories}
}

func (m *trackingMemoryDeleter) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return mem, nil
}

func (m *trackingMemoryDeleter) SoftDelete(_ context.Context, id uuid.UUID) error {
	m.softDeleted = append(m.softDeleted, id)
	return nil
}

func (m *trackingMemoryDeleter) HardDelete(_ context.Context, id uuid.UUID) error {
	m.hardDeleted = append(m.hardDeleted, id)
	return nil
}

func (m *trackingMemoryDeleter) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return nil, nil
}

// mockMemoryReaderForExport satisfies service.MemoryReader with an in-memory
// slice for use in the export integration test.
type mockMemoryReaderForExport struct {
	memories []model.Memory
}

func (m *mockMemoryReaderForExport) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for i := range m.memories {
		if m.memories[i].ID == id {
			return &m.memories[i], nil
		}
	}
	return nil, errors.New("not found")
}

func (m *mockMemoryReaderForExport) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	var out []model.Memory
	for _, id := range ids {
		for i := range m.memories {
			if m.memories[i].ID == id {
				out = append(out, m.memories[i])
			}
		}
	}
	return out, nil
}

func (m *mockMemoryReaderForExport) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return m.memories, nil
}

// ---------------------------------------------------------------------------
// Integration test helpers
// ---------------------------------------------------------------------------

// buildIntegStoreService builds a real StoreService backed by
// mockMemoryRepoWithContent so tests can verify stored content.
func buildIntegStoreService(memRepo *mockMemoryRepoWithContent, project *model.Project, ns *model.Namespace) *service.StoreService {
	return service.NewStoreService(
		memRepo,
		&mockProjectLookup{project: project},
		&mockNamespaceLookup{ns: ns},
		&mockIngestionLogRepo{},
		&mockTokenUsageRepo{},
		&mockEnrichmentQueueRepo{},
		nil,
		nil,
	)
}

// buildIntegBatchStoreService builds a real BatchStoreService backed by
// mockMemoryRepoWithContent.
func buildIntegBatchStoreService(memRepo *mockMemoryRepoWithContent, project *model.Project, ns *model.Namespace) *service.BatchStoreService {
	return service.NewBatchStoreService(
		memRepo,
		&mockProjectLookup{project: project},
		&mockNamespaceLookup{ns: ns},
		&mockIngestionLogRepo{},
		&mockTokenUsageRepo{},
		&mockEnrichmentQueueRepo{},
		nil,
		nil,
	)
}

// buildIntegForgetService builds a real ForgetService backed by trackingMemoryDeleter.
func buildIntegForgetService(deleter *trackingMemoryDeleter, project *model.Project) *service.ForgetService {
	return service.NewForgetService(
		deleter,
		&mockProjectLookup{project: project},
		nil, nil, nil, nil, nil,
	)
}

// buildIntegUpdateService builds a real UpdateService backed by mockMemoryUpdater.
func buildIntegUpdateService(updater *mockMemoryUpdater, project *model.Project) *service.UpdateService {
	return service.NewUpdateService(
		updater,
		&mockProjectLookup{project: project},
		&mockLineageCreator{},
		nil,
		&mockTokenUsageRepo{},
		nil,
	)
}

// buildIntegBatchGetService builds a real BatchGetService.
func buildIntegBatchGetService(memories []model.Memory, project *model.Project) *service.BatchGetService {
	return service.NewBatchGetService(
		&mockMemoryBatchReader{memories: memories},
		&mockProjectLookup{project: project},
	)
}

// buildIntegExportService builds a real ExportService.
// It reuses mockExportEntityLister and mockExportLineageReader from
// tool_graph_projects_export_test.go, plus the local mockMemoryReaderForExport.
func buildIntegExportService(memories []model.Memory, project *model.Project) *service.ExportService {
	return service.NewExportService(
		&mockMemoryReaderForExport{memories: memories},
		&mockExportEntityLister{entities: []model.Entity{}},
		&mockExportRelLister{rels: []model.Relationship{}},
		&mockExportLineageReader{},
		&mockProjectLookup{project: project},
	)
}

// standardIntegSetup returns a user, namespace, and project wired together for
// the most common test scenario.
func standardIntegSetup() (userID, nsID, projectID uuid.UUID, user *model.User, ns *model.Namespace, project *model.Project) {
	userID = uuid.New()
	nsID = uuid.New()
	projectID = uuid.New()
	user = &model.User{ID: userID, NamespaceID: nsID}
	ns = &model.Namespace{ID: nsID, Path: "/users/testuser", Depth: 2}
	project = &model.Project{
		ID:               projectID,
		NamespaceID:      nsID,
		OwnerNamespaceID: nsID,
		Name:             "test-project",
		Slug:             "test-project",
	}
	return
}

// receiveEventWithTimeout waits up to 500ms for an event on ch.
func receiveEventWithTimeout(ch <-chan events.Event) (events.Event, bool) {
	select {
	case ev := <-ch:
		return ev, true
	case <-time.After(500 * time.Millisecond):
		return events.Event{}, false
	}
}

// mockListByNsReader is a MemoryReader whose ListByNamespace returns a fixed set
// of memories. This is what the RecallService uses in the no-embedding path.
type mockListByNsReader struct {
	memories []model.Memory
}

func (m *mockListByNsReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for i := range m.memories {
		if m.memories[i].ID == id {
			return &m.memories[i], nil
		}
	}
	return nil, errors.New("not found")
}

func (m *mockListByNsReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	var out []model.Memory
	for _, id := range ids {
		for i := range m.memories {
			if m.memories[i].ID == id {
				out = append(out, m.memories[i])
			}
		}
	}
	return out, nil
}

func (m *mockListByNsReader) ListByNamespace(_ context.Context, _ uuid.UUID, limit, _ int) ([]model.Memory, error) {
	mems := m.memories
	if limit > 0 && len(mems) > limit {
		mems = mems[:limit]
	}
	return mems, nil
}

// ---------------------------------------------------------------------------
// 1. TestMCP_StoreAndRecall_RoundTrip
// ---------------------------------------------------------------------------

func TestMCP_StoreAndRecall_RoundTrip(t *testing.T) {
	userID, nsID, _, user, ns, project := standardIntegSetup()

	memRepo := newMockMemoryRepoWithContent()
	storeSvc := buildIntegStoreService(memRepo, project, ns)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
		Store:         storeSvc,
	}
	srv := NewServer(deps)
	ctx := buildAuthCtx(userID)

	// Store a memory.
	storeReq := mcp.CallToolRequest{}
	storeReq.Params.Name = "memory_store"
	storeReq.Params.Arguments = map[string]interface{}{
		"project": "test-project",
		"content": "The quick brown fox jumps over the lazy dog",
		"tags":    []interface{}{"animal", "speed"},
		"source":  "unit-test",
	}

	storeResult, err := handleMemoryStore(ctx, srv, storeReq)
	if err != nil {
		t.Fatalf("store: unexpected Go error: %v", err)
	}
	if storeResult.IsError {
		t.Fatalf("store: unexpected tool error: %s", extractText(storeResult))
	}

	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(extractText(storeResult)), &storeResp); err != nil {
		t.Fatalf("store: failed to unmarshal response: %v", err)
	}
	if storeResp.ID == uuid.Nil {
		t.Error("store: expected non-nil memory ID in response")
	}
	if storeResp.ProjectSlug != "test-project" {
		t.Errorf("store: expected project_slug %q, got %q", "test-project", storeResp.ProjectSlug)
	}
	if storeResp.Content != "The quick brown fox jumps over the lazy dog" {
		t.Errorf("store: content mismatch: got %q", storeResp.Content)
	}

	// Verify the memory was persisted in the repo.
	stored, ok := memRepo.memories[storeResp.ID]
	if !ok {
		t.Fatal("store: memory not found in repository after store")
	}
	if stored.Content != "The quick brown fox jumps over the lazy dog" {
		t.Errorf("store: persisted content mismatch: got %q", stored.Content)
	}

	// Now recall using a service that can see the stored memory via
	// ListByNamespace (the fallback path when there is no embedding provider).
	storedMemory := *stored
	recallSvc := service.NewRecallService(
		&mockListByNsReader{memories: []model.Memory{storedMemory}},
		&mockProjectLookup{project: project},
		&mockNamespaceLookup{ns: ns},
		&mockTokenUsageRepo{},
		nil, nil, nil, nil, nil,
	)
	deps.Recall = recallSvc
	srv2 := NewServer(deps)

	recallReq := mcp.CallToolRequest{}
	recallReq.Params.Name = "memory_recall"
	recallReq.Params.Arguments = map[string]interface{}{
		"query":   "quick brown fox",
		"project": "test-project",
	}

	recallResult, err := handleMemoryRecall(ctx, srv2, recallReq)
	if err != nil {
		t.Fatalf("recall: unexpected Go error: %v", err)
	}
	if recallResult.IsError {
		t.Fatalf("recall: unexpected tool error: %s", extractText(recallResult))
	}

	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(extractText(recallResult)), &recallResp); err != nil {
		t.Fatalf("recall: failed to unmarshal response: %v", err)
	}
	if len(recallResp.Memories) == 0 {
		t.Fatal("recall: expected at least 1 memory, got 0")
	}
	if recallResp.Memories[0].Content != "The quick brown fox jumps over the lazy dog" {
		t.Errorf("recall: content mismatch: got %q", recallResp.Memories[0].Content)
	}
	_ = nsID
}

// ---------------------------------------------------------------------------
// 2. TestMCP_StoreAutoCreatesProject
// ---------------------------------------------------------------------------

func TestMCP_StoreAutoCreatesProject(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	ns := &model.Namespace{ID: nsID, Path: "/users/testuser", Depth: 2}

	// Project not found — triggers auto-create path.
	projectRepo := &mockProjectRepoStore{getErr: errors.New("not found")}
	memRepo := newMockMemoryRepoWithContent()

	autoProject := &model.Project{
		ID:          uuid.New(),
		NamespaceID: nsID,
		Slug:        "brand-new-project",
	}
	storeSvc := service.NewStoreService(
		memRepo,
		&mockProjectLookup{project: autoProject},
		&mockNamespaceLookup{ns: ns},
		&mockIngestionLogRepo{},
		&mockTokenUsageRepo{},
		&mockEnrichmentQueueRepo{},
		nil,
		nil,
	)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   projectRepo,
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
		Store:         storeSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store"
	req.Params.Arguments = map[string]interface{}{
		"project": "brand-new-project",
		"content": "something worth remembering",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	// Verify ProjectRepo.Create was called.
	if projectRepo.created == nil {
		t.Fatal("expected ProjectRepo.Create to have been called for auto-create, but it was not")
	}
	if projectRepo.created.Slug != "brand-new-project" {
		t.Errorf("auto-created project slug: expected %q, got %q", "brand-new-project", projectRepo.created.Slug)
	}
}

// ---------------------------------------------------------------------------
// 3. TestMCP_StoreBatch_AllSucceed
// ---------------------------------------------------------------------------

func TestMCP_StoreBatch_AllSucceed(t *testing.T) {
	userID, nsID, projectID, user, ns, project := standardIntegSetup()
	memRepo := newMockMemoryRepoWithContent()
	batchSvc := buildIntegBatchStoreService(memRepo, project, ns)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
		BatchStore:    batchSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"project": "test-project",
		"items": []interface{}{
			map[string]interface{}{"content": "batch item one", "tags": []interface{}{"t1"}},
			map[string]interface{}{"content": "batch item two"},
			map[string]interface{}{"content": "batch item three", "source": "src"},
		},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStoreBatch(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var resp service.BatchStoreResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Processed != 3 {
		t.Errorf("expected processed=3, got %d", resp.Processed)
	}
	if resp.MemoriesCreated != 3 {
		t.Errorf("expected memories_created=3, got %d", resp.MemoriesCreated)
	}
	if len(resp.Errors) != 0 {
		t.Errorf("expected 0 errors, got %d: %v", len(resp.Errors), resp.Errors)
	}
	if len(memRepo.memories) != 3 {
		t.Errorf("expected 3 memories in repo, got %d", len(memRepo.memories))
	}
	_, _ = nsID, projectID
}

// ---------------------------------------------------------------------------
// 4. TestMCP_StoreBatch_PartialFailure
// ---------------------------------------------------------------------------

func TestMCP_StoreBatch_PartialFailure(t *testing.T) {
	userID, nsID, projectID, user, ns, project := standardIntegSetup()

	memRepo := newMockMemoryRepoWithContent()
	// Fail on the 2nd Create call.
	memRepo.failIndex = 2

	batchSvc := buildIntegBatchStoreService(memRepo, project, ns)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
		BatchStore:    batchSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store_batch"
	req.Params.Arguments = map[string]interface{}{
		"project": "test-project",
		"items": []interface{}{
			map[string]interface{}{"content": "item A"},
			map[string]interface{}{"content": "item B — will fail"},
			map[string]interface{}{"content": "item C"},
		},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStoreBatch(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var resp service.BatchStoreResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Processed != 3 {
		t.Errorf("expected processed=3, got %d", resp.Processed)
	}
	if resp.MemoriesCreated != 2 {
		t.Errorf("expected memories_created=2 (1st and 3rd succeed), got %d", resp.MemoriesCreated)
	}
	if len(resp.Errors) != 1 {
		t.Errorf("expected exactly 1 error (the 2nd item), got %d: %v", len(resp.Errors), resp.Errors)
	}
	if len(resp.Errors) == 1 && resp.Errors[0].Index != 1 {
		t.Errorf("expected error at index 1, got index %d", resp.Errors[0].Index)
	}
	_, _ = nsID, projectID
}

// ---------------------------------------------------------------------------
// 5. TestMCP_UpdateMemory
// ---------------------------------------------------------------------------

func TestMCP_UpdateMemory(t *testing.T) {
	userID, nsID, projectID, user, _, project := standardIntegSetup()
	memoryID := uuid.New()

	existing := &model.Memory{
		ID:          memoryID,
		NamespaceID: nsID,
		Content:     "original content",
		Tags:        []string{"old-tag"},
		Metadata:    json.RawMessage(`{}`),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	updater := &mockMemoryUpdater{mem: existing}
	updateSvc := buildIntegUpdateService(updater, project)

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
		"project": "test-project",
		"content": "updated content",
		"tags":    []interface{}{"new-tag"},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var resp service.UpdateResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.ID != memoryID {
		t.Errorf("expected ID %s, got %s", memoryID, resp.ID)
	}
	if resp.Content != "updated content" {
		t.Errorf("expected content %q, got %q", "updated content", resp.Content)
	}
	if resp.PreviousContent != "original content" {
		t.Errorf("expected previous_content %q, got %q", "original content", resp.PreviousContent)
	}
	// Without embed provider, re_embedded must be false.
	if resp.ReEmbedded {
		t.Error("expected re_embedded=false when no embed provider is configured")
	}
	if resp.ProjectID != projectID {
		t.Errorf("expected project_id %s, got %s", projectID, resp.ProjectID)
	}
}

// ---------------------------------------------------------------------------
// 6. TestMCP_GetMemory_FoundAndNotFound
// ---------------------------------------------------------------------------

func TestMCP_GetMemory_FoundAndNotFound(t *testing.T) {
	userID, nsID, projectID, user, _, project := standardIntegSetup()

	memID1 := uuid.New()
	memID2 := uuid.New()
	fakeID := uuid.New()

	now := time.Now()
	memories := []model.Memory{
		{
			ID:          memID1,
			NamespaceID: nsID,
			Content:     "first memory",
			Tags:        []string{"alpha"},
			Metadata:    json.RawMessage(`{"k":"v"}`),
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			ID:          memID2,
			NamespaceID: nsID,
			Content:     "second memory",
			Tags:        []string{"beta"},
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	batchGetSvc := buildIntegBatchGetService(memories, project)

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
		"ids":     []interface{}{memID1.String(), memID2.String(), fakeID.String()},
		"project": "test-project",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGet(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var resp service.BatchGetResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(resp.Found) != 2 {
		t.Errorf("expected 2 found memories, got %d", len(resp.Found))
	}
	if len(resp.NotFound) != 1 {
		t.Errorf("expected 1 not_found ID, got %d", len(resp.NotFound))
	}
	if len(resp.NotFound) == 1 && resp.NotFound[0] != fakeID {
		t.Errorf("expected not_found[0]=%s, got %s", fakeID, resp.NotFound[0])
	}

	foundContents := make(map[uuid.UUID]string)
	for _, m := range resp.Found {
		foundContents[m.ID] = m.Content
	}
	if foundContents[memID1] != "first memory" {
		t.Errorf("found[memID1] content: expected %q, got %q", "first memory", foundContents[memID1])
	}
	if foundContents[memID2] != "second memory" {
		t.Errorf("found[memID2] content: expected %q, got %q", "second memory", foundContents[memID2])
	}
	_ = projectID
}

// ---------------------------------------------------------------------------
// 7. TestMCP_ForgetMemory_Soft
// ---------------------------------------------------------------------------

func TestMCP_ForgetMemory_Soft(t *testing.T) {
	userID, nsID, projectID, user, _, project := standardIntegSetup()
	memoryID := uuid.New()

	memories := map[uuid.UUID]*model.Memory{
		memoryID: {
			ID:          memoryID,
			NamespaceID: nsID,
			Content:     "memory to soft-delete",
		},
	}
	deleter := newTrackingMemoryDeleter(memories)
	forgetSvc := buildIntegForgetService(deleter, project)

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
		"project": "test-project",
		"ids":     []interface{}{memoryID.String()},
		// hard omitted → defaults to false (soft delete)
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var resp service.ForgetResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("expected deleted=1, got %d", resp.Deleted)
	}
	if len(deleter.softDeleted) != 1 {
		t.Errorf("expected 1 soft-deleted memory, got %d", len(deleter.softDeleted))
	}
	if len(deleter.hardDeleted) != 0 {
		t.Errorf("expected 0 hard-deleted memories, got %d (soft-delete was requested)", len(deleter.hardDeleted))
	}
	if len(deleter.softDeleted) == 1 && deleter.softDeleted[0] != memoryID {
		t.Errorf("soft-deleted ID: expected %s, got %s", memoryID, deleter.softDeleted[0])
	}
	_ = projectID
}

// ---------------------------------------------------------------------------
// 8. TestMCP_ForgetMemory_Hard
// ---------------------------------------------------------------------------

func TestMCP_ForgetMemory_Hard(t *testing.T) {
	userID, nsID, projectID, user, _, project := standardIntegSetup()
	memoryID := uuid.New()

	memories := map[uuid.UUID]*model.Memory{
		memoryID: {
			ID:          memoryID,
			NamespaceID: nsID,
			Content:     "memory to hard-delete",
		},
	}
	deleter := newTrackingMemoryDeleter(memories)
	forgetSvc := buildIntegForgetService(deleter, project)

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
		"project": "test-project",
		"ids":     []interface{}{memoryID.String()},
		"hard":    true,
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var resp service.ForgetResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("expected deleted=1, got %d", resp.Deleted)
	}
	if len(deleter.hardDeleted) != 1 {
		t.Errorf("expected 1 hard-deleted memory, got %d", len(deleter.hardDeleted))
	}
	if len(deleter.softDeleted) != 0 {
		t.Errorf("expected 0 soft-deleted memories, got %d (hard-delete was requested)", len(deleter.softDeleted))
	}
	if len(deleter.hardDeleted) == 1 && deleter.hardDeleted[0] != memoryID {
		t.Errorf("hard-deleted ID: expected %s, got %s", memoryID, deleter.hardDeleted[0])
	}
	_ = projectID
}

// ---------------------------------------------------------------------------
// 9 & 10. TestMCP_NoAuth_RequiresAuthentication (table-driven)
// ---------------------------------------------------------------------------

func TestMCP_NoAuth_RequiresAuthentication(t *testing.T) {
	type toolCase struct {
		name   string
		toolFn func(ctx context.Context, s *Server, r mcp.CallToolRequest) (*mcp.CallToolResult, error)
		args   map[string]interface{}
	}

	tests := []toolCase{
		{
			name:   "recall_no_auth",
			toolFn: handleMemoryRecall,
			args:   map[string]interface{}{"query": "something"},
		},
		{
			name:   "store_no_auth",
			toolFn: handleMemoryStore,
			args:   map[string]interface{}{"project": "p", "content": "c"},
		},
		{
			name:   "batch_store_no_auth",
			toolFn: handleMemoryStoreBatch,
			args: map[string]interface{}{
				"project": "p",
				"items":   []interface{}{map[string]interface{}{"content": "c"}},
			},
		},
		{
			name:   "update_no_auth",
			toolFn: handleMemoryUpdate,
			args:   map[string]interface{}{"id": uuid.New().String(), "project": "p", "content": "c"},
		},
		{
			name:   "get_no_auth",
			toolFn: handleMemoryGet,
			args:   map[string]interface{}{"ids": []interface{}{uuid.New().String()}, "project": "p"},
		},
		{
			name:   "forget_no_auth",
			toolFn: handleMemoryForget,
			args:   map[string]interface{}{"project": "p", "ids": []interface{}{uuid.New().String()}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := Dependencies{Backend: storage.BackendSQLite}
			srv := NewServer(deps)

			req := mcp.CallToolRequest{}
			req.Params.Arguments = tt.args

			ctx := buildNoAuthCtx()
			result, err := tt.toolFn(ctx, srv, req)
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}
			assertToolError(t, result, "authentication required")
		})
	}
}

// ---------------------------------------------------------------------------
// 11. TestMCP_StoreEmptyContent
// ---------------------------------------------------------------------------

func TestMCP_StoreEmptyContent(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "some-project",
		"content": "   ", // whitespace only
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	assertToolError(t, result, "content is required")
}

// ---------------------------------------------------------------------------
// 12. TestMCP_StoreEmptyProject
// ---------------------------------------------------------------------------

func TestMCP_StoreEmptyProject(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"project": "",
		"content": "valid content",
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	assertToolError(t, result, "project is required")
}

// ---------------------------------------------------------------------------
// 13. TestMCP_RecallEmptyQuery
// ---------------------------------------------------------------------------

func TestMCP_RecallEmptyQuery(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]interface{}{
		"query": "   ", // whitespace only
	}

	ctx := buildAuthCtx(uuid.New())
	result, err := handleMemoryRecall(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	assertToolError(t, result, "query is required")
}

// ---------------------------------------------------------------------------
// 14. TestMCP_RecallOrgScope
// ---------------------------------------------------------------------------

func TestMCP_RecallOrgScope(t *testing.T) {
	userID := uuid.New()
	orgNSID := uuid.New()

	org := &model.Organization{
		ID:          uuid.New(),
		NamespaceID: orgNSID,
		Slug:        "acme-corp",
	}

	ns := &model.Namespace{ID: orgNSID, Path: "/orgs/acme-corp", Depth: 1}

	recallSvc := service.NewRecallService(
		&mockMemoryBatchReader{memories: []model.Memory{}},
		nil,
		&mockNamespaceLookup{ns: ns},
		&mockTokenUsageRepo{},
		nil, nil, nil, nil, nil,
	)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: &model.User{ID: userID, NamespaceID: uuid.New()}},
		ProjectRepo:   &mockProjectRepoStore{},
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
		OrgRepo:       &mockOrgRepo{org: org},
		Recall:        recallSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_recall"
	req.Params.Arguments = map[string]interface{}{
		"query": "org-wide search",
		"org":   "acme-corp",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryRecall(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var resp service.RecallResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	// Reaching here means OrgRepo.GetBySlug was called and succeeded.
	_ = resp
}

// ---------------------------------------------------------------------------
// 15. TestMCP_ProjectsList
// ---------------------------------------------------------------------------

func TestMCP_ProjectsList(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	projects := []model.Project{
		{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "Alpha", Slug: "alpha"},
		{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "Beta", Slug: "beta"},
	}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{listResult: projects},
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_projects"

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryProjects(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var items []projectItem
	if err := json.Unmarshal([]byte(extractText(result)), &items); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(items))
	}

	slugs := make(map[string]bool)
	for _, item := range items {
		slugs[item.Slug] = true
	}
	if !slugs["alpha"] {
		t.Error("expected project with slug 'alpha' to be present")
	}
	if !slugs["beta"] {
		t.Error("expected project with slug 'beta' to be present")
	}
}

// ---------------------------------------------------------------------------
// 16. TestMCP_ExportProject
// ---------------------------------------------------------------------------

func TestMCP_ExportProject(t *testing.T) {
	userID, nsID, projectID, user, _, project := standardIntegSetup()

	now := time.Now()
	memories := []model.Memory{
		{
			ID:          uuid.New(),
			NamespaceID: nsID,
			Content:     "exported memory one",
			Tags:        []string{"export"},
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	exportSvc := buildIntegExportService(memories, project)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Export:      exportSvc,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_export"
	req.Params.Arguments = map[string]interface{}{
		"project": "test-project",
		"format":  "json",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryExport(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var data service.ExportData
	if err := json.Unmarshal([]byte(extractText(result)), &data); err != nil {
		t.Fatalf("failed to unmarshal export response: %v", err)
	}
	if data.Version == "" {
		t.Error("expected export version to be set")
	}
	if data.Project.ID != projectID {
		t.Errorf("expected project ID %s, got %s", projectID, data.Project.ID)
	}
	if data.Stats.MemoryCount != 1 {
		t.Errorf("expected memory_count=1, got %d", data.Stats.MemoryCount)
	}
	if len(data.Memories) != 1 {
		t.Errorf("expected 1 memory in export, got %d", len(data.Memories))
	}
	if len(data.Memories) == 1 && data.Memories[0].Content != "exported memory one" {
		t.Errorf("exported memory content: expected %q, got %q", "exported memory one", data.Memories[0].Content)
	}
}

// ---------------------------------------------------------------------------
// 17. TestMCP_StoreEmitsEvent
// ---------------------------------------------------------------------------

func TestMCP_StoreEmitsEvent(t *testing.T) {
	userID, nsID, _, user, ns, project := standardIntegSetup()
	memRepo := newMockMemoryRepoWithContent()
	storeSvc := buildIntegStoreService(memRepo, project, ns)

	bus := events.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })

	ch, cancel, err := bus.Subscribe(context.Background(), "project:")
	if err != nil {
		t.Fatalf("failed to subscribe to bus: %v", err)
	}
	t.Cleanup(cancel)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		UserRepo:      &mockUserRepoStore{user: user},
		ProjectRepo:   &mockProjectRepoStore{project: project},
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
		Store:         storeSvc,
		EventBus:      bus,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_store"
	req.Params.Arguments = map[string]interface{}{
		"project": "test-project",
		"content": "event emission test",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryStore(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(extractText(result)), &storeResp); err != nil {
		t.Fatalf("failed to unmarshal store response: %v", err)
	}

	ev, received := receiveEventWithTimeout(ch)
	if !received {
		t.Fatal("expected memory.created event but none received within timeout")
	}
	if ev.Type != events.MemoryCreated {
		t.Errorf("expected event type %q, got %q", events.MemoryCreated, ev.Type)
	}
	expectedScope := "project:" + project.ID.String()
	if ev.Scope != expectedScope {
		t.Errorf("expected event scope %q, got %q", expectedScope, ev.Scope)
	}

	var evData map[string]string
	if err := json.Unmarshal(ev.Data, &evData); err != nil {
		t.Fatalf("failed to unmarshal event data: %v", err)
	}
	if evData["memory_id"] == "" {
		t.Error("expected memory_id in event data to be set")
	}
	if evData["memory_id"] != storeResp.ID.String() {
		t.Errorf("event memory_id: expected %s, got %s", storeResp.ID, evData["memory_id"])
	}
	_ = nsID
}

// ---------------------------------------------------------------------------
// 18. TestMCP_ForgetEmitsEvent
// ---------------------------------------------------------------------------

func TestMCP_ForgetEmitsEvent(t *testing.T) {
	userID, nsID, _, user, _, project := standardIntegSetup()
	memoryID := uuid.New()

	memories := map[uuid.UUID]*model.Memory{
		memoryID: {
			ID:          memoryID,
			NamespaceID: nsID,
			Content:     "to be deleted",
		},
	}
	deleter := newTrackingMemoryDeleter(memories)
	forgetSvc := buildIntegForgetService(deleter, project)

	bus := events.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })

	ch, cancel, err := bus.Subscribe(context.Background(), "project:")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	t.Cleanup(cancel)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Forget:      forgetSvc,
		EventBus:    bus,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_forget"
	req.Params.Arguments = map[string]interface{}{
		"project": "test-project",
		"ids":     []interface{}{memoryID.String()},
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryForget(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	ev, received := receiveEventWithTimeout(ch)
	if !received {
		t.Fatal("expected memory.deleted event but none received within timeout")
	}
	if ev.Type != events.MemoryDeleted {
		t.Errorf("expected event type %q, got %q", events.MemoryDeleted, ev.Type)
	}
	expectedScope := "project:" + project.ID.String()
	if ev.Scope != expectedScope {
		t.Errorf("expected event scope %q, got %q", expectedScope, ev.Scope)
	}

	var evData map[string]string
	if err := json.Unmarshal(ev.Data, &evData); err != nil {
		t.Fatalf("failed to unmarshal event data: %v", err)
	}
	if evData["deleted"] == "" {
		t.Error("expected 'deleted' count in event data")
	}
}

// ---------------------------------------------------------------------------
// 19. TestMCP_UpdateEmitsEvent
// ---------------------------------------------------------------------------

func TestMCP_UpdateEmitsEvent(t *testing.T) {
	userID, nsID, _, user, _, project := standardIntegSetup()
	memoryID := uuid.New()

	existing := &model.Memory{
		ID:          memoryID,
		NamespaceID: nsID,
		Content:     "pre-update content",
		Tags:        []string{},
		Metadata:    json.RawMessage(`{}`),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	updater := &mockMemoryUpdater{mem: existing}
	updateSvc := buildIntegUpdateService(updater, project)

	bus := events.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })

	ch, cancel, err := bus.Subscribe(context.Background(), "project:")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	t.Cleanup(cancel)

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: project},
		Update:      updateSvc,
		EventBus:    bus,
	}
	srv := NewServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "memory_update"
	req.Params.Arguments = map[string]interface{}{
		"id":      memoryID.String(),
		"project": "test-project",
		"content": "post-update content",
	}

	ctx := buildAuthCtx(userID)
	result, err := handleMemoryUpdate(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(result))
	}

	ev, received := receiveEventWithTimeout(ch)
	if !received {
		t.Fatal("expected memory.updated event but none received within timeout")
	}
	if ev.Type != events.MemoryUpdated {
		t.Errorf("expected event type %q, got %q", events.MemoryUpdated, ev.Type)
	}
	expectedScope := "project:" + project.ID.String()
	if ev.Scope != expectedScope {
		t.Errorf("expected event scope %q, got %q", expectedScope, ev.Scope)
	}

	var evData map[string]string
	if err := json.Unmarshal(ev.Data, &evData); err != nil {
		t.Fatalf("failed to unmarshal event data: %v", err)
	}
	if evData["memory_id"] != memoryID.String() {
		t.Errorf("event memory_id: expected %s, got %s", memoryID, evData["memory_id"])
	}
}

// ---------------------------------------------------------------------------
// 20. TestMCP_SQLiteOmitsEnrichTool
// ---------------------------------------------------------------------------

func TestMCP_SQLiteOmitsEnrichTool(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_enrich"]; ok {
		t.Error("memory_enrich tool must NOT be registered on SQLite backend")
	}
}

// ---------------------------------------------------------------------------
// 21. TestMCP_PostgresIncludesEnrichTool
// ---------------------------------------------------------------------------

func TestMCP_PostgresIncludesEnrichTool(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_enrich"]; !ok {
		t.Error("memory_enrich tool must be registered on Postgres backend")
	}
}

// ---------------------------------------------------------------------------
// 22. TestMCP_SQLiteOmitsGraphTool
// ---------------------------------------------------------------------------

func TestMCP_SQLiteOmitsGraphTool(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_graph"]; ok {
		t.Error("memory_graph tool must NOT be registered on SQLite backend")
	}
}

func TestMCP_PostgresIncludesGraphTool(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["memory_graph"]; !ok {
		t.Error("memory_graph tool must be registered on Postgres backend")
	}
}

// ---------------------------------------------------------------------------
// 23. TestMCP_ResourceProjects
// ---------------------------------------------------------------------------

func TestMCP_ResourceProjects(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	projects := []model.Project{
		{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "Gamma", Slug: "gamma"},
		{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Name: "Delta", Slug: "delta"},
	}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{listResult: projects},
	}
	srv := NewServer(deps)

	resReq := mcp.ReadResourceRequest{}
	resReq.Params.URI = "nram://projects"

	ctx := buildAuthCtx(userID)
	contents, err := handleProjectsResource(ctx, srv, resReq)
	if err != nil {
		t.Fatalf("handleProjectsResource returned error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 resource content, got %d", len(contents))
	}

	tc, ok := contents[0].(mcp.TextResourceContents)
	if !ok {
		t.Fatalf("expected TextResourceContents, got %T", contents[0])
	}
	if tc.URI != "nram://projects" {
		t.Errorf("expected URI %q, got %q", "nram://projects", tc.URI)
	}
	if tc.MIMEType != "application/json" {
		t.Errorf("expected MIME type %q, got %q", "application/json", tc.MIMEType)
	}

	var items []projectItem
	if err := json.Unmarshal([]byte(tc.Text), &items); err != nil {
		t.Fatalf("failed to unmarshal resource text: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 projects in resource, got %d", len(items))
	}
	slugs := make(map[string]bool)
	for _, item := range items {
		slugs[item.Slug] = true
	}
	if !slugs["gamma"] {
		t.Error("expected project 'gamma' in resource response")
	}
	if !slugs["delta"] {
		t.Error("expected project 'delta' in resource response")
	}
}

// ---------------------------------------------------------------------------
// 24. TestMCP_ResourceEntities
// ---------------------------------------------------------------------------

func TestMCP_ResourceEntities(t *testing.T) {
	userID, nsID, _, user, _, project := standardIntegSetup()

	entities := []model.Entity{
		{
			ID:           uuid.New(),
			NamespaceID:  nsID,
			Name:         "Alice",
			EntityType:   "person",
			Canonical:    "alice",
			MentionCount: 5,
		},
		{
			ID:           uuid.New(),
			NamespaceID:  nsID,
			Name:         "Acme Corp",
			EntityType:   "organization",
			Canonical:    "acme-corp",
			MentionCount: 3,
		},
	}

	// Reuse mockEntityReader from tool_graph_projects_export_test.go.
	entityReader := &mockEntityReader{entities: entities}

	deps := Dependencies{
		Backend:      storage.BackendSQLite,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{project: project},
		EntityReader: entityReader,
	}
	srv := NewServer(deps)

	resReq := mcp.ReadResourceRequest{}
	resReq.Params.URI = "nram://projects/test-project/entities"

	ctx := buildAuthCtx(userID)
	contents, err := handleProjectEntitiesResource(ctx, srv, resReq)
	if err != nil {
		t.Fatalf("handleProjectEntitiesResource returned error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 resource content, got %d", len(contents))
	}

	tc, ok := contents[0].(mcp.TextResourceContents)
	if !ok {
		t.Fatalf("expected TextResourceContents, got %T", contents[0])
	}
	if tc.MIMEType != "application/json" {
		t.Errorf("expected MIME type %q, got %q", "application/json", tc.MIMEType)
	}

	var items []resourceEntity
	if err := json.Unmarshal([]byte(tc.Text), &items); err != nil {
		t.Fatalf("failed to unmarshal entities resource text: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(items))
	}

	names := make(map[string]bool)
	for _, item := range items {
		names[item.Name] = true
	}
	if !names["Alice"] {
		t.Error("expected entity 'Alice' in resource response")
	}
	if !names["Acme Corp"] {
		t.Error("expected entity 'Acme Corp' in resource response")
	}

	for _, item := range items {
		if item.Name == "Alice" && item.MentionCount != 5 {
			t.Errorf("Alice: expected mention_count=5, got %d", item.MentionCount)
		}
	}
}

// ---------------------------------------------------------------------------
// Regression: resource handlers return error on missing auth
// ---------------------------------------------------------------------------

func TestMCP_ResourceEntities_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	resReq := mcp.ReadResourceRequest{}
	resReq.Params.URI = "nram://projects/test-project/entities"

	ctx := buildNoAuthCtx()
	_, err := handleProjectEntitiesResource(ctx, srv, resReq)
	if err == nil {
		t.Fatal("expected error when no auth present, got nil")
	}
	if !containsSubstring(err.Error(), "authentication required") {
		t.Errorf("expected error containing 'authentication required', got: %v", err)
	}
}

func TestMCP_ResourceProjects_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)

	resReq := mcp.ReadResourceRequest{}
	resReq.Params.URI = "nram://projects"

	ctx := buildNoAuthCtx()
	_, err := handleProjectsResource(ctx, srv, resReq)
	if err == nil {
		t.Fatal("expected error when no auth present, got nil")
	}
	if !containsSubstring(err.Error(), "authentication required") {
		t.Errorf("expected error containing 'authentication required', got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Regression: all core tools registered on each backend
// ---------------------------------------------------------------------------

func TestMCP_CoreToolsRegistered_SQLite(t *testing.T) {
	required := []string{
		"memory_store",
		"memory_store_batch",
		"memory_recall",
		"memory_update",
		"memory_get",
		"memory_forget",
		"memory_projects",
		"memory_export",
	}

	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := NewServer(deps)
	tools := srv.MCPServer().ListTools()

	for _, name := range required {
		if _, ok := tools[name]; !ok {
			t.Errorf("expected tool %q to be registered on SQLite, but it is missing", name)
		}
	}
}

func TestMCP_CoreToolsRegistered_Postgres(t *testing.T) {
	required := []string{
		"memory_store",
		"memory_store_batch",
		"memory_recall",
		"memory_update",
		"memory_get",
		"memory_forget",
		"memory_projects",
		"memory_export",
		"memory_enrich",
		"memory_graph",
	}

	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := NewServer(deps)
	tools := srv.MCPServer().ListTools()

	for _, name := range required {
		if _, ok := tools[name]; !ok {
			t.Errorf("expected tool %q to be registered on Postgres, but it is missing", name)
		}
	}
}
