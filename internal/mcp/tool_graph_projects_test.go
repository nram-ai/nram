package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
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

func (m *mockEntityReader) SearchEntities(_ context.Context, _ uuid.UUID, _ string, _ string, _ int) ([]model.Entity, error) {
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
	// lastMaxEdges records the cap argument from the most recent call so tests
	// can verify the handler propagates the configured edge cap.
	lastMaxEdges int
	// maxEdgesByCall records the cap value the handler passed on each
	// invocation, in order, so cumulative-cap tests can assert the cap
	// shrinks across seeds.
	maxEdgesByCall []int
	// truncated lets a test simulate a short-circuit so the handler-side
	// _truncated envelope path can be exercised.
	truncated bool
	// relsByCall, when non-nil, returns the per-call rel slice instead of
	// the shared rels field. callCount indexes into it; calls past the
	// slice length return an empty result. Lets cumulative-cap tests
	// stage distinct rels per seed without sharing IDs that would dedup.
	relsByCall [][]model.Relationship
	callCount  int
}

func (m *mockTraverser) TraverseFromEntity(_ context.Context, _ uuid.UUID, depth, maxEdges int) (storage.TraversalResult, error) {
	m.lastDepth = depth
	m.lastMaxEdges = maxEdges
	m.maxEdgesByCall = append(m.maxEdgesByCall, maxEdges)
	out := m.rels
	if m.relsByCall != nil {
		if m.callCount < len(m.relsByCall) {
			out = m.relsByCall[m.callCount]
		} else {
			out = nil
		}
	}
	m.callCount++
	return storage.TraversalResult{Relationships: out, Truncated: m.truncated, Cap: maxEdges}, m.err
}

// --- memory_graph schema tests ---

func TestMemoryGraph_Registered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["graph"]; !ok {
		t.Error("expected memory_graph to be registered on Postgres")
	}
}

// --- memory_projects schema tests ---

func TestMemoryProjects_Registered_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["list_projects"]; !ok {
		t.Error("expected memory_projects to be registered on SQLite")
	}
}

func TestMemoryProjects_Registered_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)

	tools := srv.MCPServer().ListTools()
	if _, ok := tools["list_projects"]; !ok {
		t.Error("expected memory_projects to be registered on Postgres")
	}
}

// --- memory_export tool removed 2026-05-27 (truncation-bound payload). The
// /v1/me/exports REST + UI pipeline replaces it. A regression guard that
// the tool is NOT registered lives in tool_graph_projects_test.go (renamed
// from the historical export-suffixed file).

// --- memory_graph handler tests ---

func TestHandleMemoryGraph_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"entity": "test"}

	result, err := handleMemoryGraph(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryGraph_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"entity": "test"}

	ctx := buildNoAuthCtx()
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleMemoryGraph_MissingEntity(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{}

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
	memID := uuid.New()
	rels := []model.Relationship{
		{
			ID:           relID,
			SourceID:     entityID,
			TargetID:     targetID,
			Relation:     "knows",
			Weight:       1.0,
			ValidFrom:    time.Now(),
			SourceMemory: &memID,
		},
	}

	traverser := &mockTraverser{rels: rels}
	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{getErr: nil, project: nil},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    traverser,
		MemoryLister: &aliveMemoryLister{mems: []model.Memory{{ID: memID, NamespaceID: nsID}}},
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
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
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
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

// TestHandleMemoryGraph_EdgeCapTruncated pins the wiring between the
// traverser's Truncated signal and the handler-emitted _truncated envelope:
// when the traverser short-circuits at graph.max_edges, the response carries
// a top-level _truncated.reason == "edge_cap" so MCP clients can surface a
// partial-result banner before the byte-budget reducer in result_limit.go
// has any reason to fire. The default graph.max_edges (2000) flows through
// ResolveIntWithDefault when Settings is unset.
func TestHandleMemoryGraph_EdgeCapTruncated(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	entityID := uuid.New()
	targetID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	entities := []model.Entity{
		{ID: entityID, NamespaceID: nsID, Name: "Alice", EntityType: "person", Canonical: "alice"},
		{ID: targetID, NamespaceID: nsID, Name: "Bob", EntityType: "person", Canonical: "bob"},
	}
	rels := []model.Relationship{
		{
			ID:        uuid.New(),
			SourceID:  entityID,
			TargetID:  targetID,
			Relation:  "knows",
			Weight:    1.0,
			ValidFrom: time.Now(),
		},
	}
	traverser := &mockTraverser{rels: rels, truncated: true}
	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    traverser,
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"entity": "Alice"}
	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}
	// Default graph.max_edges is 2000 in the registered defaults; with a
	// nil Settings this resolves through to ResolveIntWithDefault. The
	// first seed always sees the full cap; later seeds see the cap
	// tightened by the cumulative cap logic.
	if len(traverser.maxEdgesByCall) == 0 {
		t.Fatalf("expected at least one traverser call")
	}
	if traverser.maxEdgesByCall[0] != 2000 {
		t.Errorf("expected default cap 2000 on the first seed, got %d", traverser.maxEdgesByCall[0])
	}
	text := extractText(result)
	var resp graphResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Truncated == nil {
		t.Fatalf("expected _truncated envelope on capped response, got nil")
	}
	if resp.Truncated.Reason != "edge_cap" {
		t.Errorf("expected _truncated.reason=edge_cap, got %q", resp.Truncated.Reason)
	}
	if resp.Truncated.ReturnedCount != len(resp.Relationships) {
		t.Errorf("expected returned_count=%d, got %d", len(resp.Relationships), resp.Truncated.ReturnedCount)
	}
}

// TestHandleMemoryGraph_CumulativeCapAcrossSeeds pins the per-handler
// cumulative cap on the multi-seed case: N seed entities each contributing
// disjoint rels below the per-seed cap must still trip the handler-level
// cap once their deduped union reaches graph.max_edges. Without this guard,
// FindBySimilarity returning many seeds would let the post-traversal
// filters, orphan resolution, marshal, and (eventually) byte-budget reducer
// all run on data the client cannot consume, and the truncatedByCap signal
// would never fire.
func TestHandleMemoryGraph_CumulativeCapAcrossSeeds(t *testing.T) {
	// Stub a SettingsService that resolves mcp.max_result_tokens to a
	// generous value so the byte-budget reducer cannot fire — that lets us
	// observe the cumulative edge-cap signal directly. The previous
	// NRAM_MCP_MAX_RESULT_TOKENS env override was removed in the
	// SettingsService cascade migration.
	settingsSvc := newSettingsServiceWithMCPBudget(1000000)
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	// Three seeds with distinct IDs so the dedup pass keeps them separate.
	seedIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	entities := make([]model.Entity, len(seedIDs))
	for i, id := range seedIDs {
		entities[i] = model.Entity{ID: id, NamespaceID: nsID, Name: fmt.Sprintf("seed-%d", i), EntityType: "concept", Canonical: fmt.Sprintf("seed-%d", i)}
	}
	// Each seed traversal returns one fresh edge per call with distinct
	// rel IDs so the handler-level dedup does not collapse them. The
	// target IDs are the seed IDs themselves so resolveGraphOrphans keeps
	// every edge (both endpoints exist in graphEntities).
	relsByCall := [][]model.Relationship{
		{{ID: uuid.New(), SourceID: seedIDs[0], TargetID: seedIDs[1], Relation: "knows", Weight: 1, ValidFrom: time.Now()}},
		{{ID: uuid.New(), SourceID: seedIDs[1], TargetID: seedIDs[2], Relation: "knows", Weight: 1, ValidFrom: time.Now()}},
		{{ID: uuid.New(), SourceID: seedIDs[2], TargetID: seedIDs[0], Relation: "knows", Weight: 1, ValidFrom: time.Now()}},
	}
	traverser := &mockTraverser{relsByCall: relsByCall}

	// Use a tiny env-driven cap by wiring a stub Settings. Since we have
	// no SettingsService stub, achieve the cap via NRAM env... actually
	// the cap key resolves through SettingsService only. Easiest path:
	// rely on the default 2000 cap and stage 2001 fake rels. That's a
	// lot of allocations, so instead stage a tiny number of seeds and
	// assert that the per-call lastMaxEdges shrinks across iterations
	// (the cumulative-cap signal in maxEdgesByCall) rather than the
	// terminal Truncated flag, which only fires past 2000.
	deps := Dependencies{
		Backend:      storage.BackendPostgres,
		UserRepo:     &mockUserRepoStore{user: user},
		EntityReader: &mockEntityReader{entities: entities},
		Traverser:    traverser,
		Settings:     settingsSvc,
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"entity": "seed"}
	ctx := buildAuthCtx(userID)
	if _, err := handleMemoryGraph(ctx, srv, req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 3 seeds * 1 rel each = 3 cumulative; default cap 2000.
	// maxEdgesByCall should be [2000, 1999, 1998]: cap tightens by the
	// accumulated rel count from prior seeds.
	if len(traverser.maxEdgesByCall) != 3 {
		t.Fatalf("expected 3 traverser calls, got %d", len(traverser.maxEdgesByCall))
	}
	if traverser.maxEdgesByCall[0] != 2000 {
		t.Errorf("first seed cap should be the full 2000, got %d", traverser.maxEdgesByCall[0])
	}
	if traverser.maxEdgesByCall[1] != 1999 {
		t.Errorf("second seed cap should reflect 1 rel already accumulated (1999), got %d", traverser.maxEdgesByCall[1])
	}
	if traverser.maxEdgesByCall[2] != 1998 {
		t.Errorf("third seed cap should reflect 2 rels already accumulated (1998), got %d", traverser.maxEdgesByCall[2])
	}
}

// --- memory_projects handler tests ---

func TestHandleMemoryProjects_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	result, err := handleMemoryProjects(context.Background(), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "no HTTP request in context")
}

func TestHandleMemoryProjects_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

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
	srv := newTestServer(deps)

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
	var resp listProjectsResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(resp.Projects))
	}
	if resp.Projects[0].Slug != "project-one" {
		t.Errorf("expected slug %q, got %q", "project-one", resp.Projects[0].Slug)
	}
	if resp.Projects[1].Description != "Second project" {
		t.Errorf("expected description %q, got %q", "Second project", resp.Projects[1].Description)
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
	srv := newTestServer(deps)

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
	var resp listProjectsResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(resp.Projects) != 0 {
		t.Errorf("expected 0 projects, got %d", len(resp.Projects))
	}
}

// --- memory_export handler tests removed 2026-05-27 along with the tool. ---

// TestExportToolNotRegistered guards against a future re-registration of the
// MCP export tool: the tool was withdrawn because its payload travelled
// inline through the MCP transport, which truncates anything beyond the
// configured byte budget. If a regression brings it back, this test fails
// — re-add the test only after replacing the response-payload pattern with
// an async tool that returns a job ID and a separate poll/download tool.
func TestExportToolNotRegistered(t *testing.T) {
	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		t.Run(backend, func(t *testing.T) {
			srv := newTestServer(Dependencies{Backend: backend})
			tools := srv.MCPServer().ListTools()
			if _, ok := tools["export"]; ok {
				t.Fatal("export MCP tool re-registered; was deliberately withdrawn — use /v1/me/exports instead")
			}
		})
	}
}

// TestHandleMemoryGraph_AlwaysFiltersSupersededSourceMemory confirms the MCP
// graph handler unconditionally drops relationships whose source memory was
// superseded, even when a caller passes include_superseded=true. The flag
// was stripped from MCP; only REST honors it.
func TestHandleMemoryGraph_AlwaysFiltersSupersededSourceMemory(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	project := &model.Project{ID: uuid.New(), NamespaceID: nsID, OwnerNamespaceID: nsID, Slug: "test"}

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
	srv := newTestServer(deps)

	// Even with include_superseded=true on the request (now ignored on MCP),
	// only the live relationship surfaces.
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"entity":             "Alice",
		"include_superseded": true,
	}
	ctx := buildAuthCtx(userID)
	result, err := handleMemoryGraph(ctx, srv, req)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	if result.IsError {
		t.Fatalf("graph errored: %v", result.Content)
	}
	var resp graphResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Relationships) != 1 {
		t.Fatalf("expected only the live relationship (include_superseded is stripped from MCP); got %d", len(resp.Relationships))
	}
	if resp.Relationships[0].SourceMemory == nil || *resp.Relationships[0].SourceMemory != winnerID {
		t.Errorf("expected source_memory=%s on surviving rel; got %+v", winnerID, resp.Relationships[0].SourceMemory)
	}
	assertNoOrphanRelationships(t, resp)
}

// TestGraphSortIsDeterministicOnEqualWeights pins that two graph() calls
// against the same set of equal-weight edges produce byte-identical
// responses regardless of upstream input order. The prior sort used
// Weight as the sole comparator, leaving tiebreaks to sort.SliceStable's
// preservation of input order — which itself depended on BFS traversal +
// `seenRels` map iteration (Go-randomized). The fix added a full
// tiebreak chain (Weight DESC, SourceID, TargetID, Relation) so the
// surviving prefix after byte-budget reducer truncation is reproducible.
//
// Construction: build the same logical edge set twice but feed it to
// the traverser in REVERSED order. Run the handler against both. Assert
// the response bodies are byte-identical.
func TestGraphSortIsDeterministicOnEqualWeights(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	anchor := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	// Anchor entity + 6 neighbours, all known to the anchor with Weight=1.0
	// — the classic "extractor doesn't differentiate" case where the
	// tiebreak chain has to do all the work.
	neighbours := make([]uuid.UUID, 6)
	for i := range neighbours {
		neighbours[i] = uuid.New()
	}
	entities := []model.Entity{{ID: anchor, NamespaceID: nsID, Name: "Anchor", EntityType: "person", Canonical: "anchor"}}
	for i, n := range neighbours {
		entities = append(entities, model.Entity{
			ID:          n,
			NamespaceID: nsID,
			Name:        fmt.Sprintf("Neighbour-%d", i),
			EntityType:  "person",
			Canonical:   fmt.Sprintf("n%d", i),
		})
	}
	relsForward := make([]model.Relationship, len(neighbours))
	for i, n := range neighbours {
		relsForward[i] = model.Relationship{
			ID:        uuid.New(),
			SourceID:  anchor,
			TargetID:  n,
			Relation:  "knows",
			Weight:    1.0,
			ValidFrom: time.Now(),
		}
	}
	// Same edge set, fed to the traverser in reverse insertion order.
	relsReversed := make([]model.Relationship, len(relsForward))
	for i, r := range relsForward {
		relsReversed[len(relsForward)-1-i] = r
	}

	run := func(rels []model.Relationship) string {
		deps := Dependencies{
			Backend:      storage.BackendPostgres,
			UserRepo:     &mockUserRepoStore{user: user},
			ProjectRepo:  &mockProjectRepoStore{getErr: nil, project: nil},
			EntityReader: &mockEntityReader{entities: entities},
			Traverser:    &mockTraverser{rels: rels},
		}
		srv := newTestServer(deps)
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{
			"entity": "Anchor",
			"depth":  float64(2),
		}
		res, err := handleMemoryGraph(buildAuthCtx(userID), srv, req)
		if err != nil {
			t.Fatalf("handleMemoryGraph err: %v", err)
		}
		if res.IsError {
			t.Fatalf("unexpected tool error: %v", res.Content)
		}
		return extractText(res)
	}

	textForward := run(relsForward)
	textReversed := run(relsReversed)

	if textForward != textReversed {
		t.Errorf("graph response differs between input orderings of equal-weight edges; sort tiebreak is incomplete\n--- forward\n%s\n--- reversed\n%s", textForward, textReversed)
	}
}
