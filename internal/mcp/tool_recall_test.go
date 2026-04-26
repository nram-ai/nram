package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

func (m *mockMemoryReaderRecall) ListByNamespaceFiltered(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, _ int, _ int) ([]model.Memory, error) {
	return []model.Memory{}, nil
}

// --- schema tests ---

// TestBuildMCPRecallResponse_ResolvesOrphanGraphEndpoints exercises the
// orphan-resolution path that's the core MCP-side improvement: a relationship
// whose far endpoint isn't in the service-layer entities[] gets the missing
// entity batch-fetched and merged in, rather than being emitted as an orphan
// or silently dropped. This is the no-orphan invariant.
func TestBuildMCPRecallResponse_ResolvesOrphanGraphEndpoints(t *testing.T) {
	nsID := uuid.New()
	anchor := uuid.New()
	target := uuid.New()

	resp := &service.RecallResponse{
		Memories: []service.RecallResult{},
		Graph: service.RecallGraph{
			Entities: []service.RecallEntity{
				{ID: anchor, Name: "Alice", EntityType: "person"},
			},
			Relationships: []service.RecallRelationship{
				{ID: uuid.New(), SourceID: anchor, TargetID: target, Relation: "knows", Weight: 0.9},
			},
		},
	}

	reader := &mockEntityReader{entities: []model.Entity{
		{ID: target, NamespaceID: nsID, Name: "Bob", EntityType: "person"},
	}}

	out := buildMCPRecallResponse(context.Background(), reader, resp, []uuid.UUID{nsID}, projectionOpts{})

	if len(out.Graph.Entities) != 2 {
		t.Errorf("expected 2 entities (anchor + resolved target), got %d", len(out.Graph.Entities))
	}
	if len(out.Graph.Relationships) != 1 {
		t.Errorf("expected 1 relationship to survive, got %d", len(out.Graph.Relationships))
	}
	assertNoOrphanRelationships(t, out.Graph)
}

// TestBuildMCPRecallResponse_PrunesUnresolvableOrphans confirms the secondary
// path: when the missing endpoint cannot be resolved (for example, it lives in
// a namespace the caller isn't permitted to see), the relationship is pruned
// rather than emitted with a dangling endpoint.
func TestBuildMCPRecallResponse_PrunesUnresolvableOrphans(t *testing.T) {
	allowedNS := uuid.New()
	otherNS := uuid.New()
	anchor := uuid.New()
	target := uuid.New()

	resp := &service.RecallResponse{
		Memories: []service.RecallResult{},
		Graph: service.RecallGraph{
			Entities: []service.RecallEntity{
				{ID: anchor, Name: "Alice", EntityType: "person"},
			},
			Relationships: []service.RecallRelationship{
				{ID: uuid.New(), SourceID: anchor, TargetID: target, Relation: "knows", Weight: 0.9},
			},
		},
	}

	// Target exists but lives outside the allowed namespace set — must be
	// filtered out by the projector and the relationship pruned.
	reader := &mockEntityReader{entities: []model.Entity{
		{ID: target, NamespaceID: otherNS, Name: "Bob", EntityType: "person"},
	}}

	out := buildMCPRecallResponse(context.Background(), reader, resp, []uuid.UUID{allowedNS}, projectionOpts{})

	if len(out.Graph.Entities) != 1 {
		t.Errorf("expected only the anchor entity (target out of scope), got %d", len(out.Graph.Entities))
	}
	if len(out.Graph.Relationships) != 0 {
		t.Errorf("expected the orphan relationship to be pruned, got %d", len(out.Graph.Relationships))
	}
	assertNoOrphanRelationships(t, out.Graph)
}

// TestBuildMCPRecallResponse_StripsBookkeepingMetadata confirms that
// source_memory_ids are hoisted to a typed top-level derived_from field and
// that the dream-lineage and audit-stamp keys (novelty audit, contradiction
// check, paraphrase dedup) are stripped from emitted metadata, while
// user-supplied keys pass through. This is the drift catcher: if a writer
// renames a bookkeeping key without updating bookkeepingMetaKeys, the rename
// surfaces here as a bookkeeping field that fails the test.
func TestBuildMCPRecallResponse_StripsBookkeepingMetadata(t *testing.T) {
	srcA := uuid.New()
	srcB := uuid.New()
	rawMeta := json.RawMessage(fmt.Sprintf(
		`{"dream_cycle_id":"%s","source_memory_ids":["%s","%s"],`+
			`"contradictions_checked_at":"2026-04-26T09:43:17Z",`+
			`"novelty_audited_at":"2026-04-26T09:43:17Z",`+
			`"novelty_audit_reason":"orphan_no_sources",`+
			`"low_novelty":true,"low_novelty_reason":"orphan_no_sources",`+
			`"paraphrase_checked_at":"2026-04-26T09:43:17Z",`+
			`"user_key":"keep me"}`,
		uuid.New(), srcA, srcB,
	))
	resp := &service.RecallResponse{
		Memories: []service.RecallResult{
			{ID: uuid.New(), Content: "audited memory", Metadata: rawMeta},
		},
	}

	out := buildMCPRecallResponse(context.Background(), &mockEntityReader{}, resp, nil, projectionOpts{})

	if len(out.Memories) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(out.Memories))
	}
	got := out.Memories[0]

	if len(got.DerivedFrom) != 2 {
		t.Fatalf("expected derived_from of length 2, got %v", got.DerivedFrom)
	}
	want := map[uuid.UUID]bool{srcA: false, srcB: false}
	for _, id := range got.DerivedFrom {
		if _, ok := want[id]; !ok {
			t.Errorf("unexpected derived_from id: %s", id)
		}
		want[id] = true
	}
	for id, seen := range want {
		if !seen {
			t.Errorf("missing derived_from id: %s", id)
		}
	}

	if got.Metadata == nil {
		t.Fatal("expected user-supplied metadata to survive (user_key)")
	}
	var parsed map[string]any
	if err := json.Unmarshal(got.Metadata, &parsed); err != nil {
		t.Fatalf("residual metadata not valid JSON: %v", err)
	}

	stripped := []string{
		"dream_cycle_id", "source_memory_ids",
		"contradictions_checked_at", "novelty_audited_at",
		"novelty_audit_reason", "low_novelty", "low_novelty_reason",
		"paraphrase_checked_at",
	}
	for _, k := range stripped {
		if _, ok := parsed[k]; ok {
			t.Errorf("expected %s stripped from residual metadata, but it was present", k)
		}
	}
	if parsed["user_key"] != "keep me" {
		t.Errorf("expected user_key preserved, got %v", parsed["user_key"])
	}
}

// TestBuildMCPRecallResponse_IncludeLowNovelty pairs with the strip drift
// catcher: when projectionOpts.IncludeLowNovelty=true, low_novelty and
// low_novelty_reason MUST survive the strip (so the caller knows why a
// dream was demoted), while the other audit-stamp keys stay stripped (those
// are exposed only by include_audit on memory_get).
func TestBuildMCPRecallResponse_IncludeLowNovelty(t *testing.T) {
	rawMeta := json.RawMessage(
		`{"low_novelty":true,"low_novelty_reason":"orphan_no_sources",` +
			`"novelty_audited_at":"2026-04-26T09:43:17Z",` +
			`"novelty_audit_reason":"orphan_no_sources",` +
			`"contradictions_checked_at":"2026-04-26T09:43:17Z",` +
			`"paraphrase_checked_at":"2026-04-26T09:43:17Z",` +
			`"user_key":"keep me"}`,
	)
	resp := &service.RecallResponse{
		Memories: []service.RecallResult{
			{ID: uuid.New(), Content: "demoted dream", Metadata: rawMeta},
		},
	}

	out := buildMCPRecallResponse(context.Background(), &mockEntityReader{}, resp, nil, projectionOpts{IncludeLowNovelty: true})

	if len(out.Memories) != 1 || out.Memories[0].Metadata == nil {
		t.Fatalf("expected 1 memory with metadata; got %+v", out.Memories)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out.Memories[0].Metadata, &parsed); err != nil {
		t.Fatalf("residual metadata not valid JSON: %v", err)
	}
	if v, ok := parsed["low_novelty"].(bool); !ok || !v {
		t.Errorf("expected low_novelty=true to survive include_low_novelty=true; got %v", parsed["low_novelty"])
	}
	if v, _ := parsed["low_novelty_reason"].(string); v != "orphan_no_sources" {
		t.Errorf("expected low_novelty_reason preserved; got %q", v)
	}
	for _, k := range []string{"novelty_audited_at", "novelty_audit_reason", "contradictions_checked_at", "paraphrase_checked_at"} {
		if _, ok := parsed[k]; ok {
			t.Errorf("audit-stamp key %s leaked when only include_low_novelty=true was set; that's include_audit's job", k)
		}
	}
	if parsed["user_key"] != "keep me" {
		t.Errorf("expected user_key preserved; got %v", parsed["user_key"])
	}
}

// TestBuildMCPRecallResponse_FixtureShape pins the structural improvements
// the projection makes on a fixture sized like the recall that motivated this
// work (10 memories, a small anchor entity set, and a relationship set that
// references several unseen endpoints — the orphan case).
//
// The byte reduction is intentionally not asserted as a strict percentage:
// when orphans are fully resolvable the projector trades freed bytes back to
// surface useful entity rows, which is the explicit goal ("preserve valuable
// data"). We log the delta for visibility and assert the invariants that
// matter — no orphans, no internal fields, derived_from hoisted.
func TestBuildMCPRecallResponse_FixtureShape(t *testing.T) {
	nsID := uuid.New()
	mems := make([]service.RecallResult, 10)
	for i := range mems {
		sim := 0.7
		mems[i] = service.RecallResult{
			ID:          uuid.New(),
			ProjectID:   uuid.New(),
			ProjectSlug: "fixture",
			Path:        "users/" + uuid.NewString() + "/projects/" + uuid.NewString() + "/fixture",
			Content:     "fixture content " + fmt.Sprint(i),
			Tags:        []string{"alpha", "beta"},
			Score:       0.5,
			Similarity:  &sim,
			Confidence:  1.0,
			AccessCount: 3,
			Enriched:    true,
			Metadata: json.RawMessage(
				`{"dream_cycle_id":"` + uuid.NewString() + `","source_memory_ids":["` + uuid.NewString() + `"]}`,
			),
		}
	}

	anchor := uuid.New()
	entities := []service.RecallEntity{{ID: anchor, Name: "Anchor", EntityType: "concept"}}

	rels := make([]service.RecallRelationship, 30)
	missingTargets := make([]uuid.UUID, len(rels))
	for i := range rels {
		missingTargets[i] = uuid.New()
		rels[i] = service.RecallRelationship{
			ID:       uuid.New(),
			SourceID: anchor,
			TargetID: missingTargets[i],
			Relation: "related_to",
			Weight:   0.85,
		}
	}

	resp := &service.RecallResponse{
		Memories:      mems,
		Graph:         service.RecallGraph{Entities: entities, Relationships: rels},
		TotalSearched: 60,
		LatencyMs:     427,
	}

	rawBefore, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal service response: %v", err)
	}

	mockEntities := make([]model.Entity, len(missingTargets))
	for i, id := range missingTargets {
		mockEntities[i] = model.Entity{ID: id, NamespaceID: nsID, Name: "Target" + fmt.Sprint(i), EntityType: "concept"}
	}
	out := buildMCPRecallResponse(context.Background(), &mockEntityReader{entities: mockEntities}, resp, []uuid.UUID{nsID}, projectionOpts{})

	rawAfter, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal mcp response: %v", err)
	}

	t.Logf("recall payload before=%d after=%d delta=%+d (%.1f%%)",
		len(rawBefore), len(rawAfter), len(rawAfter)-len(rawBefore),
		100*(1.0-float64(len(rawAfter))/float64(len(rawBefore))))

	// Invariants — these are the actual goals.
	assertNoOrphanRelationships(t, out.Graph)
	for _, m := range out.Memories {
		if len(m.DerivedFrom) == 0 {
			t.Errorf("expected derived_from hoisted for memory %s", m.ID)
		}
	}
	// Internal fields must not appear in the serialized JSON. Audit-stamp keys
	// are written by the dreaming pipeline; the projector strips them via
	// bookkeepingMetaKeys, and this list is the drift catcher.
	bannedKeys := []string{
		`"path"`, `"project_id"`, `"similarity"`, `"confidence"`, `"access_count"`,
		`"enriched"`, `"shared_from"`, `"total_searched"`,
		`"dream_cycle_id"`, `"source_memory_ids"`,
		`"contradictions_checked_at"`, `"novelty_audited_at"`, `"novelty_audit_reason"`,
		`"low_novelty"`, `"low_novelty_reason"`, `"paraphrase_checked_at"`,
	}
	body := string(rawAfter)
	for _, k := range bannedKeys {
		if strings.Contains(body, k) {
			t.Errorf("banned key %s leaked into MCP recall payload", k)
		}
	}
	// All 30 missing targets resolved, plus the anchor: 31 entities total.
	if len(out.Graph.Entities) != 31 {
		t.Errorf("expected 31 entities (1 anchor + 30 resolved), got %d", len(out.Graph.Entities))
	}
	if len(out.Graph.Relationships) != 30 {
		t.Errorf("expected all 30 relationships preserved, got %d", len(out.Graph.Relationships))
	}
}

// TestBuildMCPRecallResponse_FixtureShape_PrunedFallback covers the worst-case
// path: when orphan endpoints can't be resolved (out-of-scope or storage
// error), the projector prunes rather than emits dangling references. This
// is where the byte reduction is at its maximum.
func TestBuildMCPRecallResponse_FixtureShape_PrunedFallback(t *testing.T) {
	mems := make([]service.RecallResult, 10)
	for i := range mems {
		mems[i] = service.RecallResult{
			ID:      uuid.New(),
			Content: "x",
		}
	}
	anchor := uuid.New()
	rels := make([]service.RecallRelationship, 30)
	for i := range rels {
		rels[i] = service.RecallRelationship{
			ID: uuid.New(), SourceID: anchor, TargetID: uuid.New(),
			Relation: "related_to", Weight: 0.85,
		}
	}
	resp := &service.RecallResponse{
		Memories: mems,
		Graph: service.RecallGraph{
			Entities:      []service.RecallEntity{{ID: anchor, Name: "Anchor", EntityType: "concept"}},
			Relationships: rels,
		},
	}

	rawBefore, _ := json.Marshal(resp)
	// EntityReader returns no rows — orphans are pruned.
	out := buildMCPRecallResponse(context.Background(), &mockEntityReader{}, resp, []uuid.UUID{uuid.New()}, projectionOpts{})
	rawAfter, _ := json.Marshal(out)

	t.Logf("pruned fallback: before=%d after=%d (%.1f%% reduction)",
		len(rawBefore), len(rawAfter),
		100*(1.0-float64(len(rawAfter))/float64(len(rawBefore))))

	assertNoOrphanRelationships(t, out.Graph)
	if len(out.Graph.Relationships) != 0 {
		t.Errorf("expected unresolvable relationships pruned, got %d", len(out.Graph.Relationships))
	}
	if len(out.Graph.Entities) != 1 {
		t.Errorf("expected only anchor entity remaining, got %d", len(out.Graph.Entities))
	}
}

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

