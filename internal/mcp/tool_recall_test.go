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
	"github.com/nram-ai/nram/internal/recallview"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- mock types for recall tool tests ---

// newMockRecallSvc creates a mock RecallService that returns a fixed response.
// Since RecallService is a concrete type with unexported fields, we create a
// real instance with minimal mocks for the recall path.
func newMockRecallSvc() *service.RecallService {
	nsID := uuid.New()
	return service.NewRecallService(
		&mockMemoryReaderRecall{},
		&mockProjectLookup{project: &model.Project{ID: uuid.New(), NamespaceID: nsID}},
		&mockNamespaceLookup{ns: &model.Namespace{ID: nsID}},
		nil, // no vector search
		nil, // no entity reader
		nil, // no traverser
		nil, // no embed provider
	)
}

type mockMemoryReaderRecall struct{}

func (m *mockMemoryReaderRecall) GetByID(_ context.Context, id uuid.UUID, _ uuid.UUID) (*model.Memory, error) {
	return &model.Memory{ID: id}, nil
}

func (m *mockMemoryReaderRecall) GetBatch(_ context.Context, ids []uuid.UUID, _ []uuid.UUID) ([]model.Memory, error) {
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

// capturingRecallSvc records the RecallRequest the handler constructs so tests
// can assert aperture/flag wiring the concrete RecallService would otherwise
// absorb. Implements the RecallRunner seam.
type capturingRecallSvc struct {
	captured *service.RecallRequest
}

func (c *capturingRecallSvc) Recall(_ context.Context, req *service.RecallRequest) (*service.RecallResponse, error) {
	c.captured = req
	return &service.RecallResponse{Memories: []service.RecallResult{}}, nil
}

func (c *capturingRecallSvc) ReinforceGraphEdgesAsync(_ []service.RelationshipRef) {}

// TestHandleMemoryRecall_OriginDemotionByScope is the fail-before / pass-after
// guard for the origin-demotion fix: a no-project recall treats global as a
// structural seed and must set DemotePrimaryOrigin=true (so global does not
// out-boost the joined about_me tier on origin alone), while a project-scoped
// recall targets a deliberately chosen focus and must keep its origin boost.
func TestHandleMemoryRecall_OriginDemotionByScope(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}

	tests := []struct {
		name       string
		project    *model.Project
		args       map[string]any
		wantDemote bool
	}{
		{
			name:       "no project: global is a structural seed, demote origin",
			project:    &model.Project{ID: uuid.New(), NamespaceID: uuid.New(), Slug: model.ReservedProjectSlugGlobal},
			args:       map[string]any{"query": "anything"},
			wantDemote: true,
		},
		{
			name:       "project-scoped: chosen focus keeps its origin boost",
			project:    &model.Project{ID: uuid.New(), NamespaceID: uuid.New(), OwnerNamespaceID: nsID, Slug: "myproj"},
			args:       map[string]any{"query": "anything", "project": "myproj"},
			wantDemote: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spy := &capturingRecallSvc{}
			srv := newTestServer(Dependencies{
				Backend:       storage.BackendSQLite,
				UserRepo:      &mockUserRepoStore{user: user},
				ProjectRepo:   &mockProjectRepoStore{project: tt.project},
				NamespaceRepo: &mockNamespaceRepoStore{ns: &model.Namespace{ID: nsID, Path: "/user"}},
				Recall:        spy,
			})

			callReq := mcp.CallToolRequest{}
			callReq.Params.Name = "recall"
			callReq.Params.Arguments = tt.args

			result, err := handleMemoryRecall(buildAuthCtx(userID), srv, callReq)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.IsError {
				t.Fatalf("unexpected tool error: %v", result.Content)
			}
			if spy.captured == nil {
				t.Fatal("recall service not invoked")
			}
			if spy.captured.DemotePrimaryOrigin != tt.wantDemote {
				t.Errorf("DemotePrimaryOrigin = %v, want %v", spy.captured.DemotePrimaryOrigin, tt.wantDemote)
			}
			if spy.captured.ProjectID != tt.project.ID {
				t.Errorf("primary ProjectID = %s, want %s", spy.captured.ProjectID, tt.project.ID)
			}
		})
	}
}

// --- schema tests ---

// TestBuildMCPRecallResponse_SurfacesOrigin confirms the typed origin field is
// projected onto the MCP recall shape and serializes (it is the dream-synthesis
// discriminator the agent reads, so it must reach the wire).
func TestBuildMCPRecallResponse_SurfacesOrigin(t *testing.T) {
	resp := &service.RecallResponse{
		Memories: []service.RecallResult{
			{ID: uuid.New(), Content: "a dream", Origin: model.OriginDream, Tags: []string{}},
			{ID: uuid.New(), Content: "a user memory", Origin: model.OriginUser, Tags: []string{}},
		},
	}

	out := buildMCPRecallResponse(context.Background(), &mockEntityReader{}, resp, nil, projectionOpts{})

	if len(out.Memories) != 2 {
		t.Fatalf("expected 2 memories, got %d", len(out.Memories))
	}
	if out.Memories[0].Origin != model.OriginDream {
		t.Errorf("expected first memory origin %q, got %q", model.OriginDream, out.Memories[0].Origin)
	}
	if out.Memories[1].Origin != model.OriginUser {
		t.Errorf("expected second memory origin %q, got %q", model.OriginUser, out.Memories[1].Origin)
	}

	blob, err := json.Marshal(out.Memories[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(blob), `"origin":"dream"`) {
		t.Errorf("origin not serialized to the wire: %s", blob)
	}
}

// TestBuildMCPRecallResponse_PopulatesMentionCount confirms the projection
// backfills MentionCount onto the originally-discovered graph entities (the
// service layer drops it) so sortGraphBySignal can rank by it, and that the
// backfill is namespace-scoped: an entity whose record lives outside the
// allowed namespaces does not pick up a mention signal.
func TestBuildMCPRecallResponse_PopulatesMentionCount(t *testing.T) {
	nsID := uuid.New()
	otherNS := uuid.New()
	a, b, c := uuid.New(), uuid.New(), uuid.New()

	resp := &service.RecallResponse{
		Memories: []service.RecallResult{},
		Graph: service.RecallGraph{
			Entities: []service.RecallEntity{
				{ID: a, Name: "Alpha", EntityType: "concept"},
				{ID: b, Name: "Beta", EntityType: "concept"},
				{ID: c, Name: "Gamma", EntityType: "concept"},
			},
			Relationships: []service.RecallRelationship{},
		},
	}
	reader := &mockEntityReader{entities: []model.Entity{
		{ID: a, NamespaceID: nsID, Name: "Alpha", EntityType: "concept", MentionCount: 7},
		{ID: b, NamespaceID: nsID, Name: "Beta", EntityType: "concept", MentionCount: 3},
		{ID: c, NamespaceID: otherNS, Name: "Gamma", EntityType: "concept", MentionCount: 99},
	}}

	out := buildMCPRecallResponse(context.Background(), reader, resp, []uuid.UUID{nsID}, projectionOpts{})

	got := map[uuid.UUID]int{}
	for _, e := range out.Graph.Entities {
		got[e.ID] = e.MentionCount
	}
	if got[a] != 7 {
		t.Errorf("entity a MentionCount = %d, want 7", got[a])
	}
	if got[b] != 3 {
		t.Errorf("entity b MentionCount = %d, want 3", got[b])
	}
	if got[c] != 0 {
		t.Errorf("out-of-scope entity c MentionCount = %d, want 0 (namespace-filtered)", got[c])
	}
	// Signal sort: highest mention count first.
	if len(out.Graph.Entities) == 0 || out.Graph.Entities[0].ID != a {
		t.Errorf("expected highest-mention entity (a) sorted first; got %+v", out.Graph.Entities)
	}
}

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

	// Target exists but lives outside the allowed namespace set; must be
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
			`"consolidation_load_checked_at":"2026-04-26T09:43:17Z",`+
			`"reinforce_checked_at":"2026-04-26T09:43:17Z",`+
			`"consolidation_cluster_checked_at":"2026-04-26T09:43:17Z",`+
			`"consolidation_cluster_fingerprint":"abc123",`+
			`"ingestion_decision":"ADD","ingestion_decision_at":"2026-04-26T09:43:17Z",`+
			`"ingestion_target_id":"%s","ingestion_rationale":"new fact",`+
			`"ingestion_match_count":0,"ingestion_top_score":0.0,"ingestion_shadow_op":"none",`+
			`"migrated_from_global":true,"migration_date":"2026-05-24","original_global_id":"%s",`+
			`"user_key":"keep me"}`,
		uuid.New(), srcA, srcB, uuid.New(), uuid.New(),
	))
	resp := &service.RecallResponse{
		Memories: []service.RecallResult{
			{ID: uuid.New(), Content: "audited memory", Confidence: 0.42, Metadata: rawMeta},
		},
	}

	out := buildMCPRecallResponse(context.Background(), &mockEntityReader{}, resp, nil, projectionOpts{})

	if len(out.Memories) != 1 {
		t.Fatalf("expected 1 memory, got %d", len(out.Memories))
	}
	got := out.Memories[0]

	// The novelty bool and confidence are hoisted to typed fields, not left in
	// the metadata blob.
	if !got.LowNovelty {
		t.Errorf("expected low_novelty hoisted to typed field LowNovelty=true, got false")
	}
	if got.Confidence != 0.42 {
		t.Errorf("expected confidence surfaced as 0.42, got %v", got.Confidence)
	}

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
		// Newly stripped on the recall path (previously leaked).
		"consolidation_load_checked_at", "reinforce_checked_at",
		"consolidation_cluster_checked_at", "consolidation_cluster_fingerprint",
		"ingestion_decision", "ingestion_decision_at", "ingestion_target_id",
		"ingestion_rationale", "ingestion_match_count", "ingestion_top_score",
		"ingestion_shadow_op",
		"migrated_from_global", "migration_date", "original_global_id",
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
// catcher: the low_novelty bool is always hoisted to the typed LowNovelty
// field (and stripped from residual metadata). When
// projectionOpts.IncludeLowNovelty=true, the low_novelty_reason detail
// additionally survives in residual metadata (so the caller knows WHY a dream
// was demoted), while the other audit-stamp keys stay stripped (those are
// exposed only by include_audit on memory_get).
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
	if !out.Memories[0].LowNovelty {
		t.Errorf("expected low_novelty hoisted to typed field LowNovelty=true, got false")
	}
	var parsed map[string]any
	if err := json.Unmarshal(out.Memories[0].Metadata, &parsed); err != nil {
		t.Fatalf("residual metadata not valid JSON: %v", err)
	}
	if _, ok := parsed["low_novelty"]; ok {
		t.Errorf("expected low_novelty key stripped from residual (it is hoisted to LowNovelty); but it was present")
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
// references several unseen endpoints (the orphan case).
//
// The byte reduction is intentionally not asserted as a strict percentage:
// when orphans are fully resolvable the projector trades freed bytes back to
// surface useful entity rows, which is the explicit goal ("preserve valuable
// data"). We log the delta for visibility and assert the invariants that
// matter: no orphans, no internal fields, derived_from hoisted.
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

	// Invariants: these are the actual goals.
	assertNoOrphanRelationships(t, out.Graph)
	for _, m := range out.Memories {
		if len(m.DerivedFrom) == 0 {
			t.Errorf("expected derived_from hoisted for memory %s", m.ID)
		}
	}
	// Internal-only fields and audit bookkeeping must not appear in the
	// serialized JSON. confidence and low_novelty are NOT banned; they are
	// surfaced decision signals (typed top-level fields). The remaining
	// internal carrier fields (path/project_id/similarity/access_count/
	// enriched) are dropped from the wire, and the dreaming/enrichment audit
	// keys are stripped by recallview; this list is the drift catcher.
	bannedKeys := []string{
		`"path"`, `"project_id"`, `"similarity"`, `"access_count"`,
		`"enriched"`, `"total_searched"`,
		`"dream_cycle_id"`, `"source_memory_ids"`,
		`"contradictions_checked_at"`, `"novelty_audited_at"`, `"novelty_audit_reason"`,
		`"low_novelty_reason"`, `"paraphrase_checked_at"`,
		`"consolidation_load_checked_at"`, `"reinforce_checked_at"`,
		`"ingestion_decision"`, `"migrated_from_global"`,
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
	// EntityReader returns no rows; orphans are pruned.
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

// TestMemoryRecall_Schema_Postgres_GraphParams confirms graph_depth remains
// available (now capped server-side) and include_graph has been stripped from
// the MCP schema (the server always includes graph when the traverser is
// wired; clients no longer have a meaningless toggle to flip).
func TestMemoryRecall_Schema_Postgres_GraphParams(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)

	tools := srv.MCPServer().ListTools()
	st, ok := tools["recall"]
	if !ok {
		t.Fatal("recall tool not registered")
	}

	raw, _ := json.Marshal(st.Tool.InputSchema)
	schema := string(raw)

	if !containsField(schema, "graph_depth") {
		t.Error("expected graph_depth param to be present on Postgres backend")
	}
	if containsField(schema, "include_graph") {
		t.Error("include_graph was removed from MCP; should not appear in schema")
	}
}

func TestMemoryRecall_Schema_HasDiversifyByTagPrefix(t *testing.T) {
	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		deps := Dependencies{Backend: backend}
		srv := newTestServer(deps)
		tools := srv.MCPServer().ListTools()
		st, ok := tools["recall"]
		if !ok {
			t.Fatalf("backend %s: memory_recall tool not registered", backend)
		}
		raw, _ := json.Marshal(st.Tool.InputSchema)
		if !containsField(string(raw), "diversify_by_tag_prefix") {
			t.Errorf("backend %s: expected diversify_by_tag_prefix param in schema, got %s", backend, string(raw))
		}
	}
}

// TestMemoryRecall_Schema_LacksSimilarityThresholdFields confirms the
// similarity_threshold and similarity_threshold_mode knobs have been stripped
// from the MCP schema. They remain available on REST for tuning/debugging,
// but the LLM tool surface no longer exposes researcher's knobs.
func TestMemoryRecall_Schema_LacksSimilarityThresholdFields(t *testing.T) {
	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		deps := Dependencies{Backend: backend}
		srv := newTestServer(deps)
		tools := srv.MCPServer().ListTools()
		st, ok := tools["recall"]
		if !ok {
			t.Fatalf("backend %s: recall tool not registered", backend)
		}
		raw, _ := json.Marshal(st.Tool.InputSchema)
		schema := string(raw)
		if containsField(schema, "similarity_threshold") {
			t.Errorf("backend %s: similarity_threshold was removed from MCP; should not appear in schema: %s", backend, schema)
		}
		if containsField(schema, "similarity_threshold_mode") {
			t.Errorf("backend %s: similarity_threshold_mode was removed from MCP; should not appear in schema: %s", backend, schema)
		}
	}
}

// TestRecallGraphReserveFraction_NilSafe confirms the reserve-fraction resolver
// falls back to the registered default (0.15) when the SettingsService is nil,
// matching the mcpBudgetBytes nil-safe pattern, and that the registered default
// is what the handler expects.
func TestRecallGraphReserveFraction_NilSafe(t *testing.T) {
	if got := recallGraphReserveFraction(context.Background(), nil); got != 0.15 {
		t.Errorf("nil settings should resolve the default 0.15; got %v", got)
	}
	if d := service.GetDefaultFloat(service.SettingRecallGraphReserveFraction); d != 0.15 {
		t.Errorf("registered default = %v, want 0.15", d)
	}
}

// TestRecallHandlerTailSurfacesBalancedGraph is the integrated proof that the
// handler tail (buildMCPRecallResponse -> mcpBudgetBytes ->
// recallGraphReserveFraction -> packGraphToByteBudget -> wrapToolResult+
// newRecallReducer, exactly as handleMemoryRecall wires them) surfaces a
// balanced graph subset on a budget-busting recall, instead of dropping the
// graph wholesale. Memories remain present and the graph stays within its
// reserved slice.
func TestRecallHandlerTailSurfacesBalancedGraph(t *testing.T) {
	nsID := uuid.New()

	mems := make([]service.RecallResult, 25)
	for i := range mems {
		mems[i] = service.RecallResult{
			ID:      uuid.New(),
			Content: strings.Repeat("memory body ", 60), // ~720 chars each -> busts budget
			Tags:    []string{"a"},
		}
	}

	anchor := uuid.New()
	ents := []service.RecallEntity{{ID: anchor, Name: "Anchor", EntityType: "concept"}}
	rels := make([]service.RecallRelationship, 40)
	targets := make([]uuid.UUID, len(rels))
	for i := range rels {
		targets[i] = uuid.New()
		rels[i] = service.RecallRelationship{
			ID:       uuid.New(),
			SourceID: anchor,
			TargetID: targets[i],
			Relation: "related_to",
			Weight:   float64(40 - i),
		}
	}
	resp := &service.RecallResponse{
		Memories: mems,
		Graph:    service.RecallGraph{Entities: ents, Relationships: rels},
	}

	readerEnts := make([]model.Entity, 0, len(targets)+1)
	readerEnts = append(readerEnts, model.Entity{ID: anchor, NamespaceID: nsID, Name: "Anchor", EntityType: "concept", MentionCount: 100})
	for i, id := range targets {
		readerEnts = append(readerEnts, model.Entity{ID: id, NamespaceID: nsID, Name: fmt.Sprintf("T%d", i), EntityType: "concept", MentionCount: i})
	}
	reader := &mockEntityReader{entities: readerEnts}

	mcpResp := buildMCPRecallResponse(context.Background(), reader, resp, []uuid.UUID{nsID}, projectionOpts{})

	// Replicate the handler tail (handleMemoryRecall) with the real helpers.
	settings := newSettingsServiceWithMCPBudget(2000) // 2000 tokens -> 4000 bytes
	ctx := context.Background()
	budget := mcpBudgetBytes(ctx, settings)
	reserveBytes := int(float64(budget) * recallGraphReserveFraction(ctx, settings))
	keptE, keptR, sentinels := packGraphToByteBudget(mcpResp.Graph.Entities, mcpResp.Graph.Relationships, reserveBytes)
	mcpResp.Graph.Entities, mcpResp.Graph.Relationships = keptE, keptR
	graphPreTrimmed := len(sentinels) > 0
	if graphPreTrimmed {
		mcpResp.Truncated = &truncationInfo{Reason: "response_too_large", Dropped: sentinels, Hint: recallGraphTrimHint}
	}
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, mcpResp, newRecallReducer(mcpResp, graphPreTrimmed))
	if err != nil {
		t.Fatalf("wrapToolResult: %v", err)
	}

	var decoded mcpRecallResponse
	if err := json.Unmarshal([]byte(extractText(res)), &decoded); err != nil {
		t.Fatalf("integrated recall result is not valid JSON: %v", err)
	}
	if len(decoded.Graph.Entities) == 0 || len(decoded.Graph.Relationships) == 0 {
		t.Fatalf("integrated recall must surface a balanced graph; got entities=%d relationships=%d",
			len(decoded.Graph.Entities), len(decoded.Graph.Relationships))
	}
	if len(decoded.Memories) == 0 {
		t.Error("memories must still be present alongside the graph")
	}
	if decoded.Truncated == nil {
		t.Fatal("expected a _truncated envelope on this budget-busting recall")
	}
	// The graph must stay within its reserved slice, not crowd out memories.
	gb, _ := json.Marshal(decoded.Graph)
	if len(gb) > budget/2 {
		t.Errorf("graph (%d B) should stay within its reserve, well under budget/2=%d", len(gb), budget/2)
	}
}

// --- handler tests ---

func TestHandleMemoryRecall_NoHTTPRequest(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "recall"
	req.Params.Arguments = map[string]any{
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
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "recall"
	req.Params.Arguments = map[string]any{
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
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Name = "recall"
	req.Params.Arguments = map[string]any{}

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
	srv := newTestServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "recall"
	callReq.Params.Arguments = map[string]any{
		"query":   "find something",
		"project": "myproj",
		"limit":   float64(5),
		"tags":    []any{"important"},
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
	srv := newTestServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "recall"
	callReq.Params.Arguments = map[string]any{
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
	srv := newTestServer(deps)

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "recall"
	callReq.Params.Arguments = map[string]any{
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

// TestRecallTransportSymmetry is the acceptance guard for the locked
// decision: a recalled memory must be byte-identical across the REST and MCP
// transports. The MCP tool projects via buildMCPRecallResponse; the REST
// handler (internal/api/handler_recall.go) projects each memory via
// recallview.Project(m, opts), the exact call replayed here. Asserting the
// two marshaled per-memory objects are byte-equal proves the transports share
// one canonical shape and cannot drift apart.
func TestRecallTransportSymmetry(t *testing.T) {
	srcA := uuid.New()
	rawMeta := json.RawMessage(fmt.Sprintf(
		`{"dream_cycle_id":"%s","source_memory_ids":["%s"],`+
			`"low_novelty":true,"low_novelty_reason":"orphan_no_sources",`+
			`"consolidation_load_checked_at":"2026-04-26T09:43:17Z",`+
			`"ingestion_decision":"ADD","migrated_from_global":true,`+
			`"user_key":"keep me"}`,
		uuid.New(), srcA,
	))
	sim := 0.7
	res := service.RecallResult{
		ID:          uuid.New(),
		ProjectID:   uuid.New(),
		ProjectSlug: "fixture",
		Path:        "users/x/projects/y/fixture",
		Content:     "symmetry content",
		Tags:        []string{"alpha", "beta"},
		Source:      nil,
		Origin:      model.OriginDream,
		Score:       0.5,
		Similarity:  &sim,
		Confidence:  0.81,
		AccessCount: 3,
		Enriched:    true,
		Metadata:    rawMeta,
	}
	resp := &service.RecallResponse{Memories: []service.RecallResult{res}}

	// MCP transport path.
	mcpOut := buildMCPRecallResponse(context.Background(), &mockEntityReader{}, resp, nil, projectionOpts{})
	if len(mcpOut.Memories) != 1 {
		t.Fatalf("expected 1 mcp memory, got %d", len(mcpOut.Memories))
	}
	mcpBytes, err := json.Marshal(mcpOut.Memories[0])
	if err != nil {
		t.Fatalf("marshal mcp memory: %v", err)
	}

	// REST transport path: handler_recall.go calls recallview.Project per memory.
	restMem := recallview.Project(res, recallview.Options{})
	restBytes, err := json.Marshal(restMem)
	if err != nil {
		t.Fatalf("marshal rest memory: %v", err)
	}

	if string(mcpBytes) != string(restBytes) {
		t.Errorf("per-memory wire shape diverged across transports\n  mcp:  %s\n  rest: %s", mcpBytes, restBytes)
	}

	// And confirm the canonical shape actually slimmed: decision signals present,
	// internal carrier + audit bookkeeping gone.
	body := string(mcpBytes)
	for _, want := range []string{`"confidence":0.81`, `"low_novelty":true`, `"origin":"dream"`, `"derived_from"`} {
		if !strings.Contains(body, want) {
			t.Errorf("expected %s in canonical memory; got %s", want, body)
		}
	}
	for _, banned := range []string{`"similarity"`, `"access_count"`, `"enriched"`, `"path"`, `"project_id"`, `"ingestion_decision"`, `"migrated_from_global"`, `"consolidation_load_checked_at"`} {
		if strings.Contains(body, banned) {
			t.Errorf("expected %s absent from canonical memory; got %s", banned, body)
		}
	}
}
