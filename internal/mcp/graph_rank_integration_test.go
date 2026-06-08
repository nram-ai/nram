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

// splitEntityReader separates the seed-resolution surface (SearchEntities, used
// by the graph tool to find seeds) from the orphan-resolution surface (GetBatch,
// used by resolveGraphOrphans to fold in edge endpoints). The shared
// mockEntityReader returns the same set for both, which would make every node a
// seed; this mock lets a test hold seeds and orphan hubs distinct so proximity
// ranking is observable.
type splitEntityReader struct {
	search []model.Entity // returned by SearchEntities (the seeds)
	all    []model.Entity // resolvable by GetBatch (seeds + orphan endpoints)
}

func (m *splitEntityReader) SearchEntities(_ context.Context, _ uuid.UUID, _ string, _ string, _ int) ([]model.Entity, error) {
	return m.search, nil
}
func (m *splitEntityReader) FindBySimilarity(_ context.Context, _ uuid.UUID, _ string, _ string, _ int) ([]model.Entity, error) {
	return m.search, nil
}
func (m *splitEntityReader) FindByAlias(_ context.Context, _ uuid.UUID, _ string) ([]model.Entity, error) {
	return m.all, nil
}
func (m *splitEntityReader) ListByNamespace(_ context.Context, _ uuid.UUID) ([]model.Entity, error) {
	return m.all, nil
}
func (m *splitEntityReader) GetBatch(_ context.Context, ids []uuid.UUID, _ []uuid.UUID) ([]model.Entity, error) {
	want := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	out := make([]model.Entity, 0, len(ids))
	for _, e := range m.all {
		if _, ok := want[e.ID]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

// aliveMemoryLister implements MemoryLister, returning the configured memories
// as "alive" (not deleted, not superseded) from GetBatch so the graph tool's
// provenance filter keeps their edges. The list/count methods are unused by the
// graph tool and return empty.
type aliveMemoryLister struct{ mems []model.Memory }

func (m *aliveMemoryLister) ListByNamespaceFiltered(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, _, _ int) ([]model.Memory, error) {
	return nil, nil
}
func (m *aliveMemoryLister) CountByNamespaceFiltered(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters) (int, error) {
	return 0, nil
}
func (m *aliveMemoryLister) GetBatch(_ context.Context, ids []uuid.UUID, _ []uuid.UUID) ([]model.Memory, error) {
	out := make([]model.Memory, 0, len(ids))
	for _, id := range ids {
		for i := range m.mems {
			if m.mems[i].ID == id {
				out = append(out, m.mems[i])
			}
		}
	}
	return out, nil
}
func (m *aliveMemoryLister) ListByNamespaceFramingOrder(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return m.mems, nil
}

// These tests exercise the REAL recall projection wiring (buildMCPRecallResponse
// -> seed capture -> dedupGraphRelationships -> rankGraphSlice), not the ranking
// helpers in isolation, so they cover the plan's "live recall observation"
// verification step in a reproducible form.

// TestBuildMCPRecallResponse_ProximityRankingTracksSeed proves the inline graph
// slice ranks by proximity to the query's seed, not global salience: a low-
// mention seed outranks high-mention hub nodes that are farther away, and a
// different seed reorders the slice.
func TestBuildMCPRecallResponse_ProximityRankingTracksSeed(t *testing.T) {
	nsID := uuid.New()
	s, a, b := uuid.New(), uuid.New(), uuid.New()

	// Orphan endpoints + the seed live in the reader so backfillMentionCounts and
	// resolveGraphOrphans can fold them in. A and B are namespace hubs (high
	// mention) two and three... here one and two hops from the seed.
	reader := &mockEntityReader{entities: []model.Entity{
		{ID: s, NamespaceID: nsID, Name: "Seed", EntityType: "concept", MentionCount: 1},
		{ID: a, NamespaceID: nsID, Name: "HubA", EntityType: "concept", MentionCount: 100},
		{ID: b, NamespaceID: nsID, Name: "HubB", EntityType: "concept", MentionCount: 500},
	}}

	// Chain seed -> A -> B. resp.Graph.Entities is seed-only (the service
	// contract); A and B arrive as orphan endpoints of the edges.
	mkResp := func(seed uuid.UUID) *service.RecallResponse {
		return &service.RecallResponse{
			Graph: service.RecallGraph{
				Entities: []service.RecallEntity{{ID: seed, Name: "Seed", EntityType: "concept"}},
				Relationships: []service.RecallRelationship{
					{ID: uuid.New(), SourceID: s, TargetID: a, Relation: "rel", Weight: 1},
					{ID: uuid.New(), SourceID: a, TargetID: b, Relation: "rel", Weight: 1},
				},
			},
		}
	}

	out := buildMCPRecallResponse(context.Background(), reader, mkResp(s), []uuid.UUID{nsID}, projectionOpts{})
	if len(out.Graph.Entities) != 3 {
		t.Fatalf("expected 3 entities, got %d", len(out.Graph.Entities))
	}
	if out.Graph.Entities[0].ID != s {
		t.Errorf("seed should rank first, got %s (mention=%d)", out.Graph.Entities[0].Name, out.Graph.Entities[0].MentionCount)
	}
	if out.Graph.Entities[2].ID != b {
		t.Errorf("hop-2 hub HubB (mention 500) should rank last, got %s", out.Graph.Entities[2].Name)
	}

	// Different seed -> different ordering. Seed B: hops B=0, A=1, S=2.
	outB := buildMCPRecallResponse(context.Background(), reader, mkResp(b), []uuid.UUID{nsID}, projectionOpts{})
	// resp.Graph.Entities for outB is seed=b; b must rank first now.
	if outB.Graph.Entities[0].ID != b {
		t.Errorf("under seed B, B should rank first, got %s", outB.Graph.Entities[0].Name)
	}
	if out.Graph.Entities[0].ID == outB.Graph.Entities[0].ID {
		t.Error("expected different prefixes for different seeds")
	}
}

// TestBuildMCPRecallResponse_DedupsRelationVariants proves relation-string
// variants collapse end-to-end through the projection: two formatting variants
// of the same edge become one canonical edge at max weight.
func TestBuildMCPRecallResponse_DedupsRelationVariants(t *testing.T) {
	nsID := uuid.New()
	s, a, b := uuid.New(), uuid.New(), uuid.New()
	reader := &mockEntityReader{entities: []model.Entity{
		{ID: s, NamespaceID: nsID, Name: "Seed", EntityType: "concept", MentionCount: 1},
		{ID: a, NamespaceID: nsID, Name: "A", EntityType: "concept", MentionCount: 2},
		{ID: b, NamespaceID: nsID, Name: "B", EntityType: "concept", MentionCount: 3},
	}}
	resp := &service.RecallResponse{
		Graph: service.RecallGraph{
			Entities: []service.RecallEntity{{ID: s, Name: "Seed", EntityType: "concept"}},
			Relationships: []service.RecallRelationship{
				{ID: uuid.New(), SourceID: s, TargetID: a, Relation: "related_to", Weight: 0.4},
				{ID: uuid.New(), SourceID: s, TargetID: a, Relation: "related to", Weight: 0.7},
				{ID: uuid.New(), SourceID: a, TargetID: b, Relation: "depends on", Weight: 0.5},
			},
		},
	}

	out := buildMCPRecallResponse(context.Background(), reader, resp, []uuid.UUID{nsID}, projectionOpts{})

	if len(out.Graph.Relationships) != 2 {
		t.Fatalf("expected 2 edges after variant collapse, got %d: %+v", len(out.Graph.Relationships), out.Graph.Relationships)
	}
	var merged *graphRelationship
	for i := range out.Graph.Relationships {
		r := &out.Graph.Relationships[i]
		if r.Relation == "related_to" {
			t.Errorf("non-canonical 'related_to' leaked into the slice")
		}
		if r.SourceID == s && r.TargetID == a {
			merged = r
		}
	}
	if merged == nil {
		t.Fatal("expected a merged seed->A edge")
	}
	if merged.Relation != "related to" {
		t.Errorf("merged edge relation = %q, want canonical 'related to'", merged.Relation)
	}
	if merged.Weight != 0.7 {
		t.Errorf("merged edge weight = %v, want max 0.7", merged.Weight)
	}
}

// TestHandleMemoryGraph_ProximityAndDedup exercises the graph() tool end-to-end
// (handleMemoryGraph -> seed capture -> dedup -> rankGraphSlice). The seed is the
// only SearchEntities hit; high-mention hubs arrive as orphan endpoints. The seed
// must rank first despite the hubs' mention counts, and relation variants must
// collapse.
func TestHandleMemoryGraph_ProximityAndDedup(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	s, a, b := uuid.New(), uuid.New(), uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	reader := &splitEntityReader{
		// Only the seed is a SearchEntities hit.
		search: []model.Entity{{ID: s, NamespaceID: nsID, Name: "Seed", EntityType: "concept", Canonical: "seed", MentionCount: 1}},
		// GetBatch resolves the orphan hubs (and the seed) with their mention counts.
		all: []model.Entity{
			{ID: s, NamespaceID: nsID, Name: "Seed", EntityType: "concept", Canonical: "seed", MentionCount: 1},
			{ID: a, NamespaceID: nsID, Name: "HubA", EntityType: "concept", Canonical: "huba", MentionCount: 100},
			{ID: b, NamespaceID: nsID, Name: "HubB", EntityType: "concept", Canonical: "hubb", MentionCount: 500},
		},
	}
	// Chain s -> a -> b, with a duplicate variant edge on s -> a. Each edge is
	// sourced from a live memory so the provenance filter keeps it (production
	// enrichment always sets source_memory; a nil pointer means the memory was
	// deleted and the edge is now dropped).
	now := time.Now()
	memID := uuid.New()
	traverser := &mockTraverser{rels: []model.Relationship{
		{ID: uuid.New(), SourceID: s, TargetID: a, Relation: "related_to", Weight: 1, ValidFrom: now, SourceMemory: &memID},
		{ID: uuid.New(), SourceID: s, TargetID: a, Relation: "related to", Weight: 1, ValidFrom: now, SourceMemory: &memID},
		{ID: uuid.New(), SourceID: a, TargetID: b, Relation: "rel", Weight: 1, ValidFrom: now, SourceMemory: &memID},
	}}

	deps := Dependencies{
		Backend:      storage.BackendSQLite,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{},
		EntityReader: reader,
		Traverser:    traverser,
		MemoryLister: &aliveMemoryLister{mems: []model.Memory{{ID: memID, NamespaceID: nsID}}},
	}
	srv := newTestServer(deps)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"entity": "Seed", "depth": float64(3)}

	result, err := handleMemoryGraph(buildAuthCtx(userID), srv, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	var resp graphResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("unmarshal graph response: %v", err)
	}

	// Proximity: seed ranks first, hop-2 hub B ranks last despite mention 500.
	if len(resp.Entities) != 3 {
		t.Fatalf("expected 3 entities, got %d", len(resp.Entities))
	}
	if resp.Entities[0].ID != s {
		t.Errorf("seed should rank first, got %s (mention=%d)", resp.Entities[0].Name, resp.Entities[0].MentionCount)
	}
	if resp.Entities[2].ID != b {
		t.Errorf("hop-2 hub HubB should rank last, got %s", resp.Entities[2].Name)
	}

	// Dedup: the two s->a variants collapse to one canonical edge; a->b remains.
	if len(resp.Relationships) != 2 {
		t.Fatalf("expected 2 edges after variant collapse, got %d: %+v", len(resp.Relationships), resp.Relationships)
	}
	for _, r := range resp.Relationships {
		if r.Relation == "related_to" {
			t.Errorf("non-canonical 'related_to' leaked into the graph slice")
		}
	}
}
