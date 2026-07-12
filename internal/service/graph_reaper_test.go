package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeRelReaper records calls and serves scripted results for the relationship
// surface of the GraphReaper.
type fakeRelReaper struct {
	bySourceCalls   []uuid.UUID
	bySourceReturn  []uuid.UUID
	lostBatches     []int64       // successive DeleteByLostProvenance delete counts
	lostEndpoints   [][]uuid.UUID // successive DeleteByLostProvenance endpoint ids
	lostBatchCalls  int
	lostCountReturn int64
}

func (f *fakeRelReaper) DeleteBySourceMemory(_ context.Context, _, memoryID uuid.UUID) ([]uuid.UUID, error) {
	f.bySourceCalls = append(f.bySourceCalls, memoryID)
	return f.bySourceReturn, nil
}

func (f *fakeRelReaper) DeleteByLostProvenance(_ context.Context, _ int) ([]uuid.UUID, int64, error) {
	i := f.lostBatchCalls
	f.lostBatchCalls++
	var eps []uuid.UUID
	if i < len(f.lostEndpoints) {
		eps = f.lostEndpoints[i]
	}
	if i < len(f.lostBatches) {
		return eps, f.lostBatches[i], nil
	}
	return nil, 0, nil
}

func (f *fakeRelReaper) CountLostProvenance(_ context.Context) (int64, error) {
	return f.lostCountReturn, nil
}

// fakeEntityRecomputer records the entity-id scopes recompute was called with.
type fakeEntityRecomputer struct {
	calls [][]uuid.UUID
}

func (f *fakeEntityRecomputer) RecomputeMentionCounts(_ context.Context, ids []uuid.UUID) (int64, error) {
	f.calls = append(f.calls, ids)
	return int64(len(ids)), nil
}

func TestGraphReaper_ReapMemoryFootprint(t *testing.T) {
	ctx := context.Background()
	e1, e2 := uuid.New(), uuid.New()
	rel := &fakeRelReaper{bySourceReturn: []uuid.UUID{e1, e2}}
	ent := &fakeEntityRecomputer{}
	r := NewGraphReaper(rel, ent)

	mem := uuid.New()
	n, err := r.ReapMemoryFootprint(ctx, uuid.New(), mem)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 affected entities, got %d", n)
	}
	if len(rel.bySourceCalls) != 1 || rel.bySourceCalls[0] != mem {
		t.Fatalf("DeleteBySourceMemory not called with the memory id: %v", rel.bySourceCalls)
	}
	// Recompute must be scoped to exactly the reaped edges' endpoints.
	if len(ent.calls) != 1 || len(ent.calls[0]) != 2 {
		t.Fatalf("expected one scoped recompute over 2 entities, got %v", ent.calls)
	}
}

func TestGraphReaper_ReapMemoryFootprint_NoEdges_SkipsRecompute(t *testing.T) {
	ctx := context.Background()
	rel := &fakeRelReaper{bySourceReturn: nil}
	ent := &fakeEntityRecomputer{}
	r := NewGraphReaper(rel, ent)

	if _, err := r.ReapMemoryFootprint(ctx, uuid.New(), uuid.New()); err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(ent.calls) != 0 {
		t.Fatalf("recompute should be skipped when no edges were reaped, got %v", ent.calls)
	}
}

func TestGraphReaper_ReapLostProvenance_LoopsThenRecomputesEndpoints(t *testing.T) {
	ctx := context.Background()
	e1, e2, e3 := uuid.New(), uuid.New(), uuid.New()
	// Two full batches then a short batch ends the loop; endpoints overlap across
	// batches so the recompute scope must be the deduped union, not the sum.
	rel := &fakeRelReaper{
		lostBatches:   []int64{lostProvenanceBatch, lostProvenanceBatch, 7},
		lostEndpoints: [][]uuid.UUID{{e1, e2}, {e2, e3}, {e3}},
	}
	ent := &fakeEntityRecomputer{}
	r := NewGraphReaper(rel, ent)

	total, err := r.ReapLostProvenance(ctx)
	if err != nil {
		t.Fatalf("reap lost provenance: %v", err)
	}
	want := int64(lostProvenanceBatch*2 + 7)
	if total != want {
		t.Fatalf("expected %d reaped, got %d", want, total)
	}
	if rel.lostBatchCalls != 3 {
		t.Fatalf("expected 3 batch calls, got %d", rel.lostBatchCalls)
	}
	// Recompute runs once, scoped to the deduped endpoint union {e1,e2,e3}: not
	// the whole table (nil) and not with duplicates.
	if len(ent.calls) != 1 {
		t.Fatalf("expected one recompute call, got %d: %v", len(ent.calls), ent.calls)
	}
	got := ent.calls[0]
	if got == nil {
		t.Fatalf("recompute must be scoped to endpoints, not global (nil)")
	}
	if len(got) != 3 {
		t.Fatalf("expected recompute scoped to 3 deduped endpoints, got %d: %v", len(got), got)
	}
	seen := map[uuid.UUID]bool{}
	for _, id := range got {
		seen[id] = true
	}
	for _, wantID := range []uuid.UUID{e1, e2, e3} {
		if !seen[wantID] {
			t.Fatalf("recompute scope missing endpoint %s: %v", wantID, got)
		}
	}
}

func TestGraphReaper_ReapLostProvenance_NothingToReap(t *testing.T) {
	ctx := context.Background()
	rel := &fakeRelReaper{lostBatches: []int64{0}}
	ent := &fakeEntityRecomputer{}
	r := NewGraphReaper(rel, ent)

	total, err := r.ReapLostProvenance(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if total != 0 {
		t.Fatalf("expected 0 reaped, got %d", total)
	}
	if len(ent.calls) != 0 {
		t.Fatalf("recompute must be skipped when nothing was reaped, got %v", ent.calls)
	}
}

// fakeGraphPruner serves scripted dangling/orphan results.
type fakeGraphPruner struct {
	dangling int64
	orphans  []uuid.UUID
}

func (f *fakeGraphPruner) DeleteDanglingRelationships(_ context.Context) (int64, error) {
	return f.dangling, nil
}

func (f *fakeGraphPruner) DeleteOrphanedEntities(_ context.Context, _ time.Time) ([]uuid.UUID, error) {
	return f.orphans, nil
}

func TestLifecycleService_RepairGraph(t *testing.T) {
	ctx := context.Background()
	rel := &fakeRelReaper{
		lostBatches:   []int64{12},
		lostEndpoints: [][]uuid.UUID{{uuid.New(), uuid.New()}},
	}
	ent := &fakeEntityRecomputer{}
	reaper := NewGraphReaper(rel, ent)
	pruner := &fakeGraphPruner{dangling: 3, orphans: []uuid.UUID{uuid.New(), uuid.New()}}

	// Pin OrphanGrace so resolveOrphanGrace does not dereference a nil settings.
	svc := NewLifecycleService(nil, nil, pruner, LifecycleConfig{OrphanGrace: time.Hour}, nil).
		WithGraphReaper(reaper)

	res, err := svc.RepairGraph(ctx)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if res.RelationshipsReaped != 12 {
		t.Fatalf("RelationshipsReaped = %d, want 12", res.RelationshipsReaped)
	}
	if res.DanglingDeleted != 3 {
		t.Fatalf("DanglingDeleted = %d, want 3", res.DanglingDeleted)
	}
	if res.OrphanedEntities != 2 {
		t.Fatalf("OrphanedEntities = %d, want 2", res.OrphanedEntities)
	}
	// The scoped reap recompute (2 endpoints) is followed by the operator-only
	// full re-normalization (nil scope). The periodic sweep does only the first;
	// RepairGraph is distinguished by ending with the whole-graph recompute.
	if len(ent.calls) != 2 {
		t.Fatalf("expected scoped reap recompute then full recompute (2 calls), got %d: %v",
			len(ent.calls), ent.calls)
	}
	if len(ent.calls[0]) != 2 {
		t.Fatalf("first recompute must be scoped to the 2 reaped endpoints, got %v", ent.calls[0])
	}
	if ent.calls[1] != nil {
		t.Fatalf("RepairGraph must end with a full (nil) recompute, got %v", ent.calls[1])
	}
}

func TestLifecycleService_GraphHealthStatus(t *testing.T) {
	ctx := context.Background()
	rel := &fakeRelReaper{lostCountReturn: 25923}
	reaper := NewGraphReaper(rel, &fakeEntityRecomputer{})
	svc := NewLifecycleService(nil, nil, nil, LifecycleConfig{}, nil).WithGraphReaper(reaper)

	h, err := svc.GraphHealthStatus(ctx)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if h.LostProvenanceEdges != 25923 {
		t.Fatalf("LostProvenanceEdges = %d, want 25923", h.LostProvenanceEdges)
	}
}

func TestLifecycleService_GraphHealthStatus_NoReaper(t *testing.T) {
	svc := NewLifecycleService(nil, nil, nil, LifecycleConfig{}, nil)
	h, err := svc.GraphHealthStatus(context.Background())
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if h.LostProvenanceEdges != 0 {
		t.Fatalf("expected zero health with no reaper, got %d", h.LostProvenanceEdges)
	}
}
