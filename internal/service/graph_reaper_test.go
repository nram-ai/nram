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
	lostBatches     []int64 // successive DeleteByLostProvenance returns
	lostBatchCalls  int
	lostCountReturn int64
}

func (f *fakeRelReaper) DeleteBySourceMemory(_ context.Context, _, memoryID uuid.UUID) ([]uuid.UUID, error) {
	f.bySourceCalls = append(f.bySourceCalls, memoryID)
	return f.bySourceReturn, nil
}

func (f *fakeRelReaper) DeleteByLostProvenance(_ context.Context, _ int) (int64, error) {
	i := f.lostBatchCalls
	f.lostBatchCalls++
	if i < len(f.lostBatches) {
		return f.lostBatches[i], nil
	}
	return 0, nil
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

func TestGraphReaper_ReapLostProvenance_LoopsThenRecomputesAll(t *testing.T) {
	ctx := context.Background()
	// Two full batches then a short batch ends the loop.
	rel := &fakeRelReaper{lostBatches: []int64{lostProvenanceBatch, lostProvenanceBatch, 7}}
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
	// A global recompute (nil scope) runs once after reaping.
	if len(ent.calls) != 1 || ent.calls[0] != nil {
		t.Fatalf("expected one global (nil) recompute, got %v", ent.calls)
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
	rel := &fakeRelReaper{lostBatches: []int64{12}}
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
