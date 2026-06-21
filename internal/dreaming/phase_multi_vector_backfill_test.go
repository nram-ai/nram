package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// fakeMVLister returns a fixed candidate list and records the limit the phase
// requested (so tests can assert the cap+1 residual probe).
type fakeMVLister struct {
	ids       []uuid.UUID
	lastLimit int
	calls     int
	err       error
}

func (f *fakeMVLister) ListMultiVectorBackfillCandidates(_ context.Context, _ []uuid.UUID, limit int) ([]storage.BackfillCandidate, error) {
	f.calls++
	f.lastLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	out := f.ids
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	cands := make([]storage.BackfillCandidate, len(out))
	for i, id := range out {
		cands[i] = storage.BackfillCandidate{ID: id}
	}
	return cands, nil
}

// mvSettings ties the backfill switch and the global multi-vector switch
// together for the shared cases; the dedicated gate tests vary them apart.
func mvSettings(enabled bool, capPerCycle int) *staticDreamSettings {
	flag := "false"
	if enabled {
		flag = "true"
	}
	return &staticDreamSettings{
		values: map[string]string{
			service.SettingDreamMultiVectorBackfillEnabled: flag,
			service.SettingMultiVectorEnabled:              flag,
		},
		ints: map[string]int{
			service.SettingDreamMultiVectorBackfillCapPerCycle: capPerCycle,
		},
	}
}

func mvTestCycle(ns uuid.UUID) *model.DreamCycle {
	return &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: ns}
}

// jobHasMultiVectorMarker reports whether a job's StepsCompleted carries the
// facet-only sentinel the enrichment worker routes on.
func jobHasMultiVectorMarker(t *testing.T, j model.EnrichmentJob) bool {
	t.Helper()
	if len(j.StepsCompleted) == 0 {
		return false
	}
	var steps []string
	if err := json.Unmarshal(j.StepsCompleted, &steps); err != nil {
		t.Fatalf("unmarshal StepsCompleted: %v", err)
	}
	return slices.Contains(steps, model.JobMarkerOnlyMultiVector)
}

// Happy path: every candidate gets a pending facet-only job (marker seeded) for
// the cycle's namespace, the lister is probed at cap+1, and no residual fires.
func TestMultiVectorBackfillPhase_EnqueuesCandidates(t *testing.T) {
	ns := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	lister := &fakeMVLister{ids: ids}
	queue := &enqueueRecorder{}

	phase := NewMultiVectorBackfillPhase(lister, queue, mvSettings(true, 200))
	cycle := mvTestCycle(ns)
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	result, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Errorf("residual must be false when all candidates fit under cap")
	}
	if lister.lastLimit != 201 {
		t.Errorf("lister limit = %d, want cap+1 (201) for the residual probe", lister.lastLimit)
	}
	jobs := queue.snapshot()
	if len(jobs) != len(ids) {
		t.Fatalf("enqueued %d jobs, want %d", len(jobs), len(ids))
	}
	want := map[uuid.UUID]bool{ids[0]: true, ids[1]: true, ids[2]: true}
	for _, j := range jobs {
		if !want[j.MemoryID] {
			t.Errorf("unexpected enqueued memory %s", j.MemoryID)
		}
		if j.NamespaceID != ns {
			t.Errorf("job namespace = %s, want %s", j.NamespaceID, ns)
		}
		if j.Status != model.EnrichmentStatusPending {
			t.Errorf("job status = %q, want pending", j.Status)
		}
		if !jobHasMultiVectorMarker(t, j) {
			t.Errorf("job %s missing %q marker in StepsCompleted", j.MemoryID, model.JobMarkerOnlyMultiVector)
		}
	}
}

// Disabled flag: the phase is a no-op; it never lists or enqueues.
func TestMultiVectorBackfillPhase_DisabledNoop(t *testing.T) {
	lister := &fakeMVLister{ids: []uuid.UUID{uuid.New()}}
	queue := &enqueueRecorder{}

	phase := NewMultiVectorBackfillPhase(lister, queue, mvSettings(false, 200))
	cycle := mvTestCycle(uuid.New())
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Errorf("disabled phase must not report residual")
	}
	if lister.calls != 0 {
		t.Errorf("disabled phase must not list candidates; got %d calls", lister.calls)
	}
	if len(queue.snapshot()) != 0 {
		t.Errorf("disabled phase must not enqueue")
	}
}

// Multi-vector faceting globally disabled: even with the backfill switch on the
// phase is a no-op, since enqueued jobs would no-op in the worker and re-loop.
func TestMultiVectorBackfillPhase_FeatureDisabledNoop(t *testing.T) {
	lister := &fakeMVLister{ids: []uuid.UUID{uuid.New()}}
	queue := &enqueueRecorder{}

	settings := &staticDreamSettings{
		values: map[string]string{
			service.SettingDreamMultiVectorBackfillEnabled: "true",
			service.SettingMultiVectorEnabled:              "false",
		},
		ints: map[string]int{
			service.SettingDreamMultiVectorBackfillCapPerCycle: 200,
		},
	}

	phase := NewMultiVectorBackfillPhase(lister, queue, settings)
	cycle := mvTestCycle(uuid.New())
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Errorf("feature-disabled phase must not report residual")
	}
	if lister.calls != 0 {
		t.Errorf("feature-disabled phase must not list candidates; got %d calls", lister.calls)
	}
	if len(queue.snapshot()) != 0 {
		t.Errorf("feature-disabled phase must not enqueue")
	}
}

// More candidates than the per-cycle cap: only cap jobs are enqueued and the
// phase reports residual so the backlog is visible as it drains across cycles.
func TestMultiVectorBackfillPhase_ResidualWhenOverCap(t *testing.T) {
	ns := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	lister := &fakeMVLister{ids: ids}
	queue := &enqueueRecorder{}

	phase := NewMultiVectorBackfillPhase(lister, queue, mvSettings(true, 2))
	cycle := mvTestCycle(ns)
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.HasResidual {
		t.Fatal("expected residual when candidates exceed the per-cycle cap")
	}
	if result.ResidualReason != ResidualReasonMoreCandidatesThanBatch {
		t.Errorf("residual reason = %q, want %q", result.ResidualReason, ResidualReasonMoreCandidatesThanBatch)
	}
	if got := len(queue.snapshot()); got != 2 {
		t.Errorf("enqueued %d jobs, want 2 (the cap)", got)
	}
}

// A lister error is soft: the phase logs and returns cleanly (no error, no
// residual) so one failed sweep does not fail the whole cycle.
func TestMultiVectorBackfillPhase_ListErrorIsSoft(t *testing.T) {
	lister := &fakeMVLister{err: errors.New("db down")}
	queue := &enqueueRecorder{}

	phase := NewMultiVectorBackfillPhase(lister, queue, mvSettings(true, 200))
	cycle := mvTestCycle(uuid.New())
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute must not return an error on list failure; got %v", err)
	}
	if result.HasResidual {
		t.Errorf("list failure must not report residual")
	}
	if len(queue.snapshot()) != 0 {
		t.Errorf("no jobs should be enqueued when listing failed")
	}
}
