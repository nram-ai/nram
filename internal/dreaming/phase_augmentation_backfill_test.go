package dreaming

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// fakeAugLister returns a fixed candidate list and records the limit the phase
// requested (so tests can assert the cap+1 residual probe).
type fakeAugLister struct {
	ids       []uuid.UUID
	lastLimit int
	calls     int
	err       error
}

func (f *fakeAugLister) ListAugmentationBackfillCandidates(_ context.Context, _ []uuid.UUID, limit int) ([]storage.BackfillCandidate, error) {
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

func augSettings(enabled bool, capPerCycle int) *staticDreamSettings {
	values := map[string]string{}
	if enabled {
		values[service.SettingDreamAugmentationBackfillEnabled] = "true"
	} else {
		values[service.SettingDreamAugmentationBackfillEnabled] = "false"
	}
	return &staticDreamSettings{
		values: values,
		ints: map[string]int{
			service.SettingDreamAugmentationBackfillCapPerCycle: capPerCycle,
		},
	}
}

func augTestCycle(ns uuid.UUID) *model.DreamCycle {
	return &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: ns}
}

// Happy path: every candidate gets a pending enrichment job for the cycle's
// namespace, the lister is probed at cap+1, and no residual is reported.
func TestAugmentationBackfillPhase_EnqueuesCandidates(t *testing.T) {
	ns := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	lister := &fakeAugLister{ids: ids}
	queue := &enqueueRecorder{}

	phase := NewAugmentationBackfillPhase(lister, queue, augSettings(true, 200))
	cycle := augTestCycle(ns)
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
	}
}

// Disabled flag: the phase is a no-op; it never lists or enqueues.
func TestAugmentationBackfillPhase_DisabledNoop(t *testing.T) {
	lister := &fakeAugLister{ids: []uuid.UUID{uuid.New()}}
	queue := &enqueueRecorder{}

	phase := NewAugmentationBackfillPhase(lister, queue, augSettings(false, 200))
	cycle := augTestCycle(uuid.New())
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

// More candidates than the per-cycle cap: only cap jobs are enqueued and the
// phase reports residual so the backlog is visible as it drains across cycles.
func TestAugmentationBackfillPhase_ResidualWhenOverCap(t *testing.T) {
	ns := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	lister := &fakeAugLister{ids: ids}
	queue := &enqueueRecorder{}

	phase := NewAugmentationBackfillPhase(lister, queue, augSettings(true, 2))
	cycle := augTestCycle(ns)
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
func TestAugmentationBackfillPhase_ListErrorIsSoft(t *testing.T) {
	lister := &fakeAugLister{err: errors.New("db down")}
	queue := &enqueueRecorder{}

	phase := NewAugmentationBackfillPhase(lister, queue, augSettings(true, 200))
	cycle := augTestCycle(uuid.New())
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
