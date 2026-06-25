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

// fakeDreamEntityLister returns a fixed candidate list and records the limit the
// phase requested (so tests can assert the cap+1 residual probe).
type fakeDreamEntityLister struct {
	ids       []uuid.UUID
	lastLimit int
	calls     int
	err       error
}

func (f *fakeDreamEntityLister) ListDreamEntityBackfillCandidates(_ context.Context, _ []uuid.UUID, limit int) ([]storage.BackfillCandidate, error) {
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

func cebSettings(capPerCycle int) *staticDreamSettings {
	return &staticDreamSettings{
		ints: map[string]int{
			service.SettingDreamConsolidationEntityBackfillCapPerCycle: capPerCycle,
		},
	}
}

func cebTestCycle(ns uuid.UUID) *model.DreamCycle {
	return &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: ns}
}

// jobStepsCompleted decodes a job's StepsCompleted into a string slice.
func jobStepsCompleted(t *testing.T, j model.EnrichmentJob) []string {
	t.Helper()
	if len(j.StepsCompleted) == 0 {
		return nil
	}
	var steps []string
	if err := json.Unmarshal(j.StepsCompleted, &steps); err != nil {
		t.Fatalf("unmarshal StepsCompleted: %v", err)
	}
	return steps
}

// Happy path: every candidate gets a pending entity-only job (fact_extraction
// pre-stamped, entity_extraction left to run) for the cycle's namespace, the
// lister is probed at cap+1, and no residual fires.
func TestConsolidationEntityBackfillPhase_EnqueuesCandidates(t *testing.T) {
	ns := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	lister := &fakeDreamEntityLister{ids: ids}
	queue := &enqueueRecorder{}

	phase := NewConsolidationEntityBackfillPhase(lister, queue, cebSettings(200))
	cycle := cebTestCycle(ns)
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
		steps := jobStepsCompleted(t, j)
		if !slices.Contains(steps, model.StepFactExtraction) {
			t.Errorf("job %s must pre-stamp %q so facts skip; got %v", j.MemoryID, model.StepFactExtraction, steps)
		}
		if slices.Contains(steps, model.StepEntityExtraction) {
			t.Errorf("job %s must NOT pre-stamp %q (entity extraction must run); got %v", j.MemoryID, model.StepEntityExtraction, steps)
		}
	}
}

// More candidates than the per-cycle cap: only cap jobs are enqueued and the
// phase reports residual so the backlog is visible as it drains across cycles.
func TestConsolidationEntityBackfillPhase_ResidualWhenOverCap(t *testing.T) {
	ns := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	lister := &fakeDreamEntityLister{ids: ids}
	queue := &enqueueRecorder{}

	phase := NewConsolidationEntityBackfillPhase(lister, queue, cebSettings(2))
	cycle := cebTestCycle(ns)
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
func TestConsolidationEntityBackfillPhase_ListErrorIsSoft(t *testing.T) {
	lister := &fakeDreamEntityLister{err: errors.New("db down")}
	queue := &enqueueRecorder{}

	phase := NewConsolidationEntityBackfillPhase(lister, queue, cebSettings(200))
	cycle := cebTestCycle(uuid.New())
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
