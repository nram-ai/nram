package dreaming

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// fakeUncoveredEnqueuer records how many times the phase invoked the bulk
// enqueue and returns a canned count/error.
type fakeUncoveredEnqueuer struct {
	n     int64
	err   error
	calls int
}

func (f *fakeUncoveredEnqueuer) EnqueueUncoveredMemories(_ context.Context) (int64, error) {
	f.calls++
	return f.n, f.err
}

func uncoveredSettings(enrichmentEnabled bool) *staticDreamSettings {
	flag := "false"
	if enrichmentEnabled {
		flag = "true"
	}
	return &staticDreamSettings{
		values: map[string]string{
			service.SettingEnrichmentEnabled: flag,
		},
	}
}

func uncoveredTestCycle() *model.DreamCycle {
	return &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: uuid.New()}
}

// Enrichment enabled: the phase delegates to the enqueuer and reports the count.
func TestUncoveredBackfillPhase_EnqueuesWhenEnrichmentEnabled(t *testing.T) {
	enq := &fakeUncoveredEnqueuer{n: 7}
	phase := NewUncoveredBackfillPhase(enq, uncoveredSettings(true))
	cycle := uncoveredTestCycle()
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Errorf("uncovered backfill must not report residual")
	}
	if enq.calls != 1 {
		t.Errorf("enqueuer calls = %d, want 1", enq.calls)
	}
}

// Enrichment disabled: the phase is a no-op and never enqueues. Enqueuing full
// jobs while enrichment is off would only grow a pending backlog the worker
// never drains.
func TestUncoveredBackfillPhase_DisabledWhenEnrichmentOff(t *testing.T) {
	enq := &fakeUncoveredEnqueuer{n: 7}
	phase := NewUncoveredBackfillPhase(enq, uncoveredSettings(false))
	cycle := uncoveredTestCycle()
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Errorf("disabled phase must not report residual")
	}
	if enq.calls != 0 {
		t.Errorf("enrichment-disabled phase must not enqueue; got %d calls", enq.calls)
	}
}

// A nil enqueuer is a no-op (defensive; matches the sibling phases).
func TestUncoveredBackfillPhase_NilEnqueuerNoop(t *testing.T) {
	phase := NewUncoveredBackfillPhase(nil, uncoveredSettings(true))
	cycle := uncoveredTestCycle()
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Errorf("nil-enqueuer phase must not report residual")
	}
}

// An enqueue error is soft: the phase logs and returns cleanly (no error, no
// residual) so one failed sweep does not fail the whole cycle.
func TestUncoveredBackfillPhase_EnqueueErrorIsSoft(t *testing.T) {
	enq := &fakeUncoveredEnqueuer{err: errors.New("db down")}
	phase := NewUncoveredBackfillPhase(enq, uncoveredSettings(true))
	cycle := uncoveredTestCycle()
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute must not return an error on enqueue failure; got %v", err)
	}
	if result.HasResidual {
		t.Errorf("enqueue failure must not report residual")
	}
	if enq.calls != 1 {
		t.Errorf("enqueuer calls = %d, want 1", enq.calls)
	}
}
