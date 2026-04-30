package dreaming

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// newSchedulerForCancelTests returns a Scheduler with no DB or runner — only
// the activeCycles registry is exercised. NewScheduler does the wiring we
// need (initializes the registry map), so we tolerate the nil dependencies.
func newSchedulerForCancelTests() *Scheduler {
	return &Scheduler{
		activeCycles: make(map[uuid.UUID]context.CancelFunc),
	}
}

func TestScheduler_CancelCycle_FoundAndUnknown(t *testing.T) {
	s := newSchedulerForCancelTests()

	id := uuid.New()
	called := atomic.Bool{}
	cancel := func() { called.Store(true) }
	s.registerCycle(id, cancel)

	if !s.CancelCycle(id) {
		t.Fatal("expected CancelCycle to return true for registered cycle")
	}
	if !called.Load() {
		t.Fatal("expected cancel func to be called")
	}

	// Second call: registry has been cleared on first cancel.
	if s.CancelCycle(id) {
		t.Fatal("expected CancelCycle to return false on duplicate cancel")
	}

	// Unknown cycle.
	if s.CancelCycle(uuid.New()) {
		t.Fatal("expected CancelCycle to return false for unknown id")
	}
}

func TestScheduler_SnapshotActiveCycles(t *testing.T) {
	s := newSchedulerForCancelTests()

	id1, id2 := uuid.New(), uuid.New()
	s.registerCycle(id1, func() {})
	s.registerCycle(id2, func() {})

	got := s.snapshotActiveCycles()
	if len(got) != 2 {
		t.Fatalf("expected 2 ids, got %d (%v)", len(got), got)
	}
	seen := map[uuid.UUID]bool{got[0]: true, got[1]: true}
	if !seen[id1] || !seen[id2] {
		t.Fatalf("snapshot missed an id: %v", got)
	}
}

func TestScheduler_CancelCycle_RaceWithUnregister(t *testing.T) {
	// Concurrent natural completion (unregisterCycle) and CancelCycle must
	// not deadlock or double-cancel. Either order is acceptable; we only
	// assert the absence of panics/deadlocks and that cancel runs at most
	// once.
	s := newSchedulerForCancelTests()

	const cycles = 200
	var wg sync.WaitGroup
	var canceled atomic.Int64

	for i := 0; i < cycles; i++ {
		id := uuid.New()
		s.registerCycle(id, func() { canceled.Add(1) })

		wg.Add(2)
		go func() {
			defer wg.Done()
			s.CancelCycle(id)
		}()
		go func() {
			defer wg.Done()
			s.unregisterCycle(id)
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: registry contention timed out")
	}

	// At most one cancel per cycle (cancellable cycles are bounded by total).
	if got := canceled.Load(); got > int64(cycles) {
		t.Fatalf("cancel called %d times across %d cycles", got, cycles)
	}
}
