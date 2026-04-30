package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
)

// fakeCycleRepo satisfies cycleProgressRepo and records TickProgress calls so
// the heartbeat goroutine can be exercised without a database. tickFn lets
// individual tests inject errors, panics, or specific token counts per call.
type fakeCycleRepo struct {
	mu        sync.Mutex
	tickCalls int
	tickFn    func(call int) (int, error) // call is 1-indexed
}

func (f *fakeCycleRepo) Start(ctx context.Context, id uuid.UUID) error { return nil }
func (f *fakeCycleRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status, phase string) error {
	return nil
}
func (f *fakeCycleRepo) Complete(ctx context.Context, id uuid.UUID, summary json.RawMessage) error {
	return nil
}
func (f *fakeCycleRepo) Fail(ctx context.Context, id uuid.UUID, errMsg string) error { return nil }

func (f *fakeCycleRepo) TickProgress(ctx context.Context, id uuid.UUID) (int, error) {
	f.mu.Lock()
	f.tickCalls++
	call := f.tickCalls
	fn := f.tickFn
	f.mu.Unlock()
	if fn != nil {
		return fn(call)
	}
	return 0, nil
}

func (f *fakeCycleRepo) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tickCalls
}

func TestHeartbeatTicksAndFeedsTokensUsedToSSE(t *testing.T) {
	repo := &fakeCycleRepo{
		tickFn: func(call int) (int, error) {
			// Each tick reports a strictly increasing token count.
			return call * 100, nil
		},
	}
	bus := &captureBus{}
	tracker := NewCycleTracker(bus, uuid.New(), uuid.New())

	r := &Runner{
		cycleRepo:         repo,
		heartbeatInterval: 30 * time.Millisecond,
	}
	budget := NewTokenBudget(10_000, 500)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.heartbeat(ctx, uuid.New(), tracker, budget)
		close(done)
	}()

	// Allow at least 3 intervals to elapse (initial tick + 2 ticker fires).
	time.Sleep(110 * time.Millisecond)
	cancel()
	<-done

	if got := repo.callCount(); got < 3 {
		t.Fatalf("expected at least 3 TickProgress calls in 110ms with 30ms interval, got %d", got)
	}

	// Every heartbeat event must carry the token count returned by TickProgress.
	heartbeats := 0
	for _, e := range bus.snapshot() {
		if e.Type != events.DreamCycleHeartbeat {
			continue
		}
		heartbeats++
		data := decodeData(t, e)
		tu, ok := data["tokens_used"].(float64)
		if !ok {
			t.Fatalf("heartbeat tokens_used missing or wrong type: %+v", data)
		}
		// Returned values are 100, 200, 300, ... — must never be zero.
		if int(tu) == 0 {
			t.Errorf("heartbeat carried tokens_used=0; expected SUM-derived value")
		}
	}
	if heartbeats < 3 {
		t.Fatalf("expected at least 3 dream.cycle.heartbeat events, got %d", heartbeats)
	}
}

func TestHeartbeatRecoversFromPanic(t *testing.T) {
	var panicTriggered atomic.Bool
	repo := &fakeCycleRepo{
		tickFn: func(call int) (int, error) {
			if call == 2 {
				panicTriggered.Store(true)
				panic("simulated DB driver panic")
			}
			return call * 10, nil
		},
	}
	bus := &captureBus{}
	tracker := NewCycleTracker(bus, uuid.New(), uuid.New())

	r := &Runner{
		cycleRepo:         repo,
		heartbeatInterval: 25 * time.Millisecond,
	}
	budget := NewTokenBudget(1000, 100)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.heartbeat(ctx, uuid.New(), tracker, budget)
		close(done)
	}()

	// Wait for at least 4 ticks: tick 1 succeeds, tick 2 panics, ticks 3+ must
	// still fire if the recover() is wired correctly.
	time.Sleep(130 * time.Millisecond)
	cancel()
	<-done

	if !panicTriggered.Load() {
		t.Fatal("test setup failure: panic never fired")
	}
	if got := repo.callCount(); got < 4 {
		t.Fatalf("heartbeat goroutine died on panic: only %d ticks (expected ≥4)", got)
	}
}

func TestHeartbeatFallbackOnTickError(t *testing.T) {
	tickErr := errors.New("simulated db lock timeout")
	repo := &fakeCycleRepo{
		tickFn: func(call int) (int, error) {
			return 0, tickErr
		},
	}
	bus := &captureBus{}
	tracker := NewCycleTracker(bus, uuid.New(), uuid.New())

	r := &Runner{
		cycleRepo:         repo,
		heartbeatInterval: 30 * time.Millisecond,
	}
	budget := NewTokenBudget(1000, 100)
	// Pre-spend so budget.Used() is non-zero — this is what the SSE fallback
	// is supposed to surface when TickProgress errors.
	_ = budget.Spend(444)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.heartbeat(ctx, uuid.New(), tracker, budget)
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	// Despite TickProgress always erroring, EmitHeartbeat events must still
	// fire carrying the budget.Used() fallback so SSE listeners stay alive.
	sawFallback := false
	for _, e := range bus.snapshot() {
		if e.Type != events.DreamCycleHeartbeat {
			continue
		}
		data := decodeData(t, e)
		if int(data["tokens_used"].(float64)) == 444 {
			sawFallback = true
			break
		}
	}
	if !sawFallback {
		t.Fatalf("expected at least one heartbeat carrying the budget.Used()=444 fallback")
	}
}
