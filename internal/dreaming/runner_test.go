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
	"github.com/nram-ai/nram/internal/model"
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

// fractionSettings overrides ResolveFloat on the existing stubSettings
// (defined in phase_contradictions_test.go) so the runner sees configurable
// per-phase budget fractions while every other resolver method falls
// through to the zero-returning defaults the package's other tests share.
type fractionSettings struct {
	stubSettings
	values map[string]float64
}

func (f fractionSettings) ResolveFloat(_ context.Context, key, _ string) (float64, error) {
	if v, ok := f.values[key]; ok {
		return v, nil
	}
	return 0, nil
}

// recordingPhase is a Phase test double that spends a configured token amount
// against whatever budget the runner hands it, then returns the configured
// error (or nil). It records the *TokenBudget pointer it received so tests
// can assert whether the runner sliced or passed through.
type recordingPhase struct {
	name   string
	spend  int
	err    error
	got    *TokenBudget
	gotCap int
}

func (p *recordingPhase) Name() string { return p.name }
func (p *recordingPhase) Execute(_ context.Context, _ *model.DreamCycle, b *TokenBudget, _ *DreamLogWriter) (bool, error) {
	p.got = b
	p.gotCap = b.Total()
	if p.spend > 0 {
		// Spend in one shot; ErrBudgetExhausted from over-spend is what we
		// want phases to surface to the runner so the slice-cap path runs.
		if err := b.Spend(p.spend); err != nil {
			return false, err
		}
	}
	return false, p.err
}

// noopRepo is a cycleProgressRepo that does nothing — the runner's writes are
// not the unit under test here.
type noopRepo struct{}

func (noopRepo) Start(context.Context, uuid.UUID) error                          { return nil }
func (noopRepo) UpdateStatus(context.Context, uuid.UUID, string, string) error   { return nil }
func (noopRepo) TickProgress(context.Context, uuid.UUID) (int, error)            { return 0, nil }
func (noopRepo) Complete(context.Context, uuid.UUID, json.RawMessage) error      { return nil }
func (noopRepo) Fail(context.Context, uuid.UUID, string) error                   { return nil }

// newTestRunner builds a Runner directly so tests can stub the cycle repo,
// settings, and phase list without going through the cmd/server wiring.
func newTestRunner(settings SettingsResolver, phases ...Phase) *Runner {
	return &Runner{
		cycleRepo:         noopRepo{},
		heartbeatInterval: 5 * time.Second, // long enough that no tick fires during a test
		settings:          settings,
		phases:            phases,
	}
}

// TestRunner_PerPhaseSliceLimitsLLMPhase verifies that a phase whose Spend
// exceeds its slice cap does not end the cycle: the runner observes
// ErrBudgetExhausted, marks the phase as residual=phase_slice_exhausted, and
// proceeds to the next phase with a fresh slice. This is the central
// behavioral guarantee of the per-phase reservation.
func TestRunner_PerPhaseSliceLimitsLLMPhase(t *testing.T) {
	// fractionSettings is keyed by setting key (the form ResolveFloat sees),
	// not by phase name. The runner translates phase name → key via
	// phaseFractionKeys.
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40,
		"dreaming.consolidation.budget_fraction": 0.40,
	}}
	first := &recordingPhase{
		name:  model.DreamPhaseContradictions,
		spend: 500, // exceeds the 400 slice cap (40% of 1000)
	}
	second := &recordingPhase{name: model.DreamPhaseConsolidation}

	r := newTestRunner(settings, first, second)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	allCompleted, hasResidual, err := r.Execute(context.Background(), cycle, budget)
	if err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}
	if allCompleted {
		t.Error("expected allCompleted=false because the first phase reported ErrBudgetExhausted")
	}
	if !hasResidual {
		t.Error("expected hasResidual=true because the slice was exhausted mid-phase")
	}

	if first.gotCap != 400 {
		t.Errorf("first phase received slice cap=%d, want 400 (40%% of 1000)", first.gotCap)
	}
	if second.got == nil {
		t.Fatal("second phase never ran — the runner broke the loop on slice exhaustion")
	}
	if second.gotCap != 400 {
		t.Errorf("second phase received slice cap=%d, want 400 (its own fresh 40%% slice)", second.gotCap)
	}
	// Root used should be 500 (first phase's spend cascaded). Second phase's
	// effective Remaining at entry is min(slice_local=400, root_remaining=500) = 400.
	if budget.Used() != 500 {
		t.Errorf("root used=%d, want 500 (first phase's spend cascaded)", budget.Used())
	}
	if rem := second.got.Remaining(); rem != 400 {
		t.Errorf("second phase slice Remaining=%d, want 400 (min of slice_cap and root_remaining)", rem)
	}
}

// TestRunner_FractionZeroPassesRootThrough verifies that SQL-only phases
// (frac=0.0 default) receive the root TokenBudget unchanged. Their spends
// (none in production) would charge the root directly; the runner does not
// carve a slice for them so they are not gated by a per-phase cap.
func TestRunner_FractionZeroPassesRootThrough(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		// No entries: pruning has no fraction registered → 0.0 default.
	}}
	pruning := &recordingPhase{name: model.DreamPhasePruning}

	r := newTestRunner(settings, pruning)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	if pruning.got == nil {
		t.Fatal("pruning phase never ran")
	}
	if pruning.got != budget {
		t.Error("pruning phase received a sliced budget; expected the root budget pointer (frac=0 must pass through)")
	}
	if pruning.gotCap != 1000 {
		t.Errorf("pruning saw cap=%d, want 1000 (the root total)", pruning.gotCap)
	}
}

// TestRunner_LaterPhaseClampedByRootRemaining verifies that when an earlier
// phase consumes more than its share, a later phase whose own slice cap
// would allow more tokens than the root has left is correctly clamped by
// the parent. SubSlice.Remaining()=min(local, parent) handles this with no
// special-case logic in the runner.
func TestRunner_LaterPhaseClampedByRootRemaining(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40, // cap=400 of 1000
		"dreaming.consolidation.budget_fraction": 0.40, // cap=400 of 1000
	}}
	// First phase overruns: spends 800 against a 400 slice → returns
	// ErrBudgetExhausted; root.used=800; root.remaining=200.
	first := &recordingPhase{name: model.DreamPhaseContradictions, spend: 800}
	second := &recordingPhase{name: model.DreamPhaseConsolidation}

	r := newTestRunner(settings, first, second)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	if budget.Used() != 800 {
		t.Errorf("root used=%d, want 800 (first phase's overrun cascaded fully)", budget.Used())
	}
	if second.got == nil {
		t.Fatal("second phase never ran")
	}
	// Second phase's own slice cap is 400, but parent has only 200 left.
	// Remaining must take the min.
	if rem := second.got.Remaining(); rem != 200 {
		t.Errorf("second phase Remaining=%d, want 200 (clamped by parent's remaining, not slice cap)", rem)
	}
}
