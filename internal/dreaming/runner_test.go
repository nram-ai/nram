package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	mu               sync.Mutex
	tickCalls        int
	tickFn           func(call int) (int, error) // call is 1-indexed
	partialSummaries []json.RawMessage           // captured by UpdatePhaseSummary
	completed        json.RawMessage             // captured by Complete
}

func (f *fakeCycleRepo) Start(ctx context.Context, id uuid.UUID) error { return nil }
func (f *fakeCycleRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status, phase string) error {
	return nil
}
func (f *fakeCycleRepo) UpdatePhaseSummary(ctx context.Context, id uuid.UUID, summary json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(json.RawMessage, len(summary))
	copy(cp, summary)
	f.partialSummaries = append(f.partialSummaries, cp)
	return nil
}
func (f *fakeCycleRepo) Complete(ctx context.Context, id uuid.UUID, summary json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed = make(json.RawMessage, len(summary))
	copy(f.completed, summary)
	return nil
}
func (f *fakeCycleRepo) Fail(ctx context.Context, id uuid.UUID, errMsg string) error { return nil }

func (f *fakeCycleRepo) snapshotPartials() []json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]json.RawMessage, len(f.partialSummaries))
	copy(out, f.partialSummaries)
	return out
}

func (f *fakeCycleRepo) finalSummary() json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completed
}

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
	result PhaseResult // returned from Execute when no error
	got    *TokenBudget
	gotCap int
}

func (p *recordingPhase) Name() string { return p.name }
func (p *recordingPhase) Execute(_ context.Context, _ *model.DreamCycle, b *TokenBudget, _ *DreamLogWriter) (PhaseResult, error) {
	p.got = b
	p.gotCap = b.Total()
	if p.spend > 0 {
		// Spend in one shot; ErrBudgetExhausted from over-spend is what we
		// want phases to surface to the runner so the slice-cap path runs.
		if err := b.Spend(p.spend); err != nil {
			return PhaseResult{}, err
		}
	}
	return p.result, p.err
}

// noopRepo is a cycleProgressRepo that does nothing — the runner's writes are
// not the unit under test here.
type noopRepo struct{}

func (noopRepo) Start(context.Context, uuid.UUID) error                               { return nil }
func (noopRepo) UpdateStatus(context.Context, uuid.UUID, string, string) error        { return nil }
func (noopRepo) UpdatePhaseSummary(context.Context, uuid.UUID, json.RawMessage) error { return nil }
func (noopRepo) TickProgress(context.Context, uuid.UUID) (int, error)                 { return 0, nil }
func (noopRepo) Complete(context.Context, uuid.UUID, json.RawMessage) error           { return nil }
func (noopRepo) Fail(context.Context, uuid.UUID, string) error                        { return nil }

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
		spend: 600, // exceeds the 500 slice cap (1000 * 0.40 / 0.80)
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

	// Proportional-of-remaining: first phase cap = Remaining(1000) * 0.40 / (0.40+0.40) = 500.
	if first.gotCap != 500 {
		t.Errorf("first phase received slice cap=%d, want 500 (1000 * 0.40 / 0.80)", first.gotCap)
	}
	if second.got == nil {
		t.Fatal("second phase never ran — the runner broke the loop on slice exhaustion")
	}
	// Second phase: sum_remaining collapses to its own 0.40, so cap = Remaining * 0.40 / 0.40 = Remaining.
	// Root used after first phase = 600, so Remaining = 400, cap = 400.
	if second.gotCap != 400 {
		t.Errorf("second phase received slice cap=%d, want 400 (root_remaining 400 * 0.40 / 0.40)", second.gotCap)
	}
	if budget.Used() != 600 {
		t.Errorf("root used=%d, want 600 (first phase's spend cascaded)", budget.Used())
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
// phase consumes more than its share, a later phase's slice cap is
// correctly bounded by what the root has left. Under proportional-of-
// remaining the second phase's cap collapses naturally because its
// denominator shrinks while its numerator (Remaining) shrinks faster.
func TestRunner_LaterPhaseClampedByRootRemaining(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40, // first cap = 1000 * 0.40 / 0.80 = 500
		"dreaming.consolidation.budget_fraction": 0.40,
	}}
	// First phase overruns: spends 800 against a 500 slice → returns
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
	// Second phase: sum_remaining = 0.40, cap = Remaining(200) * 0.40 / 0.40 = 200.
	// SubSlice.Remaining() = min(local=200, parent=200) = 200.
	if rem := second.got.Remaining(); rem != 200 {
		t.Errorf("second phase Remaining=%d, want 200 (proportional cap collapses to root remaining)", rem)
	}
}

// TestRunner_UnusedSliceFlowsToNextLLMPhase is the central guarantee of the
// proportional-of-remaining policy: when an earlier phase under-spends, the
// later phase's slice grows accordingly. Under the previous fraction-of-
// total policy the second phase would still see cap=400 even with the root
// untouched; under the new policy sum_remaining collapses to its own frac
// and the cap reaches root Remaining.
func TestRunner_UnusedSliceFlowsToNextLLMPhase(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40,
		"dreaming.consolidation.budget_fraction": 0.40,
	}}
	first := &recordingPhase{name: model.DreamPhaseContradictions} // spend = 0
	second := &recordingPhase{name: model.DreamPhaseConsolidation}

	r := newTestRunner(settings, first, second)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	if first.gotCap != 500 {
		t.Errorf("first phase cap=%d, want 500 (1000 * 0.40 / 0.80)", first.gotCap)
	}
	// First spent 0 → root.used=0 → second's cap absorbs the unspent reservation.
	if second.gotCap != 1000 {
		t.Errorf("second phase cap=%d, want 1000 (rollover: Remaining(1000) * 0.40 / 0.40)", second.gotCap)
	}
}

// TestRunner_HeadroomDistributedAcrossPhases verifies that the 0.05 default
// headroom (fractions sum to 0.95) is absorbed into per-phase caps under
// the new policy: each phase's cap is strictly greater than int(Total*frac)
// because the proportional denominator is the sum of remaining fractions
// (≤ 0.95), not 1.0.
func TestRunner_HeadroomDistributedAcrossPhases(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.embedding_backfill.budget_fraction": 0.10,
		"dreaming.paraphrase_dedup.budget_fraction":   0.05,
		"dreaming.contradiction.budget_fraction":      0.40,
		"dreaming.consolidation.budget_fraction":      0.40,
	}}
	embedding := &recordingPhase{name: model.DreamPhaseEmbeddingBackfill}
	paraphrase := &recordingPhase{name: model.DreamPhaseParaphraseDedup}
	contradiction := &recordingPhase{name: model.DreamPhaseContradictions}
	consolidation := &recordingPhase{name: model.DreamPhaseConsolidation}

	r := newTestRunner(settings, embedding, paraphrase, contradiction, consolidation)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	cases := []struct {
		name   string
		phase  *recordingPhase
		want   int
		oldCap int // strict-of-total baseline that the new cap must beat
	}{
		{"embedding", embedding, 105, 100},
		{"paraphrase", paraphrase, 58, 50},
		{"contradiction", contradiction, 500, 400},
		{"consolidation", consolidation, 1000, 400},
	}
	for _, c := range cases {
		if c.phase.gotCap != c.want {
			t.Errorf("%s cap=%d, want %d", c.name, c.phase.gotCap, c.want)
		}
		if c.phase.gotCap <= c.oldCap {
			t.Errorf("%s cap=%d should strictly exceed strict-of-total cap %d (headroom absorption)", c.name, c.phase.gotCap, c.oldCap)
		}
	}
}

// TestRunner_RolloverChainsAcrossThreePhases verifies the rollover chains
// across more than two phases: each later phase's cap reflects the
// cumulative under-spend of every prior phase, and when the last phase
// runs alone its cap reaches the full root Remaining.
func TestRunner_RolloverChainsAcrossThreePhases(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.embedding_backfill.budget_fraction": 0.30,
		"dreaming.contradiction.budget_fraction":      0.30,
		"dreaming.consolidation.budget_fraction":      0.40,
	}}
	first := &recordingPhase{name: model.DreamPhaseEmbeddingBackfill, spend: 50}
	second := &recordingPhase{name: model.DreamPhaseContradictions, spend: 100}
	third := &recordingPhase{name: model.DreamPhaseConsolidation}

	r := newTestRunner(settings, first, second, third)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	// First: 1000 * 0.30 / 1.00 = 300
	if first.gotCap != 300 {
		t.Errorf("first cap=%d, want 300", first.gotCap)
	}
	// Second: (1000-50) * 0.30 / 0.70 = 950 * 0.30 / 0.70 = 407
	if second.gotCap != 407 {
		t.Errorf("second cap=%d, want 407 (Remaining(950) * 0.30 / 0.70 with rollover)", second.gotCap)
	}
	// Third: sum_remaining collapses to 0.40, so cap = Remaining * 0.40 / 0.40 = Remaining.
	// Root used after first two phases = 150, Remaining = 850.
	if third.gotCap != 850 {
		t.Errorf("third cap=%d, want 850 (Remaining absorbs all prior under-spend)", third.gotCap)
	}
}

func newTestRunnerWithRepo(repo cycleProgressRepo, settings SettingsResolver, phases ...Phase) *Runner {
	return &Runner{
		cycleRepo:         repo,
		heartbeatInterval: 5 * time.Second,
		settings:          settings,
		phases:            phases,
	}
}

func decodeSummary(t *testing.T, raw json.RawMessage) []PhaseSummaryEntry {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var out []PhaseSummaryEntry
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decodeSummary: %v\nraw: %s", err, string(raw))
	}
	return out
}

func TestRunner_PhaseSummaryRecordsSliceCap(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40,
		"dreaming.consolidation.budget_fraction": 0.40,
		// pruning has no fraction registered → frac=0 (SQL-only).
	}}
	contradiction := &recordingPhase{name: model.DreamPhaseContradictions}
	consolidation := &recordingPhase{name: model.DreamPhaseConsolidation}
	pruning := &recordingPhase{name: model.DreamPhasePruning}

	repo := &fakeCycleRepo{}
	r := newTestRunnerWithRepo(repo, settings, contradiction, consolidation, pruning)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	entries := decodeSummary(t, repo.finalSummary())
	if len(entries) != 3 {
		t.Fatalf("expected 3 phase summary entries, got %d: %s", len(entries), string(repo.finalSummary()))
	}

	wantCaps := map[string]int{
		model.DreamPhaseContradictions: 500,  // 1000 * 0.40 / 0.80
		model.DreamPhaseConsolidation:  1000, // Remaining(1000) * 0.40 / 0.40
		model.DreamPhasePruning:        0,    // SQL-only, no slice
	}
	for _, e := range entries {
		want, ok := wantCaps[e.Phase]
		if !ok {
			t.Errorf("unexpected phase in summary: %q", e.Phase)
			continue
		}
		if e.SliceCap != want {
			t.Errorf("phase %q SliceCap=%d, want %d", e.Phase, e.SliceCap, want)
		}
	}
}

func TestRunner_StreamsPartialSummaryPerPhase(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40,
		"dreaming.consolidation.budget_fraction": 0.40,
	}}
	first := &recordingPhase{name: model.DreamPhaseContradictions}
	second := &recordingPhase{name: model.DreamPhaseConsolidation}

	repo := &fakeCycleRepo{}
	r := newTestRunnerWithRepo(repo, settings, first, second)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	partials := repo.snapshotPartials()
	if len(partials) != 2 {
		t.Fatalf("expected 2 partial-summary writes (one per phase), got %d", len(partials))
	}

	first1 := decodeSummary(t, partials[0])
	if len(first1) != 1 || first1[0].Phase != model.DreamPhaseContradictions {
		t.Errorf("first partial summary should contain only contradiction; got %+v", first1)
	}
	first2 := decodeSummary(t, partials[1])
	if len(first2) != 2 ||
		first2[0].Phase != model.DreamPhaseContradictions ||
		first2[1].Phase != model.DreamPhaseConsolidation {
		t.Errorf("second partial summary should contain both phases in order; got %+v", first2)
	}
}

func TestRunner_PhaseSuppliedResidualReasonRoundTrips(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40,
	}}
	phase := &recordingPhase{
		name: model.DreamPhaseContradictions,
		result: PhaseResult{
			HasResidual:    true,
			ResidualReason: ResidualReasonDispatchCapReached,
			ResidualDetail: map[string]any{"cap": 100, "stale": 250},
		},
	}

	repo := &fakeCycleRepo{}
	r := newTestRunnerWithRepo(repo, settings, phase)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	entries := decodeSummary(t, repo.finalSummary())
	if len(entries) != 1 {
		t.Fatalf("expected 1 summary entry, got %d", len(entries))
	}
	if !entries[0].HasResidual {
		t.Error("HasResidual should be true")
	}
	if entries[0].ResidualReason != ResidualReasonDispatchCapReached {
		t.Errorf("ResidualReason=%q, want %q", entries[0].ResidualReason, ResidualReasonDispatchCapReached)
	}
	if cap, _ := entries[0].ResidualDetail["cap"].(float64); int(cap) != 100 {
		t.Errorf("ResidualDetail[cap]=%v, want 100", entries[0].ResidualDetail["cap"])
	}
}

func TestRunner_RunnerLevelReasonOverridesPhaseSupplied(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.contradiction.budget_fraction": 0.40,
		"dreaming.consolidation.budget_fraction": 0.40,
	}}
	// Phase spends past its 500 cap → b.Spend returns ErrBudgetExhausted.
	// recordingPhase returns (PhaseResult{}, err) on Spend error so any
	// PhaseResult.ResidualReason a real phase tried to set is moot — the
	// runner-level reason path takes over.
	first := &recordingPhase{
		name:  model.DreamPhaseContradictions,
		spend: 600,
		result: PhaseResult{
			HasResidual:    true,
			ResidualReason: "phase_supplied_should_be_overridden",
		},
	}
	second := &recordingPhase{name: model.DreamPhaseConsolidation}

	repo := &fakeCycleRepo{}
	r := newTestRunnerWithRepo(repo, settings, first, second)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	entries := decodeSummary(t, repo.finalSummary())
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	first0 := entries[0]
	if !first0.HasResidual {
		t.Error("first phase HasResidual should be true on slice exhaustion")
	}
	// Root has 400 remaining when the slice (500) blew up, so root is NOT
	// exhausted → reason is phase_slice_exhausted (not budget_exhausted_during_phase).
	if first0.ResidualReason != ResidualReasonPhaseSliceExhausted {
		t.Errorf("first ResidualReason=%q, want %q", first0.ResidualReason, ResidualReasonPhaseSliceExhausted)
	}
	if first0.Error != "budget exhausted" {
		t.Errorf("first Error=%q, want %q", first0.Error, "budget exhausted")
	}
}

// TestRunner_PropagatesSubPhasesToEntry pins the runner-level contract that
// PhaseResult.SubPhases survives the JSON round-trip into the persisted
// PhaseSummaryEntry. The UI relies on this — without it, the nested
// sub-phase bar in the cycle report has nothing to render.
func TestRunner_PropagatesSubPhasesToEntry(t *testing.T) {
	settings := fractionSettings{values: map[string]float64{
		"dreaming.consolidation.budget_fraction": 0.40,
	}}
	consolidation := &recordingPhase{
		name: model.DreamPhaseConsolidation,
		result: PhaseResult{
			SubPhases: []SubPhaseSummary{
				{Name: model.DreamSubPhaseBackfillAudit, TokensUsed: 120, SliceCap: 350, HasResidual: false},
				{Name: model.DreamSubPhaseReinforce, TokensUsed: 80, SliceCap: 350, HasResidual: true},
				{Name: model.DreamSubPhaseConsolidate, TokensUsed: 200, SliceCap: 300, HasResidual: false},
			},
		},
	}

	repo := &fakeCycleRepo{}
	r := newTestRunnerWithRepo(repo, settings, consolidation)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New()}
	budget := NewTokenBudget(1000, 100)

	if _, _, err := r.Execute(context.Background(), cycle, budget); err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}

	entries := decodeSummary(t, repo.finalSummary())
	if len(entries) != 1 {
		t.Fatalf("expected 1 phase summary entry, got %d", len(entries))
	}
	got := entries[0].SubPhases
	if len(got) != 3 {
		t.Fatalf("expected 3 sub-phase entries, got %d: %+v", len(got), got)
	}
	wantNames := []string{
		model.DreamSubPhaseBackfillAudit,
		model.DreamSubPhaseReinforce,
		model.DreamSubPhaseConsolidate,
	}
	for i, w := range wantNames {
		if got[i].Name != w {
			t.Errorf("sub_phases[%d].Name=%q, want %q", i, got[i].Name, w)
		}
	}
	if got[0].TokensUsed != 120 || got[0].SliceCap != 350 {
		t.Errorf("backfill_audit TokensUsed=%d SliceCap=%d, want 120/350", got[0].TokensUsed, got[0].SliceCap)
	}
	if !got[1].HasResidual {
		t.Error("reinforce HasResidual should round-trip true")
	}
	if got[2].TokensUsed != 200 || got[2].SliceCap != 300 {
		t.Errorf("consolidate TokensUsed=%d SliceCap=%d, want 200/300", got[2].TokensUsed, got[2].SliceCap)
	}
}

// TestPhaseSummaryEntry_SubPhasesOmitemptyForEmpty pins the JSON contract
// the UI depends on: phases without sub-phase data must marshal without a
// `sub_phases` field, not as an explicit empty array.
func TestPhaseSummaryEntry_SubPhasesOmitemptyForEmpty(t *testing.T) {
	entry := PhaseSummaryEntry{
		Phase:      model.DreamPhaseContradictions,
		TokensUsed: 100,
		Operations: 5,
		DurationMs: 250,
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); strings.Contains(got, "sub_phases") {
		t.Errorf("expected sub_phases to be omitted from JSON for empty slice, got %s", got)
	}

	entry.SubPhases = []SubPhaseSummary{{Name: model.DreamSubPhaseBackfillAudit, TokensUsed: 1}}
	raw2, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw2); !strings.Contains(got, "sub_phases") {
		t.Errorf("expected sub_phases to appear in JSON for populated slice, got %s", got)
	}
}
