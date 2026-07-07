package dreaming

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// fakeStuckSettings resolves only the keys the sweeper reads.
type fakeStuckSettings struct {
	stuckThresholdSecs int
	// sweepSecs feeds dreaming.stuck_sweep_seconds (the run() interval). Left
	// 0 by tests that only drive Sweep() directly; the sweeper clamps 0 to the
	// 300s default. Tests that drive run()/Start() pin it explicitly.
	sweepSecs int
}

func (f *fakeStuckSettings) Resolve(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeStuckSettings) ResolveFloat(_ context.Context, _, _ string) (float64, error) {
	return 0, nil
}
func (f *fakeStuckSettings) ResolveInt(_ context.Context, key, _ string) (int, error) {
	switch key {
	case service.SettingDreamStuckThreshold:
		return f.stuckThresholdSecs, nil
	case service.SettingDreamStuckSweep:
		return f.sweepSecs, nil
	}
	return 0, nil
}
func (f *fakeStuckSettings) ResolveBool(_ context.Context, _, _ string) bool            { return false }
func (f *fakeStuckSettings) ResolveBoolWithDefault(_ context.Context, _, _ string) bool { return false }
func (f *fakeStuckSettings) ResolveIntWithDefault(_ context.Context, _, _ string) int {
	return f.stuckThresholdSecs
}
func (f *fakeStuckSettings) ResolveFloatWithDefault(_ context.Context, _, _ string) float64 {
	return 0
}
func (f *fakeStuckSettings) ResolveStringWithDefault(_ context.Context, _, _ string) string {
	return ""
}
func (f *fakeStuckSettings) ResolveDurationSecondsWithDefault(_ context.Context, _, _ string) time.Duration {
	return time.Duration(f.stuckThresholdSecs) * time.Second
}

type fakeStuckStore struct {
	mu         sync.Mutex
	stale      []model.DreamCycle
	abandoned  []uuid.UUID
	abandonErr error
	// abandonedCh, when non-nil, receives each abandoned cycle id so a test
	// can block on the abandon instead of sleep-polling. Buffered by the test;
	// the send is non-blocking so it never stalls the sweeper.
	abandonedCh chan uuid.UUID
}

func (s *fakeStuckStore) ListStale(_ context.Context, _ time.Duration, _ int) ([]model.DreamCycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.DreamCycle, len(s.stale))
	copy(out, s.stale)
	return out, nil
}

func (s *fakeStuckStore) Abandon(_ context.Context, id uuid.UUID, _ string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.abandonErr != nil {
		return false, s.abandonErr
	}
	s.abandoned = append(s.abandoned, id)
	if s.abandonedCh != nil {
		select {
		case s.abandonedCh <- id:
		default:
		}
	}
	return true, nil
}

type recordingCanceller struct {
	mu        sync.Mutex
	cancelled []uuid.UUID
}

func (r *recordingCanceller) CancelCycle(id uuid.UUID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelled = append(r.cancelled, id)
	return true
}

func newSweeperWithStub(store stuckCycleStore, canc CycleCanceller) *StuckCycleSweeper {
	return &StuckCycleSweeper{
		cycleRepo: store,
		canceller: canc,
		settings:  &fakeStuckSettings{stuckThresholdSecs: 60 * 5},
		eventBus:  events.NewMemoryBus(8, 8),
	}
}

func TestStuckCycleSweeper_AbandonsAndCancels(t *testing.T) {
	id := uuid.New()
	store := &fakeStuckStore{
		stale: []model.DreamCycle{{
			ID:          id,
			ProjectID:   uuid.New(),
			NamespaceID: uuid.New(),
			Status:      model.DreamStatusRunning,
			Phase:       "entity_dedup",
			UpdatedAt:   time.Now().Add(-30 * time.Minute).UTC(),
		}},
	}
	canc := &recordingCanceller{}
	sweeper := newSweeperWithStub(store, canc)

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	store.mu.Lock()
	if len(store.abandoned) != 1 || store.abandoned[0] != id {
		t.Fatalf("expected one Abandon for %s, got %v", id, store.abandoned)
	}
	store.mu.Unlock()

	canc.mu.Lock()
	if len(canc.cancelled) != 1 || canc.cancelled[0] != id {
		t.Fatalf("expected one CancelCycle for %s, got %v", id, canc.cancelled)
	}
	canc.mu.Unlock()
}

func TestStuckCycleSweeper_NilCancellerSafe(t *testing.T) {
	id := uuid.New()
	store := &fakeStuckStore{
		stale: []model.DreamCycle{{
			ID:          id,
			ProjectID:   uuid.New(),
			NamespaceID: uuid.New(),
			Status:      model.DreamStatusRunning,
			UpdatedAt:   time.Now().Add(-30 * time.Minute).UTC(),
		}},
	}
	sweeper := newSweeperWithStub(store, nil)

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	store.mu.Lock()
	if len(store.abandoned) != 1 {
		t.Fatalf("expected DB write even when canceller is nil, got %v", store.abandoned)
	}
	store.mu.Unlock()
}

func TestStuckCycleSweeper_AbandonErrorContinues(t *testing.T) {
	store := &fakeStuckStore{
		stale: []model.DreamCycle{
			{ID: uuid.New(), Status: model.DreamStatusRunning, UpdatedAt: time.Now().Add(-time.Hour)},
			{ID: uuid.New(), Status: model.DreamStatusRunning, UpdatedAt: time.Now().Add(-time.Hour)},
		},
		abandonErr: errors.New("transient"),
	}
	sweeper := newSweeperWithStub(store, nil)

	// A repo error per row should NOT propagate out of Sweep; the sweeper
	// logs and continues so a single bad row doesn't poison the batch.
	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep returned error despite per-row Abandon failures: %v", err)
	}
}

// TestStuckCycleSweeper_SweepsOnStartup drives the real run() goroutine through
// Start() and asserts a stuck cycle is abandoned before the first timer
// interval could fire. The sweep interval is pinned to 1h so only the startup
// sweep can account for the abandon.
func TestStuckCycleSweeper_SweepsOnStartup(t *testing.T) {
	id := uuid.New()
	store := &fakeStuckStore{
		stale: []model.DreamCycle{{
			ID:          id,
			ProjectID:   uuid.New(),
			NamespaceID: uuid.New(),
			Status:      model.DreamStatusRunning,
			Phase:       "entity_dedup",
			UpdatedAt:   time.Now().Add(-30 * time.Minute).UTC(),
		}},
		abandonedCh: make(chan uuid.UUID, 1),
	}
	sweeper := &StuckCycleSweeper{
		cycleRepo: store,
		canceller: nil,
		settings:  &fakeStuckSettings{stuckThresholdSecs: 300, sweepSecs: 3600},
		eventBus:  events.NewMemoryBus(8, 8),
	}

	sweeper.Start()
	defer sweeper.Stop()

	select {
	case got := <-store.abandonedCh:
		if got != id {
			t.Fatalf("startup sweep abandoned %s, want %s", got, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup sweep did not abandon the stuck cycle before the first interval elapsed")
	}
}

// TestStuckCycleSweeper_SweepIntervalHotReload proves the interval is
// re-resolved live: mutating the setting between calls to sweepInterval changes
// the returned value, so a settings change takes effect without a restart. Also
// covers the non-positive clamp to the 300s default.
func TestStuckCycleSweeper_SweepIntervalHotReload(t *testing.T) {
	settings := &fakeStuckSettings{sweepSecs: 300}
	sweeper := &StuckCycleSweeper{settings: settings}
	ctx := context.Background()

	if got := sweeper.sweepInterval(ctx); got != 300*time.Second {
		t.Fatalf("initial interval = %s, want 300s", got)
	}

	settings.sweepSecs = 42
	if got := sweeper.sweepInterval(ctx); got != 42*time.Second {
		t.Fatalf("interval after change = %s, want 42s (interval not re-resolved live)", got)
	}

	// A non-positive (or unset) value clamps to the 300s default.
	settings.sweepSecs = 0
	if got := sweeper.sweepInterval(ctx); got != 300*time.Second {
		t.Fatalf("zero interval = %s, want clamp to 300s default", got)
	}
}

func TestStuckCycleSweeper_NoStaleCyclesIsNoop(t *testing.T) {
	store := &fakeStuckStore{}
	canc := &recordingCanceller{}
	sweeper := newSweeperWithStub(store, canc)

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	store.mu.Lock()
	if len(store.abandoned) != 0 {
		t.Fatalf("expected no abandons, got %v", store.abandoned)
	}
	store.mu.Unlock()
	canc.mu.Lock()
	if len(canc.cancelled) != 0 {
		t.Fatalf("expected no cancels, got %v", canc.cancelled)
	}
	canc.mu.Unlock()
}
