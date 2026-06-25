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
}

func (f *fakeStuckSettings) Resolve(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (f *fakeStuckSettings) ResolveFloat(_ context.Context, _, _ string) (float64, error) {
	return 0, nil
}
func (f *fakeStuckSettings) ResolveInt(_ context.Context, key, _ string) (int, error) {
	if key == service.SettingDreamStuckThreshold {
		return f.stuckThresholdSecs, nil
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
