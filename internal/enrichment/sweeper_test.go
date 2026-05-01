package enrichment

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

type fakeSweeperSettings struct {
	stuckThreshold time.Duration
	sweepInterval  time.Duration
}

func (f *fakeSweeperSettings) ResolveDurationSecondsWithDefault(_ context.Context, key, _ string) time.Duration {
	switch key {
	case service.SettingEnrichmentStuckThreshold:
		if f.stuckThreshold > 0 {
			return f.stuckThreshold
		}
		return 30 * time.Minute
	case service.SettingEnrichmentStuckSweep:
		if f.sweepInterval > 0 {
			return f.sweepInterval
		}
		return 5 * time.Minute
	}
	return 0
}

// fakeStuckJobStore captures sweep activity so tests can assert on it.
type fakeStuckJobStore struct {
	mu          sync.Mutex
	stale       []*model.EnrichmentJob
	requeued    []uuid.UUID
	requeueErr  error
	requeueOK   bool // when false, RequeueStale returns (false, nil) — race path
	listErr     error
}

func (s *fakeStuckJobStore) ListStaleClaimed(_ context.Context, _ time.Duration) ([]*model.EnrichmentJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]*model.EnrichmentJob, len(s.stale))
	copy(out, s.stale)
	return out, nil
}

func (s *fakeStuckJobStore) RequeueStale(_ context.Context, id uuid.UUID, _ string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requeueErr != nil {
		return false, s.requeueErr
	}
	if !s.requeueOK {
		return false, nil
	}
	s.requeued = append(s.requeued, id)
	return true, nil
}

// stuckJob constructs a stale-looking *model.EnrichmentJob for the fake store.
func stuckJob(id uuid.UUID, claimedBy string, ageSinceUpdate time.Duration) *model.EnrichmentJob {
	worker := claimedBy
	updatedAt := time.Now().UTC().Add(-ageSinceUpdate)
	return &model.EnrichmentJob{
		ID:          id,
		MemoryID:    uuid.New(),
		NamespaceID: uuid.New(),
		Status:      "processing",
		ClaimedBy:   &worker,
		ClaimedAt:   &updatedAt,
		UpdatedAt:   updatedAt,
		Attempts:    1,
		MaxAttempts: 3,
	}
}

func newTestSweeper(store stuckJobStore, bus events.EventBus) *StuckJobSweeper {
	return NewStuckJobSweeper(store, &fakeSweeperSettings{stuckThreshold: 30 * time.Minute}, bus)
}

func TestStuckJobSweeper_RequeuesStale(t *testing.T) {
	id := uuid.New()
	store := &fakeStuckJobStore{
		stale:     []*model.EnrichmentJob{stuckJob(id, "worker-0", 35*time.Minute)},
		requeueOK: true,
	}
	bus := events.NewMemoryBus(8, 8)
	defer bus.Close()
	ch, cancelSub, err := bus.Subscribe(context.Background(), "namespace:"+store.stale[0].NamespaceID.String())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancelSub()

	sweeper := newTestSweeper(store, bus)
	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	store.mu.Lock()
	if len(store.requeued) != 1 || store.requeued[0] != id {
		t.Fatalf("expected one RequeueStale for %s, got %v", id, store.requeued)
	}
	store.mu.Unlock()

	// Drain the bus and confirm we saw the requeue event for our job.
	deadline := time.After(time.Second)
	var saw bool
	for !saw {
		select {
		case evt := <-ch:
			if evt.Type == events.EnrichmentJobRequeued {
				saw = true
			}
		case <-deadline:
			t.Fatalf("did not receive enrichment.job.requeued event")
		}
	}
}

func TestStuckJobSweeper_NoOpOnRace(t *testing.T) {
	id := uuid.New()
	store := &fakeStuckJobStore{
		stale: []*model.EnrichmentJob{stuckJob(id, "worker-0", 35*time.Minute)},
		// requeueOK = false → repo returns (false, nil) simulating the race
		// where the row transitioned out of 'processing' between
		// ListStaleClaimed and RequeueStale.
	}
	bus := events.NewMemoryBus(8, 8)
	defer bus.Close()
	ch, cancelSub, err := bus.Subscribe(context.Background(), "namespace:"+store.stale[0].NamespaceID.String())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancelSub()

	sweeper := newTestSweeper(store, bus)
	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep returned error on race path: %v", err)
	}

	// No event should fire when RequeueStale reports no-op.
	select {
	case evt := <-ch:
		t.Fatalf("expected no event on race, got %s", evt.Type)
	case <-time.After(50 * time.Millisecond):
	}

	store.mu.Lock()
	if len(store.requeued) != 0 {
		t.Fatalf("expected zero recorded requeues, got %v", store.requeued)
	}
	store.mu.Unlock()
}

func TestStuckJobSweeper_ContextCancellation(t *testing.T) {
	store := &fakeStuckJobStore{
		stale: []*model.EnrichmentJob{
			stuckJob(uuid.New(), "worker-0", 35*time.Minute),
			stuckJob(uuid.New(), "worker-0", 35*time.Minute),
		},
		requeueOK: true,
	}
	sweeper := newTestSweeper(store, events.NewMemoryBus(8, 8))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sweeper.Sweep(ctx)
	if !errors.Is(err, context.Canceled) {
		// May exit cleanly via the per-iteration ctx.Err() check after
		// processing zero or more rows; either way, a cancelled context
		// should not return a non-cancelled error.
		if err != nil {
			t.Fatalf("sweep with cancelled ctx returned non-canceled error: %v", err)
		}
	}
}

func TestStuckJobSweeper_RepoError(t *testing.T) {
	store := &fakeStuckJobStore{listErr: errors.New("boom")}
	sweeper := newTestSweeper(store, events.NewMemoryBus(8, 8))

	if err := sweeper.Sweep(context.Background()); err == nil {
		t.Fatalf("expected ListStaleClaimed error to propagate out of Sweep")
	}
}

func TestStuckJobSweeper_PerRowFailureContinues(t *testing.T) {
	store := &fakeStuckJobStore{
		stale: []*model.EnrichmentJob{
			stuckJob(uuid.New(), "worker-0", 35*time.Minute),
			stuckJob(uuid.New(), "worker-0", 35*time.Minute),
		},
		requeueErr: errors.New("transient db hiccup"),
	}
	sweeper := newTestSweeper(store, events.NewMemoryBus(8, 8))

	// A repo error per row should NOT propagate out of Sweep — the sweeper
	// logs and continues so a single bad row doesn't poison the batch.
	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep returned error despite per-row RequeueStale failures: %v", err)
	}
}
