package enrichment

import (
	"context"
	"errors"
	"strings"
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
	claimMaxAge    time.Duration
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
	case service.SettingEnrichmentClaimMaxAge:
		// Honor whatever was set, including 0 (disable). Tests that want
		// the production default should set claimMaxAge explicitly.
		return f.claimMaxAge
	}
	return 0
}

func (f *fakeSweeperSettings) ResolveIntWithDefault(_ context.Context, key, _ string) int {
	return service.GetDefaultInt(key)
}

// fakeStuckJobStore captures sweep activity so tests can assert on it.
type fakeStuckJobStore struct {
	mu              sync.Mutex
	stale           []*model.EnrichmentJob
	requeued        []uuid.UUID
	requeueReasons  map[uuid.UUID]string
	requeueErr      error
	requeueOK       bool // when false, RequeueStale returns (false, nil) — race path
	listErr         error
	lastUpdatedArg  time.Duration
	lastClaimAgeArg time.Duration
}

func (s *fakeStuckJobStore) ListStaleClaimed(_ context.Context, updatedThreshold, claimedAtMaxAge time.Duration, _ int) ([]*model.EnrichmentJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastUpdatedArg = updatedThreshold
	s.lastClaimAgeArg = claimedAtMaxAge
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]*model.EnrichmentJob, len(s.stale))
	copy(out, s.stale)
	return out, nil
}

func (s *fakeStuckJobStore) RequeueStale(_ context.Context, id uuid.UUID, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requeueErr != nil {
		return false, s.requeueErr
	}
	if !s.requeueOK {
		return false, nil
	}
	s.requeued = append(s.requeued, id)
	if s.requeueReasons == nil {
		s.requeueReasons = make(map[uuid.UUID]string)
	}
	s.requeueReasons[id] = reason
	return true, nil
}

// stuckJob constructs a stale-looking *model.EnrichmentJob for the fake store.
// Both claimed_at and updated_at are seeded at `ageSinceUpdate` ago — the
// common case where a dead worker stopped advancing both columns.
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

// wedgedJob constructs a row matching the backstop signal: claimed_at is
// past the cap but updated_at is still fresh (a heartbeat ticking under a
// colliding claimed_by, or a same-process panic loop refreshing the row).
func wedgedJob(id uuid.UUID, claimedBy string, claimAge time.Duration) *model.EnrichmentJob {
	worker := claimedBy
	now := time.Now().UTC()
	claimedAt := now.Add(-claimAge)
	return &model.EnrichmentJob{
		ID:          id,
		MemoryID:    uuid.New(),
		NamespaceID: uuid.New(),
		Status:      "processing",
		ClaimedBy:   &worker,
		ClaimedAt:   &claimedAt,
		UpdatedAt:   now, // fresh — sweeper's updated_at signal would NOT fire
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
	defer func() { _ = bus.Close() }()
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
	defer func() { _ = bus.Close() }()
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

// TestStuckJobSweeper_BackstopDisableViaZero confirms an operator setting
// enrichment.claim_max_age_seconds to 0 reaches ListStaleClaimed as 0 so
// the repo predicate's `(? > 0 AND ...)` gating disables the backstop. A
// regression here would let the sweeper silently substitute a default and
// the disable knob would become dead from the operator's perspective.
func TestStuckJobSweeper_BackstopDisableViaZero(t *testing.T) {
	store := &fakeStuckJobStore{requeueOK: true}
	settings := &fakeSweeperSettings{
		stuckThreshold: 30 * time.Minute,
		claimMaxAge:    0, // explicit disable
	}
	sweeper := NewStuckJobSweeper(store, settings, events.NewMemoryBus(8, 8))

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.lastClaimAgeArg != 0 {
		t.Errorf("claim-max-age = %s, want 0 (backstop disabled)", store.lastClaimAgeArg)
	}
}

// TestStuckJobSweeper_BackstopDisableViaNegative confirms a misconfigured
// negative duration is normalized to 0 (disabled) rather than passed
// through to the repo as a negative value the predicate would not handle.
func TestStuckJobSweeper_BackstopDisableViaNegative(t *testing.T) {
	store := &fakeStuckJobStore{requeueOK: true}
	settings := &fakeSweeperSettings{
		stuckThreshold: 30 * time.Minute,
		claimMaxAge:    -5 * time.Minute,
	}
	sweeper := NewStuckJobSweeper(store, settings, events.NewMemoryBus(8, 8))

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.lastClaimAgeArg != 0 {
		t.Errorf("claim-max-age = %s, want 0 (negative normalized)", store.lastClaimAgeArg)
	}
}

// TestStuckJobSweeper_BackstopThresholdPlumbed asserts the sweeper resolves
// both signals and passes them to ListStaleClaimed in the right order. If
// the wiring slips (e.g. arguments swapped), the listed rows match the
// wrong predicate and recovery silently breaks.
func TestStuckJobSweeper_BackstopThresholdPlumbed(t *testing.T) {
	store := &fakeStuckJobStore{requeueOK: true}
	settings := &fakeSweeperSettings{
		stuckThreshold: 17 * time.Minute,
		claimMaxAge:    91 * time.Minute,
	}
	sweeper := NewStuckJobSweeper(store, settings, events.NewMemoryBus(8, 8))

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.lastUpdatedArg != 17*time.Minute {
		t.Errorf("updated threshold = %s, want 17m", store.lastUpdatedArg)
	}
	if store.lastClaimAgeArg != 91*time.Minute {
		t.Errorf("claim-max-age = %s, want 91m", store.lastClaimAgeArg)
	}
}

// TestStuckJobSweeper_BackstopReasonString covers the case where the
// returned row has a fresh updated_at but a claimed_at past the cap (the
// backstop scenario). The recorded reason must reflect the claim-age
// trigger so operators can distinguish it from the common "heartbeat
// stopped" path.
func TestStuckJobSweeper_BackstopReasonString(t *testing.T) {
	id := uuid.New()
	store := &fakeStuckJobStore{
		stale:     []*model.EnrichmentJob{wedgedJob(id, "abc-worker-0", 3*time.Hour)},
		requeueOK: true,
	}
	sweeper := NewStuckJobSweeper(store,
		&fakeSweeperSettings{stuckThreshold: 30 * time.Minute, claimMaxAge: 2 * time.Hour},
		events.NewMemoryBus(8, 8))

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.requeued) != 1 || store.requeued[0] != id {
		t.Fatalf("expected one requeue for %s, got %v", id, store.requeued)
	}
	reason := store.requeueReasons[id]
	if !strings.Contains(reason, "claim age") || !strings.Contains(reason, "backstop") {
		t.Errorf("reason = %q, want it to mention claim age / backstop", reason)
	}
	if strings.Contains(reason, "without progress") {
		t.Errorf("reason = %q, should NOT cite updated_at staleness when the backstop fired", reason)
	}
}

// TestStuckJobSweeper_StalenessReasonString covers the common path: a row
// whose updated_at fell behind. Reason text must cite "without progress"
// rather than the backstop wording so the two recovery causes are
// distinguishable in logs and last_requeue_reason.
func TestStuckJobSweeper_StalenessReasonString(t *testing.T) {
	id := uuid.New()
	store := &fakeStuckJobStore{
		stale:     []*model.EnrichmentJob{stuckJob(id, "def-worker-1", 35*time.Minute)},
		requeueOK: true,
	}
	sweeper := newTestSweeper(store, events.NewMemoryBus(8, 8))

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	reason := store.requeueReasons[id]
	if !strings.Contains(reason, "without progress") {
		t.Errorf("reason = %q, want it to mention updated_at staleness", reason)
	}
	if strings.Contains(reason, "backstop") {
		t.Errorf("reason = %q, should NOT cite backstop when the updated_at signal fired", reason)
	}
}
