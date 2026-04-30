package enrichment

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
)

// captureBus is a minimal events.EventBus that records every Publish for
// later inspection. Subscribe / Replay / Close return zero values; the
// progress code path only uses Publish.
type captureBus struct {
	mu     sync.Mutex
	events []events.Event
}

func (b *captureBus) Publish(_ context.Context, e events.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, e)
	return nil
}

func (b *captureBus) Subscribe(context.Context, string) (<-chan events.Event, func(), error) {
	ch := make(chan events.Event)
	close(ch)
	return ch, func() {}, nil
}

func (b *captureBus) Replay(string) []events.Event { return nil }
func (b *captureBus) Close() error                 { return nil }

func (b *captureBus) snapshot() []events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]events.Event, len(b.events))
	copy(out, b.events)
	return out
}

func (b *captureBus) byType(t string) []events.Event {
	out := []events.Event{}
	for _, e := range b.snapshot() {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

func TestProgressTrackerJobLifecycleHappyPath(t *testing.T) {
	bus := &captureBus{}
	tracker := newProgressTracker(bus, nil)

	job := &model.EnrichmentJob{
		ID:          uuid.New(),
		MemoryID:    uuid.New(),
		NamespaceID: uuid.New(),
	}
	tracker.JobStarted(context.Background(), job, nil, "worker-0")

	// Job is in flight; the live map should expose it.
	if _, ok := tracker.jobs.Load(job.ID); !ok {
		t.Fatal("expected job to be in flight after JobStarted")
	}

	tracker.SetStage(job.ID, StageEmbed)
	if v, _ := tracker.jobs.Load(job.ID); v.(*inFlightJob).Stage() != StageEmbed {
		t.Errorf("stage = %s, want %s", v.(*inFlightJob).Stage(), StageEmbed)
	}

	tracker.JobCompleted(context.Background(), job.ID, job.MemoryID, job.NamespaceID,
		"worker-0", time.Now().Add(-2*time.Second), 100, 60, 40, nil)

	if _, ok := tracker.jobs.Load(job.ID); ok {
		t.Fatal("expected job to be removed after JobCompleted")
	}

	started := bus.byType(events.EnrichmentJobStarted)
	completed := bus.byType(events.EnrichmentJobCompleted)
	if len(started) != 1 {
		t.Fatalf("expected 1 job.started event, got %d", len(started))
	}
	if len(completed) != 1 {
		t.Fatalf("expected 1 job.completed event, got %d", len(completed))
	}

	startedData := decode(t, started[0])
	if startedData["job_id"] != job.ID.String() {
		t.Errorf("started.job_id = %v, want %s", startedData["job_id"], job.ID)
	}
	if startedData["worker_id"] != "worker-0" {
		t.Errorf("started.worker_id = %v, want worker-0", startedData["worker_id"])
	}

	completedData := decode(t, completed[0])
	if completedData["ok"] != true {
		t.Errorf("completed.ok = %v, want true", completedData["ok"])
	}
	tokens, ok := completedData["tokens"].(map[string]any)
	if !ok {
		t.Fatalf("tokens missing or wrong shape: %T", completedData["tokens"])
	}
	if int(tokens["total"].(float64)) != 100 {
		t.Errorf("tokens.total = %v, want 100", tokens["total"])
	}
	// latency_ms is computed from now-startedAt; the value here was 2s ago.
	if int64(completedData["latency_ms"].(float64)) < 1500 {
		t.Errorf("latency_ms = %v, want >= 1500", completedData["latency_ms"])
	}
}

func TestProgressTrackerJobCompletedOnError(t *testing.T) {
	bus := &captureBus{}
	tracker := newProgressTracker(bus, nil)

	job := &model.EnrichmentJob{
		ID:          uuid.New(),
		MemoryID:    uuid.New(),
		NamespaceID: uuid.New(),
	}
	tracker.JobStarted(context.Background(), job, nil, "worker-0")
	wantErr := errors.New("provider returned 502")
	tracker.JobCompleted(context.Background(), job.ID, job.MemoryID, job.NamespaceID,
		"worker-0", time.Now(), 0, 0, 0, wantErr)

	completed := bus.byType(events.EnrichmentJobCompleted)
	if len(completed) != 1 {
		t.Fatalf("expected 1 job.completed event, got %d", len(completed))
	}
	data := decode(t, completed[0])
	if data["ok"] != false {
		t.Errorf("ok = %v, want false", data["ok"])
	}
	if data["error"] != wantErr.Error() {
		t.Errorf("error = %v, want %q", data["error"], wantErr.Error())
	}
}

func TestProgressTrackerEmitTickReportsInFlightStateAndOldest(t *testing.T) {
	bus := &captureBus{}
	tracker := newProgressTracker(bus, nil)

	older := &model.EnrichmentJob{ID: uuid.New(), MemoryID: uuid.New(), NamespaceID: uuid.New()}
	newer := &model.EnrichmentJob{ID: uuid.New(), MemoryID: uuid.New(), NamespaceID: uuid.New()}

	// Manually inject an older claimed-at to validate oldest_claim_age_ms.
	olderClaim := time.Now().Add(-90 * time.Second).UTC()
	tracker.jobs.Store(older.ID, &inFlightJob{
		JobID: older.ID, MemoryID: older.MemoryID, NamespaceID: older.NamespaceID,
		WorkerID: "worker-A", ClaimedAt: olderClaim, StartedAt: olderClaim,
		stage: StagePreEmbed,
	})
	tracker.jobs.Store(newer.ID, &inFlightJob{
		JobID: newer.ID, MemoryID: newer.MemoryID, NamespaceID: newer.NamespaceID,
		WorkerID: "worker-B", ClaimedAt: time.Now().UTC(), StartedAt: time.Now().UTC(),
		stage: StageEmbed,
	})

	tracker.SetPaused(true)
	tracker.EmitTick(context.Background())

	ticks := bus.byType(events.EnrichmentPoolTick)
	if len(ticks) != 1 {
		t.Fatalf("expected 1 pool.tick, got %d", len(ticks))
	}
	data := decode(t, ticks[0])
	if int(data["in_flight"].(float64)) != 2 {
		t.Errorf("in_flight = %v, want 2", data["in_flight"])
	}
	if data["paused"] != true {
		t.Errorf("paused = %v, want true", data["paused"])
	}
	if int64(data["oldest_claim_age_ms"].(float64)) < 80_000 {
		t.Errorf("oldest_claim_age_ms = %v, want >= 80000", data["oldest_claim_age_ms"])
	}
	stages, ok := data["by_stage"].(map[string]any)
	if !ok {
		t.Fatalf("by_stage missing or wrong shape: %T", data["by_stage"])
	}
	if int(stages[StagePreEmbed].(float64)) != 1 || int(stages[StageEmbed].(float64)) != 1 {
		t.Errorf("by_stage = %v, want pre_embed:1 embed:1", stages)
	}
}

func TestProgressTrackerNilBusIsNoOp(t *testing.T) {
	tracker := newProgressTracker(nil, nil)
	job := &model.EnrichmentJob{
		ID:          uuid.New(),
		MemoryID:    uuid.New(),
		NamespaceID: uuid.New(),
	}
	// Must not panic; the live map should still update so the in-process
	// snapshot path stays consistent for a nil-bus pool.
	tracker.JobStarted(context.Background(), job, nil, "worker-0")
	if _, ok := tracker.jobs.Load(job.ID); !ok {
		t.Fatal("expected job to still be tracked even without a bus")
	}
	tracker.JobCompleted(context.Background(), job.ID, job.MemoryID, job.NamespaceID,
		"worker-0", time.Now(), 0, 0, 0, nil)
	if _, ok := tracker.jobs.Load(job.ID); ok {
		t.Fatal("expected job to be removed after JobCompleted")
	}
	tracker.EmitTick(context.Background())
}

func decode(t *testing.T, e events.Event) map[string]any {
	t.Helper()
	if len(e.Data) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	return m
}
