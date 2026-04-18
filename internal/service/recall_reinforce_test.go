package service

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

// --- Reinforcement test doubles ---

type recordingReinforcer struct {
	mu          sync.Mutex
	calls       []reinforceCall
	returnError error
}

type reinforceCall struct {
	IDs    []uuid.UUID
	When   time.Time
	Factor float64
}

func (r *recordingReinforcer) BumpReinforcement(_ context.Context, ids []uuid.UUID, now time.Time, factor float64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, reinforceCall{IDs: append([]uuid.UUID(nil), ids...), When: now, Factor: factor})
	if r.returnError != nil {
		return 0, r.returnError
	}
	return int64(len(ids)), nil
}

func (r *recordingReinforcer) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingReinforcer) lastCall() (reinforceCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return reinforceCall{}, false
	}
	return r.calls[len(r.calls)-1], true
}

type staticSettings struct {
	values map[string]string
	floats map[string]float64
	err    error
}

func (s *staticSettings) Resolve(_ context.Context, key string, _ string) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	if v, ok := s.values[key]; ok {
		return v, nil
	}
	return "", nil
}

func (s *staticSettings) ResolveFloat(_ context.Context, key string, _ string) (float64, error) {
	if s.err != nil {
		return 0, s.err
	}
	if v, ok := s.floats[key]; ok {
		return v, nil
	}
	return 0, errors.New("no value")
}

// collectingBus buffers published events for assertion.
type collectingBus struct {
	mu     sync.Mutex
	events []events.Event
}

func (b *collectingBus) Publish(_ context.Context, e events.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, e)
	return nil
}

func (b *collectingBus) Subscribe(_ context.Context, _ string) (<-chan events.Event, func(), error) {
	ch := make(chan events.Event)
	return ch, func() { close(ch) }, nil
}

func (b *collectingBus) Replay(_ string) []events.Event { return nil }
func (b *collectingBus) Close() error                   { return nil }

func (b *collectingBus) publishedByType(t string) []events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []events.Event
	for _, e := range b.events {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

// --- reinforce() unit tests (direct, no goroutine) ---

func TestReinforce_OffMode_NoWriteNoEvent(t *testing.T) {
	writer := &recordingReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer: writer,
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModeOff},
			floats: map[string]float64{SettingReconsolidationFactor: 0.02},
		},
		Bus: bus,
	})

	svc.reinforce(context.Background(), []uuid.UUID{uuid.New()})

	if writer.callCount() != 0 {
		t.Errorf("off mode must not write; got %d calls", writer.callCount())
	}
	if n := len(bus.publishedByType(events.MemoryReinforced)); n != 0 {
		t.Errorf("off mode must not emit; got %d events", n)
	}
}

func TestReinforce_ShadowMode_EventOnlyNoWrite(t *testing.T) {
	writer := &recordingReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer: writer,
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModeShadow},
			floats: map[string]float64{SettingReconsolidationFactor: 0.02},
		},
		Bus: bus,
	})

	ids := []uuid.UUID{uuid.New(), uuid.New()}
	svc.reinforce(context.Background(), ids)

	if writer.callCount() != 0 {
		t.Errorf("shadow mode must not write; got %d calls", writer.callCount())
	}
	evts := bus.publishedByType(events.MemoryReinforced)
	if len(evts) != 1 {
		t.Fatalf("shadow mode must emit exactly 1 event; got %d", len(evts))
	}

	var payload reinforcementEvent
	if err := json.Unmarshal(evts[0].Data, &payload); err != nil {
		t.Fatalf("unmarshal event data: %v", err)
	}
	if payload.Mode != ReconsolidationModeShadow {
		t.Errorf("event mode: want shadow, got %q", payload.Mode)
	}
	if payload.Count != len(ids) {
		t.Errorf("event count: want %d, got %d", len(ids), payload.Count)
	}
	if payload.Persisted != 0 {
		t.Errorf("shadow mode must have persisted=0; got %d", payload.Persisted)
	}
}

func TestReinforce_PersistMode_WritesAndEmits(t *testing.T) {
	writer := &recordingReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer: writer,
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist},
			floats: map[string]float64{SettingReconsolidationFactor: 0.05},
		},
		Bus: bus,
	})

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	svc.reinforce(context.Background(), ids)

	if writer.callCount() != 1 {
		t.Fatalf("persist mode must write once; got %d", writer.callCount())
	}
	call, _ := writer.lastCall()
	if len(call.IDs) != len(ids) {
		t.Errorf("writer received %d ids; want %d", len(call.IDs), len(ids))
	}
	if call.Factor != 0.05 {
		t.Errorf("writer factor: want 0.05, got %v", call.Factor)
	}

	evts := bus.publishedByType(events.MemoryReinforced)
	if len(evts) != 1 {
		t.Fatalf("persist mode must emit 1 event; got %d", len(evts))
	}
	var payload reinforcementEvent
	_ = json.Unmarshal(evts[0].Data, &payload)
	if payload.Persisted != int64(len(ids)) {
		t.Errorf("persisted: want %d, got %d", len(ids), payload.Persisted)
	}
}

func TestReinforce_WriterErrorDoesNotSuppressEvent(t *testing.T) {
	writer := &recordingReinforcer{returnError: errors.New("db down")}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer: writer,
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist},
			floats: map[string]float64{SettingReconsolidationFactor: 0.02},
		},
		Bus: bus,
	})

	svc.reinforce(context.Background(), []uuid.UUID{uuid.New()})

	// Writer was attempted; event still emitted so observers see the attempt.
	if writer.callCount() != 1 {
		t.Errorf("writer should have been attempted once; got %d", writer.callCount())
	}
	if n := len(bus.publishedByType(events.MemoryReinforced)); n != 1 {
		t.Errorf("event must still fire on write failure; got %d", n)
	}
}

func TestReinforce_EmptyIDs_NoOp(t *testing.T) {
	writer := &recordingReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer:   writer,
		Settings: &staticSettings{values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist}},
		Bus:      bus,
	})

	svc.reinforce(context.Background(), nil)

	if writer.callCount() != 0 {
		t.Errorf("empty id list must not write; got %d", writer.callCount())
	}
	if n := len(bus.publishedByType(events.MemoryReinforced)); n != 0 {
		t.Errorf("empty id list must not emit; got %d", n)
	}
}

func TestReinforce_DisabledWhenNotWired(t *testing.T) {
	svc := &RecallService{}
	// No SetReinforcement call.
	svc.reinforce(context.Background(), []uuid.UUID{uuid.New()})
	// No panic, no effect. We have no dependencies to observe, so the
	// assertion is implicit: this call must return without error and without
	// touching anything.
}

// --- Recall() integration test (spawns the goroutine) ---

// waitForCalls polls count() until it reaches want or times out.
func waitForCalls(t *testing.T, count func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d reinforce calls; got %d", want, count())
}

func TestRecall_FiresReinforcementHook(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	memID := uuid.New()
	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(memID, nsID, "hello world", []string{"t1"}, 0.5, 0, time.Now()),
		},
		memories: map[uuid.UUID]*model.Memory{},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	writer := &recordingReinforcer{}
	bus := &collectingBus{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer: writer,
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist},
			floats: map[string]float64{SettingReconsolidationFactor: 0.02},
		},
		Bus: bus,
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "hello",
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(resp.Memories) == 0 {
		t.Fatal("no results")
	}

	waitForCalls(t, writer.callCount, 1)
}

func TestRecall_NoResults_NoReinforcementFires(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{},
		nsList:   []model.Memory{},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	writer := &recordingReinforcer{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer:   writer,
		Settings: &staticSettings{values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist}},
	})

	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "nothing matches",
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}

	// Give any spurious goroutine a moment to run.
	time.Sleep(50 * time.Millisecond)
	if writer.callCount() != 0 {
		t.Errorf("empty result recall must not reinforce; got %d calls", writer.callCount())
	}
}

func TestRecall_ReinforcementPanic_DoesNotAffectCaller(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	memID := uuid.New()
	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(memID, nsID, "hello", nil, 0.5, 0, time.Now()),
		},
		memories: map[uuid.UUID]*model.Memory{},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)

	// Panicking writer through a bare func (not the recordingReinforcer).
	var attempted int32
	svc.SetReinforcement(&ReinforcementDeps{
		Writer: panicWriter{attempted: &attempted},
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist},
			floats: map[string]float64{SettingReconsolidationFactor: 0.02},
		},
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID: projectID,
		Query:     "hello",
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("recall failed because of reinforcement panic: %v", err)
	}
	if resp == nil {
		t.Fatal("recall returned nil response despite panic")
	}

	// Give the goroutine a chance to run and panic.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&attempted) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&attempted) != 1 {
		t.Error("panicWriter was never invoked; goroutine did not run")
	}
}

type panicWriter struct {
	attempted *int32
}

func (p panicWriter) BumpReinforcement(_ context.Context, _ []uuid.UUID, _ time.Time, _ float64) (int64, error) {
	atomic.StoreInt32(p.attempted, 1)
	panic("simulated reinforcement crash")
}
