package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/provider"
)

// captureBus is a minimal events.EventBus that records every Publish for
// later inspection. Subscribe / Replay / Close return zero values; the
// tracker code path only uses Publish.
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

func (b *captureBus) typesEmitted() []string {
	out := make([]string, 0)
	for _, e := range b.snapshot() {
		out = append(out, e.Type)
	}
	return out
}

func TestCycleTrackerEmitsCallStartedAndCompleted(t *testing.T) {
	bus := &captureBus{}
	cycleID := uuid.New()
	projectID := uuid.New()
	tracker := NewCycleTracker(bus, cycleID, projectID)
	ctx := WithCycleTracker(context.Background(), tracker)

	got, _, err := WrapLLMCall(ctx, nil, "alignment", "claude-test", "target-1",
		func(ctx context.Context) (string, *provider.TokenUsage, error) {
			if tracker.Snapshot() == nil {
				t.Fatal("expected in-flight call to be set during fn execution")
			}
			return "ok", &provider.TokenUsage{PromptTokens: 5, CompletionTokens: 7, TotalTokens: 12}, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ok" {
		t.Fatalf("unexpected return value: %q", got)
	}
	if tracker.Snapshot() != nil {
		t.Fatal("expected in-flight call to be cleared after WrapLLMCall returned")
	}

	emitted := bus.snapshot()
	if len(emitted) != 2 {
		t.Fatalf("expected 2 events, got %d (%v)", len(emitted), bus.typesEmitted())
	}
	if emitted[0].Type != events.DreamCallStarted {
		t.Errorf("first event = %s, want %s", emitted[0].Type, events.DreamCallStarted)
	}
	if emitted[1].Type != events.DreamCallCompleted {
		t.Errorf("second event = %s, want %s", emitted[1].Type, events.DreamCallCompleted)
	}
	wantScope := "project:" + projectID.String()
	if emitted[0].Scope != wantScope || emitted[1].Scope != wantScope {
		t.Errorf("scopes = (%s, %s), want (%s, %s)",
			emitted[0].Scope, emitted[1].Scope, wantScope, wantScope)
	}

	// call_id must match across started/completed.
	startData := decodeData(t, emitted[0])
	completeData := decodeData(t, emitted[1])
	if startData["call_id"] != completeData["call_id"] {
		t.Errorf("call_id mismatch: started=%v completed=%v",
			startData["call_id"], completeData["call_id"])
	}
	if completeData["ok"] != true {
		t.Errorf("ok = %v, want true", completeData["ok"])
	}
	if completeData["operation"] != "alignment" {
		t.Errorf("operation = %v, want alignment", completeData["operation"])
	}
	tokens, ok := completeData["tokens"].(map[string]any)
	if !ok {
		t.Fatalf("tokens missing or wrong shape: %T", completeData["tokens"])
	}
	if int(tokens["total"].(float64)) != 12 {
		t.Errorf("tokens.total = %v, want 12", tokens["total"])
	}
}

func TestCycleTrackerEmitsCompletedOnError(t *testing.T) {
	bus := &captureBus{}
	tracker := NewCycleTracker(bus, uuid.New(), uuid.New())
	ctx := WithCycleTracker(context.Background(), tracker)

	wantErr := errors.New("provider hung up")
	_, _, err := WrapLLMCall(ctx, nil, "synthesis", "model-x", "",
		func(ctx context.Context) (string, *provider.TokenUsage, error) {
			return "", nil, wantErr
		})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if tracker.Snapshot() != nil {
		t.Fatal("expected in-flight call to be cleared even on error")
	}
	emitted := bus.snapshot()
	if len(emitted) != 2 {
		t.Fatalf("expected 2 events, got %d", len(emitted))
	}
	completeData := decodeData(t, emitted[1])
	if completeData["ok"] != false {
		t.Errorf("ok = %v, want false", completeData["ok"])
	}
	if completeData["error"] != wantErr.Error() {
		t.Errorf("error = %v, want %q", completeData["error"], wantErr.Error())
	}
}

func TestWrapLLMCallNoOpsWithoutTracker(t *testing.T) {
	// No tracker bound to context — fn should still run, no events, no panic.
	got, _, err := WrapLLMCall(context.Background(), nil, "synthesis", "model-x", "",
		func(ctx context.Context) (int, *provider.TokenUsage, error) {
			return 42, nil, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Errorf("got = %d, want 42", got)
	}
}

func TestCycleTrackerEmitPhaseAndHeartbeat(t *testing.T) {
	bus := &captureBus{}
	tracker := NewCycleTracker(bus, uuid.New(), uuid.New())
	tracker.SetPhase("consolidation")

	tracker.EmitPhaseStarted(context.Background(), "consolidation", 100)
	tracker.EmitHeartbeat(context.Background(), 250)
	tracker.EmitPhaseCompleted(context.Background(), "consolidation", 250, 5, 12000, false, "")

	emitted := bus.snapshot()
	if len(emitted) != 3 {
		t.Fatalf("expected 3 events, got %d (%v)", len(emitted), bus.typesEmitted())
	}
	want := []string{
		events.DreamPhaseStarted,
		events.DreamCycleHeartbeat,
		events.DreamPhaseCompleted,
	}
	for i, w := range want {
		if emitted[i].Type != w {
			t.Errorf("event[%d] = %s, want %s", i, emitted[i].Type, w)
		}
	}

	heartbeat := decodeData(t, emitted[1])
	if heartbeat["phase"] != "consolidation" {
		t.Errorf("heartbeat.phase = %v, want consolidation", heartbeat["phase"])
	}
	if int(heartbeat["tokens_used"].(float64)) != 250 {
		t.Errorf("heartbeat.tokens_used = %v, want 250", heartbeat["tokens_used"])
	}
	if _, hasInFlight := heartbeat["in_flight_call"]; hasInFlight {
		t.Error("heartbeat carried in_flight_call when none was set")
	}
}

func TestHeartbeatIncludesInFlightCallSnapshot(t *testing.T) {
	bus := &captureBus{}
	tracker := NewCycleTracker(bus, uuid.New(), uuid.New())
	tracker.SetPhase("contradiction_detection")
	ctx := WithCycleTracker(context.Background(), tracker)

	// Run a slow call concurrently, snapshot heartbeat while it's in flight.
	heartbeatSeen := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = WrapLLMCall(ctx, nil, "contradiction_judge", "m", "id-1",
			func(ctx context.Context) (int, *provider.TokenUsage, error) {
				// Wait for the test goroutine to capture an in-flight heartbeat.
				<-heartbeatSeen
				return 0, &provider.TokenUsage{TotalTokens: 1}, nil
			})
	}()

	// Spin until Snapshot picks up the in-flight call (short, bounded).
	deadline := time.Now().Add(time.Second)
	for tracker.Snapshot() == nil {
		if time.Now().After(deadline) {
			heartbeatSeen <- struct{}{}
			<-done
			t.Fatal("in-flight call never became visible to tracker")
		}
		time.Sleep(time.Millisecond)
	}
	tracker.EmitHeartbeat(context.Background(), 99)
	heartbeatSeen <- struct{}{}
	<-done

	// The heartbeat we triggered must include in_flight_call.
	var heartbeat events.Event
	for _, e := range bus.snapshot() {
		if e.Type == events.DreamCycleHeartbeat {
			heartbeat = e
			break
		}
	}
	if heartbeat.Type == "" {
		t.Fatal("no heartbeat event was emitted")
	}
	data := decodeData(t, heartbeat)
	in, ok := data["in_flight_call"].(map[string]any)
	if !ok {
		t.Fatalf("in_flight_call missing or wrong shape: %T", data["in_flight_call"])
	}
	if in["operation"] != "contradiction_judge" {
		t.Errorf("in_flight_call.operation = %v, want contradiction_judge", in["operation"])
	}
	if in["target_id"] != "id-1" {
		t.Errorf("in_flight_call.target_id = %v, want id-1", in["target_id"])
	}
}

func TestWrapLLMCallChargesBudget(t *testing.T) {
	budget := NewTokenBudget(1000, 500)
	got, usage, err := WrapLLMCall(context.Background(), budget, "synth", "m", "",
		func(ctx context.Context) (string, *provider.TokenUsage, error) {
			return "ok", &provider.TokenUsage{PromptTokens: 100, CompletionTokens: 23, TotalTokens: 123}, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ok" {
		t.Errorf("got = %q, want ok", got)
	}
	if usage.TotalTokens != 123 {
		t.Errorf("usage.TotalTokens = %d, want 123", usage.TotalTokens)
	}
	if budget.Used() != 123 {
		t.Errorf("budget.Used() = %d, want 123 (WrapLLMCall must Spend internally)", budget.Used())
	}
}

func TestWrapLLMCallSurfacesBudgetExhaustionWhenCallSucceeded(t *testing.T) {
	budget := NewTokenBudget(50, 500) // tiny budget
	_, _, err := WrapLLMCall(context.Background(), budget, "synth", "m", "",
		func(ctx context.Context) (int, *provider.TokenUsage, error) {
			return 0, &provider.TokenUsage{TotalTokens: 100}, nil
		})
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("expected ErrBudgetExhausted to surface, got %v", err)
	}
}

func TestWrapLLMCallPreservesInnerErrorOverBudgetExhaustion(t *testing.T) {
	budget := NewTokenBudget(50, 500)
	innerErr := errors.New("provider 500")
	_, _, err := WrapLLMCall(context.Background(), budget, "synth", "m", "",
		func(ctx context.Context) (int, *provider.TokenUsage, error) {
			// Real LLM happened, returned usage AND an error. Budget should
			// still be charged but the original error must surface.
			return 0, &provider.TokenUsage{TotalTokens: 100}, innerErr
		})
	if !errors.Is(err, innerErr) {
		t.Fatalf("expected inner error to surface, got %v", err)
	}
	if budget.Used() != 100 {
		t.Errorf("budget.Used() = %d, want 100 (still charged on error path)", budget.Used())
	}
}

func TestWrapLLMCallNilBudgetSkipsSpend(t *testing.T) {
	_, _, err := WrapLLMCall(context.Background(), nil, "synth", "m", "",
		func(ctx context.Context) (int, *provider.TokenUsage, error) {
			return 0, &provider.TokenUsage{TotalTokens: 999}, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEmitPhaseProgressShapesPayload(t *testing.T) {
	bus := &captureBus{}
	tracker := NewCycleTracker(bus, uuid.New(), uuid.New())
	tracker.SetPhase("pruning")
	tracker.EmitPhaseProgress(context.Background(), 250, 1000, "memories")

	emitted := bus.snapshot()
	if len(emitted) != 1 || emitted[0].Type != events.DreamPhaseProgress {
		t.Fatalf("expected one DreamPhaseProgress event, got %+v", bus.typesEmitted())
	}
	d := decodeData(t, emitted[0])
	if d["phase"] != "pruning" || d["label"] != "memories" {
		t.Errorf("payload phase/label wrong: %+v", d)
	}
	if int(d["current"].(float64)) != 250 || int(d["total"].(float64)) != 1000 {
		t.Errorf("payload current/total wrong: %+v", d)
	}
}

func decodeData(t *testing.T, e events.Event) map[string]any {
	t.Helper()
	if len(e.Data) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		t.Fatalf("failed to decode event data: %v", err)
	}
	return m
}
