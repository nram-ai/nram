package events

// integration_test.go — comprehensive black-box tests for the SSE event
// system.  Tests are written with zero trust toward the implementation:
// every field, ordering, boundary condition, and concurrent behaviour is
// verified explicitly.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// newEvt builds an Event with the given id/type/scope.  Data is always a
// non-empty JSON object so nil-Data edge cases are not accidentally tested by
// helper-built events.
func newEvt(id, typ, scope string) Event {
	return Event{
		ID:        id,
		Type:      typ,
		Scope:     scope,
		Data:      json.RawMessage(`{"x":1}`),
		Timestamp: time.Now().UTC(),
	}
}

// recv reads one event from ch within timeout, failing the test if nothing
// arrives in time.
func recv(t *testing.T, ch <-chan Event, timeout time.Duration) Event {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out (%s) waiting for event on channel", timeout)
		panic("unreachable")
	}
}

// assertNone verifies that no event arrives on ch within window.
func assertNone(t *testing.T, ch <-chan Event, window time.Duration) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Errorf("unexpected event received: id=%s type=%s scope=%s", ev.ID, ev.Type, ev.Scope)
	case <-time.After(window):
	}
}

// ─── EventBus Core ────────────────────────────────────────────────────────────

// TestMemoryBus_PublishAndReceive verifies that every field of a published
// event arrives unchanged at the subscriber.
func TestMemoryBus_PublishAndReceive(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	want := Event{
		ID:        "integration-evt-1",
		Type:      MemoryCreated,
		Scope:     "project:test-scope",
		Data:      json.RawMessage(`{"memory_id":"abc"}`),
		Timestamp: time.Now().UTC().Round(time.Millisecond),
	}
	if err := bus.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := recv(t, ch, 2*time.Second)

	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %q", got.ID, want.ID)
	}
	if got.Type != want.Type {
		t.Errorf("Type: got %q, want %q", got.Type, want.Type)
	}
	if got.Scope != want.Scope {
		t.Errorf("Scope: got %q, want %q", got.Scope, want.Scope)
	}
	if string(got.Data) != string(want.Data) {
		t.Errorf("Data: got %s, want %s", got.Data, want.Data)
	}
	if got.Timestamp.IsZero() {
		t.Error("Timestamp must not be zero")
	}
}

// TestMemoryBus_ScopeFiltering_Exact verifies that a subscriber with an exact
// scope string only receives events whose Scope equals that string.
func TestMemoryBus_ScopeFiltering_Exact(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()

	chMatch, cancelMatch, err := bus.Subscribe(ctx, "project:abc")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancelMatch)

	// Publish a matching event.
	match := newEvt("e-match", MemoryCreated, "project:abc")
	if err := bus.Publish(ctx, match); err != nil {
		t.Fatalf("Publish match: %v", err)
	}

	// Publish a non-matching event.
	nonMatch := newEvt("e-nomatch", MemoryCreated, "project:xyz")
	if err := bus.Publish(ctx, nonMatch); err != nil {
		t.Fatalf("Publish non-match: %v", err)
	}

	got := recv(t, chMatch, 2*time.Second)
	if got.ID != "e-match" {
		t.Errorf("got event ID %q, want e-match", got.ID)
	}

	// After draining the matched event no second event should arrive.
	assertNone(t, chMatch, 80*time.Millisecond)
}

// TestMemoryBus_ScopeFiltering_Prefix verifies that a subscriber with a prefix
// scope (e.g. "project:") receives all events whose Scope starts with that prefix.
func TestMemoryBus_ScopeFiltering_Prefix(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "project:")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	events := []Event{
		newEvt("p1", MemoryCreated, "project:aaa"),
		newEvt("p2", MemoryUpdated, "project:bbb"),
		newEvt("p3", MemoryDeleted, "project:ccc"),
		newEvt("x1", MemoryCreated, "entity:xxx"),  // must not arrive
		newEvt("x2", EntityCreated, "other:zzz"),   // must not arrive
	}
	for i := range events {
		if err := bus.Publish(ctx, events[i]); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	receivedIDs := make(map[string]bool)
	deadline := time.After(2 * time.Second)
	for len(receivedIDs) < 3 {
		select {
		case ev := <-ch:
			receivedIDs[ev.ID] = true
		case <-deadline:
			t.Fatalf("timed out: only received IDs %v, want p1,p2,p3", receivedIDs)
		}
	}

	for _, want := range []string{"p1", "p2", "p3"} {
		if !receivedIDs[want] {
			t.Errorf("did not receive expected event %q", want)
		}
	}

	// Non-matching events must not arrive.
	assertNone(t, ch, 80*time.Millisecond)
	for _, bad := range []string{"x1", "x2"} {
		if receivedIDs[bad] {
			t.Errorf("received event %q that should have been filtered", bad)
		}
	}
}

// TestMemoryBus_EmptyScope_ReceivesAll verifies that subscribing with "" delivers
// every published event regardless of Scope.
func TestMemoryBus_EmptyScope_ReceivesAll(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	scopes := []string{"project:a", "ns:b", "entity:c", "other:d", ""}
	for i, sc := range scopes {
		e := newEvt(fmt.Sprintf("evt-%d", i), MemoryCreated, sc)
		if err := bus.Publish(ctx, e); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	received := 0
	deadline := time.After(2 * time.Second)
	for received < len(scopes) {
		select {
		case <-ch:
			received++
		case <-deadline:
			t.Fatalf("timed out: received %d/%d events", received, len(scopes))
		}
	}
}

// TestMemoryBus_MultipleSubscribers verifies that two subscribers with different
// scopes each receive only their matching events.
func TestMemoryBus_MultipleSubscribers(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()

	chA, cancelA, err := bus.Subscribe(ctx, "project:")
	if err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	t.Cleanup(cancelA)

	chB, cancelB, err := bus.Subscribe(ctx, "entity:")
	if err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}
	t.Cleanup(cancelB)

	if err := bus.Publish(ctx, newEvt("ep1", MemoryCreated, "project:1")); err != nil {
		t.Fatalf("Publish project: %v", err)
	}
	if err := bus.Publish(ctx, newEvt("ee1", EntityCreated, "entity:1")); err != nil {
		t.Fatalf("Publish entity: %v", err)
	}

	// Subscriber A should get only the project event.
	gotA := recv(t, chA, 2*time.Second)
	if gotA.ID != "ep1" {
		t.Errorf("subscriber A got %q, want ep1", gotA.ID)
	}
	assertNone(t, chA, 80*time.Millisecond)

	// Subscriber B should get only the entity event.
	gotB := recv(t, chB, 2*time.Second)
	if gotB.ID != "ee1" {
		t.Errorf("subscriber B got %q, want ee1", gotB.ID)
	}
	assertNone(t, chB, 80*time.Millisecond)
}

// TestMemoryBus_CancelSubscription verifies that after cancel() is called the
// channel is closed and no further events are delivered.
func TestMemoryBus_CancelSubscription(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cancel()

	// Channel must be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after cancel")
		}
	case <-time.After(time.Second):
		t.Error("channel not closed within 1s after cancel")
	}

	// Publish after cancel — event must not appear (channel already closed).
	// We verify this is panic-free and does not block.
	_ = bus.Publish(ctx, newEvt("after-cancel", MemoryCreated, "test"))

	// Double-cancel must not panic.
	cancel()
}

// TestMemoryBus_CloseStopsDelivery verifies that closing the bus closes all
// subscriber channels and that subsequent Publish returns ErrBusClosed.
func TestMemoryBus_CloseStopsDelivery(t *testing.T) {
	bus := NewMemoryBus(0, 0)

	ctx := context.Background()
	ch, _, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Channel must be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after bus.Close()")
		}
	case <-time.After(time.Second):
		t.Error("channel not closed within 1s after bus.Close()")
	}

	// Publish on closed bus must return ErrBusClosed.
	if err := bus.Publish(ctx, newEvt("x", MemoryCreated, "")); err != ErrBusClosed {
		t.Errorf("Publish after Close: got %v, want ErrBusClosed", err)
	}

	// Subscribe on closed bus must return ErrBusClosed.
	_, _, err = bus.Subscribe(ctx, "")
	if err != ErrBusClosed {
		t.Errorf("Subscribe after Close: got %v, want ErrBusClosed", err)
	}
}

// TestMemoryBus_ConcurrentPublish_AllReceived publishes events from multiple
// goroutines concurrently and verifies all events are received without
// deadlocks or races.  The total is kept below the subscriber channel buffer
// size (fallbackSubscriberBufferSize=64) so that the bus's documented drop-on-full
// behaviour does not interfere with the count assertion.  The race detector
// validates that concurrent Publish calls are safe.
func TestMemoryBus_ConcurrentPublish_AllReceived(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()

	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	// Stay well under fallbackSubscriberBufferSize (64) so no events are dropped.
	const goroutines = 5
	const perGoroutine = 10
	const total = goroutines * perGoroutine // 50, safely < 64

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := fmt.Sprintf("g%d-e%d", g, i)
				_ = bus.Publish(ctx, newEvt(id, MemoryCreated, "project:concurrent"))
			}
		}()
	}
	wg.Wait()

	received := 0
	deadline := time.After(5 * time.Second)
	for received < total {
		select {
		case <-ch:
			received++
		case <-deadline:
			t.Fatalf("timed out: received %d/%d concurrent events", received, total)
		}
	}
}

// TestMemoryBus_FullChannelDropsEvents verifies that when the subscriber
// channel is full, additional events are dropped without panicking or
// deadlocking, and Publish still returns nil.
func TestMemoryBus_FullChannelDropsEvents(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	// Fill the channel buffer exactly.
	for i := 0; i < fallbackSubscriberBufferSize; i++ {
		e := newEvt(fmt.Sprintf("fill-%d", i), MemoryCreated, "test")
		if err := bus.Publish(ctx, e); err != nil {
			t.Fatalf("Publish fill[%d]: %v", i, err)
		}
	}

	// Overflow events — must not block and must return nil.
	for i := 0; i < 5; i++ {
		e := newEvt(fmt.Sprintf("overflow-%d", i), MemoryCreated, "test")
		if err := bus.Publish(ctx, e); err != nil {
			t.Fatalf("Publish overflow[%d] returned error: %v", i, err)
		}
	}

	// Drain all buffered events.
	count := 0
drain:
	for {
		select {
		case <-ch:
			count++
		case <-time.After(50 * time.Millisecond):
			break drain
		}
	}

	if count != fallbackSubscriberBufferSize {
		t.Errorf("drained %d events, want exactly %d (overflows must be dropped)", count, fallbackSubscriberBufferSize)
	}
}

// TestMemoryBus_PublishAfterClose verifies that Publish on a closed bus returns
// ErrBusClosed, not nil or any other error.
func TestMemoryBus_PublishAfterClose(t *testing.T) {
	bus := NewMemoryBus(0, 0)

	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := bus.Publish(context.Background(), newEvt("x", MemoryCreated, ""))
	if err == nil {
		t.Fatal("expected error after publishing to closed bus, got nil")
	}
	if err != ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}
}

// ─── Replay Buffer ────────────────────────────────────────────────────────────

// TestReplayBuffer_StoresEvents verifies that added events can be retrieved in
// insertion order via Since("").
func TestReplayBuffer_StoresEvents(t *testing.T) {
	rb := NewReplayBuffer(16)

	ids := []string{"r1", "r2", "r3", "r4"}
	for _, id := range ids {
		rb.Add(newEvt(id, MemoryCreated, "test"))
	}

	got := rb.Since("")
	if len(got) != len(ids) {
		t.Fatalf("Since(\"\") returned %d events, want %d", len(got), len(ids))
	}
	for i, want := range ids {
		if got[i].ID != want {
			t.Errorf("event[%d].ID = %q, want %q", i, got[i].ID, want)
		}
	}
}

// TestReplayBuffer_Overflow verifies that when more events than the capacity
// are added the oldest events are evicted and only the newest capacity events
// are retained, in order.
func TestReplayBuffer_Overflow(t *testing.T) {
	const cap = 5
	rb := NewReplayBuffer(cap)

	for i := 0; i < 10; i++ {
		rb.Add(newEvt(fmt.Sprintf("e%d", i), MemoryCreated, "test"))
	}

	got := rb.Since("")
	if len(got) != cap {
		t.Fatalf("expected %d events after overflow, got %d", cap, len(got))
	}
	// Should contain the last 5 events: e5…e9
	for i, want := range []string{"e5", "e6", "e7", "e8", "e9"} {
		if got[i].ID != want {
			t.Errorf("got[%d].ID = %q, want %q", i, got[i].ID, want)
		}
	}
}

// TestReplayBuffer_ReplayFromID verifies that Since(id) returns only events
// strictly after the event with the given ID.
func TestReplayBuffer_ReplayFromID(t *testing.T) {
	rb := NewReplayBuffer(32)

	for i := 1; i <= 10; i++ {
		rb.Add(newEvt(fmt.Sprintf("e%d", i), MemoryCreated, "test"))
	}

	got := rb.Since("e5")
	if len(got) != 5 {
		t.Fatalf("Since(e5) returned %d events, want 5", len(got))
	}
	for i, want := range []string{"e6", "e7", "e8", "e9", "e10"} {
		if got[i].ID != want {
			t.Errorf("got[%d].ID = %q, want %q", i, got[i].ID, want)
		}
	}
}

// TestReplayBuffer_ReplayFromLastID verifies that Since(id) when id is the last
// buffered event returns an empty (or nil) slice.
func TestReplayBuffer_ReplayFromLastID(t *testing.T) {
	rb := NewReplayBuffer(16)
	rb.Add(newEvt("e1", MemoryCreated, "test"))
	rb.Add(newEvt("e2", MemoryCreated, "test"))

	got := rb.Since("e2")
	if len(got) != 0 {
		t.Errorf("Since(last id) returned %d events, want 0: %v", len(got), got)
	}
}

// TestReplayBuffer_ReplayUnknownID verifies that Since with an ID not present
// in the buffer returns all buffered events (spec: return all if not found).
func TestReplayBuffer_ReplayUnknownID(t *testing.T) {
	rb := NewReplayBuffer(16)
	for i := 1; i <= 5; i++ {
		rb.Add(newEvt(fmt.Sprintf("e%d", i), MemoryCreated, "test"))
	}

	got := rb.Since("nonexistent-id")
	if len(got) != 5 {
		t.Errorf("Since(unknown id) returned %d events, want all 5", len(got))
	}
}

// TestReplayBuffer_EmptyBuffer verifies that Since on an empty buffer returns
// nil or empty slice without panicking.
func TestReplayBuffer_EmptyBuffer(t *testing.T) {
	rb := NewReplayBuffer(16)

	got := rb.Since("")
	if len(got) != 0 {
		t.Errorf("Since(\"\") on empty buffer returned %d events, want 0", len(got))
	}

	got = rb.Since("some-id")
	if len(got) != 0 {
		t.Errorf("Since(id) on empty buffer returned %d events, want 0", len(got))
	}
}

// TestReplayBuffer_OrderPreservedAfterMultipleOverflows verifies insertion order
// is preserved through multiple wrap-arounds of the ring buffer.
func TestReplayBuffer_OrderPreservedAfterMultipleOverflows(t *testing.T) {
	const cap = 4
	rb := NewReplayBuffer(cap)

	// Write 3× the capacity so the ring wraps multiple times.
	for i := 0; i < cap*3; i++ {
		rb.Add(newEvt(fmt.Sprintf("e%d", i), MemoryCreated, "test"))
	}

	got := rb.Since("")
	if len(got) != cap {
		t.Fatalf("expected %d events, got %d", cap, len(got))
	}
	// Events should be in insertion order (oldest retained first).
	for i := 0; i < cap; i++ {
		wantID := fmt.Sprintf("e%d", cap*3-cap+i)
		if got[i].ID != wantID {
			t.Errorf("got[%d].ID = %q, want %q", i, got[i].ID, wantID)
		}
	}
}

// ─── Emit Helper ─────────────────────────────────────────────────────────────

// TestEmit_NilBus_NoOp verifies that calling Emit with a nil bus never panics.
func TestEmit_NilBus_NoOp(t *testing.T) {
	// Must not panic.
	Emit(context.Background(), nil, MemoryCreated, "project:abc", map[string]string{"key": "val"})
	Emit(context.Background(), nil, EntityCreated, "", nil)
}

// TestEmit_PublishesEvent_FieldsVerified verifies Emit publishes an event with
// the correct Type and Scope, a non-empty UUID as ID, a non-zero Timestamp,
// and that the Data is properly populated.
func TestEmit_PublishesEvent_FieldsVerified(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	Emit(ctx, bus, MemoryUpdated, "project:xyz", map[string]string{"foo": "bar"})

	got := recv(t, ch, 2*time.Second)
	if got.Type != MemoryUpdated {
		t.Errorf("Type: got %q, want %q", got.Type, MemoryUpdated)
	}
	if got.Scope != "project:xyz" {
		t.Errorf("Scope: got %q, want %q", got.Scope, "project:xyz")
	}
	if got.ID == "" {
		t.Error("ID must be non-empty")
	}
	if _, err := uuid.Parse(got.ID); err != nil {
		t.Errorf("ID %q is not a valid UUID: %v", got.ID, err)
	}
	if got.Timestamp.IsZero() {
		t.Error("Timestamp must not be zero")
	}
}

// TestEmit_DataSerialization verifies that map data passed to Emit arrives as
// valid JSON in the event's Data field.
func TestEmit_DataSerialization(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	Emit(context.Background(), bus, MemoryCreated, "project:ds",
		map[string]interface{}{
			"string_field": "hello",
			"number_field": 42,
			"bool_field":   true,
		})

	got := recv(t, ch, 2*time.Second)

	var m map[string]interface{}
	if err := json.Unmarshal(got.Data, &m); err != nil {
		t.Fatalf("Data is not valid JSON: %v (raw: %s)", err, got.Data)
	}

	if m["string_field"] != "hello" {
		t.Errorf("string_field: got %v, want hello", m["string_field"])
	}
	if m["number_field"] != float64(42) {
		t.Errorf("number_field: got %v, want 42", m["number_field"])
	}
	if m["bool_field"] != true {
		t.Errorf("bool_field: got %v, want true", m["bool_field"])
	}
}

// TestEmit_AllEventTypes verifies that every defined event type constant
// survives a round-trip through Emit and arrives with the exact string value.
func TestEmit_AllEventTypes(t *testing.T) {
	allTypes := []string{
		MemoryCreated,
		MemoryEnriched,
		MemoryUpdated,
		MemoryDeleted,
		EntityCreated,
		RelationshipCreated,
		RelationshipExpired,
		ConflictDetected,
		EnrichmentFailed,
	}

	for _, typ := range allTypes {
		typ := typ
		t.Run(typ, func(t *testing.T) {
			bus := NewMemoryBus(0, 0)
			t.Cleanup(func() { _ = bus.Close() })

			ch, cancel, err := bus.Subscribe(context.Background(), "")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			t.Cleanup(cancel)

			Emit(context.Background(), bus, typ, "project:types", map[string]string{"t": typ})

			got := recv(t, ch, 2*time.Second)
			if got.Type != typ {
				t.Errorf("Type: got %q, want %q", got.Type, typ)
			}
		})
	}
}

// TestEmit_TwoEventsHaveDifferentIDs verifies that each Emit call produces a
// unique event ID (no static/reused IDs).
func TestEmit_TwoEventsHaveDifferentIDs(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	Emit(context.Background(), bus, MemoryCreated, "project:ids", nil)
	Emit(context.Background(), bus, MemoryCreated, "project:ids", nil)

	e1 := recv(t, ch, 2*time.Second)
	e2 := recv(t, ch, 2*time.Second)

	if e1.ID == e2.ID {
		t.Errorf("two Emit calls produced the same event ID: %q", e1.ID)
	}
}

// ─── Webhook Delivery ─────────────────────────────────────────────────────────

// newWebhookForNS builds a model.Webhook targeting the provided URL, scoped to
// ns:<nsID> and listening for the given event type.
func newWebhookForNS(id, url string, nsID uuid.UUID, eventTypes []string, secret *string) model.Webhook {
	return model.Webhook{
		ID:     uuid.MustParse(id),
		URL:    url,
		Secret: secret,
		Events: eventTypes,
		Scope:  "ns:" + nsID.String(),
		Active: true,
	}
}

// publishToNS publishes an event whose Scope includes nsID so the webhook
// deliverer's parseNamespaceID can extract it and match against "ns:<nsID>".
func publishToNS(t *testing.T, bus EventBus, evtID, typ string, nsID uuid.UUID) {
	t.Helper()
	e := Event{
		ID:        evtID,
		Type:      typ,
		Scope:     "project:" + nsID.String(),
		Data:      json.RawMessage(`{"source":"integration"}`),
		Timestamp: time.Now().UTC(),
	}
	if err := bus.Publish(context.Background(), e); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// TestWebhookDeliverer_DeliversEvent verifies that when an event matching the
// webhook's namespace and event-type is published, the deliverer POSTs to the
// target URL with the correct headers and a valid JSON body.
func TestWebhookDeliverer_DeliversEvent(t *testing.T) {
	var (
		mu              sync.Mutex
		receivedBody    []byte
		receivedHeaders http.Header
		callCount       atomic.Int32
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		mu.Lock()
		receivedHeaders = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		receivedBody = b
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	nsID := uuid.New()
	whID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	evtID := uuid.New().String()
	store := newMockWebhookStore(newWebhookForNS(whID.String(), srv.URL, nsID, []string{MemoryCreated}, nil))

	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	d := NewWebhookDeliverer(bus, store, WithHTTPClient(srv.Client()), WithTimeout(5*time.Second))
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(ctxCancel)

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = d.Stop() })

	publishToNS(t, bus, evtID, MemoryCreated, nsID)

	waitFor(t, func() bool { return callCount.Load() >= 1 }, 5*time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify headers.
	if ct := receivedHeaders.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
	if xev := receivedHeaders.Get("X-NRAM-Event"); xev != MemoryCreated {
		t.Errorf("X-NRAM-Event: got %q, want %q", xev, MemoryCreated)
	}
	if xdel := receivedHeaders.Get("X-NRAM-Delivery"); xdel != evtID {
		t.Errorf("X-NRAM-Delivery: got %q, want %q", xdel, evtID)
	}

	// No signature header expected when secret is nil.
	if sig := receivedHeaders.Get("X-NRAM-Signature"); sig != "" {
		t.Errorf("X-NRAM-Signature should be absent when no secret, got %q", sig)
	}

	// Body must deserialise to a valid Event.
	var decoded Event
	if err := json.Unmarshal(receivedBody, &decoded); err != nil {
		t.Fatalf("body is not a valid Event JSON: %v (raw: %s)", err, receivedBody)
	}
	if decoded.ID != evtID {
		t.Errorf("decoded.ID = %q, want %q", decoded.ID, evtID)
	}
	if decoded.Type != MemoryCreated {
		t.Errorf("decoded.Type = %q, want %q", decoded.Type, MemoryCreated)
	}
}

// TestWebhookDeliverer_HMAC_Signature verifies that when the webhook has a
// secret the X-NRAM-Signature header is set and its value matches
// sha256=ComputeHMACSHA256(secret, body).
func TestWebhookDeliverer_HMAC_Signature(t *testing.T) {
	var (
		mu              sync.Mutex
		receivedBody    []byte
		receivedHeaders http.Header
		callCount       atomic.Int32
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		mu.Lock()
		receivedHeaders = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		receivedBody = b
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	secret := "super-secret-key-123"
	nsID := uuid.New()
	whID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	store := newMockWebhookStore(newWebhookForNS(whID.String(), srv.URL, nsID, []string{MemoryUpdated}, &secret))

	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	d := NewWebhookDeliverer(bus, store, WithHTTPClient(srv.Client()), WithTimeout(5*time.Second))
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(ctxCancel)

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = d.Stop() })

	evtID := uuid.New().String()
	publishToNS(t, bus, evtID, MemoryUpdated, nsID)

	waitFor(t, func() bool { return callCount.Load() >= 1 }, 5*time.Second)

	mu.Lock()
	defer mu.Unlock()

	sig := receivedHeaders.Get("X-NRAM-Signature")
	if sig == "" {
		t.Fatal("X-NRAM-Signature header is absent")
	}

	expectedSig := "sha256=" + ComputeHMACSHA256(secret, receivedBody)
	if sig != expectedSig {
		t.Errorf("X-NRAM-Signature: got %q, want %q", sig, expectedSig)
	}

	// Verify the signature actually validates against the secret.
	if !isValidHMAC(secret, receivedBody, sig) {
		t.Error("HMAC signature verification failed")
	}
}

// isValidHMAC is a test helper that verifies a "sha256=<hex>" signature.
func isValidHMAC(secret string, body []byte, header string) bool {
	const prefix = "sha256="
	if len(header) <= len(prefix) {
		return false
	}
	expected := ComputeHMACSHA256(secret, body)
	return header == prefix+expected
}

// TestWebhookDeliverer_RetryOnFailure verifies that a 500 on the first attempt
// is retried and that success on the second attempt causes RecordSuccess to be
// called exactly once.
func TestWebhookDeliverer_RetryOnFailure(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	nsID := uuid.New()
	whID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	store := newMockWebhookStore(newWebhookForNS(whID.String(), srv.URL, nsID, []string{MemoryCreated}, nil))

	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	d := NewWebhookDeliverer(bus, store,
		WithHTTPClient(srv.Client()),
		WithMaxRetries(3),
		WithTimeout(5*time.Second),
	)
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(ctxCancel)

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = d.Stop() })

	publishToNS(t, bus, uuid.New().String(), MemoryCreated, nsID)

	// Wait for at least 2 HTTP attempts.
	waitFor(t, func() bool { return attempts.Load() >= 2 }, 10*time.Second)
	// Wait for success to be recorded.
	waitFor(t, func() bool { return store.successCount(whID) == 1 }, 5*time.Second)

	if got := store.failureCount(whID); got != 0 {
		t.Errorf("expected 0 failures, got %d", got)
	}
	if got := store.successCount(whID); got != 1 {
		t.Errorf("expected 1 success, got %d", got)
	}
	if got := attempts.Load(); got < 2 {
		t.Errorf("expected >= 2 attempts, got %d", got)
	}
}

// TestWebhookDeliverer_ScopeFiltering verifies that a webhook scoped to a
// specific namespace does NOT receive events published for a different namespace.
func TestWebhookDeliverer_ScopeFiltering(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	targetNS := uuid.New()
	otherNS := uuid.New()
	whID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	store := newMockWebhookStore(newWebhookForNS(whID.String(), srv.URL, targetNS, []string{MemoryCreated}, nil))

	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	d := NewWebhookDeliverer(bus, store, WithHTTPClient(srv.Client()), WithTimeout(5*time.Second))
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(ctxCancel)

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = d.Stop() })

	// Publish event scoped to a DIFFERENT namespace.
	publishToNS(t, bus, uuid.New().String(), MemoryCreated, otherNS)

	// Allow enough time for any (erroneous) delivery.
	time.Sleep(300 * time.Millisecond)

	if callCount.Load() != 0 {
		t.Errorf("expected 0 webhook calls for mismatched namespace, got %d", callCount.Load())
	}
	if store.successCount(whID) != 0 {
		t.Errorf("expected 0 successes, got %d", store.successCount(whID))
	}
}

// TestWebhookDeliverer_EventTypeFiltering verifies that a webhook registered
// for MemoryCreated does NOT receive MemoryDeleted events.
func TestWebhookDeliverer_EventTypeFiltering(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	nsID := uuid.New()
	whID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	// Webhook only subscribes to MemoryCreated.
	store := newMockWebhookStore(newWebhookForNS(whID.String(), srv.URL, nsID, []string{MemoryCreated}, nil))

	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	d := NewWebhookDeliverer(bus, store, WithHTTPClient(srv.Client()), WithTimeout(5*time.Second))
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(ctxCancel)

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = d.Stop() })

	// Publish MemoryDeleted — must not trigger the webhook.
	publishToNS(t, bus, uuid.New().String(), MemoryDeleted, nsID)

	time.Sleep(300 * time.Millisecond)

	if callCount.Load() != 0 {
		t.Errorf("expected 0 webhook calls for unregistered event type, got %d", callCount.Load())
	}
}

// TestWebhookDeliverer_StopCancelsDelivery verifies that after Stop() is called
// no further HTTP requests are made.
func TestWebhookDeliverer_StopCancelsDelivery(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	nsID := uuid.New()
	whID := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	store := newMockWebhookStore(newWebhookForNS(whID.String(), srv.URL, nsID, []string{MemoryCreated}, nil))

	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	d := NewWebhookDeliverer(bus, store, WithHTTPClient(srv.Client()), WithTimeout(5*time.Second))
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(ctxCancel)

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Stop before publishing.
	if err := d.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	publishToNS(t, bus, uuid.New().String(), MemoryCreated, nsID)

	time.Sleep(300 * time.Millisecond)

	if callCount.Load() != 0 {
		t.Errorf("expected 0 HTTP calls after Stop(), got %d", callCount.Load())
	}
}

// ─── Integration: Bus + ReplayBuffer via EventBus.Replay ─────────────────────

// TestBusReplay_PopulatedByPublish verifies that events published to the bus
// are stored in the replay buffer and accessible via bus.Replay.
func TestBusReplay_PopulatedByPublish(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		e := newEvt(fmt.Sprintf("r%d", i), MemoryCreated, "test")
		if err := bus.Publish(ctx, e); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	got := bus.Replay("")
	if len(got) != 5 {
		t.Fatalf("Replay(\"\") returned %d events, want 5", len(got))
	}
}

// TestBusReplay_SinceID verifies that bus.Replay(id) returns only events after
// the given ID.
func TestBusReplay_SinceID(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		e := newEvt(fmt.Sprintf("rr%d", i), MemoryCreated, "test")
		if err := bus.Publish(ctx, e); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	got := bus.Replay("rr3")
	if len(got) != 3 {
		t.Fatalf("Replay(rr3) returned %d events, want 3", len(got))
	}
	for i, wantID := range []string{"rr4", "rr5", "rr6"} {
		if got[i].ID != wantID {
			t.Errorf("got[%d].ID = %q, want %q", i, got[i].ID, wantID)
		}
	}
}

// TestBusReplay_ClosedBusRetainsBuffer verifies that events added before
// Close() are still retrievable via Replay after Close.
func TestBusReplay_ClosedBusRetainsBuffer(t *testing.T) {
	bus := NewMemoryBus(0, 0)

	ctx := context.Background()
	e := newEvt("persist-1", MemoryCreated, "test")
	if err := bus.Publish(ctx, e); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := bus.Replay("")
	if len(got) == 0 {
		t.Error("Replay on closed bus should still return buffered events")
	}
	if got[0].ID != "persist-1" {
		t.Errorf("got[0].ID = %q, want persist-1", got[0].ID)
	}
}

// ─── matchesScope unit tests (package-level function) ────────────────────────

// TestMatchesScope verifies every code path of the matchesScope helper.
func TestMatchesScope(t *testing.T) {
	cases := []struct {
		subScope   string
		eventScope string
		want       bool
	}{
		// Empty subscriber scope matches everything.
		{"", "", true},
		{"", "project:abc", true},
		{"", "any:thing:here", true},

		// Exact match.
		{"project:abc", "project:abc", true},

		// Prefix match.
		{"project:", "project:abc", true},
		{"project:", "project:", true},

		// Longer event scope under a shorter prefix.
		{"project:abc", "project:abc:sub", true},

		// No match.
		{"project:abc", "project:xyz", false},
		{"project:", "other:abc", false},
		{"ns:", "project:abc", false},

		// Subscriber scope longer than event scope — no match.
		{"project:abc:extra", "project:abc", false},
	}

	for _, tc := range cases {
		got := matchesScope(tc.subScope, tc.eventScope)
		if got != tc.want {
			t.Errorf("matchesScope(%q, %q) = %v, want %v", tc.subScope, tc.eventScope, got, tc.want)
		}
	}
}

// ─── ComputeHMACSHA256 (package-level function) ───────────────────────────────

// TestComputeHMACSHA256_Properties verifies determinism, uniqueness on secret
// change, and uniqueness on body change.
func TestComputeHMACSHA256_Properties(t *testing.T) {
	body := []byte(`{"event":"test"}`)
	secret := "key1"

	sig1 := ComputeHMACSHA256(secret, body)
	sig2 := ComputeHMACSHA256(secret, body)
	if sig1 != sig2 {
		t.Errorf("HMAC is not deterministic: %q != %q", sig1, sig2)
	}
	if sig1 == "" {
		t.Error("HMAC must not be empty")
	}

	// Different secret → different sig.
	sig3 := ComputeHMACSHA256("key2", body)
	if sig1 == sig3 {
		t.Error("different secrets must produce different signatures")
	}

	// Different body → different sig.
	sig4 := ComputeHMACSHA256(secret, []byte(`{"event":"other"}`))
	if sig1 == sig4 {
		t.Error("different bodies must produce different signatures")
	}

	// Empty body is valid — must not panic and must produce a fixed value.
	sigEmpty := ComputeHMACSHA256(secret, []byte{})
	if sigEmpty == "" {
		t.Error("HMAC of empty body must not be empty")
	}
}

// ─── Concurrent subscribe/cancel/publish stress test ─────────────────────────

// TestMemoryBus_ConcurrentSubscribeCancel stress-tests simultaneous subscribe,
// cancel, and publish operations to surface any race conditions.  The race
// detector will report any data race.
func TestMemoryBus_ConcurrentSubscribeCancel(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			ch, cancel, err := bus.Subscribe(ctx, "project:")
			if err != nil {
				return // bus may already be closed if timing is extreme
			}
			// Publish a few events.
			for j := 0; j < 5; j++ {
				_ = bus.Publish(ctx, newEvt(fmt.Sprintf("cc-%d-%d", i, j), MemoryCreated, "project:stress"))
			}
			// Drain briefly.
			deadline := time.After(50 * time.Millisecond)
			for {
				select {
				case <-ch:
				case <-deadline:
					cancel()
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestMemoryBus_SubscribeAfterPublish verifies that events published before a
// subscription was created are NOT delivered to the new subscriber (no
// retroactive delivery), but events published afterward are.
func TestMemoryBus_SubscribeAfterPublish(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()

	// Publish before subscribe.
	before := newEvt("before-sub", MemoryCreated, "project:pre")
	if err := bus.Publish(ctx, before); err != nil {
		t.Fatalf("Publish before: %v", err)
	}

	ch, cancel, err := bus.Subscribe(ctx, "project:")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(cancel)

	// Publish after subscribe.
	after := newEvt("after-sub", MemoryCreated, "project:post")
	if err := bus.Publish(ctx, after); err != nil {
		t.Fatalf("Publish after: %v", err)
	}

	got := recv(t, ch, 2*time.Second)
	if got.ID != "after-sub" {
		t.Errorf("got ID %q, want after-sub (pre-subscription event must not be delivered)", got.ID)
	}
	assertNone(t, ch, 80*time.Millisecond)
}
