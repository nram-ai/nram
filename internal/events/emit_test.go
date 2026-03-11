package events

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestEmit_NilBus(t *testing.T) {
	// Must not panic when bus is nil.
	Emit(context.Background(), nil, MemoryCreated, "project:abc", map[string]string{"id": "123"})
}

func TestEmit_PublishesEvent(t *testing.T) {
	bus := NewMemoryBus()
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	data := map[string]string{"memory_id": "abc", "project_id": "def"}
	Emit(context.Background(), bus, MemoryCreated, "project:def", data)

	select {
	case ev := <-ch:
		if ev.Type != MemoryCreated {
			t.Errorf("expected type %s, got %s", MemoryCreated, ev.Type)
		}
		if ev.Scope != "project:def" {
			t.Errorf("expected scope project:def, got %s", ev.Scope)
		}
		if ev.ID == "" {
			t.Error("expected non-empty event ID")
		}
		if ev.Timestamp.IsZero() {
			t.Error("expected non-zero timestamp")
		}

		var payload map[string]string
		if err := json.Unmarshal(ev.Data, &payload); err != nil {
			t.Fatalf("unmarshal data: %v", err)
		}
		if payload["memory_id"] != "abc" {
			t.Errorf("expected memory_id abc, got %s", payload["memory_id"])
		}
		if payload["project_id"] != "def" {
			t.Errorf("expected project_id def, got %s", payload["project_id"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestEmit_UnmarshalableData(t *testing.T) {
	bus := NewMemoryBus()
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	// Channels (chan int) cannot be marshalled to JSON. Emit should log and skip.
	Emit(context.Background(), bus, MemoryCreated, "project:x", make(chan int))

	select {
	case <-ch:
		t.Fatal("expected no event for unmarshalable data")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}
