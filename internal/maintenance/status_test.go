package maintenance

import (
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/events"
)

func TestRegistryBeginSnapshotEnd(t *testing.T) {
	bus := events.NewEventBus("sqlite", nil, 8, 8)
	defer func() { _ = bus.Close() }()

	ch, unsub, err := bus.Subscribe(t.Context(), events.EventScopeMaintenance)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer unsub()

	reg := NewRegistry(bus)

	if active, ops := reg.Snapshot(); active || len(ops) != 0 {
		t.Fatalf("empty registry: active=%v ops=%d", active, len(ops))
	}

	end := reg.Begin("op1", "Label", "message")

	active, ops := reg.Snapshot()
	if !active || len(ops) != 1 {
		t.Fatalf("after Begin: active=%v ops=%d", active, len(ops))
	}
	if ops[0].ID != "op1" || ops[0].Label != "Label" || ops[0].Message != "message" {
		t.Fatalf("after Begin op = %+v", ops[0])
	}
	assertEvent(t, ch, events.MaintenanceStarted)

	end()
	end() // idempotent: must not emit a second ended event or panic

	if active, ops := reg.Snapshot(); active || len(ops) != 0 {
		t.Fatalf("after end: active=%v ops=%d", active, len(ops))
	}
	assertEvent(t, ch, events.MaintenanceEnded)
	assertNoEvent(t, ch)
}

func TestRegistrySnapshotOrderingAndNilBus(t *testing.T) {
	// A nil bus disables events but Snapshot must still work.
	reg := NewRegistry(nil)

	end1 := reg.Begin("a", "A", "first")
	time.Sleep(2 * time.Millisecond)
	end2 := reg.Begin("b", "B", "second")

	active, ops := reg.Snapshot()
	if !active || len(ops) != 2 {
		t.Fatalf("active=%v ops=%d", active, len(ops))
	}
	if ops[0].ID != "a" || ops[1].ID != "b" {
		t.Fatalf("expected oldest-first ordering, got %s then %s", ops[0].ID, ops[1].ID)
	}

	end1()
	end2()
	if active, _ := reg.Snapshot(); active {
		t.Fatal("registry should be inactive after all ops end")
	}
}

func assertEvent(t *testing.T, ch <-chan events.Event, want string) {
	t.Helper()
	select {
	case ev := <-ch:
		if ev.Type != want {
			t.Fatalf("event type = %q, want %q", ev.Type, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %q event", want)
	}
}

func assertNoEvent(t *testing.T, ch <-chan events.Event) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected extra event: %q", ev.Type)
	case <-time.After(100 * time.Millisecond):
	}
}
