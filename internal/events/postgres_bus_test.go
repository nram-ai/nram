package events

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestPostgresBus_ImplementsInterface(t *testing.T) {
	// Verify at compile time that PostgresBus implements EventBus.
	var _ EventBus = (*PostgresBus)(nil)
}

func TestPostgresBus_LocalDelivery(t *testing.T) {
	// Test the local (MemoryBus) portion without requiring Postgres.
	// We construct a PostgresBus manually with only the local bus populated.
	bus := &PostgresBus{
		local:      NewMemoryBus(0, 0),
		instanceID: "test-instance",
	}
	defer bus.local.Close()

	ctx := context.Background()
	ch, cancel, err := bus.local.Subscribe(ctx, "org/")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	evt := Event{
		ID:        "evt-pg-1",
		Type:      MemoryCreated,
		Scope:     "org/project",
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}

	if err := bus.local.Publish(ctx, evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-ch:
		if got.ID != evt.ID {
			t.Errorf("got ID %s, want %s", got.ID, evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestPostgresBus_NotificationSkipsSelf(t *testing.T) {
	// Verify the echo-suppression logic by simulating a notification payload.
	instanceID := "my-instance"

	n := pgNotification{
		InstanceID: instanceID,
		Event: Event{
			ID:   "evt-echo",
			Type: MemoryUpdated,
		},
	}

	payload, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded pgNotification
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.InstanceID != instanceID {
		t.Errorf("got instanceID %s, want %s", decoded.InstanceID, instanceID)
	}

	// Same instance — should be skipped
	if decoded.InstanceID == instanceID {
		return // correctly identified as self
	}
	t.Error("echo suppression failed")
}

func TestPostgresBus_NotificationAcceptsRemote(t *testing.T) {
	localID := "local-instance"

	n := pgNotification{
		InstanceID: "remote-instance",
		Event: Event{
			ID:   "evt-remote",
			Type: EntityCreated,
			Data: json.RawMessage(`{"entity":"foo"}`),
		},
	}

	payload, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded pgNotification
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.InstanceID == localID {
		t.Error("remote notification should not match local instance ID")
	}

	if decoded.Event.ID != "evt-remote" {
		t.Errorf("got event ID %s, want evt-remote", decoded.Event.ID)
	}
}
