package events

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func makeEvent(typ, scope string) Event {
	return Event{
		ID:        "evt-1",
		Type:      typ,
		Scope:     scope,
		Data:      json.RawMessage(`{"key":"value"}`),
		Timestamp: time.Now(),
	}
}

func TestMemoryBus_PublishSubscribe(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	evt := makeEvent(MemoryCreated, "org/project")
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-ch:
		if got.ID != evt.ID {
			t.Errorf("got ID %s, want %s", got.ID, evt.ID)
		}
		if got.Type != MemoryCreated {
			t.Errorf("got Type %s, want %s", got.Type, MemoryCreated)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestMemoryBus_ScopeFiltering(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ctx := context.Background()

	chAll, cancelAll, _ := bus.Subscribe(ctx, "")
	defer cancelAll()

	chOrg, cancelOrg, _ := bus.Subscribe(ctx, "org/")
	defer cancelOrg()

	chOther, cancelOther, _ := bus.Subscribe(ctx, "other/")
	defer cancelOther()

	evt := makeEvent(MemoryCreated, "org/project")
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// chAll should receive (empty scope matches everything)
	select {
	case <-chAll:
	case <-time.After(time.Second):
		t.Error("chAll: timed out")
	}

	// chOrg should receive (scope prefix matches)
	select {
	case <-chOrg:
	case <-time.After(time.Second):
		t.Error("chOrg: timed out")
	}

	// chOther should NOT receive
	select {
	case <-chOther:
		t.Error("chOther: should not have received event")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestMemoryBus_Cancel(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	cancel()

	// Channel should be closed after cancel
	_, ok := <-ch
	if ok {
		t.Error("expected channel to be closed after cancel")
	}

	// Double cancel should not panic
	cancel()
}

func TestMemoryBus_Close(t *testing.T) {
	bus := NewMemoryBus(0, 0)

	ctx := context.Background()
	ch, _, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Channel should be closed
	_, ok := <-ch
	if ok {
		t.Error("expected channel to be closed after bus close")
	}

	// Publish on closed bus returns error
	if err := bus.Publish(ctx, makeEvent(MemoryCreated, "")); err != ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}

	// Subscribe on closed bus returns error
	_, _, err = bus.Subscribe(ctx, "")
	if err != ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}
}

func TestMemoryBus_FullChannel(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	// Fill the channel buffer
	for i := 0; i < fallbackSubscriberBufferSize; i++ {
		evt := makeEvent(MemoryCreated, "test")
		evt.ID = "fill"
		if err := bus.Publish(ctx, evt); err != nil {
			t.Fatalf("publish fill %d: %v", i, err)
		}
	}

	// Next publish should not block (event dropped)
	evt := makeEvent(MemoryCreated, "test")
	evt.ID = "overflow"
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("publish overflow: %v", err)
	}

	// Drain and verify we got exactly fallbackSubscriberBufferSize events
	count := 0
	for {
		select {
		case <-ch:
			count++
		case <-time.After(50 * time.Millisecond):
			goto done
		}
	}
done:
	if count != fallbackSubscriberBufferSize {
		t.Errorf("got %d events, want %d (overflow should be dropped)", count, fallbackSubscriberBufferSize)
	}
}

func TestMemoryBus_ConcurrentPublish(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ctx := context.Background()
	ch, cancel, err := bus.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	const numPublishers = 10
	var wg sync.WaitGroup
	wg.Add(numPublishers)

	for i := 0; i < numPublishers; i++ {
		go func() {
			defer wg.Done()
			evt := makeEvent(MemoryCreated, "test")
			_ = bus.Publish(ctx, evt)
		}()
	}

	wg.Wait()

	count := 0
	for {
		select {
		case <-ch:
			count++
		case <-time.After(100 * time.Millisecond):
			goto done
		}
	}
done:
	if count != numPublishers {
		t.Errorf("got %d events, want %d", count, numPublishers)
	}
}
