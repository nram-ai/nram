package events

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// memoryPayload mirrors model.Memory fields relevant for null-vs-empty testing.
type memoryPayload struct {
	ID      string          `json:"id"`
	Content string          `json:"content"`
	Tags    []string        `json:"tags"`
	Meta    json.RawMessage `json:"metadata"`
}

// recallResponse mimics a recall endpoint response with a slice of results.
type recallResponse struct {
	Memories []memoryPayload   `json:"memories"`
	Extras   map[string]string `json:"extras"`
}

// TestEmit_NilSliceSanitized verifies that Emit sanitizes nil slices to []
// and nil json.RawMessage to {} before marshaling.
func TestEmit_NilSliceSanitized(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	payload := memoryPayload{
		ID:      "mem-1",
		Content: "hello",
		Tags:    nil, // nil slice — must become []
		Meta:    nil, // nil json.RawMessage — must become {}
	}

	Emit(context.Background(), bus, MemoryCreated, "project:abc", payload)

	select {
	case ev := <-ch:
		raw := string(ev.Data)

		if strings.Contains(raw, `"tags":null`) {
			t.Errorf("nil Tags must not serialize as null, got: %s", raw)
		}
		if !strings.Contains(raw, `"tags":[]`) {
			t.Errorf("nil Tags must serialize as [], got: %s", raw)
		}
		if strings.Contains(raw, `"metadata":null`) {
			t.Errorf("nil Meta must not serialize as null, got: %s", raw)
		}
		if !strings.Contains(raw, `"metadata":{}`) {
			t.Errorf("nil Meta must serialize as {}, got: %s", raw)
		}

		var decoded memoryPayload
		if err := json.Unmarshal(ev.Data, &decoded); err != nil {
			t.Fatalf("unmarshal event data: %v", err)
		}
		if decoded.ID != "mem-1" {
			t.Errorf("expected ID mem-1, got %s", decoded.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// TestEmit_NilSliceInNestedStruct verifies that nested nil slices and maps
// are sanitized in event payloads.
func TestEmit_NilSliceInNestedStruct(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	payload := recallResponse{
		Memories: nil, // nil slice of structs
		Extras:   nil, // nil map
	}

	Emit(context.Background(), bus, MemoryCreated, "project:abc", payload)

	select {
	case ev := <-ch:
		raw := string(ev.Data)

		if strings.Contains(raw, `"memories":null`) {
			t.Errorf("nil Memories must not serialize as null, got: %s", raw)
		}
		if !strings.Contains(raw, `"memories":[]`) {
			t.Errorf("nil Memories must serialize as [], got: %s", raw)
		}
		if strings.Contains(raw, `"extras":null`) {
			t.Errorf("nil Extras must not serialize as null, got: %s", raw)
		}
		if !strings.Contains(raw, `"extras":{}`) {
			t.Errorf("nil Extras must serialize as {}, got: %s", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// TestEmit_InitializedSliceProducesEmptyArray verifies that explicitly
// initialized empty slices still produce [].
func TestEmit_InitializedSliceProducesEmptyArray(t *testing.T) {
	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	ch, cancel, err := bus.Subscribe(context.Background(), "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	payload := memoryPayload{
		ID:      "mem-2",
		Content: "world",
		Tags:    []string{},            // initialized empty slice
		Meta:    json.RawMessage("{}"), // initialized empty JSON
	}

	Emit(context.Background(), bus, MemoryCreated, "project:abc", payload)

	select {
	case ev := <-ch:
		raw := string(ev.Data)

		if !strings.Contains(raw, `"tags":[]`) {
			t.Errorf("expected initialized empty Tags to serialize as [], got: %s", raw)
		}
		if !strings.Contains(raw, `"metadata":{}`) {
			t.Errorf("expected initialized Meta to serialize as {}, got: %s", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}
