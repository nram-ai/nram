package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/events"
)

func publishTestEvent(t *testing.T, bus events.EventBus, id, typ, scope string) {
	t.Helper()
	evt := events.Event{
		ID:        id,
		Type:      typ,
		Scope:     scope,
		Data:      json.RawMessage(`{"key":"value"}`),
		Timestamp: time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestEventsHandler_SSEDelivery(t *testing.T) {
	bus := events.NewMemoryBus()
	defer bus.Close()

	handler := NewEventsHandler(bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	// Give the handler time to subscribe.
	time.Sleep(50 * time.Millisecond)

	publishTestEvent(t, bus, "evt-1", events.MemoryCreated, "org/proj")

	// Give the handler time to write.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	if !strings.Contains(body, "id: evt-1") {
		t.Errorf("expected SSE id field, got:\n%s", body)
	}
	if !strings.Contains(body, "event: memory.created") {
		t.Errorf("expected SSE event field, got:\n%s", body)
	}
	if !strings.Contains(body, "data: ") {
		t.Errorf("expected SSE data field, got:\n%s", body)
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "text/event-stream" {
		t.Errorf("expected Content-Type text/event-stream, got %q", ct)
	}
	cc := rec.Header().Get("Cache-Control")
	if cc != "no-cache" {
		t.Errorf("expected Cache-Control no-cache, got %q", cc)
	}
}

func TestEventsHandler_ScopeFiltering(t *testing.T) {
	bus := events.NewMemoryBus()
	defer bus.Close()

	handler := NewEventsHandler(bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/events?scope=org/proj", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// This event matches the scope.
	publishTestEvent(t, bus, "evt-match", events.MemoryCreated, "org/proj/ns")
	// This event does NOT match the scope.
	publishTestEvent(t, bus, "evt-nomatch", events.MemoryCreated, "other/proj")

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	if !strings.Contains(body, "id: evt-match") {
		t.Errorf("expected matching event, got:\n%s", body)
	}
	if strings.Contains(body, "id: evt-nomatch") {
		t.Errorf("should not contain non-matching event, got:\n%s", body)
	}
}

func TestEventsHandler_LastEventIDReplay(t *testing.T) {
	bus := events.NewMemoryBus()
	defer bus.Close()

	// Publish some events before connecting.
	publishTestEvent(t, bus, "evt-a", events.MemoryCreated, "org/proj")
	publishTestEvent(t, bus, "evt-b", events.MemoryUpdated, "org/proj")
	publishTestEvent(t, bus, "evt-c", events.MemoryDeleted, "org/proj")

	handler := NewEventsHandler(bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil).WithContext(ctx)
	req.Header.Set("Last-Event-ID", "evt-a")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	// Give the handler time to write replay and subscribe.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	// Should NOT contain evt-a (it's the last seen).
	if strings.Contains(body, "id: evt-a") {
		t.Errorf("should not replay evt-a (last seen), got:\n%s", body)
	}
	// Should contain evt-b and evt-c.
	if !strings.Contains(body, "id: evt-b") {
		t.Errorf("expected replayed evt-b, got:\n%s", body)
	}
	if !strings.Contains(body, "id: evt-c") {
		t.Errorf("expected replayed evt-c, got:\n%s", body)
	}
}

func TestEventsHandler_Keepalive(t *testing.T) {
	bus := events.NewMemoryBus()
	defer bus.Close()

	// Create a handler with a very short keepalive for testing.
	handler := func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		ch, cancel, err := bus.Subscribe(r.Context(), "")
		if err != nil {
			return
		}
		defer cancel()

		keepalive := time.NewTicker(50 * time.Millisecond)
		defer keepalive.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
			case <-keepalive.C:
				w.Write([]byte(": keepalive\n\n"))
				flusher.Flush()
			}
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == ": keepalive" {
			found = true
			break
		}
	}

	if !found {
		t.Error("expected keepalive comment in SSE stream")
	}
}

func TestEventsHandler_ReplayWithScopeFilter(t *testing.T) {
	bus := events.NewMemoryBus()
	defer bus.Close()

	// Publish events with different scopes.
	publishTestEvent(t, bus, "evt-1", events.MemoryCreated, "org/proj")
	publishTestEvent(t, bus, "evt-2", events.MemoryCreated, "other/proj")
	publishTestEvent(t, bus, "evt-3", events.MemoryUpdated, "org/proj/ns")

	handler := NewEventsHandler(bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/events?scope=org/", nil).WithContext(ctx)
	req.Header.Set("Last-Event-ID", "nonexistent")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	if !strings.Contains(body, "id: evt-1") {
		t.Errorf("expected evt-1, got:\n%s", body)
	}
	if strings.Contains(body, "id: evt-2") {
		t.Errorf("should not contain evt-2, got:\n%s", body)
	}
	if !strings.Contains(body, "id: evt-3") {
		t.Errorf("expected evt-3, got:\n%s", body)
	}
}
