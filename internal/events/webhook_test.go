package events

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

// mockWebhookStore implements WebhookStore for testing.
type mockWebhookStore struct {
	mu       sync.Mutex
	webhooks []model.Webhook
	failures map[uuid.UUID]int
	successes map[uuid.UUID]int
}

func newMockWebhookStore(webhooks ...model.Webhook) *mockWebhookStore {
	return &mockWebhookStore{
		webhooks:  webhooks,
		failures:  make(map[uuid.UUID]int),
		successes: make(map[uuid.UUID]int),
	}
}

func (m *mockWebhookStore) ListActiveForEvent(_ context.Context, namespaceID uuid.UUID, event string) ([]model.Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	scope := "ns:" + namespaceID.String()
	var result []model.Webhook
	for _, wh := range m.webhooks {
		if !wh.Active || wh.Scope != scope {
			continue
		}
		for _, e := range wh.Events {
			if e == event {
				result = append(result, wh)
				break
			}
		}
	}
	return result, nil
}

func (m *mockWebhookStore) RecordFailure(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[id]++
	return nil
}

func (m *mockWebhookStore) RecordSuccess(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successes[id]++
	return nil
}

func (m *mockWebhookStore) failureCount(id uuid.UUID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failures[id]
}

func (m *mockWebhookStore) successCount(id uuid.UUID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.successes[id]
}

func TestComputeHMACSHA256(t *testing.T) {
	sig := ComputeHMACSHA256("test-secret", []byte(`{"hello":"world"}`))
	if sig == "" {
		t.Fatal("expected non-empty signature")
	}
	// Verify determinism.
	sig2 := ComputeHMACSHA256("test-secret", []byte(`{"hello":"world"}`))
	if sig != sig2 {
		t.Fatalf("expected same signature, got %s vs %s", sig, sig2)
	}
	// Different secret => different sig.
	sig3 := ComputeHMACSHA256("other-secret", []byte(`{"hello":"world"}`))
	if sig == sig3 {
		t.Fatal("expected different signature with different secret")
	}
}

func TestWebhookDelivery_Success(t *testing.T) {
	var received atomic.Int32
	var receivedBody []byte
	var receivedHeaders http.Header

	mu := sync.Mutex{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		mu.Lock()
		receivedHeaders = r.Header.Clone()
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	nsID := uuid.New()
	secret := "my-secret"
	whID := uuid.New()
	store := newMockWebhookStore(model.Webhook{
		ID:     whID,
		URL:    srv.URL,
		Secret: &secret,
		Events: []string{MemoryCreated},
		Scope:  "ns:" + nsID.String(),
		Active: true,
	})

	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	deliverer := NewWebhookDeliverer(bus, store,
		WithHTTPClient(srv.Client()),
		WithMaxRetries(3),
		WithTimeout(5*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := deliverer.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer deliverer.Stop()

	event := Event{
		ID:        uuid.New().String(),
		Type:      MemoryCreated,
		Scope:     "project:" + nsID.String(),
		Data:      json.RawMessage(`{"key":"value"}`),
		Timestamp: time.Now().UTC(),
	}
	if err := bus.Publish(ctx, event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, func() bool { return received.Load() == 1 }, 3*time.Second)

	if store.successCount(whID) != 1 {
		t.Errorf("expected 1 success, got %d", store.successCount(whID))
	}
	if store.failureCount(whID) != 0 {
		t.Errorf("expected 0 failures, got %d", store.failureCount(whID))
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify headers.
	if got := receivedHeaders.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if got := receivedHeaders.Get("X-NRAM-Event"); got != MemoryCreated {
		t.Errorf("X-NRAM-Event = %q, want %q", got, MemoryCreated)
	}
	if got := receivedHeaders.Get("X-NRAM-Delivery"); got != event.ID {
		t.Errorf("X-NRAM-Delivery = %q, want %q", got, event.ID)
	}

	// Verify signature.
	expectedSig := "sha256=" + ComputeHMACSHA256(secret, receivedBody)
	if got := receivedHeaders.Get("X-NRAM-Signature"); got != expectedSig {
		t.Errorf("X-NRAM-Signature = %q, want %q", got, expectedSig)
	}

	// Verify body is a valid event.
	var decoded Event
	if err := json.Unmarshal(receivedBody, &decoded); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if decoded.ID != event.ID {
		t.Errorf("event ID = %q, want %q", decoded.ID, event.ID)
	}
	if decoded.Type != event.Type {
		t.Errorf("event Type = %q, want %q", decoded.Type, event.Type)
	}
}

func TestWebhookDelivery_RetryThenSuccess(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	nsID := uuid.New()
	whID := uuid.New()
	store := newMockWebhookStore(model.Webhook{
		ID:     whID,
		URL:    srv.URL,
		Events: []string{MemoryCreated},
		Scope:  "ns:" + nsID.String(),
		Active: true,
	})

	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	deliverer := NewWebhookDeliverer(bus, store,
		WithHTTPClient(srv.Client()),
		WithMaxRetries(3),
		WithTimeout(5*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := deliverer.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer deliverer.Stop()

	event := Event{
		ID:        uuid.New().String(),
		Type:      MemoryCreated,
		Scope:     "project:" + nsID.String(),
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now().UTC(),
	}
	if err := bus.Publish(ctx, event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, func() bool { return attempts.Load() >= 3 }, 15*time.Second)

	if store.successCount(whID) != 1 {
		t.Errorf("expected 1 success, got %d", store.successCount(whID))
	}
	if store.failureCount(whID) != 0 {
		t.Errorf("expected 0 failures, got %d", store.failureCount(whID))
	}
}

func TestWebhookDelivery_MaxRetriesExceeded(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	nsID := uuid.New()
	whID := uuid.New()
	store := newMockWebhookStore(model.Webhook{
		ID:     whID,
		URL:    srv.URL,
		Events: []string{MemoryCreated},
		Scope:  "ns:" + nsID.String(),
		Active: true,
	})

	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	deliverer := NewWebhookDeliverer(bus, store,
		WithHTTPClient(srv.Client()),
		WithMaxRetries(3),
		WithTimeout(5*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := deliverer.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer deliverer.Stop()

	event := Event{
		ID:        uuid.New().String(),
		Type:      MemoryCreated,
		Scope:     "project:" + nsID.String(),
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now().UTC(),
	}
	if err := bus.Publish(ctx, event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, func() bool { return attempts.Load() >= 3 }, 15*time.Second)

	// Give a moment for RecordFailure to be called.
	waitFor(t, func() bool { return store.failureCount(whID) == 1 }, 3*time.Second)

	if got := attempts.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
	if store.successCount(whID) != 0 {
		t.Errorf("expected 0 successes, got %d", store.successCount(whID))
	}
	if store.failureCount(whID) != 1 {
		t.Errorf("expected 1 failure, got %d", store.failureCount(whID))
	}
}

func TestWebhookDelivery_NoSecret(t *testing.T) {
	var receivedHeaders http.Header
	mu := sync.Mutex{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedHeaders = r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	nsID := uuid.New()
	whID := uuid.New()
	store := newMockWebhookStore(model.Webhook{
		ID:     whID,
		URL:    srv.URL,
		Secret: nil,
		Events: []string{MemoryCreated},
		Scope:  "ns:" + nsID.String(),
		Active: true,
	})

	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	deliverer := NewWebhookDeliverer(bus, store,
		WithHTTPClient(srv.Client()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := deliverer.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer deliverer.Stop()

	event := Event{
		ID:        uuid.New().String(),
		Type:      MemoryCreated,
		Scope:     "project:" + nsID.String(),
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now().UTC(),
	}
	if err := bus.Publish(ctx, event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, func() bool { return store.successCount(whID) == 1 }, 3*time.Second)

	mu.Lock()
	defer mu.Unlock()

	if got := receivedHeaders.Get("X-NRAM-Signature"); got != "" {
		t.Errorf("expected no X-NRAM-Signature header, got %q", got)
	}
}

func TestParseNamespaceID(t *testing.T) {
	tests := []struct {
		scope   string
		wantOK  bool
		wantStr string
	}{
		{
			scope:   "project:" + "550e8400-e29b-41d4-a716-446655440000",
			wantOK:  true,
			wantStr: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			scope:   "ns:" + "550e8400-e29b-41d4-a716-446655440000",
			wantOK:  true,
			wantStr: "550e8400-e29b-41d4-a716-446655440000",
		},
		{scope: "", wantOK: false},
		{scope: "nocolon", wantOK: false},
		{scope: "project:", wantOK: false},
		{scope: "project:not-a-uuid", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("scope=%q", tt.scope), func(t *testing.T) {
			id, ok := parseNamespaceID(tt.scope)
			if ok != tt.wantOK {
				t.Fatalf("parseNamespaceID(%q) ok = %v, want %v", tt.scope, ok, tt.wantOK)
			}
			if ok && id.String() != tt.wantStr {
				t.Errorf("parseNamespaceID(%q) = %s, want %s", tt.scope, id, tt.wantStr)
			}
		})
	}
}

func TestWebhookDelivery_NoMatchingScope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not have received a request")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	otherNS := uuid.New()
	whID := uuid.New()
	store := newMockWebhookStore(model.Webhook{
		ID:     whID,
		URL:    srv.URL,
		Events: []string{MemoryCreated},
		Scope:  "ns:" + otherNS.String(),
		Active: true,
	})

	bus := NewMemoryBus(0, 0)
	defer bus.Close()

	deliverer := NewWebhookDeliverer(bus, store,
		WithHTTPClient(srv.Client()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := deliverer.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer deliverer.Stop()

	// Publish event with a different namespace ID.
	differentNS := uuid.New()
	event := Event{
		ID:        uuid.New().String(),
		Type:      MemoryCreated,
		Scope:     "project:" + differentNS.String(),
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now().UTC(),
	}
	if err := bus.Publish(ctx, event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait briefly then verify no delivery occurred.
	time.Sleep(200 * time.Millisecond)

	if store.successCount(whID) != 0 {
		t.Errorf("expected 0 successes, got %d", store.successCount(whID))
	}
	if store.failureCount(whID) != 0 {
		t.Errorf("expected 0 failures, got %d", store.failureCount(whID))
	}
}

// waitFor polls the condition function until it returns true or the timeout
// elapses. It calls t.Fatal on timeout.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("waitFor: timed out waiting for condition")
}
