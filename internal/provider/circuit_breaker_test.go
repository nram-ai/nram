package provider

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var errSimulated = errors.New("simulated failure")

// mockLLM is a minimal LLMProvider for testing.
type mockLLM struct {
	completeErr error
	callCount   atomic.Int64
}

func (m *mockLLM) Complete(_ context.Context, _ *CompletionRequest) (*CompletionResponse, error) {
	m.callCount.Add(1)
	if m.completeErr != nil {
		return nil, m.completeErr
	}
	return &CompletionResponse{Content: "ok"}, nil
}
func (m *mockLLM) Name() string     { return "mock-llm" }
func (m *mockLLM) Models() []string { return []string{"model-1"} }

// mockEmbedding is a minimal EmbeddingProvider for testing.
type mockEmbedding struct {
	embedErr  error
	callCount atomic.Int64
}

func (m *mockEmbedding) Embed(_ context.Context, _ *EmbeddingRequest) (*EmbeddingResponse, error) {
	m.callCount.Add(1)
	if m.embedErr != nil {
		return nil, m.embedErr
	}
	return &EmbeddingResponse{Embeddings: [][]float32{{0.1, 0.2}}}, nil
}
func (m *mockEmbedding) Name() string   { return "mock-embedding" }
func (m *mockEmbedding) Dimensions() []int { return []int{128} }

func testConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxFailures:         3,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
}

// ---------------------------------------------------------------------------
// CircuitBreaker core tests
// ---------------------------------------------------------------------------

func TestCircuitBreaker_ClosedAllowsRequests(t *testing.T) {
	cb := NewCircuitBreaker(testConfig())

	called := false
	err := cb.Execute(func() error {
		called = true
		return nil
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !called {
		t.Fatal("expected function to be called")
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", cb.State())
	}
}

func TestCircuitBreaker_OpensAfterMaxFailures(t *testing.T) {
	cb := NewCircuitBreaker(testConfig())

	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return errSimulated })
	}

	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after %d failures, got %v", 3, cb.State())
	}
}

func TestCircuitBreaker_RejectsWhenOpen(t *testing.T) {
	cb := NewCircuitBreaker(testConfig())

	// Trip the circuit.
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return errSimulated })
	}

	err := cb.Execute(func() error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_TransitionsToHalfOpenAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(testConfig())

	// Trip the circuit.
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return errSimulated })
	}

	// Advance the clock past the reset timeout.
	fakeNow := time.Now().Add(200 * time.Millisecond)
	cb.mu.Lock()
	cb.now = func() time.Time { return fakeNow }
	cb.mu.Unlock()

	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	cb := NewCircuitBreaker(testConfig())

	// Trip the circuit.
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return errSimulated })
	}

	// Advance clock to trigger half-open.
	fakeNow := time.Now().Add(200 * time.Millisecond)
	cb.mu.Lock()
	cb.now = func() time.Time { return fakeNow }
	cb.mu.Unlock()

	err := cb.Execute(func() error { return nil })
	if err != nil {
		t.Fatalf("expected success in half-open, got %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after half-open success, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(testConfig())

	// Trip the circuit.
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return errSimulated })
	}

	// Advance clock to trigger half-open.
	fakeNow := time.Now().Add(200 * time.Millisecond)
	cb.mu.Lock()
	cb.now = func() time.Time { return fakeNow }
	cb.mu.Unlock()

	_ = cb.Execute(func() error { return errSimulated })
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after half-open failure, got %v", cb.State())
	}
}

// ---------------------------------------------------------------------------
// CircuitBreakerLLM tests
// ---------------------------------------------------------------------------

func TestCircuitBreakerLLM_DelegatesCorrectly(t *testing.T) {
	mock := &mockLLM{}
	wrapped := NewCircuitBreakerLLM(mock, testConfig())

	// Verify interface satisfaction at compile time.
	var _ LLMProvider = wrapped

	// Name and Models pass through.
	if wrapped.Name() != "mock-llm" {
		t.Fatalf("expected name mock-llm, got %s", wrapped.Name())
	}
	if len(wrapped.Models()) != 1 || wrapped.Models()[0] != "model-1" {
		t.Fatalf("unexpected models: %v", wrapped.Models())
	}

	// Successful call.
	resp, err := wrapped.Complete(context.Background(), &CompletionRequest{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Content != "ok" {
		t.Fatalf("expected content 'ok', got %q", resp.Content)
	}
	if mock.callCount.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", mock.callCount.Load())
	}

	// Failing calls trip the circuit.
	mock.completeErr = errSimulated
	for i := 0; i < 3; i++ {
		_, _ = wrapped.Complete(context.Background(), &CompletionRequest{})
	}

	_, err = wrapped.Complete(context.Background(), &CompletionRequest{})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CircuitBreakerEmbedding tests
// ---------------------------------------------------------------------------

func TestCircuitBreakerEmbedding_DelegatesCorrectly(t *testing.T) {
	mock := &mockEmbedding{}
	wrapped := NewCircuitBreakerEmbedding(mock, testConfig())

	// Verify interface satisfaction at compile time.
	var _ EmbeddingProvider = wrapped

	// Name and Dimensions pass through.
	if wrapped.Name() != "mock-embedding" {
		t.Fatalf("expected name mock-embedding, got %s", wrapped.Name())
	}
	if len(wrapped.Dimensions()) != 1 || wrapped.Dimensions()[0] != 128 {
		t.Fatalf("unexpected dimensions: %v", wrapped.Dimensions())
	}

	// Successful call.
	resp, err := wrapped.Embed(context.Background(), &EmbeddingRequest{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.Embeddings) != 1 {
		t.Fatalf("expected 1 embedding, got %d", len(resp.Embeddings))
	}
	if mock.callCount.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", mock.callCount.Load())
	}

	// Failing calls trip the circuit.
	mock.embedErr = errSimulated
	for i := 0; i < 3; i++ {
		_, _ = wrapped.Embed(context.Background(), &EmbeddingRequest{})
	}

	_, err = wrapped.Embed(context.Background(), &EmbeddingRequest{})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Thread safety
// ---------------------------------------------------------------------------

func TestCircuitBreaker_ThreadSafety(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		MaxFailures:         10,
		ResetTimeout:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	})

	var wg sync.WaitGroup
	const goroutines = 50
	const iterations = 100

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if id%2 == 0 {
					_ = cb.Execute(func() error { return nil })
				} else {
					_ = cb.Execute(func() error { return errSimulated })
				}
				_ = cb.State()
			}
		}(g)
	}

	wg.Wait()

	// If we got here without a race detector complaint, the test passes.
	// Just verify the state is one of the valid values.
	s := cb.State()
	if s != StateClosed && s != StateOpen && s != StateHalfOpen {
		t.Fatalf("unexpected state: %v", s)
	}
}
