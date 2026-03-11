package provider

import (
	"context"
	"errors"
	"sync"
	"time"
)

// CircuitState represents the current state of a circuit breaker.
type CircuitState int

const (
	// StateClosed means the circuit is functioning normally; requests flow through.
	StateClosed CircuitState = iota
	// StateOpen means the circuit has tripped; requests are rejected immediately.
	StateOpen
	// StateHalfOpen means the circuit is testing whether the backend has recovered.
	StateHalfOpen
)

// ErrCircuitOpen is returned when a request is attempted while the circuit is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreakerConfig holds the tuning parameters for a CircuitBreaker.
type CircuitBreakerConfig struct {
	// MaxFailures is the number of consecutive failures required to trip the circuit.
	MaxFailures int
	// ResetTimeout is how long the circuit stays open before transitioning to half-open.
	ResetTimeout time.Duration
	// HalfOpenMaxRequests is the maximum number of trial requests allowed in the
	// half-open state before deciding whether to close or re-open the circuit.
	HalfOpenMaxRequests int
}

// DefaultCircuitBreakerConfig returns a sensible default configuration.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxFailures:         5,
		ResetTimeout:        30 * time.Second,
		HalfOpenMaxRequests: 1,
	}
}

// CircuitBreaker implements the circuit breaker pattern. It tracks consecutive
// failures and transitions between closed, open, and half-open states to protect
// downstream services from cascading failures.
type CircuitBreaker struct {
	mu                  sync.Mutex
	config              CircuitBreakerConfig
	state               CircuitState
	consecutiveFailures int
	lastStateChange     time.Time
	halfOpenRequests    int
	now                 func() time.Time // injectable clock for testing
}

// NewCircuitBreaker creates a CircuitBreaker with the given configuration.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config:          config,
		state:           StateClosed,
		lastStateChange: time.Now(),
		now:             time.Now,
	}
}

// State returns the current circuit state, performing any time-based transitions.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.stateLocked()
}

// stateLocked returns the current state, transitioning from open to half-open if
// the reset timeout has elapsed. Caller must hold cb.mu.
func (cb *CircuitBreaker) stateLocked() CircuitState {
	if cb.state == StateOpen {
		if cb.now().Sub(cb.lastStateChange) >= cb.config.ResetTimeout {
			cb.state = StateHalfOpen
			cb.halfOpenRequests = 0
			cb.lastStateChange = cb.now()
		}
	}
	return cb.state
}

// Execute runs fn through the circuit breaker. If the circuit is open, it returns
// ErrCircuitOpen without calling fn. In the half-open state, it limits the number
// of trial requests. Success and failure outcomes are recorded automatically.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	s := cb.stateLocked()

	switch s {
	case StateOpen:
		cb.mu.Unlock()
		return ErrCircuitOpen
	case StateHalfOpen:
		if cb.halfOpenRequests >= cb.config.HalfOpenMaxRequests {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		cb.halfOpenRequests++
	}
	cb.mu.Unlock()

	err := fn()
	if err != nil {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return err
}

// RecordSuccess records a successful call and transitions the circuit to closed
// if it was in the half-open state.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures = 0
	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.lastStateChange = cb.now()
	}
}

// RecordFailure records a failed call. If the failure count reaches MaxFailures
// the circuit opens. If the circuit is half-open, any failure re-opens it.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures++

	switch cb.state {
	case StateClosed:
		if cb.consecutiveFailures >= cb.config.MaxFailures {
			cb.state = StateOpen
			cb.lastStateChange = cb.now()
		}
	case StateHalfOpen:
		cb.state = StateOpen
		cb.lastStateChange = cb.now()
	}
}

// ---------------------------------------------------------------------------
// CircuitBreakerLLM
// ---------------------------------------------------------------------------

// CircuitBreakerLLM wraps an LLMProvider and delegates calls through a
// CircuitBreaker so that a repeatedly-failing provider is temporarily taken
// out of service.
type CircuitBreakerLLM struct {
	provider LLMProvider
	cb       *CircuitBreaker
}

// NewCircuitBreakerLLM creates a new CircuitBreakerLLM wrapping the given provider.
func NewCircuitBreakerLLM(provider LLMProvider, config CircuitBreakerConfig) *CircuitBreakerLLM {
	return &CircuitBreakerLLM{
		provider: provider,
		cb:       NewCircuitBreaker(config),
	}
}

// Complete delegates to the wrapped provider through the circuit breaker.
func (c *CircuitBreakerLLM) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	var resp *CompletionResponse
	err := c.cb.Execute(func() error {
		var e error
		resp, e = c.provider.Complete(ctx, req)
		return e
	})
	return resp, err
}

// Name returns the underlying provider's name.
func (c *CircuitBreakerLLM) Name() string {
	return c.provider.Name()
}

// Models returns the underlying provider's model list.
func (c *CircuitBreakerLLM) Models() []string {
	return c.provider.Models()
}

// CircuitBreaker returns the underlying circuit breaker for inspection.
func (c *CircuitBreakerLLM) CircuitBreaker() *CircuitBreaker {
	return c.cb
}

// ---------------------------------------------------------------------------
// CircuitBreakerEmbedding
// ---------------------------------------------------------------------------

// CircuitBreakerEmbedding wraps an EmbeddingProvider and delegates calls
// through a CircuitBreaker.
type CircuitBreakerEmbedding struct {
	provider EmbeddingProvider
	cb       *CircuitBreaker
}

// NewCircuitBreakerEmbedding creates a new CircuitBreakerEmbedding wrapping
// the given provider.
func NewCircuitBreakerEmbedding(provider EmbeddingProvider, config CircuitBreakerConfig) *CircuitBreakerEmbedding {
	return &CircuitBreakerEmbedding{
		provider: provider,
		cb:       NewCircuitBreaker(config),
	}
}

// Embed delegates to the wrapped provider through the circuit breaker.
func (c *CircuitBreakerEmbedding) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	var resp *EmbeddingResponse
	err := c.cb.Execute(func() error {
		var e error
		resp, e = c.provider.Embed(ctx, req)
		return e
	})
	return resp, err
}

// Name returns the underlying provider's name.
func (c *CircuitBreakerEmbedding) Name() string {
	return c.provider.Name()
}

// Dimensions returns the underlying provider's supported dimensions.
func (c *CircuitBreakerEmbedding) Dimensions() []int {
	return c.provider.Dimensions()
}

// CircuitBreaker returns the underlying circuit breaker for inspection.
func (c *CircuitBreakerEmbedding) CircuitBreaker() *CircuitBreaker {
	return c.cb
}
