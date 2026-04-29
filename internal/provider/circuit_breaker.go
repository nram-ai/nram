package provider

import (
	"context"
	"errors"
	"fmt"
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

// ErrCircuitOpen is the sentinel returned (via errors.Is) when a request is
// attempted while the circuit is open. The actual error returned by Execute is
// a *CircuitOpenError carrying provider name, last underlying error, and
// retry timing; CircuitOpenError.Is recognizes this sentinel so existing
// errors.Is(err, ErrCircuitOpen) callers keep working.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitOpenError is returned by Execute when the circuit is open. It exposes
// the provider's name, the last underlying error that tripped the breaker,
// when the breaker opened, and when the next half-open trial is permitted.
type CircuitOpenError struct {
	// Provider is the name of the wrapped provider (e.g., "ollama"). Empty if
	// the breaker was constructed without a name.
	Provider string
	// Cause is the most recent underlying error that contributed to opening
	// the circuit. nil if the breaker has never recorded a real failure (rare —
	// only possible if Execute is called before any failure in tests).
	Cause error
	// OpenSince is when the breaker last entered the open state. Used by callers
	// to distinguish a fresh trip from a sustained outage.
	OpenSince time.Time
	// RetryAt is when the breaker will permit its next half-open trial.
	RetryAt time.Time
}

// Error implements error. The format is stable so log greppers can rely on it.
func (e *CircuitOpenError) Error() string {
	provider := e.Provider
	if provider == "" {
		provider = "<unnamed>"
	}
	retryIn := time.Until(e.RetryAt).Round(time.Second)
	if retryIn < 0 {
		retryIn = 0
	}
	if e.Cause != nil {
		return fmt.Sprintf("circuit breaker open for %s (last error: %v; retry in %s)",
			provider, e.Cause, retryIn)
	}
	return fmt.Sprintf("circuit breaker open for %s (retry in %s)", provider, retryIn)
}

// Unwrap returns the underlying cause so errors.Unwrap / errors.As can recover it.
func (e *CircuitOpenError) Unwrap() error { return e.Cause }

// Is reports whether target matches the open-circuit sentinel. Lets existing
// callers rely on errors.Is(err, ErrCircuitOpen).
func (e *CircuitOpenError) Is(target error) bool {
	return target == ErrCircuitOpen
}

// CircuitBreakerConfig holds the tuning parameters for a CircuitBreaker.
type CircuitBreakerConfig struct {
	// Name labels the breaker (e.g., "ollama-fact"); embedded into CircuitOpenError
	// so log lines identify which provider tripped.
	Name string
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
	lastError           error
	// halfOpenInFlight tracks trial requests that have been admitted to the
	// underlying call but have not yet returned. Distinct from a "tried in this
	// window" flag so a panic, ctx cancellation, or never-returning fn cannot
	// strand the breaker with a stuck quota.
	halfOpenInFlight int
	// halfOpenAttempted is set when a trial in the current HalfOpen window has
	// completed (success or failure). Cleared when entering a fresh HalfOpen
	// window. While set with the trial in flight done, the next state-change
	// (Close on success, Open on failure) governs.
	halfOpenAttempted bool
	now               func() time.Time // injectable clock for testing
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
// the reset timeout has elapsed, and re-arming a stuck half-open window if no
// trial is in flight and the window is older than 2x ResetTimeout. Caller must
// hold cb.mu.
func (cb *CircuitBreaker) stateLocked() CircuitState {
	switch cb.state {
	case StateOpen:
		if cb.now().Sub(cb.lastStateChange) >= cb.config.ResetTimeout {
			cb.state = StateHalfOpen
			cb.halfOpenInFlight = 0
			cb.halfOpenAttempted = false
			cb.lastStateChange = cb.now()
		}
	case StateHalfOpen:
		// Self-heal stuck windows. If a trial was admitted but its outcome was
		// never recorded (panic between admit and return, killed goroutine,
		// etc.), halfOpenInFlight stays > 0 forever and every subsequent call
		// is rejected. After 2x ResetTimeout with no recorded outcome, refresh
		// the HalfOpen window: zero the in-flight quota and admit a new trial.
		if cb.now().Sub(cb.lastStateChange) >= 2*cb.config.ResetTimeout && !cb.halfOpenAttempted {
			cb.halfOpenInFlight = 0
			cb.halfOpenAttempted = false
			cb.lastStateChange = cb.now()
		}
	}
	return cb.state
}

// openError builds a CircuitOpenError with the breaker's current open-state
// metadata. Caller must hold cb.mu.
func (cb *CircuitBreaker) openErrorLocked() error {
	openSince := cb.lastStateChange
	retryAt := openSince.Add(cb.config.ResetTimeout)
	return &CircuitOpenError{
		Provider:  cb.config.Name,
		Cause:     cb.lastError,
		OpenSince: openSince,
		RetryAt:   retryAt,
	}
}

// Execute runs fn through the circuit breaker. If the circuit is open, it returns
// a *CircuitOpenError without calling fn. In the half-open state, it limits the
// number of trial requests. Success and failure outcomes are recorded automatically.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	s := cb.stateLocked()

	switch s {
	case StateOpen:
		err := cb.openErrorLocked()
		cb.mu.Unlock()
		return err
	case StateHalfOpen:
		if cb.halfOpenAttempted || cb.halfOpenInFlight >= cb.config.HalfOpenMaxRequests {
			err := cb.openErrorLocked()
			cb.mu.Unlock()
			return err
		}
		cb.halfOpenInFlight++
	}
	cb.mu.Unlock()

	err := fn()

	// RecordSuccess and recordFailureWithCause both reset halfOpenInFlight as
	// part of the outgoing state transition (HalfOpen->Closed on success,
	// HalfOpen->Open on failure), so no separate post-fn bookkeeping lock is
	// needed for the hot-path closed-state caller.
	if err != nil {
		cb.recordFailureWithCause(err)
	} else {
		cb.RecordSuccess()
	}
	return err
}

// RecordSuccess records a successful call and transitions the circuit to closed
// if it was in the half-open state. Always clears half-open bookkeeping so a
// concurrent re-arm cannot leave a stuck in-flight counter behind.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures = 0
	cb.lastError = nil
	cb.halfOpenInFlight = 0
	cb.halfOpenAttempted = false
	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.lastStateChange = cb.now()
	}
}

// RecordFailure records a failed call. If the failure count reaches MaxFailures
// the circuit opens. If the circuit is half-open, any failure re-opens it. The
// error is retained as lastError for inclusion in subsequent CircuitOpenError
// values.
func (cb *CircuitBreaker) RecordFailure() {
	cb.recordFailureWithCause(nil)
}

func (cb *CircuitBreaker) recordFailureWithCause(cause error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures++
	if cause != nil {
		// Don't record an open-state rejection as the "cause" — that would mask
		// the real upstream error.
		if !errors.Is(cause, ErrCircuitOpen) {
			cb.lastError = cause
		}
	}

	switch cb.state {
	case StateClosed:
		if cb.consecutiveFailures >= cb.config.MaxFailures {
			cb.state = StateOpen
			cb.lastStateChange = cb.now()
		}
	case StateHalfOpen:
		cb.state = StateOpen
		cb.lastStateChange = cb.now()
		cb.halfOpenAttempted = true
		cb.halfOpenInFlight = 0
	}
}

// LastError returns the most recent underlying error that contributed to a
// failure count. Returns nil if no failures have been recorded since the last
// success. Useful for status surfaces.
func (cb *CircuitBreaker) LastError() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.lastError
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
// If config.Name is empty the provider's Name() is used so CircuitOpenError can
// identify the source.
func NewCircuitBreakerLLM(provider LLMProvider, config CircuitBreakerConfig) *CircuitBreakerLLM {
	if config.Name == "" {
		config.Name = provider.Name()
	}
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
// the given provider. If config.Name is empty the provider's Name() is used.
func NewCircuitBreakerEmbedding(provider EmbeddingProvider, config CircuitBreakerConfig) *CircuitBreakerEmbedding {
	if config.Name == "" {
		config.Name = provider.Name()
	}
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
