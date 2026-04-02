package dreaming

import (
	"errors"
	"sync"
)

// ErrBudgetExhausted is returned when a token spend exceeds the remaining budget.
var ErrBudgetExhausted = errors.New("dream token budget exhausted")

// ErrExceedsPerCallCap is returned when a single call would exceed the per-call cap.
var ErrExceedsPerCallCap = errors.New("dream call exceeds per-call token cap")

// TokenBudget tracks token consumption within a single dream cycle.
// It is safe for concurrent use.
type TokenBudget struct {
	total     int
	used      int
	perCallCap int
	mu        sync.Mutex
}

// NewTokenBudget creates a new TokenBudget with the given total budget
// and per-call cap.
func NewTokenBudget(total, perCallCap int) *TokenBudget {
	return &TokenBudget{
		total:     total,
		perCallCap: perCallCap,
	}
}

// Total returns the total budget for this cycle.
func (b *TokenBudget) Total() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total
}

// Used returns the number of tokens consumed so far.
func (b *TokenBudget) Used() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Remaining returns the number of tokens left in the budget.
func (b *TokenBudget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total - b.used
}

// PerCallCap returns the maximum tokens allowed for a single LLM call.
func (b *TokenBudget) PerCallCap() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.perCallCap
}

// CanAfford returns true if the estimated token count fits within both
// the remaining budget and the per-call cap.
func (b *TokenBudget) CanAfford(estimated int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return estimated <= (b.total-b.used) && estimated <= b.perCallCap
}

// Spend records tokens consumed. Returns ErrBudgetExhausted if the spend
// causes the total to exceed the budget (the spend is still recorded).
func (b *TokenBudget) Spend(n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used += n
	if b.used > b.total {
		return ErrBudgetExhausted
	}
	return nil
}

// Exhausted returns true if the budget has been fully consumed.
func (b *TokenBudget) Exhausted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used >= b.total
}
