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
//
// A TokenBudget may be a root budget (parent == nil) or a sub-slice carved
// out of a parent. Sub-slices enforce a per-sub-phase cap while still
// charging spend to the root, so one sub-phase cannot starve another of
// its reserved allocation even when the root has headroom.
type TokenBudget struct {
	total           int
	used            int
	perCallCap      int
	zeroUsageWarned bool
	parent          *TokenBudget
	mu              sync.Mutex
}

// NewTokenBudget creates a new root TokenBudget with the given total budget
// and per-call cap.
func NewTokenBudget(total, perCallCap int) *TokenBudget {
	return &TokenBudget{
		total:      total,
		perCallCap: perCallCap,
	}
}

// SubSlice creates a child budget wrapping this one with the given cap.
// Spend on the child charges both child and parent (so the root budget
// sees every token), but child enforcement means a sub-phase cannot exceed
// its own cap even if the parent has headroom. The per-call cap is
// inherited from the parent.
//
// Callers should size caps so that the sum does not exceed the parent's
// current Remaining; over-allocation is permitted (the root cap always
// wins) but results in later sub-slices being starved.
func (b *TokenBudget) SubSlice(cap int) *TokenBudget {
	if cap < 0 {
		cap = 0
	}
	return &TokenBudget{
		total:      cap,
		perCallCap: b.PerCallCap(),
		parent:     b,
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

// Remaining returns the number of tokens left in the budget. For a
// sub-slice this is min(local remaining, parent remaining) so callers
// that consult Remaining before a spend do not overshoot either cap.
func (b *TokenBudget) Remaining() int {
	b.mu.Lock()
	local := b.total - b.used
	b.mu.Unlock()
	if local < 0 {
		local = 0
	}
	if b.parent == nil {
		return local
	}
	parent := b.parent.Remaining()
	if parent < local {
		return parent
	}
	return local
}

// PerCallCap returns the maximum tokens allowed for a single LLM call.
func (b *TokenBudget) PerCallCap() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.perCallCap
}

// CanAfford returns true if the estimated token count fits within both
// the remaining budget (including parent, if sub-sliced) and the per-call cap.
func (b *TokenBudget) CanAfford(estimated int) bool {
	b.mu.Lock()
	localOK := estimated <= (b.total-b.used) && estimated <= b.perCallCap
	b.mu.Unlock()
	if !localOK {
		return false
	}
	if b.parent != nil {
		return b.parent.CanAfford(estimated)
	}
	return true
}

// Spend records tokens consumed. For a sub-slice, spend is charged to both
// child and parent. Returns ErrBudgetExhausted if the spend causes either
// level to exceed its cap (the spend is still recorded at both levels).
func (b *TokenBudget) Spend(n int) error {
	b.mu.Lock()
	b.used += n
	overLocal := b.used > b.total
	b.mu.Unlock()

	var parentErr error
	if b.parent != nil {
		parentErr = b.parent.Spend(n)
	}
	if parentErr != nil {
		return parentErr
	}
	if overLocal {
		return ErrBudgetExhausted
	}
	return nil
}

// Exhausted returns true if the budget has been fully consumed at this
// level OR at any ancestor. Sub-slice exhaustion ends the sub-phase;
// root exhaustion ends the cycle.
func (b *TokenBudget) Exhausted() bool {
	b.mu.Lock()
	exh := b.used >= b.total
	b.mu.Unlock()
	if exh {
		return true
	}
	if b.parent != nil {
		return b.parent.Exhausted()
	}
	return false
}

// MarkZeroUsageWarned returns true the first time it is called on the
// ROOT budget, and false thereafter (including from sub-slices). Phases
// use it to emit a single warning per cycle when the LLM provider
// returns zero-usage responses (e.g. Ollama's OpenAI-compat endpoint,
// which omits the usage field).
func (b *TokenBudget) MarkZeroUsageWarned() bool {
	if b.parent != nil {
		return b.parent.MarkZeroUsageWarned()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.zeroUsageWarned {
		return false
	}
	b.zeroUsageWarned = true
	return true
}

// EstimateTokens returns a rough token count for a text using the
// 4-bytes-per-token heuristic. Used as a fallback when the LLM provider
// does not report usage in its response.
func EstimateTokens(text string) int {
	return len(text) / 4
}
