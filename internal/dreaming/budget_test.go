package dreaming

import (
	"errors"
	"testing"
)

func TestTokenBudget_RootSpendExhausts(t *testing.T) {
	b := NewTokenBudget(100, 50)
	if err := b.Spend(40); err != nil {
		t.Fatalf("first spend: %v", err)
	}
	if b.Remaining() != 60 {
		t.Errorf("remaining=%d want 60", b.Remaining())
	}
	if err := b.Spend(70); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("second spend should exhaust, got %v", err)
	}
	if !b.Exhausted() {
		t.Error("Exhausted() should be true after over-spend")
	}
}

func TestTokenBudget_SubSliceEnforcesChildCapAndChargesParent(t *testing.T) {
	parent := NewTokenBudget(1000, 100)
	child := parent.SubSlice(200)

	if err := child.Spend(150); err != nil {
		t.Fatalf("child spend within cap: %v", err)
	}
	if parent.Used() != 150 {
		t.Errorf("parent.Used=%d want 150 (spend should cascade)", parent.Used())
	}
	if child.Used() != 150 {
		t.Errorf("child.Used=%d want 150", child.Used())
	}

	// Spend more than the child's remaining, but within parent's remaining.
	// Child should report exhaustion even though parent has headroom.
	if err := child.Spend(100); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("child spend should exhaust child, got %v", err)
	}
	if !child.Exhausted() {
		t.Error("child.Exhausted() should be true")
	}
	if parent.Exhausted() {
		t.Error("parent.Exhausted() should be false while parent has headroom")
	}
	if parent.Used() != 250 {
		t.Errorf("parent.Used=%d want 250 after over-spend cascades", parent.Used())
	}
}

func TestTokenBudget_ParentExhaustionEndsChild(t *testing.T) {
	parent := NewTokenBudget(100, 50)
	child := parent.SubSlice(300) // over-allocated vs parent
	// Draining the parent must make the child see Exhausted() too.
	if err := parent.Spend(100); err != nil {
		t.Fatalf("parent drain: %v", err)
	}
	if !child.Exhausted() {
		t.Error("child should report Exhausted when parent is drained")
	}
	if child.Remaining() != 0 {
		t.Errorf("child.Remaining=%d want 0 when parent is drained", child.Remaining())
	}
}

func TestTokenBudget_SubSliceCanAffordRespectsBothLevels(t *testing.T) {
	parent := NewTokenBudget(500, 100)
	// Drain most of the parent before slicing.
	if err := parent.Spend(450); err != nil {
		t.Fatalf("parent prefill: %v", err)
	}
	// Child cap larger than parent's remaining.
	child := parent.SubSlice(200)
	// 60 fits in the child's local capacity (used=0 of 200) but exceeds the
	// parent's 50 remaining; CanAfford must walk the parent chain and reject.
	if child.CanAfford(60) {
		t.Error("CanAfford(60) should be false because parent cannot afford it")
	}
	if !child.CanAfford(50) {
		t.Error("CanAfford(50) should be true: fits both parent and child")
	}
}

// TestTokenBudget_CanAffordIgnoresPerCallCap verifies that CanAfford treats
// PerCallCap as orthogonal: it is the response-MaxTokens cap, not a per-call
// total-cost ceiling. Phases compose `EstimateTokens(prompt) + PerCallCap()`
// before passing to CanAfford, so a fresh budget with plenty of room must
// afford spends larger than PerCallCap.
func TestTokenBudget_CanAffordIgnoresPerCallCap(t *testing.T) {
	b := NewTokenBudget(1000, 100)
	if !b.CanAfford(500) {
		t.Error("CanAfford(500) on a 1000-cap budget with perCallCap=100 must be true (perCallCap is unrelated to total-spend fitness)")
	}
	if !b.CanAfford(1000) {
		t.Error("CanAfford(1000) on a 1000-cap fresh budget must be true (boundary case: equal to remaining)")
	}
	if b.CanAfford(1001) {
		t.Error("CanAfford(1001) on a 1000-cap budget must be false")
	}
}

func TestTokenBudget_UnspentSubSliceReleasesAutomatically(t *testing.T) {
	parent := NewTokenBudget(1000, 100)
	a := parent.SubSlice(350)
	if err := a.Spend(100); err != nil {
		t.Fatalf("a spend: %v", err)
	}
	// Even though a had 350 cap, parent only counts actual spend (100),
	// leaving 900 remaining for a sibling slice.
	b := parent.SubSlice(350)
	if b.Remaining() != 350 {
		t.Errorf("sibling b.Remaining=%d want 350 (parent has 900, cap is 350)", b.Remaining())
	}
	if parent.Remaining() != 900 {
		t.Errorf("parent.Remaining=%d want 900 after a spent only 100 of its 350", parent.Remaining())
	}
}

func TestTokenBudget_ProportionalSliceCap(t *testing.T) {
	b := NewTokenBudget(1000, 100)

	// Boundary: frac == sumRemaining → entire Remaining is allocated.
	if got := b.ProportionalSliceCap(0.40, 0.40); got != 1000 {
		t.Errorf("frac==sum on fresh budget: got %d, want 1000", got)
	}
	// Standard split: frac=0.40 of total weight 0.80 against Remaining=1000.
	if got := b.ProportionalSliceCap(0.40, 0.80); got != 500 {
		t.Errorf("frac=0.40 sum=0.80 Remaining=1000: got %d, want 500", got)
	}
	// Headroom absorption: frac=0.10 of weight sum=0.95 → 105, strictly > 100.
	if got := b.ProportionalSliceCap(0.10, 0.95); got != 105 {
		t.Errorf("frac=0.10 sum=0.95 Remaining=1000: got %d, want 105 (headroom absorbed)", got)
	}
	// Degenerate: frac<=0 or sum<=0 must return 0 without dividing.
	if got := b.ProportionalSliceCap(0, 0.40); got != 0 {
		t.Errorf("frac=0: got %d, want 0", got)
	}
	if got := b.ProportionalSliceCap(0.40, 0); got != 0 {
		t.Errorf("sum=0: got %d, want 0", got)
	}
	if got := b.ProportionalSliceCap(-0.10, 0.40); got != 0 {
		t.Errorf("frac<0: got %d, want 0", got)
	}

	// After spend, Remaining shrinks and the cap shrinks with it.
	if err := b.Spend(400); err != nil {
		t.Fatalf("Spend: %v", err)
	}
	if got := b.ProportionalSliceCap(0.40, 0.40); got != 600 {
		t.Errorf("after Spend(400): got %d, want 600 (Remaining=600 * 0.40 / 0.40)", got)
	}
}

func TestTokenBudget_MarkZeroUsageWarnedDelegatesToRoot(t *testing.T) {
	parent := NewTokenBudget(1000, 100)
	a := parent.SubSlice(300)
	b := parent.SubSlice(300)
	// First call on a child should succeed; subsequent calls anywhere in
	// the tree should return false because the warning is a once-per-cycle
	// state held at the root.
	if !a.MarkZeroUsageWarned() {
		t.Error("first MarkZeroUsageWarned via child should return true")
	}
	if b.MarkZeroUsageWarned() {
		t.Error("second MarkZeroUsageWarned via sibling should return false")
	}
	if parent.MarkZeroUsageWarned() {
		t.Error("MarkZeroUsageWarned on root should return false after child already warned")
	}
}
