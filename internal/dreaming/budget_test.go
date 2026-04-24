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
	// 60 fits in child (local=0 used of 200) but exceeds parent's 50 remaining.
	if child.CanAfford(60) {
		t.Error("CanAfford(60) should be false because parent cannot afford it")
	}
	if !child.CanAfford(50) {
		t.Error("CanAfford(50) should be true — fits both parent and child")
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
