package logging

import (
	"context"
	"testing"
	"time"
)

type fakePruner struct {
	olderThanCalls int
	beyondCalls    int
	lastKeep       int
	lastCutoff     time.Time
}

func (f *fakePruner) DeleteOlderThan(_ context.Context, before time.Time) (int64, error) {
	f.olderThanCalls++
	f.lastCutoff = before
	return 0, nil
}

func (f *fakePruner) DeleteBeyondCount(_ context.Context, keep int) (int64, error) {
	f.beyondCalls++
	f.lastKeep = keep
	return 0, nil
}

func TestRetentionSweeper_AppliesBothLimits(t *testing.T) {
	p := &fakePruner{}
	s := NewRetentionSweeper(p, func(context.Context) RetentionLimits {
		return RetentionLimits{MaxRows: 100, MaxAgeDays: 7}
	})
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if p.olderThanCalls != 1 || p.beyondCalls != 1 {
		t.Fatalf("expected both limits applied, got age=%d count=%d", p.olderThanCalls, p.beyondCalls)
	}
	if p.lastKeep != 100 {
		t.Fatalf("count cap: got keep=%d", p.lastKeep)
	}
	// Cutoff is ~7 days ago.
	if d := time.Since(p.lastCutoff); d < 6*24*time.Hour || d > 8*24*time.Hour {
		t.Fatalf("age cutoff out of range: %v ago", d)
	}
}

func TestRetentionSweeper_DisabledLimitsAreSkipped(t *testing.T) {
	p := &fakePruner{}
	s := NewRetentionSweeper(p, func(context.Context) RetentionLimits {
		return RetentionLimits{MaxRows: 0, MaxAgeDays: 0}
	})
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if p.olderThanCalls != 0 || p.beyondCalls != 0 {
		t.Fatalf("expected no pruning when both limits are 0, got age=%d count=%d", p.olderThanCalls, p.beyondCalls)
	}
}
