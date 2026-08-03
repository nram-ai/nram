package service

import (
	"context"
	"testing"
)

// TestRecallEffectiveLimit_ClampsToMax pins SEC-18: the service caps a recall's
// page size at recall.max_limit so a REST caller cannot request an unbounded
// page, and applies the default when the caller passes a non-positive value. A
// zero-value RecallService resolves through the registered defaults.
func TestRecallEffectiveLimit_ClampsToMax(t *testing.T) {
	s := &RecallService{}
	ctx := context.Background()

	max := GetDefaultInt(SettingRecallMaxLimit)
	def := GetDefaultInt(SettingRecallDefaultLimit)
	if max <= 0 || def <= 0 {
		t.Fatalf("unexpected registered defaults: max=%d def=%d", max, def)
	}

	if got := s.recallEffectiveLimit(ctx, 100000); got != max {
		t.Errorf("oversized limit: got %d, want clamp to max %d", got, max)
	}
	if got := s.recallEffectiveLimit(ctx, 0); got != def {
		t.Errorf("zero limit: got %d, want default %d", got, def)
	}
	if got := s.recallEffectiveLimit(ctx, -5); got != def {
		t.Errorf("negative limit: got %d, want default %d", got, def)
	}
	// A modest positive request under the cap is preserved.
	small := 1
	if small <= max {
		if got := s.recallEffectiveLimit(ctx, small); got != small {
			t.Errorf("small limit: got %d, want %d", got, small)
		}
	}
}
