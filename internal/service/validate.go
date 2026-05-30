package service

import (
	"fmt"
	"math"
	"strings"

	"github.com/nram-ai/nram/internal/model"
)

// isReservedSource reports whether s is the reserved "dream" provenance label
// (case- and whitespace-insensitive). The consolidation cycle owns it via
// Origin=OriginDream; user-facing write paths must never let it back into the
// source column, or the string-as-discriminator footgun returns.
func isReservedSource(s string) bool {
	return strings.EqualFold(strings.TrimSpace(s), model.DreamSource)
}

// ValidateUnitFloat checks that v is finite and in [0.0, 1.0]. Returns a
// caller-friendly error naming the offending field so handlers can reuse
// the same wording across the API surface.
func ValidateUnitFloat(name string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("%s: must be finite", name)
	}
	if v < 0 || v > 1 {
		return fmt.Errorf("%s: must be in [0.0, 1.0]", name)
	}
	return nil
}

// ValidateFloatRange checks that v is finite and in [min, max]. Used by
// per-project setting overrides whose admissible range is not the unit
// interval (e.g. graph layout forces).
func ValidateFloatRange(name string, v, min, max float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("%s: must be finite", name)
	}
	if v < min || v > max {
		return fmt.Errorf("%s: must be in [%g, %g]", name, min, max)
	}
	return nil
}
