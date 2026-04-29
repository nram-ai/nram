package service

import (
	"fmt"
	"math"
)

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
