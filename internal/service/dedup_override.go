package service

import (
	"encoding/json"
	"fmt"
	"math"
)

// DedupOverride captures a sparse per-namespace override for the dedup
// threshold. A nil Threshold means "fall through to the system-level
// enrichment.dedup_threshold setting."
type DedupOverride struct {
	Threshold *float64
}

// ParseDedupOverride decodes a single-field JSON value into a DedupOverride.
// Accepts either a bare number (legacy shape, where settings.dedup_threshold
// was a top-level numeric field) or an object {"dedup_threshold": <num>}
// (canonical). Returns an error if the value is non-finite or outside
// [0.0, 1.0].
func ParseDedupOverride(raw json.RawMessage) (DedupOverride, error) {
	var ov DedupOverride
	if len(raw) == 0 || string(raw) == "null" {
		return ov, nil
	}
	// Try bare-number first (matches the legacy top-level project.settings
	// shape: {"dedup_threshold": 0.92}).
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return ov, fmt.Errorf("dedup_threshold: must be finite")
		}
		if f < 0 || f > 1 {
			return ov, fmt.Errorf("dedup_threshold: must be in [0.0, 1.0]")
		}
		ov.Threshold = &f
		return ov, nil
	}
	return ov, fmt.Errorf("dedup_threshold: not a number")
}

// MergeDedupThreshold returns the override's value when set, otherwise base.
func MergeDedupThreshold(base float64, ov DedupOverride) float64 {
	if ov.Threshold != nil {
		return *ov.Threshold
	}
	return base
}
