package service

import (
	"encoding/json"
	"fmt"
)

// DreamingEnabledOverride captures a sparse per-namespace override for
// whether dreaming runs on a project. A nil Enabled means "fall through to
// the system-level dreaming.enabled setting." Mirrors EnrichmentEnabledOverride
// (same parse/merge contract), so cascade resolution composes uniformly.
type DreamingEnabledOverride struct {
	Enabled *bool
}

// ParseDreamingEnabledOverride decodes a JSON boolean into an override.
// Accepts a bare boolean ({"dreaming_enabled": true}). Returns an error if
// the value is not a boolean.
func ParseDreamingEnabledOverride(raw json.RawMessage) (DreamingEnabledOverride, error) {
	var ov DreamingEnabledOverride
	if len(raw) == 0 || string(raw) == "null" {
		return ov, nil
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return ov, fmt.Errorf("dreaming_enabled: not a boolean")
	}
	ov.Enabled = &b
	return ov, nil
}

// MergeDreamingEnabled returns the override's value when set, otherwise base.
func MergeDreamingEnabled(base bool, ov DreamingEnabledOverride) bool {
	if ov.Enabled != nil {
		return *ov.Enabled
	}
	return base
}
