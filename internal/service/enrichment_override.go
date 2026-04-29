package service

import (
	"encoding/json"
	"fmt"
)

// EnrichmentEnabledOverride captures a sparse per-namespace override for
// whether enrichment runs on a memory. A nil Enabled means "fall through to
// the system-level enrichment.enabled setting."
type EnrichmentEnabledOverride struct {
	Enabled *bool
}

// ParseEnrichmentEnabledOverride decodes a JSON boolean into an override.
// Accepts a bare boolean (legacy top-level project.settings shape,
// {"enrichment_enabled": true}). Returns an error if the value is not a
// boolean.
func ParseEnrichmentEnabledOverride(raw json.RawMessage) (EnrichmentEnabledOverride, error) {
	var ov EnrichmentEnabledOverride
	if len(raw) == 0 || string(raw) == "null" {
		return ov, nil
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return ov, fmt.Errorf("enrichment_enabled: not a boolean")
	}
	ov.Enabled = &b
	return ov, nil
}

// MergeEnrichmentEnabled returns the override's value when set, otherwise
// base.
func MergeEnrichmentEnabled(base bool, ov EnrichmentEnabledOverride) bool {
	if ov.Enabled != nil {
		return *ov.Enabled
	}
	return base
}
