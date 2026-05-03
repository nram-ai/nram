package service

import (
	"encoding/json"
	"fmt"
)

// GraphLayoutOverride captures sparse per-project overrides for the 3D graph
// visualization's d3-force parameters. nil pointers fall through to the
// system-level graph.* defaults (registered in settingDefaults).
type GraphLayoutOverride struct {
	CenterGravity  *float64
	ChargeStrength *float64
	LinkDistance   *float64
}

// graphLayoutBounds mirrors the schema entries in settings_store.go. Kept
// here as the single source of truth used by both write-path validation and
// any server-side cascade lookup; UI ranges are pulled from the schema
// endpoint, which serializes the same numbers.
const (
	graphCenterGravityMin  = 0.0
	graphCenterGravityMax  = 3.0
	graphChargeStrengthMin = -100.0
	graphChargeStrengthMax = 0.0
	graphLinkDistanceMin   = 5.0
	graphLinkDistanceMax   = 100.0
)

// graphLayoutSettings is the on-disk shape of the three keys inside
// project.settings JSON. Field tags match the canonical TypeScript field
// names on ProjectSettings.
type graphLayoutSettings struct {
	CenterGravity  *float64 `json:"graph_center_gravity"`
	ChargeStrength *float64 `json:"graph_charge_strength"`
	LinkDistance   *float64 `json:"graph_link_distance"`
}

// ParseGraphLayoutOverride decodes the three optional graph_* fields from a
// project's sparse settings blob. Each field validates against its declared
// range; missing fields are passed through as nil pointers (cascade fallback).
func ParseGraphLayoutOverride(raw json.RawMessage) (GraphLayoutOverride, error) {
	var ov GraphLayoutOverride
	if len(raw) == 0 || string(raw) == "null" {
		return ov, nil
	}
	var s graphLayoutSettings
	if err := json.Unmarshal(raw, &s); err != nil {
		return ov, fmt.Errorf("graph_layout: %w", err)
	}
	if s.CenterGravity != nil {
		if err := ValidateFloatRange("graph_center_gravity", *s.CenterGravity, graphCenterGravityMin, graphCenterGravityMax); err != nil {
			return ov, err
		}
		ov.CenterGravity = s.CenterGravity
	}
	if s.ChargeStrength != nil {
		if err := ValidateFloatRange("graph_charge_strength", *s.ChargeStrength, graphChargeStrengthMin, graphChargeStrengthMax); err != nil {
			return ov, err
		}
		ov.ChargeStrength = s.ChargeStrength
	}
	if s.LinkDistance != nil {
		if err := ValidateFloatRange("graph_link_distance", *s.LinkDistance, graphLinkDistanceMin, graphLinkDistanceMax); err != nil {
			return ov, err
		}
		ov.LinkDistance = s.LinkDistance
	}
	return ov, nil
}
