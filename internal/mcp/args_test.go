package mcp

import (
	"math"
	"testing"
)

// TestParseIntArgOverflowProtection pins the float→int safety contract.
// JSON decoders hand numerics back as float64, and int(1e30) on amd64 is
// implementation-defined garbage (typically MinInt64), a value that passes
// naive ">= 0" checks downstream and then underflows in slice expressions
// to panic. parseIntArg clamps to math.MaxInt32 BEFORE the int() cast so
// the resulting value is always in a safe range.
func TestParseIntArgOverflowProtection(t *testing.T) {
	cases := []struct {
		name string
		val  float64
		want int
	}{
		{"huge_positive_clamps_to_max", 1e30, 100},
		{"max_int32_passes_through_clamped", 2_147_483_647, 100},
		{"in_range_value", 50, 50},
		{"nan_falls_back_to_default", math.NaN(), 10},
		{"pos_inf_falls_back_to_default", math.Inf(1), 10},
		{"neg_inf_falls_back_to_default", math.Inf(-1), 10},
		{"negative_falls_back_to_default", -5, 10},
		{"zero_below_minVal_falls_back", 0, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{"k": tc.val}
			got := parseIntArg(args, "k", 10 /*default*/, 1 /*min*/, 100 /*max*/)
			if got != tc.want {
				t.Errorf("parseIntArg(%v) = %d, want %d", tc.val, got, tc.want)
			}
		})
	}
}

// TestParseIntArgMissingOrWrongType pins the fallback when the arg is
// absent or non-numeric. JSON booleans/strings must not bypass the float
// type assertion.
func TestParseIntArgMissingOrWrongType(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
	}{
		{"missing_key", map[string]any{}},
		{"string_value", map[string]any{"k": "50"}},
		{"bool_value", map[string]any{"k": true}},
		{"nil_value", map[string]any{"k": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseIntArg(tc.args, "k", 42, 1, 100)
			if got != 42 {
				t.Errorf("expected fallback to default (42); got %d", got)
			}
		})
	}
}
