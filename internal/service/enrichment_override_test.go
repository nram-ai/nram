package service

import (
	"encoding/json"
	"testing"
)

func TestParseEnrichmentEnabledOverride_Empty(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(``)} {
		ov, err := ParseEnrichmentEnabledOverride(raw)
		if err != nil {
			t.Fatalf("empty input: unexpected error: %v", err)
		}
		if ov.Enabled != nil {
			t.Errorf("empty input: expected nil, got %v", *ov.Enabled)
		}
	}
}

func TestParseEnrichmentEnabledOverride_Boolean(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"true", true},
		{"false", false},
	} {
		ov, err := ParseEnrichmentEnabledOverride(json.RawMessage(tc.raw))
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.raw, err)
		}
		if ov.Enabled == nil || *ov.Enabled != tc.want {
			t.Errorf("%s: expected %v, got %v", tc.raw, tc.want, ov.Enabled)
		}
	}
}

func TestParseEnrichmentEnabledOverride_NonBoolean(t *testing.T) {
	for _, raw := range []string{`"true"`, `1`, `0.5`, `[]`, `{}`} {
		_, err := ParseEnrichmentEnabledOverride(json.RawMessage(raw))
		if err == nil {
			t.Errorf("expected error for %s, got nil", raw)
		}
	}
}

func TestMergeEnrichmentEnabled_OverrideOff(t *testing.T) {
	off := false
	if got := MergeEnrichmentEnabled(true, EnrichmentEnabledOverride{Enabled: &off}); got {
		t.Error("override=false should win over base=true")
	}
}

func TestMergeEnrichmentEnabled_OverrideOn(t *testing.T) {
	on := true
	if got := MergeEnrichmentEnabled(false, EnrichmentEnabledOverride{Enabled: &on}); !got {
		t.Error("override=true should win over base=false")
	}
}

func TestMergeEnrichmentEnabled_NoOverride(t *testing.T) {
	if got := MergeEnrichmentEnabled(true, EnrichmentEnabledOverride{}); !got {
		t.Error("no override should preserve base=true")
	}
	if got := MergeEnrichmentEnabled(false, EnrichmentEnabledOverride{}); got {
		t.Error("no override should preserve base=false")
	}
}
