package service

import (
	"encoding/json"
	"testing"
)

func TestParseDedupOverride_Empty(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(``)} {
		ov, err := ParseDedupOverride(raw)
		if err != nil {
			t.Fatalf("empty input: unexpected error: %v", err)
		}
		if ov.Threshold != nil {
			t.Errorf("empty input: expected nil threshold, got %v", *ov.Threshold)
		}
	}
}

func TestParseDedupOverride_Number(t *testing.T) {
	ov, err := ParseDedupOverride(json.RawMessage(`0.85`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ov.Threshold == nil || *ov.Threshold != 0.85 {
		t.Errorf("Threshold should be 0.85, got %v", ov.Threshold)
	}
}

func TestParseDedupOverride_OutOfRange(t *testing.T) {
	for _, raw := range []string{`1.5`, `-0.1`, `"NaN"`, `[]`} {
		_, err := ParseDedupOverride(json.RawMessage(raw))
		if err == nil {
			t.Errorf("expected error for %s, got nil", raw)
		}
	}
}

func TestMergeDedupThreshold_Override(t *testing.T) {
	v := 0.85
	if got := MergeDedupThreshold(0.92, DedupOverride{Threshold: &v}); got != 0.85 {
		t.Errorf("override should win, got %v", got)
	}
}

func TestMergeDedupThreshold_NoOverride(t *testing.T) {
	if got := MergeDedupThreshold(0.92, DedupOverride{}); got != 0.92 {
		t.Errorf("no override should preserve base, got %v", got)
	}
}
