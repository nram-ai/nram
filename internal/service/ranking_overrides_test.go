package service

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseRankingOverride_Empty(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(``)} {
		ov, err := ParseRankingOverride(raw)
		if err != nil {
			t.Fatalf("empty input: unexpected error: %v", err)
		}
		if ov.Similarity != nil || ov.Recency != nil || ov.Importance != nil ||
			ov.Frequency != nil || ov.GraphRelevance != nil || ov.Confidence != nil {
			t.Errorf("empty input: expected zero override, got %+v", ov)
		}
	}
}

func TestParseRankingOverride_Sparse(t *testing.T) {
	ov, err := ParseRankingOverride(json.RawMessage(`{"similarity": 0.6}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ov.Similarity == nil || *ov.Similarity != 0.6 {
		t.Errorf("Similarity not set to 0.6, got %v", ov.Similarity)
	}
	if ov.Recency != nil || ov.Importance != nil || ov.Frequency != nil ||
		ov.GraphRelevance != nil || ov.Confidence != nil {
		t.Error("non-similarity fields should remain nil")
	}
}

func TestParseRankingOverride_Legacy(t *testing.T) {
	ov, err := ParseRankingOverride(json.RawMessage(`{"recency":0.3,"relevance":0.5,"importance":0.2}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ov.Similarity == nil || *ov.Similarity != 0.5 {
		t.Errorf("legacy relevance should map to Similarity=0.5, got %v", ov.Similarity)
	}
	if ov.Recency == nil || *ov.Recency != 0.3 {
		t.Errorf("Recency=0.3 expected, got %v", ov.Recency)
	}
	if ov.Importance == nil || *ov.Importance != 0.2 {
		t.Errorf("Importance=0.2 expected, got %v", ov.Importance)
	}
}

func TestParseRankingOverride_LegacyAndCanonical_SimilarityWins(t *testing.T) {
	ov, err := ParseRankingOverride(json.RawMessage(`{"similarity":0.6,"relevance":0.4}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ov.Similarity == nil || *ov.Similarity != 0.6 {
		t.Errorf("canonical similarity should win over legacy relevance, got %v", ov.Similarity)
	}
}

func TestParseRankingOverride_UnknownKeys(t *testing.T) {
	ov, err := ParseRankingOverride(json.RawMessage(`{"similarity":0.5,"unknown_key":42,"another":"text"}`))
	if err != nil {
		t.Fatalf("unknown keys should not error, got %v", err)
	}
	if ov.Similarity == nil || *ov.Similarity != 0.5 {
		t.Error("known fields should still parse alongside unknown keys")
	}
}

func TestParseRankingOverride_OutOfRange(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"too high", `{"similarity":1.5}`},
		{"negative", `{"recency":-0.1}`},
		{"non-number", `{"importance": "NaN"}`}, // strings hit the "not a number" branch
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseRankingOverride(json.RawMessage(tc.raw))
			if err == nil {
				t.Errorf("expected error for %s, got nil", tc.raw)
			}
		})
	}
}

func TestParseRankingOverride_OutOfRangeReportsAllFields(t *testing.T) {
	_, err := ParseRankingOverride(json.RawMessage(`{"similarity":1.5,"confidence":-0.1}`))
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "similarity") || !strings.Contains(msg, "confidence") {
		t.Errorf("error should name both offending fields, got: %s", msg)
	}
}

func TestParseRankingOverride_MalformedJSON(t *testing.T) {
	_, err := ParseRankingOverride(json.RawMessage(`{"similarity":`))
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestMergeWeights_NilOverride(t *testing.T) {
	base := DefaultRankingWeights
	merged := MergeWeights(base, ProjectRankingOverride{})
	if merged != base {
		t.Errorf("nil override should preserve base, got %+v", merged)
	}
}

func TestMergeWeights_SparseOverride(t *testing.T) {
	base := DefaultRankingWeights
	v := 0.30
	merged := MergeWeights(base, ProjectRankingOverride{Similarity: &v})
	if merged.Similarity != 0.30 {
		t.Errorf("Similarity should be replaced with 0.30, got %v", merged.Similarity)
	}
	if merged.Recency != base.Recency || merged.Importance != base.Importance ||
		merged.Frequency != base.Frequency || merged.GraphRelevance != base.GraphRelevance ||
		merged.Confidence != base.Confidence {
		t.Error("non-overridden fields should retain base values")
	}
}

func TestMergeWeights_AllFields(t *testing.T) {
	base := RankingWeights{Similarity: 1, Recency: 1, Importance: 1, Frequency: 1, GraphRelevance: 1, Confidence: 1}
	zero := 0.0
	ov := ProjectRankingOverride{
		Similarity: &zero, Recency: &zero, Importance: &zero,
		Frequency: &zero, GraphRelevance: &zero, Confidence: &zero,
	}
	merged := MergeWeights(base, ov)
	if merged != (RankingWeights{}) {
		t.Errorf("all-fields override should zero everything, got %+v", merged)
	}
}
