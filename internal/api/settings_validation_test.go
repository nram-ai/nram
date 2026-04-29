package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateProjectSettingsJSON_Empty(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(``), json.RawMessage(`{}`)} {
		if err := ValidateProjectSettingsJSON(raw); err != nil {
			t.Errorf("empty/empty-object should pass: %v", err)
		}
	}
}

func TestValidateProjectSettingsJSON_AcceptsCanonical(t *testing.T) {
	raw := json.RawMessage(`{"ranking_weights":{"similarity":0.5,"confidence":0.05},"dedup_threshold":0.92,"enrichment_enabled":true}`)
	if err := ValidateProjectSettingsJSON(raw); err != nil {
		t.Errorf("canonical payload should pass: %v", err)
	}
}

func TestValidateProjectSettingsJSON_AcceptsLegacyRankingShape(t *testing.T) {
	// Legacy shape pre-migration — must not error so existing rows stay loadable.
	raw := json.RawMessage(`{"ranking_weights":{"recency":0.3,"relevance":0.5,"importance":0.2}}`)
	if err := ValidateProjectSettingsJSON(raw); err != nil {
		t.Errorf("legacy shape should pass: %v", err)
	}
}

func TestValidateProjectSettingsJSON_RejectsOutOfRangeWeight(t *testing.T) {
	raw := json.RawMessage(`{"ranking_weights":{"similarity":1.5}}`)
	err := ValidateProjectSettingsJSON(raw)
	if err == nil {
		t.Fatal("expected rejection for similarity > 1.0")
	}
	if !strings.Contains(err.Error(), "similarity") {
		t.Errorf("error should name the offending field, got: %s", err)
	}
}

func TestValidateProjectSettingsJSON_RejectsNegativeWeight(t *testing.T) {
	raw := json.RawMessage(`{"ranking_weights":{"recency":-0.1}}`)
	if err := ValidateProjectSettingsJSON(raw); err == nil {
		t.Error("expected rejection for negative recency")
	}
}

func TestValidateProjectSettingsJSON_RejectsNonNumberWeight(t *testing.T) {
	raw := json.RawMessage(`{"ranking_weights":{"importance":"high"}}`)
	if err := ValidateProjectSettingsJSON(raw); err == nil {
		t.Error("expected rejection for non-numeric weight")
	}
}

func TestValidateProjectSettingsJSON_RejectsBadDedup(t *testing.T) {
	raw := json.RawMessage(`{"dedup_threshold":1.5}`)
	if err := ValidateProjectSettingsJSON(raw); err == nil {
		t.Error("expected rejection for dedup_threshold > 1.0")
	}
}

func TestValidateProjectSettingsJSON_RejectsBadEnrichment(t *testing.T) {
	raw := json.RawMessage(`{"enrichment_enabled":"yes"}`)
	if err := ValidateProjectSettingsJSON(raw); err == nil {
		t.Error("expected rejection for non-boolean enrichment_enabled")
	}
}

func TestValidateProjectSettingsJSON_PassesUnknownKeys(t *testing.T) {
	// Unknown top-level keys (e.g., legacy schemas, future fields) must
	// pass through without errors so handlers don't break on rolling
	// upgrades.
	raw := json.RawMessage(`{"future_field":42,"another":"text"}`)
	if err := ValidateProjectSettingsJSON(raw); err != nil {
		t.Errorf("unknown keys should pass: %v", err)
	}
}

func TestValidateUserSettingsJSON_Empty(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(``), json.RawMessage(`{}`)} {
		if err := ValidateUserSettingsJSON(raw); err != nil {
			t.Errorf("empty/empty-object should pass: %v", err)
		}
	}
}

func TestValidateUserSettingsJSON_AcceptsDedupAndEnrichment(t *testing.T) {
	raw := json.RawMessage(`{"dedup_threshold":0.85,"enrichment_enabled":false}`)
	if err := ValidateUserSettingsJSON(raw); err != nil {
		t.Errorf("dedup + enrichment should pass at user scope: %v", err)
	}
}

func TestValidateUserSettingsJSON_RejectsRankingWeights(t *testing.T) {
	raw := json.RawMessage(`{"ranking_weights":{"similarity":0.5}}`)
	err := ValidateUserSettingsJSON(raw)
	if err == nil {
		t.Fatal("ranking_weights at user scope should be rejected")
	}
	if !strings.Contains(err.Error(), "ranking_weights") {
		t.Errorf("error should name ranking_weights, got: %s", err)
	}
}

func TestValidateUserSettingsJSON_RejectsBadDedup(t *testing.T) {
	raw := json.RawMessage(`{"dedup_threshold":2.0}`)
	if err := ValidateUserSettingsJSON(raw); err == nil {
		t.Error("expected rejection for out-of-range dedup_threshold")
	}
}

func TestValidateUserSettingsJSON_NullRankingWeightsAllowed(t *testing.T) {
	// Explicit null is treated as "not set" — does not trigger the
	// not-supported-at-user-scope rejection.
	raw := json.RawMessage(`{"ranking_weights":null,"dedup_threshold":0.85}`)
	if err := ValidateUserSettingsJSON(raw); err != nil {
		t.Errorf("explicit null ranking_weights should pass: %v", err)
	}
}
