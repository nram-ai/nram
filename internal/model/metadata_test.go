package model

import (
	"encoding/json"
	"testing"
)

func TestSanitizeRelocatedMetadata(t *testing.T) {
	t.Run("strips namespace-local refs, keeps audit/user keys", func(t *testing.T) {
		in := json.RawMessage(`{"source_memory_ids":["a"],"dream_cycle_id":"c","low_novelty":true,"ingestion_target_id":"t","ingestion_decision":"ADD","user_key":"keep"}`)
		var got map[string]json.RawMessage
		if err := json.Unmarshal(SanitizeRelocatedMetadata(in), &got); err != nil {
			t.Fatalf("result is not a JSON object: %v", err)
		}
		for _, k := range []string{DreamMetaSourceMemoryIDs, DreamMetaCycleID, MetaLowNovelty, IngestionMetaTargetID} {
			if _, ok := got[k]; ok {
				t.Errorf("key %q must be stripped, got %v", k, got)
			}
		}
		for _, k := range []string{IngestionMetaDecision, "user_key"} {
			if _, ok := got[k]; !ok {
				t.Errorf("key %q must be preserved, got %v", k, got)
			}
		}
	})

	t.Run("returns input unchanged when no provenance keys present", func(t *testing.T) {
		in := json.RawMessage(`{"user_key":"keep"}`)
		if got := SanitizeRelocatedMetadata(in); string(got) != string(in) {
			t.Errorf("expected input returned verbatim, got %s", got)
		}
	})

	t.Run("empty input returned unchanged", func(t *testing.T) {
		if got := SanitizeRelocatedMetadata(nil); got != nil {
			t.Errorf("expected nil for nil input, got %s", got)
		}
		empty := json.RawMessage("")
		if got := SanitizeRelocatedMetadata(empty); len(got) != 0 {
			t.Errorf("expected empty for empty input, got %s", got)
		}
	})

	t.Run("malformed JSON returned unchanged", func(t *testing.T) {
		in := json.RawMessage(`not json`)
		if got := SanitizeRelocatedMetadata(in); string(got) != string(in) {
			t.Errorf("expected malformed input returned verbatim, got %s", got)
		}
	})
}
