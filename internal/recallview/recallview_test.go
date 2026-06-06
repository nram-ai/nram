package recallview

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// richResult builds a recall result whose metadata carries the full spread of
// bookkeeping the dreaming/enrichment pipelines stamp, plus dream lineage, the
// novelty markers, and one genuine user key.
func richResult(t *testing.T) (service.RecallResult, uuid.UUID, uuid.UUID) {
	t.Helper()
	src := uuid.New()
	id := uuid.New()
	src2 := "not-a-uuid" // must be dropped silently, not surface as a bad UUID
	meta := map[string]any{
		"dream_cycle_id":                    uuid.NewString(),
		"source_memory_ids":                 []string{src.String(), src2},
		"low_novelty":                       true,
		"low_novelty_reason":                "orphan_no_sources",
		"novelty_audited_at":                "2026-04-26T09:43:17Z",
		"novelty_audit_reason":              "orphan_no_sources",
		"contradictions_checked_at":         "2026-04-26T09:43:17Z",
		"paraphrase_checked_at":             "2026-04-26T09:43:17Z",
		"consolidation_load_checked_at":     "2026-04-26T09:43:17Z",
		"reinforce_checked_at":              "2026-04-26T09:43:17Z",
		"consolidation_cluster_checked_at":  "2026-04-26T09:43:17Z",
		"consolidation_cluster_fingerprint": "abc123",
		"ingestion_decision":                "ADD",
		"ingestion_decision_at":             "2026-04-26T09:43:17Z",
		"ingestion_match_count":             0,
		"ingestion_top_score":               0.0,
		"migrated_from_global":              true,
		"migration_date":                    "2026-05-24",
		"original_global_id":                uuid.NewString(),
		"user_key":                          "keep me",
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal fixture metadata: %v", err)
	}
	srcStr := "imported-2026"
	return service.RecallResult{
		ID:          id,
		ProjectSlug: "fixture",
		Content:     "demoted synthesis",
		Tags:        []string{"alpha", "beta"},
		Source:      &srcStr,
		Origin:      model.OriginDream,
		Score:       0.5,
		Confidence:  0.42,
		Metadata:    raw,
	}, id, src
}

func TestProject_HoistsSignalsAndStripsBookkeeping(t *testing.T) {
	res, id, src := richResult(t)

	got := Project(res, Options{})

	if got.ID != id || got.Origin != model.OriginDream || got.Score != 0.5 {
		t.Errorf("passthrough fields wrong: %+v", got)
	}
	if got.Confidence != 0.42 {
		t.Errorf("expected confidence 0.42, got %v", got.Confidence)
	}
	if !got.LowNovelty {
		t.Errorf("expected low_novelty hoisted to LowNovelty=true")
	}
	if len(got.DerivedFrom) != 1 || got.DerivedFrom[0] != src {
		t.Errorf("expected derived_from=[%s] (bad uuid dropped), got %v", src, got.DerivedFrom)
	}

	// Residual metadata must contain ONLY the genuine user key.
	var residual map[string]any
	if got.Metadata == nil {
		t.Fatalf("expected residual metadata to retain user_key")
	}
	if err := json.Unmarshal(got.Metadata, &residual); err != nil {
		t.Fatalf("residual not valid JSON: %v", err)
	}
	if len(residual) != 1 || residual["user_key"] != "keep me" {
		t.Errorf("expected residual {user_key:keep me}, got %v", residual)
	}
}

func TestProject_IncludeLowNoveltyKeepsReasonNotKey(t *testing.T) {
	res, _, _ := richResult(t)

	got := Project(res, Options{IncludeLowNovelty: true})

	if !got.LowNovelty {
		t.Errorf("expected LowNovelty=true")
	}
	var residual map[string]any
	if err := json.Unmarshal(got.Metadata, &residual); err != nil {
		t.Fatalf("residual not valid JSON: %v", err)
	}
	if _, ok := residual["low_novelty"]; ok {
		t.Errorf("low_novelty key must stay stripped (hoisted to typed field); got %v", residual["low_novelty"])
	}
	if residual["low_novelty_reason"] != "orphan_no_sources" {
		t.Errorf("expected low_novelty_reason preserved under IncludeLowNovelty, got %v", residual["low_novelty_reason"])
	}
	// Audit stamps stay stripped; only include_audit un-strips those.
	for _, k := range []string{"novelty_audited_at", "contradictions_checked_at", "paraphrase_checked_at", "ingestion_decision"} {
		if _, ok := residual[k]; ok {
			t.Errorf("audit key %s leaked under IncludeLowNovelty (that is include_audit's job)", k)
		}
	}
}

// TestProject_NoMetadata exercises the empty-metadata path: no panic, no
// spurious residual, decision fields still emitted (low_novelty defaults false).
func TestProject_NoMetadata(t *testing.T) {
	got := Project(service.RecallResult{ID: uuid.New(), Score: 0.1, Confidence: 0.9}, Options{})
	if got.LowNovelty {
		t.Errorf("expected LowNovelty=false with no metadata")
	}
	if got.DerivedFrom != nil {
		t.Errorf("expected nil derived_from with no metadata, got %v", got.DerivedFrom)
	}
	if got.Metadata != nil {
		t.Errorf("expected nil residual metadata, got %s", got.Metadata)
	}
}
