package dreaming

import (
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

// TestBuildLogSummary_ExcludesPhaseSummary pins the retention half of the
// op-count fix: phase_summary rows are per-phase metadata and must not inflate
// the compressed summary's totals or breakdowns, matching the live OpCount
// semantics in DreamLogWriter.countOp.
func TestBuildLogSummary_ExcludesPhaseSummary(t *testing.T) {
	logs := []model.DreamLog{
		{Phase: "pruning", Operation: model.DreamOpMemoryDeleted, TargetType: "memory"},
		{Phase: "pruning", Operation: model.DreamOpMemoryDemoted, TargetType: "memory"},
		{Phase: "pruning", Operation: model.DreamOpPhaseSummary, TargetType: "phase"},
		{Phase: "entity_dedup", Operation: model.DreamOpEntityMerged, TargetType: "entity"},
		{Phase: "entity_dedup", Operation: model.DreamOpPhaseSummary, TargetType: "phase"},
	}

	got := buildLogSummary(logs)

	// Three real operations across two phases; the two phase_summary rows drop.
	if got.TotalOperations != 3 {
		t.Errorf("TotalOperations: want 3 (phase_summary excluded), got %d", got.TotalOperations)
	}

	if got.ByPhase["pruning"] != 2 {
		t.Errorf("ByPhase[pruning]: want 2, got %d", got.ByPhase["pruning"])
	}
	if got.ByPhase["entity_dedup"] != 1 {
		t.Errorf("ByPhase[entity_dedup]: want 1, got %d", got.ByPhase["entity_dedup"])
	}

	// phase_summary must not appear as an operation type or a target type.
	if n, ok := got.ByOperation[model.DreamOpPhaseSummary]; ok {
		t.Errorf("ByOperation must not contain phase_summary, got count %d", n)
	}
	if n, ok := got.ByTargetType["phase"]; ok {
		t.Errorf("ByTargetType must not contain the phase metadata target, got count %d", n)
	}

	if got.ByTargetType["memory"] != 2 {
		t.Errorf("ByTargetType[memory]: want 2, got %d", got.ByTargetType["memory"])
	}
}

// TestBuildLogSummary_OnlyPhaseSummaries confirms a cycle whose logs are all
// metadata summarizes to zero operations rather than the row count.
func TestBuildLogSummary_OnlyPhaseSummaries(t *testing.T) {
	logs := []model.DreamLog{
		{Phase: "pruning", Operation: model.DreamOpPhaseSummary, TargetType: "phase"},
		{Phase: "weight_adjustment", Operation: model.DreamOpPhaseSummary, TargetType: "phase"},
	}

	got := buildLogSummary(logs)

	if got.TotalOperations != 0 {
		t.Errorf("TotalOperations: want 0, got %d", got.TotalOperations)
	}
	if len(got.ByOperation) != 0 {
		t.Errorf("ByOperation: want empty, got %v", got.ByOperation)
	}
}
