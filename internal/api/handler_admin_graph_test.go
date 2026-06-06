package api

import (
	"testing"
)

func TestApplyEdgeCap_NoTruncationUnderCap(t *testing.T) {
	entities := []GraphEntity{
		{ID: "a"}, {ID: "b"}, {ID: "c"},
	}
	rels := []GraphRelationship{
		{ID: "1", SourceID: "a", TargetID: "b", Weight: 0.5},
		{ID: "2", SourceID: "b", TargetID: "c", Weight: 0.3},
	}

	resp := applyEdgeCap(entities, rels, 10)

	if resp.Truncated {
		t.Fatalf("expected Truncated=false, got true")
	}
	if resp.TotalEdges != 0 || resp.ReturnedEdges != 0 {
		t.Fatalf("expected zero TotalEdges/ReturnedEdges when not truncating, got total=%d returned=%d", resp.TotalEdges, resp.ReturnedEdges)
	}
	if len(resp.Relationships) != 2 || len(resp.Entities) != 3 {
		t.Fatalf("expected unmodified slices, got %d entities, %d edges", len(resp.Entities), len(resp.Relationships))
	}
}

func TestApplyEdgeCap_TopByWeightPreservesAllEntities(t *testing.T) {
	// Five edges across four entities. Cap at 2 keeps the two highest-weight
	// edges. Entities are passed through unchanged regardless of edge
	// truncation; isolated nodes stay visible so namespace inventory views
	// don't silently change shape at the cap boundary.
	entities := []GraphEntity{
		{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"},
	}
	rels := []GraphRelationship{
		{ID: "e1", SourceID: "a", TargetID: "b", Weight: 0.10},
		{ID: "e2", SourceID: "b", TargetID: "c", Weight: 0.90},
		{ID: "e3", SourceID: "c", TargetID: "d", Weight: 0.20},
		{ID: "e4", SourceID: "d", TargetID: "a", Weight: 0.05},
		{ID: "e5", SourceID: "a", TargetID: "c", Weight: 0.80},
	}

	resp := applyEdgeCap(entities, rels, 2)

	if !resp.Truncated {
		t.Fatalf("expected Truncated=true")
	}
	if resp.TotalEdges != 5 {
		t.Fatalf("expected TotalEdges=5, got %d", resp.TotalEdges)
	}
	if resp.ReturnedEdges != 2 {
		t.Fatalf("expected ReturnedEdges=2, got %d", resp.ReturnedEdges)
	}
	if len(resp.Relationships) != 2 {
		t.Fatalf("expected 2 edges returned, got %d", len(resp.Relationships))
	}

	got := map[string]bool{resp.Relationships[0].ID: true, resp.Relationships[1].ID: true}
	if !got["e2"] || !got["e5"] {
		t.Fatalf("expected top-by-weight edges {e2, e5}, got %v", got)
	}

	if len(resp.Entities) != 4 {
		t.Fatalf("expected all 4 entities to be retained regardless of edge truncation, got %d", len(resp.Entities))
	}
}

func TestApplyEdgeCap_DoesNotMutateInput(t *testing.T) {
	// The helper must not reorder or alias the caller's slice. A future
	// caller may keep the slice around for telemetry / audit / async event
	// publish; silent in-place mutation would corrupt those consumers.
	rels := []GraphRelationship{
		{ID: "e1", SourceID: "a", TargetID: "b", Weight: 0.10},
		{ID: "e2", SourceID: "b", TargetID: "c", Weight: 0.90},
		{ID: "e3", SourceID: "c", TargetID: "d", Weight: 0.20},
		{ID: "e4", SourceID: "d", TargetID: "a", Weight: 0.05},
		{ID: "e5", SourceID: "a", TargetID: "c", Weight: 0.80},
	}
	original := make([]GraphRelationship, len(rels))
	copy(original, rels)

	_ = applyEdgeCap([]GraphEntity{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}, rels, 2)

	for i := range rels {
		if rels[i] != original[i] {
			t.Fatalf("input slice reordered at index %d: original=%+v current=%+v", i, original[i], rels[i])
		}
	}
}

func TestApplyEdgeCap_StableTiebreakerOnEqualWeights(t *testing.T) {
	// Two edges tie at weight 0.5. The sort must pick them deterministically
	// using the secondary ID key so the same namespace returns the same
	// top-N regardless of DB row return order (which differs across
	// replicas, after VACUUM, etc.). Use IDs that sort opposite of the
	// input order to prove the secondary key is consulted.
	entities := []GraphEntity{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	relsInputOrder := []GraphRelationship{
		{ID: "z", SourceID: "a", TargetID: "b", Weight: 0.5},
		{ID: "y", SourceID: "b", TargetID: "c", Weight: 0.5},
		{ID: "x", SourceID: "a", TargetID: "c", Weight: 0.5},
	}
	respA := applyEdgeCap(entities, relsInputOrder, 2)

	relsReverseOrder := []GraphRelationship{
		{ID: "x", SourceID: "a", TargetID: "c", Weight: 0.5},
		{ID: "y", SourceID: "b", TargetID: "c", Weight: 0.5},
		{ID: "z", SourceID: "a", TargetID: "b", Weight: 0.5},
	}
	respB := applyEdgeCap(entities, relsReverseOrder, 2)

	if respA.Relationships[0].ID != respB.Relationships[0].ID ||
		respA.Relationships[1].ID != respB.Relationships[1].ID {
		t.Fatalf("top-N selection depends on input order: A=%v B=%v",
			[]string{respA.Relationships[0].ID, respA.Relationships[1].ID},
			[]string{respB.Relationships[0].ID, respB.Relationships[1].ID})
	}
	// Lowest IDs win the tiebreaker (ascending): x, y.
	if respA.Relationships[0].ID != "x" || respA.Relationships[1].ID != "y" {
		t.Fatalf("expected ID-ascending tiebreaker {x, y}, got {%s, %s}",
			respA.Relationships[0].ID, respA.Relationships[1].ID)
	}
}

func TestApplyEdgeCap_CapZeroDisablesTruncation(t *testing.T) {
	entities := []GraphEntity{{ID: "a"}, {ID: "b"}}
	rels := []GraphRelationship{
		{ID: "1", SourceID: "a", TargetID: "b", Weight: 0.5},
		{ID: "2", SourceID: "a", TargetID: "b", Weight: 0.3},
	}

	resp := applyEdgeCap(entities, rels, 0)

	if resp.Truncated {
		t.Fatalf("expected Truncated=false when maxEdges<=0")
	}
	if len(resp.Relationships) != 2 {
		t.Fatalf("expected all edges returned when cap is disabled, got %d", len(resp.Relationships))
	}
}

func TestApplyEdgeCap_ExactlyAtCap(t *testing.T) {
	entities := []GraphEntity{{ID: "a"}, {ID: "b"}}
	rels := []GraphRelationship{
		{ID: "1", SourceID: "a", TargetID: "b", Weight: 0.5},
		{ID: "2", SourceID: "a", TargetID: "b", Weight: 0.3},
	}

	resp := applyEdgeCap(entities, rels, 2)

	if resp.Truncated {
		t.Fatalf("expected Truncated=false when edge count == cap")
	}
	if len(resp.Relationships) != 2 {
		t.Fatalf("expected both edges retained, got %d", len(resp.Relationships))
	}
}
