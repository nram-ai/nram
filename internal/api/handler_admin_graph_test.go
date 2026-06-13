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

func TestApplyEdgeCap_StrongestEdgesWhenTheyFormForest(t *testing.T) {
	// Five edges across four entities, cap at 2. The two highest-weight edges
	// (e2 b-c 0.90, e5 a-c 0.80) join distinct components, so the spanning
	// forest pass selects exactly them and the result coincides with pure
	// top-by-weight. Entities are passed through unchanged regardless of edge
	// truncation; the full namespace inventory is preserved at the data layer.
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
		t.Fatalf("expected strongest forest edges {e2, e5}, got %v", got)
	}

	if len(resp.Entities) != 4 {
		t.Fatalf("expected all 4 entities to be retained regardless of edge truncation, got %d", len(resp.Entities))
	}
}

func TestApplyEdgeCap_ConnectivityKeepsLowWeightSpokesAttached(t *testing.T) {
	// A dense high-weight cluster (c1..c4) plus a hub h with three low-weight
	// spokes (s1, s2, s3), all one component via h-c1. Cap at 7.
	//
	// Pure top-by-weight would spend the budget on the five cluster edges
	// plus h-c1 plus the single strongest spoke, stranding s2 and s3. The
	// spanning-forest pass instead keeps one edge per node, so every spoke
	// stays attached and the redundant intra-cluster cycle edges (ce4, ce5)
	// are the ones dropped, trading weight for connectivity.
	entities := []GraphEntity{
		{ID: "h"}, {ID: "s1"}, {ID: "s2"}, {ID: "s3"},
		{ID: "c1"}, {ID: "c2"}, {ID: "c3"}, {ID: "c4"},
	}
	rels := []GraphRelationship{
		{ID: "ce1", SourceID: "c1", TargetID: "c2", Weight: 0.99},
		{ID: "ce2", SourceID: "c2", TargetID: "c3", Weight: 0.98},
		{ID: "ce3", SourceID: "c3", TargetID: "c4", Weight: 0.97},
		{ID: "ce4", SourceID: "c1", TargetID: "c3", Weight: 0.96},
		{ID: "ce5", SourceID: "c2", TargetID: "c4", Weight: 0.95},
		{ID: "hc", SourceID: "h", TargetID: "c1", Weight: 0.50},
		{ID: "sp1", SourceID: "h", TargetID: "s1", Weight: 0.10},
		{ID: "sp2", SourceID: "h", TargetID: "s2", Weight: 0.09},
		{ID: "sp3", SourceID: "h", TargetID: "s3", Weight: 0.08},
	}

	resp := applyEdgeCap(entities, rels, 7)

	if !resp.Truncated || resp.TotalEdges != 9 || resp.ReturnedEdges != 7 {
		t.Fatalf("expected Truncated=true total=9 returned=7, got truncated=%v total=%d returned=%d",
			resp.Truncated, resp.TotalEdges, resp.ReturnedEdges)
	}

	touched := make(map[string]bool)
	for _, r := range resp.Relationships {
		touched[r.SourceID] = true
		touched[r.TargetID] = true
	}
	for _, id := range []string{"h", "s1", "s2", "s3", "c1", "c2", "c3", "c4"} {
		if !touched[id] {
			t.Fatalf("node %q was stranded with no surviving edge; spanning forest should attach every node-with-edges", id)
		}
	}

	// The weakest spoke survived while a higher-weight cluster cycle edge was
	// dropped: connectivity was prioritized over raw weight.
	kept := make(map[string]bool)
	for _, r := range resp.Relationships {
		kept[r.ID] = true
	}
	if !kept["sp3"] {
		t.Fatalf("expected weakest spoke sp3 retained for connectivity")
	}
	if kept["ce4"] && kept["ce5"] {
		t.Fatalf("expected at least one redundant cluster cycle edge (ce4/ce5) dropped in favor of spokes")
	}
}

func TestApplyEdgeCap_FillSpendsRemainingBudgetOnStrongestEdges(t *testing.T) {
	// Triangle a,b,c with redundant parallel edges. Cap at 4. The forest needs
	// only 2 edges to connect all three nodes; the remaining 2 of the budget
	// must go to the strongest not-yet-chosen edges, leaving only the single
	// weakest edge (t5) out.
	entities := []GraphEntity{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	rels := []GraphRelationship{
		{ID: "t1", SourceID: "a", TargetID: "b", Weight: 0.90},
		{ID: "t2", SourceID: "b", TargetID: "c", Weight: 0.80},
		{ID: "t3", SourceID: "a", TargetID: "c", Weight: 0.70},
		{ID: "t4", SourceID: "a", TargetID: "b", Weight: 0.60},
		{ID: "t5", SourceID: "b", TargetID: "c", Weight: 0.50},
	}

	resp := applyEdgeCap(entities, rels, 4)

	if !resp.Truncated || resp.ReturnedEdges != 4 {
		t.Fatalf("expected Truncated=true returned=4, got truncated=%v returned=%d", resp.Truncated, resp.ReturnedEdges)
	}
	kept := make(map[string]bool)
	for _, r := range resp.Relationships {
		kept[r.ID] = true
	}
	for _, id := range []string{"t1", "t2", "t3", "t4"} {
		if !kept[id] {
			t.Fatalf("expected strongest edge %q retained after fill, got %v", id, kept)
		}
	}
	if kept["t5"] {
		t.Fatalf("expected weakest edge t5 dropped, got %v", kept)
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
