package mcp

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func ent(id uuid.UUID, name string, mentions int) graphEntity {
	return graphEntity{ID: id, Name: name, Type: "concept", MentionCount: mentions}
}

func relAt(src, tgt uuid.UUID, relation string, weight float64) graphRelationship {
	return graphRelationship{SourceID: src, TargetID: tgt, Relation: relation, Weight: weight}
}

func seedSet(ids ...uuid.UUID) map[uuid.UUID]struct{} {
	s := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

func cloneRels(r []graphRelationship) []graphRelationship {
	return append([]graphRelationship(nil), r...)
}

func cloneEnts(e []graphEntity) []graphEntity {
	return append([]graphEntity(nil), e...)
}

// TestDedupGraphRelationships verifies relation-variant collapse: same (src,tgt)
// with formatting-variant relations becomes one canonical edge at max weight,
// the survivor carries the max-weight row's metadata, and a genuinely distinct
// relation on the same pair stays separate.
func TestDedupGraphRelationships(t *testing.T) {
	src, tgt := uuid.New(), uuid.New()
	mem := uuid.New()
	rels := []graphRelationship{
		{SourceID: src, TargetID: tgt, Relation: "related_to", Weight: 0.4},
		{SourceID: src, TargetID: tgt, Relation: "related to", Weight: 0.7, SourceMemory: &mem},
		{SourceID: src, TargetID: tgt, Relation: "depends on", Weight: 0.5},
	}
	out := dedupGraphRelationships(rels)

	// "related_to" + "related to" collapse to one canonical edge; "depends on"
	// stays. So 2 edges total.
	if len(out) != 2 {
		t.Fatalf("expected 2 edges after dedup, got %d: %+v", len(out), out)
	}
	var merged *graphRelationship
	for i := range out {
		if out[i].Relation == "related to" {
			merged = &out[i]
		}
	}
	if merged == nil {
		t.Fatal("expected a canonical 'related to' edge")
	}
	if merged.Weight != 0.7 {
		t.Errorf("merged weight = %v, want max 0.7", merged.Weight)
	}
	if merged.SourceMemory == nil || *merged.SourceMemory != mem {
		t.Errorf("merged edge should carry the max-weight row's SourceMemory")
	}
}

// TestRankGraphSlice_SeedIsHopZero pins the core fix: a high-mention non-seed
// node never outranks a low-mention seed across hop boundaries. Chain S-A-B
// with seed S; B has the highest mention count but is hop 2, so it sinks.
func TestRankGraphSlice_SeedIsHopZero(t *testing.T) {
	s, a, b := uuid.New(), uuid.New(), uuid.New()
	entities := []graphEntity{ent(b, "B", 500), ent(a, "A", 100), ent(s, "S", 1)}
	rels := []graphRelationship{relAt(s, a, "x", 1), relAt(a, b, "y", 1)}

	rankGraphSlice(seedSet(s), entities, rels)

	if entities[0].ID != s {
		t.Errorf("seed S should rank first, got %s (mention=%d)", entities[0].Name, entities[0].MentionCount)
	}
	if entities[2].ID != b {
		t.Errorf("hop-2 hub B should rank last despite mention=500, got %s", entities[2].Name)
	}
}

// TestRankGraphSlice_AvsB proves the slice tracks the seed: the same edge set
// ranked from two different seeds yields different prefixes.
func TestRankGraphSlice_AvsB(t *testing.T) {
	x, s, y, z := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	baseEnts := []graphEntity{ent(x, "X", 10), ent(s, "S", 10), ent(y, "Y", 10), ent(z, "Z", 10)}
	baseRels := []graphRelationship{relAt(x, s, "r", 1), relAt(s, y, "r", 1), relAt(y, z, "r", 1)}

	entsA := cloneEnts(baseEnts)
	rankGraphSlice(seedSet(x), entsA, cloneRels(baseRels))
	if entsA[0].ID != x {
		t.Errorf("seed X should rank first under seed A, got %s", entsA[0].Name)
	}

	entsB := cloneEnts(baseEnts)
	rankGraphSlice(seedSet(z), entsB, cloneRels(baseRels))
	if entsB[0].ID != z {
		t.Errorf("seed Z should rank first under seed B, got %s", entsB[0].Name)
	}

	if entsA[0].ID == entsB[0].ID {
		t.Error("expected different prefixes for different seeds")
	}
}

// TestRankGraphSlice_WithinTierRoundRobin verifies one source node's fan-out
// cannot monopolize the front of its hop tier. Two seeds A and B (hop 0); A has
// 4 equal-weight outgoing edges, B has 2. All edges are tier 0. Round-robin must
// interleave them, so the first 4 emitted edges contain 2 from each source
// rather than all 4 of A's first.
func TestRankGraphSlice_WithinTierRoundRobin(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	var rels []graphRelationship
	for range 4 {
		rels = append(rels, relAt(a, uuid.New(), "rel", 1))
	}
	for range 2 {
		rels = append(rels, relAt(b, uuid.New(), "rel", 1))
	}
	entities := []graphEntity{ent(a, "A", 1), ent(b, "B", 1)}

	rankGraphSlice(seedSet(a, b), entities, rels)

	countA, countB := 0, 0
	for _, r := range rels[:4] {
		switch r.SourceID {
		case a:
			countA++
		case b:
			countB++
		}
	}
	if countA != 2 || countB != 2 {
		t.Errorf("expected round-robin interleave (2 from each source in first 4), got A=%d B=%d", countA, countB)
	}
}

// TestRankGraphSlice_PrefixIsProximal confirms the prefix-trim invariant: after
// ranking, no edge in a higher hop tier precedes an edge in a lower tier, so any
// prefix the byte-budget trimmer takes stays proximity-prioritized.
func TestRankGraphSlice_PrefixIsProximal(t *testing.T) {
	s, a, b, c := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	entities := []graphEntity{ent(s, "S", 1), ent(a, "A", 1), ent(b, "B", 1), ent(c, "C", 1)}
	// tiers: S-A (0), A-B (1), B-C (2)
	rels := []graphRelationship{relAt(b, c, "r", 1), relAt(a, b, "r", 1), relAt(s, a, "r", 1)}

	info := computeGraphHops(seedSet(s), rels)
	rankGraphSlice(seedSet(s), entities, rels)

	prev := -1
	for _, r := range rels {
		tt := info.tierOf(r)
		if tt < prev {
			t.Errorf("tier %d edge followed a tier %d edge — prefix not proximity-ordered", tt, prev)
		}
		prev = tt
	}
}

// TestRankGraphSlice_Deterministic pins map-iteration safety: identical input
// yields byte-identical order on both axes across repeated runs, even with many
// Weight=1.0 ties.
func TestRankGraphSlice_Deterministic(t *testing.T) {
	s1, s2 := uuid.New(), uuid.New()
	nodes := make([]uuid.UUID, 8)
	for i := range nodes {
		nodes[i] = uuid.New()
	}
	baseEnts := []graphEntity{ent(s1, "S1", 5), ent(s2, "S2", 5)}
	for i, n := range nodes {
		baseEnts = append(baseEnts, ent(n, fmt.Sprintf("N%d", i), 3))
	}
	var baseRels []graphRelationship
	for _, n := range nodes {
		baseRels = append(baseRels, relAt(s1, n, "rel", 1))
		baseRels = append(baseRels, relAt(s2, n, "rel", 1))
	}

	sig := func(ents []graphEntity, rels []graphRelationship) string {
		out := ""
		for _, e := range ents {
			out += e.ID.String() + ";"
		}
		out += "|"
		for _, r := range rels {
			out += r.SourceID.String() + "->" + r.TargetID.String() + ";"
		}
		return out
	}

	var first string
	for i := range 8 {
		ents := cloneEnts(baseEnts)
		rels := cloneRels(baseRels)
		rankGraphSlice(seedSet(s1, s2), ents, rels)
		got := sig(ents, rels)
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("non-deterministic ranking on run %d", i)
		}
	}
}

// TestRankGraphSlice_EmptyAndSingleSeed ensures degenerate seed sets do not
// panic and produce a stable order (graceful fallback for recall with no
// query-matched entities).
func TestRankGraphSlice_EmptyAndSingleSeed(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	entities := []graphEntity{ent(a, "A", 2), ent(b, "B", 5)}
	rels := []graphRelationship{relAt(a, b, "r", 1)}

	// Empty seed set: everything unreachable, one tier, no panic.
	rankGraphSlice(seedSet(), cloneEnts(entities), cloneRels(rels))

	// Single seed: standard hop rings.
	ents := cloneEnts(entities)
	rankGraphSlice(seedSet(a), ents, cloneRels(rels))
	if ents[0].ID != a {
		t.Errorf("single seed A should rank first, got %s", ents[0].Name)
	}
}
