package service

import (
	"strconv"
	"strings"
	"testing"
)

func TestIsDegenerateEntityName(t *testing.T) {
	const (
		maxChars = 120
		maxWords = 12
		minRatio = 0.5
	)
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		// Legitimate names must pass.
		{"person", "Brandon Lehmann", false},
		{"single token", "nram", false},
		{"file with extension", "VelocityLogo_NoTagline_ColorOnWhite.svg", false},
		{"model id", "qwen3:8b-extract", false},
		{"two words", "Active Directory", false},
		{"real plan title", "Active Directory User Sync Modernization Plan", false},
		{"city", "New York City", false},
		{"dotted hostname", "test.example.com", false},
		{"ratio exactly at floor", "New York New York", false}, // distinct/total == 0.5, not < 0.5

		// Degenerate names must be rejected. Inputs mirror the deleted corpus.
		{"empty", "", true},
		{"whitespace only", "   ", true},
		{"over char cap", strings.Repeat("a", 121), true},
		{"sentence as name (15 words)", "Charge Assignment Page with Bundling Support Phase 4e Conversion Plan 004e Impact on Conversion Plans", true},
		{"word repetition loop (long)", strings.Repeat("undercutting transparency credibility ", 5), true},
		{"word repetition loop under cap", "spam spam spam spam spam spam", true},
		{"char repetition loop under cap", strings.Repeat(".svg", 20), true}, // 80 chars, one "word"
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsDegenerateEntityName(tc.input, maxChars, maxWords, minRatio); got != tc.want {
				t.Errorf("IsDegenerateEntityName(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestIsDegenerateEntityName_ZeroThresholdsDisableChecks(t *testing.T) {
	long := strings.Repeat("a", 500)
	if IsDegenerateEntityName(long, 0, 0, 0) {
		t.Error("zero thresholds should disable the length, word, and repetition checks")
	}
	// An empty name is degenerate regardless of thresholds.
	if !IsDegenerateEntityName("   ", 0, 0, 0) {
		t.Error("an empty/whitespace name must always be degenerate")
	}
}

func TestScrubEntityResult(t *testing.T) {
	garbage := strings.Repeat("loop ", 40) // 200 chars, over the cap
	res := &EntityExtractionResult{
		Entities: []ExtractedEntityData{
			{Name: "Brandon Lehmann", Type: "person"},
			{Name: garbage, Type: "concept"},
		},
		Relationships: []ExtractedRelation{
			{Source: "Brandon Lehmann", Target: "nram", Relation: "created"},
			{Source: "Brandon Lehmann", Target: garbage, Relation: "mentions"},
		},
	}

	scrubEntityResult(res, 120, 12, 0.5)

	if len(res.Entities) != 1 || res.Entities[0].Name != "Brandon Lehmann" {
		t.Fatalf("entities not scrubbed to the one valid entity: %+v", res.Entities)
	}
	if len(res.Relationships) != 1 || res.Relationships[0].Target != "nram" {
		t.Fatalf("relationship with a degenerate endpoint not dropped: %+v", res.Relationships)
	}
}

// A result whose entities are all degenerate scrubs to empty. This is what makes
// a continuation pass contribute zero new entities, so extractEntitiesWithContinuation's
// added==0 check breaks the loop instead of feeding a looping model back to itself.
func TestScrubEntityResult_AllDegenerateBecomesEmpty(t *testing.T) {
	res := &EntityExtractionResult{
		Entities: []ExtractedEntityData{
			{Name: strings.Repeat("x", 200), Type: "concept"},
			{Name: strings.Repeat(".svg", 30), Type: "file"},
		},
	}
	scrubEntityResult(res, 120, 12, 0.5)
	if len(res.Entities) != 0 {
		t.Fatalf("expected all degenerate entities dropped, got %+v", res.Entities)
	}
}

func TestCapEntityResult_TruncatesAndPrunesRelationships(t *testing.T) {
	const cap = 128
	res := &EntityExtractionResult{}
	for i := range 300 {
		res.Entities = append(res.Entities, ExtractedEntityData{
			Name: "entity-" + strconv.Itoa(i), Type: "concept",
		})
	}
	// One edge wholly inside the kept prefix, one that reaches a dropped entity.
	res.Relationships = []ExtractedRelation{
		{Source: "entity-0", Target: "entity-1", Relation: "rel"},   // both kept
		{Source: "entity-0", Target: "entity-200", Relation: "rel"}, // target dropped
	}

	dropped := CapEntityResult(res, cap)

	if dropped != 300-cap {
		t.Fatalf("dropped = %d, want %d", dropped, 300-cap)
	}
	if len(res.Entities) != cap {
		t.Fatalf("entities = %d, want %d", len(res.Entities), cap)
	}
	if len(res.Relationships) != 1 || res.Relationships[0].Target != "entity-1" {
		t.Fatalf("relationship reaching a dropped entity not pruned: %+v", res.Relationships)
	}
}

// Under the cap the clamp is a no-op: entities are untouched and relationships to
// endpoints that are not in the entity slice survive, preserving the legitimate
// stub-creation path the worker relies on when the cap does not fire.
func TestCapEntityResult_UnderCapIsNoOp(t *testing.T) {
	res := &EntityExtractionResult{
		Entities: []ExtractedEntityData{
			{Name: "Brandon Lehmann", Type: "person"},
			{Name: "nram", Type: "project"},
		},
		Relationships: []ExtractedRelation{
			// Target is not among the entities: a legitimate stub endpoint.
			{Source: "Brandon Lehmann", Target: "Anthropic", Relation: "uses"},
		},
	}

	if dropped := CapEntityResult(res, 128); dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if len(res.Entities) != 2 {
		t.Fatalf("entities mutated under the cap: %+v", res.Entities)
	}
	if len(res.Relationships) != 1 || res.Relationships[0].Target != "Anthropic" {
		t.Fatalf("stub-endpoint relationship dropped under the cap: %+v", res.Relationships)
	}
}

func TestCapEntityResult_ZeroDisables(t *testing.T) {
	res := &EntityExtractionResult{}
	for i := range 300 {
		res.Entities = append(res.Entities, ExtractedEntityData{Name: "entity-" + strconv.Itoa(i)})
	}
	if dropped := CapEntityResult(res, 0); dropped != 0 {
		t.Fatalf("dropped = %d, want 0 (cap disabled)", dropped)
	}
	if len(res.Entities) != 300 {
		t.Fatalf("entities truncated with the cap disabled: %d", len(res.Entities))
	}
}
