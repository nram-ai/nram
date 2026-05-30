package model

import "testing"

func TestCanonicalRelation(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// Pure formatting variants that MUST merge to one canonical form.
		{"snake", "related_to", "related to"},
		{"space", "related to", "related to"},
		{"hyphen", "related-to", "related to"},
		{"mixed_case_double_space", "Related  To", "related to"},
		{"upper_snake", "RELATED_TO", "related to"},
		{"could_expose_snake", "could_expose", "could expose"},
		{"could_expose_space", "could expose", "could expose"},
		// Leading/trailing separators must not survive.
		{"leading_underscore", "_related", "related"},
		{"trailing_underscore", "related_", "related"},
		{"surrounding_ws", "  related to  ", "related to"},
		{"leading_run", "__--related", "related"},
		{"trailing_run", "related--__", "related"},
		// Tabs / newlines collapse like any whitespace.
		{"tab", "related\tto", "related to"},
		{"newline", "related\nto", "related to"},
		{"mixed_separators", "maps_to architecture", "maps to architecture"},
		// Semantic differences are preserved (NOT merged): trailing token differs.
		{"semantic_keep_a", "maps_to_architecture", "maps to architecture"},
		{"semantic_keep_b", "maps to architecture of", "maps to architecture of"},
		// Degenerate inputs.
		{"empty", "", ""},
		{"only_ws", "   ", ""},
		{"only_separators", "__-- ", ""},
		{"single_word", "supports", "supports"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalRelation(tt.in)
			if got != tt.want {
				t.Errorf("CanonicalRelation(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestCanonicalRelation_SemanticPairsStayDistinct pins the out-of-scope
// guarantee: variants that differ by a trailing token are NOT merged.
func TestCanonicalRelation_SemanticPairsStayDistinct(t *testing.T) {
	a := CanonicalRelation("maps_to_architecture")
	b := CanonicalRelation("maps to architecture of")
	if a == b {
		t.Errorf("expected distinct canonical forms, both became %q", a)
	}
}

// TestCanonicalRelation_Idempotent confirms a second pass is a no-op, which the
// repo upsert and the backfill rely on (a canonical row never re-canonicalizes
// to something else).
func TestCanonicalRelation_Idempotent(t *testing.T) {
	for _, in := range []string{
		"related_to", "Related  To", "_could-expose_", "maps to architecture of",
		"", "   ", "supports", "RELATED\tTO",
	} {
		once := CanonicalRelation(in)
		twice := CanonicalRelation(once)
		if once != twice {
			t.Errorf("not idempotent for %q: once=%q twice=%q", in, once, twice)
		}
	}
}
