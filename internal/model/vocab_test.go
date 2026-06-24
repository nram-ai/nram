package model

import "testing"

func TestCanonicalEntityType(t *testing.T) {
	cases := map[string]string{
		"person":              "person",
		"Person":              "person",
		"database_table":      "data_store",
		"database table":      "data_store",
		"Database  Table":     "data_store",
		"database_technology": "data_store",
		"code_file":           "file",
		"IP address":          "identifier",
		"email":               "identifier",
		"drug":                "medication",
		"disease":             "medical_condition",
		"unknown":             "unknown", // stub sentinel preserved
		"":                    "other",
		"some-bogus-type":     "other",
		"temp,":               "other",
	}
	for in, want := range cases {
		if got := CanonicalEntityType(in); got != want {
			t.Errorf("CanonicalEntityType(%q) = %q, want %q", in, got, want)
		}
	}
	// Idempotence over the whole enum.
	for _, c := range CanonicalEntityTypes {
		if got := CanonicalEntityType(c); got != c {
			t.Errorf("CanonicalEntityType(%q) not idempotent: %q", c, got)
		}
	}
}

func TestCanonicalRelationVocab(t *testing.T) {
	cases := map[string]string{
		"related to":      "related to",
		"associated with": "related to",
		"uses":            "uses",
		"used in":         "uses",
		"belongs to":      "part of",
		"is part of":      "part of",
		"contains":        "has part",
		"married to":      "family of",
		"mother of":       "family of",
		"works at":        "member of",
		"employed_by":     "member of",
		"some weird verb": "related to", // unmapped -> escape hatch
		"":                "related to",
	}
	for in, want := range cases {
		if got := CanonicalRelationVocab(in); got != want {
			t.Errorf("CanonicalRelationVocab(%q) = %q, want %q", in, got, want)
		}
	}
	for _, c := range CanonicalRelations {
		if got := CanonicalRelationVocab(c); got != c {
			t.Errorf("CanonicalRelationVocab(%q) not idempotent: %q", c, got)
		}
	}
}

// TestRelationAliasesNoConflict guards against an alias being claimed by two
// canonical relations (map iteration order would make the winner
// nondeterministic, e.g. the historical "is" in both is_a and has_property).
func TestRelationAliasesNoConflict(t *testing.T) {
	seen := map[string]string{}
	for canonical, aliases := range relationAliases {
		for _, a := range aliases {
			n := normalizeLabel(a)
			if prev, ok := seen[n]; ok && prev != canonical {
				t.Errorf("alias %q maps to both %q and %q", a, prev, canonical)
			}
			seen[n] = canonical
		}
	}
}

func TestEntityTypeAliasesNoConflict(t *testing.T) {
	seen := map[string]string{}
	for canonical, aliases := range entityTypeAliases {
		for _, a := range aliases {
			n := normalizeLabel(a)
			if prev, ok := seen[n]; ok && prev != canonical {
				t.Errorf("entity-type alias %q maps to both %q and %q", a, prev, canonical)
			}
			seen[n] = canonical
		}
	}
}

func TestRelationKind(t *testing.T) {
	cases := map[string]string{
		"married to": "spouse",
		"wife of":    "spouse",
		"mother of":  "parent",
		"father of":  "parent",
		"son of":     "child",
		"sister of":  "sibling",
		"uses":       "",
		"related to": "",
	}
	for in, want := range cases {
		if got := RelationKind(in); got != want {
			t.Errorf("RelationKind(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestApplyRelationVocab_StampsKinship(t *testing.T) {
	rel := &Relationship{Relation: "mother of"}
	ApplyRelationVocab(rel)
	if rel.Relation != "family of" {
		t.Fatalf("relation = %q, want family of", rel.Relation)
	}
	if got := string(rel.Properties); got == "" ||
		!contains(got, `"kind"`) || !contains(got, `"parent"`) {
		t.Fatalf("properties = %q, want kind=parent stamped", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
