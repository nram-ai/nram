package storage

import (
	"testing"
)

// TestVectorUpsertItem_EffectiveKind covers the back-compat default: an item
// constructed without setting Kind must route as a memory vector so legacy
// call sites (and any deserialized JSON missing the field) keep working.
func TestVectorUpsertItem_EffectiveKind(t *testing.T) {
	tests := []struct {
		name string
		in   VectorUpsertItem
		want VectorKind
	}{
		{"zero value defaults to memory", VectorUpsertItem{}, VectorKindMemory},
		{"explicit memory", VectorUpsertItem{Kind: VectorKindMemory}, VectorKindMemory},
		{"explicit entity", VectorUpsertItem{Kind: VectorKindEntity}, VectorKindEntity},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.EffectiveKind(); got != tt.want {
				t.Errorf("EffectiveKind() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestResolveTableSpec_RoutesByKind covers the pgvector kind→table routing
// that A3 introduces. Memory and entity kinds must land on disjoint table
// families with the correct id column for namespace JOINs.
func TestResolveTableSpec_RoutesByKind(t *testing.T) {
	cases := []struct {
		name       string
		kind       VectorKind
		dim        int
		wantTable  string
		wantParent string
		wantIDCol  string
		wantErr    bool
	}{
		{"memory 768", VectorKindMemory, 768, "memory_vectors_768", "memories", "memory_id", false},
		{"memory 1536", VectorKindMemory, 1536, "memory_vectors_1536", "memories", "memory_id", false},
		{"entity 768", VectorKindEntity, 768, "entity_vectors_768", "entities", "entity_id", false},
		{"entity 3072", VectorKindEntity, 3072, "entity_vectors_3072", "entities", "entity_id", false},
		{"empty kind defaults memory", "", 384, "memory_vectors_384", "memories", "memory_id", false},
		{"unknown dim memory", VectorKindMemory, 100, "", "", "", true},
		{"unknown dim entity", VectorKindEntity, 100, "", "", "", true},
		{"unknown kind", VectorKind("bogus"), 768, "", "", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := resolveTableSpec(tc.kind, tc.dim)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveTableSpec(%q, %d) expected error, got spec=%+v", tc.kind, tc.dim, spec)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTableSpec(%q, %d) unexpected error: %v", tc.kind, tc.dim, err)
			}
			if spec.table != tc.wantTable {
				t.Errorf("table = %q, want %q", spec.table, tc.wantTable)
			}
			if spec.parent != tc.wantParent {
				t.Errorf("parent = %q, want %q", spec.parent, tc.wantParent)
			}
			if spec.idColumn != tc.wantIDCol {
				t.Errorf("idColumn = %q, want %q", spec.idColumn, tc.wantIDCol)
			}
		})
	}
}
