package enrichment

import (
	"testing"

	"github.com/nram-ai/nram/internal/service"
)

func TestDedupeEntityResult_CollapsesByCanonical(t *testing.T) {
	res := &service.EntityExtractionResult{
		Entities: []service.ExtractedEntityData{
			{Name: "Alice", Type: "person"},
			{Name: "  alice ", Type: "PERSON"},    // canonical dup of the first
			{Name: "Alice", Type: "organization"}, // distinct: different type
			{Name: "Acme", Type: "organization"},
			{Name: "", Type: "person"}, // empty name dropped
		},
	}
	dedupeEntityResult(res)
	if len(res.Entities) != 3 {
		t.Fatalf("expected 3 distinct entities (alice/person, alice/org, acme/org), got %d: %+v", len(res.Entities), res.Entities)
	}
	// First occurrence wins (original casing preserved).
	if res.Entities[0].Name != "Alice" {
		t.Errorf("expected first occurrence kept, got %q", res.Entities[0].Name)
	}
}

func TestDedupeRelationResult_CollapsesTriplesAndDropsSelfLoops(t *testing.T) {
	res := &service.EntityExtractionResult{
		Relationships: []service.ExtractedRelation{
			{Source: "Alice", Target: "Acme", Relation: "member of"},
			{Source: " alice ", Target: "ACME", Relation: "member_of"}, // canonical dup
			{Source: "Alice", Target: "Acme", Relation: "uses"},        // distinct relation
			{Source: "Acme", Target: "Acme", Relation: "uses"},         // self-loop dropped
			{Source: "Bob", Target: "", Relation: "uses"},              // empty endpoint dropped
		},
	}
	dedupeRelationResult(res)
	if len(res.Relationships) != 2 {
		t.Fatalf("expected 2 distinct non-self relationships, got %d: %+v", len(res.Relationships), res.Relationships)
	}
}

func TestBuildPartialRecoveryWarning_GatesOnTruncation(t *testing.T) {
	// A clean stop (even with PartialRecovery set, e.g. recovery ran on a
	// well-formed stop) must NOT be flagged; only a length/max_tokens finish is.
	entStop := &service.EntityExtractionEnvelope{
		PartialRecovery: true,
		FinishReason:    "stop",
		Result:          &service.EntityExtractionResult{Entities: make([]service.ExtractedEntityData, 5)},
	}
	if w := buildPartialRecoveryWarning(nil, entStop, nil); w != nil {
		t.Errorf("finish=stop must not be flagged partial, got %v", w)
	}

	entLen := &service.EntityExtractionEnvelope{
		PartialRecovery: true,
		FinishReason:    "length",
		Result:          &service.EntityExtractionResult{Entities: make([]service.ExtractedEntityData, 7)},
	}
	w := buildPartialRecoveryWarning(nil, entLen, nil)
	warnMap, ok := w.(map[string]any)
	if !ok {
		t.Fatalf("finish=length must produce a warning payload, got %T", w)
	}
	legs, ok := warnMap["warnings"].([]partialRecoveryLeg)
	if !ok || len(legs) != 1 {
		t.Fatalf("expected 1 entity leg, got %v", warnMap["warnings"])
	}
	if legs[0].Phase != service.ExtractionPhaseEntity || legs[0].EntitiesRec != 7 {
		t.Errorf("entity leg wrong: %+v", legs[0])
	}
	// Entity leg reports entities only (relationships now have their own leg).
	if legs[0].RelationsRec != 0 {
		t.Errorf("entity leg must not report relationships, got %d", legs[0].RelationsRec)
	}
}

func TestBuildPartialRecoveryWarning_RelationshipLeg(t *testing.T) {
	entLen := &service.EntityExtractionEnvelope{
		PartialRecovery: true,
		FinishReason:    "length",
		Result: &service.EntityExtractionResult{
			Entities:      make([]service.ExtractedEntityData, 3),
			Relationships: make([]service.ExtractedRelation, 4), // deduped final count
		},
	}
	relLen := &service.RelationExtractionEnvelope{
		PartialRecovery: true,
		FinishReason:    "length",
		Result:          &service.RelationExtractionResult{Relationships: make([]service.ExtractedRelation, 99)}, // pre-dedup; must NOT be used
	}
	w := buildPartialRecoveryWarning(nil, entLen, relLen)
	legs := w.(map[string]any)["warnings"].([]partialRecoveryLeg)
	if len(legs) != 2 {
		t.Fatalf("expected entity + relationship legs, got %d", len(legs))
	}
	var relLeg *partialRecoveryLeg
	for i := range legs {
		if legs[i].Phase == service.ExtractionPhaseRelationship {
			relLeg = &legs[i]
		}
	}
	if relLeg == nil {
		t.Fatal("no relationship leg emitted")
	}
	// Relationship leg reports the deduped count from entEnv.Result (4), not the
	// pre-dedup relEnv slice length (99).
	if relLeg.RelationsRec != 4 {
		t.Errorf("relationship leg should report deduped count 4, got %d", relLeg.RelationsRec)
	}
}
