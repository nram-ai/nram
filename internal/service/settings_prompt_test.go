package service

import (
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

// TestEntitySystemPromptListsClosedVocab is the drift guard between the entity
// extraction prompt's enumerated entity-type vocabulary and the authoritative
// closed set in the model package. Entity extraction is pass 1 (entity-only), so
// only the entity types live here; the relation vocabulary moved to the
// relationship prompt (see TestRelationshipSystemPromptListsClosedVocab).
func TestEntitySystemPromptListsClosedVocab(t *testing.T) {
	prompt := entitySystemPromptText
	for _, et := range model.CanonicalEntityTypes {
		if et == model.EntityTypeOther {
			continue // "other" is described as the escape hatch, not listed inline
		}
		if !strings.Contains(prompt, et) {
			t.Errorf("entity system prompt is missing entity type %q from the closed vocabulary", et)
		}
	}
}

// TestRelationshipSystemPromptListsClosedVocab is the drift guard between the
// relationship extraction prompt's enumerated relation vocabulary and the
// authoritative closed set in the model package. Relationship extraction is
// pass 2; if a relation is added to the model vocabulary without updating this
// prompt, the model is never told it can use it.
func TestRelationshipSystemPromptListsClosedVocab(t *testing.T) {
	prompt := relationshipSystemPromptText
	for _, rel := range model.CanonicalRelations {
		if rel == model.RelationRelatedTo {
			continue // "related_to" is the escape hatch
		}
		if !strings.Contains(prompt, rel) {
			t.Errorf("relationship system prompt is missing relation %q from the closed vocabulary", rel)
		}
	}
}
