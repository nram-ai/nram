package service

import (
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

// TestEntitySystemPromptListsClosedVocab is the drift guard between the entity
// extraction prompt's enumerated vocabulary and the authoritative closed sets in
// the model package. The prompt instructs the model to pick from these lists; if
// a type or relation is added to the model vocabulary without updating the
// prompt, the model is never told it can use it.
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
	for _, rel := range model.CanonicalRelations {
		if rel == model.RelationRelatedTo {
			continue // "related_to" is the escape hatch
		}
		if !strings.Contains(prompt, rel) {
			t.Errorf("entity system prompt is missing relation %q from the closed vocabulary", rel)
		}
	}
}
