package service

import (
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

// TestRelationGlossesCoverVocabulary is the drift guard between the embedding
// classifier's reference glosses and the closed relation vocabulary. The 16
// canonical relations live in four places (model.CanonicalRelations, the
// relationAliases keys, the prompt, and relationGlosses); a relation added
// without a gloss has no reference embedding, so the classifier can never select
// it and its long tail silently collapses to "related to" with no other signal.
func TestRelationGlossesCoverVocabulary(t *testing.T) {
	want := map[string]bool{}
	for _, r := range model.CanonicalRelations {
		if r != model.RelationRelatedTo {
			want[r] = true
		}
	}
	for r := range relationGlosses {
		if !want[r] {
			t.Errorf("relationGlosses has %q which is not a canonical relation (or is the escape hatch)", r)
		}
		delete(want, r)
	}
	for r := range want {
		t.Errorf("canonical relation %q has no entry in relationGlosses", r)
	}
}

func TestEntityTypeGlossesCoverVocabulary(t *testing.T) {
	want := map[string]bool{}
	for _, et := range model.CanonicalEntityTypes {
		if et != model.EntityTypeOther {
			want[et] = true
		}
	}
	for et := range entityTypeGlosses {
		if !want[et] {
			t.Errorf("entityTypeGlosses has %q which is not a canonical entity type (or is the escape hatch)", et)
		}
		delete(want, et)
	}
	for et := range want {
		t.Errorf("canonical entity type %q has no entry in entityTypeGlosses", et)
	}
}
