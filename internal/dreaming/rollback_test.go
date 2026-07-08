package dreaming

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestReverseOperation_EntityDeletedRestoresEntity locks in the rollback
// handler for DreamOpEntityDeleted: it restores the deleted entity from
// before_state via Upsert and does not fall through to the "unknown rollback
// operation" default. This covers both the entity-dedup merge delete and the
// hygiene sweep, which both log DreamOpEntityDeleted with before=full entity.
func TestReverseOperation_EntityDeletedRestoresEntity(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()

	ent := model.Entity{ID: uuid.New(), NamespaceID: ns, Name: "Absorbed", MentionCount: 4, EntityType: "person"}
	before, err := json.Marshal(ent)
	if err != nil {
		t.Fatalf("marshal entity: %v", err)
	}

	w := &recordingEntityWriter{}
	s := &RollbackService{entityWriter: w}
	entry := &model.DreamLog{
		Operation:   model.DreamOpEntityDeleted,
		TargetType:  "entity",
		TargetID:    ent.ID,
		BeforeState: before,
	}

	if err := s.reverseOperation(ctx, ns, entry); err != nil {
		t.Fatalf("reverseOperation(entity_deleted): %v", err)
	}

	if len(w.upserted) != 1 {
		t.Fatalf("expected 1 upsert to restore deleted entity, got %d", len(w.upserted))
	}
	got := w.upserted[0]
	if got.ID != ent.ID || got.Name != "Absorbed" || got.MentionCount != 4 {
		t.Fatalf("restored entity mismatch: %+v", got)
	}
}
