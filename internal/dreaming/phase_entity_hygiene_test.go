package dreaming

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// recordingEntityWriter satisfies EntityWriter and records the IDs passed to
// DeleteByIDs and the entities passed to Upsert so callers can assert the
// hygiene sweep's deletes, a merge's inline delete, or a rollback's restore.
type recordingEntityWriter struct {
	deleted  []uuid.UUID
	upserted []*model.Entity
}

func (w *recordingEntityWriter) Upsert(_ context.Context, e *model.Entity) error {
	w.upserted = append(w.upserted, e)
	return nil
}
func (w *recordingEntityWriter) DeleteByIDs(_ context.Context, ids []uuid.UUID) ([]uuid.UUID, error) {
	w.deleted = append(w.deleted, ids...)
	return ids, nil
}

func TestEntityHygieneSweep(t *testing.T) {
	ctx := context.Background()
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: uuid.New()}

	good := model.Entity{ID: uuid.New(), Name: "Brandon Lehmann", EntityType: "person"}
	loop := model.Entity{ID: uuid.New(), Name: strings.Repeat("undercutting ", 30), EntityType: "concept"}
	sentence := model.Entity{
		ID:         uuid.New(),
		Name:       "Charge Assignment Page with Bundling Support Phase 4e Conversion Plan 004e Impact on Conversion Plans",
		EntityType: "event",
	}
	input := []model.Entity{good, loop, sentence}

	t.Run("enabled deletes degenerate names and keeps valid ones", func(t *testing.T) {
		w := &recordingEntityWriter{}
		p := &EntityDedupPhase{entityWriter: w, settings: &staticDreamSettings{}}
		logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

		got := p.sweepDegenerateNames(ctx, cycle, input, logger)

		if len(got) != 1 || got[0].ID != good.ID {
			t.Fatalf("survivors = %+v, want only %q", got, good.Name)
		}
		if len(w.deleted) != 2 {
			t.Fatalf("deleted %d entities, want 2", len(w.deleted))
		}
		deleted := map[uuid.UUID]bool{w.deleted[0]: true, w.deleted[1]: true}
		if !deleted[loop.ID] || !deleted[sentence.ID] {
			t.Fatalf("expected both degenerate entities deleted, got %v", w.deleted)
		}
	})

	t.Run("disabled retains every entity", func(t *testing.T) {
		w := &recordingEntityWriter{}
		settings := &staticDreamSettings{values: map[string]string{
			service.SettingDreamEntityHygieneEnabled: "false",
		}}
		p := &EntityDedupPhase{entityWriter: w, settings: settings}
		logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

		got := p.sweepDegenerateNames(ctx, cycle, input, logger)

		if len(got) != len(input) {
			t.Fatalf("survivors = %d, want %d (sweep disabled)", len(got), len(input))
		}
		if len(w.deleted) != 0 {
			t.Fatalf("deleted %d entities with sweep disabled, want 0", len(w.deleted))
		}
	})
}
