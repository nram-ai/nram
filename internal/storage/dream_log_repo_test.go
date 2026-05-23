package storage

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestDreamLogRepo_RoundtripsSubPhase verifies the sub_phase column added in
// migration 000037 (sqlite) / 000034 (postgres) round-trips through Create
// and ListByCycle for both populated and empty values. Legacy rows persisted
// before the column existed scan back as empty string.
func TestDreamLogRepo_RoundtripsSubPhase(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		project, _ := createTestProject(t, ctx, db, "dl-subphase-"+uuid.New().String()[:8])
		cycleRepo := NewDreamCycleRepo(db)
		repo := NewDreamLogRepo(db)

		cycle := createRunningCycle(t, ctx, cycleRepo, project.ID, project.NamespaceID, 0)

		populated := &model.DreamLog{
			CycleID:    cycle.ID,
			ProjectID:  project.ID,
			Phase:      model.DreamPhaseConsolidation,
			SubPhase:   model.DreamSubPhaseReinforce,
			Operation:  model.DreamOpConfidenceAdjusted,
			TargetType: "memory",
			TargetID:   uuid.New(),
		}
		if err := repo.Create(ctx, populated); err != nil {
			t.Fatalf("create populated: %v", err)
		}

		empty := &model.DreamLog{
			CycleID:    cycle.ID,
			ProjectID:  project.ID,
			Phase:      model.DreamPhasePruning,
			SubPhase:   "",
			Operation:  model.DreamOpMemoryDeleted,
			TargetType: "memory",
			TargetID:   uuid.New(),
		}
		if err := repo.Create(ctx, empty); err != nil {
			t.Fatalf("create empty: %v", err)
		}

		out, err := repo.ListByCycle(ctx, cycle.ID)
		if err != nil {
			t.Fatalf("list by cycle: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("expected 2 logs, got %d", len(out))
		}

		var gotPopulated, gotEmpty *model.DreamLog
		for i := range out {
			switch out[i].Operation {
			case model.DreamOpConfidenceAdjusted:
				gotPopulated = &out[i]
			case model.DreamOpMemoryDeleted:
				gotEmpty = &out[i]
			}
		}
		if gotPopulated == nil || gotEmpty == nil {
			t.Fatalf("expected both ops in result, got %+v", out)
		}
		if gotPopulated.SubPhase != model.DreamSubPhaseReinforce {
			t.Errorf("populated sub_phase = %q, want %q",
				gotPopulated.SubPhase, model.DreamSubPhaseReinforce)
		}
		if gotEmpty.SubPhase != "" {
			t.Errorf("empty sub_phase decoded as %q, want \"\"", gotEmpty.SubPhase)
		}
	})
}
