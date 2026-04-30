package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// setCycleUpdatedAt rewrites updated_at directly so tests can produce stale
// rows without sleeping. Heartbeat_at is also writeable here for the few
// tests that need to disentangle the two timestamps.
func setCycleTimestamps(t *testing.T, ctx context.Context, db DB, id uuid.UUID, updatedAt time.Time, heartbeatAt *time.Time) {
	t.Helper()

	var hb interface{}
	if heartbeatAt != nil {
		hb = heartbeatAt.UTC().Format(time.RFC3339)
	}

	query := `UPDATE dream_cycles SET updated_at = ?, heartbeat_at = ? WHERE id = ?`
	if db.Backend() == BackendPostgres {
		query = `UPDATE dream_cycles SET updated_at = $1, heartbeat_at = $2 WHERE id = $3`
	}
	if _, err := db.Exec(ctx, query, updatedAt.UTC().Format(time.RFC3339), hb, id.String()); err != nil {
		t.Fatalf("set cycle timestamps: %v", err)
	}
}

// createRunningCycle inserts a dream_cycles row at status='running' (Start
// transitions pending → running, populating started_at and updated_at).
func createRunningCycle(t *testing.T, ctx context.Context, repo *DreamCycleRepo, projectID, namespaceID uuid.UUID, tokensUsed int) *model.DreamCycle {
	t.Helper()
	cycle := &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   projectID,
		NamespaceID: namespaceID,
		TokenBudget: 10_000,
		TokensUsed:  tokensUsed,
	}
	if err := repo.Create(ctx, cycle); err != nil {
		t.Fatalf("create cycle: %v", err)
	}
	if err := repo.Start(ctx, cycle.ID); err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	if tokensUsed > 0 {
		if err := repo.UpdateStatus(ctx, cycle.ID, model.DreamStatusRunning, "entity_dedup", tokensUsed); err != nil {
			t.Fatalf("seed tokens_used: %v", err)
		}
	}
	out, err := repo.GetByID(ctx, cycle.ID)
	if err != nil {
		t.Fatalf("re-read cycle: %v", err)
	}
	return out
}

func TestDreamCycleRepo_Heartbeat_OnlyRunningRows(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)

		project, _ := createTestProject(t, ctx, db, "hb-running-"+uuid.New().String()[:8])
		running := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)

		// Run heartbeat against a running row.
		if err := repo.Heartbeat(ctx, running.ID); err != nil {
			t.Fatalf("heartbeat running: %v", err)
		}
		got, err := repo.GetByID(ctx, running.ID)
		if err != nil {
			t.Fatalf("get running: %v", err)
		}
		if got.HeartbeatAt == nil {
			t.Fatalf("expected heartbeat_at to be set on running cycle")
		}

		// Transition to failed, then attempt heartbeat — must be a no-op.
		if err := repo.Fail(ctx, running.ID, "test", 0); err != nil {
			t.Fatalf("fail: %v", err)
		}
		preFail, _ := repo.GetByID(ctx, running.ID)
		// Sleep to ensure any successful Heartbeat would write a strictly
		// newer second-resolution timestamp.
		time.Sleep(1100 * time.Millisecond)
		if err := repo.Heartbeat(ctx, running.ID); err != nil {
			t.Fatalf("heartbeat on failed cycle: %v", err)
		}
		postFail, _ := repo.GetByID(ctx, running.ID)
		if postFail.HeartbeatAt == nil || preFail.HeartbeatAt == nil {
			t.Fatalf("expected pre-existing heartbeat_at to remain set")
		}
		if !postFail.HeartbeatAt.Equal(*preFail.HeartbeatAt) {
			t.Fatalf("heartbeat reached a non-running cycle: pre=%s post=%s",
				preFail.HeartbeatAt, postFail.HeartbeatAt)
		}
	})
}

func TestDreamCycleRepo_Abandon_TransitionsAndIdempotent(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)

		project, _ := createTestProject(t, ctx, db, "abandon-"+uuid.New().String()[:8])

		// Pending cycle: Abandon must transition it to failed.
		pending := &model.DreamCycle{
			ID:          uuid.New(),
			ProjectID:   project.ID,
			NamespaceID: project.NamespaceID,
			TokenBudget: 100,
		}
		if err := repo.Create(ctx, pending); err != nil {
			t.Fatalf("create pending: %v", err)
		}
		ok, err := repo.Abandon(ctx, pending.ID, "stuck for testing")
		if err != nil {
			t.Fatalf("abandon pending: %v", err)
		}
		if !ok {
			t.Fatalf("expected Abandon to return true for pending cycle")
		}
		got, _ := repo.GetByID(ctx, pending.ID)
		if got.Status != model.DreamStatusFailed {
			t.Fatalf("expected status=failed, got %q", got.Status)
		}
		if got.Error == nil || *got.Error != "stuck for testing" {
			t.Fatalf("expected reason recorded, got %v", got.Error)
		}
		if got.CompletedAt == nil {
			t.Fatalf("expected completed_at stamped")
		}

		// Idempotent: re-abandoning a failed cycle returns false and does
		// not overwrite the original error/completed_at.
		origErr := *got.Error
		origCompleted := *got.CompletedAt
		ok, err = repo.Abandon(ctx, pending.ID, "different reason")
		if err != nil {
			t.Fatalf("re-abandon: %v", err)
		}
		if ok {
			t.Fatalf("expected Abandon to return false for already-terminal cycle")
		}
		got, _ = repo.GetByID(ctx, pending.ID)
		if *got.Error != origErr {
			t.Fatalf("expected error unchanged, got %q", *got.Error)
		}
		if !got.CompletedAt.Equal(origCompleted) {
			t.Fatalf("expected completed_at unchanged")
		}
	})
}

func TestDreamCycleRepo_Abandon_PreservesTokensUsed(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)

		project, _ := createTestProject(t, ctx, db, "abandon-tokens-"+uuid.New().String()[:8])
		running := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 4242)

		ok, err := repo.Abandon(ctx, running.ID, "stuck for testing")
		if err != nil {
			t.Fatalf("abandon: %v", err)
		}
		if !ok {
			t.Fatalf("expected abandon true")
		}
		got, _ := repo.GetByID(ctx, running.ID)
		if got.TokensUsed != 4242 {
			t.Fatalf("expected tokens_used preserved at 4242, got %d", got.TokensUsed)
		}
	})
}

func TestDreamCycleRepo_ListStale(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)

		project, _ := createTestProject(t, ctx, db, "stale-"+uuid.New().String()[:8])
		old := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)
		fresh := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)
		completed := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)

		// Park `old` 20 minutes ago, leave `fresh` at now, complete the third.
		setCycleTimestamps(t, ctx, db, old.ID, time.Now().Add(-20*time.Minute), nil)
		if err := repo.Complete(ctx, completed.ID, json.RawMessage(`[]`), 0); err != nil {
			t.Fatalf("complete: %v", err)
		}
		// Even if completed.updated_at is also old, ListStale must skip it
		// because status guard requires running.
		setCycleTimestamps(t, ctx, db, completed.ID, time.Now().Add(-30*time.Minute), nil)

		// Threshold 10 minutes — old qualifies, fresh does not.
		stale, err := repo.ListStale(ctx, 10*time.Minute)
		if err != nil {
			t.Fatalf("list stale: %v", err)
		}
		mine := map[uuid.UUID]bool{old.ID: true, fresh.ID: true, completed.ID: true}
		var got []uuid.UUID
		for _, c := range stale {
			if mine[c.ID] {
				got = append(got, c.ID)
			}
		}
		if len(got) != 1 || got[0] != old.ID {
			t.Fatalf("expected only old cycle to be stale, got %v", got)
		}

		// Threshold 5 minutes — both old and fresh would be stale once we
		// park fresh too. Verify the boundary.
		setCycleTimestamps(t, ctx, db, fresh.ID, time.Now().Add(-6*time.Minute), nil)
		stale, _ = repo.ListStale(ctx, 5*time.Minute)
		got = got[:0]
		for _, c := range stale {
			if mine[c.ID] {
				got = append(got, c.ID)
			}
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 stale cycles, got %d (%v)", len(got), got)
		}
	})
}

func TestDreamCycleRepo_GuardsBlockLateWrites(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)

		project, _ := createTestProject(t, ctx, db, "guards-"+uuid.New().String()[:8])
		running := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)

		// Abandon wins. Race-loser writes from the runner must no-op.
		if ok, err := repo.Abandon(ctx, running.ID, "stuck"); err != nil || !ok {
			t.Fatalf("abandon: ok=%v err=%v", ok, err)
		}

		// Late Complete from a still-alive runner.
		if err := repo.Complete(ctx, running.ID, json.RawMessage(`[]`), 9999); err != nil {
			t.Fatalf("complete after abandon: %v", err)
		}
		got, _ := repo.GetByID(ctx, running.ID)
		if got.Status != model.DreamStatusFailed {
			t.Fatalf("late Complete clobbered abandon: status=%q", got.Status)
		}
		if got.Error == nil || *got.Error != "stuck" {
			t.Fatalf("late Complete clobbered error: %v", got.Error)
		}

		// Late UpdateStatus from a phase boundary.
		if err := repo.UpdateStatus(ctx, running.ID, model.DreamStatusRunning, "entity_dedup", 8888); err != nil {
			t.Fatalf("update status after abandon: %v", err)
		}
		got, _ = repo.GetByID(ctx, running.ID)
		if got.Status != model.DreamStatusFailed {
			t.Fatalf("late UpdateStatus clobbered abandon: status=%q", got.Status)
		}

		// Late Fail from runner.
		if err := repo.Fail(ctx, running.ID, "runner saw an error", 7777); err != nil {
			t.Fatalf("fail after abandon: %v", err)
		}
		got, _ = repo.GetByID(ctx, running.ID)
		if got.Error == nil || *got.Error != "stuck" {
			t.Fatalf("late Fail clobbered abandon's error: %v", got.Error)
		}
	})
}
