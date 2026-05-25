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
		// tokens_used is now derived live from the SUM of token_usage rows
		// attributed to the cycle via cycle_id. Tests that need a specific
		// tokens_used seed must insert token_usage rows directly; the
		// UpdateStatus path no longer accepts a tokens_used value.
		if err := repo.UpdateStatus(ctx, cycle.ID, model.DreamStatusRunning, "entity_dedup"); err != nil {
			t.Fatalf("seed phase: %v", err)
		}
	}
	out, err := repo.GetByID(ctx, cycle.ID)
	if err != nil {
		t.Fatalf("re-read cycle: %v", err)
	}
	return out
}

func TestDreamCycleRepo_TickProgress_OnlyRunningRows(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)

		project, _ := createTestProject(t, ctx, db, "hb-running-"+uuid.New().String()[:8])
		running := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)

		// Run TickProgress against a running row.
		used, err := repo.TickProgress(ctx, running.ID)
		if err != nil {
			t.Fatalf("tick progress running: %v", err)
		}
		if used != 0 {
			t.Fatalf("no token_usage rows yet, expected used=0, got %d", used)
		}
		got, err := repo.GetByID(ctx, running.ID)
		if err != nil {
			t.Fatalf("get running: %v", err)
		}
		if got.HeartbeatAt == nil {
			t.Fatalf("expected heartbeat_at to be set on running cycle")
		}

		// Transition to failed, then attempt TickProgress — must be a no-op.
		if err := repo.Fail(ctx, running.ID, "test"); err != nil {
			t.Fatalf("fail: %v", err)
		}
		preFail, _ := repo.GetByID(ctx, running.ID)
		// Sleep to ensure any successful tick would write a strictly newer
		// second-resolution timestamp.
		time.Sleep(1100 * time.Millisecond)
		used, err = repo.TickProgress(ctx, running.ID)
		if err != nil {
			t.Fatalf("tick progress on failed cycle: %v", err)
		}
		if used != 0 {
			t.Fatalf("tick on terminal row should return 0, got %d", used)
		}
		postFail, _ := repo.GetByID(ctx, running.ID)
		if postFail.HeartbeatAt == nil || preFail.HeartbeatAt == nil {
			t.Fatalf("expected pre-existing heartbeat_at to remain set")
		}
		if !postFail.HeartbeatAt.Equal(*preFail.HeartbeatAt) {
			t.Fatalf("tick reached a non-running cycle: pre=%s post=%s",
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

func TestDreamCycleRepo_Abandon_DerivesTokensUsedFromUsageRows(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)

		project, _ := createTestProject(t, ctx, db, "abandon-tokens-"+uuid.New().String()[:8])
		// Pre-seed dream_cycles.tokens_used to 4242 to verify Abandon
		// overwrites it with the derived SUM, not preserves it.
		running := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 4242)

		// Insert token_usage rows attributing 1234 tokens to this cycle.
		usageRepo := NewTokenUsageRepo(db)
		cycleID := running.ID
		if err := usageRepo.Record(ctx, &model.TokenUsage{
			NamespaceID:  project.NamespaceID,
			Operation:    "contradiction_judge",
			Provider:     "test",
			Model:        "test",
			TokensInput:  1000,
			TokensOutput: 234,
			Success:      true,
			CycleID:      &cycleID,
		}); err != nil {
			t.Fatalf("seed token_usage: %v", err)
		}

		ok, err := repo.Abandon(ctx, running.ID, "stuck for testing")
		if err != nil {
			t.Fatalf("abandon: %v", err)
		}
		if !ok {
			t.Fatalf("expected abandon true")
		}
		got, _ := repo.GetByID(ctx, running.ID)
		// Abandon now writes the live SUM (1234), not the preserved 4242.
		if got.TokensUsed != 1234 {
			t.Fatalf("expected tokens_used derived from token_usage SUM = 1234, got %d", got.TokensUsed)
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
		if err := repo.Complete(ctx, completed.ID, json.RawMessage(`[]`)); err != nil {
			t.Fatalf("complete: %v", err)
		}
		// Even if completed.updated_at is also old, ListStale must skip it
		// because status guard requires running.
		setCycleTimestamps(t, ctx, db, completed.ID, time.Now().Add(-30*time.Minute), nil)

		// Threshold 10 minutes — old qualifies, fresh does not.
		stale, err := repo.ListStale(ctx, 10*time.Minute, 0)
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
		stale, _ = repo.ListStale(ctx, 5*time.Minute, 0)
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
		if err := repo.Complete(ctx, running.ID, json.RawMessage(`[]`)); err != nil {
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
		if err := repo.UpdateStatus(ctx, running.ID, model.DreamStatusRunning, "entity_dedup"); err != nil {
			t.Fatalf("update status after abandon: %v", err)
		}
		got, _ = repo.GetByID(ctx, running.ID)
		if got.Status != model.DreamStatusFailed {
			t.Fatalf("late UpdateStatus clobbered abandon: status=%q", got.Status)
		}

		// Late Fail from runner.
		if err := repo.Fail(ctx, running.ID, "runner saw an error"); err != nil {
			t.Fatalf("fail after abandon: %v", err)
		}
		got, _ = repo.GetByID(ctx, running.ID)
		if got.Error == nil || *got.Error != "stuck" {
			t.Fatalf("late Fail clobbered abandon's error: %v", got.Error)
		}
	})
}

func TestDreamCycleRepo_TickProgress_DerivesTokensUsedFromUsageRows(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)
		usageRepo := NewTokenUsageRepo(db)

		project, _ := createTestProject(t, ctx, db, "tick-tokens-"+uuid.New().String()[:8])
		mine := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)
		other := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 0)

		// 3 rows attributed to mine: 100+50, 200+25, 0+10 = 385.
		// 1 row attributed to other: must NOT contribute.
		// 1 row with NULL cycle_id: must NOT contribute.
		mineID, otherID := mine.ID, other.ID
		seed := []*model.TokenUsage{
			{NamespaceID: project.NamespaceID, Operation: "contradiction_judge", Provider: "p", Model: "m", TokensInput: 100, TokensOutput: 50, Success: true, CycleID: &mineID},
			{NamespaceID: project.NamespaceID, Operation: "alignment_score", Provider: "p", Model: "m", TokensInput: 200, TokensOutput: 25, Success: true, CycleID: &mineID},
			{NamespaceID: project.NamespaceID, Operation: "embed_backfill", Provider: "p", Model: "m", TokensInput: 0, TokensOutput: 10, Success: true, CycleID: &mineID},
			{NamespaceID: project.NamespaceID, Operation: "synthesis", Provider: "p", Model: "m", TokensInput: 999, TokensOutput: 999, Success: true, CycleID: &otherID},
			{NamespaceID: project.NamespaceID, Operation: "memory.store", Provider: "p", Model: "m", TokensInput: 50, TokensOutput: 0, Success: true, CycleID: nil},
		}
		for _, u := range seed {
			if err := usageRepo.Record(ctx, u); err != nil {
				t.Fatalf("record token_usage: %v", err)
			}
		}

		used, err := repo.TickProgress(ctx, mine.ID)
		if err != nil {
			t.Fatalf("tick progress: %v", err)
		}
		if used != 385 {
			t.Fatalf("expected SUM=385 (only mine's rows), got %d", used)
		}

		// Row state matches the returned value.
		got, _ := repo.GetByID(ctx, mine.ID)
		if got.TokensUsed != 385 {
			t.Fatalf("dream_cycles.tokens_used = %d, want 385", got.TokensUsed)
		}
		if got.HeartbeatAt == nil {
			t.Fatal("heartbeat_at should be set after TickProgress")
		}
	})
}

func TestDreamCycleRepo_Complete_DerivesTokensUsedFromUsageRows(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamCycleRepo(db)
		usageRepo := NewTokenUsageRepo(db)

		project, _ := createTestProject(t, ctx, db, "complete-tokens-"+uuid.New().String()[:8])
		// Pre-seed the row's tokens_used to a fake value to verify Complete
		// overwrites with the live SUM, not the prior value.
		mine := createRunningCycle(t, ctx, repo, project.ID, project.NamespaceID, 999_999)
		mineID := mine.ID

		if err := usageRepo.Record(ctx, &model.TokenUsage{
			NamespaceID: project.NamespaceID,
			Operation:   "synthesis", Provider: "p", Model: "m",
			TokensInput: 700, TokensOutput: 350, Success: true, CycleID: &mineID,
		}); err != nil {
			t.Fatalf("record: %v", err)
		}

		if err := repo.Complete(ctx, mine.ID, json.RawMessage(`[]`)); err != nil {
			t.Fatalf("complete: %v", err)
		}
		got, _ := repo.GetByID(ctx, mine.ID)
		if got.TokensUsed != 1050 {
			t.Fatalf("Complete should write SUM=1050, got %d", got.TokensUsed)
		}
		if got.Status != model.DreamStatusCompleted {
			t.Fatalf("status = %q, want completed", got.Status)
		}
	})
}

// TestDreamCycleRepo_ListByNamespacePathPrefix verifies the JOIN-based prefix
// filter used by the self-tier dreaming page. Cycles whose project's
// namespace path is equal to the caller's path or descended from it must be
// included; sibling-user cycles must be excluded.
func TestDreamCycleRepo_ListByNamespacePathPrefix(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsRepo := NewNamespaceRepo(db)
		repo := NewDreamCycleRepo(db)

		// Caller's project + sibling user's project.
		mine, _ := createTestProject(t, ctx, db, "mine-"+uuid.New().String()[:8])
		theirs, _ := createTestProject(t, ctx, db, "theirs-"+uuid.New().String()[:8])

		// Resolve caller's user namespace path — that's the prefix passed to
		// the new method. createTestProject creates the project under a fresh
		// user, with parent_id pointing at the user namespace.
		mineNS, err := nsRepo.GetByID(ctx, mine.NamespaceID)
		if err != nil {
			t.Fatalf("get mine ns: %v", err)
		}
		if mineNS.ParentID == nil {
			t.Fatalf("project namespace has no parent")
		}
		userNS, err := nsRepo.GetByID(ctx, *mineNS.ParentID)
		if err != nil {
			t.Fatalf("get user ns: %v", err)
		}
		callerPath := userNS.Path

		mineCycle := createRunningCycle(t, ctx, repo, mine.ID, mine.NamespaceID, 0)
		_ = createRunningCycle(t, ctx, repo, theirs.ID, theirs.NamespaceID, 0)

		got, err := repo.ListByNamespacePathPrefix(ctx, callerPath, 50, true)
		if err != nil {
			t.Fatalf("ListByNamespacePathPrefix(withProjectName=true): %v", err)
		}

		var sawMine, sawTheirs bool
		var mineRowName string
		for _, c := range got {
			if c.ID == mineCycle.ID {
				sawMine = true
				mineRowName = c.ProjectName
			}
			if c.ProjectID == theirs.ID {
				sawTheirs = true
			}
		}
		if !sawMine {
			t.Errorf("caller's cycle missing from prefix-filtered result")
		}
		if sawTheirs {
			t.Errorf("sibling user's cycle leaked across the prefix filter")
		}
		if mineRowName == "" {
			t.Errorf("withProjectName=true: caller's cycle has empty ProjectName, want populated")
		}

		// withProjectName=false: same rows must come back (the JOIN/WHERE
		// scoping is unchanged), but ProjectName must be empty so the org
		// and system tiers surface project_id only. Re-checking the
		// sibling-isolation invariant here guards against a regression
		// that drops the projects JOIN entirely on the masked path
		// (which would also drop the namespace filter that hangs off it).
		gotMasked, err := repo.ListByNamespacePathPrefix(ctx, callerPath, 50, false)
		if err != nil {
			t.Fatalf("ListByNamespacePathPrefix(withProjectName=false): %v", err)
		}
		var sawMineMasked, sawTheirsMasked bool
		for _, c := range gotMasked {
			if c.ProjectName != "" {
				t.Errorf("withProjectName=false: cycle %s carries ProjectName=%q, want empty", c.ID, c.ProjectName)
			}
			if c.ID == mineCycle.ID {
				sawMineMasked = true
			}
			if c.ProjectID == theirs.ID {
				sawTheirsMasked = true
			}
		}
		if !sawMineMasked {
			t.Errorf("withProjectName=false: caller's cycle missing from prefix-filtered result")
		}
		if sawTheirsMasked {
			t.Errorf("withProjectName=false: sibling user's cycle leaked across the prefix filter")
		}
	})
}
