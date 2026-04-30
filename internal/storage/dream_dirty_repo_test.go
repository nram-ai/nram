package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// setDirtySince writes dirty_since directly so tests can produce deterministic
// second-precision timestamps without sequencing real-time MarkDirty calls.
func setDirtySince(t *testing.T, ctx context.Context, db DB, projectID uuid.UUID, ts time.Time) {
	t.Helper()
	query := `UPDATE dream_project_dirty SET dirty_since = ? WHERE project_id = ?`
	if db.Backend() == BackendPostgres {
		query = `UPDATE dream_project_dirty SET dirty_since = $1 WHERE project_id = $2`
	}
	if _, err := db.Exec(ctx, query, ts.UTC().Format(time.RFC3339), projectID.String()); err != nil {
		t.Fatalf("set dirty_since: %v", err)
	}
}

// listDirtyOrder calls ListDirtyProjects and returns only the IDs that belong
// to the given set, preserving order. The Postgres test path uses a shared
// schema so unrelated dirty rows from other suites must be filtered out.
func listDirtyOrder(t *testing.T, ctx context.Context, repo *DreamDirtyRepo, mine map[uuid.UUID]bool) []uuid.UUID {
	t.Helper()
	rows, err := repo.ListDirtyProjects(ctx)
	if err != nil {
		t.Fatalf("list dirty: %v", err)
	}
	out := make([]uuid.UUID, 0, len(mine))
	for _, dp := range rows {
		if mine[dp.ProjectID] {
			out = append(out, dp.ProjectID)
		}
	}
	return out
}

func TestDreamDirtyRepo_ListDirtyProjects_Ordering(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamDirtyRepo(db)

		suffix := uuid.New().String()[:8]
		pNeverA, _ := createTestProject(t, ctx, db, "dirty-never-a-"+suffix)
		pNeverB, _ := createTestProject(t, ctx, db, "dirty-never-b-"+suffix)
		pOld, _ := createTestProject(t, ctx, db, "dirty-old-"+suffix)
		pRecent, _ := createTestProject(t, ctx, db, "dirty-recent-"+suffix)

		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		if err := repo.SetLastDreamAt(ctx, pOld.ID, base.Add(1*time.Hour)); err != nil {
			t.Fatalf("set last_dream_at pOld: %v", err)
		}
		if err := repo.SetLastDreamAt(ctx, pRecent.ID, base.Add(2*time.Hour)); err != nil {
			t.Fatalf("set last_dream_at pRecent: %v", err)
		}
		// SetLastDreamAt's upsert leaves dirty_since NULL; mark every row
		// dirty so all four satisfy the WHERE clause.
		for _, p := range []uuid.UUID{pNeverA.ID, pNeverB.ID, pOld.ID, pRecent.ID} {
			if err := repo.MarkDirty(ctx, p); err != nil {
				t.Fatalf("mark dirty %s: %v", p, err)
			}
		}

		// pNeverB.dirty_since < pNeverA.dirty_since exercises the dirty_since
		// tiebreaker among never-dreamed rows.
		setDirtySince(t, ctx, db, pNeverA.ID, base.Add(10*time.Minute))
		setDirtySince(t, ctx, db, pNeverB.ID, base.Add(5*time.Minute))

		mine := map[uuid.UUID]bool{
			pNeverA.ID: true,
			pNeverB.ID: true,
			pOld.ID:    true,
			pRecent.ID: true,
		}
		want := []uuid.UUID{pNeverB.ID, pNeverA.ID, pOld.ID, pRecent.ID}
		got := listDirtyOrder(t, ctx, repo, mine)

		if len(got) != len(want) {
			t.Fatalf("expected %d dirty rows, got %d (%v)", len(want), len(got), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("position %d: want %s, got %s (full order: %v)", i, want[i], got[i], got)
			}
		}
	})
}

func TestDreamDirtyRepo_ListDirtyProjects_TiebreakerOnProjectID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewDreamDirtyRepo(db)

		suffix := uuid.New().String()[:8]
		p1, _ := createTestProject(t, ctx, db, "dirty-tie-1-"+suffix)
		p2, _ := createTestProject(t, ctx, db, "dirty-tie-2-"+suffix)

		for _, p := range []uuid.UUID{p1.ID, p2.ID} {
			if err := repo.MarkDirty(ctx, p); err != nil {
				t.Fatalf("mark dirty %s: %v", p, err)
			}
		}

		// Identical dirty_since on both rows; both have NULL last_dream_at.
		// The only ordering signal left is project_id ASC.
		shared := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		setDirtySince(t, ctx, db, p1.ID, shared)
		setDirtySince(t, ctx, db, p2.ID, shared)

		mine := map[uuid.UUID]bool{p1.ID: true, p2.ID: true}
		got := listDirtyOrder(t, ctx, repo, mine)

		if len(got) != 2 {
			t.Fatalf("expected 2 dirty rows, got %d (%v)", len(got), got)
		}

		first, second := p1.ID, p2.ID
		if p2.ID.String() < p1.ID.String() {
			first, second = p2.ID, p1.ID
		}
		if got[0] != first || got[1] != second {
			t.Fatalf("project_id tiebreaker failed: want [%s %s], got %v", first, second, got)
		}
	})
}
