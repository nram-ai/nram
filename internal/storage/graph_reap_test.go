package storage

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// reapFixture builds a namespace with four memories (live, soft-deleted,
// superseded, and the superseding winner), three entities, and four edges whose
// provenance spans every state plus a NULL-provenance edge. It returns the repos
// and the entity IDs so each test can assert against a known graph.
type reapFixture struct {
	nsID       uuid.UUID
	mrepo      *MemoryRepo
	erepo      *EntityRepo
	rrepo      *RelationshipRepo
	a, b, c    uuid.UUID
	liveMem    uuid.UUID
	deletedMem uuid.UUID
	superseded uuid.UUID
}

func newReapFixture(t *testing.T, ctx context.Context, db DB) reapFixture {
	t.Helper()
	mrepo := NewMemoryRepo(db)
	erepo := NewEntityRepo(db)
	rrepo := NewRelationshipRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	mk := func() uuid.UUID {
		m := newTestMemory(nsID)
		if err := mrepo.Create(ctx, m); err != nil {
			t.Fatalf("create memory: %v", err)
		}
		return m.ID
	}
	live := mk()
	deleted := mk()
	loser := mk()
	winner := mk()

	if err := mrepo.SoftDelete(ctx, deleted, nsID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if err := mrepo.MarkSupersededBy(ctx, loser, nsID, winner); err != nil {
		t.Fatalf("mark superseded: %v", err)
	}

	a := createTestEntity(t, ctx, db, nsID, "alice")
	b := createTestEntity(t, ctx, db, nsID, "bob")
	c := createTestEntity(t, ctx, db, nsID, "carol")

	mkEdge := func(src, tgt uuid.UUID, relation string, mem *uuid.UUID) {
		r := &model.Relationship{
			NamespaceID:  nsID,
			SourceID:     src,
			TargetID:     tgt,
			Relation:     relation,
			Weight:       1.0,
			SourceMemory: mem,
		}
		if err := rrepo.Create(ctx, r); err != nil {
			t.Fatalf("create relationship: %v", err)
		}
	}
	mkEdge(a, b, "knows", &live)     // live provenance — kept
	mkEdge(a, c, "knows", &deleted)  // soft-deleted source — lost
	mkEdge(b, c, "knows", &loser)    // superseded source — lost
	mkEdge(a, c, "likes", nil)       // NULL provenance (hard-deleted) — lost

	return reapFixture{
		nsID: nsID, mrepo: mrepo, erepo: erepo, rrepo: rrepo,
		a: a, b: b, c: c,
		liveMem: live, deletedMem: deleted, superseded: loser,
	}
}

// cleanLostProvenance reaps any orphaned edges left by earlier tests in the
// shared Postgres schema, using the method under test itself. It removes only
// garbage (edges whose sourcing memory is already gone), never live-sourced
// data, so it cannot perturb another test's own rows — and it makes the global
// counts below exact and deterministic. SQLite starts from a fresh DB per test,
// so this is effectively a no-op there.
func cleanLostProvenance(t *testing.T, ctx context.Context, db DB) {
	t.Helper()
	if _, err := NewRelationshipRepo(db).DeleteByLostProvenance(ctx, 0); err != nil {
		t.Fatalf("pre-clean lost provenance: %v", err)
	}
}

func TestRelationshipRepo_CountAndDeleteLostProvenance(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		cleanLostProvenance(t, ctx, db)
		f := newReapFixture(t, ctx, db)

		// NULL-source, soft-deleted-source, and superseded-source edges are all
		// lost provenance; only the live-sourced edge survives.
		got, err := f.rrepo.CountLostProvenance(ctx)
		if err != nil {
			t.Fatalf("count lost provenance: %v", err)
		}
		if got != 3 {
			t.Fatalf("expected 3 lost-provenance edges, got %d", got)
		}

		deleted, err := f.rrepo.DeleteByLostProvenance(ctx, 0)
		if err != nil {
			t.Fatalf("delete lost provenance: %v", err)
		}
		if deleted != 3 {
			t.Fatalf("expected to delete 3 edges, deleted %d", deleted)
		}

		after, err := f.rrepo.CountLostProvenance(ctx)
		if err != nil {
			t.Fatalf("count after: %v", err)
		}
		if after != 0 {
			t.Fatalf("expected 0 lost-provenance edges after reap, got %d", after)
		}

		// The single live-sourced edge (a-b) must remain.
		remaining, err := f.rrepo.ListByNamespace(ctx, f.nsID)
		if err != nil {
			t.Fatalf("list relationships: %v", err)
		}
		if len(remaining) != 1 {
			t.Fatalf("expected 1 surviving edge, got %d", len(remaining))
		}
		if remaining[0].SourceMemory == nil || *remaining[0].SourceMemory != f.liveMem {
			t.Fatalf("surviving edge has wrong provenance: %+v", remaining[0].SourceMemory)
		}

		// Idempotent: a second reap deletes nothing.
		again, err := f.rrepo.DeleteByLostProvenance(ctx, 0)
		if err != nil {
			t.Fatalf("second delete: %v", err)
		}
		if again != 0 {
			t.Fatalf("expected second reap to delete 0, deleted %d", again)
		}
	})
}

func TestRelationshipRepo_DeleteByLostProvenance_Batched(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		cleanLostProvenance(t, ctx, db)
		f := newReapFixture(t, ctx, db)

		// Batch size 1 forces one iteration per lost edge; each returns at most 1.
		var total int64
		for {
			n, err := f.rrepo.DeleteByLostProvenance(ctx, 1)
			if err != nil {
				t.Fatalf("batched delete: %v", err)
			}
			total += n
			if n < 1 {
				break
			}
		}
		if total != 3 {
			t.Fatalf("expected 3 edges deleted across batches, got %d", total)
		}
		after, err := f.rrepo.CountLostProvenance(ctx)
		if err != nil {
			t.Fatalf("count after: %v", err)
		}
		if after != 0 {
			t.Fatalf("expected 0 lost-provenance edges after batched reap, got %d", after)
		}
	})
}

func TestRelationshipRepo_DeleteBySourceMemory(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		f := newReapFixture(t, ctx, db)

		// Reap exactly the edges sourced by the soft-deleted memory (a-c knows).
		affected, err := f.rrepo.DeleteBySourceMemory(ctx, f.nsID, f.deletedMem)
		if err != nil {
			t.Fatalf("delete by source memory: %v", err)
		}
		// Affected endpoints are a and c (deduped).
		seen := map[uuid.UUID]bool{}
		for _, id := range affected {
			seen[id] = true
		}
		if len(affected) != 2 || !seen[f.a] || !seen[f.c] {
			t.Fatalf("expected affected endpoints {a,c}, got %v", affected)
		}

		// That edge is gone; the other three remain.
		remaining, err := f.rrepo.ListByNamespace(ctx, f.nsID)
		if err != nil {
			t.Fatalf("list relationships: %v", err)
		}
		if len(remaining) != 3 {
			t.Fatalf("expected 3 remaining edges, got %d", len(remaining))
		}
		for _, r := range remaining {
			if r.SourceMemory != nil && *r.SourceMemory == f.deletedMem {
				t.Fatalf("edge sourced by deleted memory survived: %+v", r)
			}
		}
	})
}

func TestEntityRepo_RecomputeMentionCounts(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		f := newReapFixture(t, ctx, db)

		// Recompute over all entities BEFORE reaping. mention_count must reflect
		// only live, non-superseded source memories:
		//   alice: edges a-b(live), a-c(deleted), a-c(null) -> 1 live source
		//   bob:   edges a-b(live), b-c(superseded)         -> 1 live source
		//   carol: edges a-c(deleted), b-c(superseded), a-c(null) -> 0 live
		n, err := f.erepo.RecomputeMentionCounts(ctx, nil)
		if err != nil {
			t.Fatalf("recompute mention counts: %v", err)
		}
		if n < 3 {
			t.Fatalf("expected to update at least 3 entities, updated %d", n)
		}

		assertCount := func(id uuid.UUID, name string, want int) {
			e, err := f.erepo.GetByID(ctx, id)
			if err != nil {
				t.Fatalf("get entity %s: %v", name, err)
			}
			if e.MentionCount != want {
				t.Fatalf("%s mention_count = %d, want %d", name, e.MentionCount, want)
			}
		}
		assertCount(f.a, "alice", 1)
		assertCount(f.b, "bob", 1)
		assertCount(f.c, "carol", 0)

		// Scoped recompute updates only the named entity.
		if _, err := f.erepo.RecomputeMentionCounts(ctx, []uuid.UUID{f.a}); err != nil {
			t.Fatalf("scoped recompute: %v", err)
		}
	})
}
