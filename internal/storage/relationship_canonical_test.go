package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
)

// insertRawRelationship inserts a relationship row directly, bypassing the
// repo's write-time canonicalization, so a test can plant legacy non-canonical
// rows for the backfill to clean. Backend-aware placeholders.
func insertRawRelationship(t *testing.T, ctx context.Context, db DB, ns, src, tgt uuid.UUID, relation string, weight float64, vf time.Time) {
	t.Helper()
	q := `INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, valid_from, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if db.Backend() == BackendPostgres {
		q = `INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, valid_from, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	}
	ts := vf.UTC().Format(time.RFC3339)
	if _, err := db.Exec(ctx, q, uuid.New().String(), ns.String(), src.String(), tgt.String(), relation, weight, ts, ts); err != nil {
		t.Fatalf("insert raw relationship %q: %v", relation, err)
	}
}

// TestCanonicalizeRelations_Backfill exercises the one-time existing-data
// backfill end-to-end against the live schema on every configured backend
// (including Postgres, whose $1/$2/$3 UPDATE/DELETE branch is otherwise
// untested): legacy variant rows collapse to one canonical row at max weight,
// and a re-run is a no-op.
func TestCanonicalizeRelations_Backfill(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		vf := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		// Two legacy formatting variants of one edge + an unrelated edge.
		insertRawRelationship(t, ctx, db, nsID, srcID, tgtID, "related_to", 0.4, vf)
		insertRawRelationship(t, ctx, db, nsID, srcID, tgtID, "related to", 0.7, vf)
		insertRawRelationship(t, ctx, db, nsID, srcID, tgtID, "knows", 0.5, vf)

		changed, err := migration.CanonicalizeRelations(ctx, db.WriteDB(), db.Backend())
		if err != nil {
			t.Fatalf("CanonicalizeRelations: %v", err)
		}
		if changed == 0 {
			t.Fatal("expected changes on legacy variant data, got 0")
		}

		got, err := repo.FindActiveByTriple(ctx, nsID, srcID, tgtID, "related to")
		if err != nil {
			t.Fatalf("find merged: %v", err)
		}
		if got == nil || got.Weight != 0.7 {
			t.Fatalf("expected merged 'related to' row at max weight 0.7, got %+v", got)
		}
		// 'related_to' variant must be gone; 'knows' untouched. CountActive: the
		// merged edge + 'knows' = 2.
		count, err := repo.CountActiveByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 2 {
			t.Fatalf("expected 2 active rows (merged + knows), got %d", count)
		}

		// Idempotent re-run.
		again, err := migration.CanonicalizeRelations(ctx, db.WriteDB(), db.Backend())
		if err != nil {
			t.Fatalf("CanonicalizeRelations re-run: %v", err)
		}
		if again != 0 {
			t.Errorf("expected 0 changes on idempotent re-run, got %d", again)
		}
	})
}

// TestRelationshipRepo_Create_CanonicalizesRelation verifies the repo stores
// the canonical relation form regardless of the writer's formatting.
func TestRelationshipRepo_Create_CanonicalizesRelation(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := newTestRelationship(nsID, srcID, tgtID)
		rel.Relation = "related_to"
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("create: %v", err)
		}
		// The in-memory struct is mutated to the persisted form.
		if rel.Relation != "related to" {
			t.Errorf("in-memory relation = %q, want %q", rel.Relation, "related to")
		}
		got, err := repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Relation != "related to" {
			t.Errorf("stored relation = %q, want %q", got.Relation, "related to")
		}
	})
}

// TestRelationshipRepo_BatchCreate_MergesVariants verifies that two relations
// differing only by formatting collapse onto one row via the unique key, with
// the surviving weight being the max. The two writes share an explicit
// ValidFrom so they collide (valid_from is part of the unique key); the second
// write conflicts with the first's now-canonical row and the ON CONFLICT merge
// takes max(weight).
func TestRelationshipRepo_BatchCreate_MergesVariants(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		vf := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

		a := newTestRelationship(nsID, srcID, tgtID)
		a.Relation = "related_to"
		a.Weight = 0.4
		a.ValidFrom = vf
		if _, err := repo.BatchCreate(ctx, []*model.Relationship{a}); err != nil {
			t.Fatalf("batch create A: %v", err)
		}

		b := newTestRelationship(nsID, srcID, tgtID)
		b.Relation = "related to"
		b.Weight = 0.7
		b.ValidFrom = vf
		if _, err := repo.BatchCreate(ctx, []*model.Relationship{b}); err != nil {
			t.Fatalf("batch create B: %v", err)
		}

		count, err := repo.CountActiveByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected the two variants to merge to 1 active row, got %d", count)
		}

		got, err := repo.FindActiveByTriple(ctx, nsID, srcID, tgtID, "related to")
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if got == nil {
			t.Fatal("expected the merged row, got nil")
		}
		if got.Weight != 0.7 {
			t.Errorf("merged weight = %v, want max 0.7", got.Weight)
		}
		if got.Relation != "related to" {
			t.Errorf("merged relation = %q, want %q", got.Relation, "related to")
		}
	})
}

// TestRelationshipRepo_BatchCreate_MergesIntraBatchVariants verifies that two
// relation-formatting variants of the SAME edge in a SINGLE BatchCreate call
// collapse to one row at max weight. After canonicalization the two rows share
// the unique key; Postgres rejects that mid-statement with a cardinality
// violation ("ON CONFLICT DO UPDATE command cannot affect row a second time"),
// so the repo must recover (via the per-row fallback) for the batch to still
// merge. SQLite applies the second row's ON CONFLICT in order and merges
// directly. This guards the canonicalization change against a one-extraction,
// two-variant-of-the-same-edge payload.
func TestRelationshipRepo_BatchCreate_MergesIntraBatchVariants(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		vf := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		a := newTestRelationship(nsID, srcID, tgtID)
		a.Relation, a.Weight, a.ValidFrom = "related_to", 0.4, vf
		b := newTestRelationship(nsID, srcID, tgtID)
		b.Relation, b.Weight, b.ValidFrom = "related to", 0.7, vf

		// Both variants in ONE batch; they collide on the unique key once
		// canonicalized.
		if _, err := repo.BatchCreate(ctx, []*model.Relationship{a, b}); err != nil {
			t.Fatalf("BatchCreate with intra-batch variants: %v", err)
		}

		count, err := repo.CountActiveByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected the two intra-batch variants to merge to 1 row, got %d", count)
		}
		got, err := repo.FindActiveByTriple(ctx, nsID, srcID, tgtID, "related to")
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if got == nil {
			t.Fatal("expected the merged row, got nil")
		}
		if got.Weight != 0.7 {
			t.Errorf("merged weight = %v, want max 0.7", got.Weight)
		}
	})
}

// TestRelationshipRepo_FindActiveByTriple_CanonicalizesArg verifies the lookup
// canonicalizes its argument so a raw (e.g. snake_case) relation finds the
// canonically-stored row.
func TestRelationshipRepo_FindActiveByTriple_CanonicalizesArg(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := newTestRelationship(nsID, srcID, tgtID)
		rel.Relation = "related to" // stored canonical
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("create: %v", err)
		}

		got, err := repo.FindActiveByTriple(ctx, nsID, srcID, tgtID, "related_to")
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if got == nil {
			t.Fatal("expected to find the row via a variant relation, got nil")
		}
		if got.ID != rel.ID {
			t.Errorf("found id %s, want %s", got.ID, rel.ID)
		}
	})
}
