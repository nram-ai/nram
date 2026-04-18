package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func createTestEntity(t *testing.T, ctx context.Context, db DB, nsID uuid.UUID, name string) uuid.UUID {
	t.Helper()
	repo := NewEntityRepo(db)
	entity := &model.Entity{
		NamespaceID: nsID,
		Name:        name,
		Canonical:   name,
		EntityType:  "person",
	}
	if err := repo.Create(ctx, entity); err != nil {
		t.Fatalf("failed to create test entity %q: %v", name, err)
	}
	return entity.ID
}

func newTestRelationship(nsID, sourceID, targetID uuid.UUID) *model.Relationship {
	return &model.Relationship{
		NamespaceID: nsID,
		SourceID:    sourceID,
		TargetID:    targetID,
		Relation:    "knows",
		Weight:      1.0,
		Properties:  json.RawMessage(`{"context":"work"}`),
	}
}

func TestRelationshipRepo_Create(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := newTestRelationship(nsID, srcID, tgtID)
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create relationship: %v", err)
		}

		if rel.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if rel.NamespaceID != nsID {
			t.Fatalf("expected namespace_id %s, got %s", nsID, rel.NamespaceID)
		}
		if rel.SourceID != srcID {
			t.Fatalf("expected source_id %s, got %s", srcID, rel.SourceID)
		}
		if rel.TargetID != tgtID {
			t.Fatalf("expected target_id %s, got %s", tgtID, rel.TargetID)
		}
		if rel.Relation != "knows" {
			t.Fatalf("unexpected relation: %q", rel.Relation)
		}
		if rel.Weight != 1.0 {
			t.Fatalf("expected weight 1.0, got %f", rel.Weight)
		}
		if !jsonEqual(string(rel.Properties), `{"context":"work"}`) {
			t.Fatalf("unexpected properties: %q", string(rel.Properties))
		}
		if rel.ValidFrom.IsZero() {
			t.Fatal("expected non-zero valid_from")
		}
		if rel.ValidUntil != nil {
			t.Fatalf("expected nil valid_until, got %v", rel.ValidUntil)
		}
		if rel.SourceMemory != nil {
			t.Fatalf("expected nil source_memory, got %v", rel.SourceMemory)
		}
		if rel.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
	})
}

func TestRelationshipRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := &model.Relationship{
			NamespaceID: nsID,
			SourceID:    srcID,
			TargetID:    tgtID,
			Relation:    "knows",
			Weight:      1.0,
		}
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if rel.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestRelationshipRepo_Create_ExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		explicitID := uuid.New()
		rel := &model.Relationship{
			ID:          explicitID,
			NamespaceID: nsID,
			SourceID:    srcID,
			TargetID:    tgtID,
			Relation:    "knows",
			Weight:      1.0,
		}
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if rel.ID != explicitID {
			t.Fatalf("expected ID %s, got %s", explicitID, rel.ID)
		}
	})
}

func TestRelationshipRepo_Create_NilProperties(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := &model.Relationship{
			NamespaceID: nsID,
			SourceID:    srcID,
			TargetID:    tgtID,
			Relation:    "knows",
			Weight:      1.0,
		}
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if string(rel.Properties) != "{}" {
			t.Fatalf("expected properties '{}', got %q", string(rel.Properties))
		}
	})
}

func TestRelationshipRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := newTestRelationship(nsID, srcID, tgtID)
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}

		if fetched.ID != rel.ID {
			t.Fatalf("expected ID %s, got %s", rel.ID, fetched.ID)
		}
		if fetched.Relation != rel.Relation {
			t.Fatalf("expected relation %q, got %q", rel.Relation, fetched.Relation)
		}
		if fetched.Weight != rel.Weight {
			t.Fatalf("expected weight %f, got %f", rel.Weight, fetched.Weight)
		}
	})
}

func TestRelationshipRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestRelationshipRepo_Expire(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := newTestRelationship(nsID, srcID, tgtID)
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if rel.ValidUntil != nil {
			t.Fatal("expected nil valid_until before expire")
		}

		if err := repo.Expire(ctx, rel.ID, nsID); err != nil {
			t.Fatalf("failed to expire: %v", err)
		}

		fetched, err := repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get after expire: %v", err)
		}

		if fetched.ValidUntil == nil {
			t.Fatal("expected non-nil valid_until after expire")
		}
	})
}

func TestRelationshipRepo_Expire_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)

		err := repo.Expire(ctx, uuid.New(), uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestRelationshipRepo_Reinforce(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := newTestRelationship(nsID, srcID, tgtID)
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if rel.Weight != 1.0 {
			t.Fatalf("expected initial weight 1.0, got %f", rel.Weight)
		}

		if err := repo.Reinforce(ctx, rel.ID, nsID); err != nil {
			t.Fatalf("failed to reinforce: %v", err)
		}

		fetched, err := repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get after reinforce: %v", err)
		}

		if fetched.Weight != 2.0 {
			t.Fatalf("expected weight 2.0 after reinforce, got %f", fetched.Weight)
		}

		// Reinforce again
		if err := repo.Reinforce(ctx, rel.ID, nsID); err != nil {
			t.Fatalf("failed to reinforce second time: %v", err)
		}

		fetched, err = repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get after second reinforce: %v", err)
		}

		if fetched.Weight != 3.0 {
			t.Fatalf("expected weight 3.0 after second reinforce, got %f", fetched.Weight)
		}
	})
}

func TestRelationshipRepo_Reinforce_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)

		err := repo.Reinforce(ctx, uuid.New(), uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestRelationshipRepo_ListByEntity(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")
		bob := createTestEntity(t, ctx, db, nsID, "bob")
		charlie := createTestEntity(t, ctx, db, nsID, "charlie")

		// alice -> bob
		r1 := &model.Relationship{
			NamespaceID: nsID, SourceID: alice, TargetID: bob,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("failed to create r1: %v", err)
		}

		// charlie -> alice
		r2 := &model.Relationship{
			NamespaceID: nsID, SourceID: charlie, TargetID: alice,
			Relation: "works_with", Weight: 1.0,
		}
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("failed to create r2: %v", err)
		}

		// bob -> charlie (should not appear for alice)
		r3 := &model.Relationship{
			NamespaceID: nsID, SourceID: bob, TargetID: charlie,
			Relation: "manages", Weight: 1.0,
		}
		if err := repo.Create(ctx, r3); err != nil {
			t.Fatalf("failed to create r3: %v", err)
		}

		// List for alice — should include r1 (source) and r2 (target)
		results, err := repo.ListByEntity(ctx, alice)
		if err != nil {
			t.Fatalf("failed to list by entity: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 relationships for alice, got %d", len(results))
		}

		// List for bob — should include r1 (target) and r3 (source)
		results, err = repo.ListByEntity(ctx, bob)
		if err != nil {
			t.Fatalf("failed to list by entity for bob: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 relationships for bob, got %d", len(results))
		}
	})
}

func TestRelationshipRepo_ListByEntity_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)

		results, err := repo.ListByEntity(ctx, uuid.New())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestRelationshipRepo_TraverseFromEntity_SingleHop(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")
		bob := createTestEntity(t, ctx, db, nsID, "bob")
		charlie := createTestEntity(t, ctx, db, nsID, "charlie")

		// alice -> bob
		r1 := &model.Relationship{
			NamespaceID: nsID, SourceID: alice, TargetID: bob,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("failed to create r1: %v", err)
		}

		// bob -> charlie
		r2 := &model.Relationship{
			NamespaceID: nsID, SourceID: bob, TargetID: charlie,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("failed to create r2: %v", err)
		}

		// Traverse 1 hop from alice — should only get r1
		results, err := repo.TraverseFromEntity(ctx, alice, 1)
		if err != nil {
			t.Fatalf("failed to traverse: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 relationship at 1 hop, got %d", len(results))
		}
		if results[0].ID != r1.ID {
			t.Fatalf("expected relationship %s, got %s", r1.ID, results[0].ID)
		}
	})
}

func TestRelationshipRepo_TraverseFromEntity_MultiHop(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")
		bob := createTestEntity(t, ctx, db, nsID, "bob")
		charlie := createTestEntity(t, ctx, db, nsID, "charlie")

		// alice -> bob
		r1 := &model.Relationship{
			NamespaceID: nsID, SourceID: alice, TargetID: bob,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("failed to create r1: %v", err)
		}

		// bob -> charlie
		r2 := &model.Relationship{
			NamespaceID: nsID, SourceID: bob, TargetID: charlie,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("failed to create r2: %v", err)
		}

		// Traverse 2 hops from alice — should get r1 and r2
		results, err := repo.TraverseFromEntity(ctx, alice, 2)
		if err != nil {
			t.Fatalf("failed to traverse: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 relationships at 2 hops, got %d", len(results))
		}
	})
}

func TestRelationshipRepo_TraverseFromEntity_Cycle(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")
		bob := createTestEntity(t, ctx, db, nsID, "bob")

		// alice -> bob
		r1 := &model.Relationship{
			NamespaceID: nsID, SourceID: alice, TargetID: bob,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("failed to create r1: %v", err)
		}

		// bob -> alice (cycle)
		r2 := &model.Relationship{
			NamespaceID: nsID, SourceID: bob, TargetID: alice,
			Relation: "knows_back", Weight: 1.0,
		}
		if err := repo.Create(ctx, r2); err != nil {
			t.Fatalf("failed to create r2: %v", err)
		}

		// Traverse many hops — should not loop infinitely
		results, err := repo.TraverseFromEntity(ctx, alice, 10)
		if err != nil {
			t.Fatalf("failed to traverse with cycle: %v", err)
		}

		// Should find both unique relationships but not revisit entities or duplicate relationships.
		if len(results) != 2 {
			t.Fatalf("expected exactly 2 unique relationships in cycle traversal, got %d", len(results))
		}
	})
}

func TestRelationshipRepo_TraverseFromEntity_ZeroHops(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)

		results, err := repo.TraverseFromEntity(ctx, uuid.New(), 0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for 0 hops, got %d", len(results))
		}
	})
}

func TestRelationshipRepo_TraverseFromEntity_NoRelationships(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")

		results, err := repo.TraverseFromEntity(ctx, alice, 3)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for isolated entity, got %d", len(results))
		}
	})
}

// TestRelationshipRepo_Create_ConcurrentWeightMerge pins the invariant that
// concurrent Create calls with the same (namespace, src, tgt, relation,
// valid_from) triple but different weights converge on max(inputs). If
// ON CONFLICT DO UPDATE regressed to last-writer-wins the final weight
// would match the weight of whichever goroutine's write happened to land
// second, not the maximum.
func TestRelationshipRepo_Create_ConcurrentWeightMerge(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "acme")

		// Weights chosen so max is unambiguous and interior values are
		// non-monotonic (max is not simply the last or first in the slice).
		weights := []float64{0.10, 0.55, 0.99, 0.33, 0.72, 0.21, 0.88, 0.44,
			0.63, 0.17, 0.91, 0.38, 0.77, 0.29, 0.82, 0.50}
		wantMax := 0.0
		for _, w := range weights {
			if w > wantMax {
				wantMax = w
			}
		}

		validFrom, err := time.Parse(time.RFC3339, "2026-04-01T12:00:00Z")
		if err != nil {
			t.Fatalf("parse valid_from: %v", err)
		}

		var wg sync.WaitGroup
		errs := make(chan error, len(weights))
		for _, w := range weights {
			wg.Add(1)
			go func(weight float64) {
				defer wg.Done()
				rel := &model.Relationship{
					NamespaceID: nsID,
					SourceID:    srcID,
					TargetID:    tgtID,
					Relation:    "works_at",
					Weight:      weight,
					Properties:  json.RawMessage(`{}`),
					ValidFrom:   validFrom,
				}
				if err := repo.Create(ctx, rel); err != nil {
					errs <- err
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent Create failed: %v", err)
		}

		// Exactly one row should exist for the triple.
		countQuery := `SELECT COUNT(*), MAX(weight) FROM relationships
			WHERE namespace_id = ? AND source_id = ? AND target_id = ?
			  AND relation = ? AND valid_from = ?`
		if db.Backend() == BackendPostgres {
			countQuery = `SELECT COUNT(*), MAX(weight) FROM relationships
				WHERE namespace_id = $1 AND source_id = $2 AND target_id = $3
				  AND relation = $4 AND valid_from = $5`
		}
		var count int
		var gotWeight float64
		row := db.QueryRow(ctx, countQuery,
			nsID.String(), srcID.String(), tgtID.String(),
			"works_at", validFrom.UTC().Format("2006-01-02T15:04:05Z07:00"),
		)
		if err := row.Scan(&count, &gotWeight); err != nil {
			t.Fatalf("scan count: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected exactly 1 row for triple, got %d", count)
		}
		if gotWeight != wantMax {
			t.Fatalf("expected weight == max(inputs)=%.2f, got %.2f (last-writer-wins regression?)", wantMax, gotWeight)
		}
	})
}
