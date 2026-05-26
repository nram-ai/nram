package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
		rel.Weight = 0.5
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.Reinforce(ctx, rel.ID, nsID, 0.05); err != nil {
			t.Fatalf("failed to reinforce: %v", err)
		}

		fetched, err := repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get after reinforce: %v", err)
		}

		if got := fetched.Weight; got < 0.55-1e-9 || got > 0.55+1e-9 {
			t.Fatalf("expected weight ~0.55 after delta=0.05 reinforce, got %f", got)
		}

		// Reinforce again with a different delta to verify additivity.
		if err := repo.Reinforce(ctx, rel.ID, nsID, 0.10); err != nil {
			t.Fatalf("failed to reinforce second time: %v", err)
		}

		fetched, err = repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get after second reinforce: %v", err)
		}

		if got := fetched.Weight; got < 0.65-1e-9 || got > 0.65+1e-9 {
			t.Fatalf("expected weight ~0.65 after second reinforce delta=0.10, got %f", got)
		}
	})
}

// TestRelationshipRepo_Reinforce_ClampsAt2 pins the SQL-layer ceiling. The
// recall-side write must not run away past 2.0 even under sustained recall
// traffic; the dream phase's calculateWeight applies the same upper bound, so
// the two paths converge on the same maximum.
func TestRelationshipRepo_Reinforce_ClampsAt2(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rel := newTestRelationship(nsID, srcID, tgtID)
		rel.Weight = 1.95
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Single reinforce that would overshoot the cap by 0.05.
		if err := repo.Reinforce(ctx, rel.ID, nsID, 0.10); err != nil {
			t.Fatalf("failed to reinforce: %v", err)
		}

		fetched, err := repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get after reinforce: %v", err)
		}
		if fetched.Weight != 2.0 {
			t.Fatalf("expected weight clamped at 2.0, got %f", fetched.Weight)
		}

		// Subsequent reinforces stay at the cap.
		if err := repo.Reinforce(ctx, rel.ID, nsID, 0.50); err != nil {
			t.Fatalf("failed to reinforce again: %v", err)
		}
		fetched, err = repo.GetByID(ctx, rel.ID)
		if err != nil {
			t.Fatalf("failed to get after saturated reinforce: %v", err)
		}
		if fetched.Weight != 2.0 {
			t.Fatalf("expected weight pinned at 2.0, got %f", fetched.Weight)
		}
	})
}

func TestRelationshipRepo_Reinforce_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)

		err := repo.Reinforce(ctx, uuid.New(), uuid.New(), 0.05)
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
		tr, err := repo.TraverseFromEntity(ctx, alice, 1, 0)
		if err != nil {
			t.Fatalf("failed to traverse: %v", err)
		}
		if len(tr.Relationships) != 1 {
			t.Fatalf("expected 1 relationship at 1 hop, got %d", len(tr.Relationships))
		}
		if tr.Relationships[0].ID != r1.ID {
			t.Fatalf("expected relationship %s, got %s", r1.ID, tr.Relationships[0].ID)
		}
		if tr.Truncated {
			t.Fatalf("expected untruncated for uncapped traversal")
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
		tr, err := repo.TraverseFromEntity(ctx, alice, 2, 0)
		if err != nil {
			t.Fatalf("failed to traverse: %v", err)
		}
		if len(tr.Relationships) != 2 {
			t.Fatalf("expected 2 relationships at 2 hops, got %d", len(tr.Relationships))
		}
		if tr.Truncated {
			t.Fatalf("expected untruncated for uncapped traversal")
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
		tr, err := repo.TraverseFromEntity(ctx, alice, 10, 0)
		if err != nil {
			t.Fatalf("failed to traverse with cycle: %v", err)
		}

		// Should find both unique relationships but not revisit entities or duplicate relationships.
		if len(tr.Relationships) != 2 {
			t.Fatalf("expected exactly 2 unique relationships in cycle traversal, got %d", len(tr.Relationships))
		}
		if tr.Truncated {
			t.Fatalf("expected untruncated for uncapped traversal")
		}
	})
}

func TestRelationshipRepo_TraverseFromEntity_ZeroHops(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)

		tr, err := repo.TraverseFromEntity(ctx, uuid.New(), 0, 0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(tr.Relationships) != 0 {
			t.Fatalf("expected 0 results for 0 hops, got %d", len(tr.Relationships))
		}
	})
}

func TestRelationshipRepo_TraverseFromEntity_NoRelationships(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")

		tr, err := repo.TraverseFromEntity(ctx, alice, 3, 0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(tr.Relationships) != 0 {
			t.Fatalf("expected 0 results for isolated entity, got %d", len(tr.Relationships))
		}
	})
}

// TestRelationshipRepo_TraverseFromEntity_EdgeCap_StopsAtCap pins the
// short-circuit invariant for the BFS edge cap: when maxEdges is smaller
// than the available unique relationships, traversal returns exactly
// maxEdges relationships and reports Truncated=true. Without the cap the
// MCP memory_graph path would marshal the full unbounded result before
// the byte-budget reducer in result_limit.go ever ran.
func TestRelationshipRepo_TraverseFromEntity_EdgeCap_StopsAtCap(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")

		const totalNeighbors = 20
		for i := range totalNeighbors {
			neighbor := createTestEntity(t, ctx, db, nsID, fmt.Sprintf("neighbor-%d", i))
			rel := &model.Relationship{
				NamespaceID: nsID, SourceID: alice, TargetID: neighbor,
				Relation: "knows", Weight: 1.0,
			}
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("failed to create rel %d: %v", i, err)
			}
		}

		const cap = totalNeighbors / 2
		tr, err := repo.TraverseFromEntity(ctx, alice, 1, cap)
		if err != nil {
			t.Fatalf("failed to traverse with cap: %v", err)
		}
		if len(tr.Relationships) != cap {
			t.Fatalf("expected exactly %d relationships at cap, got %d", cap, len(tr.Relationships))
		}
		if !tr.Truncated {
			t.Fatalf("expected Truncated=true when cap fired")
		}
		if tr.Cap != cap {
			t.Fatalf("expected Cap echoed as %d, got %d", cap, tr.Cap)
		}
	})
}

// TestRelationshipRepo_TraverseFromEntity_EdgeCap_PartialHop verifies the
// cap can fire mid-hop on a multi-hop traversal: when the first hop alone
// already exceeds the cap, the second hop is never entered.
func TestRelationshipRepo_TraverseFromEntity_EdgeCap_PartialHop(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")
		bob := createTestEntity(t, ctx, db, nsID, "bob")

		// alice has 10 direct neighbors plus an alice->bob edge; bob in turn
		// has a downstream neighbor only visible if the second hop runs.
		const aliceNeighbors = 10
		for i := range aliceNeighbors {
			neighbor := createTestEntity(t, ctx, db, nsID, fmt.Sprintf("a-neighbor-%d", i))
			rel := &model.Relationship{
				NamespaceID: nsID, SourceID: alice, TargetID: neighbor,
				Relation: "knows", Weight: 1.0,
			}
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("failed to create alice rel %d: %v", i, err)
			}
		}
		aliceBob := &model.Relationship{
			NamespaceID: nsID, SourceID: alice, TargetID: bob,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, aliceBob); err != nil {
			t.Fatalf("failed to create alice->bob: %v", err)
		}
		downstream := createTestEntity(t, ctx, db, nsID, "downstream")
		bobDownstream := &model.Relationship{
			NamespaceID: nsID, SourceID: bob, TargetID: downstream,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, bobDownstream); err != nil {
			t.Fatalf("failed to create bob->downstream: %v", err)
		}

		const cap = 5
		tr, err := repo.TraverseFromEntity(ctx, alice, 2, cap)
		if err != nil {
			t.Fatalf("failed to traverse: %v", err)
		}
		if len(tr.Relationships) != cap {
			t.Fatalf("expected exactly %d relationships when cap fires mid-first-hop, got %d", cap, len(tr.Relationships))
		}
		if !tr.Truncated {
			t.Fatalf("expected Truncated=true")
		}
		for _, rel := range tr.Relationships {
			if rel.ID == bobDownstream.ID {
				t.Fatalf("bob->downstream should not appear; second hop should be skipped after cap fires")
			}
		}
	})
}

// TestRelationshipRepo_TraverseFromEntity_EdgeCap_NotReached pins the
// invariant that the cap is silent when the actual edge count is smaller:
// Truncated must remain false and all available edges must be returned.
func TestRelationshipRepo_TraverseFromEntity_EdgeCap_NotReached(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")
		bob := createTestEntity(t, ctx, db, nsID, "bob")

		rel := &model.Relationship{
			NamespaceID: nsID, SourceID: alice, TargetID: bob,
			Relation: "knows", Weight: 1.0,
		}
		if err := repo.Create(ctx, rel); err != nil {
			t.Fatalf("failed to create rel: %v", err)
		}

		tr, err := repo.TraverseFromEntity(ctx, alice, 1, 100)
		if err != nil {
			t.Fatalf("failed to traverse: %v", err)
		}
		if len(tr.Relationships) != 1 {
			t.Fatalf("expected 1 relationship, got %d", len(tr.Relationships))
		}
		if tr.Truncated {
			t.Fatalf("expected Truncated=false when actual count is below cap")
		}
	})
}

// TestRelationshipRepo_TraverseFromEntity_EdgeCap_Zero pins the invariant
// that maxEdges <= 0 disables the short-circuit and behaves identically
// to the pre-cap implementation — regression guard for the interface
// migration so existing callers passing 0 keep their unbounded semantics.
func TestRelationshipRepo_TraverseFromEntity_EdgeCap_Zero(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		alice := createTestEntity(t, ctx, db, nsID, "alice")

		const totalNeighbors = 15
		for i := range totalNeighbors {
			neighbor := createTestEntity(t, ctx, db, nsID, fmt.Sprintf("zero-neighbor-%d", i))
			rel := &model.Relationship{
				NamespaceID: nsID, SourceID: alice, TargetID: neighbor,
				Relation: "knows", Weight: 1.0,
			}
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("failed to create rel %d: %v", i, err)
			}
		}

		tr, err := repo.TraverseFromEntity(ctx, alice, 1, 0)
		if err != nil {
			t.Fatalf("failed to traverse: %v", err)
		}
		if len(tr.Relationships) != totalNeighbors {
			t.Fatalf("expected %d relationships when cap is 0, got %d", totalNeighbors, len(tr.Relationships))
		}
		if tr.Truncated {
			t.Fatalf("expected Truncated=false when cap is 0")
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

// TestRelationshipRepo_ExpireLowestNTransitive proves the pressure-prune
// query expires only transitive (properties.source = "transitive") edges,
// targets the lowest-weight rows first with created_at ASC as the
// tiebreaker, leaves user-asserted edges untouched even when their weight
// is lower, and skips already-expired rows.
func TestRelationshipRepo_ExpireLowestNTransitive(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		a := createTestEntity(t, ctx, db, nsID, "alice")
		b := createTestEntity(t, ctx, db, nsID, "bob")
		c := createTestEntity(t, ctx, db, nsID, "carol")
		d := createTestEntity(t, ctx, db, nsID, "dave")
		e := createTestEntity(t, ctx, db, nsID, "eve")

		mkRel := func(src, tgt uuid.UUID, weight float64, transitive bool, suffix string) *model.Relationship {
			props := json.RawMessage(`{}`)
			if transitive {
				props = json.RawMessage(`{"source":"transitive"}`)
			}
			return &model.Relationship{
				NamespaceID: nsID,
				SourceID:    src,
				TargetID:    tgt,
				Relation:    "knows-" + suffix,
				Weight:      weight,
				Properties:  props,
			}
		}

		// Three transitive rows with distinct weights, two user-asserted rows
		// (one with a lower weight than any transitive row, to prove the
		// branch's "transitive only" filter is honored).
		rels := []*model.Relationship{
			mkRel(a, b, 0.25, true, "t1"),  // transitive, lowest
			mkRel(a, c, 0.40, true, "t2"),  // transitive, middle
			mkRel(b, c, 0.60, true, "t3"),  // transitive, highest
			mkRel(a, d, 0.05, false, "u1"), // user-asserted, lower than any transitive
			mkRel(b, e, 0.50, false, "u2"), // user-asserted, mid-band
		}
		for _, rel := range rels {
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("create rel: %v", err)
			}
		}

		// Expire one already-expired row to confirm the filter skips it.
		if err := repo.Expire(ctx, rels[0].ID, nsID); err != nil {
			t.Fatalf("pre-expire: %v", err)
		}

		// Ask to expire 2 lowest-weight transitive rows. The pre-expired t1
		// must be skipped; t2 (weight 0.40) and t3 (weight 0.60) should land.
		// Importantly, u1 (weight 0.05) must NOT be touched even though it is
		// the lowest weight overall — it is user-asserted, not transitive.
		expired, err := repo.ExpireLowestNTransitive(ctx, nsID, 2)
		if err != nil {
			t.Fatalf("ExpireLowestNTransitive: %v", err)
		}
		if expired != 2 {
			t.Fatalf("expired count = %d, want 2", expired)
		}

		// Verify t2 and t3 are now expired.
		t2, err := repo.GetByID(ctx, rels[1].ID)
		if err != nil {
			t.Fatalf("get t2: %v", err)
		}
		if t2.ValidUntil == nil {
			t.Errorf("t2 (transitive, mid weight) should be expired, valid_until is nil")
		}
		t3, err := repo.GetByID(ctx, rels[2].ID)
		if err != nil {
			t.Fatalf("get t3: %v", err)
		}
		if t3.ValidUntil == nil {
			t.Errorf("t3 (transitive, high weight) should be expired, valid_until is nil")
		}

		// User-asserted rows MUST be untouched.
		u1, err := repo.GetByID(ctx, rels[3].ID)
		if err != nil {
			t.Fatalf("get u1: %v", err)
		}
		if u1.ValidUntil != nil {
			t.Errorf("u1 (user-asserted, lowest weight) must not be expired by transitive-only branch")
		}
		u2, err := repo.GetByID(ctx, rels[4].ID)
		if err != nil {
			t.Fatalf("get u2: %v", err)
		}
		if u2.ValidUntil != nil {
			t.Errorf("u2 (user-asserted) must not be expired by transitive-only branch")
		}
	})
}

// TestRelationshipRepo_ExpireLowestNTransitive_ZeroNoOps proves the
// fast-path that avoids a wasted UPDATE when N <= 0.
func TestRelationshipRepo_ExpireLowestNTransitive_ZeroNoOps(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		expired, err := repo.ExpireLowestNTransitive(ctx, nsID, 0)
		if err != nil {
			t.Fatalf("ExpireLowestNTransitive(n=0): %v", err)
		}
		if expired != 0 {
			t.Errorf("expired = %d, want 0", expired)
		}

		expired, err = repo.ExpireLowestNTransitive(ctx, nsID, -5)
		if err != nil {
			t.Fatalf("ExpireLowestNTransitive(n=-5): %v", err)
		}
		if expired != 0 {
			t.Errorf("expired with negative n = %d, want 0", expired)
		}
	})
}

// --- Batch method tests ------------------------------------------------

func TestRelationshipRepo_BatchCreate_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		res, err := repo.BatchCreate(ctx, nil)
		if err != nil {
			t.Fatalf("BatchCreate(nil): %v", err)
		}
		if res.Affected != 0 || res.Skipped != 0 {
			t.Fatalf("empty batch must return zeros, got %+v", res)
		}
		res, err = repo.BatchCreate(ctx, []*model.Relationship{})
		if err != nil {
			t.Fatalf("BatchCreate(empty slice): %v", err)
		}
		if res.Affected != 0 || res.Skipped != 0 {
			t.Fatalf("empty batch must return zeros, got %+v", res)
		}
	})
}

func TestRelationshipRepo_BatchCreate_HappyPath(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtIDs := []uuid.UUID{
			createTestEntity(t, ctx, db, nsID, "bob"),
			createTestEntity(t, ctx, db, nsID, "carol"),
			createTestEntity(t, ctx, db, nsID, "dave"),
			createTestEntity(t, ctx, db, nsID, "erin"),
			createTestEntity(t, ctx, db, nsID, "frank"),
		}

		rels := make([]*model.Relationship, len(tgtIDs))
		for i, tgt := range tgtIDs {
			rels[i] = &model.Relationship{
				NamespaceID: nsID,
				SourceID:    srcID,
				TargetID:    tgt,
				Relation:    "knows",
				Weight:      0.5,
			}
		}
		res, err := repo.BatchCreate(ctx, rels)
		if err != nil {
			t.Fatalf("BatchCreate: %v", err)
		}
		if res.Affected != int64(len(tgtIDs)) {
			t.Fatalf("Affected = %d, want %d", res.Affected, len(tgtIDs))
		}
		if res.Skipped != 0 {
			t.Fatalf("Skipped = %d, want 0", res.Skipped)
		}
		for i, rel := range rels {
			if rel.ID == uuid.Nil {
				t.Fatalf("row %d: ID not assigned", i)
			}
			fetched, err := repo.GetByID(ctx, rel.ID)
			if err != nil {
				t.Fatalf("row %d: GetByID: %v", i, err)
			}
			if fetched.TargetID != tgtIDs[i] {
				t.Fatalf("row %d: target mismatch", i)
			}
		}
	})
}

func TestRelationshipRepo_BatchCreate_UpsertMaxWeight(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		// Pre-seed at low weight.
		seed := newTestRelationship(nsID, srcID, tgtID)
		seed.Weight = 0.3
		if err := repo.Create(ctx, seed); err != nil {
			t.Fatalf("seed: %v", err)
		}

		// Batch the same triple at higher weight (and a sibling).
		tgt2 := createTestEntity(t, ctx, db, nsID, "carol")
		conflict := &model.Relationship{
			NamespaceID: nsID, SourceID: srcID, TargetID: tgtID,
			Relation: "knows", Weight: 0.9, Properties: json.RawMessage(`{"k":"v"}`),
			ValidFrom: seed.ValidFrom,
		}
		fresh := &model.Relationship{
			NamespaceID: nsID, SourceID: srcID, TargetID: tgt2,
			Relation: "knows", Weight: 0.5,
		}
		res, err := repo.BatchCreate(ctx, []*model.Relationship{conflict, fresh})
		if err != nil {
			t.Fatalf("BatchCreate: %v", err)
		}
		if res.Affected != 2 {
			t.Fatalf("Affected = %d, want 2", res.Affected)
		}

		fetched, err := repo.GetByID(ctx, seed.ID)
		if err != nil {
			t.Fatalf("re-read conflict row: %v", err)
		}
		if fetched.Weight != 0.9 {
			t.Errorf("conflict weight = %v, want 0.9 (max wins)", fetched.Weight)
		}
		if !jsonEqual(string(fetched.Properties), `{"k":"v"}`) {
			t.Errorf("conflict properties = %q, want last-writer-wins payload", string(fetched.Properties))
		}
	})
}

func TestRelationshipRepo_BatchCreate_FKViolationFallback(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		good1 := createTestEntity(t, ctx, db, nsID, "bob")
		good2 := createTestEntity(t, ctx, db, nsID, "carol")

		bogusEntity := uuid.New()
		rels := []*model.Relationship{
			{NamespaceID: nsID, SourceID: srcID, TargetID: good1, Relation: "knows", Weight: 0.5},
			{NamespaceID: nsID, SourceID: srcID, TargetID: bogusEntity, Relation: "knows", Weight: 0.5},
			{NamespaceID: nsID, SourceID: srcID, TargetID: good2, Relation: "knows", Weight: 0.5},
		}
		res, err := repo.BatchCreate(ctx, rels)
		if err != nil {
			t.Fatalf("BatchCreate: %v", err)
		}
		if res.Affected != 2 {
			t.Errorf("Affected = %d, want 2", res.Affected)
		}
		if res.Skipped != 1 {
			t.Errorf("Skipped = %d, want 1", res.Skipped)
		}
		if _, err := repo.GetByID(ctx, rels[0].ID); err != nil {
			t.Errorf("row 0 should have committed: %v", err)
		}
		if _, err := repo.GetByID(ctx, rels[2].ID); err != nil {
			t.Errorf("row 2 should have committed: %v", err)
		}
	})
}

// TestRelationshipRepo_BatchCreate_UpsertRefreshesID pins the contract
// that BatchCreate overwrites rel.ID via RETURNING on ON CONFLICT DO
// UPDATE, so callers can reliably map back to the persisted row id.
// Without this, dream-log target_id values would point to never-inserted
// client-generated uuids and rollback could not delete the surviving row.
func TestRelationshipRepo_BatchCreate_UpsertRefreshesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		seed := newTestRelationship(nsID, srcID, tgtID)
		if err := repo.Create(ctx, seed); err != nil {
			t.Fatalf("seed: %v", err)
		}
		survivingID := seed.ID

		clientID := uuid.New()
		conflict := &model.Relationship{
			ID:          clientID,
			NamespaceID: nsID, SourceID: srcID, TargetID: tgtID,
			Relation:  "knows",
			Weight:    0.9,
			ValidFrom: seed.ValidFrom,
		}
		if _, err := repo.BatchCreate(ctx, []*model.Relationship{conflict}); err != nil {
			t.Fatalf("BatchCreate: %v", err)
		}
		if conflict.ID == clientID {
			t.Errorf("rel.ID was not refreshed: still equals client-generated %s", clientID)
		}
		if conflict.ID != survivingID {
			t.Errorf("rel.ID = %s, want surviving row id %s", conflict.ID, survivingID)
		}
	})
}

// TestRelationshipRepo_BatchCreate_SkippedSetsIDToNil pins the contract
// that per-row constraint-violation skips leave rel.ID = uuid.Nil so
// callers iterating post-batch can filter out non-persisted entries.
func TestRelationshipRepo_BatchCreate_SkippedSetsIDToNil(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		good := createTestEntity(t, ctx, db, nsID, "bob")
		bogus := uuid.New()

		goodRel := &model.Relationship{
			NamespaceID: nsID, SourceID: srcID, TargetID: good,
			Relation: "knows", Weight: 0.5,
		}
		badRel := &model.Relationship{
			NamespaceID: nsID, SourceID: srcID, TargetID: bogus,
			Relation: "knows", Weight: 0.5,
		}
		if _, err := repo.BatchCreate(ctx, []*model.Relationship{goodRel, badRel}); err != nil {
			t.Fatalf("BatchCreate: %v", err)
		}
		if goodRel.ID == uuid.Nil {
			t.Error("good row's rel.ID should not be uuid.Nil after a successful insert")
		}
		if badRel.ID != uuid.Nil {
			t.Errorf("skipped row's rel.ID should be uuid.Nil, got %s", badRel.ID)
		}
	})
}

func TestRelationshipRepo_BatchCreate_LargeBatchChunks(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		// Two chunks of 500 worth: 1100 rows forces 3 chunks.
		const total = 1100
		tgtIDs := make([]uuid.UUID, total)
		rels := make([]*model.Relationship, total)
		for i := range total {
			tgtIDs[i] = createTestEntity(t, ctx, db, nsID, "t-"+uuid.NewString())
			rels[i] = &model.Relationship{
				NamespaceID: nsID, SourceID: srcID, TargetID: tgtIDs[i],
				Relation: "knows", Weight: 0.5,
			}
		}
		res, err := repo.BatchCreate(ctx, rels)
		if err != nil {
			t.Fatalf("BatchCreate(%d): %v", total, err)
		}
		if res.Affected != int64(total) {
			t.Fatalf("Affected = %d, want %d", res.Affected, total)
		}
		count, err := repo.CountActiveByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("CountActiveByNamespace: %v", err)
		}
		if count != total {
			t.Errorf("active count = %d, want %d", count, total)
		}
	})
}

func TestRelationshipRepo_BatchExpire(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		seed := make([]uuid.UUID, 4)
		ids := make([]uuid.UUID, 4)
		for i := range seed {
			tgt := createTestEntity(t, ctx, db, nsID, "t-"+uuid.NewString())
			rel := newTestRelationship(nsID, srcID, tgt)
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			ids[i] = rel.ID
			seed[i] = tgt
		}

		// Expire three of four, plus one bogus id (no error, no count).
		expire := []uuid.UUID{ids[0], ids[1], ids[2], uuid.New()}
		n, err := repo.BatchExpire(ctx, nsID, expire)
		if err != nil {
			t.Fatalf("BatchExpire: %v", err)
		}
		if n != 3 {
			t.Errorf("Affected = %d, want 3", n)
		}
		for i := range 3 {
			fetched, err := repo.GetByID(ctx, ids[i])
			if err != nil {
				t.Fatalf("re-read %d: %v", i, err)
			}
			if fetched.ValidUntil == nil {
				t.Errorf("row %d: ValidUntil unset", i)
			}
		}
		fetched, err := repo.GetByID(ctx, ids[3])
		if err != nil {
			t.Fatalf("re-read 3: %v", err)
		}
		if fetched.ValidUntil != nil {
			t.Errorf("row 3: ValidUntil set unexpectedly")
		}
	})
}

func TestRelationshipRepo_BatchExpire_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		n, err := repo.BatchExpire(ctx, nsID, nil)
		if err != nil {
			t.Fatalf("BatchExpire(nil): %v", err)
		}
		if n != 0 {
			t.Errorf("want 0, got %d", n)
		}
	})
}

func TestRelationshipRepo_BatchReinforce(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")

		seeds := make([]*model.Relationship, 3)
		startWeights := []float64{0.5, 1.9, 0.1}
		for i := range seeds {
			tgt := createTestEntity(t, ctx, db, nsID, "t-"+uuid.NewString())
			rel := newTestRelationship(nsID, srcID, tgt)
			rel.Weight = startWeights[i]
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			seeds[i] = rel
		}

		// Mix of deltas; row 1 should clamp at 2.0.
		items := []model.ReinforceItem{
			{ID: seeds[0].ID, Delta: 0.1}, // 0.5 -> 0.6
			{ID: seeds[1].ID, Delta: 0.5}, // 1.9 -> 2.0 (clamped)
			{ID: seeds[2].ID, Delta: 0.2}, // 0.1 -> 0.3
		}
		n, err := repo.BatchReinforce(ctx, nsID, items)
		if err != nil {
			t.Fatalf("BatchReinforce: %v", err)
		}
		if n != 3 {
			t.Errorf("Affected = %d, want 3", n)
		}

		want := []float64{0.6, 2.0, 0.3}
		for i := range seeds {
			fetched, err := repo.GetByID(ctx, seeds[i].ID)
			if err != nil {
				t.Fatalf("re-read %d: %v", i, err)
			}
			if absDiff(fetched.Weight, want[i]) > 1e-9 {
				t.Errorf("row %d: weight = %v, want %v", i, fetched.Weight, want[i])
			}
		}
	})
}

func TestRelationshipRepo_BatchUpdateWeight(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")

		seeds := make([]*model.Relationship, 3)
		for i := range seeds {
			tgt := createTestEntity(t, ctx, db, nsID, "t-"+uuid.NewString())
			rel := newTestRelationship(nsID, srcID, tgt)
			rel.Weight = 1.0
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			seeds[i] = rel
		}

		items := []model.WeightUpdateItem{
			{ID: seeds[0].ID, Weight: 0.25},
			{ID: seeds[1].ID, Weight: 1.5},
			{ID: seeds[2].ID, Weight: 0.75},
			{ID: uuid.New(), Weight: 0.42}, // missing id, no error
		}
		n, err := repo.BatchUpdateWeight(ctx, nsID, items)
		if err != nil {
			t.Fatalf("BatchUpdateWeight: %v", err)
		}
		if n != 3 {
			t.Errorf("Affected = %d, want 3", n)
		}
		want := []float64{0.25, 1.5, 0.75}
		for i := range seeds {
			fetched, err := repo.GetByID(ctx, seeds[i].ID)
			if err != nil {
				t.Fatalf("re-read %d: %v", i, err)
			}
			if absDiff(fetched.Weight, want[i]) > 1e-9 {
				t.Errorf("row %d: weight = %v, want %v", i, fetched.Weight, want[i])
			}
		}
	})
}

func TestRelationshipRepo_BatchDeleteByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		ids := make([]uuid.UUID, 3)
		for i := range ids {
			tgt := createTestEntity(t, ctx, db, nsID, "t-"+uuid.NewString())
			rel := newTestRelationship(nsID, srcID, tgt)
			if err := repo.Create(ctx, rel); err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			ids[i] = rel.ID
		}

		toDelete := []uuid.UUID{ids[0], ids[2], uuid.New()}
		n, err := repo.BatchDeleteByID(ctx, nsID, toDelete)
		if err != nil {
			t.Fatalf("BatchDeleteByID: %v", err)
		}
		if n != 2 {
			t.Errorf("Affected = %d, want 2", n)
		}
		if _, err := repo.GetByID(ctx, ids[0]); !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("row 0 should be deleted, got err=%v", err)
		}
		if _, err := repo.GetByID(ctx, ids[1]); err != nil {
			t.Errorf("row 1 should remain: %v", err)
		}
		if _, err := repo.GetByID(ctx, ids[2]); !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("row 2 should be deleted, got err=%v", err)
		}
	})
}

func TestRelationshipRepo_BatchCreate_CtxCancel(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx, cancel := context.WithCancel(context.Background())
		repo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)
		srcID := createTestEntity(t, ctx, db, nsID, "alice")
		tgtID := createTestEntity(t, ctx, db, nsID, "bob")

		rels := []*model.Relationship{
			{NamespaceID: nsID, SourceID: srcID, TargetID: tgtID, Relation: "knows", Weight: 0.5},
		}
		cancel()
		_, err := repo.BatchCreate(ctx, rels)
		if err == nil {
			t.Fatal("expected error from cancelled context")
		}
		count, err := repo.CountActiveByNamespace(context.Background(), nsID)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 0 {
			t.Errorf("rollback failed: active = %d, want 0", count)
		}
	})
}

func absDiff(a, b float64) float64 {
	d := a - b
	if d < 0 {
		return -d
	}
	return d
}

// avoid "imported and not used" warnings when only some tests reference these.
var _ = sync.Mutex{}
var _ = time.Now
