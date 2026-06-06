package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestEnsureReservedUnderUser_CreatesHealsIdempotent verifies that
// EnsureReservedUnderUser provisions every reserved project with canonical
// copy, heals a drifted (empty-description) row in place, and is idempotent.
func TestEnsureReservedUnderUser_CreatesHealsIdempotent(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		user := createTestUser(t, ctx, db) // created with nil projectRepo: no reserved projects yet
		nsRepo := NewNamespaceRepo(db)
		projectRepo := NewProjectRepo(db)

		// First call: creates both reserved projects with canonical name/description.
		if err := projectRepo.EnsureReservedUnderUser(ctx, nsRepo, user.NamespaceID); err != nil {
			t.Fatalf("ensure (create): %v", err)
		}
		for _, rp := range model.ReservedProjects {
			p, err := projectRepo.GetBySlug(ctx, user.NamespaceID, rp.Slug)
			if err != nil {
				t.Fatalf("expected reserved project %q created: %v", rp.Slug, err)
			}
			if p.Name != rp.Name || p.Description != rp.Description {
				t.Errorf("%q not canonical after create: name=%q desc=%q", rp.Slug, p.Name, p.Description)
			}
			if !p.Reserved {
				t.Errorf("%q should report Reserved=true", rp.Slug)
			}
		}

		// Drift the global row (simulate a pre-description legacy global), then heal.
		g, err := projectRepo.GetBySlug(ctx, user.NamespaceID, model.ReservedProjectSlugGlobal)
		if err != nil {
			t.Fatalf("get global: %v", err)
		}
		g.Description = ""
		g.Name = "global"
		if err := projectRepo.Update(ctx, g); err != nil {
			t.Fatalf("drift update: %v", err)
		}

		if err := projectRepo.EnsureReservedUnderUser(ctx, nsRepo, user.NamespaceID); err != nil {
			t.Fatalf("ensure (heal): %v", err)
		}
		want := model.ReservedProjectBySlug(model.ReservedProjectSlugGlobal)
		healed, err := projectRepo.GetBySlug(ctx, user.NamespaceID, model.ReservedProjectSlugGlobal)
		if err != nil {
			t.Fatalf("get healed global: %v", err)
		}
		if healed.Description != want.Description {
			t.Errorf("global description not healed: got %q", healed.Description)
		}

		// Idempotent: a third call leaves exactly len(ReservedProjects) reserved rows.
		if err := projectRepo.EnsureReservedUnderUser(ctx, nsRepo, user.NamespaceID); err != nil {
			t.Fatalf("ensure (idempotent): %v", err)
		}
		countQ := "SELECT COUNT(*) FROM projects WHERE owner_namespace_id = ? AND slug IN (?, ?)"
		if db.Backend() == BackendPostgres {
			countQ = "SELECT COUNT(*) FROM projects WHERE owner_namespace_id = $1 AND slug IN ($2, $3)"
		}
		var n int
		if err := db.QueryRow(ctx, countQ, user.NamespaceID.String(),
			model.ReservedProjectSlugGlobal, model.ReservedProjectSlugAboutMe).Scan(&n); err != nil {
			t.Fatalf("count reserved: %v", err)
		}
		if n != len(model.ReservedProjects) {
			t.Errorf("expected %d reserved projects, found %d", len(model.ReservedProjects), n)
		}
	})
}

// TestListByNamespaceFramingOrder_CompositeOrder verifies the about_me framing
// order: identity-centrality (max linked-entity mention_count) first, then
// recall-count (access_count), then recency.
func TestListByNamespaceFramingOrder_CompositeOrder(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestMemoryNamespace(t, ctx, db)
		memRepo := NewMemoryRepo(db)
		entRepo := NewEntityRepo(db)
		relRepo := NewRelationshipRepo(db)

		base := time.Now().Add(-time.Hour).UTC()
		mk := func(content string, access int, createdOffset time.Duration) *model.Memory {
			m := &model.Memory{
				NamespaceID: nsID,
				Content:     content,
				Confidence:  0.9,
				Importance:  0.5,
				AccessCount: access,
				CreatedAt:   base.Add(createdOffset),
				UpdatedAt:   base.Add(createdOffset),
			}
			if err := memRepo.Create(ctx, m); err != nil {
				t.Fatalf("create memory %q: %v", content, err)
			}
			return m
		}

		// A shared low-mention target keeps MAX() anchored on each source entity.
		target := &model.Entity{NamespaceID: nsID, Name: "shared-target", Canonical: "shared-target", EntityType: "concept", MentionCount: 1}
		if err := entRepo.Create(ctx, target); err != nil {
			t.Fatalf("create target entity: %v", err)
		}
		link := func(memID uuid.UUID, mention int, name string) {
			src := &model.Entity{NamespaceID: nsID, Name: name, Canonical: name, EntityType: "concept", MentionCount: mention}
			if err := entRepo.Create(ctx, src); err != nil {
				t.Fatalf("create entity %q: %v", name, err)
			}
			rel := &model.Relationship{
				NamespaceID:  nsID,
				SourceID:     src.ID,
				TargetID:     target.ID,
				Relation:     "rel",
				Weight:       1.0,
				ValidFrom:    time.Now().UTC(),
				SourceMemory: &memID,
			}
			if err := relRepo.Create(ctx, rel); err != nil {
				t.Fatalf("create relationship: %v", err)
			}
		}

		// A: top mention. B,C: equal mid mention (B has more recalls than C).
		// D: no linked entities (mention 0) but the highest access_count — must
		// still sort last because mention dominates.
		memA := mk("A high mention", 0, 4*time.Minute)
		memB := mk("B mid mention, more recalls", 5, 3*time.Minute)
		memC := mk("C mid mention, fewer recalls", 1, 2*time.Minute)
		memD := mk("D no entities, high access", 50, 1*time.Minute)

		link(memA.ID, 100, "alpha")
		link(memB.ID, 10, "beta")
		link(memC.ID, 10, "gamma")
		// memD intentionally unlinked.

		got, err := memRepo.ListByNamespaceFramingOrder(ctx, nsID, 50, 0)
		if err != nil {
			t.Fatalf("framing order: %v", err)
		}
		wantOrder := []uuid.UUID{memA.ID, memB.ID, memC.ID, memD.ID}
		if len(got) != len(wantOrder) {
			t.Fatalf("expected %d memories, got %d", len(wantOrder), len(got))
		}
		for i, want := range wantOrder {
			if got[i].ID != want {
				t.Errorf("position %d: want %q, got %q", i, want, got[i].Content)
			}
		}
	})
}
