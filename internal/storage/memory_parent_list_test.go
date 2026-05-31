package storage

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// seedParentFamily inserts one parent memory and `childCount` extracted-fact
// children linked via memory_lineage. Returns the parent and children in
// insertion order. Children are tagged with `child-tag` and contain
// "extracted-text" in their content so descendant-filter tests have a
// signal that lives only on children.
func seedParentFamily(
	t *testing.T,
	ctx context.Context,
	memRepo *MemoryRepo,
	lineageRepo *MemoryLineageRepo,
	nsID uuid.UUID,
	parentLabel string,
	parentTags []string,
	childCount int,
) (*model.Memory, []*model.Memory) {
	t.Helper()

	src := "test-parent"
	parent := &model.Memory{
		NamespaceID: nsID,
		Content:     "Parent content for " + parentLabel,
		Source:      &src,
		Tags:        parentTags,
		Confidence:  0.9,
		Importance:  0.8,
	}
	if err := memRepo.Create(ctx, parent); err != nil {
		t.Fatalf("create parent %s: %v", parentLabel, err)
	}
	// Stagger so children are strictly newer than their parent in created_at.
	time.Sleep(1100 * time.Millisecond)

	children := make([]*model.Memory, 0, childCount)
	for i := range childCount {
		childSrc := "enrichment-worker"
		child := &model.Memory{
			NamespaceID: nsID,
			Content:     "extracted-text fact from " + parentLabel,
			Source:      &childSrc,
			Tags:        []string{"child-tag", parentLabel + "-child"},
			Confidence:  0.85,
			Importance:  0.6,
			Enriched:    true,
		}
		if err := memRepo.Create(ctx, child); err != nil {
			t.Fatalf("create child %d for %s: %v", i, parentLabel, err)
		}
		lineage := &model.MemoryLineage{
			NamespaceID: nsID,
			MemoryID:    child.ID,
			ParentID:    &parent.ID,
			Relation:    model.LineageExtractedFact,
		}
		if err := lineageRepo.Create(ctx, lineage); err != nil {
			t.Fatalf("create lineage for child %d of %s: %v", i, parentLabel, err)
		}
		children = append(children, child)
		time.Sleep(1100 * time.Millisecond)
	}

	return parent, children
}

func TestMemoryRepo_ListParentsByNamespaceFiltered_ExcludesChildren(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		lineageRepo := NewMemoryLineageRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		// Two parents, one with 3 children, one with 0.
		_, _ = seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "alpha", []string{"topic-a"}, 3)
		_, _ = seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "beta", []string{"topic-b"}, 0)

		// Without group_by_parent semantics — flat list returns 2 parents + 3
		// children = 5 rows.
		flat, err := memRepo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 100, 0)
		if err != nil {
			t.Fatalf("flat list: %v", err)
		}
		if len(flat) != 5 {
			t.Fatalf("expected 5 flat rows, got %d", len(flat))
		}

		// Parent-anchored: only the 2 parents.
		parents, err := memRepo.ListParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 100, 0)
		if err != nil {
			t.Fatalf("parent list: %v", err)
		}
		if len(parents) != 2 {
			t.Fatalf("expected 2 parents, got %d", len(parents))
		}

		// Count agrees with list.
		count, err := memRepo.CountParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{})
		if err != nil {
			t.Fatalf("count parents: %v", err)
		}
		if count != 2 {
			t.Fatalf("expected count 2, got %d", count)
		}
	})
}

func TestMemoryRepo_ListParentsByNamespaceFiltered_DescendantTagSurfacesParent(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		lineageRepo := NewMemoryLineageRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		// Parent has tag "topic-a" only; children have "child-tag".
		parent, _ := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "alpha", []string{"topic-a"}, 2)
		// Second parent has neither tag (and no children).
		_, _ = seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "beta", []string{"unrelated"}, 0)

		// Filter by a tag that only exists on children. Parent-anchored mode
		// should still surface alpha because at least one child matches.
		parents, err := memRepo.ListParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Tags: []string{"child-tag"},
		}, 100, 0)
		if err != nil {
			t.Fatalf("parent list with child-tag filter: %v", err)
		}
		if len(parents) != 1 {
			t.Fatalf("expected 1 matching parent (alpha), got %d", len(parents))
		}
		if parents[0].ID != parent.ID {
			t.Fatalf("expected alpha parent %s, got %s", parent.ID, parents[0].ID)
		}

		// Filter by a tag on the parent itself — also matches.
		parents, err = memRepo.ListParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Tags: []string{"topic-a"},
		}, 100, 0)
		if err != nil {
			t.Fatalf("parent list with parent-tag filter: %v", err)
		}
		if len(parents) != 1 {
			t.Fatalf("expected 1 matching parent on topic-a, got %d", len(parents))
		}

		// Filter by content that only lives on children.
		parents, err = memRepo.ListParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Search: "extracted-text",
		}, 100, 0)
		if err != nil {
			t.Fatalf("parent list with descendant search: %v", err)
		}
		if len(parents) != 1 {
			t.Fatalf("expected 1 parent surfaced via child content, got %d", len(parents))
		}
	})
}

func TestMemoryRepo_ListParentsByNamespaceFiltered_OrderAndPagination(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		lineageRepo := NewMemoryLineageRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		alpha, _ := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "alpha", []string{"a"}, 1)
		beta, _ := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "beta", []string{"b"}, 1)
		gamma, _ := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "gamma", []string{"g"}, 0)

		// DESC order: gamma (newest) → beta → alpha.
		first, err := memRepo.ListParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 2, 0)
		if err != nil {
			t.Fatalf("page 1: %v", err)
		}
		if len(first) != 2 {
			t.Fatalf("page 1 expected 2 rows, got %d", len(first))
		}
		if first[0].ID != gamma.ID || first[1].ID != beta.ID {
			t.Fatalf("page 1 unexpected order: got %s, %s", first[0].ID, first[1].ID)
		}

		second, err := memRepo.ListParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 2, 2)
		if err != nil {
			t.Fatalf("page 2: %v", err)
		}
		if len(second) != 1 {
			t.Fatalf("page 2 expected 1 row, got %d", len(second))
		}
		if second[0].ID != alpha.ID {
			t.Fatalf("page 2 expected alpha, got %s", second[0].ID)
		}
	})
}

func TestMemoryRepo_FindChildrenByParents_BatchAndOrder(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		lineageRepo := NewMemoryLineageRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		alpha, alphaKids := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "alpha", []string{"a"}, 3)
		beta, betaKids := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "beta", []string{"b"}, 1)
		gamma, _ := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "gamma", []string{"g"}, 0)

		buckets, err := memRepo.FindChildrenByParents(ctx, nsID, []uuid.UUID{alpha.ID, beta.ID, gamma.ID}, ExtractedChildRelations)
		if err != nil {
			t.Fatalf("find children: %v", err)
		}
		if len(buckets) != 2 {
			t.Fatalf("expected 2 buckets (alpha, beta), got %d", len(buckets))
		}
		if got := buckets[alpha.ID]; len(got) != 3 {
			t.Fatalf("alpha bucket expected 3, got %d", len(got))
		}
		if got := buckets[beta.ID]; len(got) != 1 {
			t.Fatalf("beta bucket expected 1, got %d", len(got))
		}
		// Gamma has no children — bucket absent (caller treats as zero).
		if _, ok := buckets[gamma.ID]; ok {
			t.Fatalf("gamma should have no children bucket")
		}

		// Within the alpha bucket, children come back created_at DESC, so the
		// last-inserted alpha child appears first.
		got := buckets[alpha.ID]
		if got[0].ID != alphaKids[len(alphaKids)-1].ID {
			t.Fatalf("alpha bucket order: expected last child first, got %s", got[0].ID)
		}

		// Each child carries ParentID set so JSON consumers see the link.
		for _, kid := range got {
			if kid.ParentID == nil || *kid.ParentID != alpha.ID {
				t.Fatalf("child %s missing ParentID=%s, got %v", kid.ID, alpha.ID, kid.ParentID)
			}
		}

		// Beta child also has ParentID populated.
		if buckets[beta.ID][0].ParentID == nil || *buckets[beta.ID][0].ParentID != beta.ID {
			t.Fatalf("beta child missing ParentID")
		}
		_ = betaKids
	})
}

func TestMemoryRepo_FindChildrenByParents_EmptyInputs(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		buckets, err := memRepo.FindChildrenByParents(ctx, nsID, nil, ExtractedChildRelations)
		if err != nil {
			t.Fatalf("nil parents: %v", err)
		}
		if len(buckets) != 0 {
			t.Fatalf("expected empty result, got %d buckets", len(buckets))
		}

		buckets, err = memRepo.FindChildrenByParents(ctx, nsID, []uuid.UUID{uuid.New()}, nil)
		if err != nil {
			t.Fatalf("nil relations: %v", err)
		}
		if len(buckets) != 0 {
			t.Fatalf("expected empty result with no relations, got %d buckets", len(buckets))
		}
	})
}

func TestMemoryRepo_ListParentsByNamespaceFiltered_StatusFiltersMatchParentOnly(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		lineageRepo := NewMemoryLineageRepo(db)

		stamp := time.Now().UTC()
		yes, no := true, false

		// seed creates a fresh namespace holding one parent and one extracted-
		// fact child, each with independently controlled status fields, and
		// returns the namespace and parent. The child is linked via
		// LineageExtractedFact so it is never itself a parent-anchored row;
		// it exists only to prove a child's status cannot drag its parent in.
		seed := func(
			label string,
			parentAug, childAug *time.Time,
			parentEnriched, childEnriched bool,
			parentOrigin, childOrigin model.MemoryOrigin,
		) (uuid.UUID, uuid.UUID) {
			nsID := createTestMemoryNamespace(t, ctx, db)
			src := "test-parent"
			parent := &model.Memory{
				NamespaceID:          nsID,
				Content:              "parent " + label,
				Source:               &src,
				Confidence:           0.9,
				Importance:           0.8,
				Enriched:             parentEnriched,
				Origin:               parentOrigin,
				AugmentedEmbeddingAt: parentAug,
			}
			if err := memRepo.Create(ctx, parent); err != nil {
				t.Fatalf("create parent %s: %v", label, err)
			}
			childSrc := "enrichment-worker"
			child := &model.Memory{
				NamespaceID:          nsID,
				Content:              "child " + label,
				Source:               &childSrc,
				Confidence:           0.85,
				Importance:           0.6,
				Enriched:             childEnriched,
				Origin:               childOrigin,
				AugmentedEmbeddingAt: childAug,
			}
			if err := memRepo.Create(ctx, child); err != nil {
				t.Fatalf("create child %s: %v", label, err)
			}
			lineage := &model.MemoryLineage{
				NamespaceID: nsID,
				MemoryID:    child.ID,
				ParentID:    &parent.ID,
				Relation:    model.LineageExtractedFact,
			}
			if err := lineageRepo.Create(ctx, lineage); err != nil {
				t.Fatalf("create lineage %s: %v", label, err)
			}
			return nsID, parent.ID
		}

		// assertParents asserts both the list and the count return exactly the
		// expected parent IDs (order-independent) for the given filter.
		assertParents := func(name string, nsID uuid.UUID, f MemoryListFilters, wantIDs ...uuid.UUID) {
			t.Helper()
			got, err := memRepo.ListParentsByNamespaceFiltered(ctx, nsID, f, 100, 0)
			if err != nil {
				t.Fatalf("%s: list: %v", name, err)
			}
			if len(got) != len(wantIDs) {
				t.Fatalf("%s: expected %d parents, got %d", name, len(wantIDs), len(got))
			}
			for _, want := range wantIDs {
				if !slices.ContainsFunc(got, func(m model.Memory) bool { return m.ID == want }) {
					t.Fatalf("%s: expected parent %s in results", name, want)
				}
			}
			count, err := memRepo.CountParentsByNamespaceFiltered(ctx, nsID, f)
			if err != nil {
				t.Fatalf("%s: count: %v", name, err)
			}
			if count != len(wantIDs) {
				t.Fatalf("%s: count %d disagrees with list length %d", name, count, len(wantIDs))
			}
		}

		// --- Augmented ---
		// Augmented parent + not-augmented child: must NOT leak into "not
		// augmented"; must appear under "augmented".
		nsAug, augParent := seed("aug", &stamp, nil, false, false, model.OriginUser, model.OriginUser)
		assertParents("augmented parent / not_augmented filter", nsAug, MemoryListFilters{Augmented: &no})
		assertParents("augmented parent / augmented filter", nsAug, MemoryListFilters{Augmented: &yes}, augParent)

		// Not-augmented parent + augmented child: inverse.
		nsNotAug, notAugParent := seed("notaug", nil, &stamp, false, false, model.OriginUser, model.OriginUser)
		assertParents("not-augmented parent / not_augmented filter", nsNotAug, MemoryListFilters{Augmented: &no}, notAugParent)
		assertParents("not-augmented parent / augmented filter", nsNotAug, MemoryListFilters{Augmented: &yes})

		// --- Enriched ---
		nsEnr, enrParent := seed("enr", nil, nil, true, false, model.OriginUser, model.OriginUser)
		assertParents("enriched parent / not_enriched filter", nsEnr, MemoryListFilters{Enriched: &no})
		assertParents("enriched parent / enriched filter", nsEnr, MemoryListFilters{Enriched: &yes}, enrParent)

		nsNotEnr, notEnrParent := seed("notenr", nil, nil, false, true, model.OriginUser, model.OriginUser)
		assertParents("not-enriched parent / not_enriched filter", nsNotEnr, MemoryListFilters{Enriched: &no}, notEnrParent)
		assertParents("not-enriched parent / enriched filter", nsNotEnr, MemoryListFilters{Enriched: &yes})

		// --- Origin ---
		nsDream, dreamParent := seed("dream", nil, nil, false, false, model.OriginDream, model.OriginUser)
		assertParents("dream parent / origin=user filter", nsDream, MemoryListFilters{Origin: string(model.OriginUser)})
		assertParents("dream parent / origin=dream filter", nsDream, MemoryListFilters{Origin: string(model.OriginDream)}, dreamParent)

		nsUser, userParent := seed("user", nil, nil, false, false, model.OriginUser, model.OriginDream)
		assertParents("user parent / origin=dream filter", nsUser, MemoryListFilters{Origin: string(model.OriginDream)})
		assertParents("user parent / origin=user filter", nsUser, MemoryListFilters{Origin: string(model.OriginUser)}, userParent)
	})
}

func TestMemoryRepo_ListParentsByNamespaceFiltered_HidesSoftDeletedChildren(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		lineageRepo := NewMemoryLineageRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		_, kids := seedParentFamily(t, ctx, memRepo, lineageRepo, nsID, "alpha", []string{"topic-a"}, 2)

		// Soft-delete one child.
		if err := memRepo.SoftDelete(ctx, kids[0].ID, nsID); err != nil {
			t.Fatalf("soft delete child: %v", err)
		}

		// Filter by content unique to the surviving children — parent should
		// still surface because at least one child still matches.
		parents, err := memRepo.ListParentsByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Search: "extracted-text",
		}, 100, 0)
		if err != nil {
			t.Fatalf("parent list: %v", err)
		}
		if len(parents) != 1 {
			t.Fatalf("expected 1 parent with surviving child, got %d", len(parents))
		}
	})
}
