package storage

import (
	"context"
	"slices"
	"sort"
	"testing"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
)

// createTestNamespaceWithPath creates a namespace with an explicit path so the
// path-prefix scoping of ListReExtractCandidatesByIDs can be exercised.
func createTestNamespaceWithPath(t *testing.T, ctx context.Context, db DB, path string) uuid.UUID {
	t.Helper()
	nsRepo := NewNamespaceRepo(db)
	nsID := uuid.New()
	ns := &model.Namespace{
		ID:       nsID,
		Name:     "NS " + nsID.String()[:8],
		Slug:     "ns-" + nsID.String()[:8],
		Kind:     "org",
		ParentID: &rootID,
		Path:     path,
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace path=%s: %v", path, err)
	}
	return nsID
}

func candidateIDs(cands []BackfillCandidate) []string {
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.ID.String()
	}
	sort.Strings(out)
	return out
}

func wantIDs(ids ...uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	sort.Strings(out)
	return out
}

// TestMemoryRepo_ListReExtractCandidatesByIDs verifies the per-memory re-extract
// candidate filter: only enriched, live, in-scope memories are returned, and the
// namespace path prefix drops out-of-scope IDs (empty prefix = global).
func TestMemoryRepo_ListReExtractCandidatesByIDs(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)

		nsA := createTestNamespaceWithPath(t, ctx, db, "tenantA")
		nsAsub := createTestNamespaceWithPath(t, ctx, db, "tenantA/sub")
		nsB := createTestNamespaceWithPath(t, ctx, db, "tenantB")

		mk := func(ns uuid.UUID, enriched, deleted bool) uuid.UUID {
			m := newTestMemory(ns)
			if err := repo.Create(ctx, m); err != nil {
				t.Fatalf("create memory: %v", err)
			}
			if enriched {
				if err := repo.MarkEnriched(ctx, m.ID, ns, nil, nil, nil, nil, nil); err != nil {
					t.Fatalf("mark enriched: %v", err)
				}
			}
			if deleted {
				if err := repo.SoftDelete(ctx, m.ID, ns); err != nil {
					t.Fatalf("soft delete: %v", err)
				}
			}
			return m.ID
		}

		inA := mk(nsA, true, false)
		inSub := mk(nsAsub, true, false)
		inB := mk(nsB, true, false)
		notEnriched := mk(nsA, false, false)
		deleted := mk(nsA, true, true)

		// A consolidation dream in nsA: origin=dream plus a synthesized_from
		// lineage row. The bulk ListReExtractCandidates excludes both, but the
		// explicit-ID path must honor an operator's hand-picked dream, since
		// consolidation dreams do get entity extraction.
		dream := newTestMemory(nsA)
		dream.Origin = model.OriginDream
		if err := repo.Create(ctx, dream); err != nil {
			t.Fatalf("create dream: %v", err)
		}
		if err := repo.MarkEnriched(ctx, dream.ID, nsA, nil, nil, nil, nil, nil); err != nil {
			t.Fatalf("mark dream enriched: %v", err)
		}
		dreamParent := inA
		lineageRepo := NewMemoryLineageRepo(db)
		if err := lineageRepo.Create(ctx, &model.MemoryLineage{
			NamespaceID: nsA,
			MemoryID:    dream.ID,
			ParentID:    &dreamParent,
			Relation:    model.LineageSynthesizedFrom,
		}); err != nil {
			t.Fatalf("create dream lineage: %v", err)
		}

		all := []uuid.UUID{inA, inSub, inB, notEnriched, deleted, dream.ID}

		// Scope "tenantA": inA, its descendant inSub, and the dream are eligible;
		// inB is out of scope; notEnriched and deleted are ineligible.
		got, err := repo.ListReExtractCandidatesByIDs(ctx, "tenantA", all)
		if err != nil {
			t.Fatalf("by ids tenantA: %v", err)
		}
		if g, w := candidateIDs(got), wantIDs(inA, inSub, dream.ID); !slices.Equal(g, w) {
			t.Fatalf("tenantA scope: got %v want %v", g, w)
		}

		// Global scope (empty prefix): all enriched, live IDs regardless of
		// namespace (the dream included); ineligible still dropped.
		gotGlobal, err := repo.ListReExtractCandidatesByIDs(ctx, "", all)
		if err != nil {
			t.Fatalf("by ids global: %v", err)
		}
		if g, w := candidateIDs(gotGlobal), wantIDs(inA, inSub, inB, dream.ID); !slices.Equal(g, w) {
			t.Fatalf("global scope: got %v want %v", g, w)
		}

		// Empty input is a no-op.
		if c, err := repo.ListReExtractCandidatesByIDs(ctx, "", nil); err != nil || len(c) != 0 {
			t.Fatalf("empty ids: got %d candidates err=%v", len(c), err)
		}
	})
}
