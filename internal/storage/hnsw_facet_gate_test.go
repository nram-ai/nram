package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/storage"
)

// TestHNSWStore_FacetScanGate proves the per-recall topic-facet brute-force scan
// is short-circuited when there is nothing to gain: when the namespace/dimension
// has no topic facets, and when the multi-vector feature is disabled. It observes
// the scan via ScanCount (test-only accessor) rather than results, because the
// short-circuit is a performance optimization with no effect on correct output.
func TestHNSWStore_FacetScanGate(t *testing.T) {
	ctx := context.Background()
	dim := 384
	axis := func(a int) []float32 { v := make([]float32, dim); v[a] = 1; return v }
	alwaysTTL := func() time.Duration { return time.Hour }

	newStore := func(t *testing.T, enabled bool) *storage.HNSWStore {
		db := setupHNSWTestDB(t)
		cfg := storage.DefaultHNSWConfig()
		cfg.SnapshotInterval = 1<<63 - 1
		store := storage.NewHNSWStore(db, db, cfg)
		t.Cleanup(func() { _ = store.Close() })
		store.SetFacetGate(func() bool { return enabled }, alwaysTTL)
		return store
	}

	t.Run("no topic facets: scan skipped", func(t *testing.T) {
		store := newStore(t, true)
		nsID := uuid.New()
		// Facet-0-only memory: no facet_id>0 rows exist for this namespace.
		if err := store.Upsert(ctx, storage.VectorKindMemory, uuid.New(), nsID, axis(5), dim); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if _, err := store.Search(ctx, storage.VectorKindMemory, axis(5), nsID, dim, 10); err != nil {
			t.Fatalf("Search: %v", err)
		}
		if got := store.ScanCount(); got != 0 {
			t.Fatalf("facet scan ran %d times with no topic facets, want 0 (short-circuited)", got)
		}
	})

	t.Run("topic facets present + feature on: scan runs", func(t *testing.T) {
		store := newStore(t, true)
		nsID := uuid.New()
		multi := uuid.New()
		if err := store.UpsertFacets(ctx, multi, nsID, dim, [][]float32{axis(0), axis(5)}); err != nil {
			t.Fatalf("UpsertFacets: %v", err)
		}
		results, err := store.Search(ctx, storage.VectorKindMemory, axis(5), nsID, dim, 10)
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if store.ScanCount() == 0 {
			t.Fatal("facet scan did not run despite present topic facets + feature on")
		}
		// Correctness: the axis-5 topic facet must still surface the memory.
		var found bool
		for _, r := range results {
			if r.ID == multi && r.Score > 0.99 {
				found = true
			}
		}
		if !found {
			t.Error("multi-facet memory not found at high score on its axis-5 topic facet")
		}
	})

	t.Run("topic facets present + feature off: scan skipped", func(t *testing.T) {
		store := newStore(t, false)
		nsID := uuid.New()
		multi := uuid.New()
		if err := store.UpsertFacets(ctx, multi, nsID, dim, [][]float32{axis(0), axis(5)}); err != nil {
			t.Fatalf("UpsertFacets: %v", err)
		}
		if _, err := store.Search(ctx, storage.VectorKindMemory, axis(5), nsID, dim, 10); err != nil {
			t.Fatalf("Search: %v", err)
		}
		if got := store.ScanCount(); got != 0 {
			t.Fatalf("facet scan ran %d times with the feature off, want 0 (gated)", got)
		}
	})

	// Writing facets must invalidate a cached "no facets" answer immediately, not
	// after the TTL. The store uses a 1-hour TTL, so only cache invalidation (not
	// expiry) can make the post-write recall run the scan and surface the facet.
	t.Run("writing facets invalidates the negative cache immediately", func(t *testing.T) {
		store := newStore(t, true)
		nsID := uuid.New()
		// First recall: namespace has no topic facets -> scan skipped, negative
		// answer cached under the 1-hour TTL.
		if err := store.Upsert(ctx, storage.VectorKindMemory, uuid.New(), nsID, axis(1), dim); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if _, err := store.Search(ctx, storage.VectorKindMemory, axis(5), nsID, dim, 10); err != nil {
			t.Fatalf("Search (pre-write): %v", err)
		}
		if store.ScanCount() != 0 {
			t.Fatalf("expected scan skipped before any topic facets, got %d", store.ScanCount())
		}
		// Add topic facets for a memory in this namespace.
		multi := uuid.New()
		if err := store.UpsertFacets(ctx, multi, nsID, dim, [][]float32{axis(0), axis(5)}); err != nil {
			t.Fatalf("UpsertFacets: %v", err)
		}
		// Next recall must run the scan now (invalidation), not wait out the TTL,
		// and surface the freshly-written topic facet.
		results, err := store.Search(ctx, storage.VectorKindMemory, axis(5), nsID, dim, 10)
		if err != nil {
			t.Fatalf("Search (post-write): %v", err)
		}
		if store.ScanCount() == 0 {
			t.Fatal("scan did not run after facets were written; the negative cache was not invalidated")
		}
		var found bool
		for _, r := range results {
			if r.ID == multi && r.Score > 0.99 {
				found = true
			}
		}
		if !found {
			t.Error("newly-written topic facet not surfaced on the very next recall")
		}
	})
}
