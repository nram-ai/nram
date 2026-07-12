package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFacetPresenceCache_CachesWithinTTL(t *testing.T) {
	c := newFacetPresenceCache(func() time.Duration { return time.Hour })
	ns := uuid.New()
	calls := 0
	probe := func(context.Context) (bool, error) { calls++; return true, nil }

	for range 5 {
		got, err := c.Has(context.Background(), ns, 384, probe)
		if err != nil || !got {
			t.Fatalf("Has: got (%v, %v), want (true, nil)", got, err)
		}
	}
	if calls != 1 {
		t.Fatalf("probe ran %d times within TTL, want 1", calls)
	}
}

func TestFacetPresenceCache_ReprobesAfterExpiry(t *testing.T) {
	c := newFacetPresenceCache(func() time.Duration { return 10 * time.Millisecond })
	ns := uuid.New()
	calls := 0
	probe := func(context.Context) (bool, error) { calls++; return true, nil }

	if _, err := c.Has(context.Background(), ns, 384, probe); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := c.Has(context.Background(), ns, 384, probe); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("probe ran %d times across an expiry, want 2", calls)
	}
}

func TestFacetPresenceCache_ZeroTTLProbesEveryCall(t *testing.T) {
	c := newFacetPresenceCache(func() time.Duration { return 0 })
	ns := uuid.New()
	calls := 0
	probe := func(context.Context) (bool, error) { calls++; return false, nil }

	for range 3 {
		if _, err := c.Has(context.Background(), ns, 384, probe); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 {
		t.Fatalf("probe ran %d times with TTL 0, want 3 (uncached)", calls)
	}
}

func TestFacetPresenceCache_KeyedByNamespaceAndDimension(t *testing.T) {
	c := newFacetPresenceCache(func() time.Duration { return time.Hour })
	nsA, nsB := uuid.New(), uuid.New()
	calls := 0
	probe := func(context.Context) (bool, error) { calls++; return true, nil }

	_, _ = c.Has(context.Background(), nsA, 384, probe)
	_, _ = c.Has(context.Background(), nsA, 768, probe) // same ns, different dim
	_, _ = c.Has(context.Background(), nsB, 384, probe) // different ns
	_, _ = c.Has(context.Background(), nsA, 384, probe) // repeat, cached
	if calls != 3 {
		t.Fatalf("probe ran %d times, want 3 (one per distinct ns/dim)", calls)
	}
}

func TestFacetPresenceCache_Invalidate(t *testing.T) {
	c := newFacetPresenceCache(func() time.Duration { return time.Hour })
	ns := uuid.New()
	calls := 0
	probe := func(context.Context) (bool, error) { calls++; return true, nil }

	if _, err := c.Has(context.Background(), ns, 384, probe); err != nil {
		t.Fatal(err)
	}
	c.invalidate(ns, 384)
	if _, err := c.Has(context.Background(), ns, 384, probe); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("probe ran %d times across an invalidate, want 2 (cache dropped)", calls)
	}
}

func TestFacetPresenceCache_InvalidateAll(t *testing.T) {
	c := newFacetPresenceCache(func() time.Duration { return time.Hour })
	nsA, nsB := uuid.New(), uuid.New()
	calls := 0
	probe := func(context.Context) (bool, error) { calls++; return true, nil }

	_, _ = c.Has(context.Background(), nsA, 384, probe)
	_, _ = c.Has(context.Background(), nsB, 384, probe)
	c.invalidateAll()
	_, _ = c.Has(context.Background(), nsA, 384, probe)
	_, _ = c.Has(context.Background(), nsB, 384, probe)
	if calls != 4 {
		t.Fatalf("probe ran %d times across invalidateAll, want 4 (both entries dropped)", calls)
	}
}

func TestFacetGate_InvalidateNilSafe(t *testing.T) {
	var g *facetGate
	// Must not panic on an unwired store.
	g.invalidate(uuid.New(), 384)
	g.invalidateAll()
}

// TestFacetPresenceCache_ConcurrentAccess hammers the cache with the access
// pattern it sees in production — concurrent Has (read + probe-and-store),
// invalidate, and invalidateAll over both a shared key and distinct keys — so
// the race detector can prove the locking is sound. Correctness is not asserted
// (the interleaving is nondeterministic); the point is a clean `-race` run.
func TestFacetPresenceCache_ConcurrentAccess(t *testing.T) {
	c := newFacetPresenceCache(func() time.Duration { return time.Millisecond })
	shared := uuid.New()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			own := uuid.New()
			for j := range 200 {
				_, _ = c.Has(context.Background(), own, 384, func(context.Context) (bool, error) { return j%2 == 0, nil })
				_, _ = c.Has(context.Background(), shared, 768, func(context.Context) (bool, error) { return true, nil })
				if j%5 == 0 {
					c.invalidate(shared, 768)
				}
				if j%50 == 0 {
					c.invalidateAll()
				}
			}
		})
	}
	wg.Wait()
}

func TestEffectiveOverFetch(t *testing.T) {
	ctx := context.Background()
	ns := uuid.New()
	maxFacets := func() int { return 8 }
	present := func(context.Context) (bool, error) { return true, nil }
	absent := func(context.Context) (bool, error) { return false, nil }
	ttl := func() time.Duration { return time.Hour }
	want := overFetchFor(maxFacets)

	on := newFacetGate(func() bool { return true }, ttl)
	if got, whole := effectiveOverFetch(ctx, on, ns, 384, maxFacets, present); got != want || whole {
		t.Errorf("feature on + facets present: got (%d, %v), want (%d, false)", got, whole, want)
	}
	if got, whole := effectiveOverFetch(ctx, on, uuid.New(), 384, maxFacets, absent); got != 1 || whole {
		t.Errorf("feature on + no facets: got (%d, %v), want (1, false)", got, whole)
	}
	off := newFacetGate(func() bool { return false }, ttl)
	if got, whole := effectiveOverFetch(ctx, off, ns, 384, maxFacets, present); got != 1 || !whole {
		t.Errorf("feature off: got (%d, %v), want (1, true) — whole-memory-only", got, whole)
	}
	if got, whole := effectiveOverFetch(ctx, nil, ns, 384, maxFacets, present); got != want || whole {
		t.Errorf("nil gate (pre-gate behavior): got (%d, %v), want (%d, false)", got, whole, want)
	}
}

func TestFacetGate_FeatureEnabled(t *testing.T) {
	ttl := func() time.Duration { return time.Hour }
	if on := newFacetGate(func() bool { return true }, ttl); !on.featureEnabled() {
		t.Error("feature on: featureEnabled should be true")
	}
	if off := newFacetGate(func() bool { return false }, ttl); off.featureEnabled() {
		t.Error("feature off: featureEnabled should be false")
	}
	var nilGate *facetGate
	if !nilGate.featureEnabled() {
		t.Error("nil gate: featureEnabled should be true (pre-gate behavior)")
	}
	if g := newFacetGate(nil, ttl); !g.featureEnabled() {
		t.Error("nil enabledFn: featureEnabled should be true (pre-gate behavior)")
	}
}

func TestFacetGate_Active(t *testing.T) {
	ns := uuid.New()
	ctx := context.Background()
	present := func(context.Context) (bool, error) { return true, nil }
	absent := func(context.Context) (bool, error) { return false, nil }
	boom := func(context.Context) (bool, error) { return false, errors.New("probe failed") }
	ttl := func() time.Duration { return time.Hour }

	t.Run("nil gate is active", func(t *testing.T) {
		var g *facetGate
		if !g.active(ctx, ns, 384, present) {
			t.Fatal("nil gate should report active (pre-gate behavior)")
		}
	})

	t.Run("feature off is inactive and does not probe", func(t *testing.T) {
		probed := false
		probe := func(c context.Context) (bool, error) { probed = true; return true, nil }
		g := newFacetGate(func() bool { return false }, ttl)
		if g.active(ctx, ns, 384, probe) {
			t.Fatal("feature off should report inactive")
		}
		if probed {
			t.Fatal("feature off must not run the presence probe")
		}
	})

	t.Run("feature on with facets present is active", func(t *testing.T) {
		g := newFacetGate(func() bool { return true }, ttl)
		if !g.active(ctx, ns, 384, present) {
			t.Fatal("feature on + facets present should report active")
		}
	})

	t.Run("feature on with no facets is inactive", func(t *testing.T) {
		g := newFacetGate(func() bool { return true }, ttl)
		if g.active(ctx, ns, 384, absent) {
			t.Fatal("feature on + no facets should report inactive")
		}
	})

	t.Run("nil enabledFn treats feature as on", func(t *testing.T) {
		g := newFacetGate(nil, ttl)
		if !g.active(ctx, ns, 384, present) {
			t.Fatal("nil enabledFn should behave as feature on")
		}
	})

	t.Run("probe error fails safe to active", func(t *testing.T) {
		g := newFacetGate(func() bool { return true }, ttl)
		if !g.active(ctx, ns, 384, boom) {
			t.Fatal("probe error should fail safe to active")
		}
	})
}
