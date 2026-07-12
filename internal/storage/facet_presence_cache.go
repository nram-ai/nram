package storage

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// facetPresenceKey identifies a namespace/dimension pair for the topic-facet
// presence cache.
type facetPresenceKey struct {
	ns  uuid.UUID
	dim int
}

// facetPresenceEntry is a cached presence answer with the wall-clock time it was
// probed, used to age it out against the TTL.
type facetPresenceEntry struct {
	val bool
	at  time.Time
}

// facetPresenceCache memoizes, per namespace/dimension, whether any topic-facet
// (facet_id > 0) rows exist, so the faceted Search path can skip its per-recall
// facet work when there is nothing to gain. Entries age out after the injected
// TTL; a non-positive TTL disables caching entirely (every call re-probes). Safe
// for concurrent use.
type facetPresenceCache struct {
	ttlFn func() time.Duration
	mu    sync.Mutex
	items map[facetPresenceKey]facetPresenceEntry
}

func newFacetPresenceCache(ttlFn func() time.Duration) *facetPresenceCache {
	return &facetPresenceCache{ttlFn: ttlFn, items: make(map[facetPresenceKey]facetPresenceEntry)}
}

// Has reports whether the namespace/dimension holds any topic-facet rows,
// serving a cached answer while it is fresh and otherwise running probe and
// caching the result. A non-positive TTL skips both the cache read and write, so
// probe runs on every call. probe errors propagate to the caller.
func (c *facetPresenceCache) Has(ctx context.Context, ns uuid.UUID, dim int, probe func(context.Context) (bool, error)) (bool, error) {
	var ttl time.Duration
	if c.ttlFn != nil {
		ttl = c.ttlFn()
	}
	caching := ttl > 0
	key := facetPresenceKey{ns: ns, dim: dim}

	if caching {
		c.mu.Lock()
		if e, ok := c.items[key]; ok && time.Since(e.at) < ttl {
			c.mu.Unlock()
			return e.val, nil
		}
		c.mu.Unlock()
	}

	val, err := probe(ctx)
	if err != nil {
		return false, err
	}

	if caching {
		c.mu.Lock()
		c.items[key] = facetPresenceEntry{val: val, at: time.Now()}
		c.mu.Unlock()
	}
	return val, nil
}

// invalidate drops the cached presence answer for one namespace/dimension so the
// next Has re-probes. Called on facet writes so a namespace that just gained (or
// lost) topic facets is noticed immediately instead of after the TTL.
//
// Invalidation invariant: any write that can add a facet_id>0 row must invalidate
// (UpsertFacets does, on every backend). Writes that only remove facet rows
// (Delete, TruncateAllVectors) leave the affected key to age out via the TTL:
// TruncateAllVectors invalidates wholesale because it is cheap, but Delete does
// not — a stale "has facets" answer only keeps the gate active (the fail-safe
// direction: a result-neutral scan over now-zero rows) and Delete(kind, id)
// carries no namespace/dimension to key on.
func (c *facetPresenceCache) invalidate(ns uuid.UUID, dim int) {
	c.mu.Lock()
	delete(c.items, facetPresenceKey{ns: ns, dim: dim})
	c.mu.Unlock()
}

// invalidateAll clears every cached presence answer. Called on a bulk vector
// wipe so no namespace keeps a stale "has facets" answer afterward.
func (c *facetPresenceCache) invalidateAll() {
	c.mu.Lock()
	clear(c.items)
	c.mu.Unlock()
}

// facetGate decides, per Search, whether the faceted path should run. It folds
// the multi-vector feature switch (enabledFn) and the topic-facet presence probe
// (via the cache) into one answer, so a store skips facet work when the feature
// is off or the namespace/dimension has no topic facets. A nil *facetGate
// reports active — the pre-gate behavior — so stores and code paths that never
// wire a gate are unchanged.
type facetGate struct {
	enabledFn func() bool
	cache     *facetPresenceCache
}

func newFacetGate(enabledFn func() bool, ttlFn func() time.Duration) *facetGate {
	return &facetGate{enabledFn: enabledFn, cache: newFacetPresenceCache(ttlFn)}
}

// active reports whether the faceted Search path should run for this
// namespace/dimension. It is false when the feature is disabled (probe is
// skipped); otherwise it reflects whether any topic-facet rows exist. On a probe
// error it fails safe to true so a transient probe failure never silently drops
// facet results.
func (g *facetGate) active(ctx context.Context, ns uuid.UUID, dim int, probe func(context.Context) (bool, error)) bool {
	if g == nil {
		return true
	}
	if g.enabledFn != nil && !g.enabledFn() {
		return false
	}
	present, err := g.cache.Has(ctx, ns, dim, probe)
	if err != nil {
		slog.WarnContext(ctx, "storage: topic-facet presence probe failed; treating facets as present", "err", err, "dimension", dim)
		return true
	}
	return present
}

// featureEnabled reports whether the multi-vector feature switch is on. A nil
// gate or nil enabledFn reports true (pre-gate behavior). Search uses this to
// distinguish the two inactive reasons: a disabled feature means recall should
// behave as whole-memory-only (facet 0), whereas merely-absent topic facets
// means the facet path is a no-op. Kept separate from active() so the caller can
// apply a facet-0-only filter only when the feature is genuinely disabled.
func (g *facetGate) featureEnabled() bool {
	if g == nil || g.enabledFn == nil {
		return true
	}
	return g.enabledFn()
}

// invalidate drops the cached presence answer for one namespace/dimension.
// Nil-safe so unwired stores can call it unconditionally.
func (g *facetGate) invalidate(ns uuid.UUID, dim int) {
	if g == nil {
		return
	}
	g.cache.invalidate(ns, dim)
}

// invalidateAll clears the whole presence cache. Nil-safe.
func (g *facetGate) invalidateAll() {
	if g == nil {
		return
	}
	g.cache.invalidateAll()
}
