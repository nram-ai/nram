package provider

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
)

// hostGateMaxLimit caps the per-host in-flight permits a single gate can hold.
// It sizes the backing permit channel, so a configured limit above it is
// clamped rather than panicking. Far above any sane single-host concurrency.
const hostGateMaxLimit = 1024

// hostGate is a resizable counting semaphore bounding how many requests may be
// in flight against one upstream host at once. It is shared across every
// provider slot and subsystem that targets the same host, so the host sees an
// aggregate cap rather than a per-slot one.
//
// Permits are modeled as tokens in the buffered `free` channel: a token present
// means a slot is available. Acquire takes a token (honoring ctx), Release
// returns one. setLimit grows the pool by adding tokens and shrinks it by
// reclaiming free tokens; a reduction it cannot satisfy immediately (because
// the tokens are in flight) is recorded as `debt` and absorbed on the next
// Release, so the in-flight count is never violated by a live resize.
type hostGate struct {
	free chan struct{}

	// limitAtomic mirrors limit for a lock-free read. setLimit runs before every
	// gated call but the limit rarely changes, so the common path compares this
	// atomically and skips the mutex entirely; writes still happen under mu.
	limitAtomic atomic.Int64

	mu    sync.Mutex
	limit int
	debt  int
}

// newHostGate returns a gate admitting `limit` concurrent requests.
func newHostGate(limit int) *hostGate {
	g := &hostGate{free: make(chan struct{}, hostGateMaxLimit)}
	g.setLimit(limit)
	return g
}

// setLimit changes the number of concurrent permits, preserving any in-flight
// acquisitions. Growing adds free tokens (after cancelling outstanding debt);
// shrinking reclaims free tokens and records debt for any it cannot reclaim
// because the permits are currently held. A limit below 1 is floored to 1; the
// caller decides whether to attach a gate at all (limit <= 0 means no gate).
func (g *hostGate) setLimit(n int) {
	if n < 1 {
		n = 1
	}
	if n > hostGateMaxLimit {
		n = hostGateMaxLimit
	}
	// Hot-path fast exit: the limit is unchanged on almost every call, so a
	// lock-free atomic compare avoids taking the mutex per gated request.
	if int(g.limitAtomic.Load()) == n {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.limit == n { // lost a race with a concurrent setLimit; nothing to do
		return
	}

	diff := n - g.limit
	g.limit = n
	g.limitAtomic.Store(int64(n))
	switch {
	case diff > 0:
		// Cancel debt first, then publish the remaining new permits.
		absorbed := min(diff, g.debt)
		g.debt -= absorbed
		for i := 0; i < diff-absorbed; i++ {
			g.free <- struct{}{}
		}
	case diff < 0:
		need := -diff
		for need > 0 {
			select {
			case <-g.free:
				need--
			default:
				// Permits are in flight; absorb the reduction when they Release.
				g.debt += need
				need = 0
			}
		}
	}
}

// Acquire blocks until a permit is free or ctx is done, returning ctx.Err() on
// cancellation so a shutdown or per-job timeout never deadlocks on a full gate.
func (g *hostGate) Acquire(ctx context.Context) error {
	select {
	case <-g.free:
		return nil
	default:
	}
	select {
	case <-g.free:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a permit. When a shrink left outstanding debt, the permit is
// retired against that debt instead of being returned to the free pool.
func (g *hostGate) Release() {
	g.mu.Lock()
	if g.debt > 0 {
		g.debt--
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()
	g.free <- struct{}{}
}

// currentLimit reports the configured permit count (for tests/observability),
// read lock-free from the atomic mirror.
func (g *hostGate) currentLimit() int {
	return int(g.limitAtomic.Load())
}

// hostKey derives the de-duplicated gate key for a slot: scheme://host:port from
// its base URL so multiple slots pointing at the same host share one gate. Slots
// with no base URL (hosted vendors with a fixed endpoint) key on the normalized
// provider type, so e.g. every Anthropic slot shares a vendor-API gate.
func hostKey(slot SlotConfig) string {
	if slot.BaseURL != "" {
		if u, err := url.Parse(slot.BaseURL); err == nil && u.Host != "" {
			return strings.ToLower(u.Scheme + "://" + u.Host)
		}
	}
	return "type:" + strings.ToLower(NormalizeProviderType(slot.Type))
}

// HostConcurrency holds the per-host in-flight limits for the two provider
// roles. A value <= 0 disables gating for that role (unlimited), preserving the
// pre-gate behavior for registries built without the injection (e.g. tests).
type HostConcurrency struct {
	LLM   int
	Embed int
}

// normHostLimit maps a configured limit to the gate's enforced permit count.
// A non-positive value means "no cap": it resolves to hostGateMaxLimit, which
// for a single host is effectively unlimited. This lets an operator disable the
// cap at runtime (set 0) without a restart, the same as raising or lowering it.
func normHostLimit(n int) int {
	if n <= 0 {
		return hostGateMaxLimit
	}
	return n
}

// gatedLLM bounds concurrent Complete calls to one host via a shared hostGate.
// It sits as the outermost wrapper so the permit is held only around the real
// upstream call (and its circuit-breaker/usage-recording chain). The limit is
// re-resolved live before each acquire (limitFn reads the current setting,
// served from the settings cache) so a change takes effect within the cache TTL
// without a restart, mirroring the live-config embedding cache.
type gatedLLM struct {
	inner   LLMProvider
	gate    *hostGate
	limitFn func(context.Context) int
}

// newGatedLLM wraps inner in a host gate. A nil gate returns inner unchanged so
// callers can attach gating conditionally without branching at the call site.
func newGatedLLM(inner LLMProvider, gate *hostGate, limitFn func(context.Context) int) LLMProvider {
	if gate == nil {
		return inner
	}
	return &gatedLLM{inner: inner, gate: gate, limitFn: limitFn}
}

func (g *gatedLLM) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	g.gate.setLimit(normHostLimit(g.limitFn(ctx)))
	if err := g.gate.Acquire(ctx); err != nil {
		return nil, err
	}
	defer g.gate.Release()
	return g.inner.Complete(ctx, req)
}

func (g *gatedLLM) Name() string     { return g.inner.Name() }
func (g *gatedLLM) Models() []string { return g.inner.Models() }

// gatedEmbedding is the embedding counterpart to gatedLLM. It is placed inside
// the embedding cache (so a cache hit never consumes a permit) but outside the
// usage recorder, so the permit brackets only a real upstream Embed.
type gatedEmbedding struct {
	inner   EmbeddingProvider
	gate    *hostGate
	limitFn func(context.Context) int
}

func newGatedEmbedding(inner EmbeddingProvider, gate *hostGate, limitFn func(context.Context) int) EmbeddingProvider {
	if gate == nil {
		return inner
	}
	return &gatedEmbedding{inner: inner, gate: gate, limitFn: limitFn}
}

func (g *gatedEmbedding) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	g.gate.setLimit(normHostLimit(g.limitFn(ctx)))
	if err := g.gate.Acquire(ctx); err != nil {
		return nil, err
	}
	defer g.gate.Release()
	return g.inner.Embed(ctx, req)
}

func (g *gatedEmbedding) Name() string      { return g.inner.Name() }
func (g *gatedEmbedding) Dimensions() []int { return g.inner.Dimensions() }
