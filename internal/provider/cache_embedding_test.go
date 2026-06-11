package provider

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeEmbedder records every call and returns a deterministic vector per input
// string. An optional gate lets a test hold calls open to observe concurrency.
type fakeEmbedder struct {
	name string

	mu     sync.Mutex
	calls  int
	inputs [][]string

	gate chan struct{} // when non-nil, Embed blocks until it is closed
}

func vecFor(s string) []float32 {
	var sum float32
	for _, b := range []byte(s) {
		sum += float32(b)
	}
	return []float32{sum, float32(len(s))}
}

func (f *fakeEmbedder) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	f.calls++
	cp := append([]string(nil), req.Input...)
	f.inputs = append(f.inputs, cp)
	f.mu.Unlock()

	embs := make([][]float32, len(req.Input))
	for i, in := range req.Input {
		embs[i] = vecFor(in)
	}
	return &EmbeddingResponse{
		Embeddings: embs,
		Model:      "fake-model",
		Usage:      TokenUsage{PromptTokens: len(req.Input), TotalTokens: len(req.Input)},
	}, nil
}

func (f *fakeEmbedder) Name() string      { return f.name }
func (f *fakeEmbedder) Dimensions() []int { return []int{2} }

func (f *fakeEmbedder) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestCache(inner EmbeddingProvider, cfg *EmbedCacheConfig) *CachingEmbedding {
	store := newEmbedCacheStore(func(context.Context) EmbedCacheConfig { return *cfg })
	return newCachingEmbedding(inner, "fake-model", store)
}

func vecEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCachingEmbedding_HitReturnsIdenticalVectorWithoutSecondCall(t *testing.T) {
	inner := &fakeEmbedder{name: "fake"}
	cfg := &EmbedCacheConfig{Enabled: true, MaxEntries: 100}
	c := newTestCache(inner, cfg)
	ctx := context.Background()

	r1, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"alpha"}})
	if err != nil {
		t.Fatalf("first embed: %v", err)
	}
	r2, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"alpha"}})
	if err != nil {
		t.Fatalf("second embed: %v", err)
	}
	if !vecEqual(r1.Embeddings[0], r2.Embeddings[0]) {
		t.Fatalf("hit vector %v != miss vector %v", r2.Embeddings[0], r1.Embeddings[0])
	}
	if inner.callCount() != 1 {
		t.Fatalf("provider calls = %d, want 1 (second served from cache)", inner.callCount())
	}
	if r2.Usage.TotalTokens != 0 {
		t.Fatalf("full-hit usage = %d, want 0", r2.Usage.TotalTokens)
	}
}

func TestCachingEmbedding_PartialBatchEmbedsOnlyMissesInOrder(t *testing.T) {
	inner := &fakeEmbedder{name: "fake"}
	cfg := &EmbedCacheConfig{Enabled: true, MaxEntries: 100}
	c := newTestCache(inner, cfg)
	ctx := context.Background()

	// Prime the cache with "b".
	if _, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"b"}}); err != nil {
		t.Fatalf("prime: %v", err)
	}

	resp, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"a", "b", "c"}})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	// Order preserved and vectors correct.
	for i, in := range []string{"a", "b", "c"} {
		if !vecEqual(resp.Embeddings[i], vecFor(in)) {
			t.Fatalf("pos %d (%q) = %v, want %v", i, in, resp.Embeddings[i], vecFor(in))
		}
	}
	// Second provider call carried only the two misses {a, c}.
	if got := inner.callCount(); got != 2 {
		t.Fatalf("provider calls = %d, want 2", got)
	}
	last := inner.inputs[len(inner.inputs)-1]
	if len(last) != 2 {
		t.Fatalf("miss batch carried %d inputs, want 2 (a,c): %v", len(last), last)
	}
	if resp.Usage.TotalTokens != 2 {
		t.Fatalf("usage = %d, want 2 (only the two misses)", resp.Usage.TotalTokens)
	}
}

func TestCachingEmbedding_DisabledBypassesCache(t *testing.T) {
	inner := &fakeEmbedder{name: "fake"}
	cfg := &EmbedCacheConfig{Enabled: false, MaxEntries: 100}
	c := newTestCache(inner, cfg)
	ctx := context.Background()

	for i := range 3 {
		if _, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"x"}}); err != nil {
			t.Fatalf("embed %d: %v", i, err)
		}
	}
	if inner.callCount() != 3 {
		t.Fatalf("provider calls = %d, want 3 (cache disabled)", inner.callCount())
	}
}

func TestCachingEmbedding_EvictsPastMaxEntries(t *testing.T) {
	inner := &fakeEmbedder{name: "fake"}
	cfg := &EmbedCacheConfig{Enabled: true, MaxEntries: 2}
	c := newTestCache(inner, cfg)
	ctx := context.Background()

	// Insert 3 distinct keys into a 2-slot cache; "k0" is evicted (LRU).
	for i := range 3 {
		if _, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{fmt.Sprintf("k%d", i)}}); err != nil {
			t.Fatalf("insert k%d: %v", i, err)
		}
	}
	if inner.callCount() != 3 {
		t.Fatalf("calls after inserts = %d, want 3", inner.callCount())
	}
	// k0 was evicted: re-embedding it calls the provider again.
	if _, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"k0"}}); err != nil {
		t.Fatalf("re-embed k0: %v", err)
	}
	if inner.callCount() != 4 {
		t.Fatalf("calls after re-embed of evicted k0 = %d, want 4", inner.callCount())
	}
	// k2 is still resident: served from cache, no new call.
	if _, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"k2"}}); err != nil {
		t.Fatalf("re-embed k2: %v", err)
	}
	if inner.callCount() != 4 {
		t.Fatalf("calls after resident k2 = %d, want 4 (cache hit)", inner.callCount())
	}
}

func TestCachingEmbedding_TTLExpiry(t *testing.T) {
	inner := &fakeEmbedder{name: "fake"}
	cfg := &EmbedCacheConfig{Enabled: true, MaxEntries: 100, TTL: 20 * time.Millisecond}
	c := newTestCache(inner, cfg)
	ctx := context.Background()

	if _, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"t"}}); err != nil {
		t.Fatalf("embed: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := c.Embed(ctx, &EmbeddingRequest{Input: []string{"t"}}); err != nil {
		t.Fatalf("embed after ttl: %v", err)
	}
	if inner.callCount() != 2 {
		t.Fatalf("provider calls = %d, want 2 (entry expired)", inner.callCount())
	}
}

func TestCachingEmbedding_ConcurrentIdenticalMissesCollapse(t *testing.T) {
	inner := &fakeEmbedder{name: "fake", gate: make(chan struct{})}
	cfg := &EmbedCacheConfig{Enabled: true, MaxEntries: 100}
	c := newTestCache(inner, cfg)
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, _ = c.Embed(ctx, &EmbeddingRequest{Input: []string{"shared"}})
		}()
	}
	// Give the goroutines time to enter singleflight, then release the gate.
	time.Sleep(50 * time.Millisecond)
	close(inner.gate)
	wg.Wait()

	if got := inner.callCount(); got != 1 {
		t.Fatalf("provider calls = %d, want 1 (concurrent identical misses collapsed)", got)
	}
}
