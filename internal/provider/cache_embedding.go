package provider

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// EmbedCacheConfig is the live, settings-backed configuration of the embedding
// cache. It is read on every Embed call so the admin toggle and size/TTL knobs
// take effect without a restart.
type EmbedCacheConfig struct {
	Enabled    bool
	MaxEntries int
	TTL        time.Duration
}

// embedCacheStore is the process-wide LRU shared across embedding-provider
// rebuilds. The CachingEmbedding wrapper is reconstructed on every Registry
// Reload around the freshly built inner chain, but the store (and therefore the
// cached vectors) persists for the Registry's lifetime. Keys embed the
// effective model, so a model change after Reload simply produces new keys; the
// stale entries age out under the LRU bound rather than returning wrong vectors.
type embedCacheStore struct {
	cfg func(context.Context) EmbedCacheConfig

	mu    sync.Mutex
	ll    *list.List               // front = most recently used
	items map[string]*list.Element // key -> *list.Element holding *embedCacheEntry
	sf    singleflight.Group
}

type embedCacheEntry struct {
	key     string
	vec     []float32
	expires time.Time // zero value means no expiry
}

func newEmbedCacheStore(cfg func(context.Context) EmbedCacheConfig) *embedCacheStore {
	return &embedCacheStore{
		cfg:   cfg,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

// get returns the cached vector for key, refreshing its LRU position. Expired
// entries are evicted on access and reported as a miss.
func (s *embedCacheStore) get(key string, now time.Time) ([]float32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[key]
	if !ok {
		return nil, false
	}
	ent := el.Value.(*embedCacheEntry)
	if !ent.expires.IsZero() && now.After(ent.expires) {
		s.ll.Remove(el)
		delete(s.items, key)
		return nil, false
	}
	s.ll.MoveToFront(el)
	return ent.vec, true
}

// put inserts or refreshes key, evicting least-recently-used entries once the
// store exceeds maxEntries. A non-positive maxEntries disables insertion.
func (s *embedCacheStore) put(key string, vec []float32, maxEntries int, ttl time.Duration, now time.Time) {
	if maxEntries <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expires := time.Time{}
	if ttl > 0 {
		expires = now.Add(ttl)
	}
	if el, ok := s.items[key]; ok {
		ent := el.Value.(*embedCacheEntry)
		ent.vec = vec
		ent.expires = expires
		s.ll.MoveToFront(el)
		return
	}
	el := s.ll.PushFront(&embedCacheEntry{key: key, vec: vec, expires: expires})
	s.items[key] = el
	for s.ll.Len() > maxEntries {
		back := s.ll.Back()
		if back == nil {
			break
		}
		s.ll.Remove(back)
		delete(s.items, back.Value.(*embedCacheEntry).key)
	}
}

// CachingEmbedding wraps an EmbeddingProvider with an exact-match vector cache
// keyed by (provider name, effective model, requested dimension, sha256(input)).
// Because embeddings are deterministic for a fixed model+dimension, a cache hit
// returns byte-identical vectors to a live call, so wrapping is output-neutral;
// the only observable difference is that work genuinely skipped does not land a
// token_usage row. It must sit OUTSIDE the usage-recording wrapper so a full hit
// records nothing.
type CachingEmbedding struct {
	inner EmbeddingProvider
	store *embedCacheStore
	// model is the slot's configured model captured at build time. Requests
	// usually leave EmbeddingRequest.Model empty and rely on the provider
	// default, so keying on the request model alone would alias different
	// models across a Reload; the effective model closes that gap.
	model string
}

func newCachingEmbedding(inner EmbeddingProvider, model string, store *embedCacheStore) *CachingEmbedding {
	return &CachingEmbedding{inner: inner, store: store, model: model}
}

// Name reports the underlying provider identifier.
func (c *CachingEmbedding) Name() string { return c.inner.Name() }

// Dimensions reports the underlying provider's supported dimensions.
func (c *CachingEmbedding) Dimensions() []int { return c.inner.Dimensions() }

func (c *CachingEmbedding) modelKey(req *EmbeddingRequest) string {
	if req.Model != "" {
		return req.Model
	}
	return c.model
}

func (c *CachingEmbedding) key(req *EmbeddingRequest, input string) string {
	sum := sha256.Sum256([]byte(input))
	return c.inner.Name() + "|" + c.modelKey(req) + "|" + strconv.Itoa(req.Dimension) + "|" + hex.EncodeToString(sum[:])
}

// Embed serves cached inputs from the store and forwards only the misses to the
// inner provider in a single batched call, preserving the caller's input order.
// The returned Usage reflects only the tokens actually spent on the miss batch
// (zero on a full hit).
func (c *CachingEmbedding) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	cfg := c.store.cfg(ctx)
	if !cfg.Enabled || req == nil || len(req.Input) == 0 {
		return c.inner.Embed(ctx, req)
	}

	now := time.Now()
	keys := make([]string, len(req.Input))
	out := make([][]float32, len(req.Input))
	missIdx := make([]int, 0, len(req.Input))
	for i, in := range req.Input {
		k := c.key(req, in)
		keys[i] = k
		if v, ok := c.store.get(k, now); ok {
			out[i] = v
			continue
		}
		missIdx = append(missIdx, i)
	}

	if len(missIdx) == 0 {
		// Full hit: no provider call, no usage.
		return &EmbeddingResponse{Embeddings: out, Model: c.modelKey(req)}, nil
	}

	// Deduplicate identical miss strings so each unique input is embedded once.
	uniqueKeys := make([]string, 0, len(missIdx))
	uniqueInputs := make([]string, 0, len(missIdx))
	seen := make(map[string]struct{}, len(missIdx))
	for _, i := range missIdx {
		if _, ok := seen[keys[i]]; ok {
			continue
		}
		seen[keys[i]] = struct{}{}
		uniqueKeys = append(uniqueKeys, keys[i])
		uniqueInputs = append(uniqueInputs, req.Input[i])
	}

	type missResult struct {
		vecs  map[string][]float32
		usage TokenUsage
		model string
	}

	// singleflight collapses concurrent requests whose miss set is identical
	// into one provider call. Note: when the result is shared, every caller
	// receives the same Usage value; the underlying token_usage row is still
	// written exactly once (inside the inner chain), so DB accounting stays
	// correct. The only effect is that an in-return Usage may be counted by
	// more than one caller's budget gate, which is conservative (never an
	// under-charge) and limited to the rare identical-concurrent-batch case.
	sfKey := strings.Join(uniqueKeys, "\x00")
	v, err, _ := c.store.sf.Do(sfKey, func() (any, error) {
		resp, err := c.inner.Embed(ctx, &EmbeddingRequest{
			Input:     uniqueInputs,
			Model:     req.Model,
			Dimension: req.Dimension,
		})
		if err != nil {
			return nil, err
		}
		if resp == nil || len(resp.Embeddings) != len(uniqueInputs) {
			got := 0
			if resp != nil {
				got = len(resp.Embeddings)
			}
			return nil, fmt.Errorf("embedding cache: provider returned %d vectors for %d inputs", got, len(uniqueInputs))
		}
		stamp := time.Now()
		vecs := make(map[string][]float32, len(uniqueKeys))
		for j, k := range uniqueKeys {
			vecs[k] = resp.Embeddings[j]
			c.store.put(k, resp.Embeddings[j], cfg.MaxEntries, cfg.TTL, stamp)
		}
		return missResult{vecs: vecs, usage: resp.Usage, model: resp.Model}, nil
	})
	if err != nil {
		return nil, err
	}
	mr := v.(missResult)

	for _, i := range missIdx {
		out[i] = mr.vecs[keys[i]]
	}
	model := mr.model
	if model == "" {
		model = c.modelKey(req)
	}
	return &EmbeddingResponse{Embeddings: out, Model: model, Usage: mr.usage}, nil
}
