package hnsw

import (
	"bytes"
	"container/list"
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

// IndexCache manages per-(namespace, dimension) HNSW indexes with LRU eviction.
type IndexCache struct {
	mu               sync.Mutex
	indexes          map[indexKey]*indexEntry
	lruOrder         *list.List
	maxIndexes       int
	db               *sql.DB
	graphOpts        []Option
	stopCh           chan struct{}
	wg               sync.WaitGroup
	snapshotInterval time.Duration
}

type indexKey struct {
	NamespaceID uuid.UUID
	Dimension   int
}

type indexEntry struct {
	graph   *Graph
	dirty   bool
	element *list.Element
	key     indexKey
}

// CacheConfig holds configuration for the index cache.
type CacheConfig struct {
	MaxIndexes       int
	SnapshotInterval time.Duration
	GraphOpts        []Option
}

// NewIndexCache creates a new index cache backed by the given SQLite DB.
// It starts a background goroutine that periodically flushes dirty snapshots.
func NewIndexCache(db *sql.DB, cfg CacheConfig) *IndexCache {
	maxIndexes := cfg.MaxIndexes
	if maxIndexes <= 0 {
		maxIndexes = 64
	}
	interval := cfg.SnapshotInterval
	if interval <= 0 {
		interval = 60 * time.Second
	}

	c := &IndexCache{
		indexes:          make(map[indexKey]*indexEntry),
		lruOrder:         list.New(),
		maxIndexes:       maxIndexes,
		db:               db,
		graphOpts:        cfg.GraphOpts,
		stopCh:           make(chan struct{}),
		snapshotInterval: interval,
	}

	c.wg.Add(1)
	go c.backgroundSnapshot()

	return c
}

// GetOrCreate returns the HNSW graph for the given namespace+dimension,
// loading from snapshot/rebuilding from memory_vectors if not cached.
// Creates a new empty graph if no data exists.
func (c *IndexCache) GetOrCreate(ctx context.Context, namespaceID uuid.UUID, dimension int) (*Graph, error) {
	key := indexKey{NamespaceID: namespaceID, Dimension: dimension}

	// Fast path: check cache under lock.
	c.mu.Lock()
	if entry, ok := c.indexes[key]; ok {
		c.lruOrder.MoveToFront(entry.element)
		g := entry.graph
		c.mu.Unlock()
		return g, nil
	}
	c.mu.Unlock()

	// Slow path: load outside of lock.
	graph, err := c.loadGraph(ctx, key)
	if err != nil {
		return nil, err
	}

	// Re-acquire lock to insert. Another goroutine may have loaded the same key.
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.indexes[key]; ok {
		// Another goroutine beat us; use theirs.
		c.lruOrder.MoveToFront(entry.element)
		return entry.graph, nil
	}

	// Evict if at capacity.
	if len(c.indexes) >= c.maxIndexes {
		c.evictLRU(ctx)
	}

	entry := &indexEntry{
		graph: graph,
		dirty: false,
		key:   key,
	}
	entry.element = c.lruOrder.PushFront(entry)
	c.indexes[key] = entry

	return graph, nil
}

// MarkDirty flags the index for the given key as needing a snapshot save.
func (c *IndexCache) MarkDirty(namespaceID uuid.UUID, dimension int) {
	key := indexKey{NamespaceID: namespaceID, Dimension: dimension}
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.indexes[key]; ok {
		entry.dirty = true
	}
}

// FlushAll persists all dirty snapshots to SQLite immediately.
func (c *IndexCache) FlushAll(ctx context.Context) error {
	c.mu.Lock()
	var dirtyEntries []struct {
		key   indexKey
		graph *Graph
	}
	for key, entry := range c.indexes {
		if entry.dirty {
			dirtyEntries = append(dirtyEntries, struct {
				key   indexKey
				graph *Graph
			}{key: key, graph: entry.graph})
		}
	}
	c.mu.Unlock()

	var lastErr error
	for _, de := range dirtyEntries {
		if err := c.saveSnapshot(ctx, de.key, de.graph); err != nil {
			lastErr = err
			continue
		}
		c.mu.Lock()
		if entry, ok := c.indexes[de.key]; ok {
			entry.dirty = false
		}
		c.mu.Unlock()
	}
	return lastErr
}

// Close stops the background goroutine and flushes all dirty snapshots.
func (c *IndexCache) Close() error {
	close(c.stopCh)
	c.wg.Wait()
	return c.FlushAll(context.Background())
}

// Remove evicts a specific index from the cache (used after bulk deletes).
func (c *IndexCache) Remove(namespaceID uuid.UUID, dimension int) {
	key := indexKey{NamespaceID: namespaceID, Dimension: dimension}
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.indexes[key]; ok {
		c.lruOrder.Remove(entry.element)
		delete(c.indexes, key)
	}
}

// loadGraph loads a graph from snapshot or rebuilds from memory_vectors.
// Called without holding c.mu.
func (c *IndexCache) loadGraph(ctx context.Context, key indexKey) (*Graph, error) {
	// Try snapshot first.
	var graphData []byte
	err := c.db.QueryRowContext(ctx,
		"SELECT graph_data FROM hnsw_snapshots WHERE namespace_id = ? AND dimension = ?",
		key.NamespaceID.String(), key.Dimension,
	).Scan(&graphData)

	if err == nil && len(graphData) > 0 {
		g, importErr := Import(bytes.NewReader(graphData))
		if importErr == nil {
			return g, nil
		}
		// Snapshot corrupted; fall through to rebuild.
		log.Printf("hnsw: cache: corrupted snapshot for ns=%s dim=%d: %v", key.NamespaceID, key.Dimension, importErr)
	}

	// Rebuild from memory_vectors.
	rows, err := c.db.QueryContext(ctx,
		"SELECT memory_id, embedding FROM memory_vectors WHERE namespace_id = ? AND dimension = ?",
		key.NamespaceID.String(), key.Dimension,
	)
	if err != nil {
		return nil, fmt.Errorf("hnsw: cache: query memory_vectors: %w", err)
	}
	defer rows.Close()

	g := NewGraph(key.Dimension, c.graphOpts...)
	for rows.Next() {
		var memIDStr string
		var embBlob []byte
		if err := rows.Scan(&memIDStr, &embBlob); err != nil {
			return nil, fmt.Errorf("hnsw: cache: scan memory_vectors row: %w", err)
		}
		memID, err := uuid.Parse(memIDStr)
		if err != nil {
			return nil, fmt.Errorf("hnsw: cache: parse memory_id %q: %w", memIDStr, err)
		}
		vec, err := DecodeVector(embBlob)
		if err != nil {
			return nil, fmt.Errorf("hnsw: cache: decode vector for %s: %w", memID, err)
		}
		if err := g.Add(Node{ID: memID, Vector: vec}); err != nil {
			return nil, fmt.Errorf("hnsw: cache: add node %s: %w", memID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("hnsw: cache: iterate memory_vectors: %w", err)
	}

	return g, nil
}

// saveSnapshot writes the serialized graph to hnsw_snapshots.
func (c *IndexCache) saveSnapshot(ctx context.Context, key indexKey, g *Graph) error {
	var buf bytes.Buffer
	if err := g.Export(&buf); err != nil {
		return fmt.Errorf("hnsw: cache: export graph ns=%s dim=%d: %w", key.NamespaceID, key.Dimension, err)
	}

	_, err := c.db.ExecContext(ctx, `
		INSERT INTO hnsw_snapshots (namespace_id, dimension, graph_data, node_count, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(namespace_id, dimension) DO UPDATE SET
		  graph_data = excluded.graph_data,
		  node_count = excluded.node_count,
		  updated_at = excluded.updated_at`,
		key.NamespaceID.String(),
		key.Dimension,
		buf.Bytes(),
		g.Len(),
		time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	)
	if err != nil {
		return fmt.Errorf("hnsw: cache: upsert snapshot ns=%s dim=%d: %w", key.NamespaceID, key.Dimension, err)
	}
	return nil
}

// evictLRU removes the least recently used entry from the cache.
// Caller must hold c.mu.
func (c *IndexCache) evictLRU(ctx context.Context) {
	back := c.lruOrder.Back()
	if back == nil {
		return
	}

	entry := back.Value.(*indexEntry)
	if entry.dirty {
		// Save before evicting. Best-effort; log errors.
		if err := c.saveSnapshot(ctx, entry.key, entry.graph); err != nil {
			log.Printf("hnsw: cache: evict save failed ns=%s dim=%d: %v", entry.key.NamespaceID, entry.key.Dimension, err)
		}
	}

	c.lruOrder.Remove(back)
	delete(c.indexes, entry.key)
}

// backgroundSnapshot periodically flushes dirty snapshots.
func (c *IndexCache) backgroundSnapshot() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.FlushAll(context.Background()); err != nil {
				log.Printf("hnsw: cache: background flush error: %v", err)
			}
		}
	}
}
