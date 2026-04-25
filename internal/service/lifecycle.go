package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// LifecycleStore provides the query and delete operations needed by the lifecycle service.
type LifecycleStore interface {
	ListExpired(ctx context.Context, before time.Time, limit int) ([]model.Memory, error)
	ListPurgeable(ctx context.Context, before time.Time, limit int) ([]model.Memory, error)
	SoftDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	HardDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
}

// GraphPruner cleans up orphaned graph data (Postgres only).
type GraphPruner interface {
	DeleteDanglingRelationships(ctx context.Context) (int64, error)
	DeleteOrphanedEntities(ctx context.Context) (int64, error)
}

// graphPrunerAdapter wraps entity and relationship repos into a GraphPruner.
type graphPrunerAdapter struct {
	entities      interface{ DeleteOrphaned(ctx context.Context) (int64, error) }
	relationships interface{ DeleteDangling(ctx context.Context) (int64, error) }
}

// NewGraphPruner creates a GraphPruner from entity and relationship repos.
func NewGraphPruner(
	entities interface{ DeleteOrphaned(ctx context.Context) (int64, error) },
	relationships interface{ DeleteDangling(ctx context.Context) (int64, error) },
) GraphPruner {
	return &graphPrunerAdapter{entities: entities, relationships: relationships}
}

func (a *graphPrunerAdapter) DeleteDanglingRelationships(ctx context.Context) (int64, error) {
	return a.relationships.DeleteDangling(ctx)
}

func (a *graphPrunerAdapter) DeleteOrphanedEntities(ctx context.Context) (int64, error) {
	return a.entities.DeleteOrphaned(ctx)
}

// LifecycleConfig controls the behavior of the lifecycle sweep loop.
type LifecycleConfig struct {
	SweepInterval     time.Duration // how often to run, default 5 minutes
	BatchSize         int           // max items per sweep, default 100
	DefaultPurgeDelay time.Duration // how long after soft-delete before hard purge, default 30 days
}

// defaultLifecycleConfig returns a LifecycleConfig with sensible defaults applied.
func defaultLifecycleConfig() LifecycleConfig {
	return LifecycleConfig{
		SweepInterval:     5 * time.Minute,
		BatchSize:         100,
		DefaultPurgeDelay: 30 * 24 * time.Hour,
	}
}

// LifecycleService runs a background goroutine that periodically sweeps expired
// and purgeable memories, handling TTL expiry and purge-after cleanup.
type LifecycleService struct {
	store       LifecycleStore
	vectorStore VectorDeleter
	graphPruner GraphPruner // nil on SQLite
	config      LifecycleConfig
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewLifecycleService creates a new LifecycleService. The vectorStore parameter
// may be nil if no vector store is configured. The graphPruner parameter may be
// nil if graph data is not available (e.g., SQLite backend). Zero-value fields
// in cfg are replaced with defaults.
func NewLifecycleService(store LifecycleStore, vectorStore VectorDeleter, graphPruner GraphPruner, cfg LifecycleConfig) *LifecycleService {
	defaults := defaultLifecycleConfig()
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = defaults.SweepInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaults.BatchSize
	}
	if cfg.DefaultPurgeDelay <= 0 {
		cfg.DefaultPurgeDelay = defaults.DefaultPurgeDelay
	}
	return &LifecycleService{
		store:       store,
		vectorStore: vectorStore,
		graphPruner: graphPruner,
		config:      cfg,
	}
}

// Start launches the background sweep loop. It returns immediately.
// Call Stop to shut down the loop cleanly.
func (s *LifecycleService) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.loop(ctx)
	}()
}

// Stop cancels the background loop and waits for it to finish.
func (s *LifecycleService) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// loop runs the periodic sweep until the context is cancelled.
func (s *LifecycleService) loop(ctx context.Context) {
	ticker := time.NewTicker(s.config.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _, _ = s.sweep(ctx)
		}
	}
}

// Sweep runs a single sweep pass. It can be called manually (e.g., from an
// admin API endpoint) in addition to being called by the background loop.
// Returns the count of expired memories, purged memories, and any error.
func (s *LifecycleService) Sweep(ctx context.Context) (int, int, error) {
	return s.sweep(ctx)
}

// sweep is the core logic: expire TTL'd memories, then purge soft-deleted ones.
func (s *LifecycleService) sweep(ctx context.Context) (expired int, purged int, err error) {
	now := time.Now()

	// Phase 1: Expire memories whose expires_at <= now.
	// These get soft-deleted and scheduled for purge after DefaultPurgeDelay.
	expiredMemories, err := s.store.ListExpired(ctx, now, s.config.BatchSize)
	if err != nil {
		return 0, 0, err
	}
	for _, mem := range expiredMemories {
		if err := s.store.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			continue
		}
		expired++
	}

	// Phase 2: Purge memories whose purge_after <= now (already soft-deleted).
	// These get hard-deleted along with their vectors.
	purgeableMemories, err := s.store.ListPurgeable(ctx, now, s.config.BatchSize)
	if err != nil {
		return expired, 0, err
	}
	for _, mem := range purgeableMemories {
		if err := s.store.HardDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			continue
		}
		if s.vectorStore != nil {
			_ = s.vectorStore.Delete(ctx, storage.VectorKindMemory, mem.ID)
		}
		purged++
	}

	// Phase 3: Prune orphaned graph data (Postgres only).
	// Order matters: delete dangling relationships first, then orphaned entities.
	if s.graphPruner != nil {
		danglingRels, relErr := s.graphPruner.DeleteDanglingRelationships(ctx)
		if relErr != nil {
			log.Printf("lifecycle: failed to delete dangling relationships: %v", relErr)
		} else if danglingRels > 0 {
			log.Printf("lifecycle: deleted %d dangling relationships", danglingRels)
		}

		orphanedEntities, entErr := s.graphPruner.DeleteOrphanedEntities(ctx)
		if entErr != nil {
			log.Printf("lifecycle: failed to delete orphaned entities: %v", entErr)
		} else if orphanedEntities > 0 {
			log.Printf("lifecycle: deleted %d orphaned entities", orphanedEntities)
		}
	}

	return expired, purged, nil
}
