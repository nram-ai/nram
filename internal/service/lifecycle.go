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

// GraphPruner cleans up orphaned graph data. Wired for both SQLite and
// Postgres: an entity created by enrichment but whose relationships are never
// written is otherwise leaked. The orphan filter is age-gated so in-flight
// enrichment (which writes the entity row before its relationships) cannot
// race the sweep — see EntityRepo.DeleteOrphaned.
type GraphPruner interface {
	DeleteDanglingRelationships(ctx context.Context) (int64, error)
	DeleteOrphanedEntities(ctx context.Context, olderThan time.Time) (int64, error)
}

// graphPrunerAdapter wraps entity and relationship repos into a GraphPruner.
type graphPrunerAdapter struct {
	entities      interface {
		DeleteOrphaned(ctx context.Context, olderThan time.Time) (int64, error)
	}
	relationships interface{ DeleteDangling(ctx context.Context) (int64, error) }
}

// NewGraphPruner creates a GraphPruner from entity and relationship repos.
func NewGraphPruner(
	entities interface {
		DeleteOrphaned(ctx context.Context, olderThan time.Time) (int64, error)
	},
	relationships interface{ DeleteDangling(ctx context.Context) (int64, error) },
) GraphPruner {
	return &graphPrunerAdapter{entities: entities, relationships: relationships}
}

func (a *graphPrunerAdapter) DeleteDanglingRelationships(ctx context.Context) (int64, error) {
	return a.relationships.DeleteDangling(ctx)
}

func (a *graphPrunerAdapter) DeleteOrphanedEntities(ctx context.Context, olderThan time.Time) (int64, error) {
	return a.entities.DeleteOrphaned(ctx, olderThan)
}

// LifecycleConfig pins specific tunables; zero fields fall through to the
// SettingsService cascade. See SettingLifecycle* keys.
type LifecycleConfig struct {
	SweepInterval     time.Duration // 0 → resolve from SettingLifecycleSweepIntervalSeconds
	BatchSize         int           // 0 → resolve from SettingLifecycleBatchSize per sweep
	DefaultPurgeDelay time.Duration // 0 → resolve from SettingMemorySoftDeleteRetentionDays per sweep
	// OrphanGrace is the minimum age an entity must reach before becoming
	// eligible for orphan deletion. Protects in-flight enrichment whose entity
	// rows are written before relationships and before vector upsert; without
	// this gate, a slow embed call lets the sweep delete the row mid-flight
	// and the subsequent vector upsert fails with a FOREIGN KEY violation.
	// 0 → resolve from SettingLifecycleOrphanGraceSeconds per sweep.
	OrphanGrace time.Duration
}

// LifecycleService runs a background goroutine that periodically sweeps expired
// and purgeable memories, handling TTL expiry and purge-after cleanup.
type LifecycleService struct {
	store       LifecycleStore
	vectorStore VectorDeleter
	graphPruner GraphPruner // nil on SQLite
	settings    *SettingsService
	config      LifecycleConfig
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewLifecycleService creates a new LifecycleService. The vectorStore parameter
// may be nil if no vector store is configured. The graphPruner parameter may be
// nil if graph data is not available (e.g., SQLite backend). settings may be
// nil; the per-sweep knobs fall through to settingDefaults. Zero-value config
// fields are resolved from the SettingsService at construction (SweepInterval)
// or per-sweep (everything else).
func NewLifecycleService(store LifecycleStore, vectorStore VectorDeleter, graphPruner GraphPruner, cfg LifecycleConfig, settings *SettingsService) *LifecycleService {
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = settings.ResolveDurationSecondsWithDefault(context.Background(),
			SettingLifecycleSweepIntervalSeconds, "global")
		if cfg.SweepInterval < time.Second {
			cfg.SweepInterval = time.Second
		}
	}
	return &LifecycleService{
		store:       store,
		vectorStore: vectorStore,
		graphPruner: graphPruner,
		settings:    settings,
		config:      cfg,
	}
}

// resolveBatchSize returns the per-sweep batch size, hot-reloading from
// SettingLifecycleBatchSize unless the operator pinned a value at construction.
func (s *LifecycleService) resolveBatchSize(ctx context.Context) int {
	if s.config.BatchSize > 0 {
		return s.config.BatchSize
	}
	v := s.settings.ResolveIntWithDefault(ctx, SettingLifecycleBatchSize, "global")
	if v < 1 {
		v = 1
	}
	return v
}

// resolvePurgeDelay returns the soft-delete retention window in time.Duration.
func (s *LifecycleService) resolvePurgeDelay(ctx context.Context) time.Duration {
	if s.config.DefaultPurgeDelay > 0 {
		return s.config.DefaultPurgeDelay
	}
	days := s.settings.ResolveIntWithDefault(ctx, SettingMemorySoftDeleteRetentionDays, "global")
	if days < 1 {
		days = 1
	}
	return time.Duration(days) * 24 * time.Hour
}

// resolveOrphanGrace returns the orphan-deletion grace window in time.Duration.
func (s *LifecycleService) resolveOrphanGrace(ctx context.Context) time.Duration {
	if s.config.OrphanGrace > 0 {
		return s.config.OrphanGrace
	}
	return s.settings.ResolveDurationSecondsWithDefault(ctx,
		SettingLifecycleOrphanGraceSeconds, "global")
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
	batchSize := s.resolveBatchSize(ctx)
	orphanGrace := s.resolveOrphanGrace(ctx)

	// Phase 1: Expire memories whose expires_at <= now.
	expiredMemories, err := s.store.ListExpired(ctx, now, batchSize)
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
	purgeableMemories, err := s.store.ListPurgeable(ctx, now, batchSize)
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

	// Phase 3: Prune orphaned graph data (wired for both SQLite and Postgres).
	// Order matters: delete dangling relationships first, then orphaned
	// entities. The entity sweep is age-gated against in-flight enrichment;
	// see SettingLifecycleOrphanGraceSeconds.
	if s.graphPruner != nil {
		danglingRels, relErr := s.graphPruner.DeleteDanglingRelationships(ctx)
		if relErr != nil {
			log.Printf("lifecycle: failed to delete dangling relationships: %v", relErr)
		} else if danglingRels > 0 {
			log.Printf("lifecycle: deleted %d dangling relationships", danglingRels)
		}

		orphanCutoff := now.Add(-orphanGrace)
		orphanedEntities, entErr := s.graphPruner.DeleteOrphanedEntities(ctx, orphanCutoff)
		if entErr != nil {
			log.Printf("lifecycle: failed to delete orphaned entities: %v", entErr)
		} else if orphanedEntities > 0 {
			log.Printf("lifecycle: deleted %d orphaned entities (older than %s)",
				orphanedEntities, orphanGrace)
		}
	}

	return expired, purged, nil
}
