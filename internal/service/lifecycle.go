package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/periodic"
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
// race the sweep, see EntityRepo.DeleteOrphaned.
//
// DeleteOrphanedEntities returns the IDs of deleted rows so the lifecycle
// worker can clean up out-of-band vector storage (Qdrant) for them. The
// SQL-backed entity_vectors_* tables already cascade on the entity delete,
// so the per-ID cleanup is a no-op there; only Qdrant materially needs it.
type GraphPruner interface {
	DeleteDanglingRelationships(ctx context.Context) (int64, error)
	DeleteOrphanedEntities(ctx context.Context, olderThan time.Time) ([]uuid.UUID, error)
}

// graphPrunerAdapter wraps entity and relationship repos into a GraphPruner.
type graphPrunerAdapter struct {
	entities interface {
		DeleteOrphaned(ctx context.Context, olderThan time.Time) ([]uuid.UUID, error)
	}
	relationships interface {
		DeleteDangling(ctx context.Context) (int64, error)
	}
}

// NewGraphPruner creates a GraphPruner from entity and relationship repos.
func NewGraphPruner(
	entities interface {
		DeleteOrphaned(ctx context.Context, olderThan time.Time) ([]uuid.UUID, error)
	},
	relationships interface {
		DeleteDangling(ctx context.Context) (int64, error)
	},
) GraphPruner {
	return &graphPrunerAdapter{entities: entities, relationships: relationships}
}

func (a *graphPrunerAdapter) DeleteDanglingRelationships(ctx context.Context) (int64, error) {
	return a.relationships.DeleteDangling(ctx)
}

func (a *graphPrunerAdapter) DeleteOrphanedEntities(ctx context.Context, olderThan time.Time) ([]uuid.UUID, error) {
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
	graphReaper GraphReaper // nil disables lost-provenance reaping + repair
	settings    *SettingsService
	config      LifecycleConfig
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// WithGraphReaper attaches the graph reaper, enabling the Phase-2 per-memory
// footprint reap, the Phase-3 lost-provenance sweep, and the console
// RepairGraph/GraphHealth operations. Returns the same service for chaining.
func (s *LifecycleService) WithGraphReaper(r GraphReaper) *LifecycleService {
	s.graphReaper = r
	return s
}

// NewLifecycleService creates a new LifecycleService. The vectorStore parameter
// may be nil if no vector store is configured. The graphPruner parameter may be
// nil if graph data is not available (e.g., SQLite backend). settings may be
// nil; the per-sweep knobs fall through to settingDefaults. Zero-value config
// fields are resolved from the SettingsService per sweep iteration, so admin-UI
// edits to lifecycle.* keys (including the sweep interval itself) take effect
// without restarting the service. A non-zero cfg.SweepInterval supplied by the
// caller (tests, primarily) pins the value and bypasses the live read.
func NewLifecycleService(store LifecycleStore, vectorStore VectorDeleter, graphPruner GraphPruner, cfg LifecycleConfig, settings *SettingsService) *LifecycleService {
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
	v := max(s.settings.ResolveIntWithDefault(ctx, SettingLifecycleBatchSize, "global"), 1)
	return v
}

// resolvePurgeDelay returns the soft-delete retention window in time.Duration.
func (s *LifecycleService) resolvePurgeDelay(ctx context.Context) time.Duration {
	if s.config.DefaultPurgeDelay > 0 {
		return s.config.DefaultPurgeDelay
	}
	days := max(s.settings.ResolveIntWithDefault(ctx, SettingMemorySoftDeleteRetentionDays, "global"), 1)
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

// resolveSweepInterval returns the per-iteration sleep duration before the
// next sweep fires. Pinned value (cfg.SweepInterval > 0 supplied at
// construction by tests) bypasses the live read; otherwise the value is
// re-resolved from SettingLifecycleSweepIntervalSeconds on every loop tick,
// so admin-UI edits take effect on the next iteration. Sub-second values
// from the registry are floored at one second to keep the sweep loop from
// degrading into a busy spin if an operator stores a misconfigured value.
func (s *LifecycleService) resolveSweepInterval(ctx context.Context) time.Duration {
	if s.config.SweepInterval > 0 {
		return s.config.SweepInterval
	}
	v := max(s.settings.ResolveDurationSecondsWithDefault(ctx,
		SettingLifecycleSweepIntervalSeconds, "global"), time.Second)
	return v
}

// Start launches the background sweep loop. It returns immediately.
// Call Stop to shut down the loop cleanly.
func (s *LifecycleService) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Go(func() {
		s.loop(ctx)
	})
}

// Stop cancels the background loop and waits for it to finish.
func (s *LifecycleService) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// loop runs the periodic sweep until the context is cancelled. periodic.Run
// sweeps once at startup so a restart reclaims already-expired memories without
// waiting a full interval, then re-resolves resolveSweepInterval each tick so a
// live edit to lifecycle.sweep_interval_seconds via the admin UI takes effect
// on the next tick.
func (s *LifecycleService) loop(ctx context.Context) {
	periodic.Run(ctx, s.resolveSweepInterval, func(ctx context.Context, _ bool) {
		_, _, _ = s.sweep(ctx)
	})
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
		// Reap the memory's exclusively-sourced graph footprint before the hard
		// delete fires the FK ON DELETE SET NULL that would strand its edges.
		if s.graphReaper != nil {
			if _, err := s.graphReaper.ReapMemoryFootprint(ctx, mem.NamespaceID, mem.ID); err != nil {
				slog.Warn("lifecycle: reap graph footprint failed", "memory", mem.ID, "err", err)
			}
		}
		if err := s.store.HardDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			continue
		}
		if s.vectorStore != nil {
			_ = s.vectorStore.Delete(ctx, storage.VectorKindMemory, mem.ID)
		}
		purged++
	}

	// Phase 3: Reap lost-provenance edges, then prune the dangling relationships
	// and orphaned entities they leave behind. Shared with the on-demand
	// RepairGraph via reapAndPrune so the two cannot diverge.
	res, err := s.reapAndPrune(ctx, now.Add(-orphanGrace))
	if err != nil {
		slog.Warn("lifecycle: graph cleanup failed", "err", err)
	}
	if res.RelationshipsReaped > 0 || res.DanglingDeleted > 0 || res.OrphanedEntities > 0 {
		slog.Info("lifecycle: graph cleanup",
			"lost_provenance", res.RelationshipsReaped, "dangling", res.DanglingDeleted,
			"orphaned_entities", res.OrphanedEntities, "orphan_grace", orphanGrace)
	}

	return expired, purged, nil
}

// reapAndPrune runs the shared graph-cleanup sequence used by both the periodic
// sweep and the on-demand RepairGraph: reap lost-provenance edges (recomputing
// only the reaped endpoints' mention counts inside ReapLostProvenance, not the
// whole table), then prune dangling relationships
// and orphaned entities (age-gated by orphanCutoff), cleaning up entity vectors
// for reaped orphans. Order matters: reaping first strands entities the orphan
// pass then collects in the same run. It is best-effort: every step is attempted
// even if an earlier one errors; it returns the aggregate counts plus the first
// error encountered.
func (s *LifecycleService) reapAndPrune(ctx context.Context, orphanCutoff time.Time) (GraphRepairResult, error) {
	var res GraphRepairResult
	var firstErr error
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	if s.graphReaper != nil {
		reaped, err := s.graphReaper.ReapLostProvenance(ctx)
		if err != nil {
			note(err)
		} else {
			res.RelationshipsReaped = reaped
		}
	}

	if s.graphPruner != nil {
		danglingRels, err := s.graphPruner.DeleteDanglingRelationships(ctx)
		if err != nil {
			note(err)
		} else {
			res.DanglingDeleted = danglingRels
		}

		orphanedIDs, err := s.graphPruner.DeleteOrphanedEntities(ctx, orphanCutoff)
		if err != nil {
			note(err)
		} else {
			res.OrphanedEntities = len(orphanedIDs)
			// Best-effort vector cleanup. SQL-backed stores cascade on the
			// entity row delete (entity_vectors_* FK); only Qdrant needs the
			// explicit per-ID delete to avoid leaking points.
			if s.vectorStore != nil {
				for _, id := range orphanedIDs {
					_ = s.vectorStore.Delete(ctx, storage.VectorKindEntity, id)
				}
			}
		}
	}

	return res, firstErr
}

// GraphHealth reports the number of lost-provenance relationships currently in
// the graph (edges whose sourcing memory is gone), so the console can show
// how much a repair would reap. Returns a zero result when no graph reaper is
// wired (e.g. graph features disabled).
type GraphHealth struct {
	LostProvenanceEdges int64 `json:"lost_provenance_edges"`
}

// GraphHealthStatus returns the current graph-health counts.
func (s *LifecycleService) GraphHealthStatus(ctx context.Context) (GraphHealth, error) {
	if s.graphReaper == nil {
		return GraphHealth{}, nil
	}
	n, err := s.graphReaper.CountLostProvenance(ctx)
	if err != nil {
		return GraphHealth{}, err
	}
	return GraphHealth{LostProvenanceEdges: n}, nil
}

// GraphRepairResult summarizes an operator-triggered graph repair.
type GraphRepairResult struct {
	RelationshipsReaped int64 `json:"relationships_reaped"`
	DanglingDeleted     int64 `json:"dangling_relationships_deleted"`
	OrphanedEntities    int   `json:"orphaned_entities_deleted"`
}

// RepairGraph runs the full on-demand graph cleanup the console exposes:
// reap every lost-provenance edge (scoping the reap's mention_count recompute to
// the touched endpoints via reapAndPrune), then prune dangling relationships and
// orphaned entities, then re-normalize every entity's mention_count across the
// whole graph. The orphan sweep stays age-gated by
// SettingLifecycleOrphanGraceSeconds so a repair cannot race in-flight
// enrichment. Idempotent: a second run reaps nothing and recomputes to the same
// values. Shares the reap/prune body with the periodic sweep via reapAndPrune;
// the whole-graph recompute is what makes this the deliberate operator repair
// rather than the cheap periodic sweep.
func (s *LifecycleService) RepairGraph(ctx context.Context) (GraphRepairResult, error) {
	res, err := s.reapAndPrune(ctx, time.Now().Add(-s.resolveOrphanGrace(ctx)))
	// reapAndPrune now scopes its recompute to the reaped endpoints; an operator
	// repair is the right place for a full re-normalization, so follow with a
	// whole-graph recompute. Best-effort: attempt it even if reap/prune erred,
	// keeping the first error.
	if s.graphReaper != nil {
		if _, rerr := s.graphReaper.RecomputeAllMentionCounts(ctx); rerr != nil && err == nil {
			err = rerr
		}
	}
	return res, err
}
