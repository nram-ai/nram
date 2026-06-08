package admin

import (
	"context"
	"fmt"
	"strings"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/storage"
)

// VectorMigrationAdminStore implements api.VectorMigrationAdminStore. It copies
// memory and entity vectors between the SQL primary store and Qdrant, reusing
// the fail-safe storage migrator (read-only source reads, upsert-only writes
// through the destination VectorStore). Real migrations run in the background
// and stream progress over the event bus; dry runs are synchronous counts.
type VectorMigrationAdminStore struct {
	db           storage.DB
	dsn          string
	hnswConfig   storage.HNSWConfig
	qdrantConfig func(context.Context) storage.QdrantConfig
	bus          events.EventBus

	guard singleFlight
}

// NewVectorMigrationAdminStore creates a VectorMigrationAdminStore. dsn is the
// Postgres connection string (empty on SQLite deployments); hnswConfig is the
// deployment's HNSW configuration, used when migrating back into a SQLite store
// so the rebuilt index matches the live one; qdrantConfig resolves the current
// Qdrant connection settings at call time so a freshly-saved address is honored
// without a restart; bus receives progress events.
func NewVectorMigrationAdminStore(db storage.DB, dsn string, hnswConfig storage.HNSWConfig, qdrantConfig func(context.Context) storage.QdrantConfig, bus events.EventBus) *VectorMigrationAdminStore {
	return &VectorMigrationAdminStore{
		db:           db,
		dsn:          dsn,
		hnswConfig:   hnswConfig,
		qdrantConfig: qdrantConfig,
		bus:          bus,
	}
}

// DryRun counts the vectors that a migration in the given direction would copy,
// without writing anything. It returns api.ErrVectorMigrationQdrantNotConfigured
// when no Qdrant address is set.
func (s *VectorMigrationAdminStore) DryRun(ctx context.Context, direction string, batchSize int) (*api.VectorMigrationResult, error) {
	cfg := s.qdrantConfig(ctx)
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, api.ErrVectorMigrationQdrantNotConfigured
	}
	qs, err := storage.NewQdrantStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to qdrant: %w", err)
	}
	defer func() { _ = qs.Close() }()

	stats, err := s.run(ctx, direction, qs, batchSize, true, nil)
	if err != nil {
		return nil, err
	}
	return toAPIVectorMigrationResult(direction, stats), nil
}

// Start launches a real migration in the background and returns immediately.
// Progress and the terminal result stream over the event bus under
// EventScopeVectorMigration. It returns api.ErrVectorMigrationQdrantNotConfigured
// when Qdrant is not configured and api.ErrMigrationInProgress when a migration
// is already running.
func (s *VectorMigrationAdminStore) Start(ctx context.Context, direction string, batchSize int) error {
	cfg := s.qdrantConfig(ctx)
	if strings.TrimSpace(cfg.Addr) == "" {
		return api.ErrVectorMigrationQdrantNotConfigured
	}

	if !s.guard.tryAcquire() {
		return api.ErrMigrationInProgress
	}

	// Detach from the request context so the migration survives the 202
	// response; progress is observed over SSE, not the HTTP response.
	go func() {
		bg := context.Background()
		defer s.guard.release()

		events.Emit(bg, s.bus, events.VectorMigrationStarted, events.EventScopeVectorMigration, map[string]any{
			"direction": direction,
		})

		qs, err := storage.NewQdrantStore(cfg)
		if err != nil {
			s.emitFailed(bg, direction, fmt.Errorf("connect to qdrant: %w", err))
			return
		}
		defer func() { _ = qs.Close() }()

		onProgress := func(p storage.VectorMigrateProgress) {
			events.Emit(bg, s.bus, events.VectorMigrationProgress, events.EventScopeVectorMigration, p)
		}

		stats, err := s.run(bg, direction, qs, batchSize, false, onProgress)
		if err != nil {
			s.emitFailed(bg, direction, err)
			return
		}
		events.Emit(bg, s.bus, events.VectorMigrationCompleted, events.EventScopeVectorMigration,
			toAPIVectorMigrationResult(direction, stats))
	}()

	return nil
}

func (s *VectorMigrationAdminStore) emitFailed(ctx context.Context, direction string, err error) {
	events.Emit(ctx, s.bus, events.VectorMigrationFailed, events.EventScopeVectorMigration, map[string]any{
		"direction": direction,
		"error":     err.Error(),
	})
}

// run dispatches a migration in the requested direction against the given
// Qdrant store. For from_qdrant it constructs the SQL-side destination store
// (and closes it afterward so HNSW snapshots flush).
func (s *VectorMigrationAdminStore) run(ctx context.Context, direction string, qs *storage.QdrantStore, batchSize int, dryRun bool, onProgress func(storage.VectorMigrateProgress)) (storage.VectorMigrateStats, error) {
	switch direction {
	case api.VectorMigrationToQdrant:
		return storage.MigrateVectorsToQdrant(ctx, s.db, qs, batchSize, dryRun, onProgress)
	case api.VectorMigrationFromQdrant:
		dst, cleanup, err := s.sqlVectorStore()
		if err != nil {
			return storage.VectorMigrateStats{}, err
		}
		defer cleanup()
		return storage.MigrateVectorsFromQdrant(ctx, s.db, qs, dst, batchSize, dryRun, onProgress)
	default:
		return storage.VectorMigrateStats{}, fmt.Errorf("unknown migration direction %q", direction)
	}
}

// sqlVectorStore builds the SQL-backed VectorStore for the running backend, used
// as the destination when migrating back from Qdrant. Writing through this
// store keeps the pgvector / HNSW index consistent. The returned cleanup
// releases it; for SQLite, Close flushes the HNSW snapshots so the rebuilt
// index survives a restart.
func (s *VectorMigrationAdminStore) sqlVectorStore() (storage.VectorStore, func(), error) {
	switch s.db.Backend() {
	case storage.BackendPostgres:
		pgv, err := storage.NewPgVectorStore(s.dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("open pgvector store: %w", err)
		}
		return pgv, func() { pgv.Close() }, nil
	case storage.BackendSQLite:
		h := storage.NewHNSWStore(s.db.DB(), s.db.WriteDB(), s.hnswConfig)
		return h, func() { _ = h.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown backend %q", s.db.Backend())
	}
}

func toAPIVectorMigrationResult(direction string, stats storage.VectorMigrateStats) *api.VectorMigrationResult {
	res := &api.VectorMigrationResult{
		Direction:   direction,
		DryRun:      stats.DryRun,
		MemoryCount: stats.MemoryCount,
		EntityCount: stats.EntityCount,
		Mismatch:    stats.Mismatch(),
	}
	for _, v := range stats.Verify {
		res.Verify = append(res.Verify, api.VectorMigrationDimStat{
			Kind:        v.Kind,
			Dimension:   v.Dimension,
			SourceCount: v.SourceCount,
			DestCount:   v.DestCount,
		})
	}
	return res
}
