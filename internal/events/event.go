package events

import (
	"encoding/json"
	"time"
)

// Event type constants.
const (
	MemoryCreated          = "memory.created"
	MemoryEnriched         = "memory.enriched"
	MemoryUpdated          = "memory.updated"
	MemoryDeleted          = "memory.deleted"
	MemoryReinforced       = "memory.reinforced"
	EntityCreated          = "entity.created"
	RelationshipCreated    = "relationship.created"
	RelationshipExpired    = "relationship.expired"
	RelationshipReinforced = "relationship.reinforced"
	ConflictDetected       = "conflict.detected"
	EnrichmentFailed       = "enrichment.failed"
	ProjectDeleted         = "project.deleted"
	// ProjectUpdated fires when a project's description is created or changed.
	// The dreaming DirtyTracker turns it into a MarkDirty so the
	// project_description_sync phase runs and reconciles the backing memory.
	ProjectUpdated = "project.updated"

	// Dream lifecycle events.
	DreamCycleStarted    = "dream.cycle.started"
	DreamCycleCompleted  = "dream.cycle.completed"
	DreamCycleFailed     = "dream.cycle.failed"
	DreamCycleRolledBack = "dream.cycle.rolled_back"

	// Dream progress events. Emitted from the runner and per-LLM-call sites
	// inside phases so the admin UI can show live progress without waiting
	// on phase-boundary database writes (slow providers can hold a single
	// LLM call for >100s, which would otherwise look hung).
	DreamPhaseStarted   = "dream.phase.started"
	DreamPhaseCompleted = "dream.phase.completed"
	DreamPhaseProgress  = "dream.phase.progress"
	DreamCallStarted    = "dream.call.started"
	DreamCallCompleted  = "dream.call.completed"
	DreamCycleHeartbeat = "dream.cycle.heartbeat"

	// Enrichment progress events. Per-job lifecycle plus a periodic pool
	// tick. We do not heartbeat every job (would be far too chatty at
	// scale); the pool tick carries in-flight count and oldest claim age.
	EnrichmentJobStarted   = "enrichment.job.started"
	EnrichmentJobCompleted = "enrichment.job.completed"
	EnrichmentPoolTick     = "enrichment.pool.tick"
	// Emitted by the StuckJobSweeper when it auto-requeues a presumed-dead
	// worker's job. Payload is built in internal/enrichment/sweeper.go.
	EnrichmentJobRequeued = "enrichment.job.requeued"

	// Vector migration progress events. The admin vector migration (to/from
	// Qdrant) runs in the background and streams progress so the UI does not
	// hold a multi-minute request open. Scope: EventScopeVectorMigration.
	VectorMigrationStarted   = "vector_migration.started"
	VectorMigrationProgress  = "vector_migration.progress"
	VectorMigrationCompleted = "vector_migration.completed"
	VectorMigrationFailed    = "vector_migration.failed"

	// SQLite-to-Postgres data migration progress events. Same rationale:
	// the migration runs in the background and streams per-table progress.
	// Scope: EventScopeDBMigration.
	DBMigrationStarted   = "db_migration.started"
	DBMigrationProgress  = "db_migration.progress"
	DBMigrationCompleted = "db_migration.completed"
	DBMigrationFailed    = "db_migration.failed"

	// Maintenance events. Raised by any background operation that degrades
	// server performance while it runs (starting with the SQLite VACUUM), so
	// the UI can show a maintenance banner. Scope: EventScopeMaintenance.
	MaintenanceStarted = "maintenance.started"
	MaintenanceEnded   = "maintenance.ended"
)

// Scope constants for background admin operations the UI subscribes to.
const (
	EventScopeVectorMigration = "vector-migration"
	EventScopeDBMigration     = "db-migration"
	EventScopeMaintenance     = "maintenance"
)

// Event represents a single event in the system.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Scope     string          `json:"scope"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}
