package events

import (
	"encoding/json"
	"time"
)

// Event type constants.
const (
	MemoryCreated      = "memory.created"
	MemoryEnriched     = "memory.enriched"
	MemoryUpdated      = "memory.updated"
	MemoryDeleted      = "memory.deleted"
	MemoryReinforced   = "memory.reinforced"
	EntityCreated      = "entity.created"
	RelationshipCreated    = "relationship.created"
	RelationshipExpired    = "relationship.expired"
	RelationshipReinforced = "relationship.reinforced"
	ConflictDetected   = "conflict.detected"
	EnrichmentFailed   = "enrichment.failed"
	ProjectDeleted     = "project.deleted"

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
)

// Event represents a single event in the system.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Scope     string          `json:"scope"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}
