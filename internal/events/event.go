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
	EntityCreated      = "entity.created"
	RelationshipCreated = "relationship.created"
	RelationshipExpired = "relationship.expired"
	ConflictDetected   = "conflict.detected"
	EnrichmentFailed   = "enrichment.failed"
	ProjectDeleted     = "project.deleted"
)

// Event represents a single event in the system.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Scope     string          `json:"scope"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}
