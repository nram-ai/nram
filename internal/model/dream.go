package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DreamCycleStatus constants define the lifecycle of a dream cycle.
const (
	DreamStatusPending    = "pending"
	DreamStatusRunning    = "running"
	DreamStatusCompleted  = "completed"
	DreamStatusFailed     = "failed"
	DreamStatusRolledBack = "rolled_back"
)

// DreamPhase constants define the ordered phases of a dream cycle.
const (
	DreamPhaseEntityDedup     = "entity_dedup"
	DreamPhaseTransitive      = "transitive_discovery"
	DreamPhaseContradictions  = "contradiction_detection"
	DreamPhaseConsolidation   = "consolidation"
	DreamPhasePruning         = "pruning"
	DreamPhaseWeightAdjust    = "weight_adjustment"
)

// DreamSource is the memory source value for dream-created content.
const DreamSource = "dream"

// DreamOp constants define the operation types logged during dream cycles.
const (
	DreamOpEntityMerged          = "entity_merged"
	DreamOpRelationshipCreated   = "relationship_created"
	DreamOpRelationshipUpdated   = "relationship_updated"
	DreamOpEntityUpdated         = "entity_updated"
	DreamOpContradictionDetected = "contradiction_detected"
	DreamOpMemoryCreated         = "memory_created"
	DreamOpMemoryDeleted         = "memory_deleted"
	DreamOpMemorySuperseded      = "memory_superseded"
	DreamOpConfidenceAdjusted    = "confidence_adjusted"
)

// MemorySource returns the source string for a memory, or empty string if nil.
func MemorySource(m *Memory) string {
	if m.Source != nil {
		return *m.Source
	}
	return ""
}

// DreamCycle represents a single dream processing run for a project.
type DreamCycle struct {
	ID           uuid.UUID       `json:"id"`
	ProjectID    uuid.UUID       `json:"project_id"`
	NamespaceID  uuid.UUID       `json:"namespace_id"`
	Status       string          `json:"status"`
	Phase        string          `json:"phase"`
	TokensUsed   int             `json:"tokens_used"`
	TokenBudget  int             `json:"token_budget"`
	PhaseSummary json.RawMessage `json:"phase_summary"`
	Error        *string         `json:"error"`
	StartedAt    *time.Time      `json:"started_at"`
	CompletedAt  *time.Time      `json:"completed_at"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// DreamLog records a single mutation performed during a dream cycle,
// with before/after snapshots to support rollback.
type DreamLog struct {
	ID          uuid.UUID       `json:"id"`
	CycleID     uuid.UUID       `json:"cycle_id"`
	ProjectID   uuid.UUID       `json:"project_id"`
	Phase       string          `json:"phase"`
	Operation   string          `json:"operation"`
	TargetType  string          `json:"target_type"`
	TargetID    uuid.UUID       `json:"target_id"`
	BeforeState json.RawMessage `json:"before_state"`
	AfterState  json.RawMessage `json:"after_state"`
	CreatedAt   time.Time       `json:"created_at"`
}

// DreamLogSummary is a compressed version of dream logs retained after
// the detail retention window expires.
type DreamLogSummary struct {
	ID        uuid.UUID       `json:"id"`
	CycleID   uuid.UUID       `json:"cycle_id"`
	ProjectID uuid.UUID       `json:"project_id"`
	Summary   json.RawMessage `json:"summary"`
	CreatedAt time.Time       `json:"created_at"`
}

// DirtyProject represents a project that has pending user-originated
// changes since its last dream cycle.
type DirtyProject struct {
	ProjectID   uuid.UUID  `json:"project_id"`
	DirtySince  time.Time  `json:"dirty_since"`
	LastDreamAt *time.Time `json:"last_dream_at"`
}
