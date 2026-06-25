package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Memory metadata keys written by the dream-cycle consolidation phase. These
// identify a memory's lineage to a parent dream cycle and the source memories
// that produced it. Treated as a contract between the dreaming package
// (writer) and any reader that needs to surface or strip the lineage.
const (
	DreamMetaCycleID         = "dream_cycle_id"
	DreamMetaSourceMemoryIDs = "source_memory_ids"
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
	DreamPhaseEntityDedup          = "entity_dedup"
	DreamPhaseEmbeddingBackfill    = "embedding_backfill"
	DreamPhaseAugmentationBackfill = "augmentation_backfill"
	DreamPhaseMultiVectorBackfill  = "multi_vector_backfill"
	// DreamPhaseConsolidationEntityBackfill recovers entity-graph coverage for
	// existing consolidation syntheses stranded before the fix that extracts
	// entities from dreams: it enqueues entity-only jobs for active consolidation
	// dreams that still have no sourced relationship.
	DreamPhaseConsolidationEntityBackfill = "consolidation_entity_backfill"
	DreamPhaseParaphraseDedup             = "paraphrase_dedup"
	DreamPhaseTransitive                  = "transitive_discovery"
	DreamPhaseContradictions              = "contradiction_detection"
	DreamPhaseConsolidation               = "consolidation"
	DreamPhasePruning                     = "pruning"
	DreamPhaseWeightAdjust                = "weight_adjustment"
	DreamPhaseProjectDescSync             = "project_description_sync"
)

// DreamSubPhase constants name the consolidation sub-phases. Mirrors the
// "sub_phase" stat values emitted by ConsolidationPhase.writePhaseSummary.
const (
	DreamSubPhaseBackfillAudit = "backfill_audit"
	DreamSubPhaseReinforce     = "reinforce"
	DreamSubPhaseConsolidate   = "consolidate"
)

// DreamSource is the reserved Source string formerly written on dream-created
// content. The dream cycle no longer writes it (it sets Origin=OriginDream and
// leaves Source nil); the constant survives only as the reserved value that
// user-facing write paths reject, so "dream" can never re-enter the Source
// column. Internal logic must branch on Origin, never on this string.
const DreamSource = "dream"

// DreamOp constants define the operation types logged during dream cycles.
const (
	DreamOpEntityMerged          = "entity_merged"
	DreamOpRelationshipCreated   = "relationship_created"
	DreamOpRelationshipUpdated   = "relationship_updated"
	DreamOpEntityUpdated         = "entity_updated"
	DreamOpContradictionDetected = "contradiction_detected"
	DreamOpParaphraseSuperseded  = "paraphrase_superseded"
	DreamOpMemoryCreated         = "memory_created"
	DreamOpMemoryDeleted         = "memory_deleted"
	DreamOpMemorySuperseded      = "memory_superseded"
	DreamOpConfidenceAdjusted    = "confidence_adjusted"
	DreamOpRelationshipExpired   = "relationship_expired"
	DreamOpMemoryRejected        = "memory_rejected"
	DreamOpMemoryDemoted         = "memory_demoted"
	DreamOpPhaseSummary          = "phase_summary"
)

// IsCountableDreamOp reports whether a dream-log operation counts toward a
// cycle's operation tally. phase_summary rows are per-phase metadata, not
// mutations, so they are excluded; every other DreamOp is a real operation.
// This is the single source of truth shared by the live counter
// (DreamLogWriter.OpCount) and the compressed retention summary
// (buildLogSummary); keep it aligned with the UI's phase_summary filter in
// ui/src/lib/dreaming.ts.
func IsCountableDreamOp(operation string) bool {
	return operation != DreamOpPhaseSummary
}

// MemorySource returns the source string for a memory, or empty string if nil.
func MemorySource(m *Memory) string {
	if m.Source != nil {
		return *m.Source
	}
	return ""
}

// DreamCycle represents a single dream processing run for a project.
//
// HeartbeatAt is updated by the runner during phase execution (independent
// of phase-boundary writes that touch UpdatedAt). IsStaleDiagnostic,
// IsAbandonable, and ProjectName are NOT persisted; they're computed at
// read time and remain zero/empty on direct repo scans. ProjectName is
// populated only by self-tier read paths that JOIN projects (the caller
// owns every returned project, so the name is theirs to see); org and
// system paths leave it empty so cross-tenant views show project_id only
// and an org_owner never learns the names of other users' projects.
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
	HeartbeatAt  *time.Time      `json:"heartbeat_at"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`

	IsStaleDiagnostic bool   `json:"is_stale_diagnostic"`
	IsAbandonable     bool   `json:"is_abandonable"`
	ProjectName       string `json:"project_name,omitempty"`
}

// DreamLog records a single mutation performed during a dream cycle,
// with before/after snapshots to support rollback.
//
// SubPhase is empty for phases that don't subdivide their work. Today only
// the consolidation phase emits values here (DreamSubPhase* constants).
// Legacy rows written before the column existed deserialize as empty.
type DreamLog struct {
	ID          uuid.UUID       `json:"id"`
	CycleID     uuid.UUID       `json:"cycle_id"`
	ProjectID   uuid.UUID       `json:"project_id"`
	Phase       string          `json:"phase"`
	SubPhase    string          `json:"sub_phase"`
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
