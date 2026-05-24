package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Enrichment step names persisted into EnrichmentJob.StepsCompleted by the
// worker as it finishes each phase. Read by runPreEmbed to short-circuit
// retries of jobs that already advanced past a phase before failing.
const (
	StepFactExtraction    = "fact_extraction"
	StepEntityExtraction  = "entity_extraction"
	StepQueryAugmentation = "query_augmentation"
	StepEmbedding         = "embedding"
)

// EnrichmentJob.Status values. Mirrors the schema's set; using these
// constants instead of raw strings catches typos at compile time.
const (
	EnrichmentStatusPending    = "pending"
	EnrichmentStatusProcessing = "processing"
	EnrichmentStatusCompleted  = "completed"
	EnrichmentStatusFailed     = "failed"
)

type EnrichmentJob struct {
	ID                uuid.UUID       `json:"id"`
	MemoryID          uuid.UUID       `json:"memory_id"`
	NamespaceID       uuid.UUID       `json:"namespace_id"`
	Status            string          `json:"status"`
	Priority          int             `json:"priority"`
	ClaimedAt         *time.Time      `json:"claimed_at"`
	ClaimedBy         *string         `json:"claimed_by"`
	HeartbeatAt       *time.Time      `json:"heartbeat_at"`
	Attempts          int             `json:"attempts"`
	MaxAttempts       int             `json:"max_attempts"`
	LastError         json.RawMessage `json:"last_error"`
	LastRequeueReason *string         `json:"last_requeue_reason"`
	StepsCompleted    json.RawMessage `json:"steps_completed"`
	CompletedAt       *time.Time      `json:"completed_at"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}
