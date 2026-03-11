package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EnrichmentJob struct {
	ID             uuid.UUID       `json:"id"`
	MemoryID       uuid.UUID       `json:"memory_id"`
	NamespaceID    uuid.UUID       `json:"namespace_id"`
	Status         string          `json:"status"`
	Priority       int             `json:"priority"`
	ClaimedAt      *time.Time      `json:"claimed_at"`
	ClaimedBy      *string         `json:"claimed_by"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LastError      json.RawMessage `json:"last_error"`
	StepsCompleted json.RawMessage `json:"steps_completed"`
	CompletedAt    *time.Time      `json:"completed_at"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}
