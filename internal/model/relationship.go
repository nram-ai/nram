package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Relationship struct {
	ID           uuid.UUID       `json:"id"`
	NamespaceID  uuid.UUID       `json:"namespace_id"`
	SourceID     uuid.UUID       `json:"source_id"`
	TargetID     uuid.UUID       `json:"target_id"`
	Relation     string          `json:"relation"`
	Weight       float64         `json:"weight"`
	Properties   json.RawMessage `json:"properties"`
	ValidFrom    time.Time       `json:"valid_from"`
	ValidUntil   *time.Time      `json:"valid_until"`
	SourceMemory *uuid.UUID      `json:"source_memory"`
	CreatedAt    time.Time       `json:"created_at"`
}
