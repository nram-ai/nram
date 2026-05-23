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

// BatchCreateResult is the outcome of a RelationshipRepo BatchCreate call.
// Affected counts rows that were inserted or upserted via the ON CONFLICT
// path. Skipped counts rows dropped because of per-row constraint
// violations (foreign-key, or unique outside the upsert key) that the
// batch tolerated and continued past.
type BatchCreateResult struct {
	Affected int64 `json:"affected"`
	Skipped  int64 `json:"skipped"`
}

// ReinforceItem is one entry in a BatchReinforce call: the relationship
// id and the delta to add to its weight (clamped at 2.0 by the SQL).
type ReinforceItem struct {
	ID    uuid.UUID `json:"id"`
	Delta float64   `json:"delta"`
}

// WeightUpdateItem is one entry in a BatchUpdateWeight call: the
// relationship id and the new absolute weight to set.
type WeightUpdateItem struct {
	ID     uuid.UUID `json:"id"`
	Weight float64   `json:"weight"`
}
