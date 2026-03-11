package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type MemoryLineage struct {
	ID        uuid.UUID       `json:"id"`
	MemoryID  uuid.UUID       `json:"memory_id"`
	ParentID  *uuid.UUID      `json:"parent_id"`
	Relation  string          `json:"relation"`
	Context   json.RawMessage `json:"context"`
	CreatedAt time.Time       `json:"created_at"`
}
