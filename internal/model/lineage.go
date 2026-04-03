package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Lineage relation constants.
const (
	LineageConflictsWith   = "conflicts_with"
	LineageSynthesizedFrom = "synthesized_from"
	LineageExtractedFrom   = "extracted_from"
	LineageExtractedFact   = "extracted_fact"
	LineageSupersedes      = "supersedes"
)

type MemoryLineage struct {
	ID          uuid.UUID       `json:"id"`
	NamespaceID uuid.UUID       `json:"namespace_id"`
	MemoryID    uuid.UUID       `json:"memory_id"`
	ParentID    *uuid.UUID      `json:"parent_id"`
	Relation    string          `json:"relation"`
	Context     json.RawMessage `json:"context"`
	CreatedAt   time.Time       `json:"created_at"`
}
