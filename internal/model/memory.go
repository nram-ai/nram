package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Memory struct {
	ID           uuid.UUID       `json:"id"`
	NamespaceID  uuid.UUID       `json:"namespace_id"`
	Content      string          `json:"content"`
	EmbeddingDim *int            `json:"embedding_dim"`
	Source       *string         `json:"source"`
	Tags         []string        `json:"tags"`
	Confidence   float64         `json:"confidence"`
	Importance   float64         `json:"importance"`
	AccessCount  int             `json:"access_count"`
	LastAccessed *time.Time      `json:"last_accessed"`
	ExpiresAt    *time.Time      `json:"expires_at"`
	SupersededBy *uuid.UUID      `json:"superseded_by"`
	SupersededAt *time.Time      `json:"superseded_at"`
	Enriched     bool            `json:"enriched"`
	Metadata     json.RawMessage `json:"metadata"`
	ContentHash  string          `json:"content_hash,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	DeletedAt    *time.Time      `json:"deleted_at"`
	PurgeAfter   *time.Time      `json:"purge_after"`

	// Derived from lineage table at read time; not persisted in memories table.
	ParentID *uuid.UUID `json:"parent_id,omitempty"`
}

type SystemMeta struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
