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
	Enriched     bool            `json:"enriched"`
	Metadata     json.RawMessage `json:"metadata"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	DeletedAt    *time.Time      `json:"deleted_at"`
	PurgeAfter   *time.Time      `json:"purge_after"`
}

type SystemMeta struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
