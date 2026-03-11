package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Entity struct {
	ID           uuid.UUID       `json:"id"`
	NamespaceID  uuid.UUID       `json:"namespace_id"`
	Name         string          `json:"name"`
	Canonical    string          `json:"canonical"`
	EntityType   string          `json:"entity_type"`
	EmbeddingDim *int            `json:"embedding_dim"`
	Properties   json.RawMessage `json:"properties"`
	MentionCount int             `json:"mention_count"`
	Metadata     json.RawMessage `json:"metadata"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

type EntityAlias struct {
	ID        uuid.UUID `json:"id"`
	EntityID  uuid.UUID `json:"entity_id"`
	Alias     string    `json:"alias"`
	AliasType string    `json:"alias_type"`
	CreatedAt time.Time `json:"created_at"`
}
