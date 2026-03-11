package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Project struct {
	ID               uuid.UUID       `json:"id"`
	NamespaceID      uuid.UUID       `json:"namespace_id"`
	OwnerNamespaceID uuid.UUID       `json:"owner_namespace_id"`
	Name             string          `json:"name"`
	Slug             string          `json:"slug"`
	Description      string          `json:"description"`
	DefaultTags      []string        `json:"default_tags"`
	Settings         json.RawMessage `json:"settings"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}
