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
	Path             string          `json:"path"`
	Description      string          `json:"description"`
	DefaultTags      []string        `json:"default_tags"`
	Settings         json.RawMessage `json:"settings"`
	MemoryCount      int             `json:"memory_count"`
	EntityCount      int             `json:"entity_count"`
	Owner            *ProjectOwner   `json:"owner,omitempty"`
	Organization     *ProjectOrg     `json:"organization,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// ProjectOwner is the embedded owner info in project responses.
type ProjectOwner struct {
	ID    uuid.UUID `json:"id"`
	Email string    `json:"email"`
}

// ProjectOrg is the embedded organization info in project responses.
type ProjectOrg struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}
