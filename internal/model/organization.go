package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Organization struct {
	ID          uuid.UUID       `json:"id"`
	NamespaceID uuid.UUID       `json:"namespace_id"`
	Name        string          `json:"name"`
	Slug        string          `json:"slug"`
	Settings    json.RawMessage `json:"settings"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}
