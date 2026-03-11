package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Namespace struct {
	ID        uuid.UUID       `json:"id"`
	Name      string          `json:"name"`
	Slug      string          `json:"slug"`
	Kind      string          `json:"kind"`
	ParentID  *uuid.UUID      `json:"parent_id"`
	Path      string          `json:"path"`
	Depth     int             `json:"depth"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}
