package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID           uuid.UUID       `json:"id"`
	Email        string          `json:"email"`
	DisplayName  string          `json:"display_name"`
	PasswordHash *string         `json:"-"`
	OrgID        uuid.UUID       `json:"org_id"`
	NamespaceID  uuid.UUID       `json:"namespace_id"`
	Role         string          `json:"role"`
	Settings     json.RawMessage `json:"settings"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	LastLogin    *time.Time      `json:"last_login"`
	DisabledAt   *time.Time      `json:"disabled_at"`
}
