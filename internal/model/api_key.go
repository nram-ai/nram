package model

import (
	"time"

	"github.com/google/uuid"
)

type APIKey struct {
	ID        uuid.UUID   `json:"id"`
	UserID    uuid.UUID   `json:"user_id"`
	KeyPrefix string      `json:"key_prefix"`
	KeyHash   string      `json:"-"`
	Name      string      `json:"name"`
	Scopes    []uuid.UUID `json:"scopes"`
	LastUsed  *time.Time  `json:"last_used"`
	ExpiresAt *time.Time  `json:"expires_at"`
	CreatedAt time.Time   `json:"created_at"`
}
