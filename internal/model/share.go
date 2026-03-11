package model

import (
	"time"

	"github.com/google/uuid"
)

type MemoryShare struct {
	ID         uuid.UUID  `json:"id"`
	SourceNsID uuid.UUID  `json:"source_ns_id"`
	TargetNsID uuid.UUID  `json:"target_ns_id"`
	Permission string     `json:"permission"`
	CreatedBy  *uuid.UUID `json:"created_by"`
	ExpiresAt  *time.Time `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
	CreatedAt  time.Time  `json:"created_at"`
}
