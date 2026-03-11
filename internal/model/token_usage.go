package model

import (
	"time"

	"github.com/google/uuid"
)

type TokenUsage struct {
	ID           uuid.UUID  `json:"id"`
	OrgID        *uuid.UUID `json:"org_id"`
	UserID       *uuid.UUID `json:"user_id"`
	ProjectID    *uuid.UUID `json:"project_id"`
	NamespaceID  uuid.UUID  `json:"namespace_id"`
	Operation    string     `json:"operation"`
	Provider     string     `json:"provider"`
	Model        string     `json:"model"`
	TokensInput  int        `json:"tokens_input"`
	TokensOutput int        `json:"tokens_output"`
	MemoryID     *uuid.UUID `json:"memory_id"`
	APIKeyID     *uuid.UUID `json:"api_key_id"`
	LatencyMs    *int       `json:"latency_ms"`
	CreatedAt    time.Time  `json:"created_at"`
}
