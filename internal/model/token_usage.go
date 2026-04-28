package model

import (
	"time"

	"github.com/google/uuid"
)

// UsageContext holds the ownership IDs for a project namespace, used to
// attribute token usage records to the correct org/user/project.
type UsageContext struct {
	OrgID     *uuid.UUID
	UserID    *uuid.UUID
	ProjectID *uuid.UUID
}

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
	Success      bool       `json:"success"`
	ErrorCode    *string    `json:"error_code"`
	RequestID    *string    `json:"request_id"`
	CreatedAt    time.Time  `json:"created_at"`
}
