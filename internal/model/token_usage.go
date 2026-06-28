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

// NewUsageContext builds a UsageContext from a request's ownership IDs, taking
// the address of fresh copies so callers do not have to. A zero orgID is left
// nil (a NULL org_id row is dropped from the caller-scoped analytics view, so
// callers should pass a real org when they have one).
func NewUsageContext(userID *uuid.UUID, projectID, orgID uuid.UUID) *UsageContext {
	uc := &UsageContext{UserID: userID, ProjectID: &projectID}
	if orgID != uuid.Nil {
		uc.OrgID = &orgID
	}
	return uc
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
	// CycleID attributes a row to the dream cycle that incurred the call.
	// Non-dream callers leave this nil; dream_cycles.tokens_used is computed
	// live from SUM(...) WHERE cycle_id = dream_cycles.id.
	CycleID   *uuid.UUID `json:"cycle_id"`
	CreatedAt time.Time  `json:"created_at"`
}
