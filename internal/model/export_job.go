package model

import (
	"time"

	"github.com/google/uuid"
)

// ExportJob.Status values. Mirrors the schema's CHECK-able set; constants
// catch typos at compile time across the repo/service/handler layer.
const (
	ExportStatusPending    = "pending"
	ExportStatusProcessing = "processing"
	ExportStatusSucceeded  = "succeeded"
	ExportStatusFailed     = "failed"
	ExportStatusExpired    = "expired"
)

// ExportJob.Scope values. "account" enumerates every project the user owns
// and bundles them into a zip; "project" exports a single project.
const (
	ExportScopeAccount = "account"
	ExportScopeProject = "project"
)

// ExportJob.Format values. "zip" is the multi-project archive format used
// by account-wide exports. "json" and "ndjson" are reserved for future
// per-project async paths; the synchronous /v1/projects/.../export endpoint
// already covers per-project exports today.
const (
	ExportFormatZip    = "zip"
	ExportFormatJSON   = "json"
	ExportFormatNDJSON = "ndjson"
)

// ExportJob represents a queued or completed export request. One row per
// user request. Memory content never leaves this row; the worker writes
// it to a filesystem artifact whose path is captured in ArtifactPath.
type ExportJob struct {
	ID                uuid.UUID  `json:"id"`
	UserID            uuid.UUID  `json:"user_id"`
	Scope             string     `json:"scope"`
	ProjectID         *uuid.UUID `json:"project_id,omitempty"`
	Format            string     `json:"format"`
	IncludeSuperseded bool       `json:"include_superseded"`
	Status            string     `json:"status"`
	ArtifactPath      *string    `json:"artifact_path,omitempty"`
	ArtifactBytes     *int64     `json:"artifact_bytes,omitempty"`
	ArtifactSHA256    *string    `json:"artifact_sha256,omitempty"`
	Error             *string    `json:"error,omitempty"`
	ClaimedBy         *string    `json:"claimed_by,omitempty"`
	ClaimedAt         *time.Time `json:"claimed_at,omitempty"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}
