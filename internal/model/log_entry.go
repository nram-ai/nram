package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Log level names persisted in log_entries.level. These mirror the four slog
// levels the application uses; "trace" is folded into "debug" at capture time.
const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

// LogEntry is one diagnostic log record captured from the structured logger and
// persisted to the log_entries table for the operator Logs view.
//
// It is system-global, not tenant-scoped: ProjectID, NamespaceID, and UserID
// are optional and set only when the originating log carried them as
// attributes. Attrs holds the structured key/value fields as a JSON object,
// preserved verbatim (not flattened into Message).
type LogEntry struct {
	ID          uuid.UUID       `json:"id"`
	Timestamp   time.Time       `json:"ts"`
	Level       string          `json:"level"`
	Component   string          `json:"component,omitempty"`
	Message     string          `json:"message"`
	Attrs       json.RawMessage `json:"attrs"`
	ProjectID   *uuid.UUID      `json:"project_id,omitempty"`
	NamespaceID *uuid.UUID      `json:"namespace_id,omitempty"`
	UserID      *uuid.UUID      `json:"user_id,omitempty"`
}
