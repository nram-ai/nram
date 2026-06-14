// Package logging provides the unified diagnostic-logging abstraction that all
// of nram's structured logging flows through. A FanoutHandler installed as the
// default slog handler tees every record to the console (unchanged) and,
// asynchronously, to a pluggable Sink. The SQL sink persists records to the
// log_entries table for the operator Logs view; the Sink interface leaves room
// for an external log provider later without touching any call site.
package logging

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ComponentKey is the well-known attribute key a logger sets to tag the
// subsystem a record came from (e.g. "enrichment", "dreaming"). Named() presets
// it; the handler also falls back to parsing a "word:" message prefix.
const ComponentKey = "component"

// Record is one captured log line in the package's own representation,
// independent of slog and of any storage type. Attrs holds the structured
// fields as a JSON-friendly map (string/number/bool/time/duration/nested map),
// never flattened into Message.
type Record struct {
	Time        time.Time
	Level       string
	Component   string
	Message     string
	Attrs       map[string]any
	ProjectID   *uuid.UUID
	NamespaceID *uuid.UUID
	UserID      *uuid.UUID
}

// Sink consumes batches of records. Implementations must be safe for use from
// the single writer goroutine and should treat ctx cancellation as a deadline.
type Sink interface {
	Write(ctx context.Context, records []Record) error
}

// enqueuer is the subset of the async writer the handler depends on, so the
// handler can be constructed with a nil/no-op writer in tests.
type enqueuer interface {
	Enqueue(Record)
}
