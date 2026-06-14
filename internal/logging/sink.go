package logging

import (
	"context"
	"encoding/json"

	"github.com/nram-ai/nram/internal/model"
)

// LogBatchWriter is the storage capability the SQL sink needs: a batched insert
// of log entries. *storage.LogEntryRepo satisfies it. Declaring it here (rather
// than importing storage) keeps the logging package free of a storage import.
type LogBatchWriter interface {
	BatchCreate(ctx context.Context, entries []*model.LogEntry) error
}

// SQLSink persists records to the log_entries table via a LogBatchWriter.
type SQLSink struct {
	repo LogBatchWriter
}

// NewSQLSink creates a SQL sink backed by the given batch writer.
func NewSQLSink(repo LogBatchWriter) *SQLSink {
	return &SQLSink{repo: repo}
}

// Write converts records to model.LogEntry rows and inserts them in one batch.
// The Attrs map is marshaled to a JSON object; a marshal failure degrades to an
// empty object rather than dropping the row's message.
func (s *SQLSink) Write(ctx context.Context, records []Record) error {
	entries := make([]*model.LogEntry, 0, len(records))
	for _, r := range records {
		attrs := json.RawMessage("{}")
		if len(r.Attrs) > 0 {
			if b, err := json.Marshal(r.Attrs); err == nil {
				attrs = b
			}
		}
		entries = append(entries, &model.LogEntry{
			Timestamp:   r.Time,
			Level:       r.Level,
			Component:   r.Component,
			Message:     r.Message,
			Attrs:       attrs,
			ProjectID:   r.ProjectID,
			NamespaceID: r.NamespaceID,
			UserID:      r.UserID,
		})
	}
	return s.repo.BatchCreate(ctx, entries)
}
