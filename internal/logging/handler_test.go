package logging

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

// captureEnqueuer records everything enqueued for the DB sink.
type captureEnqueuer struct {
	records []Record
}

func (c *captureEnqueuer) Enqueue(r Record) { c.records = append(c.records, r) }

// newTestLogger wires a FanoutHandler with a discard console at info and a
// capture enqueuer, returning the logger, the capture, and the DB config.
func newTestLogger(t *testing.T, dbLevel slog.Level) (*slog.Logger, *captureEnqueuer, *DBConfig) {
	t.Helper()
	console := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})
	cfg := NewDBConfig(true, dbLevel)
	cap := &captureEnqueuer{}
	h := NewFanoutHandler(console, cfg, cap)
	return slog.New(h), cap, cfg
}

func TestFanoutHandler_StructuredAttrsPreserved(t *testing.T) {
	logger, cap, _ := newTestLogger(t, slog.LevelInfo)

	logger.Info("enrichment: batch claimed",
		"job", "j1",
		"count", 3,
		"ratio", 0.5,
		"ok", true,
		slog.Group("worker", slog.String("id", "w7"), slog.Int("slot", 2)),
	)

	if len(cap.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(cap.records))
	}
	r := cap.records[0]
	if r.Level != "info" {
		t.Fatalf("level: got %q", r.Level)
	}
	if r.Component != "enrichment" {
		t.Fatalf("component from message prefix: got %q", r.Component)
	}
	if r.Attrs["job"] != "j1" {
		t.Fatalf("string attr: got %v", r.Attrs["job"])
	}
	if r.Attrs["count"] != int64(3) {
		t.Fatalf("int attr type/value: got %#v", r.Attrs["count"])
	}
	if r.Attrs["ratio"] != 0.5 {
		t.Fatalf("float attr: got %#v", r.Attrs["ratio"])
	}
	if r.Attrs["ok"] != true {
		t.Fatalf("bool attr: got %#v", r.Attrs["ok"])
	}
	worker, ok := r.Attrs["worker"].(map[string]any)
	if !ok {
		t.Fatalf("group attr should nest as map, got %#v", r.Attrs["worker"])
	}
	if worker["id"] != "w7" || worker["slot"] != int64(2) {
		t.Fatalf("nested group values: got %#v", worker)
	}
}

func TestFanoutHandler_ComponentAttrWins(t *testing.T) {
	logger, cap, _ := newTestLogger(t, slog.LevelInfo)
	Named := logger.With(slog.String(ComponentKey, "dreaming"))
	Named.Warn("something happened", "cycle", "c1")

	r := cap.records[0]
	if r.Component != "dreaming" {
		t.Fatalf("component attr should win: got %q", r.Component)
	}
	if _, present := r.Attrs[ComponentKey]; present {
		t.Fatalf("component attr should be lifted out of attrs map")
	}
	if r.Attrs["cycle"] != "c1" {
		t.Fatalf("remaining attr lost: got %#v", r.Attrs)
	}
}

func TestFanoutHandler_LiftsTenantIDs(t *testing.T) {
	logger, cap, _ := newTestLogger(t, slog.LevelInfo)
	pid := uuid.New()
	logger.Info("recall: served", "project", pid.String())

	r := cap.records[0]
	if r.ProjectID == nil || *r.ProjectID != pid {
		t.Fatalf("project id should be lifted: got %v", r.ProjectID)
	}
	// It also remains visible in the structured attrs.
	if r.Attrs["project"] != pid.String() {
		t.Fatalf("project attr should remain in map: got %#v", r.Attrs["project"])
	}
}

func TestFanoutHandler_DBLevelGating(t *testing.T) {
	// Console at info, DB level at warn: an info record reaches the console but
	// not the DB sink; a warn record reaches both.
	console := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})
	cfg := NewDBConfig(true, slog.LevelWarn)
	cap := &captureEnqueuer{}
	logger := slog.New(NewFanoutHandler(console, cfg, cap))

	logger.Info("informational")
	if len(cap.records) != 0 {
		t.Fatalf("info should not reach DB at warn level, got %d", len(cap.records))
	}
	logger.Warn("warning")
	if len(cap.records) != 1 {
		t.Fatalf("warn should reach DB, got %d", len(cap.records))
	}

	// Disabling capture stops new records without a restart.
	cfg.SetEnabled(false)
	logger.Error("boom")
	if len(cap.records) != 1 {
		t.Fatalf("capture disabled should stop DB writes, got %d", len(cap.records))
	}
}

func TestAsyncWriter_FlushAndDrop(t *testing.T) {
	sink := &countingSink{}
	w := NewAsyncWriter(sink, WriterOptions{BufferSize: 2, MaxBatch: 10, FlushInterval: 20 * time.Millisecond})
	w.Start()

	for i := range 5 {
		w.Enqueue(Record{Message: "m", Level: "info", Attrs: map[string]any{"i": i}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	// With a buffer of 2, some of the 5 may have been dropped; written+dropped
	// must account for everything offered and at least something was written.
	if got := sink.count() + int(w.Dropped()); got != 5 {
		t.Fatalf("written+dropped = %d, want 5", got)
	}
}

type countingSink struct {
	n int
}

func (s *countingSink) count() int { return s.n }
func (s *countingSink) Write(_ context.Context, records []Record) error {
	s.n += len(records)
	return nil
}
