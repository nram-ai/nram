package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// resetLogEntries clears the table so a whole-table List/Count test sees a
// blank slate. SQLite gets a fresh DB per test, but the Postgres path shares a
// schema across tests; a plain DELETE works on both backends.
func resetLogEntries(t *testing.T, ctx context.Context, db DB) {
	t.Helper()
	if _, err := db.Exec(ctx, "DELETE FROM log_entries"); err != nil {
		t.Fatalf("reset log_entries: %v", err)
	}
}

func TestLogEntryRepo_BatchCreateAndFilters(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		resetLogEntries(t, ctx, db)
		repo := NewLogEntryRepo(db)

		base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		entries := []*model.LogEntry{
			{Timestamp: base, Level: model.LogLevelInfo, Component: "enrichment",
				Message: "batch claimed", Attrs: json.RawMessage(`{"job":"j1","count":3}`)},
			{Timestamp: base.Add(1 * time.Minute), Level: model.LogLevelWarn, Component: "dreaming",
				Message: "budget low", Attrs: json.RawMessage(`{"cycle":"c1"}`)},
			{Timestamp: base.Add(2 * time.Minute), Level: model.LogLevelError, Component: "enrichment",
				Message: "extraction FAILED", Attrs: json.RawMessage(`{"job":"j2"}`)},
		}
		if err := repo.BatchCreate(ctx, entries); err != nil {
			t.Fatalf("batch create: %v", err)
		}

		// No filter: all rows, newest first.
		all, err := repo.List(ctx, LogFilter{}, 50, 0)
		if err != nil {
			t.Fatalf("list all: %v", err)
		}
		if len(all) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(all))
		}
		if all[0].Message != "extraction FAILED" {
			t.Fatalf("expected newest first, got %q", all[0].Message)
		}
		count, err := repo.Count(ctx, LogFilter{})
		if err != nil || count != 3 {
			t.Fatalf("count all: got %d err %v", count, err)
		}

		// attrs round-trips as a structured object, not flattened text.
		var gotAttrs map[string]any
		if err := json.Unmarshal(all[0].Attrs, &gotAttrs); err != nil {
			t.Fatalf("attrs not valid json: %v (%s)", err, all[0].Attrs)
		}
		if gotAttrs["job"] != "j2" {
			t.Fatalf("expected job j2 in attrs, got %v", gotAttrs)
		}

		// Level filter (OR-set).
		errs, err := repo.List(ctx, LogFilter{Levels: []string{model.LogLevelError, model.LogLevelWarn}}, 50, 0)
		if err != nil || len(errs) != 2 {
			t.Fatalf("level filter: got %d err %v", len(errs), err)
		}

		// Component filter.
		enr, err := repo.List(ctx, LogFilter{Component: "enrichment"}, 50, 0)
		if err != nil || len(enr) != 2 {
			t.Fatalf("component filter: got %d err %v", len(enr), err)
		}

		// Case-insensitive message search.
		found, err := repo.List(ctx, LogFilter{Search: "failed"}, 50, 0)
		if err != nil || len(found) != 1 || found[0].Message != "extraction FAILED" {
			t.Fatalf("search filter: got %d err %v", len(found), err)
		}

		// Structured attr filter.
		byJob, err := repo.List(ctx, LogFilter{AttrKey: "job", AttrValue: "j1"}, 50, 0)
		if err != nil || len(byJob) != 1 || byJob[0].Message != "batch claimed" {
			t.Fatalf("attr filter: got %d err %v", len(byJob), err)
		}

		// Time window: only the middle row.
		from := base.Add(30 * time.Second)
		to := base.Add(90 * time.Second)
		win, err := repo.List(ctx, LogFilter{From: &from, To: &to}, 50, 0)
		if err != nil || len(win) != 1 || win[0].Message != "budget low" {
			t.Fatalf("time window: got %d err %v", len(win), err)
		}

		// Pagination.
		page, err := repo.List(ctx, LogFilter{}, 1, 1)
		if err != nil || len(page) != 1 || page[0].Message != "budget low" {
			t.Fatalf("pagination: got %d err %v", len(page), err)
		}
	})
}

func TestLogEntryRepo_ListKeyset(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		resetLogEntries(t, ctx, db)
		repo := NewLogEntryRepo(db)

		base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		var entries []*model.LogEntry
		// Three rows share the exact same ts to exercise the (ts, id) tiebreak.
		for i := range 3 {
			entries = append(entries, &model.LogEntry{
				ID: uuid.New(), Timestamp: base, Level: model.LogLevelInfo,
				Component: "shared", Message: fmt.Sprintf("tie %d", i),
				Attrs: json.RawMessage(`{}`),
			})
		}
		// Distinct-ts rows on both sides of the shared instant.
		offsets := []time.Duration{-2 * time.Minute, -1 * time.Minute, 1 * time.Minute, 2 * time.Minute, 3 * time.Minute}
		for i, off := range offsets {
			entries = append(entries, &model.LogEntry{
				ID: uuid.New(), Timestamp: base.Add(off), Level: model.LogLevelInfo,
				Component: "spread", Message: fmt.Sprintf("row %d", i),
				Attrs: json.RawMessage(`{}`),
			})
		}
		if err := repo.BatchCreate(ctx, entries); err != nil {
			t.Fatalf("batch create: %v", err)
		}
		total := len(entries)

		// Reference: the whole set in a single page.
		want, err := repo.ListKeyset(ctx, LogFilter{}, nil, 1000)
		if err != nil {
			t.Fatalf("keyset full: %v", err)
		}
		if len(want) != total {
			t.Fatalf("full scan: want %d rows, got %d", total, len(want))
		}
		// Ordering invariant: strictly descending by (ts, id). Lexical id
		// comparison matches Postgres UUID ordering (canonical lowercase form)
		// and the SQLite TEXT column, so the check holds on both backends.
		for i := 1; i < len(want); i++ {
			prev, cur := want[i-1], want[i]
			if cur.Timestamp.After(prev.Timestamp) ||
				(cur.Timestamp.Equal(prev.Timestamp) && cur.ID.String() >= prev.ID.String()) {
				t.Fatalf("not strictly (ts DESC, id DESC) at %d: %v/%s then %v/%s",
					i, prev.Timestamp, prev.ID, cur.Timestamp, cur.ID)
			}
		}

		// Paging with a small limit must reproduce the same sequence exactly,
		// with no gaps and no duplicates.
		var paged []model.LogEntry
		var cursor *LogCursor
		for {
			batch, err := repo.ListKeyset(ctx, LogFilter{}, cursor, 2)
			if err != nil {
				t.Fatalf("keyset page: %v", err)
			}
			paged = append(paged, batch...)
			if len(batch) < 2 {
				break
			}
			last := batch[len(batch)-1]
			cursor = &LogCursor{TS: last.Timestamp, ID: last.ID}
		}
		if len(paged) != total {
			t.Fatalf("paged: want %d rows, got %d", total, len(paged))
		}
		for i := range want {
			if paged[i].ID != want[i].ID {
				t.Fatalf("paged sequence diverges at %d: want %s, got %s", i, want[i].ID, paged[i].ID)
			}
		}

		// A filter still narrows the keyset walk.
		spread, err := repo.ListKeyset(ctx, LogFilter{Component: "spread"}, nil, 1000)
		if err != nil {
			t.Fatalf("keyset filtered: %v", err)
		}
		if len(spread) != len(offsets) {
			t.Fatalf("filtered: want %d rows, got %d", len(offsets), len(spread))
		}
	})
}

func TestLogEntryRepo_Retention(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		resetLogEntries(t, ctx, db)
		repo := NewLogEntryRepo(db)

		base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
		var entries []*model.LogEntry
		for i := range 10 {
			entries = append(entries, &model.LogEntry{
				Timestamp: base.Add(time.Duration(i) * time.Hour),
				Level:     model.LogLevelInfo,
				Component: "test",
				Message:   "row",
				Attrs:     json.RawMessage(`{}`),
			})
		}
		if err := repo.BatchCreate(ctx, entries); err != nil {
			t.Fatalf("batch create: %v", err)
		}

		// Age-based: drop everything before base+5h (rows 0..4).
		cutoff := base.Add(5 * time.Hour)
		deleted, err := repo.DeleteOlderThan(ctx, cutoff)
		if err != nil {
			t.Fatalf("delete older than: %v", err)
		}
		if deleted != 5 {
			t.Fatalf("expected 5 deleted by age, got %d", deleted)
		}
		count, _ := repo.Count(ctx, LogFilter{})
		if count != 5 {
			t.Fatalf("expected 5 remaining after age sweep, got %d", count)
		}

		// Count-based: keep newest 2 of the remaining 5.
		deleted, err = repo.DeleteBeyondCount(ctx, 2)
		if err != nil {
			t.Fatalf("delete beyond count: %v", err)
		}
		if deleted != 3 {
			t.Fatalf("expected 3 deleted by count, got %d", deleted)
		}
		remaining, err := repo.List(ctx, LogFilter{}, 50, 0)
		if err != nil {
			t.Fatalf("list remaining: %v", err)
		}
		if len(remaining) != 2 {
			t.Fatalf("expected 2 remaining after count sweep, got %d", len(remaining))
		}
		// The two newest (base+8h, base+9h) survive, newest first.
		if !remaining[0].Timestamp.Equal(base.Add(9 * time.Hour)) {
			t.Fatalf("expected newest survivor at +9h, got %v", remaining[0].Timestamp)
		}

		// keep <= 0 is a no-op.
		deleted, err = repo.DeleteBeyondCount(ctx, 0)
		if err != nil || deleted != 0 {
			t.Fatalf("expected no-op for keep=0, got %d err %v", deleted, err)
		}
	})
}
