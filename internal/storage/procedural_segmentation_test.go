package storage

import (
	"context"
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

// TestProceduralRepo_NoEnrichmentEnqueueAndVerbatim is the regression guard for
// the tier's core invariant: storing a procedural entry must NOT enqueue any
// enrichment work, and the content must read back byte-for-byte. If a future
// change wires procedural into the enrichment path, this fails.
func TestProceduralRepo_NoEnrichmentEnqueueAndVerbatim(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		user := createTestUser(t, ctx, db)
		repo := NewProceduralRepo(db)

		var before int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM enrichment_queue`).Scan(&before); err != nil {
			t.Fatalf("count enrichment_queue before: %v", err)
		}

		// Content deliberately includes an em dash and trailing whitespace to
		// prove no normalization/rewrite happens.
		content := "Hard stop — never paper over a root cause.  "
		e := &model.ProceduralEntry{
			NamespaceID: user.NamespaceID,
			Content:     content,
			Enabled:     true,
		}
		if _, err := repo.GetByID(ctx, e.ID); err == nil {
			t.Fatal("expected unsaved entry to be absent")
		}
		if err := repo.Create(ctx, e); err != nil {
			t.Fatalf("create: %v", err)
		}

		var after int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM enrichment_queue`).Scan(&after); err != nil {
			t.Fatalf("count enrichment_queue after: %v", err)
		}
		if after != before {
			t.Fatalf("procedural store enqueued enrichment work: before=%d after=%d", before, after)
		}

		got, err := repo.GetByID(ctx, e.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Content != content {
			t.Fatalf("content not verbatim: stored %q got %q", content, got.Content)
		}
	})
}
