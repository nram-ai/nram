package storage

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func TestMemoryRepo_BatchCreate(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		const n = 7
		mems := make([]*model.Memory, n)
		for i := range mems {
			m := newTestMemory(nsID)
			m.ID = uuid.New()
			m.Content = "batch memory " + uuid.NewString()
			m.ContentHash = HashContent(m.Content)
			mems[i] = m
		}

		if err := repo.BatchCreate(ctx, mems); err != nil {
			t.Fatalf("BatchCreate: %v", err)
		}

		for _, m := range mems {
			got, err := repo.GetByID(ctx, m.ID, nsID)
			if err != nil {
				t.Fatalf("GetByID(%s): %v", m.ID, err)
			}
			if got.Content != m.Content || got.ContentHash != m.ContentHash {
				t.Errorf("round-trip mismatch: got content %q hash %q", got.Content, got.ContentHash)
			}
		}

		if err := repo.BatchCreate(ctx, nil); err != nil {
			t.Errorf("BatchCreate(nil): %v", err)
		}
	})
}

func TestIngestionLogRepo_BatchCreate(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewIngestionLogRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		const n = 5
		logs := make([]*model.IngestionLog, n)
		for i := range logs {
			logs[i] = &model.IngestionLog{
				NamespaceID: nsID,
				Source:      "batch-test",
				RawContent:  "raw " + uuid.NewString(),
				Status:      "completed",
			}
		}
		if err := repo.BatchCreate(ctx, logs); err != nil {
			t.Fatalf("BatchCreate: %v", err)
		}

		got, err := repo.ListByNamespace(ctx, nsID, 100, 0)
		if err != nil {
			t.Fatalf("ListByNamespace: %v", err)
		}
		if len(got) != n {
			t.Errorf("want %d ingestion logs, got %d", n, len(got))
		}
	})
}

func TestEnrichmentQueueRepo_BatchEnqueue(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		queueRepo := NewEnrichmentQueueRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		const n = 4
		jobs := make([]*model.EnrichmentJob, n)
		for i := range jobs {
			m := newTestMemory(nsID)
			m.ID = uuid.New()
			m.Content = "enqueue memory " + uuid.NewString()
			m.ContentHash = HashContent(m.Content)
			if err := memRepo.Create(ctx, m); err != nil {
				t.Fatalf("create memory: %v", err)
			}
			jobs[i] = &model.EnrichmentJob{MemoryID: m.ID, NamespaceID: nsID}
		}

		if err := queueRepo.BatchEnqueue(ctx, jobs); err != nil {
			t.Fatalf("BatchEnqueue: %v", err)
		}
		// Count per memory rather than the table-wide CountByStatus: the shared
		// Postgres test schema is populated by sibling tests, so a global count
		// is not isolated. Each enqueued memory must have exactly one pending job.
		for _, j := range jobs {
			if got := countQueueStatus(t, ctx, db, j.MemoryID, "pending"); got != 1 {
				t.Errorf("memory %s: want 1 pending job, got %d", j.MemoryID, got)
			}
		}

		// Re-enqueuing the same memories must dedup against the existing pending
		// job (ON CONFLICT ... DO NOTHING), leaving exactly one pending row each.
		dupes := make([]*model.EnrichmentJob, n)
		for i, j := range jobs {
			dupes[i] = &model.EnrichmentJob{MemoryID: j.MemoryID, NamespaceID: nsID}
		}
		if err := queueRepo.BatchEnqueue(ctx, dupes); err != nil {
			t.Fatalf("BatchEnqueue dupes: %v", err)
		}
		for _, j := range jobs {
			if got := countQueueStatus(t, ctx, db, j.MemoryID, "pending"); got != 1 {
				t.Errorf("dedup failed: memory %s has %d pending jobs, want 1", j.MemoryID, got)
			}
		}
	})
}
