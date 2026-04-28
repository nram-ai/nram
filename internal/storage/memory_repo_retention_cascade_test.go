package storage

import (
	"context"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/model"
)

// TestMemoryRepo_HardDeleteSoftDeletedBefore_FKActions verifies the FK
// ON DELETE actions on tables referencing memories(id):
//
//   - token_usage.memory_id      → SET NULL (billing row preserved)
//   - memory_lineage.memory_id   → CASCADE
//   - enrichment_queue.memory_id → CASCADE
//
// Regression for the WARN observed pre-fix:
//
//	dreaming: retention sweep hard-delete memories failed err="memory hard
//	delete soft-deleted before: violates foreign key constraint
//	token_usage_memory_id_fkey ..."
func TestMemoryRepo_HardDeleteSoftDeletedBefore_FKActions(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)

		memRepo := NewMemoryRepo(db)
		mem := newTestMemory(nsID)
		if err := memRepo.Create(ctx, mem); err != nil {
			t.Fatalf("create memory: %v", err)
		}

		tokenUsage := &model.TokenUsage{
			NamespaceID:  nsID,
			Operation:    "memory.recall",
			Provider:     "test-provider",
			Model:        "test-model",
			TokensInput:  10,
			TokensOutput: 20,
			MemoryID:     &mem.ID,
			Success:      true,
		}
		if err := NewTokenUsageRepo(db).Record(ctx, tokenUsage); err != nil {
			t.Fatalf("record token usage: %v", err)
		}

		lineage := &model.MemoryLineage{
			NamespaceID: nsID,
			MemoryID:    mem.ID,
			Relation:    model.LineageExtractedFrom,
		}
		if err := NewMemoryLineageRepo(db).Create(ctx, lineage); err != nil {
			t.Fatalf("create lineage: %v", err)
		}

		queueItem := &model.EnrichmentJob{
			MemoryID:    mem.ID,
			NamespaceID: nsID,
			Status:      "pending",
		}
		if err := NewEnrichmentQueueRepo(db).Enqueue(ctx, queueItem); err != nil {
			t.Fatalf("enqueue enrichment: %v", err)
		}

		if err := memRepo.SoftDelete(ctx, mem.ID, nsID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}

		cutoff := time.Now().UTC().Add(1 * time.Hour)
		deleted, err := memRepo.HardDeleteSoftDeletedBefore(ctx, cutoff, 1000)
		if err != nil {
			t.Fatalf("hard delete soft-deleted before: %v", err)
		}
		if deleted != 1 {
			t.Fatalf("expected 1 row hard-deleted, got %d", deleted)
		}

		memID := mem.ID.String()
		tokID := tokenUsage.ID.String()
		assertCount(t, ctx, db, 0,
			`SELECT COUNT(*) FROM memories WHERE id = ?`,
			`SELECT COUNT(*) FROM memories WHERE id = $1`, memID)
		assertCount(t, ctx, db, 0,
			`SELECT COUNT(*) FROM memory_lineage WHERE memory_id = ?`,
			`SELECT COUNT(*) FROM memory_lineage WHERE memory_id = $1`, memID)
		assertCount(t, ctx, db, 0,
			`SELECT COUNT(*) FROM enrichment_queue WHERE memory_id = ?`,
			`SELECT COUNT(*) FROM enrichment_queue WHERE memory_id = $1`, memID)

		// token_usage row preserved (id match) but memory_id nulled out.
		assertCount(t, ctx, db, 1,
			`SELECT COUNT(*) FROM token_usage WHERE id = ?`,
			`SELECT COUNT(*) FROM token_usage WHERE id = $1`, tokID)
		assertCount(t, ctx, db, 0,
			`SELECT COUNT(*) FROM token_usage WHERE id = ? AND memory_id IS NOT NULL`,
			`SELECT COUNT(*) FROM token_usage WHERE id = $1 AND memory_id IS NOT NULL`, tokID)
	})
}

func assertCount(t *testing.T, ctx context.Context, db DB, want int, sqliteQ, postgresQ string, args ...interface{}) {
	t.Helper()
	q := sqliteQ
	if db.Backend() == BackendPostgres {
		q = postgresQ
	}
	var got int
	if err := db.QueryRow(ctx, q, args...).Scan(&got); err != nil {
		t.Fatalf("count: %v\nquery: %s", err, q)
	}
	if got != want {
		t.Fatalf("count: want %d, got %d\nquery: %s", want, got, q)
	}
}
