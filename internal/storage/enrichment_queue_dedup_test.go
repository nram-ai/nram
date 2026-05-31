package storage

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// countQueueStatus returns how many enrichment_queue rows a memory has in the
// given status.
func countQueueStatus(t *testing.T, ctx context.Context, db DB, memID uuid.UUID, status string) int {
	t.Helper()
	q := "SELECT COUNT(*) FROM enrichment_queue WHERE memory_id = ? AND status = ?"
	if db.Backend() == BackendPostgres {
		q = "SELECT COUNT(*) FROM enrichment_queue WHERE memory_id = $1 AND status = $2"
	}
	var n int
	if err := db.QueryRow(ctx, q, memID.String(), status).Scan(&n); err != nil {
		t.Fatalf("count %q rows: %v", status, err)
	}
	return n
}

// A second pending job for the same memory is deduped (inserted=false), but a
// fresh pending row is allowed once the prior job has been claimed (processing)
// or has left the pending state.
func TestEnrichmentQueueRepo_PendingDedup(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		ins1, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID))
		if err != nil || !ins1 {
			t.Fatalf("first enqueue: inserted=%v err=%v, want true/nil", ins1, err)
		}
		ins2, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID))
		if err != nil {
			t.Fatalf("second enqueue err: %v", err)
		}
		if ins2 {
			t.Fatal("second pending enqueue for same memory must dedup (inserted=false)")
		}
		if got := countQueueStatus(t, ctx, db, memID, "pending"); got != 1 {
			t.Fatalf("pending rows after dedup = %d, want 1", got)
		}

		// Claim the pending job: it becomes 'processing', freeing the partial
		// unique index so a fresh pending row may coexist with the in-flight one.
		claimed, err := repo.ClaimNext(ctx, "w1")
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if claimed == nil || claimed.MemoryID != memID {
			t.Fatalf("claim returned %+v, want job for memory %s", claimed, memID)
		}
		ins3, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID))
		if err != nil {
			t.Fatalf("enqueue while processing: %v", err)
		}
		if !ins3 {
			t.Fatal("enqueue while prior job is processing must insert (coexists)")
		}
		if got := countQueueStatus(t, ctx, db, memID, "pending"); got != 1 {
			t.Fatalf("pending rows while one processing = %d, want 1", got)
		}

		// Complete the in-flight job and drain the pending one; a fresh enqueue
		// then inserts again.
		if err := repo.Complete(ctx, claimed.ID, "w1"); err != nil {
			t.Fatalf("complete: %v", err)
		}
		next, err := repo.ClaimNext(ctx, "w2")
		if err != nil {
			t.Fatalf("claim next: %v", err)
		}
		if err := repo.Complete(ctx, next.ID, "w2"); err != nil {
			t.Fatalf("complete next: %v", err)
		}
		ins4, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID))
		if err != nil || !ins4 {
			t.Fatalf("enqueue after drain: inserted=%v err=%v, want true/nil", ins4, err)
		}
	})
}

// Release of a claimed job whose memory already holds a pending sibling drops
// the released row as redundant instead of creating a duplicate pending.
func TestEnrichmentQueueRepo_ReleaseDropsRedundantPending(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		if _, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID)); err != nil {
			t.Fatalf("enqueue A: %v", err)
		}
		claimed, err := repo.ClaimNext(ctx, "w1")
		if err != nil {
			t.Fatalf("claim A: %v", err)
		}
		// B becomes the pending sibling while A is in flight.
		if ins, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID)); err != nil || !ins {
			t.Fatalf("enqueue B: inserted=%v err=%v", ins, err)
		}

		if err := repo.Release(ctx, claimed.ID, "w1"); err != nil {
			t.Fatalf("release A: %v", err)
		}
		if got := countQueueStatus(t, ctx, db, memID, "pending"); got != 1 {
			t.Fatalf("pending after redundant release-drop = %d, want 1", got)
		}
		if got := countQueueStatus(t, ctx, db, memID, "processing"); got != 0 {
			t.Fatalf("processing after release = %d, want 0", got)
		}
		if _, err := repo.GetByID(ctx, claimed.ID); err == nil {
			t.Fatal("released redundant job should have been deleted")
		}
	})
}

// Retry of a failed job whose memory already holds a pending sibling drops the
// retried row as redundant.
func TestEnrichmentQueueRepo_RetryDropsRedundantPending(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		if _, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID)); err != nil {
			t.Fatalf("enqueue A: %v", err)
		}
		claimed, err := repo.ClaimNext(ctx, "w1")
		if err != nil {
			t.Fatalf("claim A: %v", err)
		}
		if err := repo.Fail(ctx, claimed.ID, "w1", "boom"); err != nil {
			t.Fatalf("fail A: %v", err)
		}
		// With A failed, memory has no pending, so B inserts.
		if ins, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID)); err != nil || !ins {
			t.Fatalf("enqueue B: inserted=%v err=%v", ins, err)
		}

		if err := repo.Retry(ctx, claimed.ID); err != nil {
			t.Fatalf("retry A: %v", err)
		}
		if got := countQueueStatus(t, ctx, db, memID, "pending"); got != 1 {
			t.Fatalf("pending after redundant retry-drop = %d, want 1", got)
		}
		if _, err := repo.GetByID(ctx, claimed.ID); err == nil {
			t.Fatal("retried redundant job should have been deleted")
		}
	})
}

// RequeueStale of a stuck job whose memory already holds a pending sibling drops
// the stuck row as redundant and reports it as normalized (true).
func TestEnrichmentQueueRepo_RequeueStaleDropsRedundantPending(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		if _, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID)); err != nil {
			t.Fatalf("enqueue A: %v", err)
		}
		claimed, err := repo.ClaimNext(ctx, "w1")
		if err != nil {
			t.Fatalf("claim A: %v", err)
		}
		if ins, err := repo.Enqueue(ctx, newTestEnrichmentItem(nsID, memID)); err != nil || !ins {
			t.Fatalf("enqueue B: inserted=%v err=%v", ins, err)
		}

		ok, err := repo.RequeueStale(ctx, claimed.ID, "stuck")
		if err != nil {
			t.Fatalf("requeue stale A: %v", err)
		}
		if !ok {
			t.Fatal("requeue stale of a redundant stuck job should report normalized (true)")
		}
		if got := countQueueStatus(t, ctx, db, memID, "pending"); got != 1 {
			t.Fatalf("pending after redundant requeue-stale-drop = %d, want 1", got)
		}
		if _, err := repo.GetByID(ctx, claimed.ID); err == nil {
			t.Fatal("requeued redundant stuck job should have been deleted")
		}
	})
}
