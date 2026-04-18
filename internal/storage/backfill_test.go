package storage

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestBackfillEmbedJobs_EnqueuesOneJobPerUncoveredMemory verifies the
// post-write-path-refactor contract for existing deployments:
//
//  1. Memories with no enrichment job at all get one enqueued.
//  2. Memories whose only existing job is already pending/running are NOT
//     double-enqueued.
//  3. Memories whose only existing job is completed DO get re-enqueued —
//     those are the rows that may have the old worker's fact-as-parent-vector
//     bug, and re-embedding is the only way to fix it.
//  4. Running the backfill twice is a no-op the second time (idempotent).
//  5. Backfill jobs land at priority -1 so the worker drains them after any
//     newly-stored memories (priority 0 or higher).
func TestBackfillEmbedJobs_EnqueuesOneJobPerUncoveredMemory(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		queueRepo := NewEnrichmentQueueRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		// Seed fixtures:
		//   a. Plain memory, no existing job.
		//   b. Memory with an already-pending job (should be skipped).
		//   c. Memory with a completed job (should be re-enqueued).
		//   d. Soft-deleted memory (must be skipped regardless of job state).
		var ids struct {
			plainA, withPending, completed, softDeleted uuid.UUID
		}

		memA := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memA); err != nil {
			t.Fatalf("create plain memory: %v", err)
		}
		ids.plainA = memA.ID

		memB := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memB); err != nil {
			t.Fatalf("create memory-with-pending: %v", err)
		}
		ids.withPending = memB.ID
		existingPending := &model.EnrichmentJob{MemoryID: memB.ID, NamespaceID: nsID}
		if err := queueRepo.Enqueue(ctx, existingPending); err != nil {
			t.Fatalf("seed pending job: %v", err)
		}

		memC := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memC); err != nil {
			t.Fatalf("create memory-with-completed: %v", err)
		}
		ids.completed = memC.ID
		completedJob := &model.EnrichmentJob{MemoryID: memC.ID, NamespaceID: nsID}
		if err := queueRepo.Enqueue(ctx, completedJob); err != nil {
			t.Fatalf("seed completed job: %v", err)
		}
		if err := queueRepo.Complete(ctx, completedJob.ID); err != nil {
			t.Fatalf("mark completed: %v", err)
		}

		memD := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memD); err != nil {
			t.Fatalf("create soft-deleted memory: %v", err)
		}
		ids.softDeleted = memD.ID
		if err := memRepo.SoftDelete(ctx, memD.ID, nsID); err != nil {
			t.Fatalf("soft-delete memory: %v", err)
		}

		// Run the backfill.
		enqueued, err := BackfillEmbedJobs(ctx, db)
		if err != nil {
			t.Fatalf("backfill failed: %v", err)
		}
		// Expect 2 new jobs: one for plainA, one for completed. The pending
		// memory is skipped (already has a live job). The soft-deleted
		// memory is skipped (deleted_at is not null).
		if enqueued != 2 {
			t.Fatalf("first backfill: expected 2 new jobs, got %d", enqueued)
		}

		// Idempotency: running again must insert zero rows. The jobs created
		// in the first pass are themselves pending/running and will satisfy
		// the LEFT JOIN ... IS NULL guard.
		enqueuedAgain, err := BackfillEmbedJobs(ctx, db)
		if err != nil {
			t.Fatalf("second backfill failed: %v", err)
		}
		if enqueuedAgain != 0 {
			t.Fatalf("idempotency broken: second backfill enqueued %d jobs, expected 0", enqueuedAgain)
		}

		// Verify which memories actually got new jobs. We expect plainA and
		// completed to each now have a priority=-1 pending job; withPending
		// should have exactly its original priority-0 job; softDeleted
		// should have none at all.
		type jobRow struct {
			memID    uuid.UUID
			priority int
			status   string
		}

		queryAll := `SELECT memory_id, priority, status FROM enrichment_queue WHERE namespace_id = ?`
		if db.Backend() == BackendPostgres {
			queryAll = `SELECT memory_id, priority, status FROM enrichment_queue WHERE namespace_id = $1`
		}
		rows, err := db.Query(ctx, queryAll, nsID.String())
		if err != nil {
			t.Fatalf("query jobs: %v", err)
		}
		defer rows.Close()

		byMem := map[uuid.UUID][]jobRow{}
		for rows.Next() {
			var memIDStr string
			var j jobRow
			if err := rows.Scan(&memIDStr, &j.priority, &j.status); err != nil {
				t.Fatalf("scan: %v", err)
			}
			mid, err := uuid.Parse(memIDStr)
			if err != nil {
				t.Fatalf("parse memory_id: %v", err)
			}
			j.memID = mid
			byMem[mid] = append(byMem[mid], j)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}

		// plainA: exactly one new backfill job at priority -1, status pending.
		if got := byMem[ids.plainA]; len(got) != 1 {
			t.Errorf("plainA: expected 1 job, got %d (%v)", len(got), got)
		} else if got[0].priority != -1 || got[0].status != "pending" {
			t.Errorf("plainA: expected priority=-1 status=pending, got %+v", got[0])
		}

		// withPending: still exactly one job — the original priority-0
		// pending one. Backfill MUST NOT have added a duplicate.
		if got := byMem[ids.withPending]; len(got) != 1 {
			t.Errorf("withPending: expected 1 job (no duplicate), got %d (%v)", len(got), got)
		} else if got[0].priority != 0 || got[0].status != "pending" {
			t.Errorf("withPending: expected original priority=0 status=pending, got %+v", got[0])
		}

		// completed: now has 2 jobs — the original completed one and the
		// new backfill one at priority -1.
		if got := byMem[ids.completed]; len(got) != 2 {
			t.Errorf("completed: expected 2 jobs (original + backfill), got %d (%v)", len(got), got)
		} else {
			foundBackfill := false
			foundCompleted := false
			for _, j := range got {
				switch {
				case j.priority == -1 && j.status == "pending":
					foundBackfill = true
				case j.priority == 0 && j.status == "completed":
					foundCompleted = true
				}
			}
			if !foundBackfill {
				t.Errorf("completed: missing new backfill job at priority=-1")
			}
			if !foundCompleted {
				t.Errorf("completed: missing original completed job")
			}
		}

		// softDeleted: no jobs at all.
		if got := byMem[ids.softDeleted]; len(got) != 0 {
			t.Errorf("softDeleted: expected 0 jobs, got %d (%v)", len(got), got)
		}
	})
}
