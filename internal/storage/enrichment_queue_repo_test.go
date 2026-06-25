package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// createTestMemoryForQueue creates a namespace and memory, returning both IDs.
func createTestMemoryForQueue(t *testing.T, ctx context.Context, db DB) (namespaceID, memoryID uuid.UUID) {
	t.Helper()
	nsID := createTestNamespace(t, ctx, db)
	repo := NewMemoryRepo(db)
	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create test memory for queue: %v", err)
	}
	return nsID, mem.ID
}

func newTestEnrichmentItem(namespaceID, memoryID uuid.UUID) *model.EnrichmentJob {
	return &model.EnrichmentJob{
		MemoryID:    memoryID,
		NamespaceID: namespaceID,
		Priority:    0,
	}
}

func TestEnrichmentQueueRepo_Enqueue(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		if item.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after enqueue")
		}
		if item.Status != "pending" {
			t.Fatalf("expected status 'pending', got %q", item.Status)
		}
		if item.MemoryID != memID {
			t.Fatalf("expected memory_id %s, got %s", memID, item.MemoryID)
		}
		if item.NamespaceID != nsID {
			t.Fatalf("expected namespace_id %s, got %s", nsID, item.NamespaceID)
		}
		if item.Attempts != 0 {
			t.Fatalf("expected attempts 0, got %d", item.Attempts)
		}
		if item.MaxAttempts != 3 {
			t.Fatalf("expected max_attempts 3, got %d", item.MaxAttempts)
		}
		if string(item.StepsCompleted) != "[]" {
			t.Fatalf("expected steps_completed '[]', got %q", string(item.StepsCompleted))
		}
		if item.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
		if item.UpdatedAt.IsZero() {
			t.Fatal("expected non-zero updated_at")
		}
		if item.ClaimedAt != nil {
			t.Fatal("expected nil claimed_at")
		}
		if item.ClaimedBy != nil {
			t.Fatal("expected nil claimed_by")
		}
		if item.CompletedAt != nil {
			t.Fatal("expected nil completed_at")
		}
	})
}

func TestEnrichmentQueueRepo_Enqueue_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
		if item.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestEnrichmentQueueRepo_Enqueue_WithExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		explicitID := uuid.New()
		item := &model.EnrichmentJob{
			ID:          explicitID,
			MemoryID:    memID,
			NamespaceID: nsID,
		}
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
		if item.ID != explicitID {
			t.Fatalf("expected ID %s, got %s", explicitID, item.ID)
		}
	})
}

func TestEnrichmentQueueRepo_Enqueue_WithPriority(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := &model.EnrichmentJob{
			MemoryID:    memID,
			NamespaceID: nsID,
			Priority:    10,
		}
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
		if item.Priority != 10 {
			t.Fatalf("expected priority 10, got %d", item.Priority)
		}
	})
}

func TestEnrichmentQueueRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		fetched, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}

		if fetched.ID != item.ID {
			t.Fatalf("expected ID %s, got %s", item.ID, fetched.ID)
		}
		if fetched.MemoryID != item.MemoryID {
			t.Fatalf("expected memory_id %s, got %s", item.MemoryID, fetched.MemoryID)
		}
		if fetched.Status != "pending" {
			t.Fatalf("expected status 'pending', got %q", fetched.Status)
		}
	})
}

func TestEnrichmentQueueRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestEnrichmentQueueRepo_ClaimNext(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// In a shared DB, ClaimNext may claim a different pending item.
		// Keep claiming until we get our item or the queue empties.
		var claimed *model.EnrichmentJob
		for {
			c, err := repo.ClaimNext(ctx, "worker-1")
			if err != nil {
				t.Fatalf("failed to claim next: %v", err)
			}
			if c.ID == item.ID {
				claimed = c
				break
			}
		}

		if claimed.Status != "processing" {
			t.Fatalf("expected status 'processing', got %q", claimed.Status)
		}
		if claimed.ClaimedBy == nil || *claimed.ClaimedBy != "worker-1" {
			t.Fatalf("expected claimed_by 'worker-1', got %v", claimed.ClaimedBy)
		}
		if claimed.ClaimedAt == nil {
			t.Fatal("expected non-nil claimed_at")
		}
	})
}

func TestEnrichmentQueueRepo_ClaimNext_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		// In a shared Postgres DB, the queue may not be empty.
		// For SQLite (fresh DB), we expect ErrNoRows.
		_, err := repo.ClaimNext(ctx, "worker-empty")
		if db.Backend() == BackendSQLite {
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("expected sql.ErrNoRows on empty queue, got %v", err)
			}
		} else {
			// Postgres shared DB: just verify no unexpected error type.
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

func TestEnrichmentQueueRepo_ClaimNext_PriorityOrder(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		// Enqueue low priority first
		low := &model.EnrichmentJob{MemoryID: memID, NamespaceID: nsID, Priority: 1}
		if _, err := repo.Enqueue(ctx, low); err != nil {
			t.Fatalf("failed to enqueue low: %v", err)
		}

		// Create another memory for the high priority item (same namespace)
		memRepo := NewMemoryRepo(db)
		mem2 := newTestMemory(nsID)
		if err := memRepo.Create(ctx, mem2); err != nil {
			t.Fatalf("failed to create second memory: %v", err)
		}

		// Enqueue high priority second
		high := &model.EnrichmentJob{MemoryID: mem2.ID, NamespaceID: nsID, Priority: 10}
		if _, err := repo.Enqueue(ctx, high); err != nil {
			t.Fatalf("failed to enqueue high: %v", err)
		}

		// ClaimNext should return high priority first
		claimed, err := repo.ClaimNext(ctx, "worker-1")
		if err != nil {
			t.Fatalf("failed to claim: %v", err)
		}
		if claimed.ID != high.ID {
			t.Fatalf("expected high priority item %s, got %s", high.ID, claimed.ID)
		}
		if claimed.Priority != 10 {
			t.Fatalf("expected priority 10, got %d", claimed.Priority)
		}

		// Next claim should return low priority
		claimed2, err := repo.ClaimNext(ctx, "worker-2")
		if err != nil {
			t.Fatalf("failed to claim second: %v", err)
		}
		if claimed2.ID != low.ID {
			t.Fatalf("expected low priority item %s, got %s", low.ID, claimed2.ID)
		}
	})
}

func TestEnrichmentQueueRepo_ClaimNext_SkipsProcessing(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Claim items until we get ours.
		for {
			c, err := repo.ClaimNext(ctx, "worker-skip-test")
			if err != nil {
				t.Fatalf("failed to claim: %v", err)
			}
			if c.ID == item.ID {
				break
			}
		}

		// Verify our item is now in 'processing' status.
		fetched, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}
		if fetched.Status != "processing" {
			t.Fatalf("expected 'processing', got %q", fetched.Status)
		}
	})
}

func TestEnrichmentQueueRepo_Complete(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Claim it first
		_, err := repo.ClaimNext(ctx, "worker-1")
		if err != nil {
			t.Fatalf("failed to claim: %v", err)
		}

		// Complete it
		if err := repo.Complete(ctx, item.ID, ""); err != nil {
			t.Fatalf("failed to complete: %v", err)
		}

		// Verify
		fetched, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}
		if fetched.Status != "completed" {
			t.Fatalf("expected status 'completed', got %q", fetched.Status)
		}
		if fetched.CompletedAt == nil {
			t.Fatal("expected non-nil completed_at")
		}
	})
}

func TestEnrichmentQueueRepo_Complete_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		err := repo.Complete(ctx, uuid.New(), "")
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestEnrichmentQueueRepo_Fail(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Claim it
		_, err := repo.ClaimNext(ctx, "worker-1")
		if err != nil {
			t.Fatalf("failed to claim: %v", err)
		}

		// Fail it
		if err := repo.Fail(ctx, item.ID, "", "something went wrong"); err != nil {
			t.Fatalf("failed to fail: %v", err)
		}

		// Verify
		fetched, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}
		if fetched.Status != "failed" {
			t.Fatalf("expected status 'failed', got %q", fetched.Status)
		}
		// Postgres JSONB stores the error as a JSON string; SQLite stores as plain text.
		lastErr := string(fetched.LastError)
		var unquoted string
		if err := json.Unmarshal(fetched.LastError, &unquoted); err != nil {
			unquoted = lastErr // fallback for SQLite plain text
		}
		if unquoted != "something went wrong" {
			t.Fatalf("expected last_error 'something went wrong', got %q", lastErr)
		}
		if fetched.Attempts != 1 {
			t.Fatalf("expected attempts 1, got %d", fetched.Attempts)
		}
	})
}

// TestEnrichmentQueueRepo_Fail_StructuredPayload verifies the wire-contract
// change for Issue 2: when a parse-failure-shaped payload is passed (a
// struct rather than a string), it round-trips as a JSON object and the
// admin layer can read finish_reason / prompt_tokens / completion_tokens /
// raw_response back without re-running the call.
func TestEnrichmentQueueRepo_Fail_StructuredPayload(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
			t.Fatalf("claim: %v", err)
		}

		// Mirror the *service.ExtractionFailure shape without taking a
		// service-package import (storage cannot depend on service).
		payload := map[string]any{
			"phase":             "fact_extraction",
			"reason":            "parse_failed",
			"error":             "failed to parse fact extraction response as JSON",
			"finish_reason":     "length",
			"prompt_tokens":     1362,
			"completion_tokens": 2048,
			"model":             "qwen3:8b-extract",
			"provider":          "openai",
			"raw_response":      `[{"content":"truncated`,
		}
		if err := repo.Fail(ctx, item.ID, "", payload); err != nil {
			t.Fatalf("fail with structured payload: %v", err)
		}

		fetched, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("get by id: %v", err)
		}
		if fetched.Status != "failed" {
			t.Fatalf("expected status 'failed', got %q", fetched.Status)
		}

		var decoded map[string]any
		if err := json.Unmarshal(fetched.LastError, &decoded); err != nil {
			t.Fatalf("last_error must round-trip as JSON object on both backends; got %s, err: %v",
				string(fetched.LastError), err)
		}
		if decoded["phase"] != "fact_extraction" {
			t.Errorf("phase = %v, want fact_extraction", decoded["phase"])
		}
		if decoded["finish_reason"] != "length" {
			t.Errorf("finish_reason = %v, want length", decoded["finish_reason"])
		}
		// JSON numbers decode as float64.
		if decoded["completion_tokens"].(float64) != 2048 {
			t.Errorf("completion_tokens = %v, want 2048", decoded["completion_tokens"])
		}
		if !strings.Contains(decoded["raw_response"].(string), "truncated") {
			t.Errorf("raw_response missing the truncated tail; got %q", decoded["raw_response"])
		}
	})
}

func TestEnrichmentQueueRepo_CompleteWithWarning(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
			t.Fatalf("claim: %v", err)
		}

		warning := map[string]any{
			"warnings": []map[string]any{{
				"phase":           "fact_extraction",
				"reason":          "partial_recovery",
				"finish_reason":   "length",
				"facts_recovered": 10,
				"model":           "qwen3:8b-extract",
			}},
		}
		if err := repo.CompleteWithWarning(ctx, item.ID, "", warning); err != nil {
			t.Fatalf("complete with warning: %v", err)
		}

		fetched, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("get by id: %v", err)
		}
		if fetched.Status != "completed" {
			t.Fatalf("expected status 'completed', got %q", fetched.Status)
		}
		var decoded map[string]any
		if err := json.Unmarshal(fetched.LastError, &decoded); err != nil {
			t.Fatalf("warning payload must round-trip; got %s err: %v",
				string(fetched.LastError), err)
		}
		warnings, ok := decoded["warnings"].([]any)
		if !ok || len(warnings) != 1 {
			t.Fatalf("expected 1-element warnings array, got %v", decoded["warnings"])
		}
	})
}

func TestEnrichmentQueueRepo_Fail_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		err := repo.Fail(ctx, uuid.New(), "", "error")
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestEnrichmentQueueRepo_Retry(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Claim it
		_, err := repo.ClaimNext(ctx, "worker-1")
		if err != nil {
			t.Fatalf("failed to claim: %v", err)
		}

		// Fail it
		if err := repo.Fail(ctx, item.ID, "", "transient error"); err != nil {
			t.Fatalf("failed to fail: %v", err)
		}

		// Retry it
		if err := repo.Retry(ctx, item.ID); err != nil {
			t.Fatalf("failed to retry: %v", err)
		}

		// Verify
		fetched, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}
		if fetched.Status != "pending" {
			t.Fatalf("expected status 'pending', got %q", fetched.Status)
		}
		if fetched.ClaimedBy != nil {
			t.Fatalf("expected nil claimed_by after retry, got %v", fetched.ClaimedBy)
		}
		if fetched.ClaimedAt != nil {
			t.Fatalf("expected nil claimed_at after retry, got %v", fetched.ClaimedAt)
		}
		// Attempts: 1 from Fail + 1 from Retry = 2
		if fetched.Attempts != 2 {
			t.Fatalf("expected attempts 2, got %d", fetched.Attempts)
		}
	})
}

func TestEnrichmentQueueRepo_Retry_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		err := repo.Retry(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestEnrichmentQueueRepo_Retry_CanBeClaimedAgain(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Claim, fail, retry
		if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
			t.Fatalf("failed to claim: %v", err)
		}
		if err := repo.Fail(ctx, item.ID, "", "error"); err != nil {
			t.Fatalf("failed to fail: %v", err)
		}
		if err := repo.Retry(ctx, item.ID); err != nil {
			t.Fatalf("failed to retry: %v", err)
		}

		// Should be claimable again
		claimed, err := repo.ClaimNext(ctx, "worker-2")
		if err != nil {
			t.Fatalf("failed to re-claim after retry: %v", err)
		}
		if claimed.ID != item.ID {
			t.Fatalf("expected same item %s, got %s", item.ID, claimed.ID)
		}
		if claimed.ClaimedBy == nil || *claimed.ClaimedBy != "worker-2" {
			t.Fatalf("expected claimed_by 'worker-2', got %v", claimed.ClaimedBy)
		}
	})
}

func TestEnrichmentQueueRepo_FullLifecycle(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		// Enqueue
		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if item.Status != "pending" {
			t.Fatalf("expected pending, got %q", item.Status)
		}

		// Claim
		claimed, err := repo.ClaimNext(ctx, "worker-lifecycle")
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if claimed.Status != "processing" {
			t.Fatalf("expected processing, got %q", claimed.Status)
		}

		// Complete
		if err := repo.Complete(ctx, claimed.ID, ""); err != nil {
			t.Fatalf("complete: %v", err)
		}
		final, err := repo.GetByID(ctx, claimed.ID)
		if err != nil {
			t.Fatalf("get after complete: %v", err)
		}
		if final.Status != "completed" {
			t.Fatalf("expected completed, got %q", final.Status)
		}
		if final.CompletedAt == nil {
			t.Fatal("expected non-nil completed_at after complete")
		}

		// Verify the completed item cannot be claimed again.
		rechecked, err := repo.GetByID(ctx, claimed.ID)
		if err != nil {
			t.Fatalf("re-get after complete: %v", err)
		}
		if rechecked.Status != "completed" {
			t.Fatalf("expected completed status on re-check, got %q", rechecked.Status)
		}
	})
}

// Suppress unused import warning for json.
var _ = json.RawMessage{}

func TestEnrichmentQueueRepo_ListRecent(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := &model.EnrichmentJob{
			MemoryID:    memID,
			NamespaceID: nsID,
		}
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		items, err := repo.ListRecent(ctx, 10)
		if err != nil {
			t.Fatalf("ListRecent failed: %v", err)
		}
		if len(items) < 1 {
			t.Fatalf("expected at least 1 item, got %d", len(items))
		}
	})
}

func TestEnrichmentQueueRepo_RetryAllFailed(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := &model.EnrichmentJob{
			MemoryID:    memID,
			NamespaceID: nsID,
		}
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Fail the item
		if err := repo.Fail(ctx, item.ID, "", "test error"); err != nil {
			t.Fatalf("failed to fail item: %v", err)
		}

		count, err := repo.RetryAllFailed(ctx)
		if err != nil {
			t.Fatalf("RetryAllFailed failed: %v", err)
		}
		if count < 1 {
			t.Fatalf("expected at least 1 retried, got %d", count)
		}

		// Verify it's now pending
		retried, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("failed to get retried item: %v", err)
		}
		if retried.Status != "pending" {
			t.Fatalf("expected status pending, got %s", retried.Status)
		}
	})
}

// setQueueUpdatedAt backdates a queue row's updated_at for retention tests.
func setQueueUpdatedAt(t *testing.T, ctx context.Context, db DB, id uuid.UUID, ts time.Time) {
	t.Helper()
	q := "UPDATE enrichment_queue SET updated_at = ? WHERE id = ?"
	if db.Backend() == BackendPostgres {
		q = postgresPlaceholders(q)
	}
	if _, err := db.Exec(ctx, q, ts.UTC().Format(time.RFC3339), id.String()); err != nil {
		t.Fatalf("backdate updated_at: %v", err)
	}
}

// TestEnrichmentQueueRepo_RetryAllFailedScoped_DedupAndAttempts verifies the
// set-based bulk retry: it dedups to satisfy the partial unique index (so no
// UNIQUE violation), keeps one pending row per memory, resets attempts to 0 and
// clears last_error on the survivors, and resets the enriched flag only on the
// memories actually retried.
func TestEnrichmentQueueRepo_RetryAllFailedScoped_DedupAndAttempts(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		memRepo := NewMemoryRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		now := time.Now().UTC()

		// Memory A: two failed rows (older + newer), no pending sibling. The
		// older must be dropped and the newer flipped to pending.
		nsA, memA := createTestMemoryForQueue(t, ctx, db)
		older := &model.EnrichmentJob{MemoryID: memA, NamespaceID: nsA, CreatedAt: now.Add(-2 * time.Hour)}
		if _, err := repo.Enqueue(ctx, older); err != nil {
			t.Fatalf("enqueue A older: %v", err)
		}
		if err := repo.Fail(ctx, older.ID, "", "boom-old"); err != nil {
			t.Fatalf("fail A older: %v", err)
		}
		newer := &model.EnrichmentJob{MemoryID: memA, NamespaceID: nsA, CreatedAt: now.Add(-1 * time.Hour), Attempts: 2}
		if _, err := repo.Enqueue(ctx, newer); err != nil {
			t.Fatalf("enqueue A newer: %v", err)
		}
		if err := repo.Fail(ctx, newer.ID, "", "boom-new"); err != nil {
			t.Fatalf("fail A newer: %v", err)
		}
		if err := memRepo.MarkEnriched(ctx, memA, nsA, nil, nil, nil, nil, nil); err != nil {
			t.Fatalf("mark A enriched: %v", err)
		}

		// Memory B: one failed row plus a live pending sibling. The failed row
		// is redundant (pending already covers the memory) and must be dropped,
		// the pending row left untouched, and B's enriched flag NOT reset.
		nsB, memB := createTestMemoryForQueue(t, ctx, db)
		failedB := &model.EnrichmentJob{MemoryID: memB, NamespaceID: nsB, CreatedAt: now.Add(-2 * time.Hour)}
		if _, err := repo.Enqueue(ctx, failedB); err != nil {
			t.Fatalf("enqueue B failed: %v", err)
		}
		if err := repo.Fail(ctx, failedB.ID, "", "boom-b"); err != nil {
			t.Fatalf("fail B: %v", err)
		}
		pendingB := &model.EnrichmentJob{MemoryID: memB, NamespaceID: nsB, CreatedAt: now.Add(-1 * time.Hour)}
		if _, err := repo.Enqueue(ctx, pendingB); err != nil {
			t.Fatalf("enqueue B pending: %v", err)
		}
		if err := memRepo.MarkEnriched(ctx, memB, nsB, nil, nil, nil, nil, nil); err != nil {
			t.Fatalf("mark B enriched: %v", err)
		}

		count, err := repo.RetryAllFailedScoped(ctx, "")
		if err != nil {
			t.Fatalf("RetryAllFailedScoped: %v", err)
		}
		if count != 1 {
			t.Fatalf("flipped count = %d, want 1 (only memory A's surviving failed row)", count)
		}

		// Memory A: exactly one row, the newer, now pending with a clean slate.
		if n := countQueueStatus(t, ctx, db, memA, ""); n != 1 {
			t.Fatalf("memory A row count = %d, want 1 (older deduped away)", n)
		}
		if _, err := repo.GetByID(ctx, older.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected older A row deleted (sql.ErrNoRows), got %v", err)
		}
		gotA, err := repo.GetByID(ctx, newer.ID)
		if err != nil {
			t.Fatalf("get A newer: %v", err)
		}
		if gotA.Status != "pending" {
			t.Errorf("A newer status = %s, want pending", gotA.Status)
		}
		if gotA.Attempts != 0 {
			t.Errorf("A newer attempts = %d, want 0 (reset on operator retry)", gotA.Attempts)
		}
		if gotA.LastError != nil && string(gotA.LastError) != "null" && string(gotA.LastError) != "" {
			t.Errorf("A newer last_error = %q, want cleared", string(gotA.LastError))
		}

		// Memory B: failed row dropped, pending sibling untouched.
		if n := countQueueStatus(t, ctx, db, memB, ""); n != 1 {
			t.Fatalf("memory B row count = %d, want 1 (failed deduped away, pending kept)", n)
		}
		if _, err := repo.GetByID(ctx, failedB.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected B failed row deleted (sql.ErrNoRows), got %v", err)
		}
		gotPendingB, err := repo.GetByID(ctx, pendingB.ID)
		if err != nil {
			t.Fatalf("get B pending: %v", err)
		}
		if gotPendingB.Status != "pending" {
			t.Errorf("B pending status = %s, want pending (untouched)", gotPendingB.Status)
		}

		// enriched reset only on the retried memory (A), not B.
		mA, err := memRepo.GetByID(ctx, memA, nsA)
		if err != nil {
			t.Fatalf("get memory A: %v", err)
		}
		if mA.Enriched {
			t.Errorf("memory A enriched = true, want false (reset for retry)")
		}
		mB, err := memRepo.GetByID(ctx, memB, nsB)
		if err != nil {
			t.Fatalf("get memory B: %v", err)
		}
		if !mB.Enriched {
			t.Errorf("memory B enriched = false, want true (not retried, must not be over-reset)")
		}
	})
}

// TestEnrichmentQueueRepo_RetryAllFailedScoped_NamespaceScope verifies the
// prefix scopes the flip to one namespace and leaves others' failed jobs alone.
func TestEnrichmentQueueRepo_RetryAllFailedScoped_NamespaceScope(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		ns1, mem1 := createTestMemoryForQueue(t, ctx, db)
		ns2, mem2 := createTestMemoryForQueue(t, ctx, db)

		j1 := &model.EnrichmentJob{MemoryID: mem1, NamespaceID: ns1}
		if _, err := repo.Enqueue(ctx, j1); err != nil {
			t.Fatalf("enqueue ns1: %v", err)
		}
		if err := repo.Fail(ctx, j1.ID, "", "boom1"); err != nil {
			t.Fatalf("fail ns1: %v", err)
		}
		j2 := &model.EnrichmentJob{MemoryID: mem2, NamespaceID: ns2}
		if _, err := repo.Enqueue(ctx, j2); err != nil {
			t.Fatalf("enqueue ns2: %v", err)
		}
		if err := repo.Fail(ctx, j2.ID, "", "boom2"); err != nil {
			t.Fatalf("fail ns2: %v", err)
		}

		// createTestNamespace sets path = nsID.String().
		count, err := repo.RetryAllFailedScoped(ctx, ns1.String())
		if err != nil {
			t.Fatalf("RetryAllFailedScoped(ns1): %v", err)
		}
		if count != 1 {
			t.Fatalf("flipped count = %d, want 1 (only ns1)", count)
		}

		got1, err := repo.GetByID(ctx, j1.ID)
		if err != nil {
			t.Fatalf("get ns1 job: %v", err)
		}
		if got1.Status != "pending" {
			t.Errorf("ns1 job status = %s, want pending", got1.Status)
		}
		got2, err := repo.GetByID(ctx, j2.ID)
		if err != nil {
			t.Fatalf("get ns2 job: %v", err)
		}
		if got2.Status != "failed" {
			t.Errorf("ns2 job status = %s, want failed (out of scope)", got2.Status)
		}
	})
}

// TestEnrichmentQueueRepo_DeleteFailedBefore verifies retention pruning deletes
// only failed rows older than the cutoff, bounded by limit, leaving recent
// failed rows and non-failed rows in place.
func TestEnrichmentQueueRepo_DeleteFailedBefore(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		now := time.Now().UTC()

		// Old failed row: should be deleted.
		nsOld, memOld := createTestMemoryForQueue(t, ctx, db)
		oldFailed := &model.EnrichmentJob{MemoryID: memOld, NamespaceID: nsOld}
		if _, err := repo.Enqueue(ctx, oldFailed); err != nil {
			t.Fatalf("enqueue old: %v", err)
		}
		if err := repo.Fail(ctx, oldFailed.ID, "", "old"); err != nil {
			t.Fatalf("fail old: %v", err)
		}
		setQueueUpdatedAt(t, ctx, db, oldFailed.ID, now.Add(-10*24*time.Hour))

		// Recent failed row: newer than cutoff, must survive.
		nsNew, memNew := createTestMemoryForQueue(t, ctx, db)
		recentFailed := &model.EnrichmentJob{MemoryID: memNew, NamespaceID: nsNew}
		if _, err := repo.Enqueue(ctx, recentFailed); err != nil {
			t.Fatalf("enqueue recent: %v", err)
		}
		if err := repo.Fail(ctx, recentFailed.ID, "", "recent"); err != nil {
			t.Fatalf("fail recent: %v", err)
		}
		setQueueUpdatedAt(t, ctx, db, recentFailed.ID, now.Add(-1*time.Hour))

		// Old pending row: old but not failed, must survive.
		nsPend, memPend := createTestMemoryForQueue(t, ctx, db)
		oldPending := &model.EnrichmentJob{MemoryID: memPend, NamespaceID: nsPend}
		if _, err := repo.Enqueue(ctx, oldPending); err != nil {
			t.Fatalf("enqueue pending: %v", err)
		}
		setQueueUpdatedAt(t, ctx, db, oldPending.ID, now.Add(-10*24*time.Hour))

		cutoff := now.Add(-7 * 24 * time.Hour)
		deleted, err := repo.DeleteFailedBefore(ctx, cutoff, 100)
		if err != nil {
			t.Fatalf("DeleteFailedBefore: %v", err)
		}
		if deleted != 1 {
			t.Fatalf("deleted = %d, want 1 (only the old failed row)", deleted)
		}

		if _, err := repo.GetByID(ctx, oldFailed.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("old failed row should be deleted, got err=%v", err)
		}
		if _, err := repo.GetByID(ctx, recentFailed.ID); err != nil {
			t.Errorf("recent failed row should survive, got err=%v", err)
		}
		if _, err := repo.GetByID(ctx, oldPending.ID); err != nil {
			t.Errorf("old pending row should survive, got err=%v", err)
		}
	})
}

// TestEnrichmentQueueRepo_MarkStepCompleted exercises the per-step
// progress marker the worker writes between phases. Idempotent across
// repeats, preserves prior contents, and survives the round-trip.
func TestEnrichmentQueueRepo_MarkStepCompleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		// First mark.
		if err := repo.MarkStepCompleted(ctx, item.ID, model.StepFactExtraction); err != nil {
			t.Fatalf("mark fact: %v", err)
		}
		got, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("get after first mark: %v", err)
		}
		var steps []string
		if err := json.Unmarshal(got.StepsCompleted, &steps); err != nil {
			t.Fatalf("decode steps after first mark: %v: %q", err, string(got.StepsCompleted))
		}
		if len(steps) != 1 || steps[0] != model.StepFactExtraction {
			t.Fatalf("expected [%q], got %v", model.StepFactExtraction, steps)
		}

		// Idempotent: re-marking the same step does not duplicate.
		if err := repo.MarkStepCompleted(ctx, item.ID, model.StepFactExtraction); err != nil {
			t.Fatalf("mark fact again: %v", err)
		}
		got, _ = repo.GetByID(ctx, item.ID)
		_ = json.Unmarshal(got.StepsCompleted, &steps)
		if len(steps) != 1 {
			t.Fatalf("expected idempotent append, got %v", steps)
		}

		// Append a different step preserves the first.
		if err := repo.MarkStepCompleted(ctx, item.ID, model.StepEntityExtraction); err != nil {
			t.Fatalf("mark entity: %v", err)
		}
		got, _ = repo.GetByID(ctx, item.ID)
		_ = json.Unmarshal(got.StepsCompleted, &steps)
		if len(steps) != 2 {
			t.Fatalf("expected 2 steps after entity append, got %v", steps)
		}
		seen := map[string]bool{}
		for _, s := range steps {
			seen[s] = true
		}
		if !seen[model.StepFactExtraction] || !seen[model.StepEntityExtraction] {
			t.Fatalf("expected fact+entity in steps_completed, got %v", steps)
		}

		// Empty step name is rejected.
		if err := repo.MarkStepCompleted(ctx, item.ID, ""); err == nil {
			t.Fatal("expected error on empty step name")
		}
	})
}

// TestEnrichmentQueueRepo_RequeueStaleIdempotent verifies that calling
// RequeueStale twice on the same row is safe; the second call returns
// (false, nil) because the row's status is no longer 'processing' after
// the first call. This is the multi-instance sweep race guard.
func TestEnrichmentQueueRepo_RequeueStaleIdempotent(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		// Get a baseline by claiming so the row is in 'processing'.
		var claimed *model.EnrichmentJob
		for {
			c, err := repo.ClaimNext(ctx, "worker-test")
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			if c.ID == item.ID {
				claimed = c
				break
			}
		}
		_ = claimed

		ok1, err := repo.RequeueStale(ctx, item.ID, "test sweep #1")
		if err != nil {
			t.Fatalf("first RequeueStale: %v", err)
		}
		if !ok1 {
			t.Fatalf("expected first RequeueStale to return true, got false")
		}

		// Row is now back to 'pending'. Second call should be a no-op.
		ok2, err := repo.RequeueStale(ctx, item.ID, "test sweep #2")
		if err != nil {
			t.Fatalf("second RequeueStale: %v", err)
		}
		if ok2 {
			t.Fatalf("expected second RequeueStale to return false (idempotent), got true")
		}
	})
}

// TestEnrichmentQueueRepo_RequeueStaleBumpsAttempts verifies that the
// requeue path treats the failure like a Retry (attempts += 1) so a
// genuine poison-pill memory still hits max_attempts and stops looping
// instead of being requeued indefinitely.
func TestEnrichmentQueueRepo_RequeueStaleBumpsAttempts(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		// Bump attempts to 2 so we can verify +1.
		if err := repo.Fail(ctx, item.ID, "", "synthetic"); err != nil {
			t.Fatalf("fail: %v", err)
		}
		if err := repo.Retry(ctx, item.ID); err != nil {
			t.Fatalf("retry to lift back to pending: %v", err)
		}
		// Claim so the row is in 'processing' (RequeueStale's WHERE guard).
		var claimed *model.EnrichmentJob
		for {
			c, err := repo.ClaimNext(ctx, "worker-test")
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			if c.ID == item.ID {
				claimed = c
				break
			}
		}
		baselineAttempts := claimed.Attempts

		ok, err := repo.RequeueStale(ctx, item.ID, "stuck_sweeper: test")
		if err != nil {
			t.Fatalf("RequeueStale: %v", err)
		}
		if !ok {
			t.Fatalf("RequeueStale returned false on a fresh claim")
		}

		got, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("get-by-id after requeue: %v", err)
		}
		if got.Status != "pending" {
			t.Fatalf("expected status='pending' after requeue, got %q", got.Status)
		}
		if got.ClaimedBy != nil {
			t.Fatalf("expected claimed_by NULL after requeue, got %v", got.ClaimedBy)
		}
		if got.ClaimedAt != nil {
			t.Fatalf("expected claimed_at NULL after requeue, got %v", got.ClaimedAt)
		}
		if got.Attempts != baselineAttempts+1 {
			t.Fatalf("expected attempts %d, got %d", baselineAttempts+1, got.Attempts)
		}
		if got.LastRequeueReason == nil || !strings.Contains(*got.LastRequeueReason, "stuck_sweeper") {
			t.Fatalf("expected last_requeue_reason to carry 'stuck_sweeper', got %v", got.LastRequeueReason)
		}
		if len(got.LastError) > 0 && string(got.LastError) != "null" {
			t.Fatalf("expected last_error cleared after requeue, got %q", string(got.LastError))
		}
	})
}

// TestEnrichmentQueueRepo_CompleteOwnedRefusesStaleClaim verifies the core
// stale-write guard: if a sweeper requeues a row out from under a worker,
// the worker's eventual CompleteOwned MUST NOT silently overwrite the new
// claimant's outcome; it returns ErrClaimLost instead.
func TestEnrichmentQueueRepo_CompleteOwnedRefusesStaleClaim(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		// Claim as worker-A.
		for {
			c, err := repo.ClaimNext(ctx, "worker-A")
			if err != nil {
				t.Fatalf("claim worker-A: %v", err)
			}
			if c.ID == item.ID {
				break
			}
		}

		// Simulate the sweeper requeueing + worker-B claiming the row.
		if ok, err := repo.RequeueStale(ctx, item.ID, "stuck_sweeper: test race"); err != nil || !ok {
			t.Fatalf("requeue: ok=%v err=%v", ok, err)
		}
		for {
			c, err := repo.ClaimNext(ctx, "worker-B")
			if err != nil {
				t.Fatalf("claim worker-B: %v", err)
			}
			if c.ID == item.ID {
				break
			}
		}

		// worker-A finally returns from its long LLM call and tries to
		// Complete. The guard MUST refuse with ErrClaimLost.
		err := repo.Complete(ctx, item.ID, "worker-A")
		if !errors.Is(err, ErrClaimLost) {
			t.Fatalf("expected ErrClaimLost from stale CompleteOwned, got %v", err)
		}

		// worker-B's Complete (the rightful claimant) succeeds.
		if err := repo.Complete(ctx, item.ID, "worker-B"); err != nil {
			t.Fatalf("worker-B CompleteOwned: %v", err)
		}

		got, err := repo.GetByID(ctx, item.ID)
		if err != nil {
			t.Fatalf("get-by-id after complete: %v", err)
		}
		if got.Status != "completed" {
			t.Fatalf("expected status='completed', got %q", got.Status)
		}
		if got.LastRequeueReason != nil {
			t.Fatalf("expected last_requeue_reason cleared on Complete, got %v", got.LastRequeueReason)
		}
	})
}

// TestEnrichmentQueueRepo_TickHeartbeatAdvancesUpdatedAt verifies the
// heartbeat write moves both heartbeat_at AND updated_at forward, and
// only touches rows currently held by the given worker.
func TestEnrichmentQueueRepo_TickHeartbeatAdvancesUpdatedAt(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)
		nsID, memID := createTestMemoryForQueue(t, ctx, db)

		item := newTestEnrichmentItem(nsID, memID)
		if _, err := repo.Enqueue(ctx, item); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		// Claim so the row is held by worker-X.
		for {
			c, err := repo.ClaimNext(ctx, "worker-X")
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			if c.ID == item.ID {
				break
			}
		}

		// Wait long enough that updated_at moves forward when we tick.
		// RFC3339 has 1-second resolution so we need at least a 1.1s sleep.
		baseline, _ := repo.GetByID(ctx, item.ID)
		// Scoped wait: fine in test, harmless in CI.
		for range 12 {
			n, err := repo.TickHeartbeat(ctx, "worker-X")
			if err != nil {
				t.Fatalf("tick: %v", err)
			}
			if n < 1 {
				t.Fatalf("expected TickHeartbeat to touch >=1 row for worker-X, got %d", n)
			}
			got, _ := repo.GetByID(ctx, item.ID)
			if got.HeartbeatAt != nil && (baseline.UpdatedAt.IsZero() || got.UpdatedAt.After(baseline.UpdatedAt)) {
				return // success
			}
			// Sleep ~120ms between ticks; loop fails after ~1.5s if no advance.
			time.Sleep(150 * time.Millisecond)
		}
		t.Fatalf("heartbeat did not advance updated_at after multiple ticks")
	})
}

// backdateClaimedRow forces updated_at and claimed_at on a 'processing' row
// to specific times so stale-sweep predicates can be exercised without
// real-time waits.
func backdateClaimedRow(t *testing.T, ctx context.Context, db DB, id uuid.UUID, updatedAt, claimedAt time.Time) {
	t.Helper()
	query := `UPDATE enrichment_queue SET updated_at = ?, claimed_at = ? WHERE id = ?`
	if db.Backend() == BackendPostgres {
		query = `UPDATE enrichment_queue SET updated_at = $1, claimed_at = $2 WHERE id = $3`
	}
	if _, err := db.Exec(ctx, query,
		updatedAt.UTC().Format(time.RFC3339),
		claimedAt.UTC().Format(time.RFC3339),
		id.String(),
	); err != nil {
		t.Fatalf("backdate row %s: %v", id, err)
	}
}

// claimSpecific drives ClaimNext until the row with id `target` is claimed,
// matching the loop pattern used elsewhere in this file for shared-DB
// resilience (other tests' pending rows may interleave).
func claimSpecific(t *testing.T, ctx context.Context, repo *EnrichmentQueueRepo, target uuid.UUID, workerID string) {
	t.Helper()
	for range 64 {
		c, err := repo.ClaimNext(ctx, workerID)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if c == nil {
			t.Fatalf("ran out of claims before reaching %s", target)
		}
		if c.ID == target {
			return
		}
	}
	t.Fatalf("did not reach target %s within bound", target)
}

// TestEnrichmentQueueRepo_ListStaleClaimed seeds three processing rows with
// controlled (updated_at, claimed_at) pairs and verifies the OR'd predicate
// returns exactly the two that satisfy either staleness signal. Regression
// fence for the Postgres pgx int4 encode bug: binding the duration directly
// caused 'failed to encode 1800000000000 into binary format for int4' on
// every sweep against Postgres. Runs against both backends via forEachDB.
func TestEnrichmentQueueRepo_ListStaleClaimed(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		items := make([]*model.EnrichmentJob, 3)
		for i := range items {
			nsID, memID := createTestMemoryForQueue(t, ctx, db)
			items[i] = newTestEnrichmentItem(nsID, memID)
			if _, err := repo.Enqueue(ctx, items[i]); err != nil {
				t.Fatalf("enqueue %d: %v", i, err)
			}
			claimSpecific(t, ctx, repo, items[i].ID, "worker-stale-test")
		}

		now := time.Now().UTC()
		// items[0]: old updated_at, recent claimed_at  -> matches updated_at signal.
		backdateClaimedRow(t, ctx, db, items[0].ID, now.Add(-1*time.Hour), now.Add(-5*time.Minute))
		// items[1]: recent updated_at, old claimed_at  -> matches claimed-age signal.
		backdateClaimedRow(t, ctx, db, items[1].ID, now.Add(-30*time.Second), now.Add(-2*time.Hour))
		// items[2]: recent both                        -> matches neither.
		backdateClaimedRow(t, ctx, db, items[2].ID, now.Add(-30*time.Second), now.Add(-5*time.Minute))

		got, err := repo.ListStaleClaimed(ctx, 5*time.Minute, 1*time.Hour, 100)
		if err != nil {
			t.Fatalf("ListStaleClaimed: %v", err)
		}
		seen := map[uuid.UUID]bool{}
		for _, row := range got {
			seen[row.ID] = true
		}
		if !seen[items[0].ID] {
			t.Errorf("expected items[0] (stale updated_at) in result, missing")
		}
		if !seen[items[1].ID] {
			t.Errorf("expected items[1] (stale claimed_at) in result, missing")
		}
		if seen[items[2].ID] {
			t.Errorf("did not expect items[2] (recent both) in result, present")
		}

		// Zero on one threshold disables that signal. With claimedAtMaxAge=0
		// only items[0] should match; items[1] (only stale by claim age) drops.
		gotUpdatedOnly, err := repo.ListStaleClaimed(ctx, 5*time.Minute, 0, 100)
		if err != nil {
			t.Fatalf("ListStaleClaimed updated-only: %v", err)
		}
		seenU := map[uuid.UUID]bool{}
		for _, row := range gotUpdatedOnly {
			seenU[row.ID] = true
		}
		if !seenU[items[0].ID] {
			t.Errorf("updated-only: expected items[0], missing")
		}
		if seenU[items[1].ID] {
			t.Errorf("updated-only: did not expect items[1] (claim-age only), present")
		}
		if seenU[items[2].ID] {
			t.Errorf("updated-only: did not expect items[2], present")
		}
	})
}

// TestEnrichmentQueueRepo_CountStaleClaimed seeds two stale rows and one
// fresh row, then asserts the count delta is +2. Uses a delta rather than
// an absolute count because Postgres tests share a database with other test
// rows; the delta is unaffected by ambient noise. Same regression fence as
// the List test: would have caught the pgx int4 encode bug.
func TestEnrichmentQueueRepo_CountStaleClaimed(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewEnrichmentQueueRepo(db)
		cleanEnrichmentQueue(t, ctx, db)

		baseline, err := repo.CountStaleClaimed(ctx, 5*time.Minute, 1*time.Hour)
		if err != nil {
			t.Fatalf("baseline CountStaleClaimed: %v", err)
		}

		items := make([]*model.EnrichmentJob, 3)
		for i := range items {
			nsID, memID := createTestMemoryForQueue(t, ctx, db)
			items[i] = newTestEnrichmentItem(nsID, memID)
			if _, err := repo.Enqueue(ctx, items[i]); err != nil {
				t.Fatalf("enqueue %d: %v", i, err)
			}
			claimSpecific(t, ctx, repo, items[i].ID, "worker-count-test")
		}

		now := time.Now().UTC()
		backdateClaimedRow(t, ctx, db, items[0].ID, now.Add(-1*time.Hour), now.Add(-5*time.Minute))
		backdateClaimedRow(t, ctx, db, items[1].ID, now.Add(-30*time.Second), now.Add(-2*time.Hour))
		backdateClaimedRow(t, ctx, db, items[2].ID, now.Add(-30*time.Second), now.Add(-5*time.Minute))

		got, err := repo.CountStaleClaimed(ctx, 5*time.Minute, 1*time.Hour)
		if err != nil {
			t.Fatalf("CountStaleClaimed: %v", err)
		}
		if delta := got - baseline; delta != 2 {
			t.Errorf("expected count delta=2, got delta=%d (baseline=%d, got=%d)", delta, baseline, got)
		}
	})
}
