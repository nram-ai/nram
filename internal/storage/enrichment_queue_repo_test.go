package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

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
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
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
}

func TestEnrichmentQueueRepo_Enqueue_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	if item.ID == uuid.Nil {
		t.Fatal("expected non-nil generated ID")
	}
}

func TestEnrichmentQueueRepo_Enqueue_WithExplicitID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	explicitID := uuid.New()
	item := &model.EnrichmentJob{
		ID:          explicitID,
		MemoryID:    memID,
		NamespaceID: nsID,
	}
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	if item.ID != explicitID {
		t.Fatalf("expected ID %s, got %s", explicitID, item.ID)
	}
}

func TestEnrichmentQueueRepo_Enqueue_WithPriority(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := &model.EnrichmentJob{
		MemoryID:    memID,
		NamespaceID: nsID,
		Priority:    10,
	}
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	if item.Priority != 10 {
		t.Fatalf("expected priority 10, got %d", item.Priority)
	}
}

func TestEnrichmentQueueRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
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
}

func TestEnrichmentQueueRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestEnrichmentQueueRepo_ClaimNext(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	claimed, err := repo.ClaimNext(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim next: %v", err)
	}

	if claimed.ID != item.ID {
		t.Fatalf("expected ID %s, got %s", item.ID, claimed.ID)
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
}

func TestEnrichmentQueueRepo_ClaimNext_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)

	_, err := repo.ClaimNext(ctx, "worker-1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows on empty queue, got %v", err)
	}
}

func TestEnrichmentQueueRepo_ClaimNext_PriorityOrder(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	// Enqueue low priority first
	low := &model.EnrichmentJob{MemoryID: memID, NamespaceID: nsID, Priority: 1}
	if err := repo.Enqueue(ctx, low); err != nil {
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
	if err := repo.Enqueue(ctx, high); err != nil {
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
}

func TestEnrichmentQueueRepo_ClaimNext_SkipsProcessing(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Claim it
	_, err := repo.ClaimNext(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	// Second claim should find nothing
	_, err = repo.ClaimNext(ctx, "worker-2")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows when all items claimed, got %v", err)
	}
}

func TestEnrichmentQueueRepo_Complete(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Claim it first
	_, err := repo.ClaimNext(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	// Complete it
	if err := repo.Complete(ctx, item.ID); err != nil {
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
}

func TestEnrichmentQueueRepo_Complete_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)

	err := repo.Complete(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestEnrichmentQueueRepo_Fail(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Claim it
	_, err := repo.ClaimNext(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	// Fail it
	if err := repo.Fail(ctx, item.ID, "something went wrong"); err != nil {
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
	if string(fetched.LastError) != "something went wrong" {
		t.Fatalf("expected last_error 'something went wrong', got %q", string(fetched.LastError))
	}
	if fetched.Attempts != 1 {
		t.Fatalf("expected attempts 1, got %d", fetched.Attempts)
	}
}

func TestEnrichmentQueueRepo_Fail_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)

	err := repo.Fail(ctx, uuid.New(), "error")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestEnrichmentQueueRepo_Retry(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Claim it
	_, err := repo.ClaimNext(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	// Fail it
	if err := repo.Fail(ctx, item.ID, "transient error"); err != nil {
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
}

func TestEnrichmentQueueRepo_Retry_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)

	err := repo.Retry(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestEnrichmentQueueRepo_Retry_CanBeClaimedAgain(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Claim, fail, retry
	if _, err := repo.ClaimNext(ctx, "worker-1"); err != nil {
		t.Fatalf("failed to claim: %v", err)
	}
	if err := repo.Fail(ctx, item.ID, "error"); err != nil {
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
}

func TestEnrichmentQueueRepo_FullLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewEnrichmentQueueRepo(db)
	nsID, memID := createTestMemoryForQueue(t, ctx, db)

	// Enqueue
	item := newTestEnrichmentItem(nsID, memID)
	if err := repo.Enqueue(ctx, item); err != nil {
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
	if err := repo.Complete(ctx, claimed.ID); err != nil {
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

	// Queue should now be empty
	_, err = repo.ClaimNext(ctx, "worker-lifecycle")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected empty queue, got %v", err)
	}
}

// Suppress unused import warning for json.
var _ = json.RawMessage{}
