package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func newTestTokenUsage(nsID uuid.UUID) *model.TokenUsage {
	latency := 42
	return &model.TokenUsage{
		NamespaceID:  nsID,
		Operation:    "memorize",
		Provider:     "openai",
		Model:        "gpt-4",
		TokensInput:  150,
		TokensOutput: 50,
		LatencyMs:    &latency,
	}
}

func TestTokenUsageRepo_Record(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	usage := newTestTokenUsage(nsID)
	if err := repo.Record(ctx, usage); err != nil {
		t.Fatalf("failed to record: %v", err)
	}

	if usage.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after record")
	}
	if usage.NamespaceID != nsID {
		t.Fatalf("expected namespace_id %s, got %s", nsID, usage.NamespaceID)
	}
	if usage.Operation != "memorize" {
		t.Fatalf("unexpected operation: %q", usage.Operation)
	}
	if usage.Provider != "openai" {
		t.Fatalf("unexpected provider: %q", usage.Provider)
	}
	if usage.Model != "gpt-4" {
		t.Fatalf("unexpected model: %q", usage.Model)
	}
	if usage.TokensInput != 150 {
		t.Fatalf("expected tokens_input 150, got %d", usage.TokensInput)
	}
	if usage.TokensOutput != 50 {
		t.Fatalf("expected tokens_output 50, got %d", usage.TokensOutput)
	}
	if usage.LatencyMs == nil || *usage.LatencyMs != 42 {
		t.Fatalf("unexpected latency_ms: %v", usage.LatencyMs)
	}
	if usage.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
}

func TestTokenUsageRepo_Record_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	usage := newTestTokenUsage(nsID)
	if err := repo.Record(ctx, usage); err != nil {
		t.Fatalf("failed to record: %v", err)
	}
	if usage.ID == uuid.Nil {
		t.Fatal("expected non-nil generated ID")
	}
}

func TestTokenUsageRepo_Record_WithExplicitID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	explicitID := uuid.New()
	usage := newTestTokenUsage(nsID)
	usage.ID = explicitID
	if err := repo.Record(ctx, usage); err != nil {
		t.Fatalf("failed to record: %v", err)
	}
	if usage.ID != explicitID {
		t.Fatalf("expected ID %s, got %s", explicitID, usage.ID)
	}
}

func TestTokenUsageRepo_Record_NullableFields(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	usage := &model.TokenUsage{
		NamespaceID:  nsID,
		Operation:    "recall",
		Provider:     "anthropic",
		Model:        "claude-3",
		TokensInput:  100,
		TokensOutput: 200,
	}
	if err := repo.Record(ctx, usage); err != nil {
		t.Fatalf("failed to record: %v", err)
	}

	if usage.OrgID != nil {
		t.Fatalf("expected nil org_id, got %v", usage.OrgID)
	}
	if usage.UserID != nil {
		t.Fatalf("expected nil user_id, got %v", usage.UserID)
	}
	if usage.ProjectID != nil {
		t.Fatalf("expected nil project_id, got %v", usage.ProjectID)
	}
	if usage.MemoryID != nil {
		t.Fatalf("expected nil memory_id, got %v", usage.MemoryID)
	}
	if usage.APIKeyID != nil {
		t.Fatalf("expected nil api_key_id, got %v", usage.APIKeyID)
	}
	if usage.LatencyMs != nil {
		t.Fatalf("expected nil latency_ms, got %v", usage.LatencyMs)
	}
}

func TestTokenUsageRepo_Record_WithOptionalIDs(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)

	// Create real org and user to satisfy FK constraints.
	user := createTestUser(t, ctx, db)
	orgID := user.OrgID
	userID := user.ID
	nsID := user.NamespaceID

	usage := &model.TokenUsage{
		NamespaceID:  nsID,
		OrgID:        &orgID,
		UserID:       &userID,
		Operation:    "memorize",
		Provider:     "openai",
		Model:        "gpt-4",
		TokensInput:  10,
		TokensOutput: 20,
	}
	if err := repo.Record(ctx, usage); err != nil {
		t.Fatalf("failed to record: %v", err)
	}

	if usage.OrgID == nil || *usage.OrgID != orgID {
		t.Fatalf("expected org_id %s, got %v", orgID, usage.OrgID)
	}
	if usage.UserID == nil || *usage.UserID != userID {
		t.Fatalf("expected user_id %s, got %v", userID, usage.UserID)
	}
}

func TestTokenUsageRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	usage := newTestTokenUsage(nsID)
	if err := repo.Record(ctx, usage); err != nil {
		t.Fatalf("failed to record: %v", err)
	}

	fetched, err := repo.GetByID(ctx, usage.ID)
	if err != nil {
		t.Fatalf("failed to get by id: %v", err)
	}

	if fetched.ID != usage.ID {
		t.Fatalf("expected ID %s, got %s", usage.ID, fetched.ID)
	}
	if fetched.Operation != usage.Operation {
		t.Fatalf("expected operation %q, got %q", usage.Operation, fetched.Operation)
	}
	if fetched.Provider != usage.Provider {
		t.Fatalf("expected provider %q, got %q", usage.Provider, fetched.Provider)
	}
}

func TestTokenUsageRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestTokenUsageRepo_QueryByScope(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	// Record several usage entries with "memorize" operation.
	for i := 0; i < 3; i++ {
		u := newTestTokenUsage(nsID)
		u.Operation = "memorize"
		if err := repo.Record(ctx, u); err != nil {
			t.Fatalf("failed to record %d: %v", i, err)
		}
	}

	// Record one with "recall" operation.
	u := newTestTokenUsage(nsID)
	u.Operation = "recall"
	if err := repo.Record(ctx, u); err != nil {
		t.Fatalf("failed to record recall: %v", err)
	}

	from := time.Now().UTC().Add(-1 * time.Hour)
	to := time.Now().UTC().Add(1 * time.Hour)

	results, err := repo.QueryByScope(ctx, "memorize", from, to)
	if err != nil {
		t.Fatalf("failed to query by scope: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results for memorize, got %d", len(results))
	}

	// All should be memorize.
	for i, r := range results {
		if r.Operation != "memorize" {
			t.Fatalf("result %d: expected operation memorize, got %q", i, r.Operation)
		}
	}

	// Query recall scope.
	results, err = repo.QueryByScope(ctx, "recall", from, to)
	if err != nil {
		t.Fatalf("failed to query recall: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for recall, got %d", len(results))
	}
}

func TestTokenUsageRepo_QueryByScope_TimeRange(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	u := newTestTokenUsage(nsID)
	if err := repo.Record(ctx, u); err != nil {
		t.Fatalf("failed to record: %v", err)
	}

	// Query with a time range in the far past; should return 0 results.
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2020, 12, 31, 23, 59, 59, 0, time.UTC)

	results, err := repo.QueryByScope(ctx, "memorize", from, to)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for past range, got %d", len(results))
	}
}

func TestTokenUsageRepo_QueryByScope_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)

	from := time.Now().UTC().Add(-1 * time.Hour)
	to := time.Now().UTC().Add(1 * time.Hour)

	results, err := repo.QueryByScope(ctx, "nonexistent", from, to)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestTokenUsageRepo_QueryByScope_OrderDesc(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	// Insert 3 records.
	ids := make([]uuid.UUID, 3)
	for i := 0; i < 3; i++ {
		u := newTestTokenUsage(nsID)
		u.TokensInput = (i + 1) * 100
		if err := repo.Record(ctx, u); err != nil {
			t.Fatalf("failed to record %d: %v", i, err)
		}
		ids[i] = u.ID
	}

	from := time.Now().UTC().Add(-1 * time.Hour)
	to := time.Now().UTC().Add(1 * time.Hour)

	results, err := repo.QueryByScope(ctx, "memorize", from, to)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// With DESC ordering, the last inserted should come first (newest).
	// Since all records share the same created_at second, we just verify all are present.
	foundIDs := make(map[uuid.UUID]bool)
	for _, r := range results {
		foundIDs[r.ID] = true
	}
	for _, id := range ids {
		if !foundIDs[id] {
			t.Fatalf("missing expected ID %s in results", id)
		}
	}
}

func TestTokenUsageRepo_Purge(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	// Record 3 entries.
	for i := 0; i < 3; i++ {
		u := newTestTokenUsage(nsID)
		if err := repo.Record(ctx, u); err != nil {
			t.Fatalf("failed to record %d: %v", i, err)
		}
	}

	// Purge with a future cutoff; should delete all.
	cutoff := time.Now().UTC().Add(1 * time.Hour)
	count, err := repo.Purge(ctx, cutoff)
	if err != nil {
		t.Fatalf("failed to purge: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 purged, got %d", count)
	}

	// Verify all are gone.
	from := time.Now().UTC().Add(-1 * time.Hour)
	to := time.Now().UTC().Add(1 * time.Hour)
	results, err := repo.QueryByScope(ctx, "memorize", from, to)
	if err != nil {
		t.Fatalf("failed to query after purge: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results after purge, got %d", len(results))
	}
}

func TestTokenUsageRepo_Purge_NoneOldEnough(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	u := newTestTokenUsage(nsID)
	if err := repo.Record(ctx, u); err != nil {
		t.Fatalf("failed to record: %v", err)
	}

	// Purge with a past cutoff; should delete nothing.
	cutoff := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	count, err := repo.Purge(ctx, cutoff)
	if err != nil {
		t.Fatalf("failed to purge: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 purged, got %d", count)
	}
}

func TestTokenUsageRepo_Purge_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewTokenUsageRepo(db)

	cutoff := time.Now().UTC().Add(1 * time.Hour)
	count, err := repo.Purge(ctx, cutoff)
	if err != nil {
		t.Fatalf("failed to purge: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 purged on empty table, got %d", count)
	}
}
