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
		NamespaceID:      nsID,
		Operation:        "memorize",
		Provider:         "openai",
		Model:            "gpt-4",
		TokensInput:      150,
		TokensOutput:     50,
		TokensCacheRead:  90,
		TokensCacheWrite: 30,
		LatencyMs:        &latency,
	}
}

func TestTokenUsageRepo_Record(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestTokenUsageRepo_Record_DanglingMemoryIDNulled(t *testing.T) {
	// A memory hard-deleted between an embed call and this best-effort
	// accounting write would otherwise fail the memory_id FK (SQLSTATE 23503).
	// Record must keep the row and null only the dangling link.
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		usage := newTestTokenUsage(nsID)
		usage.Operation = "embedding"
		ghost := uuid.New() // never inserted into memories
		usage.MemoryID = &ghost

		if err := repo.Record(ctx, usage); err != nil {
			t.Fatalf("record should tolerate a dangling memory_id, got %v", err)
		}
		if usage.MemoryID != nil {
			t.Fatalf("expected memory_id nulled after FK violation, got %s", *usage.MemoryID)
		}
		if usage.TokensInput != 150 || usage.TokensOutput != 50 {
			t.Fatalf("accounting fields must survive the retry: in=%d out=%d",
				usage.TokensInput, usage.TokensOutput)
		}
	})
}

func TestTokenUsageRepo_Record_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		usage := newTestTokenUsage(nsID)
		if err := repo.Record(ctx, usage); err != nil {
			t.Fatalf("failed to record: %v", err)
		}
		if usage.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestTokenUsageRepo_Record_WithExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestTokenUsageRepo_Record_NullableFields(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestTokenUsageRepo_Record_WithOptionalIDs(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestTokenUsageRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
		// The cache columns are read back positionally alongside every other
		// int column, so a transposed scan compiles fine and only shows up
		// here. Assert the exact seeded values, not just non-zero.
		if fetched.TokensCacheRead != 90 {
			t.Fatalf("expected tokens_cache_read 90, got %d", fetched.TokensCacheRead)
		}
		if fetched.TokensCacheWrite != 30 {
			t.Fatalf("expected tokens_cache_write 30, got %d", fetched.TokensCacheWrite)
		}
		// Guard against a scan that lands cache values in the wrong fields.
		if fetched.TokensInput != 150 || fetched.TokensOutput != 50 {
			t.Fatalf("token columns transposed: in=%d out=%d, want 150/50",
				fetched.TokensInput, fetched.TokensOutput)
		}
	})
}

func TestTokenUsageRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

// TestTokenUsageRepo_ObservabilityColumns verifies that the columns added
// in migration 000022 (sqlite) / 000019 (postgres) (success, error_code,
// request_id) round-trip cleanly through Record + GetByID. This is the
// migration smoke per step 11 of the audit plan: it proves the migration
// applied AND that the repo writes/reads the new dimensions correctly on
// both backends.
func TestTokenUsageRepo_ObservabilityColumns(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Successful row with request correlation.
		errCode := "circuit_open"
		reqID := "req-abc-123"
		latency := 87
		row := &model.TokenUsage{
			NamespaceID:  nsID,
			Operation:    "fact_extraction",
			Provider:     "openai",
			Model:        "gpt-4o",
			TokensInput:  10,
			TokensOutput: 5,
			LatencyMs:    &latency,
			Success:      true,
			RequestID:    &reqID,
		}
		if err := repo.Record(ctx, row); err != nil {
			t.Fatalf("Record success row: %v", err)
		}
		if !row.Success {
			t.Errorf("Success: got %v want true", row.Success)
		}
		if row.RequestID == nil || *row.RequestID != reqID {
			t.Errorf("RequestID round-trip: got %v want %q", row.RequestID, reqID)
		}
		if row.ErrorCode != nil {
			t.Errorf("ErrorCode for success row: got %v want nil", *row.ErrorCode)
		}

		// Failure row with bounded error code.
		fail := &model.TokenUsage{
			NamespaceID:  nsID,
			Operation:    "embedding",
			Provider:     "ollama",
			Model:        "nomic-embed-text",
			TokensInput:  0,
			TokensOutput: 0,
			LatencyMs:    &latency,
			Success:      false,
			ErrorCode:    &errCode,
			RequestID:    &reqID,
		}
		if err := repo.Record(ctx, fail); err != nil {
			t.Fatalf("Record failure row: %v", err)
		}
		fetched, err := repo.GetByID(ctx, fail.ID)
		if err != nil {
			t.Fatalf("GetByID failure row: %v", err)
		}
		if fetched.Success {
			t.Error("Success: got true want false")
		}
		if fetched.ErrorCode == nil || *fetched.ErrorCode != errCode {
			t.Errorf("ErrorCode round-trip: got %v want %q", fetched.ErrorCode, errCode)
		}
		if fetched.RequestID == nil || *fetched.RequestID != reqID {
			t.Errorf("RequestID round-trip: got %v want %q", fetched.RequestID, reqID)
		}
		if fetched.LatencyMs == nil || *fetched.LatencyMs != latency {
			t.Errorf("LatencyMs round-trip: got %v want %d", fetched.LatencyMs, latency)
		}
	})
}

func TestTokenUsageRepo_QueryByScope(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Use unique operation names to avoid cross-test interference.
		uniqueSuffix := uuid.New().String()[:8]
		memorizeOp := "memorize-" + uniqueSuffix
		recallOp := "recall-" + uniqueSuffix

		// Record several usage entries with unique memorize operation.
		for i := range 3 {
			u := newTestTokenUsage(nsID)
			u.Operation = memorizeOp
			if err := repo.Record(ctx, u); err != nil {
				t.Fatalf("failed to record %d: %v", i, err)
			}
		}

		// Record one with unique recall operation.
		u := newTestTokenUsage(nsID)
		u.Operation = recallOp
		if err := repo.Record(ctx, u); err != nil {
			t.Fatalf("failed to record recall: %v", err)
		}

		from := time.Now().UTC().Add(-1 * time.Hour)
		to := time.Now().UTC().Add(1 * time.Hour)

		results, err := repo.QueryByScope(ctx, memorizeOp, from, to)
		if err != nil {
			t.Fatalf("failed to query by scope: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results for memorize, got %d", len(results))
		}

		// All should be the unique memorize operation.
		for i, r := range results {
			if r.Operation != memorizeOp {
				t.Fatalf("result %d: expected operation %q, got %q", i, memorizeOp, r.Operation)
			}
		}

		// Query recall scope.
		results, err = repo.QueryByScope(ctx, recallOp, from, to)
		if err != nil {
			t.Fatalf("failed to query recall: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for recall, got %d", len(results))
		}
	})
}

func TestTokenUsageRepo_QueryByScope_TimeRange(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestTokenUsageRepo_QueryByScope_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestTokenUsageRepo_QueryByScope_OrderDesc(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Use a unique operation name to avoid cross-test interference.
		uniqueOp := "memorize-desc-" + uuid.New().String()[:8]

		// Insert 3 records.
		ids := make([]uuid.UUID, 3)
		for i := range 3 {
			u := newTestTokenUsage(nsID)
			u.Operation = uniqueOp
			u.TokensInput = (i + 1) * 100
			if err := repo.Record(ctx, u); err != nil {
				t.Fatalf("failed to record %d: %v", i, err)
			}
			ids[i] = u.ID
		}

		from := time.Now().UTC().Add(-1 * time.Hour)
		to := time.Now().UTC().Add(1 * time.Hour)

		results, err := repo.QueryByScope(ctx, uniqueOp, from, to)
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
	})
}

func TestTokenUsageRepo_Purge(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Record 3 entries and track their IDs.
		ids := make([]uuid.UUID, 3)
		for i := range 3 {
			u := newTestTokenUsage(nsID)
			if err := repo.Record(ctx, u); err != nil {
				t.Fatalf("failed to record %d: %v", i, err)
			}
			ids[i] = u.ID
		}

		// Purge with a future cutoff; should delete at least our 3 records.
		cutoff := time.Now().UTC().Add(1 * time.Hour)
		count, err := repo.Purge(ctx, cutoff)
		if err != nil {
			t.Fatalf("failed to purge: %v", err)
		}
		if count < 3 {
			t.Fatalf("expected at least 3 purged, got %d", count)
		}

		// Verify our specific records are gone.
		for _, id := range ids {
			_, err := repo.GetByID(ctx, id)
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("expected record %s to be purged, got err: %v", id, err)
			}
		}
	})
}

func TestTokenUsageRepo_Purge_NoneOldEnough(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestTokenUsageRepo_Purge_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)

		// Use a far-past cutoff so no existing records are matched.
		cutoff := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		count, err := repo.Purge(ctx, cutoff)
		if err != nil {
			t.Fatalf("failed to purge: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 purged with far-past cutoff, got %d", count)
		}
	})
}

func TestTokenUsageRepo_ListByMemoryIDs(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewTokenUsageRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		memA := createTestMemoryForLineage(t, ctx, db, nsID)
		memB := createTestMemoryForLineage(t, ctx, db, nsID)
		memC := createTestMemoryForLineage(t, ctx, db, nsID) // not requested

		rec := func(memID uuid.UUID, op string, in, out int) {
			t.Helper()
			id := memID
			u := newTestTokenUsage(nsID)
			u.Operation = op
			u.MemoryID = &id
			u.TokensInput = in
			u.TokensOutput = out
			if err := repo.Record(ctx, u); err != nil {
				t.Fatalf("record %s/%s: %v", memID, op, err)
			}
		}

		rec(memA, "fact_extraction", 100, 20)
		rec(memA, "entity_extraction", 80, 10)
		rec(memA, "embedding", 17, 0)
		rec(memA, "memorize", 5, 5) // excluded by operations filter
		rec(memB, "fact_extraction", 50, 5)
		rec(memC, "fact_extraction", 9, 9) // excluded: memory not requested
		// Row with nil memory_id is excluded by the memory_id IN (...) filter.
		nilMem := newTestTokenUsage(nsID)
		nilMem.Operation = "fact_extraction"
		if err := repo.Record(ctx, nilMem); err != nil {
			t.Fatalf("record nil-memory row: %v", err)
		}

		ops := []string{
			"ingestion_decision", "fact_extraction", "entity_extraction",
			"query_augment", "embedding",
		}
		rows, err := repo.ListByMemoryIDs(ctx, []uuid.UUID{memA, memB}, ops)
		if err != nil {
			t.Fatalf("ListByMemoryIDs: %v", err)
		}

		// memA: fact + entity + embedding (3); memB: fact (1). memorize and
		// memC and the nil-memory row are all excluded.
		if len(rows) != 4 {
			t.Fatalf("expected 4 rows, got %d", len(rows))
		}
		for _, r := range rows {
			if r.Operation == "memorize" {
				t.Fatalf("memorize op should be filtered out")
			}
			if r.MemoryID == nil || (*r.MemoryID != memA && *r.MemoryID != memB) {
				t.Fatalf("unexpected memory_id in result: %v", r.MemoryID)
			}
		}

		// Empty filter lists short-circuit to nil.
		if got, err := repo.ListByMemoryIDs(ctx, nil, ops); err != nil || got != nil {
			t.Fatalf("empty memoryIDs: got %v err %v", got, err)
		}
		if got, err := repo.ListByMemoryIDs(ctx, []uuid.UUID{memA}, nil); err != nil || got != nil {
			t.Fatalf("empty operations: got %v err %v", got, err)
		}
	})
}
