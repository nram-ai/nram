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

func newTestIngestionLog(nsID uuid.UUID) *model.IngestionLog {
	hash := "sha256:abc123"
	return &model.IngestionLog{
		NamespaceID: nsID,
		Source:      "api",
		ContentHash: &hash,
		RawContent:  "This is some raw ingested content.",
		MemoryIDs:   []uuid.UUID{},
		Status:      "pending",
		Error:       json.RawMessage("null"),
		Metadata:    json.RawMessage(`{"origin":"test"}`),
	}
}

func TestIngestionLogRepo_Create(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	log := newTestIngestionLog(nsID)
	if err := repo.Create(ctx, log); err != nil {
		t.Fatalf("failed to create ingestion log: %v", err)
	}

	if log.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
	if log.NamespaceID != nsID {
		t.Fatalf("expected namespace_id %s, got %s", nsID, log.NamespaceID)
	}
	if log.Source != "api" {
		t.Fatalf("unexpected source: %q", log.Source)
	}
	if log.ContentHash == nil || *log.ContentHash != "sha256:abc123" {
		t.Fatalf("unexpected content_hash: %v", log.ContentHash)
	}
	if log.RawContent != "This is some raw ingested content." {
		t.Fatalf("unexpected raw_content: %q", log.RawContent)
	}
	if len(log.MemoryIDs) != 0 {
		t.Fatalf("expected 0 memory_ids, got %d", len(log.MemoryIDs))
	}
	if log.Status != "pending" {
		t.Fatalf("unexpected status: %q", log.Status)
	}
	if log.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
}

func TestIngestionLogRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	log := newTestIngestionLog(nsID)
	if err := repo.Create(ctx, log); err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if log.ID == uuid.Nil {
		t.Fatal("expected non-nil generated ID")
	}
}

func TestIngestionLogRepo_Create_WithExplicitID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	explicitID := uuid.New()
	log := newTestIngestionLog(nsID)
	log.ID = explicitID
	if err := repo.Create(ctx, log); err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if log.ID != explicitID {
		t.Fatalf("expected ID %s, got %s", explicitID, log.ID)
	}
}

func TestIngestionLogRepo_Create_NilMemoryIDs(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	log := &model.IngestionLog{
		NamespaceID: nsID,
		Source:      "api",
		RawContent:  "content",
		Status:      "pending",
	}
	if err := repo.Create(ctx, log); err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if log.MemoryIDs == nil {
		t.Fatal("expected non-nil memory_ids after create")
	}
	if len(log.MemoryIDs) != 0 {
		t.Fatalf("expected 0 memory_ids, got %d", len(log.MemoryIDs))
	}
}

func TestIngestionLogRepo_Create_NilContentHash(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	log := newTestIngestionLog(nsID)
	log.ContentHash = nil
	if err := repo.Create(ctx, log); err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if log.ContentHash != nil {
		t.Fatalf("expected nil content_hash, got %v", log.ContentHash)
	}
}

func TestIngestionLogRepo_Create_WithErrorJSON(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	log := newTestIngestionLog(nsID)
	log.Status = "failed"
	log.Error = json.RawMessage(`{"message":"parse error","code":400}`)
	if err := repo.Create(ctx, log); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetByID(ctx, log.ID)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	var errMap map[string]interface{}
	if err := json.Unmarshal(fetched.Error, &errMap); err != nil {
		t.Fatalf("failed to unmarshal error json: %v", err)
	}
	if errMap["message"] != "parse error" {
		t.Fatalf("unexpected error message: %v", errMap["message"])
	}
}

func TestIngestionLogRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	log := newTestIngestionLog(nsID)
	if err := repo.Create(ctx, log); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetByID(ctx, log.ID)
	if err != nil {
		t.Fatalf("failed to get by id: %v", err)
	}

	if fetched.ID != log.ID {
		t.Fatalf("expected ID %s, got %s", log.ID, fetched.ID)
	}
	if fetched.NamespaceID != log.NamespaceID {
		t.Fatalf("expected namespace_id %s, got %s", log.NamespaceID, fetched.NamespaceID)
	}
	if fetched.Source != log.Source {
		t.Fatalf("expected source %q, got %q", log.Source, fetched.Source)
	}
	if fetched.RawContent != log.RawContent {
		t.Fatalf("expected raw_content %q, got %q", log.RawContent, fetched.RawContent)
	}
	if fetched.Status != log.Status {
		t.Fatalf("expected status %q, got %q", log.Status, fetched.Status)
	}
}

func TestIngestionLogRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestIngestionLogRepo_ListByNamespace(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	// Create 5 ingestion logs
	for i := 0; i < 5; i++ {
		log := newTestIngestionLog(nsID)
		if err := repo.Create(ctx, log); err != nil {
			t.Fatalf("failed to create ingestion log %d: %v", i, err)
		}
	}

	results, err := repo.ListByNamespace(ctx, nsID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestIngestionLogRepo_ListByNamespace_Pagination(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	// Create 5 ingestion logs
	for i := 0; i < 5; i++ {
		log := newTestIngestionLog(nsID)
		if err := repo.Create(ctx, log); err != nil {
			t.Fatalf("failed to create ingestion log %d: %v", i, err)
		}
	}

	// Page 1: limit 2, offset 0
	page1, err := repo.ListByNamespace(ctx, nsID, 2, 0)
	if err != nil {
		t.Fatalf("failed to list page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 results for page 1, got %d", len(page1))
	}

	// Page 2: limit 2, offset 2
	page2, err := repo.ListByNamespace(ctx, nsID, 2, 2)
	if err != nil {
		t.Fatalf("failed to list page 2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected 2 results for page 2, got %d", len(page2))
	}

	// Page 3: limit 2, offset 4
	page3, err := repo.ListByNamespace(ctx, nsID, 2, 4)
	if err != nil {
		t.Fatalf("failed to list page 3: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("expected 1 result for page 3, got %d", len(page3))
	}

	// Ensure no overlap between pages
	if page1[0].ID == page2[0].ID {
		t.Fatal("page 1 and page 2 should not overlap")
	}
}

func TestIngestionLogRepo_ListByNamespace_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	results, err := repo.ListByNamespace(ctx, nsID, 10, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestIngestionLogRepo_ListByNamespace_Isolation(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID1 := createTestNamespace(t, ctx, db)
	nsID2 := createTestNamespace(t, ctx, db)

	// Create log in ns1
	l1 := newTestIngestionLog(nsID1)
	if err := repo.Create(ctx, l1); err != nil {
		t.Fatalf("failed to create in ns1: %v", err)
	}

	// Create log in ns2
	l2 := newTestIngestionLog(nsID2)
	if err := repo.Create(ctx, l2); err != nil {
		t.Fatalf("failed to create in ns2: %v", err)
	}

	// List ns1 should only see l1
	results, err := repo.ListByNamespace(ctx, nsID1, 10, 0)
	if err != nil {
		t.Fatalf("failed to list ns1: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for ns1, got %d", len(results))
	}
	if results[0].ID != l1.ID {
		t.Fatalf("expected log ID %s, got %s", l1.ID, results[0].ID)
	}

	// List ns2 should only see l2
	results, err = repo.ListByNamespace(ctx, nsID2, 10, 0)
	if err != nil {
		t.Fatalf("failed to list ns2: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for ns2, got %d", len(results))
	}
	if results[0].ID != l2.ID {
		t.Fatalf("expected log ID %s, got %s", l2.ID, results[0].ID)
	}
}

func TestIngestionLogRepo_ListByNamespace_OrderByCreatedAtDesc(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewIngestionLogRepo(db)
	nsID := createTestNamespace(t, ctx, db)

	// Create 3 logs
	ids := make([]uuid.UUID, 3)
	for i := 0; i < 3; i++ {
		log := newTestIngestionLog(nsID)
		if err := repo.Create(ctx, log); err != nil {
			t.Fatalf("failed to create log %d: %v", i, err)
		}
		ids[i] = log.ID
	}

	results, err := repo.ListByNamespace(ctx, nsID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify DESC order: most recent first
	for i := 1; i < len(results); i++ {
		if results[i].CreatedAt.After(results[i-1].CreatedAt) {
			t.Fatalf("results not in DESC order: result[%d].CreatedAt (%v) > result[%d].CreatedAt (%v)",
				i, results[i].CreatedAt, i-1, results[i-1].CreatedAt)
		}
	}
}
