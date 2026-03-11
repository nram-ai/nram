package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// createTestMemoryNamespace creates a namespace suitable for memory FK references.
func createTestMemoryNamespace(t *testing.T, ctx context.Context, db DB) uuid.UUID {
	t.Helper()
	return createTestNamespace(t, ctx, db)
}

func newTestMemory(namespaceID uuid.UUID) *model.Memory {
	src := "test-source"
	return &model.Memory{
		NamespaceID: namespaceID,
		Content:     "The quick brown fox jumps over the lazy dog.",
		Source:      &src,
		Tags:        []string{"test", "fox"},
		Confidence:  0.95,
		Importance:  0.7,
		Metadata:    json.RawMessage(`{"key":"value"}`),
	}
}

func TestMemoryRepo_Create(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create memory: %v", err)
	}

	if mem.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
	if mem.NamespaceID != nsID {
		t.Fatalf("expected namespace_id %s, got %s", nsID, mem.NamespaceID)
	}
	if mem.Content != "The quick brown fox jumps over the lazy dog." {
		t.Fatalf("unexpected content: %q", mem.Content)
	}
	if mem.Source == nil || *mem.Source != "test-source" {
		t.Fatalf("unexpected source: %v", mem.Source)
	}
	if len(mem.Tags) != 2 || mem.Tags[0] != "test" || mem.Tags[1] != "fox" {
		t.Fatalf("unexpected tags: %v", mem.Tags)
	}
	if mem.Confidence != 0.95 {
		t.Fatalf("expected confidence 0.95, got %f", mem.Confidence)
	}
	if mem.Importance != 0.7 {
		t.Fatalf("expected importance 0.7, got %f", mem.Importance)
	}
	if string(mem.Metadata) != `{"key":"value"}` {
		t.Fatalf("unexpected metadata: %q", string(mem.Metadata))
	}
	if mem.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if mem.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
	if mem.DeletedAt != nil {
		t.Fatal("expected nil deleted_at")
	}
}

func TestMemoryRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := &model.Memory{
		NamespaceID: nsID,
		Content:     "auto-id memory",
		Confidence:  1.0,
		Importance:  0.5,
	}
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if mem.ID == uuid.Nil {
		t.Fatal("expected non-nil generated ID")
	}
}

func TestMemoryRepo_Create_WithExplicitID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	explicitID := uuid.New()
	mem := &model.Memory{
		ID:          explicitID,
		NamespaceID: nsID,
		Content:     "explicit-id memory",
		Confidence:  1.0,
		Importance:  0.5,
	}
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if mem.ID != explicitID {
		t.Fatalf("expected ID %s, got %s", explicitID, mem.ID)
	}
}

func TestMemoryRepo_Create_NilDefaults(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := &model.Memory{
		NamespaceID: nsID,
		Content:     "defaults memory",
		Confidence:  1.0,
		Importance:  0.5,
	}
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if mem.Tags == nil {
		t.Fatal("expected non-nil tags (empty slice)")
	}
	if len(mem.Tags) != 0 {
		t.Fatalf("expected empty tags, got %v", mem.Tags)
	}
	if string(mem.Metadata) != "{}" {
		t.Fatalf("expected metadata '{}', got %q", string(mem.Metadata))
	}
	if mem.Source != nil {
		t.Fatalf("expected nil source, got %v", mem.Source)
	}
}

func TestMemoryRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetByID(ctx, mem.ID)
	if err != nil {
		t.Fatalf("failed to get by id: %v", err)
	}

	if fetched.ID != mem.ID {
		t.Fatalf("expected ID %s, got %s", mem.ID, fetched.ID)
	}
	if fetched.Content != mem.Content {
		t.Fatalf("expected content %q, got %q", mem.Content, fetched.Content)
	}
	if fetched.Source == nil || *fetched.Source != "test-source" {
		t.Fatalf("expected source 'test-source', got %v", fetched.Source)
	}
}

func TestMemoryRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestMemoryRepo_GetByID_ExcludesSoftDeleted(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.SoftDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to soft delete: %v", err)
	}

	_, err := repo.GetByID(ctx, mem.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for soft-deleted, got %v", err)
	}
}

func TestMemoryRepo_GetBatch(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	var ids []uuid.UUID
	for i := 0; i < 3; i++ {
		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create memory %d: %v", i, err)
		}
		ids = append(ids, mem.ID)
	}

	// Fetch all three
	results, err := repo.GetBatch(ctx, ids)
	if err != nil {
		t.Fatalf("failed to get batch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestMemoryRepo_GetBatch_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)

	results, err := repo.GetBatch(ctx, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if results != nil {
		t.Fatalf("expected nil result for empty input, got %v", results)
	}
}

func TestMemoryRepo_GetBatch_ExcludesSoftDeleted(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem1 := newTestMemory(nsID)
	mem2 := newTestMemory(nsID)
	if err := repo.Create(ctx, mem1); err != nil {
		t.Fatalf("failed to create mem1: %v", err)
	}
	if err := repo.Create(ctx, mem2); err != nil {
		t.Fatalf("failed to create mem2: %v", err)
	}

	// Soft-delete mem1
	if err := repo.SoftDelete(ctx, mem1.ID); err != nil {
		t.Fatalf("failed to soft delete: %v", err)
	}

	results, err := repo.GetBatch(ctx, []uuid.UUID{mem1.ID, mem2.ID})
	if err != nil {
		t.Fatalf("failed to get batch: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result (excluded soft-deleted), got %d", len(results))
	}
	if results[0].ID != mem2.ID {
		t.Fatalf("expected ID %s, got %s", mem2.ID, results[0].ID)
	}
}

func TestMemoryRepo_ListByNamespace(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	// Create 3 memories
	for i := 0; i < 3; i++ {
		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create memory %d: %v", i, err)
		}
	}

	results, err := repo.ListByNamespace(ctx, nsID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Should be ordered by created_at DESC (newest first)
	for i := 1; i < len(results); i++ {
		if results[i].CreatedAt.After(results[i-1].CreatedAt) {
			t.Fatal("expected results ordered by created_at DESC")
		}
	}
}

func TestMemoryRepo_ListByNamespace_Pagination(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	for i := 0; i < 5; i++ {
		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create memory %d: %v", i, err)
		}
	}

	// Page 1: limit 2, offset 0
	page1, err := repo.ListByNamespace(ctx, nsID, 2, 0)
	if err != nil {
		t.Fatalf("failed to list page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 results on page 1, got %d", len(page1))
	}

	// Page 2: limit 2, offset 2
	page2, err := repo.ListByNamespace(ctx, nsID, 2, 2)
	if err != nil {
		t.Fatalf("failed to list page 2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected 2 results on page 2, got %d", len(page2))
	}

	// No overlap
	if page1[0].ID == page2[0].ID || page1[1].ID == page2[1].ID {
		t.Fatal("pages should not overlap")
	}
}

func TestMemoryRepo_ListByNamespace_ExcludesSoftDeleted(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.SoftDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to soft delete: %v", err)
	}

	results, err := repo.ListByNamespace(ctx, nsID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results (excluded soft-deleted), got %d", len(results))
	}
}

func TestMemoryRepo_ListByNamespace_EmptyNamespace(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)

	results, err := repo.ListByNamespace(ctx, uuid.New(), 10, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestMemoryRepo_Update(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	originalUpdatedAt := mem.UpdatedAt

	// Update fields
	mem.Content = "Updated content"
	newSource := "updated-source"
	mem.Source = &newSource
	mem.Tags = []string{"updated"}
	mem.Metadata = json.RawMessage(`{"updated":true}`)
	mem.Importance = 0.9

	if err := repo.Update(ctx, mem); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	if mem.Content != "Updated content" {
		t.Fatalf("expected content 'Updated content', got %q", mem.Content)
	}
	if mem.Source == nil || *mem.Source != "updated-source" {
		t.Fatalf("expected source 'updated-source', got %v", mem.Source)
	}
	if len(mem.Tags) != 1 || mem.Tags[0] != "updated" {
		t.Fatalf("expected tags ['updated'], got %v", mem.Tags)
	}
	if string(mem.Metadata) != `{"updated":true}` {
		t.Fatalf("expected metadata '{\"updated\":true}', got %q", string(mem.Metadata))
	}
	if mem.Importance != 0.9 {
		t.Fatalf("expected importance 0.9, got %f", mem.Importance)
	}
	if !mem.UpdatedAt.After(originalUpdatedAt) && mem.UpdatedAt != originalUpdatedAt {
		t.Fatal("expected updated_at to advance")
	}

	// Verify via fresh fetch
	fetched, err := repo.GetByID(ctx, mem.ID)
	if err != nil {
		t.Fatalf("failed to get after update: %v", err)
	}
	if fetched.Content != "Updated content" {
		t.Fatalf("expected fetched content 'Updated content', got %q", fetched.Content)
	}
}

func TestMemoryRepo_Update_SoftDeletedFails(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.SoftDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to soft delete: %v", err)
	}

	mem.Content = "should not update"
	err := repo.Update(ctx, mem)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows when updating soft-deleted, got %v", err)
	}
}

func TestMemoryRepo_SoftDelete(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	beforeDelete := time.Now().UTC().Add(-time.Second)

	if err := repo.SoftDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to soft delete: %v", err)
	}

	// Verify it's not returned by GetByID
	_, err := repo.GetByID(ctx, mem.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after soft delete, got %v", err)
	}

	// Verify deleted_at was set by reading directly
	fetched, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
	if err != nil {
		t.Fatalf("failed to get include deleted: %v", err)
	}
	if fetched.DeletedAt == nil {
		t.Fatal("expected non-nil deleted_at after soft delete")
	}
	if fetched.DeletedAt.Before(beforeDelete) {
		t.Fatal("expected deleted_at to be recent")
	}
}

func TestMemoryRepo_SoftDelete_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)

	err := repo.SoftDelete(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestMemoryRepo_SoftDelete_AlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.SoftDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to soft delete first time: %v", err)
	}

	// Second soft delete should fail (already deleted)
	err := repo.SoftDelete(ctx, mem.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for double soft delete, got %v", err)
	}
}

func TestMemoryRepo_HardDelete(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.HardDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to hard delete: %v", err)
	}

	// Verify completely gone (not even with include deleted)
	_, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after hard delete, got %v", err)
	}
}

func TestMemoryRepo_HardDelete_SoftDeletedFirst(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.SoftDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to soft delete: %v", err)
	}

	if err := repo.HardDelete(ctx, mem.ID); err != nil {
		t.Fatalf("failed to hard delete after soft delete: %v", err)
	}

	_, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after hard delete, got %v", err)
	}
}

func TestMemoryRepo_Create_WithOptionalFields(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewMemoryRepo(db)
	nsID := createTestMemoryNamespace(t, ctx, db)

	dim := 384
	expires := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	purge := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second)

	mem := &model.Memory{
		NamespaceID:  nsID,
		Content:      "memory with optional fields",
		EmbeddingDim: &dim,
		Tags:         []string{"optional"},
		Confidence:   0.8,
		Importance:   0.6,
		ExpiresAt:    &expires,
		PurgeAfter:   &purge,
		Metadata:     json.RawMessage(`{}`),
	}

	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetByID(ctx, mem.ID)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if fetched.EmbeddingDim == nil || *fetched.EmbeddingDim != 384 {
		t.Fatalf("expected embedding_dim 384, got %v", fetched.EmbeddingDim)
	}
	if fetched.ExpiresAt == nil || !fetched.ExpiresAt.Equal(expires) {
		t.Fatalf("expected expires_at %v, got %v", expires, fetched.ExpiresAt)
	}
	if fetched.PurgeAfter == nil || !fetched.PurgeAfter.Equal(purge) {
		t.Fatalf("expected purge_after %v, got %v", purge, fetched.PurgeAfter)
	}
}
