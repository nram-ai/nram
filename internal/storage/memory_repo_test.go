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
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
		if !jsonEqual(string(mem.Metadata), `{"key":"value"}`) {
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
	})
}

func TestMemoryRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_Create_WithExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_Create_NilDefaults(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestMemoryRepo_GetByID_ExcludesSoftDeleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to soft delete: %v", err)
		}

		_, err := repo.GetByID(ctx, mem.ID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows for soft-deleted, got %v", err)
		}
	})
}

func TestMemoryRepo_GetBatch(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_GetBatch_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)

		results, err := repo.GetBatch(ctx, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected empty result for empty input, got %v", results)
		}
	})
}

func TestMemoryRepo_GetBatch_ExcludesSoftDeleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
		if err := repo.SoftDelete(ctx, mem1.ID, mem1.NamespaceID); err != nil {
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
	})
}

func TestMemoryRepo_ListByNamespace(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_ListByNamespace_Pagination(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_ListByNamespace_ExcludesSoftDeleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to soft delete: %v", err)
		}

		results, err := repo.ListByNamespace(ctx, nsID, 10, 0)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results (excluded soft-deleted), got %d", len(results))
		}
	})
}

func TestMemoryRepo_ListByNamespace_EmptyNamespace(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)

		results, err := repo.ListByNamespace(ctx, uuid.New(), 10, 0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

// seedFilterMemories inserts a fixed set of memories used by the filter
// tests in a deterministic order (alpha → beta → gamma → delta) so that
// created_at-based assertions don't depend on map iteration order.
func seedFilterMemories(t *testing.T, ctx context.Context, repo *MemoryRepo, nsID uuid.UUID) map[string]*model.Memory {
	t.Helper()

	srcA := "ingest-pipeline"
	srcB := "manual-entry"

	type seed struct {
		label string
		mem   *model.Memory
	}
	order := []seed{
		{"alpha", &model.Memory{
			NamespaceID: nsID,
			Content:     "Alpha content about cats",
			Source:      &srcA,
			Tags:        []string{"animals", "cat"},
			Confidence:  0.9, Importance: 0.5,
			Enriched: true,
		}},
		{"beta", &model.Memory{
			NamespaceID: nsID,
			Content:     "Beta content about dogs",
			Source:      &srcA,
			Tags:        []string{"animals", "dog"},
			Confidence:  0.8, Importance: 0.5,
			Enriched: false,
		}},
		{"gamma", &model.Memory{
			NamespaceID: nsID,
			Content:     "Gamma content about birds",
			Source:      &srcB,
			Tags:        []string{"animals", "bird"},
			Confidence:  0.7, Importance: 0.5,
			Enriched: true,
		}},
		{"delta", &model.Memory{
			NamespaceID: nsID,
			Content:     "Delta content unrelated",
			Source:      &srcB,
			Tags:        []string{"misc"},
			Confidence:  0.6, Importance: 0.5,
			Enriched: false,
		}},
	}

	mems := make(map[string]*model.Memory, len(order))
	for _, s := range order {
		if err := repo.Create(ctx, s.mem); err != nil {
			t.Fatalf("failed to create memory %s: %v", s.label, err)
		}
		mems[s.label] = s.mem
		// Stagger created_at to make ordering deterministic. The repo stores
		// timestamps with second-level resolution, so sleep just over 1s.
		time.Sleep(1100 * time.Millisecond)
	}
	return mems
}

func TestMemoryRepo_ListByNamespaceFiltered_Tags(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		seedFilterMemories(t, ctx, repo, nsID)

		// Single-tag filter: animals → 3 matches
		results, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Tags: []string{"animals"},
		}, 100, 0)
		if err != nil {
			t.Fatalf("filter by single tag: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 animals, got %d", len(results))
		}

		// Multi-tag filter (AND): animals + dog → 1 match
		results, err = repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Tags: []string{"animals", "dog"},
		}, 100, 0)
		if err != nil {
			t.Fatalf("filter by multi tag: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 dog, got %d", len(results))
		}
		if results[0].Content != "Beta content about dogs" {
			t.Fatalf("unexpected match: %q", results[0].Content)
		}

		// Tag with no matches
		results, err = repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Tags: []string{"nonexistent"},
		}, 100, 0)
		if err != nil {
			t.Fatalf("filter by missing tag: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestMemoryRepo_ListByNamespaceFiltered_Enriched(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		seedFilterMemories(t, ctx, repo, nsID)

		yes, no := true, false
		enriched, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Enriched: &yes,
		}, 100, 0)
		if err != nil {
			t.Fatalf("enriched filter: %v", err)
		}
		if len(enriched) != 2 {
			t.Fatalf("expected 2 enriched, got %d", len(enriched))
		}

		notEnriched, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Enriched: &no,
		}, 100, 0)
		if err != nil {
			t.Fatalf("not-enriched filter: %v", err)
		}
		if len(notEnriched) != 2 {
			t.Fatalf("expected 2 not-enriched, got %d", len(notEnriched))
		}
	})
}

func TestMemoryRepo_ListByNamespaceFiltered_Source(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		seedFilterMemories(t, ctx, repo, nsID)

		results, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Source: "INGEST", // case-insensitive substring
		}, 100, 0)
		if err != nil {
			t.Fatalf("source filter: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 ingest results, got %d", len(results))
		}
	})
}

func TestMemoryRepo_ListByNamespaceFiltered_Search(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		seedFilterMemories(t, ctx, repo, nsID)

		results, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Search: "DOGS",
		}, 100, 0)
		if err != nil {
			t.Fatalf("search filter: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 search result, got %d", len(results))
		}
		if results[0].Content != "Beta content about dogs" {
			t.Fatalf("unexpected search match: %q", results[0].Content)
		}
	})
}

func TestMemoryRepo_ListByNamespaceFiltered_DateRange(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		mems := seedFilterMemories(t, ctx, repo, nsID)

		// Filter to memories created at or after gamma's timestamp.
		gammaTime := mems["gamma"].CreatedAt
		results, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			DateFrom: &gammaTime,
		}, 100, 0)
		if err != nil {
			t.Fatalf("date_from filter: %v", err)
		}
		// gamma + delta = 2
		if len(results) != 2 {
			t.Fatalf("expected 2 results from gamma onwards, got %d", len(results))
		}
	})
}

func TestMemoryRepo_CountByNamespaceFiltered_MatchesList(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		seedFilterMemories(t, ctx, repo, nsID)

		filters := MemoryListFilters{Tags: []string{"animals"}}
		listResults, err := repo.ListByNamespaceFiltered(ctx, nsID, filters, 100, 0)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		count, err := repo.CountByNamespaceFiltered(ctx, nsID, filters)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != len(listResults) {
			t.Fatalf("count %d != list length %d", count, len(listResults))
		}
		if count != 3 {
			t.Fatalf("expected count=3, got %d", count)
		}
	})
}

func TestMemoryRepo_ListIDsByNamespaceFiltered_RespectsCap(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		seedFilterMemories(t, ctx, repo, nsID)

		// Cap below total — should truncate.
		ids, err := repo.ListIDsByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 2)
		if err != nil {
			t.Fatalf("list ids: %v", err)
		}
		if len(ids) != 2 {
			t.Fatalf("expected 2 ids capped, got %d", len(ids))
		}

		// Cap above total — should return everything.
		ids, err = repo.ListIDsByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 100)
		if err != nil {
			t.Fatalf("list ids unbounded: %v", err)
		}
		if len(ids) != 4 {
			t.Fatalf("expected 4 ids, got %d", len(ids))
		}

		// With filter
		ids, err = repo.ListIDsByNamespaceFiltered(ctx, nsID, MemoryListFilters{
			Tags: []string{"animals"},
		}, 100)
		if err != nil {
			t.Fatalf("list ids filtered: %v", err)
		}
		if len(ids) != 3 {
			t.Fatalf("expected 3 filtered ids, got %d", len(ids))
		}

		// Zero cap → empty
		ids, err = repo.ListIDsByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 0)
		if err != nil {
			t.Fatalf("list ids zero cap: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("expected 0 ids with zero cap, got %d", len(ids))
		}
	})
}

func TestMemoryRepo_Update(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		originalUpdatedAt := mem.UpdatedAt
		time.Sleep(time.Second) // Ensure updated_at advances past second boundary

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
		if !jsonEqual(string(mem.Metadata), `{"updated":true}`) {
			t.Fatalf("expected metadata '{\"updated\":true}', got %q", string(mem.Metadata))
		}
		if mem.Importance != 0.9 {
			t.Fatalf("expected importance 0.9, got %f", mem.Importance)
		}
		if mem.UpdatedAt.Before(originalUpdatedAt) {
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
	})
}

func TestMemoryRepo_Update_SoftDeletedFails(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to soft delete: %v", err)
		}

		mem.Content = "should not update"
		err := repo.Update(ctx, mem)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows when updating soft-deleted, got %v", err)
		}
	})
}

func TestMemoryRepo_SoftDelete(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		beforeDelete := time.Now().UTC().Add(-time.Second)

		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
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
	})
}

func TestMemoryRepo_SoftDelete_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)

		err := repo.SoftDelete(ctx, uuid.New(), uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestMemoryRepo_SoftDelete_AlreadyDeleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to soft delete first time: %v", err)
		}

		// Second soft delete should fail (already deleted)
		err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows for double soft delete, got %v", err)
		}
	})
}

func TestMemoryRepo_HardDelete(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.HardDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to hard delete: %v", err)
		}

		// Verify completely gone (not even with include deleted)
		_, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after hard delete, got %v", err)
		}
	})
}

// recordingVectorStore satisfies the VectorStore interface and records each
// Delete call so tests can assert the purge hook fired.
type recordingVectorStore struct {
	deletes []uuid.UUID
}

func (r *recordingVectorStore) Upsert(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ []float32, _ int) error {
	return nil
}
func (r *recordingVectorStore) UpsertBatch(_ context.Context, _ []VectorUpsertItem) error {
	return nil
}
func (r *recordingVectorStore) Search(_ context.Context, _ []float32, _ uuid.UUID, _ int, _ int) ([]VectorSearchResult, error) {
	return nil, nil
}
func (r *recordingVectorStore) GetByIDs(_ context.Context, _ []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	return map[uuid.UUID][]float32{}, nil
}
func (r *recordingVectorStore) Delete(_ context.Context, id uuid.UUID) error {
	r.deletes = append(r.deletes, id)
	return nil
}
func (r *recordingVectorStore) Ping(_ context.Context) error { return nil }

// TestMemoryRepo_SoftDelete_PurgesVector verifies that soft-delete asks
// the attached vector store to drop the vector alongside the row-level
// state change. This is the load-bearing hook for keeping the HNSW and
// pgvector indexes in sync with the recall-visible memory set.
func TestMemoryRepo_SoftDelete_PurgesVector(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		vs := &recordingVectorStore{}
		repo.AttachVectorStore(vs)

		nsID := createTestMemoryNamespace(t, ctx, db)
		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}

		if len(vs.deletes) != 1 || vs.deletes[0] != mem.ID {
			t.Errorf("expected vector store Delete called with %s, got %v", mem.ID, vs.deletes)
		}
	})
}

// TestMemoryRepo_HardDelete_PurgesVector verifies the same hook fires on
// hard delete so the in-memory HNSW graph drops the node (FK CASCADE
// handles persisted vector rows; the in-memory index needs an explicit
// call).
func TestMemoryRepo_HardDelete_PurgesVector(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		vs := &recordingVectorStore{}
		repo.AttachVectorStore(vs)

		nsID := createTestMemoryNamespace(t, ctx, db)
		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		if err := repo.HardDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("hard delete: %v", err)
		}

		if len(vs.deletes) != 1 || vs.deletes[0] != mem.ID {
			t.Errorf("expected vector store Delete called with %s, got %v", mem.ID, vs.deletes)
		}
	})
}

// TestMemoryRepo_SoftDelete_NoVectorStore_NoPanic verifies the purge hook
// gracefully handles the nil-vectorStore case so callers that never
// AttachVectorStore still function.
func TestMemoryRepo_SoftDelete_NoVectorStore_NoPanic(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		// Deliberately do not AttachVectorStore.

		nsID := createTestMemoryNamespace(t, ctx, db)
		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}
		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}
	})
}

// TestMemoryRepo_HardDeleteSoftDeletedBefore_RetentionSweep verifies the
// retention sweep hard-deletes only rows whose deleted_at is past the
// cutoff, and returns the count of rows removed.
func TestMemoryRepo_HardDeleteSoftDeletedBefore_RetentionSweep(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		old := newTestMemory(nsID)
		recent := newTestMemory(nsID)
		live := newTestMemory(nsID)
		for _, m := range []*model.Memory{old, recent, live} {
			if err := repo.Create(ctx, m); err != nil {
				t.Fatalf("create: %v", err)
			}
		}

		if err := repo.SoftDelete(ctx, old.ID, nsID); err != nil {
			t.Fatalf("soft delete old: %v", err)
		}
		if err := repo.SoftDelete(ctx, recent.ID, nsID); err != nil {
			t.Fatalf("soft delete recent: %v", err)
		}

		// Set deleted_at directly to put old well before cutoff and recent
		// well after it; avoids relying on wall-clock spacing, which
		// produces flaky results under concurrent test load.
		backdate := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
		updateSQL := `UPDATE memories SET deleted_at = ? WHERE id = ?`
		if db.Backend() == BackendPostgres {
			updateSQL = `UPDATE memories SET deleted_at = $1 WHERE id = $2`
		}
		if _, err := db.Exec(ctx, updateSQL, backdate, old.ID.String()); err != nil {
			t.Fatalf("backdate old: %v", err)
		}

		cutoff := time.Now().UTC().Add(-1 * time.Hour)
		deleted, err := repo.HardDeleteSoftDeletedBefore(ctx, cutoff, 1000)
		if err != nil {
			t.Fatalf("hard delete soft-deleted before: %v", err)
		}
		if deleted != 1 {
			t.Errorf("expected 1 row hard-deleted (the old one), got %d", deleted)
		}

		// The old memory is fully gone; the recent one is still soft-deleted;
		// the live one is unchanged.
		if _, err := repo.getByIDIncludeDeleted(ctx, old.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("expected old to be hard-deleted, got err=%v", err)
		}
		fetchedRecent, err := repo.getByIDIncludeDeleted(ctx, recent.ID)
		if err != nil {
			t.Fatalf("recent should still exist (soft-deleted): %v", err)
		}
		if fetchedRecent.DeletedAt == nil {
			t.Error("recent should still be soft-deleted")
		}
		if _, err := repo.GetByID(ctx, live.ID); err != nil {
			t.Errorf("live memory should still be readable: %v", err)
		}
	})
}

func TestMemoryRepo_HardDelete_SoftDeletedFirst(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to soft delete: %v", err)
		}

		if err := repo.HardDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to hard delete after soft delete: %v", err)
		}

		_, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after hard delete, got %v", err)
		}
	})
}

func TestMemoryRepo_Create_WithOptionalFields(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
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
	})
}

func TestMemoryRepo_ListExpired(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestMemoryNamespace(t, ctx, db)

		memRepo := NewMemoryRepo(db)
		past := time.Now().Add(-1 * time.Hour)
		mem := &model.Memory{
			NamespaceID: nsID,
			Content:     "expired memory",
			Confidence:  0.9,
			Importance:  0.5,
			ExpiresAt:   &past,
		}
		if err := memRepo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		results, err := memRepo.ListExpired(ctx, time.Now(), 10)
		if err != nil {
			t.Fatalf("ListExpired failed: %v", err)
		}
		if len(results) < 1 {
			t.Fatalf("expected at least 1 expired memory, got %d", len(results))
		}
	})
}

func TestMemoryRepo_ListPurgeable(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		nsID := createTestMemoryNamespace(t, ctx, db)

		memRepo := NewMemoryRepo(db)
		mem := &model.Memory{
			NamespaceID: nsID,
			Content:     "purgeable memory",
			Confidence:  0.9,
			Importance:  0.5,
		}
		if err := memRepo.Create(ctx, mem); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Soft delete it
		if err := memRepo.SoftDelete(ctx, mem.ID, mem.NamespaceID); err != nil {
			t.Fatalf("failed to soft delete: %v", err)
		}

		results, err := memRepo.ListPurgeable(ctx, time.Now().Add(1*time.Hour), 10)
		if err != nil {
			t.Fatalf("ListPurgeable failed: %v", err)
		}
		if len(results) < 1 {
			t.Fatalf("expected at least 1 purgeable memory, got %d", len(results))
		}
	})
}
