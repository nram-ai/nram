package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
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

func TestMemoryRepo_Create_NormalizesQuotedTags(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := &model.Memory{
			NamespaceID: nsID,
			Content:     "tag normalization on create",
			Confidence:  1.0,
			Importance:  0.5,
			Tags: []string{
				`"behavioral contract"`,
				"clean-tag",
				`"failure modes"`,
				"clean-tag",
				"  spacey  ",
			},
		}
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		want := []string{"behavioral contract", "clean-tag", "failure modes", "spacey"}
		if !reflect.DeepEqual(mem.Tags, want) {
			t.Fatalf("in-memory tags: got %v, want %v", mem.Tags, want)
		}

		reloaded, err := repo.GetByID(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if !reflect.DeepEqual(reloaded.Tags, want) {
			t.Fatalf("persisted tags: got %v, want %v", reloaded.Tags, want)
		}
	})
}

func TestMemoryRepo_Update_NormalizesQuotedTags(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := &model.Memory{
			NamespaceID: nsID,
			Content:     "tag normalization on update",
			Confidence:  1.0,
			Importance:  0.5,
			Tags:        []string{"original"},
		}
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		mem.Tags = []string{`"updated tag"`, "plain", `"updated tag"`}
		if err := repo.Update(ctx, mem); err != nil {
			t.Fatalf("update: %v", err)
		}

		want := []string{"updated tag", "plain"}
		reloaded, err := repo.GetByID(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if !reflect.DeepEqual(reloaded.Tags, want) {
			t.Fatalf("persisted tags: got %v, want %v", reloaded.Tags, want)
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

func TestMemoryRepo_ListByNamespaceFiltered_HideSuperseded(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		winner := newTestMemory(nsID)
		winner.Content = "winner content"
		if err := repo.Create(ctx, winner); err != nil {
			t.Fatalf("create winner: %v", err)
		}
		loser := newTestMemory(nsID)
		loser.Content = "loser content"
		if err := repo.Create(ctx, loser); err != nil {
			t.Fatalf("create loser: %v", err)
		}
		now := time.Now().UTC()
		loser.SupersededBy = &winner.ID
		loser.SupersededAt = &now
		if err := repo.Update(ctx, loser); err != nil {
			t.Fatalf("mark loser superseded: %v", err)
		}

		all, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{}, 100, 0)
		if err != nil {
			t.Fatalf("list default: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("default filters should include superseded: got %d, want 2", len(all))
		}

		hidden, err := repo.ListByNamespaceFiltered(ctx, nsID, MemoryListFilters{HideSuperseded: true}, 100, 0)
		if err != nil {
			t.Fatalf("list hide: %v", err)
		}
		if len(hidden) != 1 {
			t.Fatalf("HideSuperseded should drop loser: got %d, want 1", len(hidden))
		}
		if hidden[0].ID != winner.ID {
			t.Fatalf("survivor should be winner; got %s, want %s", hidden[0].ID, winner.ID)
		}

		count, err := repo.CountByNamespaceFiltered(ctx, nsID, MemoryListFilters{HideSuperseded: true})
		if err != nil {
			t.Fatalf("count hide: %v", err)
		}
		if count != len(hidden) {
			t.Fatalf("count %d != list length %d", count, len(hidden))
		}
	})
}

func TestMemoryRepo_LookupByContentHash_SkipsSuperseded(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		hash := "deadbeef"
		older := newTestMemory(nsID)
		older.Content = "duplicate content"
		older.ContentHash = hash
		if err := repo.Create(ctx, older); err != nil {
			t.Fatalf("create older: %v", err)
		}

		// Stagger so created_at ordering is deterministic regardless of backend.
		time.Sleep(1100 * time.Millisecond)

		newer := newTestMemory(nsID)
		newer.Content = "duplicate content"
		newer.ContentHash = hash
		if err := repo.Create(ctx, newer); err != nil {
			t.Fatalf("create newer: %v", err)
		}

		// Mark the older one superseded (mirrors paraphrase-dedup picking the
		// newer row as winner). Without the supersede filter, the
		// `ORDER BY created_at ASC LIMIT 1` would return the older loser.
		now := time.Now().UTC()
		older.SupersededBy = &newer.ID
		older.SupersededAt = &now
		if err := repo.Update(ctx, older); err != nil {
			t.Fatalf("mark older superseded: %v", err)
		}

		got, err := repo.LookupByContentHash(ctx, nsID, hash)
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if got == nil {
			t.Fatalf("expected a hit, got nil")
		}
		if got.ID != newer.ID {
			t.Fatalf("expected newer winner %s, got loser %s", newer.ID, got.ID)
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
// Delete call so tests can assert the purge hook fired. Both the simple
// `deletes` slice (ID-only, for memory tests) and `deleteCalls` slice
// (with VectorKind, for entity tests) are populated on every Delete.
type recordingVectorStore struct {
	deletes     []uuid.UUID
	deleteCalls []recordedVectorDelete
}

type recordedVectorDelete struct {
	kind VectorKind
	id   uuid.UUID
}

func (r *recordingVectorStore) Upsert(_ context.Context, _ VectorKind, _ uuid.UUID, _ uuid.UUID, _ []float32, _ int) error {
	return nil
}
func (r *recordingVectorStore) UpsertBatch(_ context.Context, _ []VectorUpsertItem) error {
	return nil
}
func (r *recordingVectorStore) Search(_ context.Context, _ VectorKind, _ []float32, _ uuid.UUID, _ int, _ int) ([]VectorSearchResult, error) {
	return nil, nil
}
func (r *recordingVectorStore) GetByIDs(_ context.Context, _ VectorKind, _ []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	return map[uuid.UUID][]float32{}, nil
}
func (r *recordingVectorStore) Delete(_ context.Context, kind VectorKind, id uuid.UUID) error {
	r.deletes = append(r.deletes, id)
	r.deleteCalls = append(r.deleteCalls, recordedVectorDelete{kind: kind, id: id})
	return nil
}
func (r *recordingVectorStore) TruncateAllVectors(_ context.Context) error { return nil }
func (r *recordingVectorStore) Ping(_ context.Context) error               { return nil }

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

// TestMemoryRepo_ClearAllEmbeddingDims is the load-bearing assertion for
// the embedding-model switch cascade: every live memory's embedding_dim
// gets NULL'd in one call so the re-embed worker treats every row as
// needing fresh vectors. Soft-deleted memories are left untouched (they
// will not be re-enqueued by EnqueueAllLiveMemories anyway).
func TestMemoryRepo_ClearAllEmbeddingDims(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		// ClearAllEmbeddingDims is a whole-DB UPDATE; the shared Postgres
		// schema retains memories from prior tests with embedding_dim set,
		// inflating the rows-affected count.
		truncateAllForTest(t, db)
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		dim := 768

		// 3 live memories with embedding_dim, 1 with NULL, 1 soft-deleted with dim.
		var liveDimIDs []uuid.UUID
		for i := 0; i < 3; i++ {
			m := newTestMemory(nsID)
			m.EmbeddingDim = &dim
			if err := repo.Create(ctx, m); err != nil {
				t.Fatalf("create with dim: %v", err)
			}
			liveDimIDs = append(liveDimIDs, m.ID)
		}
		mNull := newTestMemory(nsID)
		if err := repo.Create(ctx, mNull); err != nil {
			t.Fatalf("create null: %v", err)
		}
		mDeleted := newTestMemory(nsID)
		mDeleted.EmbeddingDim = &dim
		if err := repo.Create(ctx, mDeleted); err != nil {
			t.Fatalf("create deleted: %v", err)
		}
		if err := repo.SoftDelete(ctx, mDeleted.ID, nsID); err != nil {
			t.Fatalf("soft-delete: %v", err)
		}

		n, err := repo.ClearAllEmbeddingDims(ctx)
		if err != nil {
			t.Fatalf("clear: %v", err)
		}
		// Exactly the 3 live, non-null rows.
		if n != 3 {
			t.Errorf("expected 3 rows affected (live + non-null), got %d", n)
		}

		// Each live row now has NULL embedding_dim.
		for _, id := range liveDimIDs {
			got, err := repo.GetByID(ctx, id)
			if err != nil {
				t.Fatalf("get %s: %v", id, err)
			}
			if got.EmbeddingDim != nil {
				t.Errorf("live memory %s embedding_dim should be NULL after clear, got %d", id, *got.EmbeddingDim)
			}
		}

		// Idempotent: second call affects 0 rows.
		n2, err := repo.ClearAllEmbeddingDims(ctx)
		if err != nil {
			t.Fatalf("second clear: %v", err)
		}
		if n2 != 0 {
			t.Errorf("second clear should be no-op, affected %d rows", n2)
		}
	})
}

func TestMemoryRepo_SearchByText(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)
		otherNsID := createTestMemoryNamespace(t, ctx, db)

		// Three memories with distinctive lexical content. SQLite uses
		// FTS5 (memories_fts triggers backfill on insert); Postgres uses
		// the content_tsv generated column added in migration 000018.
		mkMem := func(content string, ns uuid.UUID) uuid.UUID {
			m := newTestMemory(ns)
			m.Content = content
			if err := repo.Create(ctx, m); err != nil {
				t.Fatalf("create: %v", err)
			}
			return m.ID
		}
		hitID := mkMem("retatrutide-2.4mg dosing protocol Q4 2025", nsID)
		missID := mkMem("the quick brown fox jumps over the lazy dog", nsID)
		otherNsID2 := mkMem("retatrutide should not surface from a different namespace", otherNsID)

		results, err := repo.SearchByText(ctx, nsID, "retatrutide", 10)
		if err != nil {
			t.Fatalf("SearchByText: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected exactly 1 result for 'retatrutide' in nsID, got %d (ids: %v)", len(results), results)
		}
		if results[0].ID != hitID {
			t.Errorf("expected hit %v, got %v", hitID, results[0].ID)
		}

		// Empty query short-circuits.
		empty, err := repo.SearchByText(ctx, nsID, "   ", 10)
		if err != nil {
			t.Fatalf("empty query SearchByText: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("empty query: expected 0 results, got %d", len(empty))
		}

		// Soft-deleted memories must drop out of the lexical index.
		if err := repo.SoftDelete(ctx, hitID, nsID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}
		afterDelete, err := repo.SearchByText(ctx, nsID, "retatrutide", 10)
		if err != nil {
			t.Fatalf("post-delete SearchByText: %v", err)
		}
		if len(afterDelete) != 0 {
			t.Errorf("expected 0 results after soft delete, got %d", len(afterDelete))
		}

		// No match in this namespace's content corpus.
		none, err := repo.SearchByText(ctx, nsID, "kangaroo", 10)
		if err != nil {
			t.Fatalf("no-match SearchByText: %v", err)
		}
		if len(none) != 0 {
			t.Errorf("expected 0 results for 'kangaroo', got %d", len(none))
		}

		_ = missID
		_ = otherNsID2
	})
}

// TestMemoryRepo_ListByNamespaceStale exercises the SQL-level stale filter
// that drives dreaming-phase candidate selection. Asserts: missing-stamp
// rows are stale, fresh-stamped rows are skipped, stale-stamped rows
// (stamp < updated_at) are returned, oldest-updated_at first, limit is
// honored, and stamps are scoped per-key (paraphrase vs contradictions
// don't cross-pollute). Runs against both SQLite and Postgres via
// forEachDB.
func TestMemoryRepo_ListByNamespaceStale(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		const paraphraseKey = "paraphrase_checked_at"
		const contradictionKey = "contradictions_checked_at"

		// Fixed reference time so updated_at and stamps are deterministic
		// across backends. RFC3339 second-precision because Postgres
		// timestamptz parses strings written via Format(RFC3339).
		now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

		create := func(label string, updatedAt time.Time, metadata string) *model.Memory {
			t.Helper()
			mem := &model.Memory{
				NamespaceID: nsID,
				Content:     label,
				Confidence:  1.0,
				Importance:  0.5,
				CreatedAt:   updatedAt.Add(-1 * time.Hour),
				UpdatedAt:   updatedAt,
				Metadata:    json.RawMessage(metadata),
			}
			if err := repo.Create(ctx, mem); err != nil {
				t.Fatalf("create %s: %v", label, err)
			}
			return mem
		}

		// older-tail row, no stamp → stale (oldest updated_at)
		oldest := create("oldest_unstamped", now.Add(-72*time.Hour), `{}`)

		// older row with FRESH stamp (stamp >= updated_at) → not stale
		fresh1Stamp := now.Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
		_ = create("fresh_stamped",
			now.Add(-48*time.Hour),
			`{"`+paraphraseKey+`":"`+fresh1Stamp+`"}`)

		// row with STALE stamp (stamp < updated_at) → stale
		staleStamp := now.Add(-30 * time.Hour).UTC().Format(time.RFC3339Nano)
		stale := create("stale_stamped",
			now.Add(-24*time.Hour),
			`{"`+paraphraseKey+`":"`+staleStamp+`"}`)

		// row stamped only under a DIFFERENT key → stale for paraphrase
		// (other-key stamp doesn't satisfy the predicate)
		otherKeyStamp := now.UTC().Format(time.RFC3339Nano)
		otherKey := create("other_key_only",
			now.Add(-12*time.Hour),
			`{"`+contradictionKey+`":"`+otherKeyStamp+`"}`)

		// newest row, no stamp → stale but later in ORDER BY
		newest := create("newest_unstamped", now.Add(-1*time.Hour), `{}`)

		// soft-deleted row, no stamp → must be excluded
		softDel := create("soft_deleted", now.Add(-2*time.Hour), `{}`)
		if err := repo.SoftDelete(ctx, softDel.ID, nsID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}

		// Empty stamp key → error.
		if _, err := repo.ListByNamespaceStale(ctx, nsID, "", 100); err == nil {
			t.Fatal("expected error on empty stamp key")
		}

		// Paraphrase stamp lookup.
		got, err := repo.ListByNamespaceStale(ctx, nsID, paraphraseKey, 100)
		if err != nil {
			t.Fatalf("ListByNamespaceStale paraphrase: %v", err)
		}

		gotIDs := make([]uuid.UUID, len(got))
		for i := range got {
			gotIDs[i] = got[i].ID
		}

		wantSet := map[uuid.UUID]bool{
			oldest.ID:   true,
			stale.ID:    true,
			otherKey.ID: true,
			newest.ID:   true,
		}
		if len(got) != len(wantSet) {
			t.Fatalf("paraphrase stale: expected %d rows, got %d (%v)", len(wantSet), len(got), gotIDs)
		}
		for _, id := range gotIDs {
			if !wantSet[id] {
				t.Errorf("paraphrase stale: unexpected row %s in result", id)
			}
		}

		// Oldest updated_at first.
		if len(gotIDs) >= 2 && gotIDs[0] != oldest.ID {
			t.Errorf("ordering: expected oldest_unstamped first, got %s", gotIDs[0])
		}
		if len(gotIDs) >= 1 && gotIDs[len(gotIDs)-1] != newest.ID {
			t.Errorf("ordering: expected newest_unstamped last, got %s", gotIDs[len(gotIDs)-1])
		}

		// Limit honored.
		limited, err := repo.ListByNamespaceStale(ctx, nsID, paraphraseKey, 2)
		if err != nil {
			t.Fatalf("ListByNamespaceStale limit: %v", err)
		}
		if len(limited) != 2 {
			t.Fatalf("limit=2: expected 2 rows, got %d", len(limited))
		}
		if limited[0].ID != oldest.ID {
			t.Errorf("limit ordering: expected oldest first, got %s", limited[0].ID)
		}
	})
}

// TestMemoryRepo_ListByNamespaceStale_AllStamped covers the convergence
// state where every memory has a fresh stamp — the result must be empty.
// This is the steady-state shape of a fully-drained namespace.
func TestMemoryRepo_ListByNamespaceStale_AllStamped(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		const key = "paraphrase_checked_at"
		now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 3; i++ {
			updated := now.Add(-time.Duration(i+1) * time.Hour)
			stampVal := updated.UTC().Format(time.RFC3339Nano)
			mem := &model.Memory{
				NamespaceID: nsID,
				Content:     "all_stamped",
				Confidence:  1.0,
				Importance:  0.5,
				CreatedAt:   updated.Add(-1 * time.Hour),
				UpdatedAt:   updated,
				Metadata:    json.RawMessage(`{"` + key + `":"` + stampVal + `"}`),
			}
			if err := repo.Create(ctx, mem); err != nil {
				t.Fatalf("create: %v", err)
			}
		}

		got, err := repo.ListByNamespaceStale(ctx, nsID, key, 100)
		if err != nil {
			t.Fatalf("ListByNamespaceStale: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected 0 stale rows when all stamped, got %d", len(got))
		}
	})
}

// TestMemoryRepo_ListByNamespaceStale_EmptyNamespace asserts the trivial
// boundary — listing stale rows in a namespace that has none returns
// an empty (non-nil) slice.
func TestMemoryRepo_ListByNamespaceStale_EmptyNamespace(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		got, err := repo.ListByNamespaceStale(ctx, nsID, "paraphrase_checked_at", 100)
		if err != nil {
			t.Fatalf("ListByNamespaceStale empty namespace: %v", err)
		}
		if got == nil {
			t.Fatal("expected non-nil empty slice")
		}
		if len(got) != 0 {
			t.Fatalf("expected 0 rows in empty namespace, got %d", len(got))
		}
	})
}

func TestMemoryRepo_FindBySupersededBy(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		head := newTestMemory(nsID)
		head.Content = "active head"
		if err := repo.Create(ctx, head); err != nil {
			t.Fatalf("create head: %v", err)
		}

		ancestorA := newTestMemory(nsID)
		ancestorA.Content = "ancestor A"
		if err := repo.Create(ctx, ancestorA); err != nil {
			t.Fatalf("create ancestor A: %v", err)
		}
		ancestorB := newTestMemory(nsID)
		ancestorB.Content = "ancestor B"
		if err := repo.Create(ctx, ancestorB); err != nil {
			t.Fatalf("create ancestor B: %v", err)
		}

		now := time.Now().UTC()
		ancestorA.SupersededBy = &head.ID
		ancestorA.SupersededAt = &now
		if err := repo.Update(ctx, ancestorA); err != nil {
			t.Fatalf("supersede A: %v", err)
		}
		ancestorB.SupersededBy = &head.ID
		ancestorB.SupersededAt = &now
		if err := repo.Update(ctx, ancestorB); err != nil {
			t.Fatalf("supersede B: %v", err)
		}

		ids, err := repo.FindBySupersededBy(ctx, nsID, head.ID)
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if len(ids) != 2 {
			t.Fatalf("expected 2 ancestors, got %d (%v)", len(ids), ids)
		}

		got := map[uuid.UUID]bool{}
		for _, id := range ids {
			got[id] = true
		}
		if !got[ancestorA.ID] || !got[ancestorB.ID] {
			t.Fatalf("expected both ancestors, got %v", ids)
		}

		empty, err := repo.FindBySupersededBy(ctx, nsID, ancestorA.ID)
		if err != nil {
			t.Fatalf("find empty: %v", err)
		}
		if len(empty) != 0 {
			t.Fatalf("expected 0 ancestors of leaf, got %d", len(empty))
		}
	})
}

func TestMemoryRepo_FindBySupersededBy_ExcludesSoftDeleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		head := newTestMemory(nsID)
		head.Content = "head"
		if err := repo.Create(ctx, head); err != nil {
			t.Fatalf("create head: %v", err)
		}
		ancestor := newTestMemory(nsID)
		ancestor.Content = "ancestor"
		if err := repo.Create(ctx, ancestor); err != nil {
			t.Fatalf("create ancestor: %v", err)
		}

		now := time.Now().UTC()
		ancestor.SupersededBy = &head.ID
		ancestor.SupersededAt = &now
		if err := repo.Update(ctx, ancestor); err != nil {
			t.Fatalf("supersede: %v", err)
		}
		if err := repo.SoftDelete(ctx, ancestor.ID, nsID); err != nil {
			t.Fatalf("soft delete ancestor: %v", err)
		}

		ids, err := repo.FindBySupersededBy(ctx, nsID, head.ID)
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("expected 0 (soft-deleted excluded), got %d", len(ids))
		}
	})
}

func TestMemoryRepo_SupersedeReplacing(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		old := newTestMemory(nsID)
		old.Content = "Alice works at Acme"
		old.Enriched = true
		dim := 768
		old.EmbeddingDim = &dim
		if err := repo.Create(ctx, old); err != nil {
			t.Fatalf("create old: %v", err)
		}

		newMem := &model.Memory{
			NamespaceID: nsID,
			Content:     "Alice works at Beta Corp",
			Tags:        old.Tags,
			Confidence:  1.0,
			Importance:  old.Importance,
			Source:      old.Source,
			Metadata:    old.Metadata,
		}
		// lineage.MemoryID is left nil; SupersedeReplacing fills it from
		// the assigned newMem.ID since the caller cannot know that value
		// before the helper runs.
		lineage := &model.MemoryLineage{
			NamespaceID: nsID,
			ParentID:    &old.ID,
			Relation:    model.LineageSupersedes,
		}

		err := repo.SupersedeReplacing(ctx, old.ID, newMem, lineage)
		if err != nil {
			t.Fatalf("SupersedeReplacing: %v", err)
		}

		if lineage.MemoryID != newMem.ID {
			t.Fatalf("expected lineage.MemoryID = newMem.ID (%s), got %s", newMem.ID, lineage.MemoryID)
		}

		if newMem.ID == uuid.Nil {
			t.Fatal("expected newMem.ID to be assigned")
		}
		if lineage.ID == uuid.Nil {
			t.Fatal("expected lineage.ID to be assigned")
		}

		// Old row should now be superseded.
		reloadedOld, err := repo.GetByID(ctx, old.ID)
		if err != nil {
			t.Fatalf("reload old: %v", err)
		}
		if reloadedOld.SupersededBy == nil || *reloadedOld.SupersededBy != newMem.ID {
			t.Fatalf("expected old.superseded_by = newMem.ID, got %v", reloadedOld.SupersededBy)
		}
		if reloadedOld.SupersededAt == nil {
			t.Fatal("expected old.superseded_at set")
		}
		// Old row's enrichment and embedding_dim should be intact: the
		// vector survives; pruning eventually purges row + vector together.
		if !reloadedOld.Enriched {
			t.Fatal("expected old.enriched intact (frozen with original content)")
		}
		if reloadedOld.EmbeddingDim == nil || *reloadedOld.EmbeddingDim != dim {
			t.Fatalf("expected old.embedding_dim intact (%d), got %v", dim, reloadedOld.EmbeddingDim)
		}

		// New row should exist with fresh content and Enriched=false.
		newReloaded, err := repo.GetByID(ctx, newMem.ID)
		if err != nil {
			t.Fatalf("reload new: %v", err)
		}
		if newReloaded.Content != "Alice works at Beta Corp" {
			t.Fatalf("unexpected new content: %q", newReloaded.Content)
		}
		if newReloaded.Enriched {
			t.Fatal("expected new.enriched=false (fresh content needs enrichment)")
		}
		if newReloaded.SupersededBy != nil {
			t.Fatal("expected new.superseded_by nil (active head)")
		}
		if newReloaded.ContentHash == "" || newReloaded.ContentHash != HashContent("Alice works at Beta Corp") {
			t.Fatalf("expected ContentHash recomputed, got %q", newReloaded.ContentHash)
		}
	})
}

func TestMemoryRepo_SupersedeReplacing_ConcurrentReturnsSentinel(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		old := newTestMemory(nsID)
		if err := repo.Create(ctx, old); err != nil {
			t.Fatalf("create: %v", err)
		}
		// Pre-supersede the old row to simulate a concurrent writer winning.
		preempt := newTestMemory(nsID)
		preempt.Content = "preempt"
		if err := repo.Create(ctx, preempt); err != nil {
			t.Fatalf("create preempt: %v", err)
		}
		now := time.Now().UTC()
		old.SupersededBy = &preempt.ID
		old.SupersededAt = &now
		if err := repo.Update(ctx, old); err != nil {
			t.Fatalf("preempt supersede: %v", err)
		}

		newMem := &model.Memory{
			NamespaceID: nsID,
			Content:     "loser content",
			Confidence:  1.0,
		}
		lineage := &model.MemoryLineage{
			NamespaceID: nsID,
			ParentID:    &old.ID,
			Relation:    model.LineageSupersedes,
		}
		err := repo.SupersedeReplacing(ctx, old.ID, newMem, lineage)
		if !errors.Is(err, ErrConcurrentSupersede) {
			t.Fatalf("expected ErrConcurrentSupersede, got %v", err)
		}

		// The whole transaction must roll back: no new memory, no lineage row.
		if _, err := repo.GetByID(ctx, newMem.ID); err == nil {
			t.Fatal("expected new memory not to exist after rollback")
		}
	})
}

func TestMemoryRepo_SupersedeReplacing_MissingOldReturnsSentinel(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		nonExistent := uuid.New()
		newMem := &model.Memory{
			NamespaceID: nsID,
			Content:     "orphan",
			Confidence:  1.0,
		}
		lineage := &model.MemoryLineage{
			NamespaceID: nsID,
			ParentID:    &nonExistent,
			Relation:    model.LineageSupersedes,
		}
		err := repo.SupersedeReplacing(ctx, nonExistent, newMem, lineage)
		if !errors.Is(err, ErrConcurrentSupersede) {
			t.Fatalf("expected ErrConcurrentSupersede for missing old, got %v", err)
		}
	})
}

// seedMemoryWithSupersededBy creates two memories: a sentinel "winner" row
// and a primary row whose superseded_by points at the winner. Returns the
// primary row so callers can assert its other columns survive partial
// updates without the FK constraint biting.
func seedMemoryWithSupersededBy(t *testing.T, ctx context.Context, repo *MemoryRepo, nsID uuid.UUID) (*model.Memory, uuid.UUID) {
	t.Helper()
	winner := newTestMemory(nsID)
	winner.Content = "winner sentinel"
	if err := repo.Create(ctx, winner); err != nil {
		t.Fatalf("create winner: %v", err)
	}
	primary := newTestMemory(nsID)
	primary.Content = "primary under test"
	primary.SupersededBy = &winner.ID
	now := time.Now().UTC()
	primary.SupersededAt = &now
	if err := repo.Create(ctx, primary); err != nil {
		t.Fatalf("create primary: %v", err)
	}
	return primary, winner.ID
}

func TestMemoryRepo_ClearEmbeddingDim(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem, winnerID := seedMemoryWithSupersededBy(t, ctx, repo, nsID)
		dim := 768
		mem.EmbeddingDim = &dim
		if err := repo.Update(ctx, mem); err != nil {
			t.Fatalf("seed dim: %v", err)
		}

		if err := repo.ClearEmbeddingDim(ctx, mem.ID, nsID); err != nil {
			t.Fatalf("ClearEmbeddingDim: %v", err)
		}

		got, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if got.EmbeddingDim != nil {
			t.Errorf("embedding_dim should be NULL, got %v", *got.EmbeddingDim)
		}
		if got.SupersededBy == nil || *got.SupersededBy != winnerID {
			t.Errorf("SupersededBy clobbered: got %v, want %s", got.SupersededBy, winnerID)
		}
	})
}

func TestMemoryRepo_UpdateConfidence(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem, winnerID := seedMemoryWithSupersededBy(t, ctx, repo, nsID)

		if err := repo.UpdateConfidence(ctx, mem.ID, nsID, 0.91); err != nil {
			t.Fatalf("UpdateConfidence: %v", err)
		}

		got, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if got.Confidence != 0.91 {
			t.Errorf("Confidence = %f, want 0.91", got.Confidence)
		}
		if got.SupersededBy == nil || *got.SupersededBy != winnerID {
			t.Errorf("SupersededBy clobbered: got %v, want %s", got.SupersededBy, winnerID)
		}
	})
}

func TestMemoryRepo_Demote(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem, winnerID := seedMemoryWithSupersededBy(t, ctx, repo, nsID)
		dim := 1024
		mem.Confidence = 0.8
		mem.EmbeddingDim = &dim
		if err := repo.Update(ctx, mem); err != nil {
			t.Fatalf("seed state: %v", err)
		}

		newMeta := json.RawMessage(`{"low_novelty":true,"reason":"orphan"}`)
		if err := repo.Demote(ctx, mem.ID, nsID, newMeta); err != nil {
			t.Fatalf("Demote: %v", err)
		}

		got, err := repo.getByIDIncludeDeleted(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if got.Confidence != 0 {
			t.Errorf("Confidence = %f, want 0 after demote", got.Confidence)
		}
		if got.EmbeddingDim != nil {
			t.Errorf("EmbeddingDim should be NULL after demote, got %v", *got.EmbeddingDim)
		}
		if !jsonEqual(string(got.Metadata), `{"low_novelty":true,"reason":"orphan"}`) {
			t.Errorf("Metadata = %s, want low_novelty payload", string(got.Metadata))
		}
		if got.SupersededBy == nil || *got.SupersededBy != winnerID {
			t.Errorf("SupersededBy clobbered: got %v, want %s", got.SupersededBy, winnerID)
		}
	})
}

func TestMemoryRepo_MarkSupersededBy(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		old := newTestMemory(nsID)
		dim := 384
		old.EmbeddingDim = &dim
		if err := repo.Create(ctx, old); err != nil {
			t.Fatalf("create old: %v", err)
		}
		newMem := newTestMemory(nsID)
		newMem.Content = "successor"
		if err := repo.Create(ctx, newMem); err != nil {
			t.Fatalf("create new: %v", err)
		}

		if err := repo.MarkSupersededBy(ctx, old.ID, nsID, newMem.ID); err != nil {
			t.Fatalf("MarkSupersededBy: %v", err)
		}

		got, err := repo.GetByID(ctx, old.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if got.SupersededBy == nil || *got.SupersededBy != newMem.ID {
			t.Errorf("SupersededBy = %v, want %s", got.SupersededBy, newMem.ID)
		}
		if got.SupersededAt == nil {
			t.Error("SupersededAt should be set")
		}
		if got.EmbeddingDim != nil {
			t.Errorf("EmbeddingDim should be cleared, got %v", *got.EmbeddingDim)
		}

		// Calling again on the now-superseded row must return the
		// concurrent-supersede sentinel; the partial UPDATE's WHERE
		// clause requires superseded_by IS NULL.
		err = repo.MarkSupersededBy(ctx, old.ID, nsID, uuid.New())
		if !errors.Is(err, ErrConcurrentSupersede) {
			t.Fatalf("second MarkSupersededBy should return ErrConcurrentSupersede, got %v", err)
		}
	})
}
