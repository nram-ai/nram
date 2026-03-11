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

// createTestMemoryForLineage creates a memory record suitable for FK references.
func createTestMemoryForLineage(t *testing.T, ctx context.Context, db DB) uuid.UUID {
	t.Helper()
	nsID := createTestNamespace(t, ctx, db)
	repo := NewMemoryRepo(db)
	mem := newTestMemory(nsID)
	if err := repo.Create(ctx, mem); err != nil {
		t.Fatalf("failed to create test memory for lineage: %v", err)
	}
	return mem.ID
}

func TestMemoryLineageRepo_Create(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memID := createTestMemoryForLineage(t, ctx, db)
		parentID := createTestMemoryForLineage(t, ctx, db)

		lineage := &model.MemoryLineage{
			MemoryID: memID,
			ParentID: &parentID,
			Relation: "derived_from",
			Context:  json.RawMessage(`{"reason":"update"}`),
		}
		if err := repo.Create(ctx, lineage); err != nil {
			t.Fatalf("failed to create lineage: %v", err)
		}

		if lineage.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if lineage.MemoryID != memID {
			t.Fatalf("expected memory_id %s, got %s", memID, lineage.MemoryID)
		}
		if lineage.ParentID == nil || *lineage.ParentID != parentID {
			t.Fatalf("expected parent_id %s, got %v", parentID, lineage.ParentID)
		}
		if lineage.Relation != "derived_from" {
			t.Fatalf("unexpected relation: %q", lineage.Relation)
		}
		if !jsonEqual(string(lineage.Context), `{"reason":"update"}`) {
			t.Fatalf("unexpected context: %q", string(lineage.Context))
		}
		if lineage.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
	})
}

func TestMemoryLineageRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memID := createTestMemoryForLineage(t, ctx, db)

		lineage := &model.MemoryLineage{
			MemoryID: memID,
			Relation: "root",
		}
		if err := repo.Create(ctx, lineage); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if lineage.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestMemoryLineageRepo_Create_NilDefaults(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memID := createTestMemoryForLineage(t, ctx, db)

		lineage := &model.MemoryLineage{
			MemoryID: memID,
			Relation: "root",
		}
		if err := repo.Create(ctx, lineage); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if string(lineage.Context) != "{}" {
			t.Fatalf("expected context '{}', got %q", string(lineage.Context))
		}
		if lineage.ParentID != nil {
			t.Fatalf("expected nil parent_id, got %v", lineage.ParentID)
		}
	})
}

func TestMemoryLineageRepo_Create_WithExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memID := createTestMemoryForLineage(t, ctx, db)
		explicitID := uuid.New()

		lineage := &model.MemoryLineage{
			ID:       explicitID,
			MemoryID: memID,
			Relation: "root",
		}
		if err := repo.Create(ctx, lineage); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if lineage.ID != explicitID {
			t.Fatalf("expected ID %s, got %s", explicitID, lineage.ID)
		}
	})
}

func TestMemoryLineageRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memID := createTestMemoryForLineage(t, ctx, db)
		parentID := createTestMemoryForLineage(t, ctx, db)

		lineage := &model.MemoryLineage{
			MemoryID: memID,
			ParentID: &parentID,
			Relation: "supersedes",
			Context:  json.RawMessage(`{"v":2}`),
		}
		if err := repo.Create(ctx, lineage); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByID(ctx, lineage.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}

		if fetched.ID != lineage.ID {
			t.Fatalf("expected ID %s, got %s", lineage.ID, fetched.ID)
		}
		if fetched.MemoryID != memID {
			t.Fatalf("expected memory_id %s, got %s", memID, fetched.MemoryID)
		}
		if fetched.ParentID == nil || *fetched.ParentID != parentID {
			t.Fatalf("expected parent_id %s, got %v", parentID, fetched.ParentID)
		}
		if fetched.Relation != "supersedes" {
			t.Fatalf("expected relation 'supersedes', got %q", fetched.Relation)
		}
	})
}

func TestMemoryLineageRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestMemoryLineageRepo_ListByMemory(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memA := createTestMemoryForLineage(t, ctx, db)
		memB := createTestMemoryForLineage(t, ctx, db)
		memC := createTestMemoryForLineage(t, ctx, db)

		// memB derived from memA
		l1 := &model.MemoryLineage{
			MemoryID: memB,
			ParentID: &memA,
			Relation: "derived_from",
		}
		if err := repo.Create(ctx, l1); err != nil {
			t.Fatalf("failed to create l1: %v", err)
		}

		// memC supersedes memA
		l2 := &model.MemoryLineage{
			MemoryID: memC,
			ParentID: &memA,
			Relation: "supersedes",
		}
		if err := repo.Create(ctx, l2); err != nil {
			t.Fatalf("failed to create l2: %v", err)
		}

		// memC contradicts memB (memA not involved)
		l3 := &model.MemoryLineage{
			MemoryID: memC,
			ParentID: &memB,
			Relation: "contradicts",
		}
		if err := repo.Create(ctx, l3); err != nil {
			t.Fatalf("failed to create l3: %v", err)
		}

		// ListByMemory for memA should return l1 and l2 (memA is parent in both)
		results, err := repo.ListByMemory(ctx, memA)
		if err != nil {
			t.Fatalf("failed to list by memory for memA: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results for memA, got %d", len(results))
		}

		// ListByMemory for memB should return l1 (as memory_id) and l3 (as parent_id)
		results, err = repo.ListByMemory(ctx, memB)
		if err != nil {
			t.Fatalf("failed to list by memory for memB: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results for memB, got %d", len(results))
		}

		// ListByMemory for memC should return l2 and l3 (as memory_id in both)
		results, err = repo.ListByMemory(ctx, memC)
		if err != nil {
			t.Fatalf("failed to list by memory for memC: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results for memC, got %d", len(results))
		}
	})
}

func TestMemoryLineageRepo_ListByMemory_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		results, err := repo.ListByMemory(ctx, uuid.New())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestMemoryLineageRepo_FindConflicts(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memA := createTestMemoryForLineage(t, ctx, db)
		memB := createTestMemoryForLineage(t, ctx, db)
		memC := createTestMemoryForLineage(t, ctx, db)

		// memB contradicts memA
		l1 := &model.MemoryLineage{
			MemoryID: memB,
			ParentID: &memA,
			Relation: "contradicts",
		}
		if err := repo.Create(ctx, l1); err != nil {
			t.Fatalf("failed to create l1: %v", err)
		}

		// memC derived from memA (not a conflict)
		l2 := &model.MemoryLineage{
			MemoryID: memC,
			ParentID: &memA,
			Relation: "derived_from",
		}
		if err := repo.Create(ctx, l2); err != nil {
			t.Fatalf("failed to create l2: %v", err)
		}

		// memC contradicts memB
		l3 := &model.MemoryLineage{
			MemoryID: memC,
			ParentID: &memB,
			Relation: "contradicts",
		}
		if err := repo.Create(ctx, l3); err != nil {
			t.Fatalf("failed to create l3: %v", err)
		}

		// FindConflicts for memA should return l1 (memA is parent_id)
		conflicts, err := repo.FindConflicts(ctx, memA)
		if err != nil {
			t.Fatalf("failed to find conflicts for memA: %v", err)
		}
		if len(conflicts) != 1 {
			t.Fatalf("expected 1 conflict for memA, got %d", len(conflicts))
		}
		if conflicts[0].ID != l1.ID {
			t.Fatalf("expected conflict ID %s, got %s", l1.ID, conflicts[0].ID)
		}

		// FindConflicts for memB should return l1 (as memory_id) and l3 (as parent_id)
		conflicts, err = repo.FindConflicts(ctx, memB)
		if err != nil {
			t.Fatalf("failed to find conflicts for memB: %v", err)
		}
		if len(conflicts) != 2 {
			t.Fatalf("expected 2 conflicts for memB, got %d", len(conflicts))
		}

		// FindConflicts for memC should return l3 (as memory_id)
		conflicts, err = repo.FindConflicts(ctx, memC)
		if err != nil {
			t.Fatalf("failed to find conflicts for memC: %v", err)
		}
		if len(conflicts) != 1 {
			t.Fatalf("expected 1 conflict for memC, got %d", len(conflicts))
		}
		if conflicts[0].ID != l3.ID {
			t.Fatalf("expected conflict ID %s, got %s", l3.ID, conflicts[0].ID)
		}
	})
}

func TestMemoryLineageRepo_FindConflicts_None(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memA := createTestMemoryForLineage(t, ctx, db)
		memB := createTestMemoryForLineage(t, ctx, db)

		// Only non-conflict relations
		l1 := &model.MemoryLineage{
			MemoryID: memB,
			ParentID: &memA,
			Relation: "derived_from",
		}
		if err := repo.Create(ctx, l1); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		conflicts, err := repo.FindConflicts(ctx, memA)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(conflicts) != 0 {
			t.Fatalf("expected 0 conflicts, got %d", len(conflicts))
		}
	})
}

func TestMemoryLineageRepo_NilParentID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryLineageRepo(db)

		memID := createTestMemoryForLineage(t, ctx, db)

		lineage := &model.MemoryLineage{
			MemoryID: memID,
			Relation: "root",
		}
		if err := repo.Create(ctx, lineage); err != nil {
			t.Fatalf("failed to create with nil parent: %v", err)
		}

		fetched, err := repo.GetByID(ctx, lineage.ID)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}
		if fetched.ParentID != nil {
			t.Fatalf("expected nil parent_id, got %v", fetched.ParentID)
		}
	})
}
