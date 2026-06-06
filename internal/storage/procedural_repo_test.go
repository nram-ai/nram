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

func TestProceduralRepo_CreateAndGet(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		user := createTestUser(t, ctx, db)
		repo := NewProceduralRepo(db)

		e := &model.ProceduralEntry{
			NamespaceID: user.NamespaceID,
			Content:     "Never ship em dashes — verbatim hard stop.",
			Title:       "Em-dash rule",
			Category:    "failure-mode",
			Tags:        []string{"em-dash", "non-negotiable"},
			Priority:    10,
			Enabled:     true,
		}
		if err := repo.Create(ctx, e); err != nil {
			t.Fatalf("create: %v", err)
		}
		if e.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if e.Origin != string(model.OriginUser) {
			t.Fatalf("expected origin %q, got %q", model.OriginUser, e.Origin)
		}

		got, err := repo.GetByID(ctx, e.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Content != e.Content {
			t.Fatalf("content mismatch: %q != %q", got.Content, e.Content)
		}
		if got.Title != "Em-dash rule" || got.Category != "failure-mode" {
			t.Fatalf("title/category mismatch: %q / %q", got.Title, got.Category)
		}
		if len(got.Tags) != 2 || got.Tags[0] != "em-dash" {
			t.Fatalf("tags mismatch: %v", got.Tags)
		}
		if got.Priority != 10 {
			t.Fatalf("priority mismatch: %d", got.Priority)
		}
		if !got.Enabled {
			t.Fatal("expected enabled=true")
		}
	})
}

func TestProceduralRepo_ListOrderedByPriorityThenRecency(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		user := createTestUser(t, ctx, db)
		repo := NewProceduralRepo(db)

		// Lower priority, created first.
		low := &model.ProceduralEntry{NamespaceID: user.NamespaceID, Content: "low", Priority: 1, Enabled: true}
		if err := repo.Create(ctx, low); err != nil {
			t.Fatalf("create low: %v", err)
		}
		// Higher priority, created second; must sort first.
		high := &model.ProceduralEntry{NamespaceID: user.NamespaceID, Content: "high", Priority: 5, Enabled: true}
		if err := repo.Create(ctx, high); err != nil {
			t.Fatalf("create high: %v", err)
		}

		list, err := repo.ListByNamespace(ctx, user.NamespaceID)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(list))
		}
		if list[0].Content != "high" || list[1].Content != "low" {
			t.Fatalf("expected high before low, got %q then %q", list[0].Content, list[1].Content)
		}
	})
}

func TestProceduralRepo_UpdateAndDelete(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		user := createTestUser(t, ctx, db)
		repo := NewProceduralRepo(db)

		e := &model.ProceduralEntry{NamespaceID: user.NamespaceID, Content: "v1", Enabled: true}
		if err := repo.Create(ctx, e); err != nil {
			t.Fatalf("create: %v", err)
		}

		e.Content = "v2"
		e.Enabled = false
		e.Priority = 3
		e.Metadata = json.RawMessage(`{"k":"v"}`)
		if err := repo.Update(ctx, e); err != nil {
			t.Fatalf("update: %v", err)
		}
		got, err := repo.GetByID(ctx, e.ID)
		if err != nil {
			t.Fatalf("get after update: %v", err)
		}
		if got.Content != "v2" || got.Enabled || got.Priority != 3 {
			t.Fatalf("update not applied: %+v", got)
		}

		// Delete in the wrong namespace must not affect the row.
		if err := repo.Delete(ctx, e.ID, uuid.New()); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected ErrNoRows deleting in foreign namespace, got %v", err)
		}
		if err := repo.Delete(ctx, e.ID, user.NamespaceID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, err := repo.GetByID(ctx, e.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected ErrNoRows after delete, got %v", err)
		}

		list, err := repo.ListByNamespace(ctx, user.NamespaceID)
		if err != nil {
			t.Fatalf("list after delete: %v", err)
		}
		if len(list) != 0 {
			t.Fatalf("expected empty list after delete, got %d", len(list))
		}
	})
}
