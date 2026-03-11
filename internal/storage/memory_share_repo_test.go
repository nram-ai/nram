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

func newTestMemoryShare(sourceNsID, targetNsID uuid.UUID) *model.MemoryShare {
	return &model.MemoryShare{
		SourceNsID: sourceNsID,
		TargetNsID: targetNsID,
		Permission: "recall",
	}
}

func TestMemoryShareRepo_Create(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		share := newTestMemoryShare(sourceNs, targetNs)
		if err := repo.Create(ctx, share); err != nil {
			t.Fatalf("failed to create memory share: %v", err)
		}

		if share.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if share.SourceNsID != sourceNs {
			t.Fatalf("expected source_ns_id %s, got %s", sourceNs, share.SourceNsID)
		}
		if share.TargetNsID != targetNs {
			t.Fatalf("expected target_ns_id %s, got %s", targetNs, share.TargetNsID)
		}
		if share.Permission != "recall" {
			t.Fatalf("expected permission 'recall', got %q", share.Permission)
		}
		if share.CreatedBy != nil {
			t.Fatalf("expected nil created_by, got %v", share.CreatedBy)
		}
		if share.ExpiresAt != nil {
			t.Fatalf("expected nil expires_at, got %v", share.ExpiresAt)
		}
		if share.RevokedAt != nil {
			t.Fatalf("expected nil revoked_at, got %v", share.RevokedAt)
		}
		if share.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
	})
}

func TestMemoryShareRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		share := newTestMemoryShare(sourceNs, targetNs)
		if err := repo.Create(ctx, share); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if share.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestMemoryShareRepo_Create_WithExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		explicitID := uuid.New()
		share := newTestMemoryShare(sourceNs, targetNs)
		share.ID = explicitID
		if err := repo.Create(ctx, share); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if share.ID != explicitID {
			t.Fatalf("expected ID %s, got %s", explicitID, share.ID)
		}
	})
}

func TestMemoryShareRepo_Create_WithOptionalFields(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		user := createTestUser(t, ctx, db)
		sourceNs := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		expires := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
		share := &model.MemoryShare{
			SourceNsID: sourceNs,
			TargetNsID: targetNs,
			Permission: "recall+enrich",
			CreatedBy:  &user.ID,
			ExpiresAt:  &expires,
		}
		if err := repo.Create(ctx, share); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if share.CreatedBy == nil || *share.CreatedBy != user.ID {
			t.Fatalf("expected created_by %s, got %v", user.ID, share.CreatedBy)
		}
		if share.ExpiresAt == nil {
			t.Fatal("expected non-nil expires_at")
		}
		if share.Permission != "recall+enrich" {
			t.Fatalf("expected permission 'recall+enrich', got %q", share.Permission)
		}
	})
}

func TestMemoryShareRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		share := newTestMemoryShare(sourceNs, targetNs)
		if err := repo.Create(ctx, share); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByID(ctx, share.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}

		if fetched.ID != share.ID {
			t.Fatalf("expected ID %s, got %s", share.ID, fetched.ID)
		}
		if fetched.SourceNsID != sourceNs {
			t.Fatalf("expected source_ns_id %s, got %s", sourceNs, fetched.SourceNsID)
		}
		if fetched.TargetNsID != targetNs {
			t.Fatalf("expected target_ns_id %s, got %s", targetNs, fetched.TargetNsID)
		}
	})
}

func TestMemoryShareRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestMemoryShareRepo_Revoke(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		share := newTestMemoryShare(sourceNs, targetNs)
		if err := repo.Create(ctx, share); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.Revoke(ctx, share.ID); err != nil {
			t.Fatalf("failed to revoke: %v", err)
		}

		fetched, err := repo.GetByID(ctx, share.ID)
		if err != nil {
			t.Fatalf("failed to get after revoke: %v", err)
		}

		if fetched.RevokedAt == nil {
			t.Fatal("expected revoked_at to be set after revoke")
		}
	})
}

func TestMemoryShareRepo_Revoke_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)

		err := repo.Revoke(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestMemoryShareRepo_ListSharedToNamespace(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs1 := createTestNamespace(t, ctx, db)
		sourceNs2 := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		// Create two shares targeting the same namespace from different sources.
		s1 := newTestMemoryShare(sourceNs1, targetNs)
		if err := repo.Create(ctx, s1); err != nil {
			t.Fatalf("failed to create s1: %v", err)
		}
		s2 := newTestMemoryShare(sourceNs2, targetNs)
		if err := repo.Create(ctx, s2); err != nil {
			t.Fatalf("failed to create s2: %v", err)
		}

		results, err := repo.ListSharedToNamespace(ctx, targetNs)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
	})
}

func TestMemoryShareRepo_ListSharedToNamespace_ExcludesRevoked(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs1 := createTestNamespace(t, ctx, db)
		sourceNs2 := createTestNamespace(t, ctx, db)
		targetNs := createTestNamespace(t, ctx, db)

		// Create two shares, revoke one.
		s1 := newTestMemoryShare(sourceNs1, targetNs)
		if err := repo.Create(ctx, s1); err != nil {
			t.Fatalf("failed to create s1: %v", err)
		}
		s2 := newTestMemoryShare(sourceNs2, targetNs)
		if err := repo.Create(ctx, s2); err != nil {
			t.Fatalf("failed to create s2: %v", err)
		}

		if err := repo.Revoke(ctx, s1.ID); err != nil {
			t.Fatalf("failed to revoke s1: %v", err)
		}

		results, err := repo.ListSharedToNamespace(ctx, targetNs)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result (revoked excluded), got %d", len(results))
		}
		if results[0].ID != s2.ID {
			t.Fatalf("expected share %s, got %s", s2.ID, results[0].ID)
		}
	})
}

func TestMemoryShareRepo_ListSharedToNamespace_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)

		results, err := repo.ListSharedToNamespace(ctx, uuid.New())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestMemoryShareRepo_ListSharedToNamespace_Isolation(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryShareRepo(db)
		sourceNs := createTestNamespace(t, ctx, db)
		targetNs1 := createTestNamespace(t, ctx, db)
		targetNs2 := createTestNamespace(t, ctx, db)

		// Create share to targetNs1.
		s1 := newTestMemoryShare(sourceNs, targetNs1)
		if err := repo.Create(ctx, s1); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// List for targetNs2 should be empty.
		results, err := repo.ListSharedToNamespace(ctx, targetNs2)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for different namespace, got %d", len(results))
		}
	})
}
