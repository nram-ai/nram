package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func newTestWebhook(nsID uuid.UUID) *model.Webhook {
	scope := "ns:" + nsID.String()
	secret := "whsec_test123"
	return &model.Webhook{
		URL:    "https://example.com/hook",
		Secret: &secret,
		Events: []string{"memory.created", "memory.updated"},
		Scope:  scope,
		Active: true,
	}
}

func TestWebhookRepo_Create(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create webhook: %v", err)
		}

		if webhook.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if webhook.URL != "https://example.com/hook" {
			t.Fatalf("unexpected URL: %q", webhook.URL)
		}
		if webhook.Secret == nil || *webhook.Secret != "whsec_test123" {
			t.Fatalf("unexpected secret: %v", webhook.Secret)
		}
		if len(webhook.Events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(webhook.Events))
		}
		if webhook.Events[0] != "memory.created" || webhook.Events[1] != "memory.updated" {
			t.Fatalf("unexpected events: %v", webhook.Events)
		}
		if !webhook.Active {
			t.Fatal("expected webhook to be active")
		}
		if webhook.FailureCount != 0 {
			t.Fatalf("expected failure_count 0, got %d", webhook.FailureCount)
		}
		if webhook.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
		if webhook.UpdatedAt.IsZero() {
			t.Fatal("expected non-zero updated_at")
		}
	})
}

func TestWebhookRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if webhook.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestWebhookRepo_Create_WithExplicitID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		explicitID := uuid.New()
		webhook := newTestWebhook(nsID)
		webhook.ID = explicitID
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if webhook.ID != explicitID {
			t.Fatalf("expected ID %s, got %s", explicitID, webhook.ID)
		}
	})
}

func TestWebhookRepo_Create_NilEvents(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := &model.Webhook{
			URL:    "https://example.com/hook",
			Scope:  "ns:" + nsID.String(),
			Active: true,
		}
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}
		if webhook.Events == nil {
			t.Fatal("expected non-nil events after create")
		}
		if len(webhook.Events) != 0 {
			t.Fatalf("expected 0 events, got %d", len(webhook.Events))
		}
	})
}

func TestWebhookRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByID(ctx, webhook.ID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}

		if fetched.ID != webhook.ID {
			t.Fatalf("expected ID %s, got %s", webhook.ID, fetched.ID)
		}
		if fetched.URL != webhook.URL {
			t.Fatalf("expected URL %q, got %q", webhook.URL, fetched.URL)
		}
		if fetched.Scope != webhook.Scope {
			t.Fatalf("expected scope %q, got %q", webhook.Scope, fetched.Scope)
		}
	})
}

func TestWebhookRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestWebhookRepo_Update(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		webhook.URL = "https://updated.example.com/hook"
		webhook.Events = []string{"memory.deleted"}
		newSecret := "whsec_updated"
		webhook.Secret = &newSecret

		if err := repo.Update(ctx, webhook); err != nil {
			t.Fatalf("failed to update: %v", err)
		}

		fetched, err := repo.GetByID(ctx, webhook.ID)
		if err != nil {
			t.Fatalf("failed to get after update: %v", err)
		}

		if fetched.URL != "https://updated.example.com/hook" {
			t.Fatalf("expected updated URL, got %q", fetched.URL)
		}
		if len(fetched.Events) != 1 || fetched.Events[0] != "memory.deleted" {
			t.Fatalf("expected updated events, got %v", fetched.Events)
		}
		if fetched.Secret == nil || *fetched.Secret != "whsec_updated" {
			t.Fatalf("expected updated secret, got %v", fetched.Secret)
		}
	})
}

func TestWebhookRepo_Delete(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.Delete(ctx, webhook.ID); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}

		_, err := repo.GetByID(ctx, webhook.ID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
		}
	})
}

func TestWebhookRepo_ListActiveForEvent(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Create active webhook subscribed to memory.created
		w1 := newTestWebhook(nsID)
		w1.Events = []string{"memory.created", "memory.updated"}
		if err := repo.Create(ctx, w1); err != nil {
			t.Fatalf("failed to create w1: %v", err)
		}

		// Create active webhook subscribed only to memory.deleted
		w2 := newTestWebhook(nsID)
		w2.Events = []string{"memory.deleted"}
		if err := repo.Create(ctx, w2); err != nil {
			t.Fatalf("failed to create w2: %v", err)
		}

		// Create inactive webhook subscribed to memory.created
		w3 := newTestWebhook(nsID)
		w3.Events = []string{"memory.created"}
		w3.Active = false
		if err := repo.Create(ctx, w3); err != nil {
			t.Fatalf("failed to create w3: %v", err)
		}

		// Query for memory.created - should return only w1
		results, err := repo.ListActiveForEvent(ctx, nsID, "memory.created")
		if err != nil {
			t.Fatalf("failed to list active for event: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for memory.created, got %d", len(results))
		}
		if results[0].ID != w1.ID {
			t.Fatalf("expected webhook %s, got %s", w1.ID, results[0].ID)
		}

		// Query for memory.deleted - should return only w2
		results, err = repo.ListActiveForEvent(ctx, nsID, "memory.deleted")
		if err != nil {
			t.Fatalf("failed to list active for memory.deleted: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for memory.deleted, got %d", len(results))
		}
		if results[0].ID != w2.ID {
			t.Fatalf("expected webhook %s, got %s", w2.ID, results[0].ID)
		}

		// Query for non-existent event
		results, err = repo.ListActiveForEvent(ctx, nsID, "memory.nonexistent")
		if err != nil {
			t.Fatalf("failed to list for nonexistent event: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for nonexistent event, got %d", len(results))
		}
	})
}

func TestWebhookRepo_ListActiveForEvent_NamespaceIsolation(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID1 := createTestNamespace(t, ctx, db)
		nsID2 := createTestNamespace(t, ctx, db)

		// Create webhook in ns1
		w1 := newTestWebhook(nsID1)
		if err := repo.Create(ctx, w1); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Query ns2 should return empty
		results, err := repo.ListActiveForEvent(ctx, nsID2, "memory.created")
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results for different namespace, got %d", len(results))
		}
	})
}

func TestWebhookRepo_RecordFailure(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Record 3 failures
		for i := 0; i < 3; i++ {
			if err := repo.RecordFailure(ctx, webhook.ID); err != nil {
				t.Fatalf("failed to record failure %d: %v", i+1, err)
			}
		}

		fetched, err := repo.GetByID(ctx, webhook.ID)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}
		if fetched.FailureCount != 3 {
			t.Fatalf("expected failure_count 3, got %d", fetched.FailureCount)
		}
		if !fetched.Active {
			t.Fatal("expected webhook to still be active after 3 failures")
		}
	})
}

func TestWebhookRepo_RecordFailure_AutoDisable(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Record 10 failures to trigger auto-disable
		for i := 0; i < 10; i++ {
			if err := repo.RecordFailure(ctx, webhook.ID); err != nil {
				t.Fatalf("failed to record failure %d: %v", i+1, err)
			}
		}

		fetched, err := repo.GetByID(ctx, webhook.ID)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}
		if fetched.FailureCount != 10 {
			t.Fatalf("expected failure_count 10, got %d", fetched.FailureCount)
		}
		if fetched.Active {
			t.Fatal("expected webhook to be auto-disabled after 10 failures")
		}
	})
}

func TestWebhookRepo_RecordSuccess(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		webhook := newTestWebhook(nsID)
		if err := repo.Create(ctx, webhook); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Record some failures first
		for i := 0; i < 5; i++ {
			if err := repo.RecordFailure(ctx, webhook.ID); err != nil {
				t.Fatalf("failed to record failure: %v", err)
			}
		}

		// Record success should reset failure count
		if err := repo.RecordSuccess(ctx, webhook.ID); err != nil {
			t.Fatalf("failed to record success: %v", err)
		}

		fetched, err := repo.GetByID(ctx, webhook.ID)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}
		if fetched.FailureCount != 0 {
			t.Fatalf("expected failure_count 0 after success, got %d", fetched.FailureCount)
		}
		if fetched.LastFired == nil {
			t.Fatal("expected last_fired to be set after success")
		}
	})
}

func TestWebhookRepo_ListByNamespace(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// Create 3 webhooks
		for i := 0; i < 3; i++ {
			w := newTestWebhook(nsID)
			if err := repo.Create(ctx, w); err != nil {
				t.Fatalf("failed to create webhook %d: %v", i, err)
			}
		}

		results, err := repo.ListByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}
	})
}

func TestWebhookRepo_ListByNamespace_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)

		results, err := repo.ListByNamespace(ctx, uuid.New())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestWebhookRepo_ListByNamespace_Isolation(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewWebhookRepo(db)
		nsID1 := createTestNamespace(t, ctx, db)
		nsID2 := createTestNamespace(t, ctx, db)

		// Create webhook in ns1
		w1 := newTestWebhook(nsID1)
		if err := repo.Create(ctx, w1); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// Create webhook in ns2
		w2 := newTestWebhook(nsID2)
		if err := repo.Create(ctx, w2); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		// List ns1 should only see w1
		results, err := repo.ListByNamespace(ctx, nsID1)
		if err != nil {
			t.Fatalf("failed to list ns1: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result for ns1, got %d", len(results))
		}
		if results[0].ID != w1.ID {
			t.Fatalf("expected webhook ID %s, got %s", w1.ID, results[0].ID)
		}
	})
}
