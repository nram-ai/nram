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

// createTestNamespace is a helper that creates an org-kind namespace for FK satisfaction.
func createTestNamespace(t *testing.T, ctx context.Context, db DB) uuid.UUID {
	t.Helper()
	nsRepo := NewNamespaceRepo(db)
	nsID := uuid.New()
	ns := &model.Namespace{
		ID:       nsID,
		Name:     "NS " + nsID.String()[:8],
		Slug:     "ns-" + nsID.String()[:8],
		Kind:     "org",
		ParentID: &rootID,
		Path:     nsID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("failed to create test namespace: %v", err)
	}
	return nsID
}

func TestOrganizationRepo_Create(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		orgID := uuid.New()
		org := &model.Organization{
			ID:          orgID,
			NamespaceID: nsID,
			Name:        "Acme Corp " + suffix,
			Slug:        "acme-corp-" + suffix,
		}

		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create organization: %v", err)
		}

		if org.ID != orgID {
			t.Fatalf("expected ID %s, got %s", orgID, org.ID)
		}
		if org.NamespaceID != nsID {
			t.Fatalf("expected namespace_id %s, got %s", nsID, org.NamespaceID)
		}
		if org.Name != "Acme Corp "+suffix {
			t.Fatalf("expected name %q, got %q", "Acme Corp "+suffix, org.Name)
		}
		if org.Slug != "acme-corp-"+suffix {
			t.Fatalf("expected slug %q, got %q", "acme-corp-"+suffix, org.Slug)
		}
		if org.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
		if org.UpdatedAt.IsZero() {
			t.Fatal("expected non-zero updated_at")
		}
		if string(org.Settings) != "{}" {
			t.Fatalf("expected settings '{}', got %q", string(org.Settings))
		}
	})
}

func TestOrganizationRepo_Create_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		org := &model.Organization{
			NamespaceID: nsID,
			Name:        "Auto ID Org " + suffix,
			Slug:        "auto-id-org-" + suffix,
		}

		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create organization: %v", err)
		}
		if org.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
	})
}

func TestOrganizationRepo_Create_WithSettings(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		org := &model.Organization{
			NamespaceID: nsID,
			Name:        "Settings Org " + suffix,
			Slug:        "settings-org-" + suffix,
			Settings:    json.RawMessage(`{"theme":"dark"}`),
		}

		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create organization: %v", err)
		}
		if !jsonEqual(string(org.Settings), `{"theme":"dark"}`) {
			t.Fatalf("expected settings '{\"theme\":\"dark\"}', got %q", string(org.Settings))
		}
	})
}

func TestOrganizationRepo_GetByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		orgID := uuid.New()
		nameVal := "GetByID Org " + suffix
		slugVal := "getbyid-org-" + suffix
		org := &model.Organization{
			ID:          orgID,
			NamespaceID: nsID,
			Name:        nameVal,
			Slug:        slugVal,
			Settings:    json.RawMessage(`{"key":"value"}`),
		}
		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByID(ctx, orgID)
		if err != nil {
			t.Fatalf("failed to get by id: %v", err)
		}

		if fetched.ID != orgID {
			t.Fatalf("expected ID %s, got %s", orgID, fetched.ID)
		}
		if fetched.NamespaceID != nsID {
			t.Fatalf("expected namespace_id %s, got %s", nsID, fetched.NamespaceID)
		}
		if fetched.Name != nameVal {
			t.Fatalf("expected name %q, got %q", nameVal, fetched.Name)
		}
		if fetched.Slug != slugVal {
			t.Fatalf("expected slug %q, got %q", slugVal, fetched.Slug)
		}
		if !jsonEqual(string(fetched.Settings), `{"key":"value"}`) {
			t.Fatalf("expected settings '{\"key\":\"value\"}', got %q", string(fetched.Settings))
		}
		if fetched.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
		if fetched.UpdatedAt.IsZero() {
			t.Fatal("expected non-zero updated_at")
		}
	})
}

func TestOrganizationRepo_GetByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		_, err := repo.GetByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOrganizationRepo_GetBySlug(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		orgID := uuid.New()
		slugVal := "slug-org-" + suffix
		org := &model.Organization{
			ID:          orgID,
			NamespaceID: nsID,
			Name:        "Slug Org " + suffix,
			Slug:        slugVal,
		}
		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetBySlug(ctx, slugVal)
		if err != nil {
			t.Fatalf("failed to get by slug: %v", err)
		}
		if fetched.ID != orgID {
			t.Fatalf("expected ID %s, got %s", orgID, fetched.ID)
		}
		if fetched.Slug != slugVal {
			t.Fatalf("expected slug %q, got %q", slugVal, fetched.Slug)
		}
	})
}

func TestOrganizationRepo_GetBySlug_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		_, err := repo.GetBySlug(ctx, "nonexistent-slug-"+uuid.New().String()[:8])
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOrganizationRepo_GetByNamespaceID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		orgID := uuid.New()
		org := &model.Organization{
			ID:          orgID,
			NamespaceID: nsID,
			Name:        "NS Org " + suffix,
			Slug:        "ns-org-" + suffix,
		}
		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		fetched, err := repo.GetByNamespaceID(ctx, nsID)
		if err != nil {
			t.Fatalf("failed to get by namespace_id: %v", err)
		}
		if fetched.ID != orgID {
			t.Fatalf("expected ID %s, got %s", orgID, fetched.ID)
		}
		if fetched.NamespaceID != nsID {
			t.Fatalf("expected namespace_id %s, got %s", nsID, fetched.NamespaceID)
		}
	})
}

func TestOrganizationRepo_List(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		suffix := uuid.New().String()[:8]

		// Create two orgs with names that sort alphabetically.
		nsID1 := createTestNamespace(t, ctx, db)
		org1 := &model.Organization{
			NamespaceID: nsID1,
			Name:        "Alpha Org " + suffix,
			Slug:        "alpha-org-" + suffix,
		}
		if err := repo.Create(ctx, org1); err != nil {
			t.Fatalf("failed to create org1: %v", err)
		}

		nsID2 := createTestNamespace(t, ctx, db)
		org2 := &model.Organization{
			NamespaceID: nsID2,
			Name:        "Beta Org " + suffix,
			Slug:        "beta-org-" + suffix,
		}
		if err := repo.Create(ctx, org2); err != nil {
			t.Fatalf("failed to create org2: %v", err)
		}

		orgs, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		if len(orgs) < 2 {
			t.Fatalf("expected at least 2 organizations, got %d", len(orgs))
		}

		// Verify both created orgs appear in the results (ordered by name).
		foundAlpha, foundBeta := false, false
		for _, o := range orgs {
			if o.ID == org1.ID {
				foundAlpha = true
			}
			if o.ID == org2.ID {
				foundBeta = true
			}
		}
		if !foundAlpha {
			t.Fatal("expected Alpha Org in list results")
		}
		if !foundBeta {
			t.Fatal("expected Beta Org in list results")
		}
	})
}

func TestOrganizationRepo_Update(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		org := &model.Organization{
			NamespaceID: nsID,
			Name:        "Original Name " + suffix,
			Slug:        "original-slug-" + suffix,
		}
		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		originalUpdatedAt := org.UpdatedAt
		time.Sleep(time.Second)

		// Update fields.
		updatedSuffix := uuid.New().String()[:8]
		updatedName := "Updated Name " + updatedSuffix
		updatedSlug := "updated-slug-" + updatedSuffix
		org.Name = updatedName
		org.Slug = updatedSlug
		org.Settings = json.RawMessage(`{"updated":true}`)

		if err := repo.Update(ctx, org); err != nil {
			t.Fatalf("failed to update: %v", err)
		}

		if org.Name != updatedName {
			t.Fatalf("expected name %q, got %q", updatedName, org.Name)
		}
		if org.Slug != updatedSlug {
			t.Fatalf("expected slug %q, got %q", updatedSlug, org.Slug)
		}
		if !jsonEqual(string(org.Settings), `{"updated":true}`) {
			t.Fatalf("expected settings '{\"updated\":true}', got %q", string(org.Settings))
		}
		if org.UpdatedAt.Before(originalUpdatedAt) {
			t.Fatal("expected updated_at to be updated")
		}

		// Verify via fresh fetch.
		fetched, err := repo.GetByID(ctx, org.ID)
		if err != nil {
			t.Fatalf("failed to get after update: %v", err)
		}
		if fetched.Name != updatedName {
			t.Fatalf("expected fetched name %q, got %q", updatedName, fetched.Name)
		}
		if fetched.Slug != updatedSlug {
			t.Fatalf("expected fetched slug %q, got %q", updatedSlug, fetched.Slug)
		}
	})
}

func TestOrganizationRepo_Delete(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOrganizationRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		suffix := uuid.New().String()[:8]
		org := &model.Organization{
			NamespaceID: nsID,
			Name:        "Delete Me " + suffix,
			Slug:        "delete-me-" + suffix,
		}
		if err := repo.Create(ctx, org); err != nil {
			t.Fatalf("failed to create: %v", err)
		}

		if err := repo.Delete(ctx, org.ID); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}

		_, err := repo.GetByID(ctx, org.ID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
		}
	})
}
