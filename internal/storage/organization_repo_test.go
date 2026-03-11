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
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	orgID := uuid.New()
	org := &model.Organization{
		ID:          orgID,
		NamespaceID: nsID,
		Name:        "Acme Corp",
		Slug:        "acme-corp",
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
	if org.Name != "Acme Corp" {
		t.Fatalf("expected name %q, got %q", "Acme Corp", org.Name)
	}
	if org.Slug != "acme-corp" {
		t.Fatalf("expected slug %q, got %q", "acme-corp", org.Slug)
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
}

func TestOrganizationRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	org := &model.Organization{
		NamespaceID: nsID,
		Name:        "Auto ID Org",
		Slug:        "auto-id-org",
	}

	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create organization: %v", err)
	}
	if org.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
}

func TestOrganizationRepo_Create_WithSettings(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	org := &model.Organization{
		NamespaceID: nsID,
		Name:        "Settings Org",
		Slug:        "settings-org",
		Settings:    json.RawMessage(`{"theme":"dark"}`),
	}

	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create organization: %v", err)
	}
	if string(org.Settings) != `{"theme":"dark"}` {
		t.Fatalf("expected settings '{\"theme\":\"dark\"}', got %q", string(org.Settings))
	}
}

func TestOrganizationRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	orgID := uuid.New()
	org := &model.Organization{
		ID:          orgID,
		NamespaceID: nsID,
		Name:        "GetByID Org",
		Slug:        "getbyid-org",
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
	if fetched.Name != "GetByID Org" {
		t.Fatalf("expected name %q, got %q", "GetByID Org", fetched.Name)
	}
	if fetched.Slug != "getbyid-org" {
		t.Fatalf("expected slug %q, got %q", "getbyid-org", fetched.Slug)
	}
	if string(fetched.Settings) != `{"key":"value"}` {
		t.Fatalf("expected settings '{\"key\":\"value\"}', got %q", string(fetched.Settings))
	}
	if fetched.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if fetched.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
}

func TestOrganizationRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestOrganizationRepo_GetBySlug(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	orgID := uuid.New()
	org := &model.Organization{
		ID:          orgID,
		NamespaceID: nsID,
		Name:        "Slug Org",
		Slug:        "slug-org",
	}
	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetBySlug(ctx, "slug-org")
	if err != nil {
		t.Fatalf("failed to get by slug: %v", err)
	}
	if fetched.ID != orgID {
		t.Fatalf("expected ID %s, got %s", orgID, fetched.ID)
	}
	if fetched.Slug != "slug-org" {
		t.Fatalf("expected slug %q, got %q", "slug-org", fetched.Slug)
	}
}

func TestOrganizationRepo_GetBySlug_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	_, err := repo.GetBySlug(ctx, "nonexistent-slug")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestOrganizationRepo_GetByNamespaceID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	orgID := uuid.New()
	org := &model.Organization{
		ID:          orgID,
		NamespaceID: nsID,
		Name:        "NS Org",
		Slug:        "ns-org",
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
}

func TestOrganizationRepo_List(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	// Create two orgs with names that sort alphabetically.
	nsID1 := createTestNamespace(t, ctx, db)
	org1 := &model.Organization{
		NamespaceID: nsID1,
		Name:        "Alpha Org",
		Slug:        "alpha-org",
	}
	if err := repo.Create(ctx, org1); err != nil {
		t.Fatalf("failed to create org1: %v", err)
	}

	nsID2 := createTestNamespace(t, ctx, db)
	org2 := &model.Organization{
		NamespaceID: nsID2,
		Name:        "Beta Org",
		Slug:        "beta-org",
	}
	if err := repo.Create(ctx, org2); err != nil {
		t.Fatalf("failed to create org2: %v", err)
	}

	orgs, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}

	if len(orgs) != 2 {
		t.Fatalf("expected 2 organizations, got %d", len(orgs))
	}

	// Results ordered by name: Alpha < Beta.
	if orgs[0].Name != "Alpha Org" {
		t.Fatalf("expected first org name %q, got %q", "Alpha Org", orgs[0].Name)
	}
	if orgs[1].Name != "Beta Org" {
		t.Fatalf("expected second org name %q, got %q", "Beta Org", orgs[1].Name)
	}
}

func TestOrganizationRepo_Update(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	org := &model.Organization{
		NamespaceID: nsID,
		Name:        "Original Name",
		Slug:        "original-slug",
	}
	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	originalUpdatedAt := org.UpdatedAt

	// Update fields.
	org.Name = "Updated Name"
	org.Slug = "updated-slug"
	org.Settings = json.RawMessage(`{"updated":true}`)

	if err := repo.Update(ctx, org); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	if org.Name != "Updated Name" {
		t.Fatalf("expected name %q, got %q", "Updated Name", org.Name)
	}
	if org.Slug != "updated-slug" {
		t.Fatalf("expected slug %q, got %q", "updated-slug", org.Slug)
	}
	if string(org.Settings) != `{"updated":true}` {
		t.Fatalf("expected settings '{\"updated\":true}', got %q", string(org.Settings))
	}
	if !org.UpdatedAt.After(originalUpdatedAt) && org.UpdatedAt != originalUpdatedAt {
		t.Fatal("expected updated_at to be updated")
	}

	// Verify via fresh fetch.
	fetched, err := repo.GetByID(ctx, org.ID)
	if err != nil {
		t.Fatalf("failed to get after update: %v", err)
	}
	if fetched.Name != "Updated Name" {
		t.Fatalf("expected fetched name %q, got %q", "Updated Name", fetched.Name)
	}
	if fetched.Slug != "updated-slug" {
		t.Fatalf("expected fetched slug %q, got %q", "updated-slug", fetched.Slug)
	}
}

func TestOrganizationRepo_Delete(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewOrganizationRepo(db)

	nsID := createTestNamespace(t, ctx, db)

	org := &model.Organization{
		NamespaceID: nsID,
		Name:        "Delete Me",
		Slug:        "delete-me",
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
}
