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

// createTestProject creates a project under a user namespace for testing.
// Returns the project and the user's namespace ID.
func createTestProject(t *testing.T, ctx context.Context, db DB, slug string) (*model.Project, uuid.UUID) {
	t.Helper()
	user := createTestUser(t, ctx, db)
	nsRepo := NewNamespaceRepo(db)
	projectRepo := NewProjectRepo(db)

	// Create a project namespace under the user's namespace.
	userNS, err := nsRepo.GetByID(ctx, user.NamespaceID)
	if err != nil {
		t.Fatalf("failed to get user namespace: %v", err)
	}

	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     slug,
		Slug:     slug,
		Kind:     "project",
		ParentID: &user.NamespaceID,
		Path:     userNS.Path + "/" + slug,
		Depth:    userNS.Depth + 1,
	}
	if err := nsRepo.Create(ctx, projectNS); err != nil {
		t.Fatalf("failed to create project namespace: %v", err)
	}

	project := &model.Project{
		NamespaceID:      projectNSID,
		OwnerNamespaceID: user.NamespaceID,
		Name:             slug,
		Slug:             slug,
		Description:      "A test project",
		DefaultTags:      []string{"test"},
		Settings:         json.RawMessage(`{}`),
	}
	if err := projectRepo.Create(ctx, project); err != nil {
		t.Fatalf("failed to create test project: %v", err)
	}

	return project, user.NamespaceID
}

func TestProjectRepo_Create(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)

	project, _ := createTestProject(t, ctx, db, "test-proj")

	if project.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
	if project.Name != "test-proj" {
		t.Fatalf("expected name %q, got %q", "test-proj", project.Name)
	}
	if project.Slug != "test-proj" {
		t.Fatalf("expected slug %q, got %q", "test-proj", project.Slug)
	}
	if project.Description != "A test project" {
		t.Fatalf("expected description %q, got %q", "A test project", project.Description)
	}
	if len(project.DefaultTags) != 1 || project.DefaultTags[0] != "test" {
		t.Fatalf("expected default_tags [test], got %v", project.DefaultTags)
	}
	if string(project.Settings) != "{}" {
		t.Fatalf("expected settings '{}', got %q", string(project.Settings))
	}
	if project.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if project.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
}

func TestProjectRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)

	project, _ := createTestProject(t, ctx, db, "auto-id-proj")

	if project.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
}

func TestProjectRepo_Create_WithSettings(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	nsRepo := NewNamespaceRepo(db)
	projectRepo := NewProjectRepo(db)

	userNS, err := nsRepo.GetByID(ctx, user.NamespaceID)
	if err != nil {
		t.Fatalf("failed to get user namespace: %v", err)
	}

	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     "settings-proj",
		Slug:     "settings-proj",
		Kind:     "project",
		ParentID: &user.NamespaceID,
		Path:     userNS.Path + "/settings-proj",
		Depth:    userNS.Depth + 1,
	}
	if err := nsRepo.Create(ctx, projectNS); err != nil {
		t.Fatalf("failed to create project namespace: %v", err)
	}

	project := &model.Project{
		NamespaceID:      projectNSID,
		OwnerNamespaceID: user.NamespaceID,
		Name:             "Settings Project",
		Slug:             "settings-proj",
		Settings:         json.RawMessage(`{"theme":"dark"}`),
	}

	if err := projectRepo.Create(ctx, project); err != nil {
		t.Fatalf("failed to create project: %v", err)
	}
	if string(project.Settings) != `{"theme":"dark"}` {
		t.Fatalf("expected settings '{\"theme\":\"dark\"}', got %q", string(project.Settings))
	}
}

func TestProjectRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)

	project, _ := createTestProject(t, ctx, db, "getbyid-proj")

	fetched, err := repo.GetByID(ctx, project.ID)
	if err != nil {
		t.Fatalf("failed to get by id: %v", err)
	}

	if fetched.ID != project.ID {
		t.Fatalf("expected ID %s, got %s", project.ID, fetched.ID)
	}
	if fetched.Name != "getbyid-proj" {
		t.Fatalf("expected name %q, got %q", "getbyid-proj", fetched.Name)
	}
	if fetched.Slug != "getbyid-proj" {
		t.Fatalf("expected slug %q, got %q", "getbyid-proj", fetched.Slug)
	}
	if fetched.NamespaceID != project.NamespaceID {
		t.Fatalf("expected namespace_id %s, got %s", project.NamespaceID, fetched.NamespaceID)
	}
	if fetched.OwnerNamespaceID != project.OwnerNamespaceID {
		t.Fatalf("expected owner_namespace_id %s, got %s", project.OwnerNamespaceID, fetched.OwnerNamespaceID)
	}
}

func TestProjectRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestProjectRepo_GetBySlug(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)

	project, ownerNSID := createTestProject(t, ctx, db, "slug-proj")

	fetched, err := repo.GetBySlug(ctx, ownerNSID, "slug-proj")
	if err != nil {
		t.Fatalf("failed to get by slug: %v", err)
	}
	if fetched.ID != project.ID {
		t.Fatalf("expected ID %s, got %s", project.ID, fetched.ID)
	}
	if fetched.Slug != "slug-proj" {
		t.Fatalf("expected slug %q, got %q", "slug-proj", fetched.Slug)
	}
}

func TestProjectRepo_GetBySlug_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)

	_, err := repo.GetBySlug(ctx, uuid.New(), "nonexistent-slug")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestProjectRepo_ListByUser(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)
	nsRepo := NewNamespaceRepo(db)

	user := createTestUser(t, ctx, db)
	userNS, err := nsRepo.GetByID(ctx, user.NamespaceID)
	if err != nil {
		t.Fatalf("failed to get user namespace: %v", err)
	}

	// Create two projects under this user.
	for _, slug := range []string{"alpha-proj", "beta-proj"} {
		projectNSID := uuid.New()
		projectNS := &model.Namespace{
			ID:       projectNSID,
			Name:     slug,
			Slug:     slug,
			Kind:     "project",
			ParentID: &user.NamespaceID,
			Path:     userNS.Path + "/" + slug,
			Depth:    userNS.Depth + 1,
		}
		if err := nsRepo.Create(ctx, projectNS); err != nil {
			t.Fatalf("failed to create project namespace: %v", err)
		}

		project := &model.Project{
			NamespaceID:      projectNSID,
			OwnerNamespaceID: user.NamespaceID,
			Name:             slug,
			Slug:             slug,
		}
		if err := repo.Create(ctx, project); err != nil {
			t.Fatalf("failed to create project: %v", err)
		}
	}

	projects, err := repo.ListByUser(ctx, user.NamespaceID)
	if err != nil {
		t.Fatalf("failed to list by user: %v", err)
	}
	if len(projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(projects))
	}
	// Ordered by name.
	if projects[0].Name != "alpha-proj" {
		t.Fatalf("expected first project name %q, got %q", "alpha-proj", projects[0].Name)
	}
	if projects[1].Name != "beta-proj" {
		t.Fatalf("expected second project name %q, got %q", "beta-proj", projects[1].Name)
	}
}

func TestProjectRepo_ListByUser_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)

	projects, err := repo.ListByUser(ctx, uuid.New())
	if err != nil {
		t.Fatalf("failed to list by user: %v", err)
	}
	if len(projects) != 0 {
		t.Fatalf("expected 0 projects, got %d", len(projects))
	}
}

func TestProjectRepo_Update(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)

	project, _ := createTestProject(t, ctx, db, "update-proj")
	originalUpdatedAt := project.UpdatedAt

	project.Name = "Updated Name"
	project.Slug = "updated-slug"
	project.Description = "Updated description"
	project.DefaultTags = []string{"updated", "tags"}
	project.Settings = json.RawMessage(`{"updated":true}`)

	if err := repo.Update(ctx, project); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	if project.Name != "Updated Name" {
		t.Fatalf("expected name %q, got %q", "Updated Name", project.Name)
	}
	if project.Slug != "updated-slug" {
		t.Fatalf("expected slug %q, got %q", "updated-slug", project.Slug)
	}
	if project.Description != "Updated description" {
		t.Fatalf("expected description %q, got %q", "Updated description", project.Description)
	}
	if len(project.DefaultTags) != 2 || project.DefaultTags[0] != "updated" || project.DefaultTags[1] != "tags" {
		t.Fatalf("expected default_tags [updated tags], got %v", project.DefaultTags)
	}
	if string(project.Settings) != `{"updated":true}` {
		t.Fatalf("expected settings '{\"updated\":true}', got %q", string(project.Settings))
	}
	if !project.UpdatedAt.After(originalUpdatedAt) && project.UpdatedAt != originalUpdatedAt {
		t.Fatal("expected updated_at to be updated")
	}

	// Verify via fresh fetch.
	fetched, err := repo.GetByID(ctx, project.ID)
	if err != nil {
		t.Fatalf("failed to get after update: %v", err)
	}
	if fetched.Name != "Updated Name" {
		t.Fatalf("expected fetched name %q, got %q", "Updated Name", fetched.Name)
	}
}

func TestProjectRepo_Delete(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewProjectRepo(db)

	project, _ := createTestProject(t, ctx, db, "delete-proj")

	if err := repo.Delete(ctx, project.ID); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	_, err := repo.GetByID(ctx, project.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
	}
}

func TestProjectRepo_AutoCreateUnderUser(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	projectRepo := NewProjectRepo(db)
	nsRepo := NewNamespaceRepo(db)

	user := createTestUser(t, ctx, db)

	project, err := projectRepo.AutoCreateUnderUser(ctx, nsRepo, user.NamespaceID, "auto-proj")
	if err != nil {
		t.Fatalf("failed to auto create: %v", err)
	}

	if project.ID == uuid.Nil {
		t.Fatal("expected non-nil ID")
	}
	if project.Slug != "auto-proj" {
		t.Fatalf("expected slug %q, got %q", "auto-proj", project.Slug)
	}
	if project.Name != "auto-proj" {
		t.Fatalf("expected name %q, got %q", "auto-proj", project.Name)
	}
	if project.OwnerNamespaceID != user.NamespaceID {
		t.Fatalf("expected owner_namespace_id %s, got %s", user.NamespaceID, project.OwnerNamespaceID)
	}
	if project.NamespaceID == uuid.Nil {
		t.Fatal("expected non-nil namespace_id")
	}

	// Verify the project namespace was created.
	projectNS, err := nsRepo.GetByID(ctx, project.NamespaceID)
	if err != nil {
		t.Fatalf("failed to get project namespace: %v", err)
	}
	if projectNS.Kind != "project" {
		t.Fatalf("expected namespace kind %q, got %q", "project", projectNS.Kind)
	}
	if projectNS.ParentID == nil || *projectNS.ParentID != user.NamespaceID {
		t.Fatalf("expected namespace parent_id %s, got %v", user.NamespaceID, projectNS.ParentID)
	}
}

func TestProjectRepo_AutoCreateUnderUser_Idempotent(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	projectRepo := NewProjectRepo(db)
	nsRepo := NewNamespaceRepo(db)

	user := createTestUser(t, ctx, db)

	project1, err := projectRepo.AutoCreateUnderUser(ctx, nsRepo, user.NamespaceID, "idempotent-proj")
	if err != nil {
		t.Fatalf("failed to auto create first: %v", err)
	}

	project2, err := projectRepo.AutoCreateUnderUser(ctx, nsRepo, user.NamespaceID, "idempotent-proj")
	if err != nil {
		t.Fatalf("failed to auto create second: %v", err)
	}

	if project1.ID != project2.ID {
		t.Fatalf("expected same project ID on second call, got %s vs %s", project1.ID, project2.ID)
	}
}
