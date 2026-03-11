package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
)

// testDBWithMigrations opens a SQLite DB and runs all migrations.
func testDBWithMigrations(t *testing.T) DB {
	t.Helper()
	db := testSQLiteDB(t)

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}
	// Note: we intentionally do not call migrator.Close() here because it
	// closes the underlying *sql.DB via the migrate driver. The DB will be
	// cleaned up by testSQLiteDB's cleanup handler.

	return db
}

var rootID = uuid.MustParse("00000000-0000-0000-0000-000000000000")

func TestNamespaceRepo_Create(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	orgID := uuid.New()
	org := &model.Namespace{
		ID:       orgID,
		Name:     "Acme Corp",
		Slug:     "acme-corp",
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgID.String(),
		Depth:    1,
	}

	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create org namespace: %v", err)
	}

	if org.ID != orgID {
		t.Fatalf("expected ID %s, got %s", orgID, org.ID)
	}
	if org.Name != "Acme Corp" {
		t.Fatalf("expected name %q, got %q", "Acme Corp", org.Name)
	}
	if org.Kind != "org" {
		t.Fatalf("expected kind %q, got %q", "org", org.Kind)
	}
	if org.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if org.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
	if string(org.Metadata) != "{}" {
		t.Fatalf("expected metadata '{}', got %q", string(org.Metadata))
	}

	// Create a user under the org.
	userID := uuid.New()
	user := &model.Namespace{
		ID:       userID,
		Name:     "Alice",
		Slug:     "alice",
		Kind:     "user",
		ParentID: &orgID,
		Path:     orgID.String() + "/" + userID.String(),
		Depth:    2,
		Metadata: json.RawMessage(`{"role":"admin"}`),
	}

	if err := repo.Create(ctx, user); err != nil {
		t.Fatalf("failed to create user namespace: %v", err)
	}
	if user.Depth != 2 {
		t.Fatalf("expected depth 2, got %d", user.Depth)
	}
	if string(user.Metadata) != `{"role":"admin"}` {
		t.Fatalf("expected custom metadata, got %q", string(user.Metadata))
	}

	// Create a project under the user.
	projID := uuid.New()
	proj := &model.Namespace{
		ID:       projID,
		Name:     "My Project",
		Slug:     "my-project",
		Kind:     "project",
		ParentID: &userID,
		Path:     orgID.String() + "/" + userID.String() + "/" + projID.String(),
		Depth:    3,
	}

	if err := repo.Create(ctx, proj); err != nil {
		t.Fatalf("failed to create project namespace: %v", err)
	}
	if proj.Kind != "project" {
		t.Fatalf("expected kind %q, got %q", "project", proj.Kind)
	}
}

func TestNamespaceRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	ns := &model.Namespace{
		Name:     "Auto ID",
		Slug:     "auto-id",
		Kind:     "org",
		ParentID: &rootID,
		Path:     "auto",
		Depth:    1,
	}

	if err := repo.Create(ctx, ns); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}
	if ns.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
}

func TestNamespaceRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	orgID := uuid.New()
	org := &model.Namespace{
		ID:       orgID,
		Name:     "GetByID Org",
		Slug:     "getbyid-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgID.String(),
		Depth:    1,
		Metadata: json.RawMessage(`{"key":"value"}`),
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
	if fetched.Name != "GetByID Org" {
		t.Fatalf("expected name %q, got %q", "GetByID Org", fetched.Name)
	}
	if fetched.Slug != "getbyid-org" {
		t.Fatalf("expected slug %q, got %q", "getbyid-org", fetched.Slug)
	}
	if fetched.Kind != "org" {
		t.Fatalf("expected kind %q, got %q", "org", fetched.Kind)
	}
	if fetched.ParentID == nil || *fetched.ParentID != rootID {
		t.Fatalf("expected parent_id %s, got %v", rootID, fetched.ParentID)
	}
	if fetched.Path != orgID.String() {
		t.Fatalf("expected path %q, got %q", orgID.String(), fetched.Path)
	}
	if fetched.Depth != 1 {
		t.Fatalf("expected depth 1, got %d", fetched.Depth)
	}
	if string(fetched.Metadata) != `{"key":"value"}` {
		t.Fatalf("expected metadata '{\"key\":\"value\"}', got %q", string(fetched.Metadata))
	}
	if fetched.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if fetched.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
}

func TestNamespaceRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestNamespaceRepo_GetByPath(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	orgID := uuid.New()
	path := orgID.String()
	org := &model.Namespace{
		ID:       orgID,
		Name:     "Path Org",
		Slug:     "path-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     path,
		Depth:    1,
	}
	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetByPath(ctx, path)
	if err != nil {
		t.Fatalf("failed to get by path: %v", err)
	}
	if fetched.ID != orgID {
		t.Fatalf("expected ID %s, got %s", orgID, fetched.ID)
	}
	if fetched.Path != path {
		t.Fatalf("expected path %q, got %q", path, fetched.Path)
	}
}

func TestNamespaceRepo_GetByPath_Root(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	// The root namespace has path="" and is seeded by migration.
	fetched, err := repo.GetByPath(ctx, "")
	if err != nil {
		t.Fatalf("failed to get root by path: %v", err)
	}
	if fetched.ID != rootID {
		t.Fatalf("expected root ID %s, got %s", rootID, fetched.ID)
	}
	if fetched.Kind != "root" {
		t.Fatalf("expected kind %q, got %q", "root", fetched.Kind)
	}
}

func TestNamespaceRepo_ListByParent(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	// Create org under root.
	orgID := uuid.New()
	org := &model.Namespace{
		ID:       orgID,
		Name:     "List Org",
		Slug:     "list-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgID.String(),
		Depth:    1,
	}
	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create org: %v", err)
	}

	// Create two users under the org.
	user1ID := uuid.New()
	user1 := &model.Namespace{
		ID:       user1ID,
		Name:     "Alice",
		Slug:     "alice",
		Kind:     "user",
		ParentID: &orgID,
		Path:     orgID.String() + "/" + user1ID.String(),
		Depth:    2,
	}
	if err := repo.Create(ctx, user1); err != nil {
		t.Fatalf("failed to create user1: %v", err)
	}

	user2ID := uuid.New()
	user2 := &model.Namespace{
		ID:       user2ID,
		Name:     "Bob",
		Slug:     "bob",
		Kind:     "user",
		ParentID: &orgID,
		Path:     orgID.String() + "/" + user2ID.String(),
		Depth:    2,
	}
	if err := repo.Create(ctx, user2); err != nil {
		t.Fatalf("failed to create user2: %v", err)
	}

	children, err := repo.ListByParent(ctx, orgID)
	if err != nil {
		t.Fatalf("failed to list by parent: %v", err)
	}

	if len(children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(children))
	}

	// Results ordered by slug: alice < bob.
	if children[0].Slug != "alice" {
		t.Fatalf("expected first child slug %q, got %q", "alice", children[0].Slug)
	}
	if children[1].Slug != "bob" {
		t.Fatalf("expected second child slug %q, got %q", "bob", children[1].Slug)
	}
}

func TestNamespaceRepo_FindBySlugUnderParent(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	orgID := uuid.New()
	org := &model.Namespace{
		ID:       orgID,
		Name:     "Slug Org",
		Slug:     "slug-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgID.String(),
		Depth:    1,
	}
	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create org: %v", err)
	}

	userID := uuid.New()
	user := &model.Namespace{
		ID:       userID,
		Name:     "Charlie",
		Slug:     "charlie",
		Kind:     "user",
		ParentID: &orgID,
		Path:     orgID.String() + "/" + userID.String(),
		Depth:    2,
	}
	if err := repo.Create(ctx, user); err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	found, err := repo.FindBySlugUnderParent(ctx, orgID, "charlie")
	if err != nil {
		t.Fatalf("failed to find by slug: %v", err)
	}
	if found.ID != userID {
		t.Fatalf("expected ID %s, got %s", userID, found.ID)
	}
	if found.Slug != "charlie" {
		t.Fatalf("expected slug %q, got %q", "charlie", found.Slug)
	}
}

func TestNamespaceRepo_FindBySlugUnderParent_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	_, err := repo.FindBySlugUnderParent(ctx, rootID, "nonexistent-slug")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestNamespaceRepo_CreateIfNotExists_New(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	orgID := uuid.New()
	ns := &model.Namespace{
		ID:       orgID,
		Name:     "New Org",
		Slug:     "new-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgID.String(),
		Depth:    1,
	}

	result, created, err := repo.CreateIfNotExists(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create if not exists: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for new namespace")
	}
	if result.ID != orgID {
		t.Fatalf("expected ID %s, got %s", orgID, result.ID)
	}
	if result.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
}

func TestNamespaceRepo_CreateIfNotExists_Existing(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	orgID := uuid.New()
	ns := &model.Namespace{
		ID:       orgID,
		Name:     "Existing Org",
		Slug:     "existing-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgID.String(),
		Depth:    1,
	}

	// First call: creates.
	first, created, err := repo.CreateIfNotExists(ctx, ns)
	if err != nil {
		t.Fatalf("first create if not exists failed: %v", err)
	}
	if !created {
		t.Fatal("expected created=true on first call")
	}

	// Second call with same parent_id+slug but different ID.
	ns2 := &model.Namespace{
		ID:       uuid.New(),
		Name:     "Existing Org v2",
		Slug:     "existing-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     "different-path",
		Depth:    1,
	}

	second, created, err := repo.CreateIfNotExists(ctx, ns2)
	if err != nil {
		t.Fatalf("second create if not exists failed: %v", err)
	}
	if created {
		t.Fatal("expected created=false on second call")
	}
	if second.ID != first.ID {
		t.Fatalf("expected existing ID %s, got %s", first.ID, second.ID)
	}
	if second.Name != "Existing Org" {
		t.Fatalf("expected original name %q, got %q", "Existing Org", second.Name)
	}
}

func TestNamespaceRepo_ResolvePathPrefix(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewNamespaceRepo(db)

	// Build hierarchy: root -> org -> user1, user2 -> project
	orgID := uuid.New()
	org := &model.Namespace{
		ID:       orgID,
		Name:     "Resolve Org",
		Slug:     "resolve-org",
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgID.String(),
		Depth:    1,
	}
	if err := repo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create org: %v", err)
	}

	user1ID := uuid.New()
	user1 := &model.Namespace{
		ID:       user1ID,
		Name:     "User1",
		Slug:     "user1",
		Kind:     "user",
		ParentID: &orgID,
		Path:     orgID.String() + "/" + user1ID.String(),
		Depth:    2,
	}
	if err := repo.Create(ctx, user1); err != nil {
		t.Fatalf("failed to create user1: %v", err)
	}

	user2ID := uuid.New()
	user2 := &model.Namespace{
		ID:       user2ID,
		Name:     "User2",
		Slug:     "user2",
		Kind:     "user",
		ParentID: &orgID,
		Path:     orgID.String() + "/" + user2ID.String(),
		Depth:    2,
	}
	if err := repo.Create(ctx, user2); err != nil {
		t.Fatalf("failed to create user2: %v", err)
	}

	projID := uuid.New()
	proj := &model.Namespace{
		ID:       projID,
		Name:     "Project1",
		Slug:     "project1",
		Kind:     "project",
		ParentID: &user1ID,
		Path:     orgID.String() + "/" + user1ID.String() + "/" + projID.String(),
		Depth:    3,
	}
	if err := repo.Create(ctx, proj); err != nil {
		t.Fatalf("failed to create project: %v", err)
	}

	// Resolve with org path prefix — should match org, user1, user2, project.
	ids, err := repo.ResolvePathPrefix(ctx, orgID.String())
	if err != nil {
		t.Fatalf("failed to resolve path prefix: %v", err)
	}

	if len(ids) != 4 {
		t.Fatalf("expected 4 IDs, got %d", len(ids))
	}

	// Verify all expected IDs are present.
	idSet := make(map[uuid.UUID]bool)
	for _, id := range ids {
		idSet[id] = true
	}
	for _, expected := range []uuid.UUID{orgID, user1ID, user2ID, projID} {
		if !idSet[expected] {
			t.Fatalf("expected ID %s in results, but not found", expected)
		}
	}

	// Resolve with user1 path prefix — should match user1 and project.
	user1Path := orgID.String() + "/" + user1ID.String()
	ids2, err := repo.ResolvePathPrefix(ctx, user1Path)
	if err != nil {
		t.Fatalf("failed to resolve user1 path prefix: %v", err)
	}

	if len(ids2) != 2 {
		t.Fatalf("expected 2 IDs for user1 prefix, got %d", len(ids2))
	}

	idSet2 := make(map[uuid.UUID]bool)
	for _, id := range ids2 {
		idSet2[id] = true
	}
	if !idSet2[user1ID] {
		t.Fatalf("expected user1 ID %s in results", user1ID)
	}
	if !idSet2[projID] {
		t.Fatalf("expected project ID %s in results", projID)
	}
}
