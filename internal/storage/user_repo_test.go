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

func createTestOrg(t *testing.T, ctx context.Context, db DB) (orgID uuid.UUID, orgNSID uuid.UUID, orgNSPath string) {
	t.Helper()
	nsRepo := NewNamespaceRepo(db)
	orgRepo := NewOrganizationRepo(db)

	orgNSID = uuid.New()
	ns := &model.Namespace{
		ID:       orgNSID,
		Name:     "Org " + orgNSID.String()[:8],
		Slug:     orgNSID.String(),
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgNSID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("failed to create org namespace: %v", err)
	}

	org := &model.Organization{
		NamespaceID: orgNSID,
		Name:        "Test Org",
		Slug:        "test-org-" + orgNSID.String()[:8],
	}
	if err := orgRepo.Create(ctx, org); err != nil {
		t.Fatalf("failed to create org: %v", err)
	}

	return org.ID, orgNSID, ns.Path
}

func TestUserRepo_Create(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	pw, err := HashPassword("secret123")
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	userID := uuid.New()
	user := &model.User{
		ID:           userID,
		Email:        "alice@example.com",
		DisplayName:  "Alice",
		PasswordHash: &pw,
		OrgID:        orgID,
		Role:         "member",
	}

	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	if user.ID != userID {
		t.Fatalf("expected ID %s, got %s", userID, user.ID)
	}
	if user.Email != "alice@example.com" {
		t.Fatalf("expected email %q, got %q", "alice@example.com", user.Email)
	}
	if user.DisplayName != "Alice" {
		t.Fatalf("expected display_name %q, got %q", "Alice", user.DisplayName)
	}
	if user.OrgID != orgID {
		t.Fatalf("expected org_id %s, got %s", orgID, user.OrgID)
	}
	if user.NamespaceID == uuid.Nil {
		t.Fatal("expected non-nil namespace_id")
	}
	if user.Role != "member" {
		t.Fatalf("expected role %q, got %q", "member", user.Role)
	}
	if string(user.Settings) != "{}" {
		t.Fatalf("expected settings '{}', got %q", string(user.Settings))
	}
	if user.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if user.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
	if user.PasswordHash == nil {
		t.Fatal("expected non-nil password_hash")
	}
}

func TestUserRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "autoid@example.com",
		DisplayName: "Auto ID",
		OrgID:       orgID,
		Role:        "member",
	}

	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create user: %v", err)
	}
	if user.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
}

func TestUserRepo_GetByID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "getbyid@example.com",
		DisplayName: "Get By ID",
		OrgID:       orgID,
		Role:        "admin",
		Settings:    json.RawMessage(`{"theme":"dark"}`),
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to get by id: %v", err)
	}

	if fetched.ID != user.ID {
		t.Fatalf("expected ID %s, got %s", user.ID, fetched.ID)
	}
	if fetched.Email != "getbyid@example.com" {
		t.Fatalf("expected email %q, got %q", "getbyid@example.com", fetched.Email)
	}
	if fetched.DisplayName != "Get By ID" {
		t.Fatalf("expected display_name %q, got %q", "Get By ID", fetched.DisplayName)
	}
	if fetched.Role != "admin" {
		t.Fatalf("expected role %q, got %q", "admin", fetched.Role)
	}
	if string(fetched.Settings) != `{"theme":"dark"}` {
		t.Fatalf("expected settings '{\"theme\":\"dark\"}', got %q", string(fetched.Settings))
	}
	if fetched.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
}

func TestUserRepo_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewUserRepo(db)

	_, err := repo.GetByID(ctx, uuid.New())
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestUserRepo_GetByEmail(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "byemail@example.com",
		DisplayName: "By Email",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	fetched, err := repo.GetByEmail(ctx, "byemail@example.com")
	if err != nil {
		t.Fatalf("failed to get by email: %v", err)
	}

	if fetched.ID != user.ID {
		t.Fatalf("expected ID %s, got %s", user.ID, fetched.ID)
	}
	if fetched.Email != "byemail@example.com" {
		t.Fatalf("expected email %q, got %q", "byemail@example.com", fetched.Email)
	}
}

func TestUserRepo_GetByEmail_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewUserRepo(db)

	_, err := repo.GetByEmail(ctx, "nonexistent@example.com")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestUserRepo_Authenticate_Success(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	pw, err := HashPassword("correct-password")
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	user := &model.User{
		Email:        "auth@example.com",
		DisplayName:  "Auth User",
		PasswordHash: &pw,
		OrgID:        orgID,
		Role:         "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if user.LastLogin != nil {
		t.Fatal("expected nil last_login before authenticate")
	}

	authed, err := repo.Authenticate(ctx, "auth@example.com", "correct-password")
	if err != nil {
		t.Fatalf("failed to authenticate: %v", err)
	}

	if authed.ID != user.ID {
		t.Fatalf("expected ID %s, got %s", user.ID, authed.ID)
	}
	if authed.LastLogin == nil {
		t.Fatal("expected non-nil last_login after authenticate")
	}
}

func TestUserRepo_Authenticate_WrongPassword(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	pw, err := HashPassword("correct-password")
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	user := &model.User{
		Email:        "wrongpw@example.com",
		DisplayName:  "Wrong PW",
		PasswordHash: &pw,
		OrgID:        orgID,
		Role:         "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	_, err = repo.Authenticate(ctx, "wrongpw@example.com", "wrong-password")
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestUserRepo_Authenticate_Disabled(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	pw, err := HashPassword("password")
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	user := &model.User{
		Email:        "disabled@example.com",
		DisplayName:  "Disabled",
		PasswordHash: &pw,
		OrgID:        orgID,
		Role:         "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.Disable(ctx, user.ID); err != nil {
		t.Fatalf("failed to disable: %v", err)
	}

	_, err = repo.Authenticate(ctx, "disabled@example.com", "password")
	if !errors.Is(err, ErrUserDisabled) {
		t.Fatalf("expected ErrUserDisabled, got %v", err)
	}
}

func TestUserRepo_Authenticate_NoPassword(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "nopw@example.com",
		DisplayName: "No Password",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	_, err := repo.Authenticate(ctx, "nopw@example.com", "anything")
	if !errors.Is(err, ErrNoPassword) {
		t.Fatalf("expected ErrNoPassword, got %v", err)
	}
}

func TestUserRepo_ListByOrg(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user1 := &model.User{
		Email:       "charlie@example.com",
		DisplayName: "Charlie",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := repo.Create(ctx, user1, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create user1: %v", err)
	}

	user2 := &model.User{
		Email:       "alice@example.com",
		DisplayName: "Alice",
		OrgID:       orgID,
		Role:        "admin",
	}
	if err := repo.Create(ctx, user2, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create user2: %v", err)
	}

	users, err := repo.ListByOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	if users[0].Email != "alice@example.com" {
		t.Fatalf("expected first user email %q, got %q", "alice@example.com", users[0].Email)
	}
	if users[1].Email != "charlie@example.com" {
		t.Fatalf("expected second user email %q, got %q", "charlie@example.com", users[1].Email)
	}
}

func TestUserRepo_Update(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "update@example.com",
		DisplayName: "Original",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	originalUpdatedAt := user.UpdatedAt

	user.DisplayName = "Updated"
	user.Role = "admin"
	user.Settings = json.RawMessage(`{"updated":true}`)

	if err := repo.Update(ctx, user); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	if user.DisplayName != "Updated" {
		t.Fatalf("expected display_name %q, got %q", "Updated", user.DisplayName)
	}
	if user.Role != "admin" {
		t.Fatalf("expected role %q, got %q", "admin", user.Role)
	}
	if string(user.Settings) != `{"updated":true}` {
		t.Fatalf("expected settings '{\"updated\":true}', got %q", string(user.Settings))
	}
	if !user.UpdatedAt.After(originalUpdatedAt) && user.UpdatedAt != originalUpdatedAt {
		t.Fatal("expected updated_at to be updated")
	}

	fetched, err := repo.GetByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to get after update: %v", err)
	}
	if fetched.DisplayName != "Updated" {
		t.Fatalf("expected fetched display_name %q, got %q", "Updated", fetched.DisplayName)
	}
	if fetched.Role != "admin" {
		t.Fatalf("expected fetched role %q, got %q", "admin", fetched.Role)
	}
}

func TestUserRepo_Disable(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "disable@example.com",
		DisplayName: "Disable Me",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.Disable(ctx, user.ID); err != nil {
		t.Fatalf("failed to disable: %v", err)
	}

	fetched, err := repo.GetByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to get after disable: %v", err)
	}
	if fetched.DisabledAt == nil {
		t.Fatal("expected non-nil disabled_at after disable")
	}
}

func TestUserRepo_Enable(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "enable@example.com",
		DisplayName: "Enable Me",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.Disable(ctx, user.ID); err != nil {
		t.Fatalf("failed to disable: %v", err)
	}

	if err := repo.Enable(ctx, user.ID); err != nil {
		t.Fatalf("failed to enable: %v", err)
	}

	fetched, err := repo.GetByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to get after enable: %v", err)
	}
	if fetched.DisabledAt != nil {
		t.Fatal("expected nil disabled_at after enable")
	}
}

func TestUserRepo_Delete(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	nsRepo := NewNamespaceRepo(db)
	repo := NewUserRepo(db)

	orgID, _, orgNSPath := createTestOrg(t, ctx, db)

	user := &model.User{
		Email:       "delete@example.com",
		DisplayName: "Delete Me",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := repo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.Delete(ctx, user.ID); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	_, err := repo.GetByID(ctx, user.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
	}
}
