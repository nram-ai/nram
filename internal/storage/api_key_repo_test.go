package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func createTestUser(t *testing.T, ctx context.Context, db DB) *model.User {
	t.Helper()
	nsRepo := NewNamespaceRepo(db)
	userRepo := NewUserRepo(db)

	_, _, orgNSPath := createTestOrg(t, ctx, db)
	orgNS, err := nsRepo.GetByPath(ctx, orgNSPath)
	if err != nil {
		t.Fatalf("failed to get org namespace: %v", err)
	}

	// Find org by namespace_id
	var orgIDStr string
	row := db.QueryRow(ctx, "SELECT id FROM organizations WHERE namespace_id = ?", orgNS.ID.String())
	if err := row.Scan(&orgIDStr); err != nil {
		t.Fatalf("failed to find org: %v", err)
	}
	orgID, _ := uuid.Parse(orgIDStr)

	user := &model.User{
		Email:       "apitest-" + uuid.New().String()[:8] + "@example.com",
		DisplayName: "API Test User",
		OrgID:       orgID,
		Role:        "member",
	}
	if err := userRepo.Create(ctx, user, nsRepo, orgNSPath); err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}
	return user
}

func TestAPIKeyRepo_Create(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	key := &model.APIKey{
		UserID: user.ID,
		Name:   "test-key",
		Scopes: []uuid.UUID{uuid.New(), uuid.New()},
	}

	rawKey, err := repo.Create(ctx, key)
	if err != nil {
		t.Fatalf("failed to create api key: %v", err)
	}

	if !strings.HasPrefix(rawKey, "nram_k_") {
		t.Fatalf("expected raw key to start with 'nram_k_', got %q", rawKey[:10])
	}
	if len(rawKey) != 71 { // "nram_k_" (7) + 64 hex chars
		t.Fatalf("expected raw key length 71, got %d", len(rawKey))
	}
	if key.ID == uuid.Nil {
		t.Fatal("expected non-nil ID after create")
	}
	if key.KeyPrefix == "" {
		t.Fatal("expected non-empty key_prefix")
	}
	if !strings.HasPrefix(key.KeyPrefix, "nram_k_") {
		t.Fatalf("expected key_prefix to start with 'nram_k_', got %q", key.KeyPrefix)
	}
	if key.Name != "test-key" {
		t.Fatalf("expected name %q, got %q", "test-key", key.Name)
	}
	if len(key.Scopes) != 2 {
		t.Fatalf("expected 2 scopes, got %d", len(key.Scopes))
	}
	if key.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if key.ExpiresAt != nil {
		t.Fatal("expected nil expires_at")
	}
}

func TestAPIKeyRepo_Create_WithExpiry(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	expires := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	key := &model.APIKey{
		UserID:    user.ID,
		Name:      "expiring-key",
		Scopes:    []uuid.UUID{},
		ExpiresAt: &expires,
	}

	_, err := repo.Create(ctx, key)
	if err != nil {
		t.Fatalf("failed to create api key with expiry: %v", err)
	}

	if key.ExpiresAt == nil {
		t.Fatal("expected non-nil expires_at")
	}
}

func TestAPIKeyRepo_Create_GeneratesID(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	key := &model.APIKey{
		UserID: user.ID,
		Name:   "auto-id",
		Scopes: []uuid.UUID{},
	}

	_, err := repo.Create(ctx, key)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	if key.ID == uuid.Nil {
		t.Fatal("expected non-nil ID")
	}
}

func TestAPIKeyRepo_Validate_Success(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	key := &model.APIKey{
		UserID: user.ID,
		Name:   "validate-key",
		Scopes: []uuid.UUID{uuid.New()},
	}

	rawKey, err := repo.Create(ctx, key)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	validated, err := repo.Validate(ctx, rawKey)
	if err != nil {
		t.Fatalf("failed to validate: %v", err)
	}

	if validated.ID != key.ID {
		t.Fatalf("expected ID %s, got %s", key.ID, validated.ID)
	}
	if validated.UserID != user.ID {
		t.Fatalf("expected user_id %s, got %s", user.ID, validated.UserID)
	}
	if validated.LastUsed == nil {
		t.Fatal("expected non-nil last_used after validate")
	}
}

func TestAPIKeyRepo_Validate_InvalidKey(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewAPIKeyRepo(db)

	_, err := repo.Validate(ctx, "nram_k_0000000000000000000000000000000000000000000000000000000000000000")
	if !errors.Is(err, ErrAPIKeyNotFound) {
		t.Fatalf("expected ErrAPIKeyNotFound, got %v", err)
	}
}

func TestAPIKeyRepo_Validate_Expired(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	expired := time.Now().UTC().Add(-1 * time.Hour)
	key := &model.APIKey{
		UserID:    user.ID,
		Name:      "expired-key",
		Scopes:    []uuid.UUID{},
		ExpiresAt: &expired,
	}

	rawKey, err := repo.Create(ctx, key)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	_, err = repo.Validate(ctx, rawKey)
	if !errors.Is(err, ErrAPIKeyExpired) {
		t.Fatalf("expected ErrAPIKeyExpired, got %v", err)
	}
}

func TestAPIKeyRepo_ListByUser(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	key1 := &model.APIKey{UserID: user.ID, Name: "key-1", Scopes: []uuid.UUID{}}
	key2 := &model.APIKey{UserID: user.ID, Name: "key-2", Scopes: []uuid.UUID{}}

	if _, err := repo.Create(ctx, key1); err != nil {
		t.Fatalf("failed to create key1: %v", err)
	}
	if _, err := repo.Create(ctx, key2); err != nil {
		t.Fatalf("failed to create key2: %v", err)
	}

	keys, err := repo.ListByUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	names := map[string]bool{keys[0].Name: true, keys[1].Name: true}
	if !names["key-1"] || !names["key-2"] {
		t.Fatalf("expected keys named key-1 and key-2, got %q and %q", keys[0].Name, keys[1].Name)
	}
}

func TestAPIKeyRepo_ListByUser_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewAPIKeyRepo(db)

	keys, err := repo.ListByUser(ctx, uuid.New())
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if keys != nil {
		t.Fatalf("expected nil for no keys, got %d keys", len(keys))
	}
}

func TestAPIKeyRepo_Revoke(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	key := &model.APIKey{UserID: user.ID, Name: "revoke-me", Scopes: []uuid.UUID{}}
	rawKey, err := repo.Create(ctx, key)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	if err := repo.Revoke(ctx, key.ID); err != nil {
		t.Fatalf("failed to revoke: %v", err)
	}

	// Verify key is gone
	_, err = repo.GetByID(ctx, key.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after revoke, got %v", err)
	}

	// Verify validate fails
	_, err = repo.Validate(ctx, rawKey)
	if !errors.Is(err, ErrAPIKeyNotFound) {
		t.Fatalf("expected ErrAPIKeyNotFound after revoke, got %v", err)
	}
}

func TestAPIKeyRepo_Revoke_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewAPIKeyRepo(db)

	err := repo.Revoke(ctx, uuid.New())
	if !errors.Is(err, ErrAPIKeyNotFound) {
		t.Fatalf("expected ErrAPIKeyNotFound, got %v", err)
	}
}

func TestAPIKeyRepo_CheckExpiry(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	// Create an expired key
	expired := time.Now().UTC().Add(-2 * time.Hour)
	expiredKey := &model.APIKey{
		UserID:    user.ID,
		Name:      "expired",
		Scopes:    []uuid.UUID{},
		ExpiresAt: &expired,
	}
	if _, err := repo.Create(ctx, expiredKey); err != nil {
		t.Fatalf("failed to create expired key: %v", err)
	}

	// Create a valid key (no expiry)
	validKey := &model.APIKey{
		UserID: user.ID,
		Name:   "no-expiry",
		Scopes: []uuid.UUID{},
	}
	if _, err := repo.Create(ctx, validKey); err != nil {
		t.Fatalf("failed to create valid key: %v", err)
	}

	// Create a future key
	future := time.Now().UTC().Add(24 * time.Hour)
	futureKey := &model.APIKey{
		UserID:    user.ID,
		Name:      "future",
		Scopes:    []uuid.UUID{},
		ExpiresAt: &future,
	}
	if _, err := repo.Create(ctx, futureKey); err != nil {
		t.Fatalf("failed to create future key: %v", err)
	}

	expiredKeys, err := repo.CheckExpiry(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to check expiry: %v", err)
	}

	if len(expiredKeys) != 1 {
		t.Fatalf("expected 1 expired key, got %d", len(expiredKeys))
	}
	if expiredKeys[0].Name != "expired" {
		t.Fatalf("expected expired key name %q, got %q", "expired", expiredKeys[0].Name)
	}
}

func TestAPIKeyRepo_CheckExpiry_None(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	user := createTestUser(t, ctx, db)
	repo := NewAPIKeyRepo(db)

	// Only create a valid key
	key := &model.APIKey{
		UserID: user.ID,
		Name:   "valid",
		Scopes: []uuid.UUID{},
	}
	if _, err := repo.Create(ctx, key); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	expiredKeys, err := repo.CheckExpiry(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to check expiry: %v", err)
	}
	if expiredKeys != nil {
		t.Fatalf("expected nil expired keys, got %d", len(expiredKeys))
	}
}
