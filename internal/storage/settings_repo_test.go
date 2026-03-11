package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nram-ai/nram/internal/model"
)

func TestSettingsRepo_Set(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	setting := &model.Setting{
		Key:   "theme",
		Value: json.RawMessage(`"dark"`),
		Scope: "global",
	}

	if err := repo.Set(ctx, setting); err != nil {
		t.Fatalf("failed to set setting: %v", err)
	}

	if setting.Key != "theme" {
		t.Fatalf("expected key %q, got %q", "theme", setting.Key)
	}
	if setting.Scope != "global" {
		t.Fatalf("expected scope %q, got %q", "global", setting.Scope)
	}
	if string(setting.Value) != `"dark"` {
		t.Fatalf("expected value %q, got %q", `"dark"`, string(setting.Value))
	}
	if setting.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
}

func TestSettingsRepo_Set_WithUpdatedBy(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	user := createTestUser(t, ctx, db)
	userID := user.ID

	setting := &model.Setting{
		Key:       "language",
		Value:     json.RawMessage(`"en"`),
		Scope:     "global",
		UpdatedBy: &userID,
	}

	if err := repo.Set(ctx, setting); err != nil {
		t.Fatalf("failed to set setting: %v", err)
	}

	if setting.UpdatedBy == nil {
		t.Fatal("expected non-nil updated_by")
	}
	if *setting.UpdatedBy != userID {
		t.Fatalf("expected updated_by %s, got %s", userID, *setting.UpdatedBy)
	}
}

func TestSettingsRepo_Set_Upsert(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	setting := &model.Setting{
		Key:   "color",
		Value: json.RawMessage(`"blue"`),
		Scope: "global",
	}

	if err := repo.Set(ctx, setting); err != nil {
		t.Fatalf("failed to set setting: %v", err)
	}

	if string(setting.Value) != `"blue"` {
		t.Fatalf("expected value %q, got %q", `"blue"`, string(setting.Value))
	}

	// Upsert with new value.
	setting.Value = json.RawMessage(`"red"`)
	if err := repo.Set(ctx, setting); err != nil {
		t.Fatalf("failed to upsert setting: %v", err)
	}

	if string(setting.Value) != `"red"` {
		t.Fatalf("expected updated value %q, got %q", `"red"`, string(setting.Value))
	}

	// Verify via fresh fetch.
	fetched, err := repo.Get(ctx, "color", "global")
	if err != nil {
		t.Fatalf("failed to get after upsert: %v", err)
	}
	if string(fetched.Value) != `"red"` {
		t.Fatalf("expected fetched value %q, got %q", `"red"`, string(fetched.Value))
	}
}

func TestSettingsRepo_Get_Exact(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	setting := &model.Setting{
		Key:   "timeout",
		Value: json.RawMessage(`30`),
		Scope: "org:abc-123",
	}
	if err := repo.Set(ctx, setting); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	fetched, err := repo.Get(ctx, "timeout", "org:abc-123")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if fetched.Key != "timeout" {
		t.Fatalf("expected key %q, got %q", "timeout", fetched.Key)
	}
	if fetched.Scope != "org:abc-123" {
		t.Fatalf("expected scope %q, got %q", "org:abc-123", fetched.Scope)
	}
	if string(fetched.Value) != `30` {
		t.Fatalf("expected value %q, got %q", `30`, string(fetched.Value))
	}
}

func TestSettingsRepo_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	_, err := repo.Get(ctx, "nonexistent", "global")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestSettingsRepo_Get_CascadeToGlobal(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	// Set a global default.
	globalSetting := &model.Setting{
		Key:   "max_retries",
		Value: json.RawMessage(`3`),
		Scope: "global",
	}
	if err := repo.Set(ctx, globalSetting); err != nil {
		t.Fatalf("failed to set global: %v", err)
	}

	// Request from project scope — should cascade to global.
	fetched, err := repo.Get(ctx, "max_retries", "project:proj-1")
	if err != nil {
		t.Fatalf("failed to get with cascade: %v", err)
	}

	if fetched.Scope != "global" {
		t.Fatalf("expected cascaded scope %q, got %q", "global", fetched.Scope)
	}
	if string(fetched.Value) != `3` {
		t.Fatalf("expected value %q, got %q", `3`, string(fetched.Value))
	}
}

func TestSettingsRepo_Get_CascadeStopsAtFirstMatch(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	// Set global and org-level values.
	globalSetting := &model.Setting{
		Key:   "log_level",
		Value: json.RawMessage(`"info"`),
		Scope: "global",
	}
	if err := repo.Set(ctx, globalSetting); err != nil {
		t.Fatalf("failed to set global: %v", err)
	}

	orgSetting := &model.Setting{
		Key:   "log_level",
		Value: json.RawMessage(`"debug"`),
		Scope: "org",
	}
	if err := repo.Set(ctx, orgSetting); err != nil {
		t.Fatalf("failed to set org: %v", err)
	}

	// Request from user scope — should find org before global.
	fetched, err := repo.Get(ctx, "log_level", "user:user-1")
	if err != nil {
		t.Fatalf("failed to get with cascade: %v", err)
	}

	if fetched.Scope != "org" {
		t.Fatalf("expected cascaded scope %q, got %q", "org", fetched.Scope)
	}
	if string(fetched.Value) != `"debug"` {
		t.Fatalf("expected value %q, got %q", `"debug"`, string(fetched.Value))
	}
}

func TestSettingsRepo_Get_CascadeFromProjectToUser(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	// Only set at user scope.
	userSetting := &model.Setting{
		Key:   "editor",
		Value: json.RawMessage(`"vim"`),
		Scope: "user",
	}
	if err := repo.Set(ctx, userSetting); err != nil {
		t.Fatalf("failed to set user: %v", err)
	}

	// Request from project scope — should cascade to user.
	fetched, err := repo.Get(ctx, "editor", "project:proj-1")
	if err != nil {
		t.Fatalf("failed to get with cascade: %v", err)
	}

	if fetched.Scope != "user" {
		t.Fatalf("expected cascaded scope %q, got %q", "user", fetched.Scope)
	}
	if string(fetched.Value) != `"vim"` {
		t.Fatalf("expected value %q, got %q", `"vim"`, string(fetched.Value))
	}
}

func TestSettingsRepo_Get_CascadeNoneFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	// Nothing set at any scope.
	_, err := repo.Get(ctx, "missing_key", "project:proj-1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestSettingsRepo_Delete(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	setting := &model.Setting{
		Key:   "delete_me",
		Value: json.RawMessage(`true`),
		Scope: "global",
	}
	if err := repo.Set(ctx, setting); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	if err := repo.Delete(ctx, "delete_me", "global"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	_, err := repo.Get(ctx, "delete_me", "global")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
	}
}

func TestSettingsRepo_Delete_NonExistent(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	// Deleting a non-existent key should not error.
	if err := repo.Delete(ctx, "nonexistent", "global"); err != nil {
		t.Fatalf("expected no error deleting nonexistent setting, got %v", err)
	}
}

func TestSettingsRepo_ListByScope(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	// Insert multiple settings in the same scope.
	s1 := &model.Setting{
		Key:   "beta_feature",
		Value: json.RawMessage(`true`),
		Scope: "org:test-org",
	}
	s2 := &model.Setting{
		Key:   "alpha_feature",
		Value: json.RawMessage(`false`),
		Scope: "org:test-org",
	}
	// Different scope — should not appear.
	s3 := &model.Setting{
		Key:   "gamma_feature",
		Value: json.RawMessage(`true`),
		Scope: "global",
	}

	for _, s := range []*model.Setting{s1, s2, s3} {
		if err := repo.Set(ctx, s); err != nil {
			t.Fatalf("failed to set %q: %v", s.Key, err)
		}
	}

	settings, err := repo.ListByScope(ctx, "org:test-org")
	if err != nil {
		t.Fatalf("failed to list by scope: %v", err)
	}

	if len(settings) != 2 {
		t.Fatalf("expected 2 settings, got %d", len(settings))
	}

	// Results ordered by key: alpha < beta.
	if settings[0].Key != "alpha_feature" {
		t.Fatalf("expected first key %q, got %q", "alpha_feature", settings[0].Key)
	}
	if settings[1].Key != "beta_feature" {
		t.Fatalf("expected second key %q, got %q", "beta_feature", settings[1].Key)
	}
}

func TestSettingsRepo_ListByScope_Empty(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	settings, err := repo.ListByScope(ctx, "org:nonexistent")
	if err != nil {
		t.Fatalf("failed to list by scope: %v", err)
	}

	if len(settings) != 0 {
		t.Fatalf("expected 0 settings, got %d", len(settings))
	}
}

func TestSettingsRepo_GetSchema(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	schema := &model.Setting{
		Key:   "max_tokens",
		Value: json.RawMessage(`{"type":"integer","default":4096,"min":1,"max":100000}`),
		Scope: "global",
	}
	if err := repo.Set(ctx, schema); err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	fetched, err := repo.GetSchema(ctx, "max_tokens")
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

	if fetched.Key != "max_tokens" {
		t.Fatalf("expected key %q, got %q", "max_tokens", fetched.Key)
	}
	if fetched.Scope != "global" {
		t.Fatalf("expected scope %q, got %q", "global", fetched.Scope)
	}
	if string(fetched.Value) != `{"type":"integer","default":4096,"min":1,"max":100000}` {
		t.Fatalf("unexpected schema value: %s", string(fetched.Value))
	}
}

func TestSettingsRepo_GetSchema_NotFound(t *testing.T) {
	ctx := context.Background()
	db := testDBWithMigrations(t)
	repo := NewSettingsRepo(db)

	_, err := repo.GetSchema(ctx, "nonexistent")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestCascadeScopes(t *testing.T) {
	tests := []struct {
		scope    string
		expected []string
	}{
		{"global", []string{"global"}},
		{"org", []string{"org", "global"}},
		{"org:abc", []string{"org:abc", "global"}},
		{"user", []string{"user", "org", "global"}},
		{"user:xyz", []string{"user:xyz", "org", "global"}},
		{"project:p1", []string{"project:p1", "user", "org", "global"}},
		{"unknown:foo", []string{"unknown:foo", "global"}},
	}

	for _, tt := range tests {
		t.Run(tt.scope, func(t *testing.T) {
			got := cascadeScopes(tt.scope)
			if len(got) != len(tt.expected) {
				t.Fatalf("cascadeScopes(%q) = %v, want %v", tt.scope, got, tt.expected)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Fatalf("cascadeScopes(%q)[%d] = %q, want %q", tt.scope, i, got[i], tt.expected[i])
				}
			}
		})
	}
}
