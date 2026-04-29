package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// mockSettingsRepo implements SettingsRepository for testing.
type mockSettingsRepo struct {
	settings map[string]map[string]*model.Setting // key -> scope -> setting
	getCalls int
	setCalls int
	delCalls int
}

func newMockSettingsRepo() *mockSettingsRepo {
	return &mockSettingsRepo{
		settings: make(map[string]map[string]*model.Setting),
	}
}

func (m *mockSettingsRepo) put(key, scope, value string) {
	if m.settings[key] == nil {
		m.settings[key] = make(map[string]*model.Setting)
	}
	jsonVal, _ := json.Marshal(value)
	m.settings[key][scope] = &model.Setting{
		Key:       key,
		Value:     json.RawMessage(jsonVal),
		Scope:     scope,
		UpdatedAt: time.Now(),
	}
}

func (m *mockSettingsRepo) Get(_ context.Context, key string, scope string) (*model.Setting, error) {
	m.getCalls++
	scopes, ok := m.settings[key]
	if !ok {
		return nil, sql.ErrNoRows
	}
	s, ok := scopes[scope]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return s, nil
}

func (m *mockSettingsRepo) Set(_ context.Context, setting *model.Setting) error {
	m.setCalls++
	if m.settings[setting.Key] == nil {
		m.settings[setting.Key] = make(map[string]*model.Setting)
	}
	m.settings[setting.Key][setting.Scope] = setting
	return nil
}

func (m *mockSettingsRepo) Delete(_ context.Context, key string, scope string) error {
	m.delCalls++
	if scopes, ok := m.settings[key]; ok {
		delete(scopes, scope)
	}
	return nil
}

func (m *mockSettingsRepo) ListByScope(_ context.Context, scope string) ([]model.Setting, error) {
	var result []model.Setting
	for _, scopes := range m.settings {
		if s, ok := scopes[scope]; ok {
			result = append(result, *s)
		}
	}
	return result, nil
}

func TestResolveFromDatabase(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put("custom.key", "project:abc", "project-value")
	svc := NewSettingsService(repo)

	val, err := svc.Resolve(context.Background(), "custom.key", "project:abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "project-value" {
		t.Fatalf("expected %q, got %q", "project-value", val)
	}
}

func TestResolveFallsBackToDefault(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	val, err := svc.Resolve(context.Background(), SettingDedupThreshold, "project:abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "0.92" {
		t.Fatalf("expected default %q, got %q", "0.92", val)
	}
}

func TestResolveReturnsEmptyWhenNoValueAndNoDefault(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	val, err := svc.Resolve(context.Background(), "unknown.key", "global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "" {
		t.Fatalf("expected empty string, got %q", val)
	}
}

func TestResolveFloatSuccess(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingRankWeightSim, "global", "0.75")
	svc := NewSettingsService(repo)

	f, err := svc.ResolveFloat(context.Background(), SettingRankWeightSim, "global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f != 0.75 {
		t.Fatalf("expected 0.75, got %f", f)
	}
}

func TestResolveFloatWithDefault(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	f, err := svc.ResolveFloat(context.Background(), SettingRankWeightSim, "global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f != 0.5 {
		t.Fatalf("expected default 0.5, got %f", f)
	}
}

func TestResolveIntSuccess(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingTokenRetention, "org:123", "90")
	svc := NewSettingsService(repo)

	i, err := svc.ResolveInt(context.Background(), SettingTokenRetention, "org:123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if i != 90 {
		t.Fatalf("expected 90, got %d", i)
	}
}

func TestResolveIntWithDefault(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	i, err := svc.ResolveInt(context.Background(), SettingTokenRetention, "global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if i != 365 {
		t.Fatalf("expected default 365, got %d", i)
	}
}

func TestSetWritesToDatabase(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	uid := uuid.New()
	err := svc.Set(context.Background(), "custom.key", "my-value", "project:abc", &uid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.setCalls != 1 {
		t.Fatalf("expected 1 Set call, got %d", repo.setCalls)
	}

	// Verify the value was stored correctly.
	stored := repo.settings["custom.key"]["project:abc"]
	if stored == nil {
		t.Fatal("setting not found in repo")
	}
	if stored.Scope != "project:abc" {
		t.Fatalf("expected scope %q, got %q", "project:abc", stored.Scope)
	}
	if stored.UpdatedBy == nil || *stored.UpdatedBy != uid {
		t.Fatalf("expected updated_by %v, got %v", uid, stored.UpdatedBy)
	}

	// Verify we can resolve the value back.
	val, err := svc.Resolve(context.Background(), "custom.key", "project:abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "my-value" {
		t.Fatalf("expected %q, got %q", "my-value", val)
	}
}

func TestResolveDreamContradictionParaphraseDefaults(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	enabled := svc.ResolveBool(context.Background(), SettingDreamContradictionParaphraseEnabled, "global")
	if !enabled {
		t.Errorf("expected paraphrase_enabled default true, got false")
	}

	threshold, err := svc.ResolveFloat(context.Background(), SettingDreamContradictionParaphraseThreshold, "global")
	if err != nil {
		t.Fatalf("ResolveFloat: %v", err)
	}
	if threshold != 0.97 {
		t.Errorf("expected paraphrase_threshold default 0.97, got %f", threshold)
	}
}

func TestResolveDreamContradictionParaphraseOverride(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingDreamContradictionParaphraseEnabled, "global", "false")
	repo.put(SettingDreamContradictionParaphraseThreshold, "global", "0.93")
	svc := NewSettingsService(repo)

	if svc.ResolveBool(context.Background(), SettingDreamContradictionParaphraseEnabled, "global") {
		t.Errorf("expected paraphrase_enabled override false, got true")
	}

	threshold, err := svc.ResolveFloat(context.Background(), SettingDreamContradictionParaphraseThreshold, "global")
	if err != nil {
		t.Fatalf("ResolveFloat: %v", err)
	}
	if threshold != 0.93 {
		t.Errorf("expected override 0.93, got %f", threshold)
	}
}

func TestDeleteRemovesFromDatabase(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put("custom.key", "global", "some-value")
	svc := NewSettingsService(repo)

	err := svc.Delete(context.Background(), "custom.key", "global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.delCalls != 1 {
		t.Fatalf("expected 1 Delete call, got %d", repo.delCalls)
	}

	// Verify the setting is gone.
	val, err := svc.Resolve(context.Background(), "custom.key", "global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "" {
		t.Fatalf("expected empty string after delete, got %q", val)
	}
}

func TestListByScope(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put("key1", "global", "val1")
	repo.put("key2", "global", "val2")
	repo.put("key3", "project:abc", "val3")
	svc := NewSettingsService(repo)

	results, err := svc.ListByScope(context.Background(), "global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 settings, got %d", len(results))
	}
}

func TestScopeFormats(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	scopes := []string{
		"global",
		"org:" + uuid.New().String(),
		"user:" + uuid.New().String(),
		"project:" + uuid.New().String(),
	}

	for _, scope := range scopes {
		err := svc.Set(context.Background(), "test.key", "value", scope, nil)
		if err != nil {
			t.Fatalf("Set failed for scope %q: %v", scope, err)
		}

		val, err := svc.Resolve(context.Background(), "test.key", scope)
		if err != nil {
			t.Fatalf("Resolve failed for scope %q: %v", scope, err)
		}
		if val != "value" {
			t.Fatalf("expected %q for scope %q, got %q", "value", scope, val)
		}
	}
}

func TestResolveFloatErrorOnEmpty(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	_, err := svc.ResolveFloat(context.Background(), "nonexistent.key", "global")
	if err == nil {
		t.Fatal("expected error for empty float resolve")
	}
}

func TestResolveIntErrorOnEmpty(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)

	_, err := svc.ResolveInt(context.Background(), "nonexistent.key", "global")
	if err == nil {
		t.Fatal("expected error for empty int resolve")
	}
}

func TestResolveCachesRepeatedReads(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put("cache.key", "global", "value")
	svc := NewSettingsService(repo)

	for i := 0; i < 5; i++ {
		v, err := svc.Resolve(context.Background(), "cache.key", "global")
		if err != nil || v != "value" {
			t.Fatalf("iteration %d: got (%q, %v)", i, v, err)
		}
	}
	if repo.getCalls != 1 {
		t.Errorf("expected 1 repo.Get call, got %d", repo.getCalls)
	}
}

func TestResolveCachesMissPath(t *testing.T) {
	// Built-in defaults must also be cached so callers polling for the
	// fallback value don't pay sql.ErrNoRows on every iteration.
	repo := newMockSettingsRepo()
	svc := NewSettingsService(repo)
	for i := 0; i < 3; i++ {
		_, _ = svc.Resolve(context.Background(), SettingRankWeightSim, "global")
	}
	if repo.getCalls != 1 {
		t.Errorf("expected 1 repo.Get call (default-fallback path cached), got %d", repo.getCalls)
	}
}

func TestSetInvalidatesCache(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put("invalidate.key", "global", "first")
	svc := NewSettingsService(repo)

	v, _ := svc.Resolve(context.Background(), "invalidate.key", "global")
	if v != "first" {
		t.Fatalf("expected 'first', got %q", v)
	}
	// Set must drop the cached entry so the next Resolve sees the new value.
	if err := svc.Set(context.Background(), "invalidate.key", "second", "global", nil); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	v, _ = svc.Resolve(context.Background(), "invalidate.key", "global")
	if v != "second" {
		t.Errorf("expected 'second' after Set, got %q (cache not invalidated)", v)
	}
}

func TestDeleteInvalidatesCache(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put("delete.key", "global", "value")
	svc := NewSettingsService(repo)

	v, _ := svc.Resolve(context.Background(), "delete.key", "global")
	if v != "value" {
		t.Fatalf("expected 'value', got %q", v)
	}
	if err := svc.Delete(context.Background(), "delete.key", "global"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	v, _ = svc.Resolve(context.Background(), "delete.key", "global")
	if v != "" {
		t.Errorf("expected empty after Delete, got %q (cache not invalidated)", v)
	}
}
