package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// testSQLiteDBWithMigrations opens a SQLite in-memory DB and runs all migrations.
func testSQLiteDBWithMigrations(t *testing.T) storage.DB {
	t.Helper()
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("create migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return db
}

// newOllamaTestServer creates a mock Ollama HTTP server.
func newOllamaTestServer(t *testing.T, models []provider.OllamaModel) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": models,
			})
		case "/api/pull":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
			})
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestListOllamaModels(t *testing.T) {
	now := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	srv := newOllamaTestServer(t, []provider.OllamaModel{
		{Name: "llama3:latest", Size: 4700000000, ModifiedAt: now},
		{Name: "mistral:7b", Size: 3800000000, ModifiedAt: now},
	})
	defer srv.Close()

	store := NewProviderAdminStore(ProviderAdminDeps{})
	models, err := store.ListOllamaModels(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatalf("ListOllamaModels: %v", err)
	}

	if len(models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(models))
	}
	if models[0].Name != "llama3:latest" {
		t.Errorf("expected llama3:latest, got %q", models[0].Name)
	}
	if models[1].Name != "mistral:7b" {
		t.Errorf("expected mistral:7b, got %q", models[1].Name)
	}
	if models[0].Size != 4700000000 {
		t.Errorf("expected size 4700000000, got %d", models[0].Size)
	}
}

func TestListOllamaModelsEmpty(t *testing.T) {
	srv := newOllamaTestServer(t, []provider.OllamaModel{})
	defer srv.Close()

	store := NewProviderAdminStore(ProviderAdminDeps{})
	models, err := store.ListOllamaModels(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatalf("ListOllamaModels: %v", err)
	}

	if len(models) != 0 {
		t.Fatalf("expected 0 models, got %d", len(models))
	}
}

// newOpenAIModelsServer serves GET /v1/models with the given ids and records the
// Authorization header and an X-Proxy header it received, for credential-fallback
// assertions.
func newOpenAIModelsServer(t *testing.T, ids []string, gotAuth, gotProxy *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if gotAuth != nil {
			*gotAuth = r.Header.Get("Authorization")
		}
		if gotProxy != nil {
			*gotProxy = r.Header.Get("X-Proxy")
		}
		data := make([]map[string]string, len(ids))
		for i, id := range ids {
			data[i] = map[string]string{"id": id}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestListProviderModels(t *testing.T) {
	srv := newOpenAIModelsServer(t, []string{"Qwen/Qwen3-8B"}, nil, nil)

	store := NewProviderAdminStore(ProviderAdminDeps{})
	models, err := store.ListProviderModels(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatalf("ListProviderModels: %v", err)
	}
	if len(models) != 1 || models[0] != "Qwen/Qwen3-8B" {
		t.Fatalf("expected [Qwen/Qwen3-8B], got %v", models)
	}
}

func TestAutodetectServedModel(t *testing.T) {
	store := NewProviderAdminStore(ProviderAdminDeps{})

	t.Run("single model fills blank", func(t *testing.T) {
		srv := newOpenAIModelsServer(t, []string{"/srv/Qwen3-Embedding-0.6B"}, nil, nil)
		cfg := &api.ProviderSlotConfig{Type: "sglang", URL: srv.URL, Model: ""}
		store.autodetectServedModel(context.Background(), cfg)
		if cfg.Model != "/srv/Qwen3-Embedding-0.6B" {
			t.Errorf("Model = %q, want the served id", cfg.Model)
		}
	})

	t.Run("single model replaces mismatch", func(t *testing.T) {
		srv := newOpenAIModelsServer(t, []string{"/srv/Qwen3-Embedding-0.6B"}, nil, nil)
		cfg := &api.ProviderSlotConfig{Type: "sglang", URL: srv.URL, Model: "qwen3-embedding:0.6b"}
		store.autodetectServedModel(context.Background(), cfg)
		if cfg.Model != "/srv/Qwen3-Embedding-0.6B" {
			t.Errorf("Model = %q, want the served id (stale Ollama tag should be replaced)", cfg.Model)
		}
	})

	t.Run("single model leaves matching value", func(t *testing.T) {
		srv := newOpenAIModelsServer(t, []string{"Qwen/Qwen3-8B"}, nil, nil)
		cfg := &api.ProviderSlotConfig{Type: "vllm", URL: srv.URL, Model: "Qwen/Qwen3-8B"}
		store.autodetectServedModel(context.Background(), cfg)
		if cfg.Model != "Qwen/Qwen3-8B" {
			t.Errorf("Model = %q, want unchanged", cfg.Model)
		}
	})

	t.Run("multi model never clobbers", func(t *testing.T) {
		srv := newOpenAIModelsServer(t, []string{"gpt-4o", "gpt-4o-mini", "text-embedding-3-small"}, nil, nil)
		cfg := &api.ProviderSlotConfig{Type: "openai", URL: srv.URL, Model: "custom-choice"}
		store.autodetectServedModel(context.Background(), cfg)
		if cfg.Model != "custom-choice" {
			t.Errorf("Model = %q, want unchanged on a multi-model endpoint", cfg.Model)
		}
	})

	t.Run("unreachable endpoint leaves model", func(t *testing.T) {
		cfg := &api.ProviderSlotConfig{Type: "sglang", URL: "http://127.0.0.1:1", Model: "keep-me"}
		store.autodetectServedModel(context.Background(), cfg)
		if cfg.Model != "keep-me" {
			t.Errorf("Model = %q, want unchanged when detection fails", cfg.Model)
		}
	})
}

func TestListProviderModelsCredentialFallback(t *testing.T) {
	var gotAuth, gotProxy string
	srv := newOpenAIModelsServer(t, []string{"Qwen/Qwen3-8B"}, &gotAuth, &gotProxy)

	// A saved slot at the same URL carries the API key and proxy header; with no
	// forwarded form headers, ListProviderModels must borrow both.
	reg, err := provider.NewRegistry(provider.RegistryConfig{
		Fact: provider.SlotConfig{
			Type:          "vllm",
			BaseURL:       srv.URL,
			APIKey:        "sk-saved",
			Model:         "Qwen/Qwen3-8B",
			CustomHeaders: map[string]string{"X-Proxy": "saved"},
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	store := NewProviderAdminStore(ProviderAdminDeps{Registry: reg})
	if _, err := store.ListProviderModels(context.Background(), srv.URL, nil); err != nil {
		t.Fatalf("ListProviderModels: %v", err)
	}
	if gotAuth != "Bearer sk-saved" {
		t.Errorf("Authorization=%q, want %q", gotAuth, "Bearer sk-saved")
	}
	if gotProxy != "saved" {
		t.Errorf("X-Proxy=%q, want %q", gotProxy, "saved")
	}
}

func TestResolveOllamaURLExplicitOverride(t *testing.T) {
	store := NewProviderAdminStore(ProviderAdminDeps{})
	url := store.resolveOllamaURL("http://custom:9999")
	if url != "http://custom:9999" {
		t.Errorf("expected http://custom:9999, got %q", url)
	}
}

func TestResolveOllamaURLStripsV1Suffix(t *testing.T) {
	store := NewProviderAdminStore(ProviderAdminDeps{})
	url := store.resolveOllamaURL("http://localhost:11434/v1")
	if url != "http://localhost:11434" {
		t.Errorf("expected http://localhost:11434, got %q", url)
	}
	url = store.resolveOllamaURL("http://localhost:11434/v1/")
	if url != "http://localhost:11434" {
		t.Errorf("expected http://localhost:11434, got %q", url)
	}
}

func TestResolveOllamaURLDefaultFallback(t *testing.T) {
	store := NewProviderAdminStore(ProviderAdminDeps{})
	url := store.resolveOllamaURL("")
	if url != "http://localhost:11434" {
		t.Errorf("expected http://localhost:11434, got %q", url)
	}
}

func TestResolveOllamaURLFromRegistryPort(t *testing.T) {
	reg, err := provider.NewRegistry(provider.RegistryConfig{
		Fact: provider.SlotConfig{
			Type:    "openai",
			BaseURL: "http://myollama:11434",
			APIKey:  "test",
			Model:   "llama3",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	store := NewProviderAdminStore(ProviderAdminDeps{Registry: reg})
	url := store.resolveOllamaURL("")
	if url != "http://myollama:11434" {
		t.Errorf("expected http://myollama:11434, got %q", url)
	}
}

func TestPullOllamaModel(t *testing.T) {
	srv := newOllamaTestServer(t, nil)
	defer srv.Close()

	store := NewProviderAdminStore(ProviderAdminDeps{})
	err := store.PullOllamaModel(context.Background(), "llama3:latest", srv.URL, nil)
	if err != nil {
		t.Fatalf("PullOllamaModel: %v", err)
	}
}

func TestUpdateProviderSlotTriggersReload(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)

	reg, err := provider.NewRegistry(provider.RegistryConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	if reg.IsConfigured() {
		t.Fatal("expected registry not configured initially")
	}

	store := NewProviderAdminStore(ProviderAdminDeps{Registry: reg, SettingsRepo: settingsRepo})

	_, err = store.UpdateProviderSlot(context.Background(), "embedding", api.ProviderSlotConfig{
		Type:   "openai",
		URL:    "https://api.openai.com",
		APIKey: "sk-test",
		Model:  "text-embedding-3-small",
	}, api.UpdateProviderSlotOpts{})
	if err != nil {
		t.Fatalf("UpdateProviderSlot: %v", err)
	}

	if !reg.IsConfigured() {
		t.Error("expected registry to be configured after hot-reload")
	}

	cfg := reg.GetConfig()
	if cfg.Embedding.Type != "openai" {
		t.Errorf("expected embedding type openai, got %q", cfg.Embedding.Type)
	}
	if cfg.Embedding.BaseURL != "https://api.openai.com" {
		t.Errorf("expected base URL https://api.openai.com, got %q", cfg.Embedding.BaseURL)
	}
	if cfg.Embedding.Model != "text-embedding-3-small" {
		t.Errorf("expected model text-embedding-3-small, got %q", cfg.Embedding.Model)
	}
}

func TestUpdateProviderSlotNilRegistryNoError(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)

	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})

	_, err := store.UpdateProviderSlot(context.Background(), "fact", api.ProviderSlotConfig{
		Type:   "openai",
		URL:    "https://api.openai.com",
		APIKey: "sk-test",
		Model:  "gpt-4",
	}, api.UpdateProviderSlotOpts{})
	if err != nil {
		t.Fatalf("UpdateProviderSlot with nil registry: %v", err)
	}
}

// readStoredSlot decodes the persisted provider config for a slot.
func readStoredSlot(t *testing.T, repo *storage.SettingsRepo, slot string) api.ProviderSlotConfig {
	t.Helper()
	setting, err := repo.Get(context.Background(), "provider."+slot, "global")
	if err != nil {
		t.Fatalf("get setting provider.%s: %v", slot, err)
	}
	var cfg api.ProviderSlotConfig
	if err := json.Unmarshal(setting.Value, &cfg); err != nil {
		t.Fatalf("unmarshal provider.%s: %v", slot, err)
	}
	return cfg
}

func TestUpdateProviderSlotPreservesAPIKeyOnBlank(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	// Seed with a key.
	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "openai", URL: "https://api.openai.com", APIKey: "sk-secret", Model: "gpt-4",
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	// Edit another field, leaving the key blank: it must be preserved.
	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "openai", URL: "https://proxy.example.com", APIKey: "", Model: "gpt-4",
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("edit update: %v", err)
	}

	got := readStoredSlot(t, settingsRepo, "fact")
	if got.APIKey != "sk-secret" {
		t.Errorf("api_key = %q, want sk-secret (preserved on blank)", got.APIKey)
	}
	if got.URL != "https://proxy.example.com" {
		t.Errorf("url = %q, want updated", got.URL)
	}
}

func TestUpdateProviderSlotClearAPIKey(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "openai", URL: "https://api.openai.com", APIKey: "sk-secret", Model: "gpt-4",
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "openai", URL: "https://api.openai.com", APIKey: "", Model: "gpt-4",
	}, api.UpdateProviderSlotOpts{ClearAPIKey: true}); err != nil {
		t.Fatalf("clear update: %v", err)
	}

	if got := readStoredSlot(t, settingsRepo, "fact"); got.APIKey != "" {
		t.Errorf("api_key = %q, want empty after ClearAPIKey", got.APIKey)
	}
}

func TestUpdateProviderSlotCustomHeadersMergeAndRemove(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "openai", URL: "https://api.openai.com", APIKey: "sk", Model: "gpt-4",
		CustomHeaders: map[string]string{"X-A": "1", "X-B": "2"},
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	// New full set: keep X-A (blank => preserve), drop X-B (absent), add X-C.
	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "openai", URL: "https://api.openai.com", APIKey: "sk", Model: "gpt-4",
		CustomHeaders: map[string]string{"X-A": "", "X-C": "3"},
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("edit update: %v", err)
	}

	got := readStoredSlot(t, settingsRepo, "fact").CustomHeaders
	if got["X-A"] != "1" {
		t.Errorf("X-A = %q, want 1 (preserved on blank)", got["X-A"])
	}
	if _, ok := got["X-B"]; ok {
		t.Errorf("X-B should be removed (absent from new set), got %q", got["X-B"])
	}
	if got["X-C"] != "3" {
		t.Errorf("X-C = %q, want 3 (added)", got["X-C"])
	}
}

func TestGetProviderConfigExposesHeaderKeysAndAPIKeySet(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "openai", URL: "https://api.openai.com", APIKey: "sk-secret", Model: "gpt-4",
		CustomHeaders: map[string]string{"X-Beta": "b", "X-Alpha": "a"},
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	resp, err := store.GetProviderConfig(ctx)
	if err != nil {
		t.Fatalf("GetProviderConfig: %v", err)
	}
	var fact *api.ProviderSlotStatus
	for i := range resp {
		if resp[i].Slot == "fact" {
			fact = &resp[i]
		}
	}
	if fact == nil {
		t.Fatal("fact slot not in response")
	}
	if !fact.APIKeySet {
		t.Error("APIKeySet = false, want true")
	}
	want := []string{"X-Alpha", "X-Beta"} // sorted
	if len(fact.CustomHeaderKeys) != 2 || fact.CustomHeaderKeys[0] != want[0] || fact.CustomHeaderKeys[1] != want[1] {
		t.Errorf("CustomHeaderKeys = %v, want %v (sorted, names only)", fact.CustomHeaderKeys, want)
	}
}

func TestMigrateProviderTypesRewritesCustom(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	// Seed a slot with the legacy "custom" type (UpdateProviderSlot persists the
	// type verbatim; normalization happens on read/migrate).
	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "custom", URL: "http://localhost:8000", Model: "m",
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("seed update: %v", err)
	}
	if got := readStoredSlot(t, settingsRepo, "fact").Type; got != "custom" {
		t.Fatalf("seed stored type = %q, want custom", got)
	}

	n, err := MigrateProviderTypes(ctx, settingsRepo)
	if err != nil {
		t.Fatalf("MigrateProviderTypes: %v", err)
	}
	if n != 1 {
		t.Errorf("migrated count = %d, want 1", n)
	}
	if got := readStoredSlot(t, settingsRepo, "fact").Type; got != provider.ProviderTypeOpenAICompatible {
		t.Errorf("stored type = %q, want %q", got, provider.ProviderTypeOpenAICompatible)
	}

	// Idempotent: a second run rewrites nothing.
	if n2, err := MigrateProviderTypes(ctx, settingsRepo); err != nil {
		t.Fatalf("MigrateProviderTypes (2nd): %v", err)
	} else if n2 != 0 {
		t.Errorf("second migrate count = %d, want 0", n2)
	}
}

func TestUpdateProviderSlotRoundTripsExtraBody(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
		Type: "vllm", URL: "http://localhost:8000", Model: "Qwen/Qwen3-8B",
		ExtraBody: map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": true}},
	}, api.UpdateProviderSlotOpts{}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Persisted verbatim.
	stored := readStoredSlot(t, settingsRepo, "fact").ExtraBody
	ctk, ok := stored["chat_template_kwargs"].(map[string]any)
	if !ok || ctk["enable_thinking"] != true {
		t.Errorf("stored extra_body = %v, want chat_template_kwargs.enable_thinking=true", stored)
	}

	// Returned on the read path (not secret, unlike headers).
	resp, err := store.GetProviderConfig(ctx)
	if err != nil {
		t.Fatalf("GetProviderConfig: %v", err)
	}
	var fact *api.ProviderSlotStatus
	for i := range resp {
		if resp[i].Slot == "fact" {
			fact = &resp[i]
		}
	}
	if fact == nil || fact.ExtraBody == nil {
		t.Fatalf("fact slot missing extra_body in status: %+v", fact)
	}
}

func TestUpdateProviderSlotRoundTripsDisableThinking(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	cases := []struct {
		name string
		in   *bool
	}{
		{"explicit_false", ptr(false)},
		{"explicit_true", ptr(true)},
		{"unset", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
				Type: "vllm", URL: "http://localhost:8000", Model: "Qwen/Qwen3-8B",
				DisableThinking: tc.in,
			}, api.UpdateProviderSlotOpts{}); err != nil {
				t.Fatalf("update: %v", err)
			}

			stored := readStoredSlot(t, settingsRepo, "fact").DisableThinking
			switch {
			case tc.in == nil:
				if stored != nil {
					t.Errorf("unset must omit disable_thinking, stored = %v", *stored)
				}
			case stored == nil || *stored != *tc.in:
				t.Errorf("stored disable_thinking = %v, want %v", stored, *tc.in)
			}
		})
	}
}

func TestGetRegistryConfig(t *testing.T) {
	cfg := provider.RegistryConfig{
		Embedding: provider.SlotConfig{
			Type:    "openai",
			BaseURL: "https://api.openai.com",
			APIKey:  "sk-test",
			Model:   "text-embedding-3-small",
		},
	}
	reg, err := provider.NewRegistry(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	got := reg.GetConfig()
	if got.Embedding.Type != "openai" {
		t.Errorf("expected openai, got %q", got.Embedding.Type)
	}
	if got.Embedding.BaseURL != "https://api.openai.com" {
		t.Errorf("expected https://api.openai.com, got %q", got.Embedding.BaseURL)
	}
}
