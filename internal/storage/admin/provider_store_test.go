package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
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
	reg, err := provider.NewRegistry(provider.RegistryConfig{Slots: map[string]provider.SlotConfig{
		provider.SlotFact: {
			Type:          "vllm",
			BaseURL:       srv.URL,
			APIKey:        "sk-saved",
			Model:         "Qwen/Qwen3-8B",
			CustomHeaders: map[string]string{"X-Proxy": "saved"},
		},
	}}, nil, nil)
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
	reg, err := provider.NewRegistry(provider.RegistryConfig{Slots: map[string]provider.SlotConfig{
		provider.SlotFact: {
			Type:    "openai",
			BaseURL: "http://myollama:11434",
			APIKey:  "test",
			Model:   "llama3",
		},
	}}, nil, nil)
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
	embedding := cfg.Slots[provider.SlotEmbedding]
	if embedding.Type != "openai" {
		t.Errorf("expected embedding type openai, got %q", embedding.Type)
	}
	if embedding.BaseURL != "https://api.openai.com" {
		t.Errorf("expected base URL https://api.openai.com, got %q", embedding.BaseURL)
	}
	if embedding.Model != "text-embedding-3-small" {
		t.Errorf("expected model text-embedding-3-small, got %q", embedding.Model)
	}
}

// TestUpdateProviderSlotModelValidationWarning exercises the full wired save path
// end to end: UpdateProviderSlot -> ListOpenAIModels (against a live fake
// /v1/models server) -> validateConfiguredModel -> warning on the returned result.
// A multi-model served list is used so auto-detect leaves the typed model in place
// (it only fills single-model endpoints), letting the validation step act.
func TestUpdateProviderSlotModelValidationWarning(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: settingsRepo})
	ctx := context.Background()

	t.Run("reachable host, unserved model warns and still saves", func(t *testing.T) {
		srv := newOpenAIModelsServer(t, []string{"gpt-4o", "gpt-4o-mini"}, nil, nil)
		res, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
			Type: "openai", URL: srv.URL, Model: "gpt-4o-typo",
		}, api.UpdateProviderSlotOpts{})
		if err != nil {
			t.Fatalf("UpdateProviderSlot: %v", err)
		}
		if res == nil || res.Warning == "" {
			t.Fatalf("expected a warning result, got %+v", res)
		}
		if !strings.Contains(res.Warning, "gpt-4o-typo") {
			t.Errorf("warning should name the model, got %q", res.Warning)
		}
		// The save still persisted despite the warning.
		if stored := readStoredSlot(t, settingsRepo, "fact"); stored.Model != "gpt-4o-typo" {
			t.Errorf("expected the slot to persist despite the warning, stored model = %q", stored.Model)
		}
	})

	t.Run("reachable host, served model does not warn", func(t *testing.T) {
		srv := newOpenAIModelsServer(t, []string{"gpt-4o", "gpt-4o-mini"}, nil, nil)
		res, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
			Type: "openai", URL: srv.URL, Model: "gpt-4o",
		}, api.UpdateProviderSlotOpts{})
		if err != nil {
			t.Fatalf("UpdateProviderSlot: %v", err)
		}
		if res != nil && res.Warning != "" {
			t.Errorf("expected no warning for a served model, got %q", res.Warning)
		}
	})

	t.Run("unreachable host does not warn", func(t *testing.T) {
		res, err := store.UpdateProviderSlot(ctx, "fact", api.ProviderSlotConfig{
			Type: "openai", URL: "http://127.0.0.1:1", Model: "anything",
		}, api.UpdateProviderSlotOpts{})
		if err != nil {
			t.Fatalf("UpdateProviderSlot: %v", err)
		}
		if res != nil && res.Warning != "" {
			t.Errorf("expected no warning when the host is unreachable, got %q", res.Warning)
		}
	})
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
	cfg := provider.RegistryConfig{Slots: map[string]provider.SlotConfig{
		provider.SlotEmbedding: {
			Type:    "openai",
			BaseURL: "https://api.openai.com",
			APIKey:  "sk-test",
			Model:   "text-embedding-3-small",
		},
	}}
	reg, err := provider.NewRegistry(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	got := reg.GetConfig()
	embedding := got.Slots[provider.SlotEmbedding]
	if embedding.Type != "openai" {
		t.Errorf("expected openai, got %q", embedding.Type)
	}
	if embedding.BaseURL != "https://api.openai.com" {
		t.Errorf("expected https://api.openai.com, got %q", embedding.BaseURL)
	}
}

// newJudgeRerankTestServer serves a chat endpoint that answers the calibration
// fixture, and 404s /v1/rerank so ProbeRerankMethod detects "judge" (the shape of
// any plain chat server). reply is called with the completion's max_tokens and the
// user message, and returns the content to answer with, so a test can model a
// model that only behaves above a certain token cap.
// lastChatTemperature records the temperature field of the most recent chat
// request the helper below served, so a test can assert the judge was driven at
// the value production resolves rather than the raw stored one. nil means the
// field was omitted, which is how the provider sends temperature 0.
var lastChatTemperature *float64

// newJudgeRerankTestServer serves POST /v1/chat/completions with reply(maxTokens,
// user) as the content and 404s every other path (so /v1/rerank makes the probe
// pick the judge path). An optional authorized predicate gates the chat endpoint
// with 401 when it fails, letting a test assert a masked secret actually reached
// the provider.
func newJudgeRerankTestServer(t *testing.T, reply func(maxTokens int, user string) string, authorized ...func(r *http.Request) bool) *httptest.Server {
	t.Helper()
	lastChatTemperature = nil
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r) // includes /v1/rerank -> probe says "judge"
			return
		}
		for _, ok := range authorized {
			if !ok(r) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		var body struct {
			MaxTokens int `json:"max_tokens"`
			// Temperature is a pointer so a test can tell "omitted" (nil) from an
			// explicitly sent value, including a deliberate 0 for greedy decoding.
			Temperature *float64 `json:"temperature"`
			Messages    []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode chat request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		lastChatTemperature = body.Temperature
		user := ""
		if len(body.Messages) > 1 {
			user = body.Messages[len(body.Messages)-1].Content
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "stub",
			"choices": []map[string]any{
				{"message": map[string]any{"content": reply(body.MaxTokens, user)}},
			},
			"usage": map[string]any{"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
		})
	}))
}

// newCalibrationStore builds a provider admin store wired the way production
// wires it (cmd/server/main.go), with a real cached SettingsService over a
// migrated DB.
func newCalibrationStore(t *testing.T) (*ProviderAdminStore, *service.SettingsService, *storage.SettingsRepo) {
	t.Helper()
	repo := storage.NewSettingsRepo(testSQLiteDBWithMigrations(t))
	settings := service.NewSettingsService(repo)
	return NewProviderAdminStore(ProviderAdminDeps{SettingsRepo: repo, Settings: settings}), settings, repo
}

// testReranker runs the reranker slot's Test against url/model.
func testReranker(t *testing.T, store *ProviderAdminStore, url, model string) *api.ProviderTestResult {
	t.Helper()
	res, err := store.TestProvider(context.Background(), api.ProviderTestRequest{
		Slot:   provider.SlotReranker,
		Config: api.ProviderSlotConfig{Type: provider.ProviderTypeSGLang, URL: url, Model: model},
	})
	if err != nil {
		t.Fatalf("TestProvider: %v", err)
	}
	return res
}

// judgeMaxTokens reads the cap back through the same cached resolver the live
// rerank stage uses (withRerankJudgeConfig), not the raw row. A calibration write
// that skipped the resolver's cache invalidation reads stale here, which is the
// operator-visible bug: "Saved 16 -> 32" next to a reranker still running at 16.
func judgeMaxTokens(t *testing.T, settings *service.SettingsService) int {
	t.Helper()
	return settings.ResolveIntWithDefault(context.Background(), service.SettingRerankJudgeMaxTokens, "global")
}

// TestTestProvider_JudgeCalibration drives the reranker Test end to end against a
// chat server. Before this, the reranker Test returned as soon as the method probe
// said "judge" and never invoked the model, so a judge that emitted nothing usable
// still reported success. Each case asserts the Test now reflects what the model
// actually did.
func TestTestProvider_JudgeCalibration(t *testing.T) {
	t.Run("healthy judge calibrates on the first rung and writes nothing", func(t *testing.T) {
		store, settings, _ := newCalibrationStore(t)

		var caps []int
		srv := newJudgeRerankTestServer(t, func(maxTokens int, user string) string {
			caps = append(caps, maxTokens)
			if strings.Contains(user, "Paris") {
				return "1.0"
			}
			return "0.0"
		})
		defer srv.Close()

		res := testReranker(t, store, srv.URL, "qwen")
		if !res.Success {
			t.Fatalf("Success = false, message %q", res.Message)
		}
		if res.RerankMethod != provider.RerankMethodJudge {
			t.Errorf("RerankMethod = %q, want judge", res.RerankMethod)
		}
		if res.Calibration == nil || !res.Calibration.Calibrated {
			t.Fatalf("Calibration = %+v, want calibrated", res.Calibration)
		}
		if !res.Calibration.DisableThinking {
			t.Error("DisableThinking = false, want the thinking-off rung to win")
		}
		if res.Calibration.RelevantScore <= res.Calibration.IrrelevantScore {
			t.Errorf("scores %v/%v do not discriminate",
				res.Calibration.RelevantScore, res.Calibration.IrrelevantScore)
		}
		// Only the first rung ran (2 docs), at the configured cap.
		if len(caps) != 2 || caps[0] != 16 {
			t.Errorf("completion caps = %v, want two calls at 16", caps)
		}
		// A winning cap equal to the stored one must not write the setting.
		if res.Calibration.MaxTokensApplied {
			t.Error("MaxTokensApplied = true, want no write when the cap is unchanged")
		}
		if got := judgeMaxTokens(t, settings); got != 16 {
			t.Errorf("judge max_tokens = %d, want 16 (untouched)", got)
		}
	})

	t.Run("cap too small to emit a number raises and saves max_tokens", func(t *testing.T) {
		store, settings, _ := newCalibrationStore(t)

		// Models the live failure: at 16 the reasoning trace eats the budget and
		// no number is ever emitted; with more room the model answers.
		srv := newJudgeRerankTestServer(t, func(maxTokens int, user string) string {
			if maxTokens < 32 {
				return "<think>Let me weigh whether this document"
			}
			if strings.Contains(user, "Paris") {
				return "0.95"
			}
			return "0.05"
		})
		defer srv.Close()

		// Prime the resolver cache with the pre-calibration value, so reading it
		// back below exercises the cache invalidation rather than a cold lookup.
		// This is what the live rerank stage would have cached.
		if got := judgeMaxTokens(t, settings); got != 16 {
			t.Fatalf("pre-calibration max_tokens = %d, want the registered default 16", got)
		}

		res := testReranker(t, store, srv.URL, "qwen")
		if !res.Success || res.Calibration == nil || !res.Calibration.Calibrated {
			t.Fatalf("want a calibrated success, got success=%v cal=%+v msg=%q",
				res.Success, res.Calibration, res.Message)
		}
		if res.Calibration.MaxTokens != judgeCalibrationTokenFloor {
			t.Errorf("MaxTokens = %d, want the floor %d", res.Calibration.MaxTokens, judgeCalibrationTokenFloor)
		}
		if !res.Calibration.MaxTokensApplied {
			t.Error("MaxTokensApplied = false, want the raised cap written")
		}
		// Through the cached resolver the reranker itself reads: a write that
		// skipped invalidation would still report 16 here for the cache TTL (~30s),
		// leaving the operator a green "Saved 16 -> 32" beside a judge still failing
		// at 16.
		if got := judgeMaxTokens(t, settings); got != judgeCalibrationTokenFloor {
			t.Errorf("judge max_tokens via the cached resolver = %d, want %d (stale cache: the write did not invalidate)",
				got, judgeCalibrationTokenFloor)
		}
	})

	t.Run("cross-encoder driven as a judge fails with a diagnosis", func(t *testing.T) {
		store, settings, _ := newCalibrationStore(t)

		// A cross-encoder on a chat endpoint: token noise, never a number.
		srv := newJudgeRerankTestServer(t, func(int, string) string {
			return "query passage relevance yes yes"
		})
		defer srv.Close()

		res := testReranker(t, store, srv.URL, "bge-reranker")
		if res.Success {
			t.Fatal("Success = true for a model that never emitted a number")
		}
		if res.Calibration == nil || res.Calibration.Calibrated {
			t.Fatalf("Calibration = %+v, want an uncalibrated result", res.Calibration)
		}
		if !strings.Contains(res.Calibration.Diagnosis, "cross-encoder") {
			t.Errorf("diagnosis = %q, want it to name the cross-encoder mis-detection",
				res.Calibration.Diagnosis)
		}
		if res.Calibration.LastOutput == "" {
			t.Error("LastOutput empty, want the raw completion echoed")
		}
		// A failed calibration must not write the setting.
		if got := judgeMaxTokens(t, settings); got != 16 {
			t.Errorf("judge max_tokens = %d, want 16 (untouched by a failed calibration)", got)
		}
	})

	t.Run("flat scorer fails without a write", func(t *testing.T) {
		store, settings, _ := newCalibrationStore(t)

		srv := newJudgeRerankTestServer(t, func(int, string) string { return "1.0" })
		defer srv.Close()

		res := testReranker(t, store, srv.URL, "m")
		if res.Success {
			t.Fatal("Success = true for a model that scores every candidate alike")
		}
		if res.Calibration == nil || !strings.Contains(res.Calibration.Diagnosis, "discriminate") {
			t.Fatalf("Calibration = %+v, want a no-discrimination diagnosis", res.Calibration)
		}
		if got := judgeMaxTokens(t, settings); got != 16 {
			t.Errorf("judge max_tokens = %d, want 16 (untouched)", got)
		}
	})

	// The calibration must drive the judge with the knobs the live rerank stage
	// resolves, not the raw stored row, or it certifies a configuration production
	// never runs — the exact class of silent mismatch this feature exists to end.
	// Temperature is the one knob production range-clamps (rerank_stage.go's
	// ResolveFloatInRange(...,0,1,0)), so an out-of-range row is the test for it.
	t.Run("out-of-range temperature is clamped to the value production uses", func(t *testing.T) {
		store, _, repo := newCalibrationStore(t)

		if err := repo.Set(context.Background(), &model.Setting{
			Key:   service.SettingRerankJudgeTemperature,
			Value: json.RawMessage(`5`),
			Scope: "global",
		}); err != nil {
			t.Fatalf("seed temperature: %v", err)
		}

		srv := newJudgeRerankTestServer(t, func(_ int, user string) string {
			if strings.Contains(user, "Paris") {
				return "1.0"
			}
			return "0.0"
		})
		defer srv.Close()

		res := testReranker(t, store, srv.URL, "qwen")
		if !res.Success {
			t.Fatalf("Success = false: %q", res.Message)
		}
		// Production clamps 5 to the 0 fallback and now sends that 0 explicitly
		// (greedy decoding). Reading the row raw would have sent 5.
		if lastChatTemperature == nil {
			t.Error("judge temperature omitted; want production's clamped 0 sent explicitly")
		} else if *lastChatTemperature != 0 {
			t.Errorf("judge driven at temperature %v, want production's clamped 0", *lastChatTemperature)
		}
	})

	t.Run("cross-encoder endpoint skips calibration entirely", func(t *testing.T) {
		store, _, _ := newCalibrationStore(t)

		// Answers /v1/rerank, so the probe detects cross_encoder and the judge is
		// never built. A chat call here would be a bug.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/chat/completions" {
				t.Error("cross-encoder test called the chat endpoint")
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{"index": 0, "relevance_score": 0.9}},
			})
		}))
		defer srv.Close()

		res := testReranker(t, store, srv.URL, "bge")
		if !res.Success {
			t.Fatalf("Success = false: %q", res.Message)
		}
		if res.RerankMethod != provider.RerankMethodCrossEncoder {
			t.Errorf("RerankMethod = %q, want cross_encoder", res.RerankMethod)
		}
		if res.Calibration != nil {
			t.Errorf("Calibration = %+v, want none for a cross-encoder", res.Calibration)
		}
	})
}

// TestUpdateProviderSlot_CrossEncoderDropsThinking pins the invariant at the layer
// that owns it: only the generative judge honors the thinking toggle, because
// createRerankProvider builds the cross-encoder without DisableThinking, so a
// value stored on a cross-encoder slot is dead config nothing emits. The console
// hides the control for a cross-encoder, but it decides that from the last method
// it saw — re-point a judge slot at a cross-encoder and save without testing and
// the client happily sends the stale value. The server normalizes on the method it
// just probed, so it holds for every client.
func TestUpdateProviderSlot_CrossEncoderDropsThinking(t *testing.T) {
	ctx := context.Background()
	disable := true

	t.Run("cross-encoder save drops the dead toggle", func(t *testing.T) {
		store, _, repo := newCalibrationStore(t)
		// Answers /v1/rerank, so the probe detects cross_encoder.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{"index": 0, "relevance_score": 0.9}},
			})
		}))
		defer srv.Close()

		if _, err := store.UpdateProviderSlot(ctx, provider.SlotReranker, api.ProviderSlotConfig{
			Type: provider.ProviderTypeSGLang, URL: srv.URL, Model: "bge",
			DisableThinking: &disable, // what a stale client sends
		}, api.UpdateProviderSlotOpts{}); err != nil {
			t.Fatalf("UpdateProviderSlot: %v", err)
		}

		row, err := repo.Get(ctx, "provider."+provider.SlotReranker, "global")
		if err != nil || row == nil {
			t.Fatalf("read back slot: %v", err)
		}
		var stored api.ProviderSlotConfig
		if err := json.Unmarshal(row.Value, &stored); err != nil {
			t.Fatalf("decode slot: %v", err)
		}
		if stored.RerankMethod != provider.RerankMethodCrossEncoder {
			t.Fatalf("RerankMethod = %q, want cross_encoder", stored.RerankMethod)
		}
		if stored.DisableThinking != nil {
			t.Errorf("stored disable_thinking = %v on a cross_encoder, want it dropped as dead config",
				*stored.DisableThinking)
		}
		if strings.Contains(string(row.Value), "disable_thinking") {
			t.Errorf("row still carries the dead key: %s", row.Value)
		}
	})

	t.Run("judge save keeps the toggle", func(t *testing.T) {
		store, _, repo := newCalibrationStore(t)
		// 404s /v1/rerank, so the probe detects judge.
		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()

		if _, err := store.UpdateProviderSlot(ctx, provider.SlotReranker, api.ProviderSlotConfig{
			Type: provider.ProviderTypeSGLang, URL: srv.URL, Model: "qwen",
			DisableThinking: &disable,
		}, api.UpdateProviderSlotOpts{}); err != nil {
			t.Fatalf("UpdateProviderSlot: %v", err)
		}

		row, err := repo.Get(ctx, "provider."+provider.SlotReranker, "global")
		if err != nil || row == nil {
			t.Fatalf("read back slot: %v", err)
		}
		var stored api.ProviderSlotConfig
		if err := json.Unmarshal(row.Value, &stored); err != nil {
			t.Fatalf("decode slot: %v", err)
		}
		if stored.RerankMethod != provider.RerankMethodJudge {
			t.Fatalf("RerankMethod = %q, want judge", stored.RerankMethod)
		}
		if stored.DisableThinking == nil || !*stored.DisableThinking {
			t.Errorf("stored disable_thinking = %v, want true kept for a judge", stored.DisableThinking)
		}
	})
}

// TestJudgeCalibrationLadder pins the sweep order: thinking off at the configured
// cap first (the resting state of every slot, so a healthy judge calibrates in one
// attempt), then a raised cap, and thinking-on last and only at the ceiling, since
// a reasoning trace never fits a cap sized for one number.
func TestJudgeCalibrationLadder(t *testing.T) {
	// Resolve the setting's registered Max by a path independent of
	// judgeCalibrationCeiling(). Expressing the expectation in terms of that
	// function would be tautological: a maxForKey that silently missed would fall
	// back to the floor and both sides would agree, which is precisely the
	// stranding the schema lookup exists to prevent.
	var schemaMax float64
	for _, sc := range settingsSchemas {
		if sc.Key == service.SettingRerankJudgeMaxTokens {
			if sc.Max == nil {
				t.Fatalf("%s has no registered Max; the ladder ceiling has nothing to track",
					service.SettingRerankJudgeMaxTokens)
			}
			schemaMax = *sc.Max
		}
	}
	if schemaMax == 0 {
		t.Fatalf("%s is not in settingsSchemas", service.SettingRerankJudgeMaxTokens)
	}
	if ceiling := judgeCalibrationCeiling(); float64(ceiling) != schemaMax {
		t.Errorf("ceiling = %d, want the registered Max %v (maxForKey missed and fell back)", ceiling, schemaMax)
	}

	got := judgeCalibrationLadder(16)
	want := []provider.JudgeCalibrationCandidate{
		{DisableThinking: true, MaxTokens: 16},
		{DisableThinking: true, MaxTokens: judgeCalibrationTokenFloor},
		{DisableThinking: false, MaxTokens: int(schemaMax)},
	}
	if len(got) != len(want) {
		t.Fatalf("ladder = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("rung %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// A cap already at or above the floor does not get a duplicate rung.
	if got := judgeCalibrationLadder(64); len(got) != 2 || got[0].MaxTokens != 64 {
		t.Errorf("ladder(64) = %+v, want the configured rung then thinking-on", got)
	}

	// An unset/invalid cap falls back to the registered default.
	if got := judgeCalibrationLadder(0); got[0].MaxTokens != service.GetDefaultInt(service.SettingRerankJudgeMaxTokens) {
		t.Errorf("ladder(0) first rung = %+v, want the registered default cap", got[0])
	}
}

// persistSlot writes a slot config to the settings repo under the same key/scope
// loadPersistedSlot reads (provider.<slot>, global), so a subsequent Test resolves
// it as the stored config.
func persistSlot(t *testing.T, repo *storage.SettingsRepo, slot string, cfg api.ProviderSlotConfig) {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal slot %q: %v", slot, err)
	}
	if err := repo.Set(context.Background(), &model.Setting{
		Key:   "provider." + slot,
		Value: json.RawMessage(raw),
		Scope: "global",
	}); err != nil {
		t.Fatalf("persist slot %q: %v", slot, err)
	}
}

// newGatedChatServer is a chat-completions stub that gates the endpoint on
// authorized(r), for asserting a masked secret (api_key or custom header) reaches
// the provider. It delegates to newJudgeRerankTestServer with a constant reply;
// the LLM slot tests exercise only the chat endpoint, not the rerank probe.
func newGatedChatServer(t *testing.T, authorized func(r *http.Request) bool) *httptest.Server {
	t.Helper()
	srv := newJudgeRerankTestServer(t, func(int, string) string { return "ok" }, authorized)
	t.Cleanup(srv.Close)
	return srv
}

// TestTestProvider_InheritsSavedSecrets is the fail-before / pass-after guard for
// the masked-secret drop: the Providers page re-tests a saved slot by sending only
// type/url/model, so a slot whose endpoint requires a key or a custom auth header
// used to fail its own Test for want of a credential it can never post. TestProvider
// now inherits the stored api_key and custom_headers first.
func TestTestProvider_InheritsSavedSecrets(t *testing.T) {
	// The cases differ only in which masked field carries the credential and how
	// the server gates on it; the flow (persist, test without the secret, expect
	// success) is shared.
	cases := []struct {
		name       string
		saved      api.ProviderSlotConfig // secret field(s) only; type/url/model filled per run
		authorized func(r *http.Request) bool
	}{
		{
			name:       "stored api_key reaches the provider",
			saved:      api.ProviderSlotConfig{APIKey: "sekret"},
			authorized: func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer sekret" },
		},
		{
			name:       "stored custom header reaches the provider",
			saved:      api.ProviderSlotConfig{CustomHeaders: map[string]string{"X-Proxy-Auth": "gate-pass"}},
			authorized: func(r *http.Request) bool { return r.Header.Get("X-Proxy-Auth") == "gate-pass" },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, _, repo := newCalibrationStore(t)
			srv := newGatedChatServer(t, tc.authorized)

			saved := tc.saved
			saved.Type, saved.URL, saved.Model = provider.ProviderTypeSGLang, srv.URL, "qwen"
			persistSlot(t, repo, provider.SlotFact, saved)

			// Mirror the Providers page: only type/url/model, no secret.
			res, err := store.TestProvider(context.Background(), api.ProviderTestRequest{
				Slot:   provider.SlotFact,
				Config: api.ProviderSlotConfig{Type: provider.ProviderTypeSGLang, URL: srv.URL, Model: "qwen"},
			})
			if err != nil {
				t.Fatalf("TestProvider: %v", err)
			}
			if !res.Success {
				t.Fatalf("Success = false, message %q; the stored secret was not inherited", res.Message)
			}
		})
	}
}

// TestTestProvider_JudgeInheritsSavedAPIKey proves the inherited key flows through
// the reranker judge path (probe + real calibration completions), not just the
// generic LLM path. The server has no /v1/rerank route (probe -> judge) and gates
// the chat endpoint on the bearer, so calibration only succeeds if the stored key
// was inherited.
func TestTestProvider_JudgeInheritsSavedAPIKey(t *testing.T) {
	store, _, repo := newCalibrationStore(t)
	// A discriminating judge (1.0 for the relevant fixture, else 0.0) gated on the
	// bearer: calibration only succeeds if the stored key was inherited into the
	// judge completions.
	srv := newJudgeRerankTestServer(t,
		func(_ int, user string) string {
			if strings.Contains(user, "Paris") {
				return "1.0"
			}
			return "0.0"
		},
		func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer sekret" },
	)
	defer srv.Close()

	persistSlot(t, repo, provider.SlotReranker, api.ProviderSlotConfig{
		Type: provider.ProviderTypeSGLang, URL: srv.URL, Model: "qwen", APIKey: "sekret",
	})

	// Mirror the Providers page: no api_key in the request.
	res, err := store.TestProvider(context.Background(), api.ProviderTestRequest{
		Slot:   provider.SlotReranker,
		Config: api.ProviderSlotConfig{Type: provider.ProviderTypeSGLang, URL: srv.URL, Model: "qwen"},
	})
	if err != nil {
		t.Fatalf("TestProvider: %v", err)
	}
	if !res.Success {
		t.Fatalf("Success = false, message %q; the judge path did not inherit the stored api_key", res.Message)
	}
	if res.Calibration == nil || !res.Calibration.Calibrated {
		t.Fatalf("Calibration = %+v, want calibrated", res.Calibration)
	}
}

// TestRerankProbeConfig_CarriesTimeout guards the field the probe now depends on:
// a configured per-slot timeout must survive into the SlotConfig the probe reads.
func TestRerankProbeConfig_CarriesTimeout(t *testing.T) {
	timeout := 45
	pc := rerankProbeConfig(api.ProviderSlotConfig{
		URL: "http://example", Model: "m", Timeout: &timeout,
	})
	if pc.Timeout != 45 {
		t.Errorf("Timeout = %d, want 45", pc.Timeout)
	}

	// A nil timeout leaves the probe default (0) in place.
	pc = rerankProbeConfig(api.ProviderSlotConfig{URL: "http://example", Model: "m"})
	if pc.Timeout != 0 {
		t.Errorf("Timeout = %d, want 0 for an unset timeout", pc.Timeout)
	}
}
