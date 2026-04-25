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
	t.Cleanup(func() { os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

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
			json.NewEncoder(w).Encode(map[string]interface{}{
				"models": models,
			})
		case "/api/pull":
			w.Header().Set("Content-Type", "application/x-ndjson")
			json.NewEncoder(w).Encode(map[string]interface{}{
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
	models, err := store.ListOllamaModels(context.Background(), srv.URL)
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
	models, err := store.ListOllamaModels(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("ListOllamaModels: %v", err)
	}

	if len(models) != 0 {
		t.Fatalf("expected 0 models, got %d", len(models))
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
	})
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
	err := store.PullOllamaModel(context.Background(), "llama3:latest", srv.URL)
	if err != nil {
		t.Fatalf("PullOllamaModel: %v", err)
	}
}

func TestUpdateProviderSlotTriggersReload(t *testing.T) {
	db := testSQLiteDBWithMigrations(t)
	settingsRepo := storage.NewSettingsRepo(db)

	reg, err := provider.NewRegistry(provider.RegistryConfig{})
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

func TestGetRegistryConfig(t *testing.T) {
	cfg := provider.RegistryConfig{
		Embedding: provider.SlotConfig{
			Type:    "openai",
			BaseURL: "https://api.openai.com",
			APIKey:  "sk-test",
			Model:   "text-embedding-3-small",
		},
	}
	reg, err := provider.NewRegistry(cfg)
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
