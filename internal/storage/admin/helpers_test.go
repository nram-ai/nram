package admin

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/storage"
)

// setupAdminTestDB opens a fresh sqlite DB in a temp dir and runs migrations.
// Shared by analytics_store_test.go, usage_store_test.go, and any future
// admin-package test that needs an isolated migrated database.
func setupAdminTestDB(t *testing.T) storage.DB {
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

// insertTestNamespace creates a single org-kind namespace and returns its ID.
// Many child rows (token_usage, memories, projects) require namespace_id
// NOT NULL — this is the cheapest fixture that satisfies the FK.
func insertTestNamespace(t *testing.T, db storage.DB, ctx context.Context) uuid.UUID {
	t.Helper()
	nsID := uuid.New()
	_, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth) VALUES (?, ?, ?, ?, ?, ?)",
		nsID.String(), "test-org", "test-org", "org", "test-org", 0,
	)
	if err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	return nsID
}
