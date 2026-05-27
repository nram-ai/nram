package admin

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
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
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth) VALUES (?, ?, ?, ?, ?, ?)",
		nsID.String(), "test-org", "test-org", "org", "test-org", 0,
	)
	return nsID
}

// setupAdminTestPostgres opens a fresh per-test Postgres database against
// the embedded server started by TestMain (data_migrator_test_main_test.go),
// runs migrations, and returns a storage.DB. Each test gets an isolated
// database name so seeds and assertions don't bleed across cases.
func setupAdminTestPostgres(t *testing.T) storage.DB {
	t.Helper()

	admin, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open admin postgres: %v", err)
	}
	defer admin.Close()

	dbName := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	// SQL_ASCII encoding so the database accepts arbitrary bytes in TEXT
	// columns. Production databases that pre-date the UTF-8 default end up
	// here, which is how the dev failure (invalid UTF-8 in memories.content
	// raising SQLSTATE 22021 on read) became reproducible at all. UTF-8
	// encoded test databases would have rejected the bad bytes at INSERT
	// and we'd never exercise the read path.
	if _, err := admin.Exec(
		"CREATE DATABASE " + dbName + " ENCODING 'SQL_ASCII' TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C'",
	); err != nil {
		t.Fatalf("create test db: %v", err)
	}

	testURL := strings.Replace(resolvedPostgresURL, "/nram_test?", "/"+dbName+"?", 1)
	db, err := storage.Open(config.DatabaseConfig{URL: testURL})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		admin2, err := sql.Open("pgx", resolvedPostgresURL)
		if err == nil {
			// FORCE disconnects any stragglers so DROP succeeds even if a
			// pooled conn hasn't fully released. Postgres 13+.
			admin2.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")
			admin2.Close()
		}
	})

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("create migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return db
}

// adminTestBackends names the storage backends that admin-package tests run
// against. Tests that should cover both invoke a sub-test per entry.
var adminTestBackends = []struct {
	name  string
	setup func(*testing.T) storage.DB
}{
	{"sqlite", setupAdminTestDB},
	{"postgres", setupAdminTestPostgres},
}

// execSeed runs an INSERT/UPDATE seed statement, converting `?` placeholders
// to `$N` for Postgres. SQLite gets the query unchanged. Test seed code can
// be written once with `?` and run against either backend.
func execSeed(t *testing.T, db storage.DB, ctx context.Context, q string, args ...any) {
	t.Helper()
	if db.Backend() == storage.BackendPostgres {
		q = qmarkToDollar(q)
	}
	if _, err := db.Exec(ctx, q, args...); err != nil {
		t.Fatalf("seed exec: %v\nquery: %s", err, q)
	}
}

func qmarkToDollar(q string) string {
	var b strings.Builder
	n := 0
	for i := range len(q) {
		if q[i] == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteByte(q[i])
	}
	return b.String()
}
