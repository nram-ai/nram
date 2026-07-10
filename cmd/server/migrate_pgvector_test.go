package main

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nram-ai/nram/internal/migration"
)

// TestEnsurePgvectorForMigrate_NonPostgresIsNoop verifies the backend gate: on a
// non-Postgres backend the helper returns nil without touching the database (a
// nil handle is safe precisely because it never reaches it).
func TestEnsurePgvectorForMigrate_NonPostgresIsNoop(t *testing.T) {
	if err := ensurePgvectorForMigrate(context.Background(), "sqlite", nil); err != nil {
		t.Fatalf("ensurePgvectorForMigrate(sqlite) = %v, want nil", err)
	}
}

// withMigrateDatabase rewrites dsn to point at a different database name,
// preserving user, host, port, and query (sslmode) of the original.
func withMigrateDatabase(dsn, dbName string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	u.Path = "/" + dbName
	return u.String(), nil
}

// migrateTableExists reports whether a table is present in the connection's
// current schema.
func migrateTableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var reg sql.NullString
	if err := db.QueryRow("SELECT to_regclass($1)", name).Scan(&reg); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return reg.Valid
}

// newThrowawayMigrateDB creates a dedicated, isolated database off adminDSN and
// returns an open handle to it, registering cleanup that drops it. A freshly
// created database on a pgvector-enabled server has the extension available but
// not yet created — exactly the "available-but-not-enabled" state that trips the
// 000006/000007/000057 migration guards.
func newThrowawayMigrateDB(t *testing.T, adminDSN, name string) *sql.DB {
	t.Helper()
	ctx := context.Background()
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin dsn: %v", err)
	}
	_, _ = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		_ = admin.Close()
		t.Skipf("cannot create throwaway database (role needs CREATEDB): %v", err)
	}
	_ = admin.Close()
	t.Cleanup(func() {
		c, err := sql.Open("pgx", adminDSN)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_, _ = c.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	tmpDSN, err := withMigrateDatabase(adminDSN, name)
	if err != nil {
		t.Fatalf("derive throwaway dsn: %v", err)
	}
	db, err := sql.Open("pgx", tmpDSN)
	if err != nil {
		t.Fatalf("open throwaway db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestMigrateUp_EnablesPgvectorOnAvailableNotEnabledDB is the regression guard
// for the CLI/startup divergence: `nram migrate up` used to abort at version 6
// with `type "vector" does not exist` on a Postgres host where pgvector is
// installed but was never `CREATE EXTENSION`'d in the target database, because
// only the startup auto-migrate path enabled pgvector first. It proves both the
// bug (bare RunCLI up fails) and the fix (ensurePgvectorForMigrate + RunCLI up
// succeeds and creates the vector tables), each on its own throwaway database.
//
// Gated on PGVECTOR_TEST_DSN whose role must have CREATEDB + superuser (to
// CREATE EXTENSION vector), pointing at a server where pgvector is available.
func TestMigrateUp_EnablesPgvectorOnAvailableNotEnabledDB(t *testing.T) {
	dsn := os.Getenv("PGVECTOR_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGVECTOR_TEST_DSN (CREATEDB + superuser pgvector server) to run the migrate-up pgvector test")
	}
	ctx := context.Background()

	// The scenario only exists when pgvector is available on the server; on a
	// vanilla server the migration guards skip the vector tables and there is no
	// bug to reproduce.
	probe, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open probe dsn: %v", err)
	}
	var available bool
	if err := probe.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')").Scan(&available); err != nil {
		_ = probe.Close()
		t.Fatalf("probe pg_available_extensions: %v", err)
	}
	_ = probe.Close()
	if !available {
		t.Skip("pgvector is not available on PGVECTOR_TEST_DSN server; nothing to reproduce")
	}

	migrateUp := []string{"nram", "migrate", "up"}

	// Bug: without enabling pgvector first, `migrate up` aborts creating the
	// vector(384) table because the type does not exist in this database.
	t.Run("bare-migrate-up-fails", func(t *testing.T) {
		db := newThrowawayMigrateDB(t, dsn, "nram_migrate_up_bug")
		_, err := migration.RunCLI(migrateUp, db, "postgres")
		if err == nil {
			t.Fatal("bare `migrate up` unexpectedly succeeded; expected `type \"vector\" does not exist`")
		}
		if !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("bare `migrate up` error = %v, want it to mention the missing vector type", err)
		}
	})

	// Fix: enabling pgvector first (as main now does for `migrate up`) lets the
	// same command apply cleanly and create the vector tables.
	t.Run("ensure-then-migrate-up-succeeds", func(t *testing.T) {
		db := newThrowawayMigrateDB(t, dsn, "nram_migrate_up_fix")
		if err := ensurePgvectorForMigrate(ctx, "postgres", db); err != nil {
			t.Fatalf("ensurePgvectorForMigrate: %v (role must be able to CREATE EXTENSION vector)", err)
		}
		if _, err := migration.RunCLI(migrateUp, db, "postgres"); err != nil {
			t.Fatalf("`migrate up` after ensure failed: %v", err)
		}
		for _, tbl := range []string{"memory_vectors_384", "entity_vectors_384"} {
			if !migrateTableExists(t, db, tbl) {
				t.Errorf("expected %s after `migrate up`", tbl)
			}
		}
	})
}
