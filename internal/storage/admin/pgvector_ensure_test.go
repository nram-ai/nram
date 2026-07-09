package admin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/storage"
)

// TestVectorDimsMatchPreflightTables is the drift guard: every vector table that
// EnsureVectorTables provisions from storage.OrderedVectorDimensions must appear
// in preflightTables and vice versa, so the self-heal DDL and the migrate/reset
// coverage lists cannot silently diverge when a dimension is added or removed.
func TestVectorDimsMatchPreflightTables(t *testing.T) {
	want := map[string]bool{}
	for _, d := range storage.OrderedVectorDimensions {
		want[fmt.Sprintf("memory_vectors_%d", d)] = true
		want[fmt.Sprintf("entity_vectors_%d", d)] = true
	}
	got := map[string]bool{}
	for _, tbl := range preflightTables {
		if strings.HasPrefix(tbl, "memory_vectors_") || strings.HasPrefix(tbl, "entity_vectors_") {
			got[tbl] = true
		}
	}
	for tbl := range want {
		if !got[tbl] {
			t.Errorf("OrderedVectorDimensions provisions %q but preflightTables does not list it", tbl)
		}
	}
	for tbl := range got {
		if !want[tbl] {
			t.Errorf("preflightTables lists vector table %q not produced by OrderedVectorDimensions", tbl)
		}
	}
}

// TestEnsurePgvector_UnavailableOnEmbedded verifies the sentinel path: the
// embedded Postgres used by the admin test suite ships without pgvector, so
// EnsurePgvector must return ErrPgvectorUnavailable (callers then run text-only)
// rather than a hard error.
func TestEnsurePgvector_UnavailableOnEmbedded(t *testing.T) {
	db, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open embedded postgres: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := EnsurePgvector(context.Background(), db); !errors.Is(err, ErrPgvectorUnavailable) {
		t.Fatalf("EnsurePgvector on embedded postgres = %v, want ErrPgvectorUnavailable", err)
	}
}

// tableExists reports whether a table is present in the connection's current schema.
func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var reg sql.NullString
	if err := db.QueryRow("SELECT to_regclass($1)", name).Scan(&reg); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return reg.Valid
}

// TestEnsureVectorTables_SelfHealsTrap drives the degenerate "migrated without
// pgvector, enabled it later" trap against a live pgvector Postgres (gated on
// PGVECTOR_TEST_DSN, which must point at a throwaway superuser database):
// migrations record 000006/000007 as applied, the vector tables are then dropped
// to reproduce the trap, and EnsureVectorTables must recreate all twelve at the
// current schema (memory vectors carrying facet_id) idempotently.
func TestEnsureVectorTables_SelfHealsTrap(t *testing.T) {
	dsn := os.Getenv("PGVECTOR_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGVECTOR_TEST_DSN (throwaway superuser pgvector database) to run the self-heal test")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pgvector dsn: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Clean slate: drop every nram table plus schema_migrations so the migrator
	// starts from zero on this throwaway DB.
	existing, err := existingNramTables(ctx, db)
	if err != nil {
		t.Fatalf("enumerate tables: %v", err)
	}
	for _, tbl := range existing {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + quoteIdent(tbl) + " CASCADE"); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	_, _ = db.Exec("DROP TABLE IF EXISTS schema_migrations")

	// Enable pgvector (role must be a superuser) and apply the full schema.
	if err := EnsurePgvector(ctx, db); err != nil {
		t.Fatalf("EnsurePgvector: %v (PGVECTOR_TEST_DSN role must be able to CREATE EXTENSION vector)", err)
	}
	mg, err := migration.NewMigrator(db, "postgres")
	if err != nil {
		t.Fatalf("new migrator: %v", err)
	}
	if err := mg.Up(); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	_ = mg.Close()

	// Sanity: the migrations created the vector tables.
	if !tableExists(t, db, "memory_vectors_384") {
		t.Fatal("expected memory_vectors_384 after migrations")
	}

	// Reproduce the trap: drop the vector tables while schema_migrations still
	// records 000006/000007 as applied, so the migrator would never recreate them.
	for _, d := range storage.OrderedVectorDimensions {
		for _, tbl := range []string{fmt.Sprintf("memory_vectors_%d", d), fmt.Sprintf("entity_vectors_%d", d)} {
			if _, err := db.Exec("DROP TABLE IF EXISTS " + quoteIdent(tbl) + " CASCADE"); err != nil {
				t.Fatalf("drop %s: %v", tbl, err)
			}
		}
	}
	if tableExists(t, db, "memory_vectors_384") {
		t.Fatal("trap setup failed: memory_vectors_384 still present after drop")
	}

	// Self-heal.
	if err := EnsureVectorTables(ctx, db); err != nil {
		t.Fatalf("EnsureVectorTables: %v", err)
	}

	// Every vector table is back.
	for _, d := range storage.OrderedVectorDimensions {
		for _, tbl := range []string{fmt.Sprintf("memory_vectors_%d", d), fmt.Sprintf("entity_vectors_%d", d)} {
			if !tableExists(t, db, tbl) {
				t.Errorf("EnsureVectorTables did not recreate %s", tbl)
			}
		}
	}

	// memory_vectors carry the facet_id column and (memory_id, facet_id) PK from 000057.
	var hasFacet bool
	if err := db.QueryRow(`SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'memory_vectors_384' AND column_name = 'facet_id')`).Scan(&hasFacet); err != nil {
		t.Fatalf("check facet_id: %v", err)
	}
	if !hasFacet {
		t.Error("recreated memory_vectors_384 missing facet_id column")
	}
	var pkCols int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.key_column_usage k
		JOIN information_schema.table_constraints c ON c.constraint_name = k.constraint_name
		WHERE c.table_name = 'memory_vectors_384' AND c.constraint_type = 'PRIMARY KEY'`).Scan(&pkCols); err != nil {
		t.Fatalf("check pk: %v", err)
	}
	if pkCols != 2 {
		t.Errorf("memory_vectors_384 primary key has %d columns, want 2 (memory_id, facet_id)", pkCols)
	}

	// Idempotent second pass.
	if err := EnsureVectorTables(ctx, db); err != nil {
		t.Fatalf("EnsureVectorTables second pass: %v", err)
	}
}
