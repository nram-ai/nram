package admin

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/migration"
)

func newPreflightStore(t *testing.T) *DatabaseAdminStore {
	t.Helper()
	sqliteDB := openSQLiteInMemory(t)
	t.Cleanup(func() { _ = sqliteDB.Close() })
	return &DatabaseAdminStore{db: &testSQLiteDB{db: sqliteDB}}
}

// resetEmbeddedPostgres wipes every nram table from the embedded Postgres instance
// so tests can start from a known state. It uses ResetTarget directly with drop_schema
// to also clear the schema_migrations table.
func resetEmbeddedPostgres(t *testing.T) {
	t.Helper()
	db, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open embedded postgres: %v", err)
	}
	defer db.Close()

	tables, err := existingNramTables(context.Background(), db)
	if err != nil {
		t.Fatalf("enumerate tables: %v", err)
	}
	for _, table := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + quoteIdent(table) + " CASCADE"); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	_, _ = db.Exec("DROP TABLE IF EXISTS schema_migrations")
}

func applyPostgresMigrations(t *testing.T) {
	t.Helper()
	db, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open embedded postgres: %v", err)
	}
	defer db.Close()

	mg, err := migration.NewMigrator(db, "postgres")
	if err != nil {
		t.Fatalf("create postgres migrator: %v", err)
	}
	if err := mg.Up(); err != nil {
		t.Fatalf("apply postgres migrations: %v", err)
	}
	_ = mg.Close()
}

func findCheck(t *testing.T, report *api.PreflightReport, name string) api.PreflightCheck {
	t.Helper()
	for _, c := range report.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("preflight report missing check %q; got %+v", name, report.Checks)
	return api.PreflightCheck{}
}

func TestPreflight_BadURL(t *testing.T) {
	store := newPreflightStore(t)
	report, err := store.Preflight(context.Background(), "not-a-valid-url")
	if err != nil {
		t.Fatalf("Preflight returned go error on bad url (should be captured as check): %v", err)
	}
	if report.OK {
		t.Error("expected ok=false for invalid url")
	}
	conn := findCheck(t, report, "connection")
	if conn.Status != api.PreflightStatusError {
		t.Errorf("expected connection status=error, got %q (msg=%s)", conn.Status, conn.Message)
	}
	if conn.Remediation == "" {
		t.Error("expected remediation text on connection failure")
	}
}

func TestPreflight_EmptyTarget(t *testing.T) {
	resetEmbeddedPostgres(t)
	store := newPreflightStore(t)

	report, err := store.Preflight(context.Background(), resolvedPostgresURL)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}

	// Check every non-pgvector dimension succeeds. The embedded postgres used
	// in tests does not ship pgvector, so that specific check may be error;
	// we just record the status rather than failing.
	conn := findCheck(t, report, "connection")
	if conn.Status != api.PreflightStatusOK {
		t.Errorf("connection check = %q, want ok", conn.Status)
	}
	sv := findCheck(t, report, "server_version")
	if sv.Status == api.PreflightStatusError {
		t.Errorf("server_version errored: %s", sv.Message)
	}
	ts := findCheck(t, report, "target_state")
	if ts.Status != api.PreflightStatusOK {
		t.Errorf("target_state on empty DB = %q, want ok; msg=%s", ts.Status, ts.Message)
	}
	priv := findCheck(t, report, "privileges")
	if priv.Status == api.PreflightStatusError {
		t.Errorf("privileges check errored: %s (remediation: %s)", priv.Message, priv.Remediation)
	}
	pgv := findCheck(t, report, "pgvector")
	if pgv.Status == api.PreflightStatusError {
		t.Logf("pgvector check: %s (%s) — embedded postgres ships without pgvector, which is expected in tests", pgv.Status, pgv.Message)
		if pgv.Remediation == "" {
			t.Error("pgvector error should include remediation text")
		}
	}

	// report.OK reflects aggregate status; when pgvector is missing in the test env,
	// OK will be false. That is the correct signal for a real deployment too,
	// so assert the flag matches the error-check presence rather than demanding ok=true.
	wantOK := pgv.Status != api.PreflightStatusError &&
		conn.Status != api.PreflightStatusError &&
		priv.Status != api.PreflightStatusError &&
		ts.Status != api.PreflightStatusError &&
		sv.Status != api.PreflightStatusError
	if report.OK != wantOK {
		t.Errorf("report.OK = %v, computed = %v", report.OK, wantOK)
	}
}

func TestPreflight_TargetHasLeftoverData(t *testing.T) {
	resetEmbeddedPostgres(t)
	applyPostgresMigrations(t)

	// Write one row so target_state reports warn, not ok.
	db, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO system_meta (key, value) VALUES ('preflight_probe', 'x')`); err != nil {
		t.Fatalf("seed system_meta: %v", err)
	}

	store := newPreflightStore(t)
	report, err := store.Preflight(context.Background(), resolvedPostgresURL)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	ts := findCheck(t, report, "target_state")
	if ts.Status != api.PreflightStatusWarn {
		t.Errorf("target_state with data = %q, want warn; msg=%s", ts.Status, ts.Message)
	}
	if ts.TableCounts["system_meta"] == 0 {
		t.Errorf("expected system_meta count > 0 in TableCounts; got %+v", ts.TableCounts)
	}
	if !strings.Contains(ts.Remediation, "reset") {
		t.Errorf("expected remediation to mention reset; got %q", ts.Remediation)
	}
}

func TestResetTarget_TruncateKeepsSchema(t *testing.T) {
	resetEmbeddedPostgres(t)
	applyPostgresMigrations(t)

	db, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO system_meta (key, value) VALUES ('reset_probe', 'y')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := newPreflightStore(t)
	result, err := store.ResetTarget(context.Background(), resolvedPostgresURL, api.ResetModeTruncate)
	if err != nil {
		t.Fatalf("ResetTarget: %v", err)
	}
	if result.Status != "ok" {
		t.Errorf("expected status=ok, got %q", result.Status)
	}
	if result.Mode != api.ResetModeTruncate {
		t.Errorf("expected mode=truncate, got %q", result.Mode)
	}

	// Schema should still exist — probe one table.
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM system_meta").Scan(&n); err != nil {
		t.Fatalf("truncate left schema broken: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows in system_meta after truncate, got %d", n)
	}
}

func TestResetTarget_DropSchemaRemovesTables(t *testing.T) {
	resetEmbeddedPostgres(t)
	applyPostgresMigrations(t)

	store := newPreflightStore(t)
	result, err := store.ResetTarget(context.Background(), resolvedPostgresURL, api.ResetModeDropSchema)
	if err != nil {
		t.Fatalf("ResetTarget: %v", err)
	}
	if result.Mode != api.ResetModeDropSchema {
		t.Errorf("expected mode=drop_schema, got %q", result.Mode)
	}

	// After drop_schema, nram tables should be gone.
	db, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	remaining, err := existingNramTables(context.Background(), db)
	if err != nil {
		t.Fatalf("enumerate post-drop: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("expected 0 nram tables after drop_schema, got %d (%v)", len(remaining), remaining)
	}
}

func TestResetTarget_RejectsInvalidMode(t *testing.T) {
	store := newPreflightStore(t)
	_, err := store.ResetTarget(context.Background(), resolvedPostgresURL, "nuke")
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "invalid mode") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestResetTarget_EmptyTargetIsNoOp(t *testing.T) {
	resetEmbeddedPostgres(t)
	store := newPreflightStore(t)

	result, err := store.ResetTarget(context.Background(), resolvedPostgresURL, api.ResetModeTruncate)
	if err != nil {
		t.Fatalf("ResetTarget: %v", err)
	}
	if result.Status != "ok" {
		t.Errorf("expected ok status on empty target, got %q", result.Status)
	}
	if !strings.Contains(result.Message, "clean") {
		t.Errorf("expected 'already clean' message, got %q", result.Message)
	}
}
