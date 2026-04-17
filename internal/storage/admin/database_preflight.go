package admin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx stdlib driver
	"github.com/nram-ai/nram/internal/api"
)

// preflightTables enumerates every table nram would populate on a
// SQLite→Postgres migration. Used for target-state checks and reset operations.
// Order matters for reset truncation (children before parents) to avoid FK failures
// when CASCADE is not used.
var preflightTables = []string{
	// Leaf / child tables first.
	"memory_vectors_384",
	"memory_vectors_512",
	"memory_vectors_768",
	"memory_vectors_1024",
	"memory_vectors_1536",
	"memory_vectors_3072",
	"entity_vectors_384",
	"entity_vectors_512",
	"entity_vectors_768",
	"entity_vectors_1024",
	"entity_vectors_1536",
	"entity_vectors_3072",
	"dream_logs",
	"dream_log_summaries",
	"dream_project_dirty",
	"dream_cycles",
	"enrichment_queue",
	"ingestion_log",
	"memory_lineage",
	"relationships",
	"entity_aliases",
	"webauthn_credentials",
	"oauth_refresh_tokens",
	"oauth_authorization_codes",
	"oauth_idp_configs",
	"oauth_clients",
	"token_usage",
	"webhooks",
	"memory_shares",
	"api_keys",
	"settings",
	"entities",
	"memories",
	// Parent tables last.
	"projects",
	"users",
	"organizations",
	"system_meta",
	"namespaces",
}

// Preflight runs a battery of read-only checks against a target Postgres URL
// to surface problems before any data is migrated.
//
// Checks (in order): connection, server_version, pgvector, privileges, target_state.
// Each check is independent; errors are captured as check results, not Go errors.
// A Go error is only returned if the preflight itself cannot run.
func (s *DatabaseAdminStore) Preflight(ctx context.Context, url string) (*api.PreflightReport, error) {
	report := &api.PreflightReport{OK: true, Checks: []api.PreflightCheck{}}

	db, err := sql.Open("pgx", url)
	if err != nil {
		report.OK = false
		report.Checks = append(report.Checks, api.PreflightCheck{
			Name:        "connection",
			Status:      api.PreflightStatusError,
			Message:     fmt.Sprintf("failed to open connection: %v", err),
			Remediation: "Check that the URL is a valid libpq connection string (e.g. postgres://user:pass@host:5432/dbname).",
		})
		return report, nil
	}
	defer db.Close()

	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(30 * time.Second)

	// Check 1: connection.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		report.OK = false
		report.Checks = append(report.Checks, api.PreflightCheck{
			Name:        "connection",
			Status:      api.PreflightStatusError,
			Message:     fmt.Sprintf("ping failed: %v", err),
			Remediation: "Verify the Postgres server is reachable on the configured host/port and credentials are correct.",
		})
		// Without a working connection, later checks cannot run. Return early.
		return report, nil
	}
	report.Checks = append(report.Checks, api.PreflightCheck{
		Name:    "connection",
		Status:  api.PreflightStatusOK,
		Message: "connected successfully",
	})

	// Check 2: server_version.
	report.Checks = append(report.Checks, checkServerVersion(ctx, db))

	// Check 3: pgvector.
	pgv := checkPgvector(ctx, db)
	report.Checks = append(report.Checks, pgv)

	// Check 4: privileges (CREATE on current schema).
	report.Checks = append(report.Checks, checkPrivileges(ctx, db))

	// Check 5: target_state — row counts for every nram table that exists.
	report.Checks = append(report.Checks, checkTargetState(ctx, db))

	for _, c := range report.Checks {
		if c.Status == api.PreflightStatusError {
			report.OK = false
			break
		}
	}
	return report, nil
}

// checkServerVersion returns a preflight check reporting the Postgres server version.
// Reports an error if the version is older than Postgres 12 (pgvector's minimum).
func checkServerVersion(ctx context.Context, db *sql.DB) api.PreflightCheck {
	var version string
	var serverNum int
	// server_version_num is an int like 160002 for 16.2.
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&serverNum); err != nil {
		return api.PreflightCheck{
			Name:        "server_version",
			Status:      api.PreflightStatusWarn,
			Message:     fmt.Sprintf("could not determine server version: %v", err),
			Remediation: "Migration may still work; verify server is Postgres 12 or newer.",
		}
	}
	_ = db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if serverNum < 120000 {
		return api.PreflightCheck{
			Name:        "server_version",
			Status:      api.PreflightStatusError,
			Message:     fmt.Sprintf("server version %s is too old", version),
			Remediation: "Upgrade to Postgres 12 or newer. pgvector requires Postgres 12+.",
		}
	}
	return api.PreflightCheck{
		Name:    "server_version",
		Status:  api.PreflightStatusOK,
		Message: version,
	}
}

// checkPgvector verifies that the pgvector extension is installed and enabled.
// If missing but available, returns an error with CREATE EXTENSION remediation.
// If unavailable entirely, returns an error with install-at-OS-level remediation.
func checkPgvector(ctx context.Context, db *sql.DB) api.PreflightCheck {
	var installedVersion sql.NullString
	err := db.QueryRowContext(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'vector'").Scan(&installedVersion)
	if err == nil && installedVersion.Valid {
		return api.PreflightCheck{
			Name:    "pgvector",
			Status:  api.PreflightStatusOK,
			Message: fmt.Sprintf("pgvector %s enabled", installedVersion.String),
		}
	}

	// Not installed in this DB. Is it available on the server?
	var availableVersion sql.NullString
	availErr := db.QueryRowContext(ctx,
		"SELECT default_version FROM pg_available_extensions WHERE name = 'vector'",
	).Scan(&availableVersion)
	if availErr == nil && availableVersion.Valid {
		return api.PreflightCheck{
			Name:        "pgvector",
			Status:      api.PreflightStatusError,
			Message:     fmt.Sprintf("pgvector %s is available but not enabled on this database", availableVersion.String),
			Remediation: "Run: CREATE EXTENSION vector; (requires superuser or a role with CREATEEXTENSION privilege).",
		}
	}

	return api.PreflightCheck{
		Name:        "pgvector",
		Status:      api.PreflightStatusError,
		Message:     "pgvector extension is not available on this server",
		Remediation: "Install the pgvector package at the OS level (e.g. apt-get install postgresql-16-pgvector) and restart Postgres, then CREATE EXTENSION vector.",
	}
}

// checkPrivileges verifies the current role has CREATE privilege on the current schema.
// Migration requires CREATE TABLE and CREATE INDEX, both gated by schema CREATE.
func checkPrivileges(ctx context.Context, db *sql.DB) api.PreflightCheck {
	var schema string
	if err := db.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return api.PreflightCheck{
			Name:        "privileges",
			Status:      api.PreflightStatusWarn,
			Message:     fmt.Sprintf("could not determine current schema: %v", err),
			Remediation: "Verify the connection URL specifies a valid database and schema.",
		}
	}

	var hasCreate bool
	var currentUser string
	_ = db.QueryRowContext(ctx, "SELECT current_user").Scan(&currentUser)
	err := db.QueryRowContext(ctx,
		"SELECT has_schema_privilege(current_user, $1, 'CREATE')", schema,
	).Scan(&hasCreate)
	if err != nil {
		return api.PreflightCheck{
			Name:        "privileges",
			Status:      api.PreflightStatusWarn,
			Message:     fmt.Sprintf("could not check schema privileges: %v", err),
			Remediation: "Verify the role has CREATE on the target schema.",
		}
	}
	if !hasCreate {
		return api.PreflightCheck{
			Name:        "privileges",
			Status:      api.PreflightStatusError,
			Message:     fmt.Sprintf("role %q lacks CREATE on schema %q", currentUser, schema),
			Remediation: fmt.Sprintf("GRANT CREATE ON SCHEMA %s TO %s;", schema, currentUser),
		}
	}
	return api.PreflightCheck{
		Name:    "privileges",
		Status:  api.PreflightStatusOK,
		Message: fmt.Sprintf("role %q has CREATE on schema %q", currentUser, schema),
	}
}

// checkTargetState enumerates nram tables and reports row counts for each one
// that already exists in the target DB. Non-zero counts surface leftover data
// from prior failed migrations that the user should reset before retrying.
func checkTargetState(ctx context.Context, db *sql.DB) api.PreflightCheck {
	counts, err := countTargetTables(ctx, db)
	if err != nil {
		return api.PreflightCheck{
			Name:    "target_state",
			Status:  api.PreflightStatusWarn,
			Message: fmt.Sprintf("could not enumerate target tables: %v", err),
		}
	}
	if len(counts) == 0 {
		return api.PreflightCheck{
			Name:    "target_state",
			Status:  api.PreflightStatusOK,
			Message: "target database is empty (no nram tables found)",
		}
	}

	var totalRows int
	for _, n := range counts {
		totalRows += n
	}
	if totalRows == 0 {
		return api.PreflightCheck{
			Name:        "target_state",
			Status:      api.PreflightStatusOK,
			Message:     fmt.Sprintf("schema exists (%d tables) but contains no rows", len(counts)),
			TableCounts: counts,
		}
	}

	return api.PreflightCheck{
		Name:        "target_state",
		Status:      api.PreflightStatusWarn,
		Message:     fmt.Sprintf("target contains %d rows across %d nram tables (likely leftover from a prior attempt)", totalRows, len(counts)),
		Remediation: "Run POST /v1/admin/database/reset with mode=truncate or mode=drop_schema before re-running migration.",
		TableCounts: counts,
	}
}

// countTargetTables returns row counts for every nram table present in the target DB.
// Tables that do not exist yet are simply omitted from the result map.
func countTargetTables(ctx context.Context, db *sql.DB) (map[string]int, error) {
	existing, err := existingNramTables(ctx, db)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int, len(existing))
	for _, table := range existing {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&n); err != nil {
			// Skip tables that error (e.g. permission denied) rather than fail the whole check.
			continue
		}
		counts[table] = n
	}
	return counts, nil
}

// existingNramTables returns the subset of preflightTables that actually exist
// in the target DB's current schema.
func existingNramTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT tablename FROM pg_tables WHERE schemaname = current_schema()
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	present := make(map[string]bool)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		present[t] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var existing []string
	for _, t := range preflightTables {
		if present[t] {
			existing = append(existing, t)
		}
	}
	return existing, nil
}

// ResetTarget wipes nram-owned tables from the target Postgres database.
//
// Mode semantics:
//   - "truncate": TRUNCATE RESTART IDENTITY CASCADE on every nram table that exists.
//     Schema is preserved; pgvector stays enabled. Fast and owner-privilege sufficient.
//   - "drop_schema": DROP TABLE IF EXISTS ... CASCADE for every nram table.
//     Schema migrations must be re-run (the migrator does this automatically on next migrate).
//     Extensions are not touched.
//
// Only tables in preflightTables are touched; unrelated user tables in the same
// DB are left alone.
func (s *DatabaseAdminStore) ResetTarget(ctx context.Context, url, mode string) (*api.ResetResult, error) {
	if mode != api.ResetModeTruncate && mode != api.ResetModeDropSchema {
		return nil, fmt.Errorf("invalid mode %q (must be %q or %q)", mode, api.ResetModeTruncate, api.ResetModeDropSchema)
	}

	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(30 * time.Second)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	existing, err := existingNramTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("enumerate tables: %w", err)
	}
	if len(existing) == 0 {
		return &api.ResetResult{
			Status:  "ok",
			Mode:    mode,
			Message: "target database already clean (no nram tables found)",
		}, nil
	}

	switch mode {
	case api.ResetModeTruncate:
		return resetTruncate(ctx, db, existing)
	case api.ResetModeDropSchema:
		return resetDropTables(ctx, db, existing)
	default:
		return nil, errors.New("unreachable")
	}
}

// resetTruncate issues a single TRUNCATE statement covering all nram tables
// that exist in the target DB. RESTART IDENTITY resets any sequences; CASCADE
// handles FK dependencies between nram tables (and any tables a user has added
// that FK to nram tables, intentionally).
func resetTruncate(ctx context.Context, db *sql.DB, tables []string) (*api.ResetResult, error) {
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = quoteIdent(t)
	}
	stmt := fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", strings.Join(quoted, ", "))
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return nil, fmt.Errorf("truncate: %w", err)
	}
	return &api.ResetResult{
		Status:        "ok",
		Mode:          api.ResetModeTruncate,
		Message:       fmt.Sprintf("truncated %d nram tables", len(tables)),
		TablesDropped: tables,
	}, nil
}

// resetDropTables issues DROP TABLE IF EXISTS ... CASCADE for every nram table.
// The schema_migrations row is also cleared so golang-migrate starts fresh.
func resetDropTables(ctx context.Context, db *sql.DB, tables []string) (*api.ResetResult, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for _, t := range tables {
		if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(t)+" CASCADE"); err != nil {
			return nil, fmt.Errorf("drop %s: %w", t, err)
		}
	}

	// golang-migrate tracks applied migrations in schema_migrations; clear it so
	// a fresh migration run starts from zero.
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations"); err != nil {
		return nil, fmt.Errorf("drop schema_migrations: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return &api.ResetResult{
		Status:        "ok",
		Mode:          api.ResetModeDropSchema,
		Message:       fmt.Sprintf("dropped %d nram tables; re-run migration to recreate schema", len(tables)),
		TablesDropped: tables,
	}, nil
}

// quoteIdent wraps an identifier in double quotes per Postgres syntax, escaping
// embedded double quotes. Used on table names from preflightTables (a static,
// known list) before interpolation into DDL — we never accept user identifiers.
func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
