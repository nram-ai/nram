package admin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx stdlib driver
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// ErrPgvectorUnavailable is returned by EnsurePgvector when the pgvector
// extension is not installed on the server at all (not merely disabled). It is
// a sentinel so callers can distinguish "operator has no pgvector, run in
// text-only / Qdrant-backed mode" from "pgvector is present but the connecting
// role cannot enable it" (the latter is a hard, actionable failure).
var ErrPgvectorUnavailable = errors.New("pgvector extension is not available on this server")

// pgvectorHNSWMaxDims is pgvector's HNSW/IVFFlat index dimension ceiling; tables
// wider than this (3072) use sequential scan, matching migrations 000006/000007.
// The dimension set itself is storage.OrderedVectorDimensions (the canonical list).
const pgvectorHNSWMaxDims = 2000

// preflightTables enumerates every table nram would populate on a
// SQLite→Postgres migration. Used for target-state checks and reset operations.
// Order matters for reset truncation (children before parents) to avoid FK failures
// when CASCADE is not used.
var preflightTables = []string{
	// Leaf / child tables first.
	// log_entries is a standalone, FK-free diagnostic-log table; listed here so
	// ResetTarget clears it and the preflight leftover-data check covers it. It
	// is also copied by the migrator (see migratedTables / migrateLogEntries).
	"log_entries",
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
	// share_token_grants references share_tokens (and projects); drop it
	// first. share_tokens is referenced by oauth_clients /
	// oauth_authorization_codes / oauth_refresh_tokens via the
	// share_token_id columns added in migration 000040, so it must drop
	// after those oauth_* entries above.
	"share_token_grants",
	"share_tokens",
	"token_usage",
	"webhooks",
	"api_keys",
	"audit_events",
	"settings",
	"entities",
	"memories",
	// procedural_entries references namespaces only; list before that parent.
	"procedural_entries",
	// export_jobs holds FKs to both users (ON DELETE CASCADE) and projects
	// (ON DELETE CASCADE). Listed before its parents so the dependency-aware
	// truncate path orders it correctly.
	"export_jobs",
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
	defer func() { _ = db.Close() }()

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

	// Check 5: target_state, row counts for every nram table that exists.
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
	if version, enabled, err := pgvectorEnabled(ctx, db); err == nil && enabled {
		return api.PreflightCheck{
			Name:    "pgvector",
			Status:  api.PreflightStatusOK,
			Message: fmt.Sprintf("pgvector %s enabled", version),
		}
	}

	// Not enabled in this DB. Is it available on the server?
	if available, ok, err := pgvectorAvailable(ctx, db); err == nil && ok {
		// pgvector is a non-trusted extension (verified against pgvector 0.8.2's
		// vector.control, which carries no `trusted = true`), so CREATE EXTENSION
		// requires a superuser. Probe rolsuper to tell the operator up front
		// whether nram can auto-enable it during migration (EnsurePgvector) or
		// whether a superuser must run CREATE EXTENSION first. If a future
		// pgvector ships trusted, this advisory signal needs refining; the
		// migration path (EnsurePgvector) is empirical and stays correct either way.
		user, isSuper, _ := roleSuperuser(ctx, db)
		if isSuper {
			return api.PreflightCheck{
				Name:        "pgvector",
				Status:      api.PreflightStatusWarn,
				Message:     fmt.Sprintf("pgvector %s is available but not enabled; nram will enable it during migration", available),
				Remediation: fmt.Sprintf("No action needed: %s is a superuser, so nram runs CREATE EXTENSION vector automatically.", roleDesc(user)),
			}
		}
		return api.PreflightCheck{
			Name:        "pgvector",
			Status:      api.PreflightStatusError,
			Message:     fmt.Sprintf("pgvector %s is available but not enabled, and %s cannot create it", available, roleDesc(user)),
			Remediation: "Have a superuser run: CREATE EXTENSION vector; (pgvector is not a trusted extension, so a non-superuser cannot create it).",
		}
	}

	return api.PreflightCheck{
		Name:        "pgvector",
		Status:      api.PreflightStatusError,
		Message:     "pgvector extension is not available on this server",
		Remediation: "Install the pgvector package at the OS level (e.g. apt-get install postgresql-16-pgvector) and restart Postgres, then CREATE EXTENSION vector.",
	}
}

// pgvectorEnabled reports whether the pgvector extension is enabled (created) in
// the connected database, returning its version. The single source of the
// "is pgvector enabled?" query shared by the preflight and the ensure paths.
func pgvectorEnabled(ctx context.Context, db *sql.DB) (version string, enabled bool, err error) {
	var v sql.NullString
	err = db.QueryRowContext(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'vector'").Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v.String, v.Valid, err
}

// pgvectorAvailable reports whether the pgvector extension is available to
// install on the server (present in pg_available_extensions), returning its
// default version.
func pgvectorAvailable(ctx context.Context, db *sql.DB) (version string, available bool, err error) {
	var v sql.NullString
	err = db.QueryRowContext(ctx, "SELECT default_version FROM pg_available_extensions WHERE name = 'vector'").Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v.String, v.Valid, err
}

// roleDesc renders a role for an operator-facing message, falling back to a
// generic phrase when the role name is unknown (roleSuperuser returns "" on error).
func roleDesc(user string) string {
	if user == "" {
		return "the connecting role"
	}
	return fmt.Sprintf("role %q", user)
}

// roleSuperuser reports the connecting role's name and whether it is a Postgres
// superuser. pgvector is a non-trusted extension, so only a superuser may
// CREATE EXTENSION vector; this is the proactive "can we enable it?" probe.
func roleSuperuser(ctx context.Context, db *sql.DB) (string, bool, error) {
	var user string
	var isSuper bool
	err := db.QueryRowContext(ctx,
		"SELECT current_user, COALESCE(rolsuper, false) FROM pg_roles WHERE rolname = current_user",
	).Scan(&user, &isSuper)
	return user, isSuper, err
}

// EnsurePgvector guarantees the pgvector extension is enabled on the target
// Postgres database before schema migrations that use the `vector` type run,
// turning the note's opaque `type "vector" does not exist` failure into either a
// clean auto-enable or an actionable error.
//
// Contract: a nil return means the extension IS enabled on exit. Behavior:
//   - Already enabled: no-op, nil.
//   - Available but not enabled: attempts CREATE EXTENSION IF NOT EXISTS vector.
//     Succeeds when the connecting role may create it (superuser); on
//     insufficient_privilege (SQLSTATE 42501) returns an actionable error naming
//     the fix (have a superuser run CREATE EXTENSION vector).
//   - Not available on the server at all: returns ErrPgvectorUnavailable so
//     callers can choose to proceed in text-only / Qdrant-backed mode (the
//     migration guards skip the vector tables) rather than treat it as fatal.
//
// It must run before golang-migrate's Up(), which takes pg_advisory_lock(1);
// EnsurePgvector holds no such lock.
func EnsurePgvector(ctx context.Context, db *sql.DB) error {
	if _, enabled, err := pgvectorEnabled(ctx, db); err == nil && enabled {
		return nil // already enabled
	}

	available, ok, err := pgvectorAvailable(ctx, db)
	if err != nil || !ok {
		return ErrPgvectorUnavailable
	}

	// Available but not enabled: try to create it. IF NOT EXISTS keeps this
	// idempotent under a race with a concurrent creator.
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" { // insufficient_privilege
			user, _, _ := roleSuperuser(ctx, db)
			return fmt.Errorf("pgvector %s is available but not enabled and %s cannot create it: have a superuser run CREATE EXTENSION vector (pgvector is not a trusted extension, so creation requires superuser)", available, roleDesc(user))
		}
		return fmt.Errorf("enable pgvector: %w", err)
	}
	return nil
}

// EnsureVectorTables idempotently (re)creates every memory_vectors_<dim> and
// entity_vectors_<dim> table at its current schema (memory vectors carry the
// facet_id column and (memory_id, facet_id) primary key added in migration
// 000057). It self-heals the "migrated without pgvector, enabled it later" trap:
// golang-migrate records 000006/000007 as applied even when their
// pg_available_extensions guard skipped the tables, and never re-runs them, so
// nothing else recreates the vector tables once the extension is finally enabled.
//
// The caller must ensure the pgvector extension is enabled first (e.g. via
// EnsurePgvector returning nil); EnsureVectorTables re-checks and returns an
// error rather than emitting the opaque `type "vector" does not exist`.
//
// Each table is created only when it is actually MISSING (checked via
// to_regclass). This matters because Postgres checks the CREATE-on-schema
// privilege BEFORE the IF NOT EXISTS existence check, so issuing CREATE TABLE
// unconditionally would fail with "permission denied for schema" on every boot
// for a non-superuser role that lacks CREATE on the schema (the common PG15+
// default) even though the tables already exist. Skipping present tables keeps
// the steady-state boot a pure read: it needs CREATE privilege only in the
// genuine self-heal case, where table creation is unavoidable anyway.
func EnsureVectorTables(ctx context.Context, db *sql.DB) error {
	if _, enabled, err := pgvectorEnabled(ctx, db); err != nil || !enabled {
		return fmt.Errorf("cannot ensure vector tables: pgvector extension is not enabled")
	}

	// One round-trip to learn which vector tables already exist, so steady-state
	// boots issue zero DDL (see the doc comment: unconditional CREATE would trip
	// the schema-CREATE privilege check for non-superuser roles).
	present, err := existingVectorTables(ctx, db)
	if err != nil {
		return fmt.Errorf("list vector tables: %w", err)
	}

	for _, dim := range storage.OrderedVectorDimensions {
		specs := []struct{ table, createDDL, index string }{
			{
				table: fmt.Sprintf("memory_vectors_%d", dim),
				createDDL: fmt.Sprintf(`CREATE TABLE memory_vectors_%d (
					memory_id UUID NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
					embedding vector(%d) NOT NULL,
					facet_id smallint NOT NULL DEFAULT 0,
					PRIMARY KEY (memory_id, facet_id)
				)`, dim, dim),
				index: fmt.Sprintf("idx_mv_%d_hnsw", dim),
			},
			{
				table: fmt.Sprintf("entity_vectors_%d", dim),
				createDDL: fmt.Sprintf(`CREATE TABLE entity_vectors_%d (
					entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
					embedding vector(%d) NOT NULL
				)`, dim, dim),
				index: fmt.Sprintf("idx_ev_%d_hnsw", dim),
			},
		}
		for _, s := range specs {
			if present[s.table] {
				continue // no DDL, so no CREATE-on-schema privilege needed
			}
			stmts := []string{s.createDDL}
			// pgvector's HNSW index supports up to pgvectorHNSWMaxDims dimensions;
			// wider tables (3072) rely on sequential scan, matching 000006/000007.
			// Index names match the migrations.
			if dim <= pgvectorHNSWMaxDims {
				stmts = append(stmts, fmt.Sprintf(`CREATE INDEX %s ON %s USING hnsw (embedding vector_cosine_ops)`, s.index, s.table))
			}
			for _, stmt := range stmts {
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("ensure %s: %w", s.table, err)
				}
			}
		}
	}
	return nil
}

// existingVectorTables returns the set of memory_vectors_<dim>/entity_vectors_<dim>
// tables present in the connection's current schema, in a single query so
// EnsureVectorTables can gate DDL without a per-table round-trip.
func existingVectorTables(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT tablename FROM pg_tables
		WHERE schemaname = current_schema()
		  AND (tablename LIKE 'memory_vectors_%' OR tablename LIKE 'entity_vectors_%')`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	present := make(map[string]bool)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		present[t] = true
	}
	return present, rows.Err()
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
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = db.Close() }()

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
// known list) before interpolation into DDL; we never accept user identifiers.
func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
