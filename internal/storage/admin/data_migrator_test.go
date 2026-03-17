package admin

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/nram-ai/nram/internal/migration"
)

const testPostgresURL = "postgres://nram:nram@192.168.2.63/nram"

// postgresAvailable returns true if a Postgres connection can be established.
func postgresAvailable() bool {
	db, err := sql.Open("pgx", testPostgresURL)
	if err != nil {
		return false
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return db.PingContext(ctx) == nil
}

// openSQLiteInMemory creates an in-memory SQLite database with the full nram schema.
func openSQLiteInMemory(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	mg, err := migration.NewMigrator(db, "sqlite")
	if err != nil {
		t.Fatalf("create sqlite migrator: %v", err)
	}
	if err := mg.Up(); err != nil {
		t.Fatalf("apply sqlite migrations: %v", err)
	}
	_ = mg.Close()
	return db
}

// seedSQLite inserts a minimal set of test rows that cover every migrated table.
// The rows are designed to exercise array and nullable column conversions.
func seedSQLite(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed: %s\nargs: %v\nerr: %v", query, args, err)
		}
	}

	// ── namespaces (root already inserted by migration) ────────────────────
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000001', 'TestOrg', 'testorg', 'organization',
		        '00000000-0000-0000-0000-000000000000', 'testorg', 1)`)

	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000002', 'TestUser', 'testuser', 'user',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'testorg/testuser', 2)`)

	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000003', 'TestProject', 'testproject', 'project',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'testorg/testproject', 2)`)

	// ── organizations ──────────────────────────────────────────────────────
	mustExec(`INSERT INTO organizations (id, namespace_id, name, slug)
		VALUES ('bbbbbbbb-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'TestOrg', 'testorg')`)

	// ── users ──────────────────────────────────────────────────────────────
	mustExec(`INSERT INTO users (id, email, display_name, password_hash, org_id, namespace_id, role)
		VALUES ('cccccccc-0000-0000-0000-000000000001',
		        'test@example.com', 'Test User', '$2a$10$hash',
		        'bbbbbbbb-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002', 'administrator')`)

	// ── api_keys (scopes = JSON array of UUIDs) ────────────────────────────
	mustExec(`INSERT INTO api_keys (id, user_id, key_prefix, key_hash, name, scopes)
		VALUES ('dddddddd-0000-0000-0000-000000000001',
		        'cccccccc-0000-0000-0000-000000000001',
		        'nram_k_', 'hashvalue123', 'test-key', '[]')`)

	// ── projects (default_tags = JSON text array) ──────────────────────────
	mustExec(`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, default_tags)
		VALUES ('eeeeeeee-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000003',
		        'aaaaaaaa-0000-0000-0000-000000000001',
		        'TestProject', 'testproject', '["alpha","beta"]')`)

	// ── settings ───────────────────────────────────────────────────────────
	// settings.value is JSONB in Postgres; store a valid JSON value in SQLite.
	mustExec(`INSERT INTO settings (key, value, scope) VALUES ('test_key', '"test_value"', 'global')`)

	// system_meta already has rows from the migration; add one more.
	mustExec(`INSERT OR IGNORE INTO system_meta (key, value) VALUES ('test_meta', 'value1')`)

	// ── memories ───────────────────────────────────────────────────────────
	mustExec(`INSERT INTO memories (id, namespace_id, content, tags, confidence, importance)
		VALUES ('ffffffff-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'test memory content', '["tag1","tag2"]', 0.9, 0.7)`)

	// ── entities ───────────────────────────────────────────────────────────
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type)
		VALUES ('11111111-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'Alice', 'alice', 'person')`)

	// ── entity_aliases ─────────────────────────────────────────────────────
	mustExec(`INSERT INTO entity_aliases (id, entity_id, alias, alias_type)
		VALUES ('22222222-0000-0000-0000-000000000001',
		        '11111111-0000-0000-0000-000000000001', 'Ali', 'nickname')`)

	// ── relationships ──────────────────────────────────────────────────────
	// Add a second entity so we have src/tgt.
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type)
		VALUES ('11111111-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'Bob', 'bob', 'person')`)

	mustExec(`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation)
		VALUES ('33333333-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        '11111111-0000-0000-0000-000000000001',
		        '11111111-0000-0000-0000-000000000002',
		        'knows')`)

	// ── memory_lineage ─────────────────────────────────────────────────────
	mustExec(`INSERT INTO memory_lineage (id, memory_id, parent_id, relation)
		VALUES ('44444444-0000-0000-0000-000000000001',
		        'ffffffff-0000-0000-0000-000000000001', NULL, 'origin')`)

	// ── ingestion_log (memory_ids = JSON UUID array) ───────────────────────
	mustExec(`INSERT INTO ingestion_log (id, namespace_id, source, raw_content, memory_ids)
		VALUES ('55555555-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'api', 'raw content here',
		        '["ffffffff-0000-0000-0000-000000000001"]')`)

	// ── enrichment_queue ───────────────────────────────────────────────────
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, steps_completed)
		VALUES ('66666666-0000-0000-0000-000000000001',
		        'ffffffff-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'pending', '[]')`)

	// ── webhooks (events = JSON text array) ────────────────────────────────
	mustExec(`INSERT INTO webhooks (id, url, events, scope)
		VALUES ('77777777-0000-0000-0000-000000000001',
		        'https://example.com/hook', '["memory.created","memory.deleted"]', 'global')`)

	// ── memory_shares ──────────────────────────────────────────────────────
	mustExec(`INSERT INTO memory_shares (id, source_ns_id, target_ns_id, permission)
		VALUES ('88888888-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000001',
		        'recall')`)

	// ── oauth_clients (redirect_uris, grant_types = JSON text arrays) ──────
	mustExec(`INSERT INTO oauth_clients (id, client_id, name, redirect_uris, grant_types)
		VALUES ('99999999-0000-0000-0000-000000000001',
		        'test-client-id', 'Test App',
		        '["https://app.example.com/callback"]',
		        '["authorization_code","refresh_token"]')`)

	// ── oauth_authorization_codes ──────────────────────────────────────────
	mustExec(`INSERT INTO oauth_authorization_codes
		(code, client_id, user_id, redirect_uri, scope, expires_at)
		VALUES ('testcode123', 'test-client-id',
		        'cccccccc-0000-0000-0000-000000000001',
		        'https://app.example.com/callback', 'read',
		        strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '+10 minutes')))`)

	// ── oauth_refresh_tokens ───────────────────────────────────────────────
	mustExec(`INSERT INTO oauth_refresh_tokens
		(token_hash, client_id, user_id, scope)
		VALUES ('refreshhash123', 'test-client-id',
		        'cccccccc-0000-0000-0000-000000000001', 'read')`)

	// ── oauth_idp_configs (allowed_domains = JSON text array) ──────────────
	mustExec(`INSERT INTO oauth_idp_configs (id, org_id, provider_type, client_id, client_secret,
		                                       allowed_domains, default_role)
		VALUES ('aaaaaaaa-1111-0000-0000-000000000001',
		        'bbbbbbbb-0000-0000-0000-000000000001',
		        'google', 'google-client-id', 'google-client-secret',
		        '["example.com"]', 'member')`)
}

// cleanPostgres truncates all migrated tables in reverse dependency order so
// the test can be run multiple times without leftover data.
func cleanPostgres(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	// Reverse of migratedTables order to respect FK constraints.
	tables := []string{
		"oauth_idp_configs",
		"oauth_refresh_tokens",
		"oauth_authorization_codes",
		"oauth_clients",
		"memory_shares",
		"webhooks",
		"enrichment_queue",
		"ingestion_log",
		"memory_lineage",
		"relationships",
		"entity_aliases",
		"entities",
		"memories",
		"system_meta",
		"settings",
		"projects",
		"api_keys",
		"users",
		"organizations",
		"namespaces",
	}
	for _, table := range tables {
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table+" CASCADE"); err != nil {
			t.Logf("cleanup truncate %s: %v", table, err)
		}
	}
	// Re-insert the root namespace that the migrations expect.
	_, _ = db.ExecContext(ctx, `
		INSERT INTO namespaces (id, name, slug, kind, path, depth)
		VALUES ('00000000-0000-0000-0000-000000000000', 'root', 'root', 'root', '', 0)
		ON CONFLICT DO NOTHING
	`)
}

func TestDataMigrator_SQLiteToPostgres(t *testing.T) {
	if !postgresAvailable() {
		t.Skip("postgres not reachable at " + testPostgresURL)
	}

	ctx := context.Background()

	// Build source SQLite database with test data.
	srcDB := openSQLiteInMemory(t)
	defer srcDB.Close()
	seedSQLite(t, srcDB)

	// Open the Postgres target and clean it before the test.
	pgDB, err := sql.Open("pgx", testPostgresURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer pgDB.Close()
	cleanPostgres(t, pgDB)
	pgDB.Close() // DataMigrator will open its own connection.

	// Run the migration.
	dm, err := newDataMigrator(ctx, srcDB, testPostgresURL)
	if err != nil {
		t.Fatalf("newDataMigrator: %v", err)
	}
	defer dm.Close()

	if err := dm.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify row counts per table.
	pgConn, err := sql.Open("pgx", testPostgresURL)
	if err != nil {
		t.Fatalf("open postgres for verification: %v", err)
	}
	defer pgConn.Close()

	t.Log("verifying row counts …")
	for _, table := range migratedTables {
		var srcCount, dstCount int
		if err := srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&srcCount); err != nil {
			t.Logf("  SKIP %s (not in sqlite): %v", table, err)
			continue
		}
		if err := pgConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&dstCount); err != nil {
			t.Errorf("  FAIL count %s in postgres: %v", table, err)
			continue
		}
		// system_meta receives an extra 'storage_backend' row after data copy,
		// so expect dstCount == srcCount + 1 for that table.
		expectedDst := srcCount
		if table == "system_meta" {
			expectedDst = srcCount + 1
		}
		if dstCount != expectedDst {
			t.Errorf("  FAIL %s: sqlite=%d postgres=%d (expected %d)", table, srcCount, dstCount, expectedDst)
		} else {
			t.Logf("  OK   %s: %d rows", table, dstCount)
		}
	}

	// Verify storage_backend system_meta entry.
	var backendVal string
	if err := pgConn.QueryRowContext(ctx,
		"SELECT value FROM system_meta WHERE key = 'storage_backend'",
	).Scan(&backendVal); err != nil {
		t.Errorf("read storage_backend: %v", err)
	} else if backendVal != "postgres" {
		t.Errorf("storage_backend = %q, want %q", backendVal, "postgres")
	}

	// Cleanup Postgres after test.
	cleanPostgres(t, pgConn)

	t.Log("migration test complete")
}

func TestDataMigrator_ArrayConversions(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`[]`, `{}`},
		{`["a"]`, `{"a"}`},
		{`["a","b","c"]`, `{"a","b","c"}`},
		{`["tag1","tag2"]`, `{"tag1","tag2"}`},
		{`["has \"quote\""]`, `{"has \"quote\""}`},
		{``, `{}`},
		{`null`, `{}`},
	}
	for _, tt := range tests {
		got, err := jsonArrayToPostgresTextArray(tt.input)
		if err != nil {
			t.Errorf("input=%q err=%v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("input=%q got=%q want=%q", tt.input, got, tt.want)
		}
	}
}

func TestDatabaseAdminStore_TestConnection(t *testing.T) {
	if !postgresAvailable() {
		t.Skip("postgres not reachable at " + testPostgresURL)
	}

	srcDB, err := sql.Open("sqlite", "file::memory:?cache=shared&_test_conn=1")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer srcDB.Close()

	// We need a storage.DB to build the store; use a minimal wrapper via the
	// package-level openSQLiteInMemory helper.
	sqliteDB := openSQLiteInMemory(t)
	defer sqliteDB.Close()

	// Wrap in a dbWrapper so we can build the store.
	store := &DatabaseAdminStore{db: &testSQLiteDB{db: sqliteDB}}

	ctx := context.Background()
	result, err := store.TestConnection(ctx, testPostgresURL)
	if err != nil {
		t.Fatalf("TestConnection returned error: %v", err)
	}
	if !result.Success {
		t.Errorf("TestConnection.Success = false: %s", result.Message)
	}
	if result.LatencyMs < 0 {
		t.Errorf("unexpected negative latency: %d", result.LatencyMs)
	}
	t.Logf("pgvector installed: %v, latency: %dms", result.PgvectorInstalled, result.LatencyMs)
}

func TestDatabaseAdminStore_TriggerMigration_RejectsNonSQLite(t *testing.T) {
	store := &DatabaseAdminStore{db: &testPostgresDB{}}
	ctx := context.Background()
	status, err := store.TriggerMigration(ctx, testPostgresURL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.Status != "error" {
		t.Errorf("expected status=error for postgres source, got %q", status.Status)
	}
}

// ── minimal storage.DB stubs for unit tests ───────────────────────────────────

type testSQLiteDB struct {
	db *sql.DB
}

func (d *testSQLiteDB) Backend() string { return "sqlite" }
func (d *testSQLiteDB) Ping(ctx context.Context) error { return d.db.PingContext(ctx) }
func (d *testSQLiteDB) Close() error                  { return d.db.Close() }
func (d *testSQLiteDB) DB() *sql.DB                   { return d.db }
func (d *testSQLiteDB) Exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, q, args...)
}
func (d *testSQLiteDB) Query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, q, args...)
}
func (d *testSQLiteDB) QueryRow(ctx context.Context, q string, args ...any) *sql.Row {
	return d.db.QueryRowContext(ctx, q, args...)
}
func (d *testSQLiteDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.db.BeginTx(ctx, opts)
}

type testPostgresDB struct{}

func (d *testPostgresDB) Backend() string { return "postgres" }
func (d *testPostgresDB) Ping(_ context.Context) error { return nil }
func (d *testPostgresDB) Close() error                 { return nil }
func (d *testPostgresDB) DB() *sql.DB                  { return nil }
func (d *testPostgresDB) Exec(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, fmt.Errorf("not implemented")
}
func (d *testPostgresDB) Query(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("not implemented")
}
func (d *testPostgresDB) QueryRow(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}
func (d *testPostgresDB) BeginTx(_ context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	return nil, fmt.Errorf("not implemented")
}
