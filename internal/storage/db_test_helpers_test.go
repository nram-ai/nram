package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
)

// jsonEqual compares two JSON strings for semantic equality, ignoring
// formatting differences such as whitespace after colons and commas
// (e.g. Postgres JSONB normalization). Falls back to string comparison
// if either value is not valid JSON.
func jsonEqual(a, b string) bool {
	var ja, jb interface{}
	if err := json.Unmarshal([]byte(a), &ja); err != nil {
		return a == b
	}
	if err := json.Unmarshal([]byte(b), &jb); err != nil {
		return a == b
	}
	return reflect.DeepEqual(ja, jb)
}

// sharedPostgresDB is a package-level Postgres DB used by all tests.
// It is initialized once in TestMain (see db_test_main_test.go).
var sharedPostgresDB DB
var sharedPostgresSchema string
var sharedPostgresSetupDB *sql.DB

// setupSharedPostgres initializes the shared Postgres test database.
// Called from TestMain. Returns false if DATABASE_URL is not set.
func setupSharedPostgres() bool {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return false
	}

	schema := "test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")

	setupDB, err := sql.Open("pgx", url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to postgres: %v\n", err)
		return false
	}

	if _, err := setupDB.Exec("CREATE SCHEMA " + schema); err != nil {
		setupDB.Close()
		fmt.Fprintf(os.Stderr, "failed to create test schema: %v\n", err)
		return false
	}

	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	testURL := url + sep + "search_path=" + schema + ",public"

	db, err := Open(config.DatabaseConfig{URL: testURL})
	if err != nil {
		setupDB.Exec("DROP SCHEMA " + schema + " CASCADE")
		setupDB.Close()
		fmt.Fprintf(os.Stderr, "failed to open postgres with test schema: %v\n", err)
		return false
	}

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		db.Close()
		setupDB.Exec("DROP SCHEMA " + schema + " CASCADE")
		setupDB.Close()
		fmt.Fprintf(os.Stderr, "failed to create migrator: %v\n", err)
		return false
	}
	if err := migrator.Up(); err != nil {
		db.Close()
		setupDB.Exec("DROP SCHEMA " + schema + " CASCADE")
		setupDB.Close()
		fmt.Fprintf(os.Stderr, "failed to run postgres migrations: %v\n", err)
		return false
	}

	sharedPostgresDB = db
	sharedPostgresSchema = schema
	sharedPostgresSetupDB = setupDB
	return true
}

// teardownSharedPostgres cleans up the shared Postgres test database.
// Called from TestMain.
func teardownSharedPostgres() {
	if sharedPostgresDB != nil {
		sharedPostgresDB.Close()
	}
	if sharedPostgresSetupDB != nil && sharedPostgresSchema != "" {
		sharedPostgresSetupDB.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", sharedPostgresSchema))
		sharedPostgresSetupDB.Close()
	}
}

// forEachDB runs the given test function against both SQLite and (if
// DATABASE_URL is set) Postgres as subtests.
func forEachDB(t *testing.T, fn func(t *testing.T, db DB)) {
	t.Helper()

	t.Run("sqlite", func(t *testing.T) {
		db := testDBWithMigrations(t)
		fn(t, db)
	})

	t.Run("postgres", func(t *testing.T) {
		if sharedPostgresDB == nil {
			t.Skip("DATABASE_URL not set; skipping Postgres test")
		}
		fn(t, sharedPostgresDB)
	})
}

// truncateAllForTest clears every test-relevant table so the next test sees a
// blank slate. Required only for tests that exercise whole-DB scans
// (NormalizeMemoryTags, EnqueueAllLiveMemories) or otherwise rely on row
// counts: SQLite tests already get a fresh DB via testDBWithMigrations, but
// the Postgres path shares one schema across tests and accumulates rows
// across runs. Per-test schemas would be the more general fix; this is a
// targeted truncate for the affected tests.
//
// CASCADE handles the FK chains (entity_aliases, relationships, memory_lineage,
// enrichment_queue, etc.) added in 000007 / 000020 / 000023 / 000032 / 000035.
func truncateAllForTest(t *testing.T, db DB) {
	t.Helper()
	if db.Backend() != BackendPostgres {
		return
	}
	ctx := context.Background()
	tables := []string{
		"enrichment_queue",
		"memory_lineage",
		"ingestion_log",
		"token_usage",
		"relationships",
		"entity_aliases",
		"entities",
		"memories",
	}
	for _, table := range tables {
		if _, err := db.Exec(ctx, "TRUNCATE TABLE "+table+" CASCADE"); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}
