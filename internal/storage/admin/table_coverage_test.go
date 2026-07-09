package admin

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// virtualTableSkipper returns a predicate that reports whether a table name is a
// virtual table or one of its auto-created shadow tables. Virtual tables carry a
// "CREATE VIRTUAL TABLE ..." definition in sqlite_master; their shadow tables are
// named "<virtual>_<suffix>" and have no such definition. Neither is nram data,
// so the migrator ignores them.
func virtualTableSkipper(t *testing.T, db *sql.DB) func(string) bool {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND sql LIKE 'CREATE VIRTUAL TABLE%'`)
	if err != nil {
		t.Fatalf("enumerate virtual tables: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var virtual []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan virtual table: %v", err)
		}
		virtual = append(virtual, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate virtual tables: %v", err)
	}

	return func(tbl string) bool {
		for _, v := range virtual {
			if tbl == v || strings.HasPrefix(tbl, v+"_") {
				return true
			}
		}
		return false
	}
}

// vectorPathTables are the SQLite tables intentionally absent from migratedTables:
//   - memory_vectors / entity_vectors are copied by the dedicated vector path
//     (the migrateMemoryVectors / migrateEntityVectors tasks in Run), which fans
//     each single SQLite table out into the per-dimension Postgres tables.
//   - hnsw_snapshots / entity_hnsw_snapshots are SQLite-only HNSW graph
//     persistence with no Postgres counterpart (pgvector maintains its own
//     index), so they are not migrated at all.
var vectorPathTables = map[string]bool{
	"memory_vectors":        true,
	"entity_vectors":        true,
	"hnsw_snapshots":        true,
	"entity_hnsw_snapshots": true,
}

// TestMigratedTablesCoverSQLiteSchema enumerates every table in a freshly
// migrated SQLite schema and asserts each is either copied by the data migrator
// (present in migratedTables) or explicitly excluded with a documented reason.
// This turns "added a table, forgot to register it in migratedTables" into a
// build failure instead of a silently dropped table on a SQLite->Postgres
// switch (as happened once with log_entries).
func TestMigratedTablesCoverSQLiteSchema(t *testing.T) {
	sqliteDB := openSQLiteInMemory(t)
	defer func() { _ = sqliteDB.Close() }()

	tables, err := existingSQLiteTables(context.Background(), sqliteDB)
	if err != nil {
		t.Fatalf("enumerate sqlite tables: %v", err)
	}

	// FTS5 virtual tables (e.g. memories_fts) auto-create shadow tables
	// (<name>_data, _idx, _content, _docsize, _config) that appear as type='table'
	// in sqlite_master. They are a SQLite-only derived index rebuilt from the base
	// table (memories), have no Postgres counterpart, and are not migrated. Skip
	// each virtual table and its shadow tables generically so a new FTS index does
	// not need five suffixes hand-listed.
	isVirtualOrShadow := virtualTableSkipper(t, sqliteDB)

	migrated := toSet(migratedTables)

	for tbl := range tables {
		// SQLite internal tables (sqlite_sequence, sqlite_stat*, ...) are never
		// nram data.
		if strings.HasPrefix(tbl, "sqlite_") || isVirtualOrShadow(tbl) {
			continue
		}
		if _, ok := migrated[tbl]; ok {
			continue
		}
		// schema_migrations is golang-migrate bookkeeping, not nram data; the
		// vector-path tables are handled/excluded per vectorPathTables above.
		if tbl == "schema_migrations" || vectorPathTables[tbl] {
			continue
		}
		t.Errorf("table %q exists in the SQLite schema but is not in migratedTables "+
			"(register it in data_migrator.go, or add it to the documented exclusions "+
			"in this file if it is handled elsewhere)", tbl)
	}

	// Reverse direction: every migratedTables entry must exist in the schema, so
	// a renamed or dropped table cannot linger silently in the copy list.
	for _, tbl := range migratedTables {
		if !tables[tbl] {
			t.Errorf("migratedTables lists %q but it does not exist in the SQLite schema "+
				"(remove it or fix the name)", tbl)
		}
	}

	// Rot check: every vector-path exclusion must still exist in the schema, so a
	// dropped or renamed table cannot linger silently in the exclusion set.
	for tbl := range vectorPathTables {
		if !tables[tbl] {
			t.Errorf("vectorPathTables lists %q but it no longer exists in the SQLite "+
				"schema (drop the stale exclusion)", tbl)
		}
	}
}

// TestPreflightTablesMatchMigrated ties the Postgres-domain reset/preflight list
// to the SQLite-domain copy list without needing a live Postgres. The two lists
// must describe the same logical set of tables, differing only by the
// per-dimension vector-table fan-out on the Postgres side. Together with
// TestMigratedTablesCoverSQLiteSchema this makes a newly added table a build
// failure unless it is registered in both lists.
func TestPreflightTablesMatchMigrated(t *testing.T) {
	preflight := toSet(preflightTables)

	// Every migrated table must be reset-covered, so a failed/partial migration
	// can be fully truncated before a retry.
	for _, tbl := range migratedTables {
		if _, ok := preflight[tbl]; !ok {
			t.Errorf("migratedTables lists %q but preflightTables does not "+
				"(it would be copied but never reset); register it in database_preflight.go", tbl)
		}
	}

	migrated := toSet(migratedTables)

	// The only entries preflightTables may carry beyond migratedTables are the
	// per-dimension Postgres vector tables (the Postgres target splits the single
	// SQLite memory_vectors / entity_vectors tables per embedding dimension). Key
	// off the same lists the migrator derives from storage.SupportedVectorDimensions
	// so the allowed set stays in lockstep with the supported dimensions rather
	// than re-encoding the naming convention here.
	allowedExtras := toSet(vectorDimensionTables)
	for _, tbl := range entityVectorDimensionTables {
		allowedExtras[tbl] = struct{}{}
	}

	for _, tbl := range preflightTables {
		if _, ok := migrated[tbl]; ok {
			continue
		}
		if _, ok := allowedExtras[tbl]; ok {
			continue
		}
		t.Errorf("preflightTables lists %q which is neither in migratedTables nor a "+
			"supported per-dimension vector table; add it to migratedTables, or if it "+
			"is intentionally preflight-only, extend the allowed-extras check with a "+
			"documented reason", tbl)
	}
}
