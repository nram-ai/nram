package admin

import (
	"context"
	"database/sql"
	"testing"

	"github.com/nram-ai/nram/internal/api"
)

func TestMigrationAudit_CleanDB(t *testing.T) {
	sqliteDB := openSQLiteInMemory(t)
	defer sqliteDB.Close()
	store := &DatabaseAdminStore{db: &testSQLiteDB{db: sqliteDB}}

	audit, err := store.MigrationAudit(context.Background())
	if err != nil {
		t.Fatalf("MigrationAudit: %v", err)
	}
	if audit.Backend != "sqlite" {
		t.Errorf("expected backend=sqlite, got %q", audit.Backend)
	}
	if audit.TotalOrphans != 0 {
		t.Errorf("expected 0 orphans on clean DB, got %d: %+v", audit.TotalOrphans, audit.Orphans)
	}
	if len(audit.Orphans) != 0 {
		t.Errorf("expected empty orphan list on clean DB, got %+v", audit.Orphans)
	}
}

func TestMigrationAudit_DetectsOrphans(t *testing.T) {
	sqliteDB := openSQLiteInMemory(t)
	defer sqliteDB.Close()

	// Disable FK enforcement to seed deliberate orphans — mimics production SQLite
	// accumulating orphans over time with PRAGMA foreign_keys=OFF.
	if _, err := sqliteDB.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatalf("disable FK: %v", err)
	}

	ctx := context.Background()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := sqliteDB.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed: %s\nerr: %v", q, err)
		}
	}

	// Build a minimal valid parent graph (org, user, namespace).
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, path, depth, metadata, created_at, updated_at)
		VALUES ('11111111-0000-0000-0000-000000000001', 'OrgNS', 'org', 'organization',
		        'org', 1, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO organizations (id, namespace_id, name, slug, settings, created_at, updated_at)
		VALUES ('22222222-0000-0000-0000-000000000001', '11111111-0000-0000-0000-000000000001',
		        'Org', 'org', '{}', '2025-01-01', '2025-01-01')`)

	// --- orphan 1: memories.namespace_id → non-existent namespace ---
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000001', '99999999-9999-9999-9999-999999999999',
		        'orphan', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)

	// Two valid memories that reference each other via superseded_by.
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000002', '11111111-0000-0000-0000-000000000001',
		        'valid-a', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000003', '11111111-0000-0000-0000-000000000001',
		        'valid-b', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)

	// --- orphan 2: memories.superseded_by → deleted memory ---
	mustExec(`UPDATE memories SET superseded_by = '99999999-0000-0000-0000-000000000999'
	          WHERE id = '33333333-0000-0000-0000-000000000002'`)

	// --- orphan 3 + 4: two relationships.source_memory → non-existent memory ---
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type, properties, mention_count, metadata, created_at, updated_at)
		VALUES ('44444444-0000-0000-0000-000000000001', '11111111-0000-0000-0000-000000000001',
		        'E1', 'e1', 'concept', '{}', 1, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type, properties, mention_count, metadata, created_at, updated_at)
		VALUES ('44444444-0000-0000-0000-000000000002', '11111111-0000-0000-0000-000000000001',
		        'E2', 'e2', 'concept', '{}', 1, '{}', '2025-01-01', '2025-01-01')`)

	mustExec(`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, source_memory, created_at)
		VALUES ('55555555-0000-0000-0000-000000000001', '11111111-0000-0000-0000-000000000001',
		        '44444444-0000-0000-0000-000000000001', '44444444-0000-0000-0000-000000000002',
		        'related', 1.0, '{}', '2025-01-01',
		        '99999999-0000-0000-0000-000000aaaaaa', '2025-01-01')`)
	mustExec(`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, source_memory, created_at)
		VALUES ('55555555-0000-0000-0000-000000000002', '11111111-0000-0000-0000-000000000001',
		        '44444444-0000-0000-0000-000000000001', '44444444-0000-0000-0000-000000000002',
		        'related2', 1.0, '{}', '2025-01-01',
		        '99999999-0000-0000-0000-000000bbbbbb', '2025-01-01')`)

	store := &DatabaseAdminStore{db: &testSQLiteDB{db: sqliteDB}}
	audit, err := store.MigrationAudit(ctx)
	if err != nil {
		t.Fatalf("MigrationAudit: %v", err)
	}

	// Expect exactly: 1 orphan on memories.namespace_id, 1 on memories.superseded_by,
	// 2 on relationships.source_memory. Total = 4.
	if audit.TotalOrphans != 4 {
		t.Errorf("expected 4 total orphans, got %d (detail: %+v)", audit.TotalOrphans, audit.Orphans)
	}

	found := map[string]int{}
	for _, o := range audit.Orphans {
		found[o.Table+"."+o.Column] = o.Count
	}
	assertCount := func(key string, want int) {
		t.Helper()
		if got := found[key]; got != want {
			t.Errorf("%s: got %d orphans, want %d", key, got, want)
		}
	}
	assertCount("memories.namespace_id", 1)
	assertCount("memories.superseded_by", 1)
	assertCount("relationships.source_memory", 2)

	// Output should be sorted by count desc then table.column.
	for i := 1; i < len(audit.Orphans); i++ {
		if audit.Orphans[i-1].Count < audit.Orphans[i].Count {
			t.Errorf("orphans not sorted by count desc at index %d: %+v", i, audit.Orphans)
		}
	}
}

func TestMigrationAudit_RejectsPostgresBackend(t *testing.T) {
	store := &DatabaseAdminStore{db: &testPostgresDB{}}
	_, err := store.MigrationAudit(context.Background())
	if err == nil {
		t.Fatal("expected error when backend is postgres")
	}
}

// sanity-check that the FK relation list matches reality: every parent/child
// table in sqliteFKRelations should exist in a freshly migrated SQLite DB.
func TestMigrationAudit_AllRelationsReferExistingTables(t *testing.T) {
	sqliteDB := openSQLiteInMemory(t)
	defer sqliteDB.Close()

	tables, err := existingSQLiteTables(context.Background(), sqliteDB)
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}

	// Verify every FK in the static list references tables that exist in a
	// fresh SQLite schema. If this fails, either the migrations changed or
	// the sqliteFKRelations list is stale.
	for _, rel := range sqliteFKRelations {
		if !tables[rel.ChildTable] {
			t.Errorf("fkRelations references missing child table %q", rel.ChildTable)
		}
		if !tables[rel.ParentTable] {
			t.Errorf("fkRelations references missing parent table %q", rel.ParentTable)
		}
	}
}

// smoke helper: ensure the audit query compiles and runs for every FK without
// syntax error against a fresh SQLite schema (even with zero rows).
func TestMigrationAudit_QueriesCompile(t *testing.T) {
	sqliteDB := openSQLiteInMemory(t)
	defer sqliteDB.Close()

	for _, rel := range sqliteFKRelations {
		if _, err := countOrphans(context.Background(), sqliteDB, rel); err != nil {
			t.Errorf("countOrphans(%s): %v", rel, err)
		}
	}
}

var _ = sql.ErrNoRows // keep sql import for future expansion
var _ = api.AuditError{}
