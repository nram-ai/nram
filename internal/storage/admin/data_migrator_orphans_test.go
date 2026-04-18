package admin

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestTextToJSONB_HandlesProblematicText verifies the JSONB encoder survives
// the inputs that broke the initial migrator on production data:
// backslashes, newlines, embedded quotes, and control characters that must
// be escaped in a JSON string but were being passed through verbatim by the
// old manual-quoting approach (SQLSTATE 22P02).
func TestTextToJSONB_HandlesProblematicText(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"backslash", `C:\path\to\file`},
		{"newline", "line one\nline two"},
		{"tab and quote", "tab\there and \"quote\""},
		{"null byte stripped", "\x00before\x00after\x00"},
		{"control chars", "\x01\x02\x07\x1b"},
		{"mixed", "ERROR: failed to parse \"C:\\x\": unexpected\ntoken"},
		{"unicode", "café — 日本語"},
		{"already valid json", `{"code":"X","detail":"y"}`},
		{"json array", `["a","b"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := textToJSONB(sql.NullString{String: tc.input, Valid: true})
			if err != nil {
				t.Fatalf("textToJSONB err: %v", err)
			}
			s, ok := v.(string)
			if !ok {
				t.Fatalf("expected string, got %T", v)
			}
			// Round-trip through Postgres to prove JSONB accepts it.
			db, err := sql.Open("pgx", resolvedPostgresURL)
			if err != nil {
				t.Fatalf("open pg: %v", err)
			}
			defer db.Close()
			var got string
			if err := db.QueryRowContext(context.Background(),
				"SELECT $1::jsonb::text", s,
			).Scan(&got); err != nil {
				t.Fatalf("postgres rejected encoded jsonb: %v\nencoded: %q", err, s)
			}
		})
	}
}

// TestDataMigrator_FinalizesStuckJobs verifies the post-copy finalization
// resets enrichment_queue processing rows back to pending and marks
// non-terminal dream_cycles as failed, so the Postgres instance isn't stuck
// waiting on workers that no longer exist.
func TestDataMigrator_FinalizesStuckJobs(t *testing.T) {
	ctx := context.Background()

	srcDB := openSQLiteInMemory(t)
	defer srcDB.Close()
	if _, err := srcDB.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatalf("disable FKs: %v", err)
	}

	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := srcDB.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed: %s\nerr: %v", q, err)
		}
	}
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, path, depth, metadata, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000001', 'NS', 'ns', 'organization', 'ns', 1, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000002', 'PNS', 'pns', 'project',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'ns/pns', 2, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at)
		VALUES ('dddddddd-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'P', 'p', '', '[]', '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'm', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)

	// 2 stuck enrichment jobs + 1 already-pending (should stay pending) + 1 completed (untouched).
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, claimed_at, claimed_by, attempts, steps_completed, created_at, updated_at)
		VALUES ('77777777-0000-0000-0000-000000000001', '33333333-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'processing', 0, '2025-01-01', 'worker-1', 0, '[]', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, claimed_at, claimed_by, attempts, steps_completed, created_at, updated_at)
		VALUES ('77777777-0000-0000-0000-000000000002', '33333333-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'processing', 0, '2025-01-01', 'worker-2', 0, '[]', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, steps_completed, created_at, updated_at)
		VALUES ('77777777-0000-0000-0000-000000000003', '33333333-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'pending', 0, 0, '[]', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, attempts, steps_completed, completed_at, created_at, updated_at)
		VALUES ('77777777-0000-0000-0000-000000000004', '33333333-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'completed', 0, 0, '[]', '2025-01-01', '2025-01-01', '2025-01-01')`)

	// 3 stuck dream cycles (2 running, 1 pending) + 1 completed (untouched).
	for i, st := range []string{"running", "running", "pending"} {
		mustExec(`INSERT INTO dream_cycles (id, project_id, namespace_id, status, phase, tokens_used, token_budget, phase_summary, created_at, updated_at)
			VALUES (?, 'dddddddd-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000002',
			        ?, 'analyze', 0, 1000, '{}', '2025-01-01', '2025-01-01')`,
			fmt.Sprintf("88888888-0000-0000-0000-00000000000%d", i+1), st)
	}
	mustExec(`INSERT INTO dream_cycles (id, project_id, namespace_id, status, phase, tokens_used, token_budget, phase_summary, completed_at, created_at, updated_at)
		VALUES ('88888888-0000-0000-0000-000000000099', 'dddddddd-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002', 'completed', 'done', 500, 1000, '{}',
		        '2025-01-02', '2025-01-01', '2025-01-02')`)

	pgDB, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	cleanPostgres(t, pgDB)
	pgDB.Close()

	dm, err := newDataMigrator(ctx, srcDB, resolvedPostgresURL)
	if err != nil {
		t.Fatalf("newDataMigrator: %v", err)
	}
	defer dm.Close()
	if err := dm.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	stats := dm.Stats()
	if stats.ResetStuck["enrichment_queue"] != 2 {
		t.Errorf("enrichment reset = %d, want 2 (full: %+v)", stats.ResetStuck["enrichment_queue"], stats.ResetStuck)
	}
	if stats.ResetStuck["dream_cycles"] != 3 {
		t.Errorf("dream_cycles reset = %d, want 3 (full: %+v)", stats.ResetStuck["dream_cycles"], stats.ResetStuck)
	}

	pg, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("reopen pg: %v", err)
	}
	defer pg.Close()

	// Previously-processing rows are now pending with cleared claim fields.
	var pending int
	if err := pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM enrichment_queue
		 WHERE status = 'pending' AND claimed_at IS NULL AND claimed_by IS NULL`,
	).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 3 {
		t.Errorf("pending enrichment rows = %d, want 3 (2 reset + 1 originally pending)", pending)
	}

	// Completed rows were left alone.
	var completed int
	if err := pg.QueryRowContext(ctx, `SELECT COUNT(*) FROM enrichment_queue WHERE status = 'completed'`).Scan(&completed); err != nil {
		t.Fatalf("count completed: %v", err)
	}
	if completed != 1 {
		t.Errorf("completed enrichment rows = %d, want 1", completed)
	}

	// Stuck dream cycles are now failed with an error message; the completed one is untouched.
	var failed, dreamsCompleted int
	if err := pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM dream_cycles WHERE status = 'failed' AND error IS NOT NULL`,
	).Scan(&failed); err != nil {
		t.Fatalf("count failed dreams: %v", err)
	}
	if failed != 3 {
		t.Errorf("failed dream_cycles = %d, want 3", failed)
	}
	if err := pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM dream_cycles WHERE status = 'completed'`,
	).Scan(&dreamsCompleted); err != nil {
		t.Fatalf("count completed dreams: %v", err)
	}
	if dreamsCompleted != 1 {
		t.Errorf("completed dream_cycles = %d, want 1", dreamsCompleted)
	}
}

func TestSanitizeJSONB(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		fallback string
		want     string
	}{
		{"empty string falls back", "", "{}", "{}"},
		{"whitespace falls back", "   \t\n  ", "{}", "{}"},
		{"null bytes stripped and empty falls back", "\x00\x00", "{}", "{}"},
		{"malformed json falls back", "not json", "{}", "{}"},
		{"truncated object falls back", `{"a":`, "{}", "{}"},
		{"valid object passes", `{"a":1}`, "{}", `{"a":1}`},
		{"valid array passes", `[1,2,3]`, "[]", `[1,2,3]`},
		{"valid with null bytes stripped", "\x00{\"a\":1}\x00", "{}", `{"a":1}`},
		{"nested object", `{"a":{"b":[1,2]}}`, "{}", `{"a":{"b":[1,2]}}`},
		{"array fallback for bad input", "garbage", "[]", "[]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeJSONB(tc.input, tc.fallback)
			if got != tc.want {
				t.Errorf("sanitizeJSONB(%q, %q) = %q, want %q", tc.input, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestTextToJSONB_NullAndEmpty(t *testing.T) {
	v, err := textToJSONB(sql.NullString{Valid: false})
	if err != nil || v != nil {
		t.Errorf("NULL -> want (nil, nil), got (%v, %v)", v, err)
	}
	v, err = textToJSONB(sql.NullString{String: "", Valid: true})
	if err != nil || v != nil {
		t.Errorf("empty string -> want (nil, nil), got (%v, %v)", v, err)
	}
}

// TestDataMigrator_DropsOrphansAndSetsSupersededBy verifies the Phase 3 fixes:
//   - orphan rows pointing at missing FK parents are skipped, not errored
//   - self-referential memories.superseded_by is populated in pass 2
//   - new dreaming + webauthn tables are migrated
//
// This is the regression test for the 15K-memory production bug where
// accumulated SQLite orphans aborted the migration with Postgres FK errors.
func TestDataMigrator_DropsOrphansAndSetsSupersededBy(t *testing.T) {
	ctx := context.Background()

	srcDB := openSQLiteInMemory(t)
	defer srcDB.Close()

	// Turn FKs off in source so we can seed deliberate orphans (matching
	// production SQLite that ran for years with PRAGMA foreign_keys=OFF).
	if _, err := srcDB.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatalf("disable FKs: %v", err)
	}
	seedOrphanFixture(t, srcDB)

	pgDB, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer pgDB.Close()
	cleanPostgres(t, pgDB)
	pgDB.Close()

	dm, err := newDataMigrator(ctx, srcDB, resolvedPostgresURL)
	if err != nil {
		t.Fatalf("newDataMigrator: %v", err)
	}
	defer dm.Close()

	if err := dm.Run(ctx); err != nil {
		t.Fatalf("Run (should succeed despite orphans): %v", err)
	}

	stats := dm.Stats()

	// Verify the orphan skips we deliberately introduced are recorded.
	want := map[string]int{
		"memories.namespace_id":         1, // 1 memory with bad namespace
		"relationships.source_memory":   2, // 2 relationships with bad source_memory
		"memory_lineage.parent_id":      1, // 1 lineage with bad parent_id
		"enrichment_queue.memory_id":    1, // 1 enrichment entry with bad memory_id
	}
	for key, want := range want {
		if got := stats.SkippedOrphans[key]; got != want {
			t.Errorf("SkippedOrphans[%q] = %d, want %d (full: %+v)", key, got, want, stats.SkippedOrphans)
		}
	}

	// Verify the column-update skip: memory M2 had superseded_by pointing at a
	// memory we dropped, so pass 2 should record that as a skipped update.
	if got := stats.SkippedUpdates["memories.superseded_by"]; got < 1 {
		t.Errorf("expected at least 1 skipped superseded_by update, got %d (full: %+v)", got, stats.SkippedUpdates)
	}

	// Verify valid self-ref DID land: M3.superseded_by = M4, both valid.
	pg, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer pg.Close()
	var supersededBy sql.NullString
	if err := pg.QueryRowContext(ctx,
		"SELECT superseded_by FROM memories WHERE id = $1",
		"33333333-0000-0000-0000-000000000003",
	).Scan(&supersededBy); err != nil {
		t.Fatalf("read M3: %v", err)
	}
	if !supersededBy.Valid || supersededBy.String != "33333333-0000-0000-0000-000000000004" {
		t.Errorf("M3.superseded_by = %+v, want 33333333-0000-0000-0000-000000000004", supersededBy)
	}

	// Verify dream_cycles was actually migrated.
	var dreamCount int
	if err := pg.QueryRowContext(ctx, "SELECT COUNT(*) FROM dream_cycles").Scan(&dreamCount); err != nil {
		t.Fatalf("count dream_cycles: %v", err)
	}
	if dreamCount != 1 {
		t.Errorf("dream_cycles count = %d, want 1", dreamCount)
	}
}

// seedOrphanFixture inserts a minimal graph with several deliberate orphans
// that would each trigger a Postgres FK error under the old migrator.
func seedOrphanFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed: %s\nerr: %v", q, err)
		}
	}

	// Parent graph: 1 namespace, 1 org, 1 user, 1 project.
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, path, depth, metadata, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000001', 'NS', 'ns', 'organization',
		        'ns', 1, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000002', 'ProjectNS', 'pns', 'project',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'ns/pns', 2, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO organizations (id, namespace_id, name, slug, settings, created_at, updated_at)
		VALUES ('bbbbbbbb-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'Org', 'org', '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO users (id, email, display_name, password_hash, org_id, namespace_id, role, settings, created_at, updated_at)
		VALUES ('cccccccc-0000-0000-0000-000000000001', 'u@x.com', 'U', NULL,
		        'bbbbbbbb-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'member', '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at)
		VALUES ('dddddddd-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'P', 'p', '', '[]', '{}',
		        '2025-01-01', '2025-01-01')`)

	// --- orphan M1: memory with bad namespace_id ---
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000001', '99999999-9999-9999-9999-999999999999',
		        'orphan', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)

	// Valid memories M2, M3, M4.
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000002', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'm2', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000003', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'm3', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO memories (id, namespace_id, content, confidence, importance, access_count, enriched, metadata, created_at, updated_at)
		VALUES ('33333333-0000-0000-0000-000000000004', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'm4', 1.0, 0.5, 0, 0, '{}', '2025-01-01', '2025-01-01')`)

	// M2.superseded_by → dropped memory (orphan after M1 is dropped; M2's target is the bogus id).
	mustExec(`UPDATE memories SET superseded_by = '99999999-0000-0000-0000-0000000000ff'
	          WHERE id = '33333333-0000-0000-0000-000000000002'`)
	// M3.superseded_by → M4 (valid self-ref; should land after pass 2).
	mustExec(`UPDATE memories SET superseded_by = '33333333-0000-0000-0000-000000000004'
	          WHERE id = '33333333-0000-0000-0000-000000000003'`)

	// Entity + 2 orphan relationships (source_memory → non-existent memory).
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type, properties, mention_count, metadata, created_at, updated_at)
		VALUES ('44444444-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'E1', 'e1', 'concept', '{}', 1, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type, properties, mention_count, metadata, created_at, updated_at)
		VALUES ('44444444-0000-0000-0000-000000000002', 'aaaaaaaa-0000-0000-0000-000000000001',
		        'E2', 'e2', 'concept', '{}', 1, '{}', '2025-01-01', '2025-01-01')`)
	mustExec(`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, source_memory, created_at)
		VALUES ('55555555-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000001',
		        '44444444-0000-0000-0000-000000000001', '44444444-0000-0000-0000-000000000002',
		        'r1', 1.0, '{}', '2025-01-01',
		        '99999999-0000-0000-0000-0000000000aa', '2025-01-01')`)
	mustExec(`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, properties, valid_from, source_memory, created_at)
		VALUES ('55555555-0000-0000-0000-000000000002', 'aaaaaaaa-0000-0000-0000-000000000001',
		        '44444444-0000-0000-0000-000000000001', '44444444-0000-0000-0000-000000000002',
		        'r2', 1.0, '{}', '2025-01-01',
		        '99999999-0000-0000-0000-0000000000bb', '2025-01-01')`)

	// memory_lineage: 1 orphan parent_id.
	mustExec(`INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context, created_at)
		VALUES ('66666666-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-000000000001',
		        '33333333-0000-0000-0000-000000000002',
		        '99999999-0000-0000-0000-0000000000cc',
		        'derived', '{}', '2025-01-01')`)

	// enrichment_queue: 1 orphan memory_id.
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, steps_completed, created_at, updated_at)
		VALUES ('77777777-0000-0000-0000-000000000001', '99999999-0000-0000-0000-0000000000dd',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'pending', 0, '[]',
		        '2025-01-01', '2025-01-01')`)

	// dream_cycle referencing the valid project.
	mustExec(`INSERT INTO dream_cycles (id, project_id, namespace_id, status, phase, tokens_used, token_budget, phase_summary, error, started_at, completed_at, created_at, updated_at)
		VALUES ('88888888-0000-0000-0000-000000000001', 'dddddddd-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002', 'completed', 'analyze', 100, 1000,
		        '{}', NULL, '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01')`)
}
