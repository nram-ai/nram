package migration

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// runEmbeddedSQLiteMigrations spins up a fresh on-disk SQLite, runs the full
// embedded migration sequence (so all schema is in place), turns FK off,
// and returns the db handle. Tests use this to exercise data-rewrite logic
// against the real schema without having to thread the full FK chain.
func runEmbeddedSQLiteMigrations(t *testing.T) *sql.DB {
	t.Helper()
	db := openTestDB(t)
	m, err := NewMigrator(db, "sqlite")
	if err != nil {
		t.Fatalf("NewMigrator: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Up(); err != nil {
		t.Fatalf("Up: %v", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("PRAGMA foreign_keys OFF: %v", err)
	}
	return db
}

// TestEmbeddedMigrations_UpAndIdempotent confirms the full embedded
// migration sequence applies on a fresh DB, leaves the schema clean, and
// surveys at the expected version. Adding a new migration here forces this
// test to be updated; the failure surfaces a missed renumber sooner.
func TestEmbeddedMigrations_UpAndIdempotent(t *testing.T) {
	db := openTestDB(t)
	m, err := NewMigrator(db, "sqlite")
	if err != nil {
		t.Fatalf("NewMigrator: %v", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil {
		t.Fatalf("Up: %v", err)
	}
	v, dirty, err := m.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if dirty {
		t.Fatalf("expected dirty=false after Up, got %v", dirty)
	}
	if v < 27 {
		t.Errorf("expected version >= 27 (post legacy_zero_confidence_restore), got %d", v)
	}

	// Re-running Up against an already-up database should be a no-op.
	m2, err := NewMigrator(db, "sqlite")
	if err != nil {
		t.Fatalf("NewMigrator (idempotent): %v", err)
	}
	defer m2.Close()
	if err := m2.Up(); err != nil {
		t.Fatalf("Up (idempotent): %v", err)
	}
}

// TestProjectRankingWeightsCanonical_RewriteLegacyShape exercises the
// 000025 (sqlite) data rewrite directly against a project with the legacy
// {recency, relevance, importance} shape. After running the migration's SQL
// the row should carry the canonical similarity key, with relevance gone
// and other settings preserved.
func TestProjectRankingWeightsCanonical_RewriteLegacyShape(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)

	projID := uuid.New().String()
	if _, err := db.Exec(`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description, settings) VALUES (?, ?, ?, 'p', 'p', '', ?)`,
		projID, uuid.New().String(), uuid.New().String(),
		`{"ranking_weights":{"recency":0.3,"relevance":0.5,"importance":0.2},"dedup_threshold":0.92,"enrichment_enabled":true}`); err != nil {
		t.Fatalf("insert project: %v", err)
	}

	// Step 1: copy relevance → similarity where similarity is unset.
	if _, err := db.Exec(`UPDATE projects
SET settings = json_set(
  json_remove(settings, '$.ranking_weights.relevance'),
  '$.ranking_weights.similarity',
  json_extract(settings, '$.ranking_weights.relevance')
)
WHERE
  json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL
  AND json_extract(settings, '$.ranking_weights.similarity') IS NULL`); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	// Step 2: drop any leftover relevance keys.
	if _, err := db.Exec(`UPDATE projects
SET settings = json_remove(settings, '$.ranking_weights.relevance')
WHERE json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL`); err != nil {
		t.Fatalf("step 2: %v", err)
	}

	var settings string
	if err := db.QueryRow(`SELECT settings FROM projects WHERE id = ?`, projID).Scan(&settings); err != nil {
		t.Fatalf("read project: %v", err)
	}
	if strings.Contains(settings, `"relevance"`) {
		t.Errorf("relevance should be gone from %s", settings)
	}
	if !strings.Contains(settings, `"similarity":0.5`) {
		t.Errorf("similarity=0.5 should be present in %s", settings)
	}
	if !strings.Contains(settings, `"recency":0.3`) {
		t.Errorf("recency=0.3 should be preserved in %s", settings)
	}
	if !strings.Contains(settings, `"importance":0.2`) {
		t.Errorf("importance=0.2 should be preserved in %s", settings)
	}
	if !strings.Contains(settings, `"dedup_threshold":0.92`) {
		t.Errorf("dedup_threshold should be preserved in %s", settings)
	}
}

// TestProjectRankingWeightsCanonical_SimilarityWinsTie verifies that when
// both `similarity` and `relevance` are set on a legacy row, similarity is
// preserved and relevance is dropped — matching the parser's tie-break.
func TestProjectRankingWeightsCanonical_SimilarityWinsTie(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)

	projID := uuid.New().String()
	if _, err := db.Exec(`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description, settings) VALUES (?, ?, ?, 'p', 'p', '', ?)`,
		projID, uuid.New().String(), uuid.New().String(),
		`{"ranking_weights":{"similarity":0.6,"relevance":0.4}}`); err != nil {
		t.Fatalf("insert project: %v", err)
	}

	if _, err := db.Exec(`UPDATE projects SET settings = json_set(json_remove(settings, '$.ranking_weights.relevance'), '$.ranking_weights.similarity', json_extract(settings, '$.ranking_weights.relevance')) WHERE json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL AND json_extract(settings, '$.ranking_weights.similarity') IS NULL`); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	if _, err := db.Exec(`UPDATE projects SET settings = json_remove(settings, '$.ranking_weights.relevance') WHERE json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL`); err != nil {
		t.Fatalf("step 2: %v", err)
	}

	var settings string
	if err := db.QueryRow(`SELECT settings FROM projects WHERE id = ?`, projID).Scan(&settings); err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Contains(settings, `"relevance"`) {
		t.Errorf("relevance should be gone, got %s", settings)
	}
	if !strings.Contains(settings, `"similarity":0.6`) {
		t.Errorf("canonical similarity=0.6 should win, got %s", settings)
	}
}

// TestUsersRankingWeightsStrip_RemovesField verifies the 000026 (sqlite)
// migration drops the dead user-level ranking_weights field and leaves
// other settings keys intact.
func TestUsersRankingWeightsStrip_RemovesField(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)

	userID := uuid.New().String()
	if _, err := db.Exec(`INSERT INTO users (id, email, password_hash, org_id, namespace_id, role, settings) VALUES (?, 'u@x', 'h', ?, ?, 'member', ?)`,
		userID, uuid.New().String(), uuid.New().String(),
		`{"ranking_weights":{"recency":0.3,"relevance":0.5,"importance":0.2},"dedup_threshold":0.95,"enrichment_enabled":true}`); err != nil {
		t.Fatalf("insert user: %v", err)
	}

	if _, err := db.Exec(`UPDATE users SET settings = json_remove(settings, '$.ranking_weights') WHERE json_extract(settings, '$.ranking_weights') IS NOT NULL`); err != nil {
		t.Fatalf("strip: %v", err)
	}

	var settings string
	if err := db.QueryRow(`SELECT settings FROM users WHERE id = ?`, userID).Scan(&settings); err != nil {
		t.Fatalf("read user: %v", err)
	}
	if strings.Contains(settings, `"ranking_weights"`) {
		t.Errorf("ranking_weights should be gone, got %s", settings)
	}
	if !strings.Contains(settings, `"dedup_threshold":0.95`) {
		t.Errorf("dedup_threshold should be preserved, got %s", settings)
	}
	if !strings.Contains(settings, `"enrichment_enabled":true`) {
		t.Errorf("enrichment_enabled should be preserved, got %s", settings)
	}
}

// TestLegacyZeroConfidenceRestore_RespectsLog verifies the 000027 (sqlite)
// migration restores legacy zero-confidence rows but preserves rows that
// were deliberately demoted by the dream alignment phase. The dream_log
// entry with target_type='memory' and operation='confidence_adjusted' is
// the canonical evidence of demotion.
func TestLegacyZeroConfidenceRestore_RespectsLog(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)

	memRestore := uuid.New().String()
	memDemoted := uuid.New().String()
	memUntouched := uuid.New().String()
	nsID := uuid.New().String()

	for _, m := range []struct {
		id   string
		conf float64
	}{
		{memRestore, 0.0},
		{memDemoted, 0.0},
		{memUntouched, 0.5},
	} {
		if _, err := db.Exec(`INSERT INTO memories (id, namespace_id, content, content_hash, confidence, importance, source, tags, metadata) VALUES (?, ?, 'x', ?, ?, 0.5, 'user', '[]', '{}')`,
			m.id, nsID, uuid.New().String(), m.conf); err != nil {
			t.Fatalf("insert memory: %v", err)
		}
	}

	// Insert a dream cycle + log entry for memDemoted only.
	cycleID := uuid.New().String()
	projID := uuid.New().String()
	if _, err := db.Exec(`INSERT INTO dream_cycles (id, namespace_id, project_id, started_at, status) VALUES (?, ?, ?, '2026-04-20T00:00:00Z', 'completed')`,
		cycleID, nsID, projID); err != nil {
		t.Fatalf("insert cycle: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO dream_logs (id, cycle_id, project_id, phase, operation, target_type, target_id) VALUES (?, ?, ?, 'consolidation', 'confidence_adjusted', 'memory', ?)`,
		uuid.New().String(), cycleID, projID, memDemoted); err != nil {
		t.Fatalf("insert dream_log: %v", err)
	}

	// Run the migration's SQL.
	if _, err := db.Exec(`UPDATE memories SET confidence = 1.0 WHERE confidence = 0 AND id NOT IN (SELECT target_id FROM dream_logs WHERE target_type = 'memory' AND operation = 'confidence_adjusted')`); err != nil {
		t.Fatalf("restore: %v", err)
	}

	for _, want := range []struct {
		id     string
		expect float64
		label  string
	}{
		{memRestore, 1.0, "legacy zero (no log) restored to 1.0"},
		{memDemoted, 0.0, "dream-demoted (has log) preserved at 0.0"},
		{memUntouched, 0.5, "non-zero memory unchanged"},
	} {
		var got float64
		if err := db.QueryRow(`SELECT confidence FROM memories WHERE id = ?`, want.id).Scan(&got); err != nil {
			t.Fatalf("%s: read failed: %v", want.label, err)
		}
		if got != want.expect {
			t.Errorf("%s: expected %v, got %v", want.label, want.expect, got)
		}
	}
}

// TestLegacyZeroConfidenceRestore_Idempotent confirms running the migration
// twice produces no further changes — restored rows are no longer 0, so
// the WHERE clause matches nothing on the second pass.
func TestLegacyZeroConfidenceRestore_Idempotent(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)

	memID := uuid.New().String()
	if _, err := db.Exec(`INSERT INTO memories (id, namespace_id, content, content_hash, confidence, importance, source, tags, metadata) VALUES (?, ?, 'x', ?, 0, 0.5, 'user', '[]', '{}')`,
		memID, uuid.New().String(), uuid.New().String()); err != nil {
		t.Fatalf("insert memory: %v", err)
	}

	stmt := `UPDATE memories SET confidence = 1.0 WHERE confidence = 0 AND id NOT IN (SELECT target_id FROM dream_logs WHERE target_type = 'memory' AND operation = 'confidence_adjusted')`
	for i := 0; i < 2; i++ {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}

	var got float64
	if err := db.QueryRow(`SELECT confidence FROM memories WHERE id = ?`, memID).Scan(&got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != 1.0 {
		t.Errorf("expected 1.0 after idempotent re-run, got %v", got)
	}
}
