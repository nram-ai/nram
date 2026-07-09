package migration

import (
	"io/fs"
	"sort"
	"strings"
	"testing"

	"github.com/nram-ai/nram/migrations"
)

// readMigrationDir lists the entries of a backend's embedded migration
// directory (e.g. dir "sqlite" within migrations.SQLiteFS).
func readMigrationDir(t *testing.T, embedded fs.FS, dir string) []fs.DirEntry {
	t.Helper()
	entries, err := fs.ReadDir(embedded, dir)
	if err != nil {
		t.Fatalf("fs.ReadDir(%s): %v", dir, err)
	}
	return entries
}

// backendSlugSet returns the set of migration slugs for a backend. A slug is
// the filename with its zero-padded numeric prefix and .up.sql suffix removed
// (e.g. "000021_recall_fusion.up.sql" -> "recall_fusion").
//
// The slug, not the number, is the shared identity of a migration across
// backends. The per-backend numbering intentionally diverges: a feature that
// touches only one backend shifts that backend's sequence, so the same number
// can hold different migrations (e.g. sqlite/000021 is recall_fusion while
// postgres/000021 is enrichment_probe_indexes). Auditing "did migration X
// ship?" is done by slug, never by number.
func backendSlugSet(t *testing.T, embedded fs.FS, dir string) map[string]bool {
	t.Helper()
	slugs := make(map[string]bool)
	for _, e := range readMigrationDir(t, embedded, dir) {
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		base := strings.TrimSuffix(name, ".up.sql")
		parts := strings.SplitN(base, "_", 2)
		if len(parts) != 2 || parts[1] == "" {
			t.Fatalf("%s/%s: malformed migration filename, want NNNNNN_slug.up.sql", dir, name)
		}
		slug := parts[1]
		if slugs[slug] {
			t.Fatalf("%s/%s: duplicate migration slug %q within backend", dir, name, slug)
		}
		slugs[slug] = true
	}
	return slugs
}

// sortedKeys returns the keys of a string set in stable order for deterministic
// failure messages.
func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestMigrationSlugParity guards that the hand-maintained sqlite and postgres
// migration directories stay in sync by slug. Every slug present in one backend
// must be present in the other, except a small allowlist of intentionally
// backend-specific migrations. This catches the real drift risk: adding a
// schema change to one backend and forgetting the other.
func TestMigrationSlugParity(t *testing.T) {
	sqliteSlugs := backendSlugSet(t, migrations.SQLiteFS, "sqlite")
	postgresSlugs := backendSlugSet(t, migrations.PostgresFS, "postgres")

	// Intentionally SQLite-only migrations: SQLite splits vector storage into
	// extra tables that pgvector makes unnecessary on Postgres, so these slugs
	// exist only under migrations/sqlite. There are no Postgres-only migrations
	// today; add a symmetric allowlist in the postgres direction below the day
	// one exists.
	sqliteOnly := map[string]bool{
		"entity_vector_tables": true,
		"vector_tables_fk":     true,
	}

	// Allowlist hygiene: every allowlisted slug must still exist, otherwise the
	// allowlist has gone stale and must be pruned.
	for _, slug := range sortedKeys(sqliteOnly) {
		if !sqliteSlugs[slug] {
			t.Errorf("allowlist rot: SQLite-only slug %q is allowlisted but absent from migrations/sqlite", slug)
		}
	}

	// Every SQLite slug must have a Postgres counterpart unless intentionally
	// SQLite-only.
	for _, slug := range sortedKeys(sqliteSlugs) {
		if postgresSlugs[slug] || sqliteOnly[slug] {
			continue
		}
		t.Errorf("migration %q exists in migrations/sqlite but not migrations/postgres (add the Postgres migration, or allowlist it as SQLite-only)", slug)
	}

	// And every Postgres slug must have a SQLite counterpart. There is no
	// Postgres-only allowlist today; introduce one here if that ever changes.
	for _, slug := range sortedKeys(postgresSlugs) {
		if sqliteSlugs[slug] {
			continue
		}
		t.Errorf("migration %q exists in migrations/postgres but not migrations/sqlite (add the SQLite migration)", slug)
	}
}

// TestMigrationUpDownPaired guards that every migration ships both an .up.sql
// and a matching .down.sql in each backend.
func TestMigrationUpDownPaired(t *testing.T) {
	backends := []struct {
		embedded fs.FS
		dir      string
	}{
		{migrations.SQLiteFS, "sqlite"},
		{migrations.PostgresFS, "postgres"},
	}
	for _, b := range backends {
		ups := make(map[string]bool)
		downs := make(map[string]bool)
		for _, e := range readMigrationDir(t, b.embedded, b.dir) {
			name := e.Name()
			switch {
			case strings.HasSuffix(name, ".up.sql"):
				ups[strings.TrimSuffix(name, ".up.sql")] = true
			case strings.HasSuffix(name, ".down.sql"):
				downs[strings.TrimSuffix(name, ".down.sql")] = true
			}
		}
		for _, base := range sortedKeys(ups) {
			if !downs[base] {
				t.Errorf("%s/%s.up.sql has no matching .down.sql", b.dir, base)
			}
		}
		for _, base := range sortedKeys(downs) {
			if !ups[base] {
				t.Errorf("%s/%s.down.sql has no matching .up.sql", b.dir, base)
			}
		}
	}
}
