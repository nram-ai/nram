package service

import (
	"database/sql"
	"os"
	"testing"

	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/maintenance"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/storage"
)

// chdirTemp switches into a fresh temp dir (where the SQLite "nram.db" file
// lives) for the duration of the test.
func chdirTemp(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })
}

func migrate(t *testing.T, db storage.DB) {
	t.Helper()
	m, err := migration.NewMigrator(db.WriteDB(), db.Backend())
	if err != nil {
		t.Fatalf("migrator: %v", err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func openMigratedSQLite(t *testing.T) storage.DB {
	t.Helper()
	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	migrate(t, db)
	return db
}

func newMaintSvc(db storage.DB, settings *SettingsService) *SQLiteMaintenanceService {
	return NewSQLiteMaintenance(db, settings, maintenance.NewRegistry(nil))
}

// TestSQLiteMaintenance_LightPassRuns exercises the light pass against a real
// migrated database and confirms it completes without error.
func TestSQLiteMaintenance_LightPassRuns(t *testing.T) {
	chdirTemp(t)
	db := openMigratedSQLite(t)
	svc := newMaintSvc(db, NewSettingsService(newMockSettingsRepo()))

	svc.lightPass(t.Context()) // must not panic or error out
}

// TestSQLiteMaintenance_FullPassRecordsMarker proves a due full pass on a fresh
// (already-incremental) database runs the VACUUM and records the completion
// timestamp so a restart within the interval will not repeat it.
func TestSQLiteMaintenance_FullPassRecordsMarker(t *testing.T) {
	chdirTemp(t)
	db := openMigratedSQLite(t)
	svc := newMaintSvc(db, NewSettingsService(newMockSettingsRepo()))
	ctx := t.Context()

	if marker, _ := storage.GetSystemMeta(ctx, db, metaLastFullVacuum); marker != "" {
		t.Fatalf("precondition: marker should be empty, got %q", marker)
	}

	svc.fullPass(ctx)

	marker, err := storage.GetSystemMeta(ctx, db, metaLastFullVacuum)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if marker == "" {
		t.Fatal("full pass did not record the last-vacuum marker")
	}
}

// TestSQLiteMaintenance_NotDueSkips proves that when the marker is fresh (within
// the interval) and no conversion is needed, the full pass does not run VACUUM,
// leaving the marker untouched.
func TestSQLiteMaintenance_NotDueSkips(t *testing.T) {
	chdirTemp(t)
	db := openMigratedSQLite(t)
	svc := newMaintSvc(db, NewSettingsService(newMockSettingsRepo()))
	ctx := t.Context()

	const sentinel = "2999-01-01T00:00:00Z" // far future: definitely within any interval
	if err := storage.SetSystemMeta(ctx, db, metaLastFullVacuum, sentinel); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	svc.fullPass(ctx)

	marker, _ := storage.GetSystemMeta(ctx, db, metaLastFullVacuum)
	if marker != sentinel {
		t.Errorf("marker changed to %q; full pass ran when it was not due", marker)
	}
}

// TestSQLiteMaintenance_DisabledSkips proves the enable toggle gates both passes.
func TestSQLiteMaintenance_DisabledSkips(t *testing.T) {
	chdirTemp(t)
	db := openMigratedSQLite(t)

	repo := newMockSettingsRepo()
	repo.put(SettingSqliteMaintEnabled, "global", "false")
	svc := newMaintSvc(db, NewSettingsService(repo))
	ctx := t.Context()

	svc.fullPass(ctx)

	if marker, _ := storage.GetSystemMeta(ctx, db, metaLastFullVacuum); marker != "" {
		t.Errorf("disabled full pass ran anyway; marker = %q", marker)
	}
}

// TestSQLiteMaintenance_ConvertsExistingNoneModeDB proves an existing database
// created before this feature (auto_vacuum NONE) is converted to incremental
// mode by the one-time converting VACUUM, regardless of the schedule.
func TestSQLiteMaintenance_ConvertsExistingNoneModeDB(t *testing.T) {
	chdirTemp(t)

	// Create a populated database in the default NONE auto-vacuum mode, which
	// locks the file's auto_vacuum mode at NONE for its lifetime (until VACUUM).
	plain, err := sql.Open("sqlite", "nram.db")
	if err != nil {
		t.Fatalf("open plain sqlite: %v", err)
	}
	if _, err := plain.Exec("CREATE TABLE seed(x INTEGER)"); err != nil {
		t.Fatalf("seed table: %v", err)
	}
	for i := range 50 {
		if _, err := plain.Exec("INSERT INTO seed VALUES (?)", i); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}
	_ = plain.Close()

	db := openMigratedSQLite(t) // reopens nram.db via the incremental DSN + migrations
	ctx := t.Context()

	var before int
	if err := db.WriteDB().QueryRow("PRAGMA auto_vacuum").Scan(&before); err != nil {
		t.Fatalf("read auto_vacuum: %v", err)
	}
	if before != 0 {
		t.Fatalf("precondition: existing NONE-mode db should report auto_vacuum=0, got %d", before)
	}

	svc := newMaintSvc(db, NewSettingsService(newMockSettingsRepo()))
	svc.fullPass(ctx)

	var after int
	if err := db.WriteDB().QueryRow("PRAGMA auto_vacuum").Scan(&after); err != nil {
		t.Fatalf("read auto_vacuum after: %v", err)
	}
	if after != 2 {
		t.Errorf("after conversion auto_vacuum = %d, want 2 (INCREMENTAL)", after)
	}
	if marker, _ := storage.GetSystemMeta(ctx, db, metaLastFullVacuum); marker == "" {
		t.Error("conversion did not record the last-vacuum marker")
	}
}
