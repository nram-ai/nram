package migration

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestNewMigratorSQLite(t *testing.T) {
	db := openTestDB(t)
	m, err := NewMigrator(db, "sqlite")
	if err != nil {
		t.Fatalf("NewMigrator failed: %v", err)
	}
	defer m.Close()

	if m.backend != "sqlite" {
		t.Errorf("expected backend sqlite, got %s", m.backend)
	}
}

func TestNewMigratorInvalidBackend(t *testing.T) {
	db := openTestDB(t)
	_, err := NewMigrator(db, "mysql")
	if err == nil {
		t.Fatal("expected error for unsupported backend")
	}
}

func TestMigratorUpDownStatus(t *testing.T) {
	// Create temp migration files to test lifecycle.
	tmpDir := t.TempDir()
	sqliteDir := filepath.Join(tmpDir, "sqlite")
	postgresDir := filepath.Join(tmpDir, "postgres")

	if err := os.MkdirAll(sqliteDir, 0o755); err != nil {
		t.Fatalf("failed to create sqlite dir: %v", err)
	}
	if err := os.MkdirAll(postgresDir, 0o755); err != nil {
		t.Fatalf("failed to create postgres dir: %v", err)
	}

	// Write a simple test migration.
	upSQL := "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"
	downSQL := "DROP TABLE IF EXISTS test_table;"

	if err := os.WriteFile(filepath.Join(sqliteDir, "000001_test.up.sql"), []byte(upSQL), 0o644); err != nil {
		t.Fatalf("failed to write up migration: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sqliteDir, "000001_test.down.sql"), []byte(downSQL), 0o644); err != nil {
		t.Fatalf("failed to write down migration: %v", err)
	}

	// Use NewMigratorWithFS to test with custom FS.
	db := openTestDB(t)
	m, err := NewMigratorWithDir(db, "sqlite", sqliteDir)
	if err != nil {
		t.Fatalf("NewMigratorWithDir failed: %v", err)
	}

	// Test Up.
	if err := m.Up(); err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	// Test Status after Up.
	version, dirty, err := m.Status()
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
	if dirty {
		t.Error("expected dirty=false")
	}

	// Test Up again (no change).
	if err := m.Up(); err != nil {
		t.Fatalf("Up (no change) failed: %v", err)
	}

	// Test Down.
	if err := m.Down(); err != nil {
		t.Fatalf("Down failed: %v", err)
	}

	// Verify table was dropped.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test_table'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to check table existence: %v", err)
	}
	if count != 0 {
		t.Error("expected test_table to be dropped after Down")
	}

	m.Close()
}

func TestParseMigrateArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantCmd string
		wantArg string
		wantErr bool
	}{
		{
			name:    "migrate up",
			args:    []string{"nram", "migrate", "up"},
			wantCmd: "up",
			wantArg: "",
		},
		{
			name:    "migrate down",
			args:    []string{"nram", "migrate", "down"},
			wantCmd: "down",
			wantArg: "",
		},
		{
			name:    "migrate status",
			args:    []string{"nram", "migrate", "status"},
			wantCmd: "status",
			wantArg: "",
		},
		{
			name:    "migrate create with name",
			args:    []string{"nram", "migrate", "create", "add_users"},
			wantCmd: "create",
			wantArg: "add_users",
		},
		{
			name:    "migrate-to-postgres with url",
			args:    []string{"nram", "migrate-to-postgres", "--database-url", "postgres://localhost/nram"},
			wantCmd: "migrate-to-postgres",
			wantArg: "postgres://localhost/nram",
		},
		{
			name:    "no command",
			args:    []string{"nram"},
			wantErr: true,
		},
		{
			name:    "not a migration command",
			args:    []string{"nram", "serve"},
			wantErr: true,
		},
		{
			name:    "unknown subcommand",
			args:    []string{"nram", "migrate", "reset"},
			wantErr: true,
		},
		{
			name:    "no subcommand",
			args:    []string{"nram", "migrate"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, arg, err := ParseMigrateArgs(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cmd != tt.wantCmd {
				t.Errorf("command: got %q, want %q", cmd, tt.wantCmd)
			}
			if arg != tt.wantArg {
				t.Errorf("extra arg: got %q, want %q", arg, tt.wantArg)
			}
		})
	}
}

func TestCreateMigrationFiles(t *testing.T) {
	// Set up temp directories that mimic the project structure.
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(origDir) })

	sqliteDir := filepath.Join(tmpDir, "migrations", "sqlite")
	postgresDir := filepath.Join(tmpDir, "migrations", "postgres")
	if err := os.MkdirAll(sqliteDir, 0o755); err != nil {
		t.Fatalf("failed to create sqlite dir: %v", err)
	}
	if err := os.MkdirAll(postgresDir, 0o755); err != nil {
		t.Fatalf("failed to create postgres dir: %v", err)
	}

	// Create first migration.
	if err := createMigrationFiles("init"); err != nil {
		t.Fatalf("createMigrationFiles failed: %v", err)
	}

	// Verify files exist.
	for _, dir := range []string{sqliteDir, postgresDir} {
		for _, suffix := range []string{".up.sql", ".down.sql"} {
			path := filepath.Join(dir, "000001_init"+suffix)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				t.Errorf("expected file %s to exist", path)
			}
		}
	}

	// Create second migration and verify numbering.
	if err := createMigrationFiles("add_users"); err != nil {
		t.Fatalf("createMigrationFiles failed: %v", err)
	}

	path := filepath.Join(sqliteDir, "000002_add_users.up.sql")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("expected file %s to exist", path)
	}
}
