package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nram-ai/nram/internal/config"
)

func testSQLiteDB(t *testing.T) DB {
	t.Helper()
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.Chdir(origDir)
	})

	db, err := Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

func TestOpenSQLiteCreatesFile(t *testing.T) {
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.Chdir(origDir)
	})

	db, err := Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	defer db.Close()

	dbPath := filepath.Join(tmpDir, "nram.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("expected database file at %s to exist", dbPath)
	}
}

func TestSQLiteBackend(t *testing.T) {
	db := testSQLiteDB(t)
	if db.Backend() != BackendSQLite {
		t.Fatalf("expected backend %q, got %q", BackendSQLite, db.Backend())
	}
}

func TestSQLitePing(t *testing.T) {
	db := testSQLiteDB(t)
	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestSQLiteExecQueryQueryRow(t *testing.T) {
	ctx := context.Background()
	db := testSQLiteDB(t)

	_, err := db.Exec(ctx, "CREATE TABLE test_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	result, err := db.Exec(ctx, "INSERT INTO test_items (name) VALUES (?)", "alpha")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", rowsAffected)
	}

	_, err = db.Exec(ctx, "INSERT INTO test_items (name) VALUES (?)", "beta")
	if err != nil {
		t.Fatalf("failed to insert second row: %v", err)
	}

	rows, err := db.Query(ctx, "SELECT id, name FROM test_items ORDER BY id")
	if err != nil {
		t.Fatalf("failed to query rows: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration error: %v", err)
	}
	if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
		t.Fatalf("expected [alpha beta], got %v", names)
	}

	var name string
	err = db.QueryRow(ctx, "SELECT name FROM test_items WHERE id = ?", 1).Scan(&name)
	if err != nil {
		t.Fatalf("failed to query single row: %v", err)
	}
	if name != "alpha" {
		t.Fatalf("expected name %q, got %q", "alpha", name)
	}
}

func TestSQLiteBeginTxCommit(t *testing.T) {
	ctx := context.Background()
	db := testSQLiteDB(t)

	_, err := db.Exec(ctx, "CREATE TABLE tx_test (val TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO tx_test (val) VALUES (?)", "committed")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert in transaction: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	var val string
	err = db.QueryRow(ctx, "SELECT val FROM tx_test").Scan(&val)
	if err != nil {
		t.Fatalf("failed to query after commit: %v", err)
	}
	if val != "committed" {
		t.Fatalf("expected %q, got %q", "committed", val)
	}
}

func TestSQLiteBeginTxRollback(t *testing.T) {
	ctx := context.Background()
	db := testSQLiteDB(t)

	_, err := db.Exec(ctx, "CREATE TABLE tx_rollback_test (val TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO tx_rollback_test (val) VALUES (?)", "rolled_back")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert in transaction: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback transaction: %v", err)
	}

	var count int
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM tx_rollback_test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query after rollback: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}
}

func TestSQLiteClose(t *testing.T) {
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.Chdir(origDir)
	})

	db, err := Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// After close, ping should fail.
	if err := db.Ping(context.Background()); err == nil {
		t.Fatal("expected ping to fail after close")
	}
}

func TestSQLiteDBAccessor(t *testing.T) {
	db := testSQLiteDB(t)
	underlying := db.DB()
	if underlying == nil {
		t.Fatal("expected non-nil *sql.DB from DB() accessor")
	}
}

func TestOpenPostgresAttempted(t *testing.T) {
	// This test verifies that Open with a postgres URL attempts a Postgres connection.
	// It will fail because no real Postgres server is available, which is expected.
	_, err := Open(config.DatabaseConfig{
		URL: "postgres://localhost:5432/nonexistent_nram_test_db?sslmode=disable",
	})
	if err == nil {
		t.Fatal("expected error when connecting to non-existent postgres server")
	}
}
