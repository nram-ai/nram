package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/nram-ai/nram/internal/config"
)

// Backend type constants.
const (
	BackendSQLite   = "sqlite"
	BackendPostgres = "postgres"
)

// DB is the database abstraction interface.
type DB interface {
	// Backend returns "sqlite" or "postgres" (immutable per process).
	Backend() string

	// Ping checks database connectivity.
	Ping(ctx context.Context) error

	// Close gracefully releases all database resources.
	Close() error

	// Exec executes a query without returning rows.
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)

	// Query executes a query that returns rows.
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)

	// QueryRow executes a query that returns at most one row.
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row

	// BeginTx starts a transaction.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	// DB returns the underlying *sql.DB for use by migration tools, etc.
	DB() *sql.DB
}

// Open creates a new DB connection based on config.
// If cfg.URL is non-empty, it connects to Postgres via pgx.
// Otherwise, it opens a SQLite file at "nram.db" in the working directory.
func Open(cfg config.DatabaseConfig) (DB, error) {
	if cfg.URL != "" {
		return openPostgres(cfg)
	}
	return openSQLite(cfg)
}

// openSQLite opens a SQLite database connection.
func openSQLite(cfg config.DatabaseConfig) (DB, error) {
	db, err := sql.Open("sqlite", "nram.db")
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Enable WAL mode for better concurrent read performance.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Enable foreign key constraint enforcement.
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Set busy timeout to avoid SQLITE_BUSY errors under contention.
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	return &sqliteDB{db: db}, nil
}

// openPostgres opens a Postgres database connection via pgx.
func openPostgres(cfg config.DatabaseConfig) (DB, error) {
	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	maxConns := cfg.MaxConnections
	if maxConns <= 0 {
		maxConns = 20
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	return &postgresDB{db: db}, nil
}
