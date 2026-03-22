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

	// Exec executes a write query (routed to write pool for SQLite).
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)

	// Query executes a read query (routed to read pool for SQLite).
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)

	// QueryRow executes a read query returning one row (routed to read pool for SQLite).
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row

	// BeginTx starts a write transaction (routed to write pool for SQLite).
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	// DB returns the read *sql.DB (or shared pool for Postgres). Used by migration tools.
	DB() *sql.DB

	// WriteDB returns the write *sql.DB (same as DB() for Postgres).
	// For SQLite, this is a separate single-connection pool to serialize writes.
	WriteDB() *sql.DB
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

// sqlitePragmas are applied to every SQLite connection (both read and write pools).
var sqlitePragmas = []string{
	"PRAGMA journal_mode=WAL",        // concurrent readers + single writer
	"PRAGMA busy_timeout=10000",      // 10s wait on lock contention
	"PRAGMA foreign_keys=ON",         // enforce FK constraints
	"PRAGMA synchronous=NORMAL",      // safe with WAL, much faster than FULL
	"PRAGMA cache_size=-64000",       // 64MB in-memory page cache
}

// applySQLitePragmas sets all required PRAGMAs on a SQLite *sql.DB.
func applySQLitePragmas(db *sql.DB) error {
	for _, pragma := range sqlitePragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("sqlite pragma %q: %w", pragma, err)
		}
	}
	return nil
}

// openSQLite opens a SQLite database with separate read and write connection
// pools. The write pool is limited to a single connection to serialize all
// writes and eliminate SQLITE_BUSY contention. The read pool allows multiple
// concurrent readers via WAL mode.
func openSQLite(cfg config.DatabaseConfig) (DB, error) {
	// Write pool: single connection, all writes serialized.
	writeDB, err := sql.Open("sqlite", "nram.db")
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite write pool: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0) // keep connection alive forever

	if err := applySQLitePragmas(writeDB); err != nil {
		writeDB.Close()
		return nil, err
	}

	// Read pool: multiple connections for concurrent reads.
	readDB, err := sql.Open("sqlite", "nram.db")
	if err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("failed to open sqlite read pool: %w", err)
	}
	readDB.SetMaxOpenConns(4)
	readDB.SetMaxIdleConns(4)
	readDB.SetConnMaxLifetime(0)

	if err := applySQLitePragmas(readDB); err != nil {
		writeDB.Close()
		readDB.Close()
		return nil, err
	}

	if err := writeDB.Ping(); err != nil {
		writeDB.Close()
		readDB.Close()
		return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	return &sqliteDB{readDB: readDB, writeDB: writeDB}, nil
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
