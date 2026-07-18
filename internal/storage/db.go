package storage

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/google/uuid"

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

	// WriteQueryRow executes a write that returns one row, e.g.
	// INSERT … RETURNING. Routed to the write pool for SQLite.
	WriteQueryRow(ctx context.Context, query string, args ...any) *sql.Row

	// WriteQuery executes a write that returns multiple rows, e.g.
	// DELETE … RETURNING. Routed to the write pool for SQLite.
	WriteQuery(ctx context.Context, query string, args ...any) (*sql.Rows, error)

	// BeginTx starts a write transaction (routed to write pool for SQLite).
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	// WithMemoryLock serializes the critical section against concurrent
	// writes to the given memory row. The body runs exactly once inside a
	// write transaction that already holds the lock; commit on success is
	// automatic, rollback on any returned error. See rowlock.go for the
	// backend-specific implementations (pg_advisory_xact_lock on Postgres,
	// in-process sync.Mutex on SQLite).
	WithMemoryLock(ctx context.Context, memoryID uuid.UUID, fn func(ctx context.Context, tx *sql.Tx) error) error

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
	return openSQLite()
}

// sqliteDBPath is the SQLite database filename, relative to the working
// directory (honoured after any --workdir chdir in main).
const sqliteDBPath = "nram.db"

// buildSQLiteDSN returns a modernc.org/sqlite DSN that carries every
// connection-level PRAGMA as a "_pragma" query parameter. This matters because
// PRAGMAs like cache_size, busy_timeout, synchronous, and foreign_keys are
// per-connection: applying them once via db.Exec after Open only touches
// whichever pooled connection database/sql happens to hand out, so later-opened
// pool connections would silently run on SQLite defaults (a ~2MB cache and
// busy_timeout=0). modernc applies each _pragma on every connection it opens
// (see modernc.org/sqlite conn.go newConn -> applyQueryParams), so routing the
// PRAGMAs through the DSN guarantees the whole read pool is configured.
//
// The DSN is deliberately built without a "file:" prefix so modernc strips the
// query from the filesystem path and hands SQLite the bare path; the _pragma
// values are percent-encoded by url.Values and decoded again by modernc.
//
//   - journal_mode=WAL      concurrent readers + single writer
//   - busy_timeout=10000    10s wait on lock contention (not the default 0)
//   - foreign_keys=ON       enforce FK constraints
//   - synchronous=NORMAL    safe with WAL, much faster than FULL
//   - cache_size=-128000    128MB in-memory page cache (per connection)
//   - auto_vacuum=INCREMENTAL  born incremental on a fresh DB so the maintenance
//     sweeper can reclaim free pages; a no-op on an already-populated DB until
//     the sweeper runs a one-time converting VACUUM (see SQLiteMaintenanceService).
func buildSQLiteDSN(path string) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "cache_size(-128000)")
	q.Add("_pragma", "auto_vacuum(INCREMENTAL)")
	return path + "?" + q.Encode()
}

// openSQLite opens a SQLite database with separate read and write connection
// pools. The write pool is limited to a single connection to serialize all
// writes and eliminate SQLITE_BUSY contention. The read pool allows multiple
// concurrent readers via WAL mode. All connection PRAGMAs are carried in the
// DSN so every pooled connection is configured identically (see buildSQLiteDSN).
func openSQLite() (DB, error) {
	dsn := buildSQLiteDSN(sqliteDBPath)

	// Write pool: single connection, all writes serialized.
	writeDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite write pool: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0) // keep connection alive forever

	// Read pool: multiple connections for concurrent reads.
	readDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		_ = writeDB.Close()
		return nil, fmt.Errorf("failed to open sqlite read pool: %w", err)
	}
	readDB.SetMaxOpenConns(4)
	readDB.SetMaxIdleConns(4)
	readDB.SetConnMaxLifetime(0)

	if err := writeDB.Ping(); err != nil {
		_ = writeDB.Close()
		_ = readDB.Close()
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
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	return &postgresDB{db: db}, nil
}
