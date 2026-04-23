package storage

import (
	"context"
	"database/sql"

	_ "modernc.org/sqlite" // Pure Go SQLite driver.
)

// sqliteDB wraps separate read and write connection pools for SQLite.
// The write pool has MaxOpenConns=1 to serialize all writes and eliminate
// SQLITE_BUSY contention. The read pool allows concurrent readers via WAL mode.
type sqliteDB struct {
	readDB  *sql.DB
	writeDB *sql.DB
}

func (s *sqliteDB) Backend() string {
	return BackendSQLite
}

func (s *sqliteDB) Ping(ctx context.Context) error {
	return s.writeDB.PingContext(ctx)
}

func (s *sqliteDB) Close() error {
	rerr := s.readDB.Close()
	werr := s.writeDB.Close()
	if werr != nil {
		return werr
	}
	return rerr
}

// Exec routes to the write pool (single connection, serialized).
func (s *sqliteDB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.writeDB.ExecContext(ctx, query, args...)
}

// Query routes to the read pool (multiple connections, concurrent).
func (s *sqliteDB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.readDB.QueryContext(ctx, query, args...)
}

// QueryRow routes to the read pool (multiple connections, concurrent).
func (s *sqliteDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.readDB.QueryRowContext(ctx, query, args...)
}

// WriteQueryRow routes to the write pool (single connection, serialized) for
// statements that mutate but return a row, like INSERT … RETURNING.
func (s *sqliteDB) WriteQueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.writeDB.QueryRowContext(ctx, query, args...)
}

// BeginTx routes to the write pool (single connection, serialized).
func (s *sqliteDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return s.writeDB.BeginTx(ctx, opts)
}

// DB returns the read pool for use by migration tools and read-only callers.
func (s *sqliteDB) DB() *sql.DB {
	return s.readDB
}

// WriteDB returns the write pool (single connection) for callers that need
// direct write access (e.g., HNSW store, migration runner).
func (s *sqliteDB) WriteDB() *sql.DB {
	return s.writeDB
}
