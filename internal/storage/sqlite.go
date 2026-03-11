package storage

import (
	"context"
	"database/sql"

	_ "modernc.org/sqlite" // Pure Go SQLite driver.
)

// sqliteDB wraps a *sql.DB connected to a SQLite database.
type sqliteDB struct {
	db *sql.DB
}

func (s *sqliteDB) Backend() string {
	return BackendSQLite
}

func (s *sqliteDB) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *sqliteDB) Close() error {
	return s.db.Close()
}

func (s *sqliteDB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

func (s *sqliteDB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

func (s *sqliteDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, query, args...)
}

func (s *sqliteDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, opts)
}

func (s *sqliteDB) DB() *sql.DB {
	return s.db
}
