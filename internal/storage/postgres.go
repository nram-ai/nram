package storage

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx database/sql driver for Postgres.
)

// postgresDB wraps a *sql.DB connected to a PostgreSQL database.
type postgresDB struct {
	db *sql.DB
}

func (p *postgresDB) Backend() string {
	return BackendPostgres
}

func (p *postgresDB) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

func (p *postgresDB) Close() error {
	return p.db.Close()
}

func (p *postgresDB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return p.db.ExecContext(ctx, query, args...)
}

func (p *postgresDB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return p.db.QueryContext(ctx, query, args...)
}

func (p *postgresDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return p.db.QueryRowContext(ctx, query, args...)
}

func (p *postgresDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return p.db.BeginTx(ctx, opts)
}

func (p *postgresDB) DB() *sql.DB {
	return p.db
}

func (p *postgresDB) WriteDB() *sql.DB {
	return p.db
}
