package storage

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/google/uuid"
)

// sqlExecer is the minimal handle interface satisfied by both *sql.Tx and
// the dbExec adapter wrapping a DB. Repo helpers operate against this so
// the same SQL can run inside or outside an explicit transaction.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// dbExec adapts a DB to sqlExecer, routing each *Context call to the
// matching DB method (which routes to the right SQLite pool internally).
type dbExec struct{ db DB }

func (a dbExec) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.db.Exec(ctx, query, args...)
}

func (a dbExec) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.db.Query(ctx, query, args...)
}

func (a dbExec) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.db.QueryRow(ctx, query, args...)
}

// hashUUIDToInt64 maps a UUID's 16 bytes into a 64-bit integer suitable
// for pg_advisory_xact_lock's bigint key. FNV-1a 64-bit; a collision would
// only cause spurious serialization between two unrelated locks, never
// incorrectness, and the probability is negligible at any realistic
// memory-row cardinality. If collisions ever become operationally visible,
// switching to the two-int form of pg_advisory_xact_lock is a one-line
// change in WithMemoryLock on the postgresDB receiver.
func hashUUIDToInt64(id uuid.UUID) int64 {
	h := fnv.New64a()
	_, _ = h.Write(id[:])
	return int64(h.Sum64()) //nolint:gosec // bit-reinterpretation: pg bigint covers the full signed range
}

// WithMemoryLock serializes a critical section against concurrent writes to
// the same memory row. The body runs exactly once inside a write
// transaction; commit on success is automatic, rollback on any returned
// error.
//
// On SQLite the lock is an in-process sync.Mutex keyed by memory_id.
// SQLite is single-process by definition, so in-process serialization
// fully covers cross-writer races.
func (s *sqliteDB) WithMemoryLock(
	ctx context.Context,
	memoryID uuid.UUID,
	fn func(ctx context.Context, tx *sql.Tx) error,
) error {
	release := s.lockMemory(memoryID)
	defer release()

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("memory lock begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // safe after commit; idempotent

	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("memory lock commit: %w", err)
	}
	return nil
}

// lockMemory returns a release closure for the given memory's per-process
// mutex. Entries are lazy-created on first use and intentionally not
// removed (the map is bounded by the number of distinct memory_ids
// touched concurrently, which is small in practice). Callers MUST invoke
// the release closure exactly once, typically via defer.
func (s *sqliteDB) lockMemory(id uuid.UUID) func() {
	mxAny, _ := s.memoryLocks.LoadOrStore(id, &sync.Mutex{})
	mx := mxAny.(*sync.Mutex)
	mx.Lock()
	return mx.Unlock
}

// WithMemoryLock serializes a critical section against concurrent writes to
// the same memory row, across processes. The body runs exactly once inside
// a write transaction that already holds a pg_advisory_xact_lock keyed on
// the memory id; commit on success is automatic, rollback on any returned
// error. The advisory lock auto-releases at transaction end (commit,
// rollback, or connection death), so a crashed worker cannot leak the lock.
func (p *postgresDB) WithMemoryLock(
	ctx context.Context,
	memoryID uuid.UUID,
	fn func(ctx context.Context, tx *sql.Tx) error,
) error {
	tx, err := p.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("memory lock begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // safe after commit; idempotent

	key := hashUUIDToInt64(memoryID)
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", key); err != nil {
		return fmt.Errorf("memory advisory lock: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("memory lock commit: %w", err)
	}
	return nil
}
