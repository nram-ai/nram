package storage

import (
	"context"
	"database/sql"
	"testing"
)

// TestSQLitePragmasAppliedToEveryReadConnection proves the DSN-based PRAGMA
// application configures every connection in the read pool, not just the first
// one database/sql happens to hand out. Before the DSN change, cache_size and
// busy_timeout were applied with a post-open db.Exec that only reached one
// pooled connection, leaving later readers on SQLite's ~2MB default cache and a
// zero busy timeout.
func TestSQLitePragmasAppliedToEveryReadConnection(t *testing.T) {
	db := testSQLiteDB(t)

	readPool := db.DB() // read pool: MaxOpenConns == 4
	ctx := context.Background()

	// Grab all four connections and hold them so the pool must open every one,
	// then verify each carries the configured PRAGMAs.
	conns := make([]*sql.Conn, 0, 4)
	for i := range 4 {
		c, err := readPool.Conn(ctx)
		if err != nil {
			t.Fatalf("open read conn %d: %v", i, err)
		}
		conns = append(conns, c)
	}
	t.Cleanup(func() {
		for _, c := range conns {
			_ = c.Close()
		}
	})

	for i, c := range conns {
		var cacheSize int
		if err := c.QueryRowContext(ctx, "PRAGMA cache_size").Scan(&cacheSize); err != nil {
			t.Fatalf("conn %d cache_size: %v", i, err)
		}
		if cacheSize != -128000 {
			t.Errorf("conn %d cache_size = %d, want -128000", i, cacheSize)
		}

		var busyTimeout int
		if err := c.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
			t.Fatalf("conn %d busy_timeout: %v", i, err)
		}
		if busyTimeout != 10000 {
			t.Errorf("conn %d busy_timeout = %d, want 10000", i, busyTimeout)
		}
	}
}

// TestSQLiteFreshDBIncrementalAutoVacuum proves a freshly-created, migrated
// database is born in incremental auto-vacuum mode (2), so the maintenance
// sweeper does not perform a one-time converting VACUUM on a new install.
func TestSQLiteFreshDBIncrementalAutoVacuum(t *testing.T) {
	db := testDBWithMigrations(t)

	var mode int
	if err := db.WriteDB().QueryRow("PRAGMA auto_vacuum").Scan(&mode); err != nil {
		t.Fatalf("read auto_vacuum: %v", err)
	}
	if mode != 2 {
		t.Errorf("fresh DB auto_vacuum = %d, want 2 (INCREMENTAL)", mode)
	}
}
