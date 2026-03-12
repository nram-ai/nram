package migration

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/golang-migrate/migrate/v4"
	mpg "github.com/golang-migrate/migrate/v4/database/postgres"
	msqlite "github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/nram-ai/nram/migrations"
)

// Migrator wraps golang-migrate to apply schema migrations.
type Migrator struct {
	m       *migrate.Migrate
	db      *sql.DB
	backend string
}

// NewMigrator creates a Migrator for the given backend ("sqlite" or "postgres").
func NewMigrator(db *sql.DB, backend string) (*Migrator, error) {
	var embedFS fs.FS
	var driverName string

	switch backend {
	case "sqlite":
		sub, err := fs.Sub(migrations.SQLiteFS, "sqlite")
		if err != nil {
			return nil, fmt.Errorf("failed to create sqlite sub-filesystem: %w", err)
		}
		embedFS = sub
		driverName = "sqlite"
	case "postgres":
		sub, err := fs.Sub(migrations.PostgresFS, "postgres")
		if err != nil {
			return nil, fmt.Errorf("failed to create postgres sub-filesystem: %w", err)
		}
		embedFS = sub
		driverName = "postgres"
	default:
		return nil, fmt.Errorf("unsupported backend: %s", backend)
	}

	source, err := iofs.New(embedFS, ".")
	if err != nil {
		return nil, fmt.Errorf("failed to create iofs source: %w", err)
	}

	var m *migrate.Migrate

	switch driverName {
	case "sqlite":
		driver, err := msqlite.WithInstance(db, &msqlite.Config{
			DatabaseName:    "nram",
			MigrationsTable: "schema_migrations",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create sqlite migration driver: %w", err)
		}
		m, err = migrate.NewWithInstance("iofs", source, "sqlite", driver)
		if err != nil {
			return nil, fmt.Errorf("failed to create migrator: %w", err)
		}
	case "postgres":
		// Acquire advisory lock to prevent concurrent migrations.
		if _, err := db.Exec("SELECT pg_advisory_lock(1)"); err != nil {
			return nil, fmt.Errorf("failed to acquire advisory lock: %w", err)
		}
		driver, err := mpg.WithInstance(db, &mpg.Config{
			MigrationsTable: "schema_migrations",
		})
		if err != nil {
			// Release advisory lock on failure.
			_, _ = db.Exec("SELECT pg_advisory_unlock(1)")
			return nil, fmt.Errorf("failed to create postgres migration driver: %w", err)
		}
		m, err = migrate.NewWithInstance("iofs", source, "postgres", driver)
		if err != nil {
			_, _ = db.Exec("SELECT pg_advisory_unlock(1)")
			return nil, fmt.Errorf("failed to create migrator: %w", err)
		}
	}

	return &Migrator{m: m, db: db, backend: backend}, nil
}

// Up applies all pending migrations. Returns nil if already up to date.
func (mg *Migrator) Up() error {
	err := mg.m.Up()
	if errors.Is(err, migrate.ErrNoChange) {
		return nil
	}
	return err
}

// Down rolls back one migration step.
func (mg *Migrator) Down() error {
	return mg.m.Steps(-1)
}

// Status returns the current migration version and dirty flag.
func (mg *Migrator) Status() (version uint, dirty bool, err error) {
	v, d, err := mg.m.Version()
	return v, d, err
}

// Close releases advisory locks. It intentionally does NOT call m.Close()
// because golang-migrate's Close() closes the underlying database driver,
// which would close the shared *sql.DB owned by the caller.
func (mg *Migrator) Close() error {
	if mg.backend == "postgres" {
		_, err := mg.db.Exec("SELECT pg_advisory_unlock(1)")
		return err
	}
	return nil
}

// NewMigratorWithDir creates a Migrator using migration files from a filesystem directory.
// This is primarily used for testing with temporary migration files.
func NewMigratorWithDir(db *sql.DB, backend string, dir string) (*Migrator, error) {
	dirFS := os.DirFS(dir)

	source, err := iofs.New(dirFS, ".")
	if err != nil {
		return nil, fmt.Errorf("failed to create iofs source from directory: %w", err)
	}

	var m *migrate.Migrate

	switch backend {
	case "sqlite":
		driver, err := msqlite.WithInstance(db, &msqlite.Config{
			DatabaseName:    "nram",
			MigrationsTable: "schema_migrations",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create sqlite migration driver: %w", err)
		}
		m, err = migrate.NewWithInstance("iofs", source, "sqlite", driver)
		if err != nil {
			return nil, fmt.Errorf("failed to create migrator: %w", err)
		}
	case "postgres":
		if _, err := db.Exec("SELECT pg_advisory_lock(1)"); err != nil {
			return nil, fmt.Errorf("failed to acquire advisory lock: %w", err)
		}
		driver, err := mpg.WithInstance(db, &mpg.Config{
			MigrationsTable: "schema_migrations",
		})
		if err != nil {
			_, _ = db.Exec("SELECT pg_advisory_unlock(1)")
			return nil, fmt.Errorf("failed to create postgres migration driver: %w", err)
		}
		m, err = migrate.NewWithInstance("iofs", source, "postgres", driver)
		if err != nil {
			_, _ = db.Exec("SELECT pg_advisory_unlock(1)")
			return nil, fmt.Errorf("failed to create migrator: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported backend: %s", backend)
	}

	return &Migrator{m: m, db: db, backend: backend}, nil
}
