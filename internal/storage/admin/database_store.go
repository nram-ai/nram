package admin

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx stdlib driver
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// DatabaseAdminStore implements api.DatabaseAdminStore using the DB interface.
type DatabaseAdminStore struct {
	db storage.DB
}

// NewDatabaseAdminStore creates a new DatabaseAdminStore.
func NewDatabaseAdminStore(db storage.DB) *DatabaseAdminStore {
	return &DatabaseAdminStore{db: db}
}

func (s *DatabaseAdminStore) GetDatabaseInfo(ctx context.Context) (*api.DatabaseInfo, error) {
	info := &api.DatabaseInfo{
		Backend: s.db.Backend(),
	}

	// Data counts.
	tables := []struct {
		table string
		dest  *int
	}{
		{"memories", &info.DataCounts.Memories},
		{"entities", &info.DataCounts.Entities},
		{"projects", &info.DataCounts.Projects},
		{"users", &info.DataCounts.Users},
		{"organizations", &info.DataCounts.Organizations},
	}
	for _, t := range tables {
		row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM "+t.table)
		if err := row.Scan(t.dest); err != nil {
			*t.dest = 0
		}
	}

	// Vector counts: SQLite has a single memory_vectors table; Postgres has per-dimension tables.
	if s.db.Backend() == storage.BackendSQLite {
		row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM memory_vectors")
		if err := row.Scan(&info.DataCounts.Vectors); err != nil {
			info.DataCounts.Vectors = 0
		}
	} else {
		pgVectorTables := []string{
			"memory_vectors_384", "memory_vectors_512", "memory_vectors_768",
			"memory_vectors_1024", "memory_vectors_1536", "memory_vectors_3072",
		}
		var total int
		for _, vt := range pgVectorTables {
			var count int
			row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM "+vt)
			if row.Scan(&count) == nil {
				total += count
			}
		}
		info.DataCounts.Vectors = total
	}

	if s.db.Backend() == storage.BackendSQLite {
		// SQLite version.
		row := s.db.QueryRow(ctx, "SELECT sqlite_version()")
		row.Scan(&info.Version)

		// File info.
		fi, err := os.Stat("nram.db")
		if err == nil {
			info.SQLite = &api.SQLiteInfo{
				FilePath: "nram.db",
				FileSize: fi.Size(),
			}
		}
	} else {
		// PostgreSQL version.
		row := s.db.QueryRow(ctx, "SELECT version()")
		row.Scan(&info.Version)

		pgInfo := &api.PostgresInfo{}

		// Connection pool stats from the standard sql.DB.
		dbStats := s.db.DB().Stats()
		pgInfo.ActiveConns = dbStats.InUse
		pgInfo.IdleConns = dbStats.Idle
		pgInfo.MaxConns = dbStats.MaxOpenConnections

		// Check pgvector.
		var pgvVersion string
		row = s.db.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'vector'")
		if row.Scan(&pgvVersion) == nil {
			pgInfo.PgvectorVersion = pgvVersion
		}

		info.Postgres = pgInfo
	}

	return info, nil
}

// TestConnection opens a temporary Postgres connection to the provided URL,
// pings it, checks for pgvector, measures latency, and then closes the connection.
func (s *DatabaseAdminStore) TestConnection(ctx context.Context, url string) (*api.ConnectionTestResult, error) {
	start := time.Now()

	db, err := sql.Open("pgx", url)
	if err != nil {
		return &api.ConnectionTestResult{
			Success: false,
			Message: fmt.Sprintf("failed to open connection: %v", err),
		}, nil
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(10 * time.Second)

	if err := db.PingContext(ctx); err != nil {
		return &api.ConnectionTestResult{
			Success:   false,
			Message:   fmt.Sprintf("ping failed: %v", err),
			LatencyMs: time.Since(start).Milliseconds(),
		}, nil
	}

	latency := time.Since(start).Milliseconds()

	// Check whether pgvector is installed.
	var pgvVersion string
	pgvInstalled := false
	row := db.QueryRowContext(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'vector'")
	if row.Scan(&pgvVersion) == nil {
		pgvInstalled = true
	}

	return &api.ConnectionTestResult{
		Success:           true,
		Message:           "connection successful",
		PgvectorInstalled: pgvInstalled,
		LatencyMs:         latency,
	}, nil
}

// TriggerMigration runs a full SQLite-to-Postgres data migration.
// It rejects the request if the current backend is already Postgres.
func (s *DatabaseAdminStore) TriggerMigration(ctx context.Context, url string) (*api.MigrationStatus, error) {
	if s.db.Backend() != storage.BackendSQLite {
		return &api.MigrationStatus{
			Status:  "error",
			Message: "migration is only supported from SQLite; current backend is already postgres",
		}, nil
	}

	dm, err := newDataMigrator(ctx, s.db.DB(), url)
	if err != nil {
		return &api.MigrationStatus{
			Status:  "error",
			Message: fmt.Sprintf("failed to initialize migration: %v", err),
		}, nil
	}
	defer dm.Close()

	if err := dm.Run(ctx); err != nil {
		return &api.MigrationStatus{
			Status:  "error",
			Message: fmt.Sprintf("migration failed: %v", err),
		}, nil
	}

	return &api.MigrationStatus{
		Status:  "complete",
		Message: "all data successfully migrated to postgres",
	}, nil
}
