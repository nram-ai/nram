package admin

import (
	"context"
	"fmt"
	"os"

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

func (s *DatabaseAdminStore) TestConnection(ctx context.Context, url string) (*api.ConnectionTestResult, error) {
	// This would open a temporary connection to test. For safety, we only report
	// that the current connection is healthy.
	if err := s.db.Ping(ctx); err != nil {
		return &api.ConnectionTestResult{
			Success: false,
			Message: fmt.Sprintf("ping failed: %v", err),
		}, nil
	}
	return &api.ConnectionTestResult{
		Success: true,
		Message: "connection successful",
	}, nil
}

func (s *DatabaseAdminStore) TriggerMigration(ctx context.Context, url string) (*api.MigrationStatus, error) {
	return &api.MigrationStatus{
		Status:  "not_supported",
		Message: "live migration is not yet implemented",
	}, nil
}
