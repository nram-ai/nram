package migration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// migrationTables is the ordered list of tables to copy during SQLite-to-Postgres migration.
var migrationTables = []string{
	"namespaces",
	"organizations",
	"users",
	"api_keys",
	"projects",
	"settings",
	"memories",
	"entities",
	"relationships",
	"entity_aliases",
	"memory_lineage",
	"ingestion_log",
	"enrichment_queue",
	"webhooks",
	"memory_shares",
	"oauth_clients",
	"oauth_authorization_codes",
	"oauth_refresh_tokens",
	"oauth_idp_configs",
	"system_meta",
}

// RunCLI processes migration CLI commands.
// Returns true if a CLI command was handled (caller should exit), false if not a migration command.
func RunCLI(args []string, db *sql.DB, backend string) (bool, error) {
	if len(args) < 2 {
		return false, nil
	}

	switch args[1] {
	case "migrate":
		return handleMigrate(args, db, backend)
	case "migrate-to-postgres":
		return true, handleMigrateToPostgres(args, db)
	case "migrate-vectors":
		return true, handleMigrateVectors(args, db)
	default:
		return false, nil
	}
}

func handleMigrate(args []string, db *sql.DB, backend string) (bool, error) {
	if len(args) < 3 {
		return true, fmt.Errorf("usage: nram migrate <up|down|status|create> [name]")
	}

	switch args[2] {
	case "up":
		m, err := NewMigrator(db, backend)
		if err != nil {
			return true, fmt.Errorf("failed to create migrator: %w", err)
		}
		defer m.Close()
		if err := m.Up(); err != nil {
			return true, fmt.Errorf("migration up failed: %w", err)
		}
		fmt.Println("migrations applied successfully")
		return true, nil

	case "down":
		m, err := NewMigrator(db, backend)
		if err != nil {
			return true, fmt.Errorf("failed to create migrator: %w", err)
		}
		defer m.Close()
		if err := m.Down(); err != nil {
			return true, fmt.Errorf("migration down failed: %w", err)
		}
		fmt.Println("rolled back one migration step")
		return true, nil

	case "status":
		m, err := NewMigrator(db, backend)
		if err != nil {
			return true, fmt.Errorf("failed to create migrator: %w", err)
		}
		defer m.Close()
		version, dirty, err := m.Status()
		if err != nil {
			return true, fmt.Errorf("failed to get migration status: %w", err)
		}
		fmt.Printf("migration version: %d, dirty: %v\n", version, dirty)
		return true, nil

	case "create":
		if len(args) < 4 {
			return true, fmt.Errorf("usage: nram migrate create <name>")
		}
		name := args[3]
		return true, createMigrationFiles(name)

	default:
		return true, fmt.Errorf("unknown migrate command: %s\nusage: nram migrate <up|down|status|create> [name]", args[2])
	}
}

// createMigrationFiles creates empty migration file pairs in both sqlite and postgres directories.
func createMigrationFiles(name string) error {
	sqlitePath := filepath.Join("migrations", "sqlite")
	postgresPath := filepath.Join("migrations", "postgres")

	nextNum, err := findNextMigrationNumber(sqlitePath, postgresPath)
	if err != nil {
		return fmt.Errorf("failed to determine next migration number: %w", err)
	}

	prefix := fmt.Sprintf("%06d", nextNum)

	for _, dir := range []string{sqlitePath, postgresPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}

		upFile := filepath.Join(dir, prefix+"_"+name+".up.sql")
		downFile := filepath.Join(dir, prefix+"_"+name+".down.sql")

		if err := os.WriteFile(upFile, []byte(""), 0o644); err != nil {
			return fmt.Errorf("failed to create %s: %w", upFile, err)
		}
		if err := os.WriteFile(downFile, []byte(""), 0o644); err != nil {
			return fmt.Errorf("failed to create %s: %w", downFile, err)
		}

		fmt.Printf("created %s\n", upFile)
		fmt.Printf("created %s\n", downFile)
	}

	return nil
}

// findNextMigrationNumber scans both migration directories and returns the next sequence number.
func findNextMigrationNumber(dirs ...string) (int, error) {
	maxNum := 0

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			parts := strings.SplitN(name, "_", 2)
			if len(parts) < 2 {
				continue
			}
			num, err := strconv.Atoi(parts[0])
			if err != nil {
				continue
			}
			if num > maxNum {
				maxNum = num
			}
		}
	}

	return maxNum + 1, nil
}

func handleMigrateToPostgres(args []string, sqliteDB *sql.DB) error {
	var pgURL string
	for i, arg := range args {
		if arg == "--database-url" && i+1 < len(args) {
			pgURL = args[i+1]
			break
		}
	}
	if pgURL == "" {
		return fmt.Errorf("usage: nram migrate-to-postgres --database-url <url>")
	}

	// Open Postgres connection.
	pgDB, err := sql.Open("pgx", pgURL)
	if err != nil {
		return fmt.Errorf("failed to open postgres database: %w", err)
	}
	defer pgDB.Close()

	if err := pgDB.Ping(); err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	fmt.Println("connected to postgres, running migrations...")

	// Run Postgres migrations.
	m, err := NewMigrator(pgDB, "postgres")
	if err != nil {
		return fmt.Errorf("failed to create postgres migrator: %w", err)
	}
	if err := m.Up(); err != nil {
		m.Close()
		return fmt.Errorf("postgres migration failed: %w", err)
	}
	m.Close()

	fmt.Println("postgres migrations applied, copying data...")

	// Copy data table by table.
	for _, table := range migrationTables {
		if err := copyTable(sqliteDB, pgDB, table); err != nil {
			return fmt.Errorf("failed to copy table %s: %w", table, err)
		}
	}

	// Validate row counts.
	fmt.Println("validating row counts...")
	for _, table := range migrationTables {
		if err := validateRowCount(sqliteDB, pgDB, table); err != nil {
			return fmt.Errorf("row count validation failed for table %s: %w", table, err)
		}
	}

	// Update system_meta to indicate postgres backend.
	_, err = pgDB.Exec("UPDATE system_meta SET value = 'postgres' WHERE key = 'storage_backend'")
	if err != nil {
		// Try insert if the key doesn't exist.
		_, err = pgDB.Exec("INSERT INTO system_meta (key, value) VALUES ('storage_backend', 'postgres')")
		if err != nil {
			return fmt.Errorf("failed to update system_meta storage_backend: %w", err)
		}
	}

	fmt.Println("migration to postgres completed successfully")
	return nil
}

// copyTable copies all rows from a SQLite table to the same Postgres table.
func copyTable(srcDB, dstDB *sql.DB, table string) error {
	ctx := context.Background()

	// Get column names from source.
	rows, err := srcDB.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", table))
	if err != nil {
		// Table might not exist in source; skip silently.
		fmt.Printf("  skipping %s (not found in source)\n", table)
		return nil
	}
	columns, err := rows.Columns()
	rows.Close()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	if len(columns) == 0 {
		fmt.Printf("  skipping %s (no columns)\n", table)
		return nil
	}

	// Read all rows from source.
	dataRows, err := srcDB.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return fmt.Errorf("failed to query source: %w", err)
	}
	defer dataRows.Close()

	// Build insert statement with positional parameters.
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Begin transaction on destination.
	tx, err := dstDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	rowCount := 0
	for dataRows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := dataRows.Scan(valuePtrs...); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert row: %w", err)
		}
		rowCount++
	}

	if err := dataRows.Err(); err != nil {
		tx.Rollback()
		return fmt.Errorf("error reading source rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("  copied %d rows from %s\n", rowCount, table)
	return nil
}

// validateRowCount checks that the row count matches between source and destination.
func validateRowCount(srcDB, dstDB *sql.DB, table string) error {
	var srcCount, dstCount int64

	err := srcDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&srcCount)
	if err != nil {
		// Source table might not exist; treat as 0 rows.
		srcCount = 0
	}

	err = dstDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&dstCount)
	if err != nil {
		return fmt.Errorf("failed to count destination rows: %w", err)
	}

	if srcCount != dstCount {
		return fmt.Errorf("count mismatch: source=%d, destination=%d", srcCount, dstCount)
	}

	return nil
}

// ParseMigrateArgs is exported for testing CLI argument parsing.
// It returns the subcommand and any additional argument.
func ParseMigrateArgs(args []string) (command string, extra string, err error) {
	if len(args) < 2 {
		return "", "", fmt.Errorf("no command provided")
	}

	if args[1] == "migrate-to-postgres" {
		pgURL := ""
		for i, arg := range args {
			if arg == "--database-url" && i+1 < len(args) {
				pgURL = args[i+1]
				break
			}
		}
		return "migrate-to-postgres", pgURL, nil
	}

	if args[1] == "migrate-vectors" {
		qdrantAddr := ""
		for i, arg := range args {
			if arg == "--qdrant-addr" && i+1 < len(args) {
				qdrantAddr = args[i+1]
				break
			}
		}
		return "migrate-vectors", qdrantAddr, nil
	}

	if args[1] != "migrate" {
		return "", "", fmt.Errorf("not a migration command")
	}

	if len(args) < 3 {
		return "", "", fmt.Errorf("no subcommand provided")
	}

	subcmd := args[2]
	validCmds := []string{"up", "down", "status", "create"}
	sort.Strings(validCmds)
	idx := sort.SearchStrings(validCmds, subcmd)
	if idx >= len(validCmds) || validCmds[idx] != subcmd {
		return "", "", fmt.Errorf("unknown subcommand: %s", subcmd)
	}

	extra = ""
	if subcmd == "create" && len(args) >= 4 {
		extra = args[3]
	}

	return subcmd, extra, nil
}
