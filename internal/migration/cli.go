package migration

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// migrateUsageLine is the one-line synopsis shared by the parse-error messages
// and the first line of MigrateUsage, so they never drift.
const migrateUsageLine = "usage: nram migrate <up|down|status|create> [name]"

// MigrateUsage is the full help text for the `migrate` subcommand, printed by
// the top-level CLI help dispatcher when the operator runs `nram migrate --help`.
const MigrateUsage = migrateUsageLine + `

  up               apply all pending migrations
  down             roll back one migration step
  status           print the current migration version and dirty flag
  create <name>    scaffold a new up/down SQL migration pair (in migrations/sqlite and migrations/postgres)`

// RunCLI processes migration CLI commands.
// Returns true if a CLI command was handled (caller should exit), false if not a migration command.
func RunCLI(args []string, db *sql.DB, backend string) (bool, error) {
	if len(args) < 2 {
		return false, nil
	}

	switch args[1] {
	case "migrate":
		return handleMigrate(args, db, backend)
	default:
		return false, nil
	}
}

func handleMigrate(args []string, db *sql.DB, backend string) (bool, error) {
	if len(args) < 3 {
		return true, errors.New(migrateUsageLine)
	}

	switch args[2] {
	case "up":
		m, err := NewMigrator(db, backend)
		if err != nil {
			return true, fmt.Errorf("failed to create migrator: %w", err)
		}
		defer func() { _ = m.Close() }()
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
		defer func() { _ = m.Close() }()
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
		defer func() { _ = m.Close() }()
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
		return true, fmt.Errorf("unknown migrate command: %s\n%s", args[2], migrateUsageLine)
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
