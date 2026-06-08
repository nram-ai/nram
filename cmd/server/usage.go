package main

import (
	"fmt"
	"io"
	"os"

	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/version"
)

// migrateToPostgresUsage is the focused usage for the migrate-to-postgres
// subcommand. It is shared by the help dispatcher and the parse-error path in
// main so the two never drift.
const migrateToPostgresUsage = "usage: nram migrate-to-postgres --database-url <url>\n\n" +
	"Copy every table (memories, vectors, graph, settings, ...) from the active\n" +
	"SQLite database into the PostgreSQL instance at <url>. Conflict-safe and\n" +
	"type-aware; run it once against a freshly provisioned Postgres database."

// usageText is the top-level help screen. nram parses its command line by hand
// (no flag package), so the usage is hand-maintained here. The bootstrap env
// vars mirror internal/config/load.go applyEnv; the default port mirrors
// internal/config DefaultConfig.
const usageText = `nram ` + version.Version + ` — persistent memory server for AI agents

Usage:
  nram [flags]                 start the server (default when no command is given)
  nram <command> [args]        run a one-shot command, then exit

Commands:
  migrate up                   apply all pending schema migrations
  migrate down                 roll back one migration step
  migrate status               print the current migration version and dirty flag
  migrate create <name>        scaffold a new up/down SQL migration pair
  migrate-to-postgres --database-url <url>
                               copy the SQLite database into PostgreSQL, then exit

Flags:
  --config <path>              load configuration from this YAML file
                               (default: $NRAM_CONFIG, else ./config.yaml)
  --backfill-enrichment        enqueue enrichment for memories missing it, then exit
  --reembed-all-memories       force re-embed every live memory, then exit
  --normalize-memory-tags      rewrite memory tags to canonical form, then exit
  -h, --help                   show this help and exit
  -v, --version                show version information and exit

Environment:
  PORT                         HTTP listen port (default 8674)
  LOG_LEVEL                    debug | info | warn | error (default info)
  DATABASE_URL                 database connection string (overrides config)
  NRAM_CONFIG                  config file path (alternative to --config)
  NRAM_ADMIN_EMAIL             headless bootstrap administrator email (first boot)
  NRAM_ADMIN_PASS              headless bootstrap administrator password (first boot)

Append --help to a command (e.g. nram migrate --help) for command-specific usage.`

// printUsage writes the top-level help screen. A failed write to the help
// stream is not actionable, so the error is deliberately ignored (matching how
// errcheck already treats os.Stderr help output elsewhere).
func printUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, usageText)
}

// printVersion writes the build identity (semantic version, short commit, dirty
// flag, and Go runtime) using the embedded VCS stamps. Writes to the version
// stream are best-effort, so the return values are deliberately ignored.
func printVersion(w io.Writer) {
	b := version.Get()
	_, _ = fmt.Fprintf(w, "nram %s\n", b.Version)
	_, _ = fmt.Fprintf(w, "  commit: %s", b.Commit)
	if b.Dirty {
		_, _ = fmt.Fprint(w, " (dirty)")
	}
	_, _ = fmt.Fprintln(w)
	if b.Time != "" {
		_, _ = fmt.Fprintf(w, "  built:  %s\n", b.Time)
	}
	_, _ = fmt.Fprintf(w, "  go:     %s\n", b.Go)
}

// hasHelpToken reports whether any of the given args is a help request.
func hasHelpToken(args []string) bool {
	for _, a := range args {
		switch a {
		case "-h", "--help", "help":
			return true
		}
	}
	return false
}

// handleInfoFlags answers the --help/-h, --version/-v, and per-subcommand help
// requests. These must work without a config file or database, so main calls
// this first and returns when it reports true. Output goes to stdout and the
// process exits 0, unlike the error usages that go to stderr with a non-zero
// status.
func handleInfoFlags(args []string) bool {
	if len(args) < 2 {
		return false
	}

	switch args[1] {
	case "-v", "--version":
		printVersion(os.Stdout)
		return true
	case "-h", "--help", "help":
		printUsage(os.Stdout)
		return true
	case "migrate":
		if hasHelpToken(args[2:]) {
			_, _ = fmt.Fprintln(os.Stdout, migration.MigrateUsage)
			return true
		}
	case "migrate-to-postgres":
		if hasHelpToken(args[2:]) {
			_, _ = fmt.Fprintln(os.Stdout, migrateToPostgresUsage)
			return true
		}
	}

	return false
}
