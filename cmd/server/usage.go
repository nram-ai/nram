package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/version"
)

// migrateToPostgresUsage is the focused usage for the migrate-to-postgres
// subcommand. It is shared by the help dispatcher and the parse-error path in
// main so the two never drift.
const migrateToPostgresUsage = "usage: nram migrate-to-postgres --database-url <url>\n\n" +
	"Copy every table (memories, vectors, graph, settings, ...) from the active\n" +
	"SQLite database into the PostgreSQL instance at <url>. Conflict-safe and\n" +
	"type-aware; run it once against a freshly provisioned Postgres database."

// serviceUsage is the focused usage for the service subcommand. It is shared by
// the help dispatcher and the parse-error path in dispatchServiceCommand so the
// two never drift.
const serviceUsage = "usage: nram service <install|uninstall|start|stop|restart|status> [--user] [--config <path>]\n\n" +
	"Register nram with the native OS service manager (Windows SCM, Linux systemd,\n" +
	"macOS launchd) and control it. Installed services start at boot and restart on\n" +
	"failure. install snapshots the current directory and the DATABASE_URL, PORT,\n" +
	"LOG_LEVEL, and NRAM_CONFIG environment variables into the service.\n\n" +
	"Actions:\n" +
	"  install     register the service (runs 'nram' from the current directory)\n" +
	"  uninstall   remove the service\n" +
	"  start       start the installed service\n" +
	"  stop        stop the installed service\n" +
	"  restart     stop then start the installed service\n" +
	"  status      report whether the service is running, stopped, or absent\n\n" +
	"Flags:\n" +
	"  --user      install a per-user service (Linux systemctl --user, macOS\n" +
	"              LaunchAgent); ignored on Windows\n" +
	"  --config <path>\n" +
	"              record this config path in the installed service (resolved to an\n" +
	"              absolute path at install time)\n\n" +
	"install and uninstall usually require elevated privileges (sudo, or an\n" +
	"Administrator shell on Windows) unless --user is given."

// usageText is the top-level help screen. nram parses its command line by hand
// (no flag package), so the usage is hand-maintained here. The bootstrap env
// vars mirror internal/config/load.go applyEnv; the default port mirrors
// internal/config DefaultConfig.
const usageText = version.Name + ` (` + version.Short + `) ` + version.Version + `: ` + version.Tagline + `

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
  service <action> [--user]    install/uninstall/start/stop/restart/status the
                               binary as a native OS service, then exit

Flags:
  --config <path>              load configuration from this YAML file
                               (default: $NRAM_CONFIG, else ./config.yaml)
  --workdir <path>             change to this directory before loading config
                               (set automatically by 'service install')
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

// printVersion writes the product identity banner (full name, semantic version,
// copyright, and license) followed by the build identity (short commit, dirty
// flag, build time, and Go runtime) read from the embedded VCS stamps. Writes to
// the version stream are best-effort, so the return values are deliberately
// ignored.
func printVersion(w io.Writer) {
	b := version.Get()
	_, _ = fmt.Fprintf(w, "%s (%s) %s\n", version.Name, version.Short, b.Version)
	_, _ = fmt.Fprintf(w, "%s\n", version.Copyright)
	_, _ = fmt.Fprintf(w, "%s. %s\n\n", version.License, version.Homepage)
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

// instanceIDForVersion does a best-effort, side-effect-free read of the
// persistent instance UUID for the --version banner. It never creates a SQLite
// database file: when the backend is SQLite and ./nram.db does not exist, it
// reports the instance as uninitialized rather than opening (and thus creating)
// the file. Any config-load, connect, or read failure collapses to (", false).
func instanceIDForVersion() (string, bool) {
	cfg, err := config.Load(configPathFromArgs(os.Args))
	if err != nil {
		return "", false
	}

	// Guard the SQLite path so a version check never creates ./nram.db.
	if cfg.Database.URL == "" {
		if _, statErr := os.Stat("nram.db"); statErr != nil {
			return "", false
		}
	}

	db, err := storage.Open(cfg.Database)
	if err != nil {
		return "", false
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return storage.ReadInstanceID(ctx, db)
}

// flagValueFromArgs returns the value following the named value-flag in args, or
// "" when the flag is absent or has no value. nram parses its command line by
// hand (no flag package), so this is the one place value flags like --config and
// --workdir are resolved, keeping every caller consistent.
func flagValueFromArgs(args []string, name string) string {
	for i, arg := range args[1:] {
		if arg == name && i+1 < len(args[1:]) {
			return args[i+2]
		}
	}
	return ""
}

// configPathFromArgs returns the value of the --config flag from the command
// line, or "" when it is absent. Shared by main (to load config) and the
// --version banner so the two never drift in how they resolve the flag.
func configPathFromArgs(args []string) string {
	return flagValueFromArgs(args, "--config")
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
		if id, ok := instanceIDForVersion(); ok {
			_, _ = fmt.Fprintf(os.Stdout, "  instance: %s\n", id)
		} else {
			_, _ = fmt.Fprintln(os.Stdout, "  instance: (not initialized)")
		}
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
	case "service":
		if hasHelpToken(args[2:]) {
			_, _ = fmt.Fprintln(os.Stdout, serviceUsage)
			return true
		}
	}

	return false
}
