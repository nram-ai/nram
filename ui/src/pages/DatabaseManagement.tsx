import { useState, useCallback, useMemo, useEffect } from "react";
import {
  useDatabaseInfo,
  useMigrationAudit,
  usePreflightDatabase,
  useResetDatabase,
  useTriggerMigration,
} from "../hooks/useApi";
import type {
  MigrationAudit,
  MigrationStats,
  PreflightReport,
  ResetMode,
} from "../api/client";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function poolBarWidth(value: number, max: number): string {
  if (max <= 0) return "0%";
  return `${Math.min(100, Math.round((value / max) * 100))}%`;
}

// ---------------------------------------------------------------------------
// Spinner
// ---------------------------------------------------------------------------

function Spinner({ className = "h-4 w-4" }: { className?: string }) {
  return (
    <svg
      className={`animate-spin ${className}`}
      fill="none"
      viewBox="0 0 24 24"
    >
      <circle
        className="opacity-25"
        cx="12"
        cy="12"
        r="10"
        stroke="currentColor"
        strokeWidth="4"
      />
      <path
        className="opacity-75"
        fill="currentColor"
        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
      />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Data Counts Card
// ---------------------------------------------------------------------------

function DataCountsCard({
  counts,
}: {
  counts: {
    memories: number;
    entities: number;
    projects: number;
    users: number;
    organizations: number;
    vectors: number;
  };
}) {
  const items = [
    { label: "Memories", value: counts.memories },
    { label: "Vectors", value: counts.vectors },
    { label: "Entities", value: counts.entities },
    { label: "Projects", value: counts.projects },
    { label: "Users", value: counts.users },
    { label: "Organizations", value: counts.organizations },
  ];

  return (
    <div className="rounded-lg border border-border bg-card shadow-sm">
      <div className="border-b border-border px-5 py-4">
        <h3 className="text-sm font-semibold text-foreground">Data Counts</h3>
      </div>
      <div className="grid grid-cols-2 gap-4 px-5 py-4 sm:grid-cols-3 lg:grid-cols-6">
        {items.map((item) => (
          <div key={item.label} className="text-center">
            <p className="text-2xl font-bold text-foreground">
              {item.value.toLocaleString()}
            </p>
            <p className="text-xs text-muted-foreground">{item.label}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// SQLite View
// ---------------------------------------------------------------------------

type MigrationStep =
  | "input"
  | "preflight"
  | "audit"
  | "migrating"
  | "complete";

// ---------------------------------------------------------------------------
// Migration error display — translates raw backend errors into actionable messages
// ---------------------------------------------------------------------------

interface FriendlyError {
  title: string;
  description: string;
  hint?: string;
}

function parseMigrationError(raw: string): FriendlyError {
  const lower = raw.toLowerCase();

  if (lower.includes('type "vector" does not exist') || lower.includes('42704')) {
    return {
      title: "pgvector extension not installed",
      description:
        "The PostgreSQL database is missing the pgvector extension, which is required for vector similarity search.",
      hint: 'Run as a database superuser: CREATE EXTENSION IF NOT EXISTS vector;',
    };
  }

  if (lower.includes("permission denied") || lower.includes("42501")) {
    return {
      title: "Insufficient database permissions",
      description:
        "The database user does not have permission to create tables or extensions.",
      hint: "Ensure the nram database user has CREATE privileges on the target database, or run migrations as a superuser.",
    };
  }

  if (lower.includes("connection refused") || lower.includes("could not connect")) {
    return {
      title: "Cannot connect to PostgreSQL",
      description: "The database server is not reachable. Check that PostgreSQL is running and the connection URL is correct.",
    };
  }

  if (lower.includes("password authentication failed") || lower.includes("28p01")) {
    return {
      title: "Authentication failed",
      description: "The database username or password is incorrect.",
    };
  }

  if (lower.includes("database") && lower.includes("does not exist")) {
    return {
      title: "Database does not exist",
      description: "The target database has not been created yet.",
      hint: "Create it first: CREATE DATABASE nram OWNER nram;",
    };
  }

  if (lower.includes("dirty database")) {
    return {
      title: "Database is in a dirty state",
      description:
        "A previous migration was interrupted. The schema_migrations table has a dirty flag set.",
      hint: "Connect to the database and run: UPDATE schema_migrations SET dirty = false;",
    };
  }

  return {
    title: "Migration failed",
    description: raw.length > 300 ? raw.slice(0, 300) + "..." : raw,
  };
}

function MigratingIndicator() {
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => setElapsed((e) => e + 1), 1000);
    return () => clearInterval(interval);
  }, []);

  const minutes = Math.floor(elapsed / 60);
  const seconds = elapsed % 60;
  const timeStr =
    minutes > 0
      ? `${minutes}m ${seconds.toString().padStart(2, "0")}s`
      : `${seconds}s`;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Spinner className="h-5 w-5 text-primary" />
        <div>
          <p className="text-sm font-medium text-foreground">
            Migration in progress...
          </p>
          <p className="text-xs text-muted-foreground">
            Transferring data from SQLite to PostgreSQL. Do not close this page.
          </p>
        </div>
        <span className="ml-auto text-xs font-mono text-muted-foreground">
          {timeStr}
        </span>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-muted">
        <div className="h-full animate-pulse rounded-full bg-primary" />
      </div>
      {elapsed > 30 && (
        <p className="text-xs text-muted-foreground">
          Large databases may take several minutes. Schema creation and index building can be slow on first run.
        </p>
      )}
      {elapsed > 120 && (
        <p className="text-xs text-amber-600 dark:text-amber-400">
          This is taking longer than expected. Check the server logs for errors. If the server process has stopped, you may need to resolve the issue and restart.
        </p>
      )}
    </div>
  );
}

function MigrationErrorDisplay({ error }: { error: string }) {
  const parsed = useMemo(() => parseMigrationError(error), [error]);

  return (
    <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-700 dark:bg-red-900/30">
      <div className="flex items-start gap-3">
        <svg
          className="mt-0.5 h-5 w-5 flex-shrink-0 text-red-600 dark:text-red-400"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
          />
        </svg>
        <div className="space-y-1">
          <p className="text-sm font-semibold text-red-800 dark:text-red-200">
            {parsed.title}
          </p>
          <p className="text-sm text-red-700 dark:text-red-300">
            {parsed.description}
          </p>
          {parsed.hint && (
            <div className="mt-2 rounded-md border border-red-200 bg-red-100/50 px-3 py-2 dark:border-red-800 dark:bg-red-950/50">
              <p className="text-xs font-medium text-red-800 dark:text-red-300">
                How to fix:
              </p>
              <code className="mt-1 block text-xs text-red-700 dark:text-red-400">
                {parsed.hint}
              </code>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function SQLiteView({
  filePath,
  fileSizeBytes,
  dataCounts,
}: {
  filePath: string;
  fileSizeBytes: number;
  dataCounts: {
    memories: number;
    entities: number;
    projects: number;
    users: number;
    organizations: number;
    vectors: number;
  };
}) {
  const [step, setStep] = useState<MigrationStep>("input");
  const [dbUrl, setDbUrl] = useState("");
  const [preflight, setPreflight] = useState<PreflightReport | null>(null);
  const [audit, setAudit] = useState<MigrationAudit | null>(null);
  const [migrationError, setMigrationError] = useState<string | null>(null);
  const [migrationStats, setMigrationStats] = useState<MigrationStats | null>(
    null,
  );

  const preflightMutation = usePreflightDatabase();
  const resetMutation = useResetDatabase();
  const auditMutation = useMigrationAudit();
  const migrateMutation = useTriggerMigration();

  const handleRunPreflight = useCallback(() => {
    setPreflight(null);
    setMigrationError(null);
    preflightMutation.mutate(dbUrl, {
      onSuccess: (report) => {
        setPreflight(report);
        setStep("preflight");
      },
      onError: (err) => {
        setPreflight({
          ok: false,
          checks: [
            {
              name: "connection",
              status: "error",
              message: err instanceof Error ? err.message : "Preflight request failed",
              remediation: "Check that the server is reachable and the URL is valid.",
            },
          ],
        });
        setStep("preflight");
      },
    });
  }, [dbUrl, preflightMutation]);

  const handleReset = useCallback(
    (mode: ResetMode) => {
      if (
        !window.confirm(
          mode === "truncate"
            ? "Truncate all nram tables in the target database? Existing data will be wiped but the schema and pgvector extension will be preserved."
            : "Drop all nram tables in the target database? The schema will be recreated on the next migration. This is destructive — continue?",
        )
      ) {
        return;
      }
      resetMutation.mutate(
        { url: dbUrl, mode },
        {
          onSuccess: () => {
            // Re-run preflight so the target_state check reflects the reset.
            handleRunPreflight();
          },
          onError: (err) => {
            setMigrationError(
              err instanceof Error ? err.message : "Reset failed",
            );
          },
        },
      );
    },
    [dbUrl, resetMutation, handleRunPreflight],
  );

  const handleRunAudit = useCallback(() => {
    auditMutation.mutate(undefined, {
      onSuccess: (result) => {
        setAudit(result);
        setStep("audit");
      },
      onError: (err) => {
        setMigrationError(
          err instanceof Error ? err.message : "Audit failed",
        );
      },
    });
  }, [auditMutation]);

  const handleStartMigration = useCallback(() => {
    setMigrationError(null);
    setMigrationStats(null);
    setStep("migrating");
    migrateMutation.mutate(dbUrl, {
      onSuccess: (data) => {
        setMigrationStats(data.stats ?? null);
        if (data.status === "complete") {
          setStep("complete");
        } else {
          setMigrationError(data.message || "Migration failed");
          setStep("audit");
        }
      },
      onError: (error) => {
        setMigrationError(
          error instanceof Error ? error.message : "Migration failed",
        );
        setStep("audit");
      },
    });
  }, [dbUrl, migrateMutation]);

  const handleStartOver = useCallback(() => {
    setStep("input");
    setDbUrl("");
    setPreflight(null);
    setAudit(null);
    setMigrationError(null);
    setMigrationStats(null);
  }, []);

  const preflightTargetState = preflight?.checks.find(
    (c) => c.name === "target_state",
  );
  const preflightBlocking =
    preflight?.checks.some(
      (c) => c.status === "error" && c.name !== "target_state",
    ) ?? false;

  return (
    <div className="space-y-6">
      {/* Backend info card */}
      <div className="rounded-lg border border-border bg-card shadow-sm">
        <div className="border-b border-border px-5 py-4">
          <div className="flex items-center gap-3">
            <span className="inline-block h-3 w-3 rounded-full bg-blue-500" />
            <h3 className="text-sm font-semibold text-foreground">
              SQLite Backend
            </h3>
          </div>
        </div>
        <div className="px-5 py-4">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <p className="text-xs text-muted-foreground">File Path</p>
              <p className="mt-1 break-all font-mono text-sm text-foreground">
                {filePath}
              </p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">File Size</p>
              <p className="mt-1 text-sm font-medium text-foreground">
                {formatBytes(fileSizeBytes)}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Warning banner */}
      <div className="rounded-lg border border-amber-300 bg-amber-50 p-4 dark:border-amber-700 dark:bg-amber-900/30">
        <div className="flex items-start gap-3">
          <svg
            className="mt-0.5 h-5 w-5 flex-shrink-0 text-amber-600 dark:text-amber-400"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"
            />
          </svg>
          <div>
            <p className="text-sm font-medium text-amber-800 dark:text-amber-200">
              Single-Instance Limitation
            </p>
            <p className="mt-1 text-sm text-amber-700 dark:text-amber-300">
              SQLite is single-instance only. For horizontal scaling, vector
              search, and LLM enrichment, upgrade to PostgreSQL.
            </p>
          </div>
        </div>
      </div>

      {/* Data counts */}
      <DataCountsCard counts={dataCounts} />

      {/* Upgrade to Postgres section */}
      <div className="rounded-lg border border-border bg-card shadow-sm">
        <div className="border-b border-border px-5 py-4">
          <h3 className="text-sm font-semibold text-foreground">
            Upgrade to PostgreSQL
          </h3>
          <p className="mt-1 text-xs text-muted-foreground">
            Migrate your data from SQLite to PostgreSQL for full feature support.
          </p>
        </div>
        <div className="px-5 py-4">
          {/* Step indicators */}
          <div className="mb-6 flex items-center gap-2 flex-wrap">
            {(
              [
                { key: "input", label: "1. Connect" },
                { key: "preflight", label: "2. Preflight" },
                { key: "audit", label: "3. Audit orphans" },
                { key: "migrating", label: "4. Migrate" },
              ] as const
            ).map((s, i) => {
              const order = ["input", "preflight", "audit", "migrating", "complete"];
              const curIdx = order.indexOf(step);
              const sIdx = order.indexOf(s.key);
              const isActive =
                step === s.key ||
                (step === "complete" && s.key === "migrating");
              const isPast = curIdx > sIdx;
              return (
                <div key={s.key} className="flex items-center gap-2">
                  {i > 0 && (
                    <div
                      className={`h-px w-6 ${isPast || isActive ? "bg-primary" : "bg-border"}`}
                    />
                  )}
                  <span
                    className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${
                      isActive
                        ? "bg-primary text-primary-foreground"
                        : isPast
                          ? "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300"
                          : "bg-muted text-muted-foreground"
                    }`}
                  >
                    {isPast && !isActive ? (
                      <svg
                        className="mr-1 h-3 w-3"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M5 13l4 4L19 7"
                        />
                      </svg>
                    ) : null}
                    {s.label}
                  </span>
                </div>
              );
            })}
          </div>

          {/* Step 1: Enter DATABASE_URL */}
          {step === "input" && (
            <div className="space-y-4">
              <div>
                <label className="mb-1 block text-sm font-medium text-foreground">
                  PostgreSQL Connection URL
                </label>
                <input
                  type="text"
                  value={dbUrl}
                  onChange={(e) => {
                    setDbUrl(e.target.value);
                    setPreflight(null);
                  }}
                  placeholder="postgres://user:password@host:5432/dbname?sslmode=disable"
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
                />
                <p className="mt-1 text-xs text-muted-foreground">
                  Enter a full PostgreSQL connection string with pgvector
                  extension installed.
                </p>
              </div>
              <button
                type="button"
                onClick={handleRunPreflight}
                disabled={!dbUrl.trim() || preflightMutation.isPending}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {preflightMutation.isPending ? (
                  <span className="flex items-center gap-1.5">
                    <Spinner className="h-3.5 w-3.5" />
                    Running preflight...
                  </span>
                ) : (
                  "Run Preflight Checks"
                )}
              </button>
              <p className="text-xs text-muted-foreground">
                Checks connection, pgvector, privileges, and any data left over
                from prior migration attempts.
              </p>
            </div>
          )}

          {/* Step 2: Preflight checklist */}
          {step === "preflight" && preflight && (
            <div className="space-y-4">
              <PreflightChecklist report={preflight} />

              {/* Reset options if target has leftover data */}
              {preflightTargetState?.status === "warn" && (
                <ResetOptions
                  targetCounts={preflightTargetState.table_counts}
                  pending={resetMutation.isPending}
                  onReset={handleReset}
                />
              )}

              {migrationError && <MigrationErrorDisplay error={migrationError} />}

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={handleRunAudit}
                  disabled={preflightBlocking || auditMutation.isPending}
                  className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {auditMutation.isPending ? (
                    <span className="flex items-center gap-1.5">
                      <Spinner className="h-3.5 w-3.5" />
                      Scanning SQLite...
                    </span>
                  ) : (
                    "Scan for Orphans"
                  )}
                </button>
                <button
                  type="button"
                  onClick={handleRunPreflight}
                  disabled={preflightMutation.isPending}
                  className="rounded-md border border-input px-4 py-2 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
                >
                  Re-run Preflight
                </button>
                <button
                  type="button"
                  onClick={handleStartOver}
                  className="rounded-md border border-input px-4 py-2 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
                >
                  Back
                </button>
              </div>
              {preflightBlocking && (
                <p className="text-xs text-red-600 dark:text-red-400">
                  Resolve the preflight errors above before proceeding.
                </p>
              )}
            </div>
          )}

          {/* Step 3: Orphan audit */}
          {step === "audit" && audit && (
            <div className="space-y-4">
              <OrphanAuditSummary audit={audit} />

              <div className="rounded-md bg-muted/50 p-4">
                <p className="text-sm font-medium text-foreground">Source data summary</p>
                <div className="mt-2 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
                  {[
                    { label: "Memories", count: dataCounts.memories },
                    { label: "Vectors", count: dataCounts.vectors },
                    { label: "Entities", count: dataCounts.entities },
                    { label: "Projects", count: dataCounts.projects },
                    { label: "Users", count: dataCounts.users },
                    { label: "Organizations", count: dataCounts.organizations },
                  ].map((item) => (
                    <div
                      key={item.label}
                      className="rounded-md border border-border bg-background p-3 text-center"
                    >
                      <p className="text-lg font-bold text-foreground">
                        {item.count.toLocaleString()}
                      </p>
                      <p className="text-xs text-muted-foreground">{item.label}</p>
                    </div>
                  ))}
                </div>
              </div>

              {migrationError && <MigrationErrorDisplay error={migrationError} />}

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={handleStartMigration}
                  className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90"
                >
                  {audit.total_orphans > 0
                    ? `Drop ${audit.total_orphans} orphan${audit.total_orphans === 1 ? "" : "s"} and migrate`
                    : "Confirm Migration"}
                </button>
                <button
                  type="button"
                  onClick={() => setStep("preflight")}
                  className="rounded-md border border-input px-4 py-2 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
                >
                  Back
                </button>
              </div>
            </div>
          )}

          {/* Step 4: Migrating */}
          {step === "migrating" && <MigratingIndicator />}

          {/* Step 5: Complete */}
          {step === "complete" && (
            <div className="space-y-4">
              <div className="flex items-start gap-3 rounded-md bg-green-50 p-4 dark:bg-green-900/30">
                <svg
                  className="mt-0.5 h-5 w-5 flex-shrink-0 text-green-600 dark:text-green-400"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M5 13l4 4L19 7"
                  />
                </svg>
                <div>
                  <p className="text-sm font-medium text-green-800 dark:text-green-200">
                    Migration Complete
                  </p>
                  <p className="mt-1 text-sm text-green-700 dark:text-green-300">
                    Your data has been successfully migrated to PostgreSQL.
                  </p>
                </div>
              </div>

              {migrationStats && <MigrationStatsPanel stats={migrationStats} />}

              <div className="rounded-lg border border-amber-300 bg-amber-50 p-4 dark:border-amber-700 dark:bg-amber-950/30">
                <p className="text-sm font-medium text-amber-800 dark:text-amber-200">
                  Restart Required
                </p>
                <p className="mt-1 text-sm text-amber-700 dark:text-amber-300">
                  The server is still running on SQLite. Restart nram with the Postgres connection URL to complete the switch:
                </p>
                <pre className="mt-2 overflow-x-auto rounded-md border border-amber-300 bg-white px-3 py-2 text-sm font-mono dark:border-amber-700 dark:bg-amber-950/50">
                  DATABASE_URL="{dbUrl}" nram
                </pre>
                <p className="mt-2 text-xs text-amber-600 dark:text-amber-400">
                  Or set <code className="font-mono">database.url</code> in your config file, then restart.
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Preflight / Audit / Stats sub-components
// ---------------------------------------------------------------------------

function PreflightChecklist({ report }: { report: PreflightReport }) {
  return (
    <div className="rounded-md border border-border bg-background">
      <div className="border-b border-border px-4 py-2">
        <p className="text-sm font-medium text-foreground">
          Preflight Results{" "}
          <span
            className={`ml-2 text-xs ${report.ok ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}
          >
            {report.ok ? "ALL OK" : "ISSUES FOUND"}
          </span>
        </p>
      </div>
      <ul className="divide-y divide-border">
        {report.checks.map((c) => {
          const color =
            c.status === "ok"
              ? "text-green-600 dark:text-green-400"
              : c.status === "warn"
                ? "text-amber-600 dark:text-amber-400"
                : "text-red-600 dark:text-red-400";
          return (
            <li key={c.name} className="px-4 py-3">
              <div className="flex items-start gap-3">
                <span className={`mt-0.5 text-xs font-bold uppercase ${color}`}>
                  {c.status}
                </span>
                <div className="flex-1 space-y-1">
                  <p className="text-sm font-medium text-foreground">
                    {prettifyCheckName(c.name)}
                  </p>
                  <p className="text-sm text-muted-foreground">{c.message}</p>
                  {c.remediation && (
                    <p className="text-xs text-foreground">
                      <span className="font-medium">Fix:</span>{" "}
                      <code className="font-mono">{c.remediation}</code>
                    </p>
                  )}
                  {c.table_counts && Object.keys(c.table_counts).length > 0 && (
                    <details className="mt-1">
                      <summary className="cursor-pointer text-xs text-muted-foreground">
                        Table counts (click to expand)
                      </summary>
                      <div className="mt-1 grid grid-cols-2 gap-1 sm:grid-cols-3 lg:grid-cols-4 text-xs font-mono">
                        {Object.entries(c.table_counts)
                          .sort(([, a], [, b]) => b - a)
                          .map(([t, n]) => (
                            <div key={t} className="flex justify-between gap-2">
                              <span className="text-muted-foreground">{t}</span>
                              <span className="text-foreground">{n}</span>
                            </div>
                          ))}
                      </div>
                    </details>
                  )}
                </div>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function prettifyCheckName(name: string): string {
  switch (name) {
    case "connection":
      return "Connection";
    case "server_version":
      return "Server Version";
    case "pgvector":
      return "pgvector Extension";
    case "privileges":
      return "Privileges";
    case "target_state":
      return "Target Database State";
    default:
      return name;
  }
}

function ResetOptions({
  targetCounts,
  pending,
  onReset,
}: {
  targetCounts: Record<string, number> | undefined;
  pending: boolean;
  onReset: (mode: ResetMode) => void;
}) {
  const total = targetCounts
    ? Object.values(targetCounts).reduce((a, b) => a + b, 0)
    : 0;
  return (
    <div className="rounded-md border border-amber-300 bg-amber-50 p-4 dark:border-amber-700 dark:bg-amber-900/30">
      <p className="text-sm font-medium text-amber-800 dark:text-amber-200">
        Leftover data detected
      </p>
      <p className="mt-1 text-sm text-amber-700 dark:text-amber-300">
        The target database already contains {total.toLocaleString()} nram rows.
        Migration will not insert duplicates but row-count validation will fail.
        Choose a reset strategy:
      </p>
      <div className="mt-3 flex flex-wrap gap-2">
        <button
          type="button"
          disabled={pending}
          onClick={() => onReset("truncate")}
          className="rounded-md bg-amber-600 px-3 py-1.5 text-xs font-medium text-white shadow-sm hover:bg-amber-700 disabled:opacity-50"
        >
          {pending ? "Resetting..." : "TRUNCATE (preserve schema)"}
        </button>
        <button
          type="button"
          disabled={pending}
          onClick={() => onReset("drop_schema")}
          className="rounded-md bg-red-600 px-3 py-1.5 text-xs font-medium text-white shadow-sm hover:bg-red-700 disabled:opacity-50"
        >
          {pending ? "Resetting..." : "DROP TABLES (rebuild schema)"}
        </button>
      </div>
      <p className="mt-2 text-xs text-amber-700 dark:text-amber-400">
        TRUNCATE is usually what you want — fast, keeps pgvector enabled, owner
        privileges are sufficient. DROP TABLES is heavier and requires the
        migration to recreate the schema on the next run.
      </p>
    </div>
  );
}

function OrphanAuditSummary({ audit }: { audit: MigrationAudit }) {
  if (audit.total_orphans === 0) {
    return (
      <div className="rounded-md bg-green-50 p-4 dark:bg-green-900/30">
        <p className="text-sm font-medium text-green-800 dark:text-green-200">
          No orphans found
        </p>
        <p className="mt-1 text-sm text-green-700 dark:text-green-300">
          Every FK in the source database points at a valid parent. Migration
          should complete without dropping any rows.
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-md border border-amber-300 bg-amber-50 p-4 dark:border-amber-700 dark:bg-amber-900/30">
      <p className="text-sm font-medium text-amber-800 dark:text-amber-200">
        {audit.total_orphans.toLocaleString()} orphan row
        {audit.total_orphans === 1 ? "" : "s"} will be dropped
      </p>
      <p className="mt-1 text-sm text-amber-700 dark:text-amber-300">
        These rows reference a parent (namespace, memory, entity, etc.) that no
        longer exists in SQLite. They would cause Postgres FK violations. The
        migrator will skip them and record each skip in the final report.
        Confirmed by you: drop policy = skip-on-read (SQLite is not modified).
      </p>
      <div className="mt-3 overflow-x-auto">
        <table className="min-w-full divide-y divide-amber-300 text-xs dark:divide-amber-700">
          <thead>
            <tr className="text-left text-amber-800 dark:text-amber-200">
              <th className="py-1 pr-4">Table.Column</th>
              <th className="py-1 pr-4">References</th>
              <th className="py-1 text-right">Count</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-amber-200 font-mono dark:divide-amber-800">
            {audit.orphans.map((o) => (
              <tr key={`${o.table}.${o.column}`}>
                <td className="py-1 pr-4">
                  {o.table}.{o.column}
                </td>
                <td className="py-1 pr-4 text-amber-700 dark:text-amber-400">
                  {o.references}
                </td>
                <td className="py-1 text-right font-bold">
                  {o.count.toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {audit.errors && audit.errors.length > 0 && (
        <p className="mt-2 text-xs text-amber-700 dark:text-amber-400">
          {audit.errors.length} FK relation
          {audit.errors.length === 1 ? " was" : "s were"} not audited (see
          server logs).
        </p>
      )}
    </div>
  );
}

function MigrationStatsPanel({ stats }: { stats: MigrationStats }) {
  const skippedEntries = Object.entries(stats.skipped_orphans ?? {}).sort(
    ([, a], [, b]) => b - a,
  );
  const updateEntries = Object.entries(stats.skipped_updates ?? {});
  const insertedEntries = Object.entries(stats.inserted ?? {}).sort(
    ([, a], [, b]) => b - a,
  );
  const resetEntries = Object.entries(stats.reset_stuck ?? {});
  if (
    !skippedEntries.length &&
    !updateEntries.length &&
    !insertedEntries.length &&
    !resetEntries.length
  ) {
    return null;
  }

  return (
    <div className="rounded-md border border-border bg-background p-4">
      <p className="text-sm font-medium text-foreground">Migration Report</p>
      {insertedEntries.length > 0 && (
        <details className="mt-2" open={false}>
          <summary className="cursor-pointer text-xs font-medium text-muted-foreground">
            Inserted rows by table ({insertedEntries.length} tables)
          </summary>
          <div className="mt-1 grid grid-cols-2 gap-1 sm:grid-cols-3 text-xs font-mono">
            {insertedEntries.map(([t, n]) => (
              <div key={t} className="flex justify-between gap-2">
                <span className="text-muted-foreground">{t}</span>
                <span className="text-foreground">{n.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </details>
      )}
      {skippedEntries.length > 0 && (
        <details className="mt-2" open>
          <summary className="cursor-pointer text-xs font-medium text-amber-700 dark:text-amber-400">
            Orphan rows dropped ({skippedEntries.length} FK relations)
          </summary>
          <div className="mt-1 grid grid-cols-1 gap-1 sm:grid-cols-2 text-xs font-mono">
            {skippedEntries.map(([k, n]) => (
              <div key={k} className="flex justify-between gap-2">
                <span className="text-muted-foreground">{k}</span>
                <span className="text-amber-700 dark:text-amber-400">
                  {n.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </details>
      )}
      {updateEntries.length > 0 && (
        <details className="mt-2">
          <summary className="cursor-pointer text-xs font-medium text-muted-foreground">
            Column updates skipped (self-ref orphans) ({updateEntries.length})
          </summary>
          <div className="mt-1 text-xs font-mono">
            {updateEntries.map(([k, n]) => (
              <div key={k} className="flex justify-between gap-2">
                <span className="text-muted-foreground">{k}</span>
                <span className="text-foreground">{n.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </details>
      )}
      {resetEntries.length > 0 && (
        <details className="mt-2" open>
          <summary className="cursor-pointer text-xs font-medium text-blue-700 dark:text-blue-400">
            In-flight jobs normalized ({resetEntries.length})
          </summary>
          <p className="mt-1 text-xs text-muted-foreground">
            Rows that were mid-flight on the SQLite instance were reset so the
            Postgres deployment doesn&apos;t wait on a worker that no longer
            exists. enrichment_queue rows returned to the queue;
            dream_cycles rows marked as failed.
          </p>
          <div className="mt-1 text-xs font-mono">
            {resetEntries.map(([k, n]) => (
              <div key={k} className="flex justify-between gap-2">
                <span className="text-muted-foreground">{k}</span>
                <span className="text-blue-700 dark:text-blue-400">
                  {n.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </details>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Postgres View
// ---------------------------------------------------------------------------

function PostgresView({
  host,
  database,
  version,
  pgvectorVersion,
  activeConns,
  idleConns,
  maxConns,
  dataCounts,
}: {
  host: string;
  database: string;
  version: string;
  pgvectorVersion: string;
  activeConns: number;
  idleConns: number;
  maxConns: number;
  dataCounts: {
    memories: number;
    entities: number;
    projects: number;
    users: number;
    organizations: number;
    vectors: number;
  };
}) {
  return (
    <div className="space-y-6">
      {/* Connection info card */}
      <div className="rounded-lg border border-border bg-card shadow-sm">
        <div className="border-b border-border px-5 py-4">
          <div className="flex items-center gap-3">
            <span className="inline-block h-3 w-3 rounded-full bg-green-500" />
            <h3 className="text-sm font-semibold text-foreground">
              PostgreSQL Backend
            </h3>
          </div>
        </div>
        <div className="px-5 py-4">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div>
              <p className="text-xs text-muted-foreground">Host</p>
              <p className="mt-1 font-mono text-sm text-foreground">{host}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Database</p>
              <p className="mt-1 font-mono text-sm text-foreground">
                {database}
              </p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Version</p>
              <p className="mt-1 text-sm font-medium text-foreground">
                {version}
              </p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">pgvector Version</p>
              <p className="mt-1 text-sm font-medium text-foreground">
                {pgvectorVersion || "Not installed"}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Connection pool stats */}
      <div className="rounded-lg border border-border bg-card shadow-sm">
        <div className="border-b border-border px-5 py-4">
          <h3 className="text-sm font-semibold text-foreground">
            Connection Pool
          </h3>
        </div>
        <div className="px-5 py-4 space-y-4">
          {/* Active connections */}
          <div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Active</span>
              <span className="font-medium text-foreground">
                {activeConns} / {maxConns}
              </span>
            </div>
            <div className="mt-1.5 h-2 overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-blue-500 transition-all duration-300"
                style={{ width: poolBarWidth(activeConns, maxConns) }}
              />
            </div>
          </div>

          {/* Idle connections */}
          <div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Idle</span>
              <span className="font-medium text-foreground">
                {idleConns} / {maxConns}
              </span>
            </div>
            <div className="mt-1.5 h-2 overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-green-500 transition-all duration-300"
                style={{ width: poolBarWidth(idleConns, maxConns) }}
              />
            </div>
          </div>

          {/* Max connections */}
          <div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Max Connections</span>
              <span className="font-medium text-foreground">{maxConns}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Data counts */}
      <DataCountsCard counts={dataCounts} />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

function DatabaseManagement() {
  const dbQuery = useDatabaseInfo();

  const isLoading = dbQuery.isLoading;
  const isError = dbQuery.isError;
  const data = dbQuery.data;

  return (
    <div>
      {/* Page header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">
          Database Management
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Database status, migrations, and maintenance.
        </p>
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <Spinner className="h-8 w-8 text-muted-foreground" />
        </div>
      )}

      {/* Error state */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-900/30">
          <p className="text-sm text-red-800 dark:text-red-300">
            Failed to load database information. Please try refreshing the page.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && data && (
        <>
          {data.backend === "sqlite" && data.sqlite ? (
            <SQLiteView
              filePath={data.sqlite.file_path}
              fileSizeBytes={data.sqlite.file_size_bytes}
              dataCounts={data.data_counts}
            />
          ) : data.backend === "postgres" && data.postgres ? (
            <PostgresView
              host={data.postgres.host}
              database={data.postgres.database}
              version={data.version}
              pgvectorVersion={data.postgres.pgvector_version || ""}
              activeConns={data.postgres.active_connections}
              idleConns={data.postgres.idle_connections}
              maxConns={data.postgres.max_connections}
              dataCounts={data.data_counts}
            />
          ) : (
            <div className="rounded-lg border border-border bg-card p-6 text-center">
              <p className="text-sm text-muted-foreground">
                Unknown database backend: {data.backend}
              </p>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default DatabaseManagement;
