import { useState, useCallback, useMemo, useEffect } from "react";
import {
  useDatabaseInfo,
  useTestDatabaseConnection,
  useTriggerMigration,
} from "../hooks/useApi";
import type { ConnectionTestResult } from "../api/client";

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
  };
}) {
  const items = [
    { label: "Memories", value: counts.memories },
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
      <div className="grid grid-cols-2 gap-4 px-5 py-4 sm:grid-cols-3 lg:grid-cols-5">
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

type MigrationStep = "input" | "review" | "migrating" | "complete";

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
  };
}) {
  const [step, setStep] = useState<MigrationStep>("input");
  const [dbUrl, setDbUrl] = useState("");
  const [testResult, setTestResult] = useState<ConnectionTestResult | null>(
    null,
  );
  const [migrationError, setMigrationError] = useState<string | null>(null);

  const testMutation = useTestDatabaseConnection();
  const migrateMutation = useTriggerMigration();

  const handleTestConnection = useCallback(() => {
    setTestResult(null);
    testMutation.mutate(dbUrl, {
      onSuccess: (result) => {
        setTestResult(result);
        if (result.success) {
          setStep("review");
        }
      },
      onError: () => {
        setTestResult({
          success: false,
          message: "Failed to test connection. Check the URL and try again.",
          pgvector_installed: false,
          latency_ms: 0,
        });
      },
    });
  }, [dbUrl, testMutation]);

  const handleStartMigration = useCallback(() => {
    setMigrationError(null);
    setStep("migrating");
    migrateMutation.mutate(dbUrl, {
      onSuccess: (data) => {
        if (data.status === "complete") {
          setStep("complete");
        } else if (data.status === "in_progress") {
          // Server is processing asynchronously — stay on the migrating step.
        } else {
          setMigrationError(data.message || "Migration failed");
          setStep("review");
        }
      },
      onError: (error) => {
        setMigrationError(
          error instanceof Error ? error.message : "Migration failed",
        );
        setStep("review");
      },
    });
  }, [dbUrl, migrateMutation]);

  const handleReset = useCallback(() => {
    setStep("input");
    setDbUrl("");
    setTestResult(null);
    setMigrationError(null);
  }, []);

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
          <div className="mb-6 flex items-center gap-2">
            {(
              [
                { key: "input", label: "1. Connect" },
                { key: "review", label: "2. Review" },
                { key: "migrating", label: "3. Migrate" },
              ] as const
            ).map((s, i) => {
              const isActive =
                step === s.key ||
                (step === "complete" && s.key === "migrating");
              const isPast =
                (s.key === "input" && step !== "input") ||
                (s.key === "review" &&
                  (step === "migrating" || step === "complete"));
              return (
                <div key={s.key} className="flex items-center gap-2">
                  {i > 0 && (
                    <div
                      className={`h-px w-8 ${isPast || isActive ? "bg-primary" : "bg-border"}`}
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
                    setTestResult(null);
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
                onClick={handleTestConnection}
                disabled={!dbUrl.trim() || testMutation.isPending}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {testMutation.isPending ? (
                  <span className="flex items-center gap-1.5">
                    <Spinner className="h-3.5 w-3.5" />
                    Testing...
                  </span>
                ) : (
                  "Test Connection"
                )}
              </button>

              {/* Test result */}
              {testResult && (
                <div
                  className={`flex items-start gap-2 rounded-md px-3 py-2 text-sm ${
                    testResult.success
                      ? "bg-green-50 text-green-800 dark:bg-green-900/30 dark:text-green-300"
                      : "bg-red-50 text-red-800 dark:bg-red-900/30 dark:text-red-300"
                  }`}
                >
                  <svg
                    className="mt-0.5 h-4 w-4 flex-shrink-0"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    {testResult.success ? (
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M5 13l4 4L19 7"
                      />
                    ) : (
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M6 18L18 6M6 6l12 12"
                      />
                    )}
                  </svg>
                  <div>
                    <p>{testResult.message}</p>
                    {testResult.success && (
                      <p className="mt-1 text-xs">
                        Latency: {testResult.latency_ms}ms | pgvector:{" "}
                        {testResult.pgvector_installed
                          ? "installed"
                          : "not found"}
                      </p>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Step 2: Review */}
          {step === "review" && (
            <div className="space-y-4">
              <div className="rounded-md bg-muted/50 p-4">
                <p className="text-sm font-medium text-foreground">
                  Connection Verified
                </p>
                <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                  {dbUrl}
                </p>
                {testResult && (
                  <p className="mt-1 text-xs text-muted-foreground">
                    Latency: {testResult.latency_ms}ms | pgvector:{" "}
                    {testResult.pgvector_installed ? "installed" : "not found"}
                  </p>
                )}
              </div>

              <div>
                <p className="mb-2 text-sm font-medium text-foreground">
                  Data to Migrate
                </p>
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
                  {[
                    {
                      label: "Memories",
                      count: dataCounts.memories,
                    },
                    {
                      label: "Entities",
                      count: dataCounts.entities,
                    },
                    {
                      label: "Projects",
                      count: dataCounts.projects,
                    },
                    { label: "Users", count: dataCounts.users },
                    {
                      label: "Organizations",
                      count: dataCounts.organizations,
                    },
                  ].map((item) => (
                    <div
                      key={item.label}
                      className="rounded-md border border-border bg-background p-3 text-center"
                    >
                      <p className="text-lg font-bold text-foreground">
                        {item.count.toLocaleString()}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {item.label}
                      </p>
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
                  Confirm Migration
                </button>
                <button
                  type="button"
                  onClick={handleReset}
                  className="rounded-md border border-input px-4 py-2 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
                >
                  Back
                </button>
              </div>
            </div>
          )}

          {/* Step 3: Migrating */}
          {step === "migrating" && (
            <MigratingIndicator />
          )}

          {/* Step 4: Complete */}
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
