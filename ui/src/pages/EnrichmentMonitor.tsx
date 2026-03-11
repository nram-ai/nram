import { useState, useCallback, useMemo } from "react";
import {
  useEnrichmentStatus,
  useRetryEnrichment,
  usePauseEnrichment,
} from "../hooks/useApi";
import type { EnrichmentQueueItem } from "../api/client";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const STATUS_BADGES: Record<string, string> = {
  pending:
    "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300",
  processing:
    "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300",
  completed:
    "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300",
  failed: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300",
};

type SortField = "status" | "attempts" | "created_at";
type SortDir = "asc" | "desc";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function relativeTime(timestamp: string): string {
  const now = Date.now();
  const then = new Date(timestamp).getTime();
  const diffSec = Math.floor((now - then) / 1000);
  if (diffSec < 0) return "just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin} min ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDay = Math.floor(diffHr / 24);
  if (diffDay === 1) return "yesterday";
  if (diffDay < 30) return `${diffDay}d ago`;
  return new Date(timestamp).toLocaleDateString();
}

function truncateId(id: string): string {
  if (id.length <= 12) return id;
  return id.slice(0, 8) + "...";
}

// ---------------------------------------------------------------------------
// Spinner
// ---------------------------------------------------------------------------

function Spinner({ className = "h-3.5 w-3.5" }: { className?: string }) {
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
// Stat Card
// ---------------------------------------------------------------------------

function StatCard({
  label,
  count,
  colorClass,
}: {
  label: string;
  count: number;
  colorClass: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-4 shadow-sm">
      <p className="text-sm font-medium text-muted-foreground">{label}</p>
      <p className={`mt-1 text-2xl font-bold ${colorClass}`}>{count}</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Expandable Error Cell
// ---------------------------------------------------------------------------

function ErrorCell({ error }: { error?: string }) {
  const [expanded, setExpanded] = useState(false);

  if (!error) {
    return (
      <span className="text-xs text-muted-foreground">&mdash;</span>
    );
  }

  const isLong = error.length > 60;

  return (
    <div className="max-w-xs">
      <p className={`text-xs text-red-600 dark:text-red-400 ${!expanded && isLong ? "line-clamp-2" : ""}`}>
        {error}
      </p>
      {isLong && (
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="mt-0.5 text-xs font-medium text-primary hover:underline"
        >
          {expanded ? "Show less" : "Show more"}
        </button>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sort Header
// ---------------------------------------------------------------------------

function SortHeader({
  label,
  field,
  currentField,
  currentDir,
  onSort,
}: {
  label: string;
  field: SortField;
  currentField: SortField;
  currentDir: SortDir;
  onSort: (field: SortField) => void;
}) {
  const isActive = currentField === field;

  return (
    <button
      type="button"
      onClick={() => onSort(field)}
      className="inline-flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground"
    >
      {label}
      {isActive && (
        <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          {currentDir === "asc" ? (
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
          ) : (
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          )}
        </svg>
      )}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Queue Table
// ---------------------------------------------------------------------------

function QueueTable({
  items,
  selectedIds,
  onToggleSelect,
  onToggleSelectAll,
  onRetryOne,
  retrying,
}: {
  items: EnrichmentQueueItem[];
  selectedIds: Set<string>;
  onToggleSelect: (id: string) => void;
  onToggleSelectAll: () => void;
  onRetryOne: (id: string) => void;
  retrying: boolean;
}) {
  const [sortField, setSortField] = useState<SortField>("created_at");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const handleSort = useCallback(
    (field: SortField) => {
      if (sortField === field) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
      } else {
        setSortField(field);
        setSortDir("desc");
      }
    },
    [sortField],
  );

  const sorted = useMemo(() => {
    const copy = [...items];
    copy.sort((a, b) => {
      let cmp = 0;
      switch (sortField) {
        case "status":
          cmp = a.status.localeCompare(b.status);
          break;
        case "attempts":
          cmp = a.attempts - b.attempts;
          break;
        case "created_at":
          cmp =
            new Date(a.created_at).getTime() -
            new Date(b.created_at).getTime();
          break;
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return copy;
  }, [items, sortField, sortDir]);

  const failedIds = useMemo(
    () => new Set(items.filter((i) => i.status === "failed").map((i) => i.id)),
    [items],
  );

  const allFailedSelected =
    failedIds.size > 0 &&
    [...failedIds].every((id) => selectedIds.has(id));

  if (items.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center rounded-lg border border-border bg-card py-12 shadow-sm">
        <svg
          className="h-12 w-12 text-muted-foreground/40"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={1.5}
            d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
          />
        </svg>
        <p className="mt-3 text-sm font-medium text-muted-foreground">
          No items in the enrichment queue
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          Queue items will appear here when memories are submitted for enrichment.
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-border bg-card shadow-sm">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-muted/50">
            <th className="px-3 py-2.5 text-left">
              <input
                type="checkbox"
                checked={allFailedSelected && failedIds.size > 0}
                onChange={onToggleSelectAll}
                disabled={failedIds.size === 0}
                className="h-4 w-4 rounded border-gray-300 text-primary focus:ring-primary disabled:opacity-30"
                title="Select all failed items"
              />
            </th>
            <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
              Memory ID
            </th>
            <th className="px-3 py-2.5 text-left">
              <SortHeader
                label="Status"
                field="status"
                currentField={sortField}
                currentDir={sortDir}
                onSort={handleSort}
              />
            </th>
            <th className="px-3 py-2.5 text-left">
              <SortHeader
                label="Attempts"
                field="attempts"
                currentField={sortField}
                currentDir={sortDir}
                onSort={handleSort}
              />
            </th>
            <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
              Last Error
            </th>
            <th className="px-3 py-2.5 text-left">
              <SortHeader
                label="Queued"
                field="created_at"
                currentField={sortField}
                currentDir={sortDir}
                onSort={handleSort}
              />
            </th>
            <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
              Actions
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {sorted.map((item) => {
            const isFailed = item.status === "failed";
            const badgeCls =
              STATUS_BADGES[item.status] || STATUS_BADGES.pending;

            return (
              <tr
                key={item.id}
                className="hover:bg-muted/30 transition-colors"
              >
                <td className="px-3 py-2.5">
                  <input
                    type="checkbox"
                    checked={selectedIds.has(item.id)}
                    onChange={() => onToggleSelect(item.id)}
                    disabled={!isFailed}
                    className="h-4 w-4 rounded border-gray-300 text-primary focus:ring-primary disabled:opacity-30"
                  />
                </td>
                <td className="px-3 py-2.5">
                  <span
                    className="font-mono text-xs text-foreground"
                    title={item.memory_id}
                  >
                    {truncateId(item.memory_id)}
                  </span>
                </td>
                <td className="px-3 py-2.5">
                  <span
                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${badgeCls}`}
                  >
                    {item.status}
                  </span>
                </td>
                <td className="px-3 py-2.5 text-xs text-foreground">
                  {item.attempts}
                </td>
                <td className="px-3 py-2.5">
                  <ErrorCell error={item.last_error} />
                </td>
                <td className="px-3 py-2.5 text-xs text-muted-foreground">
                  <span title={new Date(item.created_at).toLocaleString()}>
                    {relativeTime(item.created_at)}
                  </span>
                </td>
                <td className="px-3 py-2.5">
                  {isFailed && (
                    <button
                      type="button"
                      onClick={() => onRetryOne(item.id)}
                      disabled={retrying}
                      className="rounded-md border border-input px-2 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      Retry
                    </button>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

function EnrichmentMonitor() {
  const statusQuery = useEnrichmentStatus();
  const retryMutation = useRetryEnrichment();
  const pauseMutation = usePauseEnrichment();

  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  const data = statusQuery.data;
  const counts = data?.counts ?? {
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
  };
  const items = data?.items ?? [];
  const isPaused = data?.paused ?? false;

  const failedItems = useMemo(
    () => items.filter((i) => i.status === "failed"),
    [items],
  );

  const handleToggleSelect = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }, []);

  const handleToggleSelectAll = useCallback(() => {
    setSelectedIds((prev) => {
      const failedIds = failedItems.map((i) => i.id);
      const allSelected = failedIds.every((id) => prev.has(id));
      if (allSelected) {
        return new Set();
      }
      return new Set(failedIds);
    });
  }, [failedItems]);

  const handleRetryOne = useCallback(
    (id: string) => {
      retryMutation.mutate([id], {
        onSuccess: () => {
          setSelectedIds((prev) => {
            const next = new Set(prev);
            next.delete(id);
            return next;
          });
        },
      });
    },
    [retryMutation],
  );

  const handleRetrySelected = useCallback(() => {
    const ids = [...selectedIds];
    if (ids.length === 0) return;
    retryMutation.mutate(ids, {
      onSuccess: () => {
        setSelectedIds(new Set());
      },
    });
  }, [selectedIds, retryMutation]);

  const handleRetryAllFailed = useCallback(() => {
    retryMutation.mutate(undefined, {
      onSuccess: () => {
        setSelectedIds(new Set());
      },
    });
  }, [retryMutation]);

  const handleTogglePause = useCallback(() => {
    pauseMutation.mutate(!isPaused);
  }, [isPaused, pauseMutation]);

  return (
    <div>
      {/* Page header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">
          Enrichment Queue
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Monitor the enrichment processing queue and manage worker state.
        </p>
      </div>

      {/* Loading state */}
      {statusQuery.isLoading && (
        <div className="flex items-center justify-center py-16">
          <Spinner className="h-8 w-8 text-muted-foreground" />
        </div>
      )}

      {/* Error state */}
      {statusQuery.isError && !statusQuery.isLoading && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-900/30">
          <p className="text-sm text-red-800 dark:text-red-300">
            Failed to load enrichment status. Please try refreshing the page.
          </p>
        </div>
      )}

      {/* Content */}
      {!statusQuery.isLoading && !statusQuery.isError && (
        <div className="space-y-6">
          {/* Status cards */}
          <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
            <StatCard
              label="Pending"
              count={counts.pending}
              colorClass="text-yellow-600 dark:text-yellow-400"
            />
            <StatCard
              label="Processing"
              count={counts.processing}
              colorClass="text-blue-600 dark:text-blue-400"
            />
            <StatCard
              label="Completed"
              count={counts.completed}
              colorClass="text-green-600 dark:text-green-400"
            />
            <StatCard
              label="Failed"
              count={counts.failed}
              colorClass="text-red-600 dark:text-red-400"
            />
          </div>

          {/* Controls bar */}
          <div className="flex flex-wrap items-center gap-3">
            {/* Pause/Resume button */}
            <button
              type="button"
              onClick={handleTogglePause}
              disabled={pauseMutation.isPending}
              className={`inline-flex items-center gap-2 rounded-md px-4 py-2 text-sm font-medium shadow-sm disabled:opacity-50 disabled:cursor-not-allowed ${
                isPaused
                  ? "bg-green-600 text-white hover:bg-green-700"
                  : "bg-yellow-600 text-white hover:bg-yellow-700"
              }`}
            >
              {pauseMutation.isPending ? (
                <Spinner />
              ) : isPaused ? (
                <svg
                  className="h-4 w-4"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"
                  />
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
              ) : (
                <svg
                  className="h-4 w-4"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
              )}
              {isPaused ? "Resume Workers" : "Pause Workers"}
            </button>

            {/* Paused indicator */}
            {isPaused && (
              <span className="inline-flex items-center gap-1.5 rounded-full bg-yellow-100 px-3 py-1 text-xs font-medium text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300">
                <span className="h-2 w-2 rounded-full bg-yellow-500 animate-pulse" />
                Workers paused
              </span>
            )}

            <div className="flex-1" />

            {/* Bulk actions */}
            {selectedIds.size > 0 && (
              <button
                type="button"
                onClick={handleRetrySelected}
                disabled={retryMutation.isPending}
                className="inline-flex items-center gap-1.5 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {retryMutation.isPending && <Spinner />}
                Retry Selected ({selectedIds.size})
              </button>
            )}

            {failedItems.length > 0 && (
              <button
                type="button"
                onClick={handleRetryAllFailed}
                disabled={retryMutation.isPending}
                className="inline-flex items-center gap-1.5 rounded-md border border-red-300 px-3 py-2 text-sm font-medium text-red-700 shadow-sm hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {retryMutation.isPending && <Spinner />}
                Retry All Failed ({failedItems.length})
              </button>
            )}
          </div>

          {/* Mutation feedback */}
          {retryMutation.isSuccess && (
            <div className="flex items-center gap-2 rounded-md bg-green-50 px-3 py-2 text-sm text-green-800 dark:bg-green-900/30 dark:text-green-300">
              <svg
                className="h-4 w-4 flex-shrink-0"
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
              {retryMutation.data.retried} item(s) queued for retry.
            </div>
          )}

          {retryMutation.isError && (
            <div className="flex items-center gap-2 rounded-md bg-red-50 px-3 py-2 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-300">
              <svg
                className="h-4 w-4 flex-shrink-0"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M6 18L18 6M6 6l12 12"
                />
              </svg>
              Failed to retry: {(retryMutation.error as Error).message}
            </div>
          )}

          {pauseMutation.isError && (
            <div className="flex items-center gap-2 rounded-md bg-red-50 px-3 py-2 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-300">
              <svg
                className="h-4 w-4 flex-shrink-0"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M6 18L18 6M6 6l12 12"
                />
              </svg>
              Failed to update pause state:{" "}
              {(pauseMutation.error as Error).message}
            </div>
          )}

          {/* Queue table */}
          <div>
            <h2 className="mb-3 text-lg font-medium text-foreground">
              Queue Items
            </h2>
            <QueueTable
              items={items}
              selectedIds={selectedIds}
              onToggleSelect={handleToggleSelect}
              onToggleSelectAll={handleToggleSelectAll}
              onRetryOne={handleRetryOne}
              retrying={retryMutation.isPending}
            />
          </div>

          {/* Auto-refresh indicator */}
          <p className="text-xs text-muted-foreground">
            Auto-refreshing every 10 seconds.
            {statusQuery.isFetching && !statusQuery.isLoading && (
              <span className="ml-2 inline-flex items-center gap-1">
                <Spinner className="h-3 w-3" />
                Updating...
              </span>
            )}
          </p>
        </div>
      )}
    </div>
  );
}

export default EnrichmentMonitor;
