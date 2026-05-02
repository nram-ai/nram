import { useState, useCallback, useMemo } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  useEnrichmentStatus,
  useRetryEnrichment,
  usePauseEnrichment,
} from "../hooks/useApi";
import { useEventStream } from "../hooks/useEventStream";
import {
  useElapsedTicker,
  elapsedSeconds,
  formatElapsed,
} from "../hooks/useElapsedTicker";
import { useAuth } from "../context/AuthContext";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faSpinner,
  faChevronUp,
  faChevronDown,
  faFolderOpen,
  faCirclePlay,
  faCirclePause,
  faCheck,
  faXmark,
} from "../lib/icons";
import type { EnrichmentQueueItem } from "../api/client";

// Tier picker — administrators can switch between their own queue items
// (the "Mine" tab, /v1/me/enrichment) and the cross-tenant pipeline view
// (the "System" tab, /v1/admin/enrichment). Non-admin callers see only
// "Mine" with no UI affordance for system access.
type EnrichmentTier = "self" | "system";

// Live SSE state for the enrichment worker pool. liveJobs is keyed by
// queue job id (the EnrichmentQueueItem.id, identical to the worker's
// EnrichmentJob.ID and the job_id field in events). poolTick mirrors the
// pool ticker payload so the banner can compute oldest-claim age live.
type LiveJob = {
  jobId: string;
  stage: string;
  startedAt: string;
};

type PoolTick = {
  inFlight: number;
  oldestClaimAt?: string;
  oldestClaimAgeMs?: number;
  paused: boolean;
  byStage: Record<string, number>;
  receivedAt: number;
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const STATUS_BADGES: Record<string, string> = {
  pending:
    "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300",
  processing:
    "bg-info/10 text-info",
  completed:
    "bg-success/10 text-success",
  failed: "bg-destructive/10 text-destructive",
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
  return <FontAwesomeIcon icon={faSpinner} spin className={className} />;
}

// ---------------------------------------------------------------------------
// Live SSE state
// ---------------------------------------------------------------------------

function useEnrichmentLiveState() {
  const qc = useQueryClient();
  const [liveJobs, setLiveJobs] = useState<Record<string, LiveJob>>({});
  const [poolTick, setPoolTick] = useState<PoolTick | null>(null);

  const { connected } = useEventStream({
    scope: "",
    onEvent: (evt) => {
      const data = (evt.data ?? {}) as Record<string, any>;
      switch (evt.type) {
        case "enrichment.job.started": {
          if (!data.job_id) return;
          setLiveJobs((prev) => ({
            ...prev,
            [data.job_id]: {
              jobId: data.job_id,
              stage: data.stage ?? "started",
              startedAt: data.started_at ?? new Date().toISOString(),
            },
          }));
          break;
        }
        case "enrichment.job.completed": {
          if (!data.job_id) return;
          setLiveJobs((prev) => {
            const next = { ...prev };
            delete next[data.job_id];
            return next;
          });
          // Authoritative refresh — the row's status flipped to
          // completed/failed, the queue endpoint will reflect it on the
          // next poll, but we invalidate so the UI updates immediately.
          // Refresh both tier caches; the SSE hook is shared between
          // admin (system) and self viewers.
          qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
          qc.invalidateQueries({ queryKey: ["me", "enrichment"] });
          break;
        }
        case "enrichment.pool.tick": {
          setPoolTick({
            inFlight: data.in_flight ?? 0,
            oldestClaimAt: data.oldest_claim_at,
            oldestClaimAgeMs: data.oldest_claim_age_ms,
            paused: !!data.paused,
            byStage: (data.by_stage ?? {}) as Record<string, number>,
            receivedAt: Date.now(),
          });
          break;
        }
        case "enrichment.job.requeued": {
          // Sweeper auto-requeued a stuck job. Drop any live-job state for
          // it (the original claim is gone) and refresh the queue cache so
          // the row's status flips to 'pending' and the RequeuedPill renders.
          if (data.job_id) {
            setLiveJobs((prev) => {
              const next = { ...prev };
              delete next[data.job_id];
              return next;
            });
          }
          qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
          qc.invalidateQueries({ queryKey: ["me", "enrichment"] });
          break;
        }
        default:
          break;
      }
    },
  });
  return { liveJobs, poolTick, connected };
}

const STAGE_LABELS: Record<string, string> = {
  started: "starting",
  pre_embed: "extracting",
  embed: "embedding",
  finalize: "finalizing",
};

function StageChip({ stage }: { stage: string }) {
  return (
    <span className="inline-flex items-center rounded-full bg-info/10 px-2 py-0.5 text-[10px] font-medium text-info">
      {STAGE_LABELS[stage] ?? stage}
    </span>
  );
}

// Row-level alert state: at most one of stale/requeued applies per row, so a
// discriminated union beats two parallel booleans plus a tint string. Keeping
// the threshold check on the server (item.is_stale_diagnostic) avoids
// shipping enrichment.stuck_threshold_seconds to the UI on every poll.
type RowAlert =
  | { kind: "stale"; ageMs: number }
  | { kind: "requeued"; reason: string }
  | null;

const ROW_TINTS: Record<NonNullable<RowAlert>["kind"], string> = {
  stale: "bg-warning/40",
  requeued: "bg-destructive/40",
};

function rowAlert(item: EnrichmentQueueItem): RowAlert {
  if (item.last_requeue_reason && item.status === "pending") {
    return { kind: "requeued", reason: item.last_requeue_reason };
  }
  if (
    item.status === "processing" &&
    item.is_stale_diagnostic &&
    item.claimed_at_age_ms != null
  ) {
    return { kind: "stale", ageMs: item.claimed_at_age_ms };
  }
  return null;
}

function StaleDiagnosticPill({
  claimedAtAgeMs,
  claimedBy,
}: {
  claimedAtAgeMs: number;
  claimedBy?: string;
}) {
  const secs = Math.max(0, Math.floor(claimedAtAgeMs / 1000));
  const title = claimedBy
    ? `Worker ${claimedBy} has held this row for ${formatElapsed(secs)} without finishing. The stuck-job sweeper will auto-requeue it once it crosses the staleness threshold.`
    : `This row has been claimed for ${formatElapsed(secs)} without finishing. The stuck-job sweeper will auto-requeue it.`;
  return (
    <span
      className="inline-flex items-center rounded-full bg-warning/20 px-2 py-0.5 text-[10px] font-semibold text-warning"
      title={title}
    >
      stale {formatElapsed(secs)}
    </span>
  );
}

function RequeuedPill({ reason }: { reason: string }) {
  return (
    <span
      className="inline-flex items-center rounded-full bg-destructive/20 px-2 py-0.5 text-[10px] font-semibold text-destructive"
      title={reason}
    >
      requeued
    </span>
  );
}

function NoProgressChip({ secs }: { secs: number }) {
  if (secs <= 60) return null;
  const cls =
    secs > 300
      ? "bg-destructive/10 text-destructive"
      : "bg-warning/10 text-warning";
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold ${cls}`}
      title="No progress reported recently"
    >
      no progress {formatElapsed(secs)}
    </span>
  );
}

function EnrichmentPoolBanner({
  tick,
  liveJobs,
  fallbackInFlightCount,
}: {
  tick: PoolTick | null;
  liveJobs: Record<string, LiveJob>;
  fallbackInFlightCount: number;
}) {
  const live = Object.values(liveJobs);
  const inFlight = tick?.inFlight ?? live.length;
  const visible = inFlight > 0 || fallbackInFlightCount > 0;
  // Tick once a second only while the banner is visible — avoids a 1Hz
  // wake on the page when the pool is idle.
  useElapsedTicker(visible);
  if (!visible) return null;

  const tickStale = tick ? Date.now() - tick.receivedAt > 12_000 : true;
  const oldestIso =
    tick?.oldestClaimAt ??
    (live.length > 0
      ? live.reduce(
          (acc, j) => (acc && acc < j.startedAt ? acc : j.startedAt),
          live[0].startedAt,
        )
      : undefined);
  const oldestSecs = elapsedSeconds(oldestIso);

  let oldestCls = "text-emerald-700 dark:text-emerald-300";
  if (oldestSecs > 300) oldestCls = "text-destructive";
  else if (oldestSecs > 60) oldestCls = "text-warning";

  const stages = tick?.byStage ?? {};

  return (
    <div className="rounded-lg border border-dashed border-info/40 bg-info/50 p-4">
      <div className="flex flex-wrap items-center gap-3 text-xs">
        <span className="font-medium text-info">
          Worker pool active
        </span>
        <span className="font-mono">
          {inFlight} in flight
        </span>
        {Object.keys(stages).length > 0 && (
          <span className="text-muted-foreground">
            {Object.entries(stages)
              .map(([k, v]) => `${STAGE_LABELS[k] ?? k}: ${v}`)
              .join(" · ")}
          </span>
        )}
        {oldestIso && (
          <span className={`font-medium ${oldestCls}`}>
            oldest claim: {formatElapsed(oldestSecs)}
          </span>
        )}
        {tick?.paused && (
          <span className="inline-flex items-center gap-1 rounded-full bg-yellow-100 px-2 py-0.5 text-[10px] font-medium text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300">
            <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-yellow-500" />
            paused
          </span>
        )}
        {tickStale && (
          <span className="text-muted-foreground">(tick stale — using polled fallback)</span>
        )}
      </div>
    </div>
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
      <p className={`text-xs text-destructive ${!expanded && isLong ? "line-clamp-2" : ""}`}>
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
        <FontAwesomeIcon icon={currentDir === "asc" ? faChevronUp : faChevronDown} className="h-3 w-3" />
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
  liveJobs,
  showWriteActions = true,
}: {
  items: EnrichmentQueueItem[];
  selectedIds: Set<string>;
  onToggleSelect: (id: string) => void;
  onToggleSelectAll: () => void;
  onRetryOne: (id: string) => void;
  retrying: boolean;
  liveJobs: Record<string, LiveJob>;
  showWriteActions?: boolean;
}) {
  // Re-render every second so processing-row Elapsed counters tick.
  const hasProcessing = items.some((i) => i.status === "processing");
  useElapsedTicker(hasProcessing);
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
        <FontAwesomeIcon icon={faFolderOpen} className="h-12 w-12 text-muted-foreground/40" />
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
            {showWriteActions && (
              <th className="px-3 py-2.5 text-left">
                <input
                  type="checkbox"
                  checked={allFailedSelected && failedIds.size > 0}
                  onChange={onToggleSelectAll}
                  disabled={failedIds.size === 0}
                  className="h-4 w-4 rounded border-input text-primary focus:ring-primary disabled:opacity-30"
                  title="Select all failed items"
                />
              </th>
            )}
            <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
              Memory ID
            </th>
            <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
              Project
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
            const alert = rowAlert(item);
            const rowTint = alert ? ROW_TINTS[alert.kind] : "";

            return (
              <tr
                key={item.id}
                className={`hover:bg-muted/30 transition-colors ${rowTint}`}
              >
                {showWriteActions && (
                  <td className="px-3 py-2.5">
                    <input
                      type="checkbox"
                      checked={selectedIds.has(item.id)}
                      onChange={() => onToggleSelect(item.id)}
                      disabled={!isFailed}
                      className="h-4 w-4 rounded border-input text-primary focus:ring-primary disabled:opacity-30"
                    />
                  </td>
                )}
                <td className="px-3 py-2.5">
                  <span
                    className="font-mono text-xs text-foreground"
                    title={item.memory_id}
                  >
                    {truncateId(item.memory_id)}
                  </span>
                </td>
                <td
                  className="px-3 py-2.5 text-xs text-muted-foreground"
                  title={item.project_id ?? ""}
                >
                  {item.project_name
                    ? item.project_name
                    : item.project_id
                      ? truncateId(item.project_id)
                      : "—"}
                </td>
                <td className="px-3 py-2.5">
                  <div className="flex flex-wrap items-center gap-1.5">
                    <span
                      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${badgeCls}`}
                    >
                      {item.status}
                    </span>
                    {alert?.kind === "stale" && (
                      <StaleDiagnosticPill
                        claimedAtAgeMs={alert.ageMs}
                        claimedBy={item.claimed_by}
                      />
                    )}
                    {alert?.kind === "requeued" && (
                      <RequeuedPill reason={alert.reason} />
                    )}
                    {item.status === "processing" &&
                      (() => {
                        const lj = liveJobs[item.id];
                        // Anchor the elapsed timer to the current claim, not
                        // to original queue insertion. Retry/RequeueStale
                        // null out claimed_at on the backend, so item.claimed_at
                        // reflects only the active attempt — that's what the
                        // user thinks of as "how long has this been running."
                        const startedIso =
                          lj?.startedAt ?? item.claimed_at ?? item.created_at;
                        const secs = elapsedSeconds(startedIso);
                        return (
                          <>
                            <span className="font-mono text-[11px] text-muted-foreground">
                              {formatElapsed(secs)}
                            </span>
                            {lj?.stage && <StageChip stage={lj.stage} />}
                            <NoProgressChip secs={secs} />
                          </>
                        );
                      })()}
                  </div>
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
                {showWriteActions && (
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
                )}
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
  const { liveJobs, poolTick, connected } = useEnrichmentLiveState();
  const statusIntervalMs = connected ? 10_000 : 3_000;
  const { isAdmin } = useAuth();

  // Default to self-tier for everyone. Non-admin users have no system
  // option; admin can switch via the tab picker.
  const [tier, setTier] = useState<EnrichmentTier>("self");

  const statusQuery = useEnrichmentStatus({
    intervalMs: statusIntervalMs,
    tier,
  });
  const retryMutation = useRetryEnrichment();
  const pauseMutation = usePauseEnrichment();
  const showWriteActions = tier === "system" && isAdmin;

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
          {tier === "system" ? "Enrichment Queue" : "My Enrichment Queue"}
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {tier === "system"
            ? "Monitor the enrichment processing queue and manage worker state."
            : "Read-only view of your own enrichment queue items. Worker controls are administrator-only."}
        </p>
      </div>

      {/* Tier picker — admin only. Non-admin users have no system view. */}
      {isAdmin && (
        <div className="mb-6 border-b">
          <nav className="-mb-px flex gap-6" role="tablist" aria-label="Enrichment scope">
            <button
              role="tab"
              aria-selected={tier === "self"}
              onClick={() => setTier("self")}
              className={`border-b-2 px-1 py-3 text-sm font-medium ${
                tier === "self"
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              }`}
            >
              Mine
            </button>
            <button
              role="tab"
              aria-selected={tier === "system"}
              onClick={() => setTier("system")}
              className={`border-b-2 px-1 py-3 text-sm font-medium ${
                tier === "system"
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              }`}
            >
              System
            </button>
          </nav>
        </div>
      )}

      {/* Loading state */}
      {statusQuery.isLoading && (
        <div className="flex items-center justify-center py-16">
          <Spinner className="h-8 w-8 text-muted-foreground" />
        </div>
      )}

      {/* Error state */}
      {statusQuery.isError && !statusQuery.isLoading && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
          <p className="text-sm text-destructive">
            Failed to load enrichment status. Please try refreshing the page.
          </p>
        </div>
      )}

      {/* Content */}
      {!statusQuery.isLoading && !statusQuery.isError && (
        <div className="space-y-6">
          {/* Live pool banner */}
          <EnrichmentPoolBanner
            tick={poolTick}
            liveJobs={liveJobs}
            fallbackInFlightCount={counts.processing}
          />

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
              colorClass="text-info"
            />
            <StatCard
              label="Completed"
              count={counts.completed}
              colorClass="text-success"
            />
            <StatCard
              label="Failed"
              count={counts.failed}
              colorClass="text-destructive"
            />
          </div>

          {/* Controls bar — write actions (pause/retry) only on the system */}
          {/* tier for administrators. Self tier is strictly read-only. */}
          {showWriteActions && (
          <div className="flex flex-wrap items-center gap-3">
            {/* Pause/Resume button */}
            <button
              type="button"
              onClick={handleTogglePause}
              disabled={pauseMutation.isPending}
              className={`inline-flex items-center gap-2 rounded-md px-4 py-2 text-sm font-medium shadow-sm disabled:opacity-50 disabled:cursor-not-allowed ${
                isPaused
                  ? "bg-success text-white hover:bg-success"
                  : "bg-yellow-600 text-white hover:bg-yellow-700"
              }`}
            >
              {pauseMutation.isPending ? (
                <Spinner />
              ) : isPaused ? (
                <FontAwesomeIcon icon={faCirclePlay} className="h-4 w-4" />
              ) : (
                <FontAwesomeIcon icon={faCirclePause} className="h-4 w-4" />
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
                className="inline-flex items-center gap-1.5 rounded-md border border-destructive/40 px-3 py-2 text-sm font-medium text-destructive shadow-sm hover:bg-destructive/10 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {retryMutation.isPending && <Spinner />}
                Retry All Failed ({failedItems.length})
              </button>
            )}
          </div>
          )}

          {/* Mutation feedback */}
          {retryMutation.isSuccess && (
            <div className="flex items-center gap-2 rounded-md bg-success/10 px-3 py-2 text-sm text-success">
              <FontAwesomeIcon icon={faCheck} className="h-4 w-4 flex-shrink-0" />
              {retryMutation.data.retried} item(s) queued for retry.
            </div>
          )}

          {retryMutation.isError && (
            <div className="flex items-center gap-2 rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
              <FontAwesomeIcon icon={faXmark} className="h-4 w-4 flex-shrink-0" />
              Failed to retry: {(retryMutation.error as Error).message}
            </div>
          )}

          {pauseMutation.isError && (
            <div className="flex items-center gap-2 rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
              <FontAwesomeIcon icon={faXmark} className="h-4 w-4 flex-shrink-0" />
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
              liveJobs={liveJobs}
              showWriteActions={showWriteActions}
            />
          </div>

          {/* Auto-refresh indicator */}
          <p className="text-xs text-muted-foreground">
            {connected
              ? "Live updates connected"
              : `Polling every ${statusIntervalMs / 1000}s`}
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
