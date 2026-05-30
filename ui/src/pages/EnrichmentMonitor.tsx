import {
  Fragment,
  useState,
  useCallback,
  useMemo,
  useRef,
  useEffect,
} from "react";
import { Link } from "react-router-dom";
import {
  useEnrichmentStatusInfinite,
  useRetryEnrichment,
  usePauseEnrichment,
  enrichmentTotalForFilter,
} from "../hooks/useApi";
import { useEventStream } from "../hooks/useEventStream";
import {
  useElapsedTicker,
  elapsedSeconds,
  formatElapsed,
} from "../hooks/useElapsedTicker";
import { useAuth, type Tier } from "../context/AuthContext";
import { TierTabs } from "../components/TierTabs";
import { ExtractionErrorView } from "../lib/extractionError";
import { MemoryAugmentPreviewBlock } from "../components/MemoryAugmentPreviewBlock";
import { StatusNode } from "../components/StatusNode/StatusNode";
import { firePulse } from "../components/NeuralNetwork/networkBus";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faSpinner,
  faChevronUp,
  faChevronDown,
  faChevronRight,
  faFolderOpen,
  faCirclePlay,
  faCirclePause,
  faCheck,
  faXmark,
} from "../lib/icons";
import type {
  EnrichmentQueueItem,
  EnrichmentQueueCounts,
  EnrichmentStatusFilter,
} from "../api/client";
import { truncateId } from "../lib/formatters";
import { memoryFocusHref } from "../lib/dreaming";

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
  const [liveJobs, setLiveJobs] = useState<Record<string, LiveJob>>({});
  const [poolTick, setPoolTick] = useState<PoolTick | null>(null);

  // The table is intentionally NOT invalidated on per-job SSE events. During
  // active draining those fire many times per second; invalidating the queue
  // query on each one refetched and reordered the whole table constantly,
  // which is what made the list jump. Instead the live pool banner + per-row
  // liveJobs overlay reflect activity in real time (updating in place, no
  // reordering), and the table itself refreshes on its poll interval with
  // placeholderData keeping the prior page steady between fetches.
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
          // Row status flips on the next poll (see note above); no per-event
          // table invalidation.
          break;
        }
        case "enrichment.pool.tick": {
          const inFlight = data.in_flight ?? 0;
          setPoolTick({
            inFlight,
            oldestClaimAt: data.oldest_claim_at,
            oldestClaimAgeMs: data.oldest_claim_age_ms,
            paused: !!data.paused,
            byStage: (data.by_stage ?? {}) as Record<string, number>,
            receivedAt: Date.now(),
          });
          // Push a signal through the neural-network backdrop when work is
          // happening so the visual reflects system activity.
          if (inFlight > 0 && !data.paused) {
            firePulse(0, 1);
          }
          break;
        }
        case "enrichment.job.requeued": {
          // Sweeper auto-requeued a stuck job. Drop any live-job state for
          // it (the original claim is gone); the row's status flips to
          // 'pending' and the RequeuedPill renders on the next poll.
          if (data.job_id) {
            setLiveJobs((prev) => {
              const next = { ...prev };
              delete next[data.job_id];
              return next;
            });
          }
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
          <StatusNode kind="paused" label="paused" />
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
// Expandable Error Cell — delegates to the shared ExtractionErrorView so
// failed-job and partial-recovery JSON envelopes render structured headlines
// with click-to-expand diagnostics, while plain-string errors fall through
// to a line-clamped destructive cell.
// ---------------------------------------------------------------------------

function ErrorCell({ error }: { error?: string }) {
  return <ExtractionErrorView value={error} variant="cell" />;
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
// Job expansion (accordion content)
// ---------------------------------------------------------------------------

// STEPS lists the enrichment phases the worker records in steps_completed,
// in the order the pipeline runs them. Keep aligned with model.Step*
// constants on the server (fact_extraction, entity_extraction,
// query_augmentation, embedding).
const STEPS: Array<{ name: string; label: string }> = [
  { name: "fact_extraction", label: "Fact extraction" },
  { name: "entity_extraction", label: "Entity extraction" },
  { name: "query_augmentation", label: "Query augmentation" },
  { name: "embedding", label: "Embedding" },
];

// Maps the structured query_augment_skip_reason values written by the worker
// onto human-readable labels appended to "skipped" so the accordion row tells
// the operator why augmentation did not land in the persisted vector.
const QUERY_AUGMENT_SKIP_LABELS: Record<string, string> = {
  disabled: "feature disabled",
  content_empty: "empty content",
  provider_unavailable: "fact provider unavailable",
  llm_error: "LLM error",
  parse_error: "LLM response unparseable",
};

function StepStatusIcon({
  ran,
  jobStatus,
}: {
  ran: boolean;
  jobStatus: string;
}) {
  if (ran) {
    return (
      <FontAwesomeIcon
        icon={faCheck}
        className="h-3.5 w-3.5 text-success"
        aria-hidden="true"
      />
    );
  }
  if (jobStatus === "failed") {
    return (
      <FontAwesomeIcon
        icon={faXmark}
        className="h-3.5 w-3.5 text-destructive"
        aria-hidden="true"
      />
    );
  }
  return (
    <span
      className="inline-block h-3.5 w-3.5 rounded-full border border-muted-foreground/30"
      aria-hidden="true"
    />
  );
}

function JobExpansion({ item }: { item: EnrichmentQueueItem }) {
  const done = new Set(item.steps_completed);
  return (
    <div className="space-y-3 text-xs">
      <div>
        <h4 className="mb-1.5 font-medium text-muted-foreground">Steps</h4>
        <ul className="space-y-1">
          {STEPS.map((s) => {
            const ran = done.has(s.name);
            let label: string;
            if (ran) {
              label = "ran";
            } else if (item.status === "failed") {
              label = "did not run (job failed)";
            } else if (item.status === "completed") {
              label = "skipped";
              if (
                s.name === "query_augmentation" &&
                item.query_augment_skip_reason
              ) {
                const detail =
                  QUERY_AUGMENT_SKIP_LABELS[item.query_augment_skip_reason] ??
                  item.query_augment_skip_reason;
                label = `skipped (${detail})`;
              }
            } else {
              label = "pending";
            }
            return (
              <li key={s.name} className="flex items-center gap-2">
                <StepStatusIcon ran={ran} jobStatus={item.status} />
                <span className="font-medium text-foreground">{s.label}</span>
                <span className="text-muted-foreground">{label}</span>
              </li>
            );
          })}
        </ul>
      </div>

      {(item.claimed_by || item.claimed_at || item.last_requeue_reason) && (
        <div className="grid grid-cols-1 gap-x-6 gap-y-1 text-muted-foreground md:grid-cols-3">
          {item.claimed_by && (
            <div>
              <span className="font-medium text-foreground">Worker:</span>{" "}
              <span className="font-mono">{item.claimed_by}</span>
            </div>
          )}
          {item.claimed_at && (
            <div>
              <span className="font-medium text-foreground">Claimed:</span>{" "}
              {new Date(item.claimed_at).toLocaleString()}
            </div>
          )}
          {item.last_requeue_reason && (
            <div className="md:col-span-3">
              <span className="font-medium text-foreground">
                Last requeue:
              </span>{" "}
              {item.last_requeue_reason}
            </div>
          )}
        </div>
      )}

      {item.last_error && (
        <div>
          <h4 className="mb-1.5 font-medium text-muted-foreground">
            Last error
          </h4>
          <ExtractionErrorView value={item.last_error} variant="block" />
        </div>
      )}

      {item.project_id && (
        <div>
          <h4 className="mb-1.5 font-medium text-muted-foreground">
            Augmentation
          </h4>
          <MemoryAugmentPreviewBlock
            projectId={item.project_id}
            memoryId={item.memory_id}
            persistedQueries={item.augmented_queries ?? null}
            persistedAt={item.augmented_embedding_at ?? null}
          />
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Status filter tabs
// ---------------------------------------------------------------------------

const STATUS_TAB_ORDER: { key: EnrichmentStatusFilter; label: string }[] = [
  { key: "pending", label: "Pending" },
  { key: "processing", label: "Processing" },
  { key: "completed", label: "Completed" },
  { key: "failed", label: "Failed" },
];

// StatusFilterTabs scopes the server-side query to a single queue state (or
// All). Counts come from the queue-wide totals so each tab shows how many rows
// exist in that state regardless of which page is loaded.
function StatusFilterTabs({
  current,
  counts,
  onSelect,
}: {
  current: EnrichmentStatusFilter | undefined;
  counts: EnrichmentQueueCounts;
  onSelect: (next: EnrichmentStatusFilter | undefined) => void;
}) {
  const tabCls = (active: boolean) =>
    `inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium transition-colors ${
      active
        ? "bg-primary text-primary-foreground"
        : "bg-muted text-muted-foreground hover:bg-muted/70"
    }`;
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <button
        type="button"
        className={tabCls(current === undefined)}
        onClick={() => onSelect(undefined)}
      >
        All
      </button>
      {STATUS_TAB_ORDER.map((t) => (
        <button
          key={t.key}
          type="button"
          className={tabCls(current === t.key)}
          onClick={() => onSelect(t.key)}
        >
          {t.label}
          <span className="rounded-full bg-background/30 px-1.5 text-[10px] tabular-nums">
            {counts[t.key]}
          </span>
        </button>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Load-more sentinel (infinite scroll)
// ---------------------------------------------------------------------------

// LoadMoreSentinel auto-fetches the next page when it scrolls into view and
// also exposes an explicit button as a fallback. It mirrors the Memory
// Browser's IntersectionObserver pattern and renders a "Showing N of M"
// progress line driven by the queue-wide totals.
function LoadMoreSentinel({
  hasNextPage,
  isFetchingNextPage,
  loadedCount,
  totalCount,
  onLoadMore,
}: {
  hasNextPage: boolean;
  isFetchingNextPage: boolean;
  loadedCount: number;
  totalCount: number;
  onLoadMore: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!hasNextPage) return;
    const node = ref.current;
    if (!node) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && !isFetchingNextPage) {
          onLoadMore();
        }
      },
      { rootMargin: "200px" },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [hasNextPage, isFetchingNextPage, onLoadMore]);

  if (loadedCount === 0) return null;

  return (
    <div ref={ref} className="mt-3 flex flex-col items-center gap-2">
      {hasNextPage && (
        <button
          type="button"
          onClick={() => onLoadMore()}
          disabled={isFetchingNextPage}
          className="inline-flex items-center gap-2 rounded-md border border-border px-4 py-2 text-sm font-medium text-muted-foreground shadow-sm hover:bg-muted/50 disabled:opacity-50"
        >
          {isFetchingNextPage && <Spinner className="h-3.5 w-3.5" />}
          {isFetchingNextPage ? "Loading..." : "Load more"}
        </button>
      )}
      <p className="text-xs text-muted-foreground tabular-nums">
        Showing {loadedCount} of {totalCount}
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Queue Table
// ---------------------------------------------------------------------------

function QueueTable({
  items,
  sortField,
  sortDir,
  onSort,
  selectedIds,
  onToggleSelect,
  onToggleSelectAll,
  onRetryOne,
  retrying,
  liveJobs,
  showWriteActions = true,
  showProjectName = false,
  linkMemoryIds = false,
}: {
  items: EnrichmentQueueItem[];
  // Sort is owned by the page and applied server-side; the table only renders
  // items in the order they arrive and reports header clicks back up.
  sortField: SortField;
  sortDir: SortDir;
  onSort: (field: SortField) => void;
  selectedIds: Set<string>;
  onToggleSelect: (id: string) => void;
  onToggleSelectAll: () => void;
  onRetryOne: (id: string) => void;
  retrying: boolean;
  liveJobs: Record<string, LiveJob>;
  showWriteActions?: boolean;
  // showProjectName toggles the rendering of project_name vs project_id in
  // the Project column. Only the self tier populates project_name; org and
  // system tiers leave it empty so callers see project_id only and never
  // learn the names of other users' projects.
  showProjectName?: boolean;
  // linkMemoryIds renders the Memory ID cell as a Link to the MemoryBrowser
  // detail panel. Self tier only — org/system viewers may see jobs for
  // memories they cannot open in the standard browser.
  linkMemoryIds?: boolean;
}) {
  // Re-render every second so processing-row Elapsed counters tick.
  const hasProcessing = items.some((i) => i.status === "processing");
  useElapsedTicker(hasProcessing);
  // expandedJobs holds the queue-job ids whose per-pass detail accordion is
  // open. Held inside QueueTable (not above) so it resets only when the
  // table unmounts, surviving live SSE-driven row updates that re-key the
  // same item.id.
  const [expandedJobs, setExpandedJobs] = useState<Set<string>>(
    () => new Set(),
  );
  const toggleJob = useCallback((id: string) => {
    setExpandedJobs((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }, []);

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
            <th className="w-8 px-3 py-2.5" aria-label="Expand row" />
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
                onSort={onSort}
              />
            </th>
            <th className="px-3 py-2.5 text-left">
              <SortHeader
                label="Attempts"
                field="attempts"
                currentField={sortField}
                currentDir={sortDir}
                onSort={onSort}
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
                onSort={onSort}
              />
            </th>
            <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
              Actions
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {items.map((item) => {
            const isFailed = item.status === "failed";
            const badgeCls =
              STATUS_BADGES[item.status] || STATUS_BADGES.pending;
            const alert = rowAlert(item);
            const rowTint = alert ? ROW_TINTS[alert.kind] : "";
            const expanded = expandedJobs.has(item.id);
            // Total column count for the expansion row's colSpan. Header
            // always renders: chevron, [checkbox if showWriteActions],
            // memory_id, project, status, attempts, last_error, queued,
            // actions. That's 9 with checkbox, 8 without.
            const totalCols = showWriteActions ? 9 : 8;

            return (
              <Fragment key={item.id}>
              <tr
                className={`cursor-pointer hover:bg-muted/30 transition-colors ${rowTint}`}
                onClick={() => toggleJob(item.id)}
              >
                <td className="px-3 py-2.5">
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleJob(item.id);
                    }}
                    aria-expanded={expanded}
                    aria-label={expanded ? "Collapse row" : "Expand row"}
                    className="inline-flex h-5 w-5 items-center justify-center text-muted-foreground hover:text-foreground"
                  >
                    <FontAwesomeIcon
                      icon={expanded ? faChevronDown : faChevronRight}
                      className="h-3.5 w-3.5"
                      aria-hidden="true"
                    />
                  </button>
                </td>
                {showWriteActions && (
                  <td
                    className="px-3 py-2.5"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <input
                      type="checkbox"
                      checked={selectedIds.has(item.id)}
                      onChange={() => onToggleSelect(item.id)}
                      disabled={!isFailed}
                      className="h-4 w-4 rounded border-input text-primary focus:ring-primary disabled:opacity-30"
                    />
                  </td>
                )}
                {linkMemoryIds && item.project_id ? (
                  <td
                    className="px-3 py-2.5"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <Link
                      to={memoryFocusHref(item.project_id, item.memory_id)}
                      className="font-mono text-xs text-info hover:underline"
                      title={item.memory_id}
                    >
                      {truncateId(item.memory_id)}
                    </Link>
                  </td>
                ) : (
                  <td className="px-3 py-2.5">
                    <span
                      className="font-mono text-xs text-foreground"
                      title={item.memory_id}
                    >
                      {truncateId(item.memory_id)}
                    </span>
                  </td>
                )}
                <td
                  className="px-3 py-2.5 text-xs text-muted-foreground"
                  title={item.project_id ?? ""}
                >
                  {showProjectName && item.project_name
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
                  <td
                    className="px-3 py-2.5"
                    onClick={(e) => e.stopPropagation()}
                  >
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
              {expanded && (
                <tr className="border-b border-border bg-muted/10">
                  <td
                    colSpan={totalCols}
                    className="px-6 py-3"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <JobExpansion item={item} />
                  </td>
                </tr>
              )}
              </Fragment>
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
  const { isAdmin, isOrgOwner, user } = useAuth();
  const orgId = user?.org_id;
  const { liveJobs, poolTick, connected } = useEnrichmentLiveState();
  const statusIntervalMs = connected ? 10_000 : 3_000;

  // Default to self-tier for everyone. The TierTabs picker only renders
  // additional tiers when the caller's role grants them; plain users see
  // no picker at all.
  const [tier, setTier] = useState<Tier>("self");

  // Sort and status filter are owned here and pushed to the server through the
  // infinite query, so they order/scope the entire queue rather than the
  // currently loaded window. statusFilter undefined = all statuses.
  const [sortField, setSortField] = useState<SortField>("created_at");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [statusFilter, setStatusFilter] = useState<
    EnrichmentStatusFilter | undefined
  >(undefined);

  const statusQuery = useEnrichmentStatusInfinite({
    intervalMs: statusIntervalMs,
    tier,
    orgId,
    sort: sortField,
    dir: sortDir,
    status: statusFilter,
  });
  const retryMutation = useRetryEnrichment({ tier, orgId });
  const pauseMutation = usePauseEnrichment();
  const showWriteActions =
    (tier === "self") ||
    (tier === "org" && isOrgOwner) ||
    (tier === "system" && isAdmin);

  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  // Counts and paused are queue-wide and identical on every page, so read them
  // off the first page; items concatenate every loaded page in server order.
  const firstPage = statusQuery.data?.pages[0];
  const counts = firstPage?.counts ?? {
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
  };
  const pages = statusQuery.data?.pages;
  const items = useMemo(
    () => pages?.flatMap((p) => p.items) ?? [],
    [pages],
  );
  const isPaused = firstPage?.paused ?? false;

  const handleSort = useCallback(
    (field: SortField) => {
      if (field === sortField) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
      } else {
        setSortField(field);
        setSortDir("desc");
      }
    },
    [sortField],
  );

  const handleSelectStatus = useCallback(
    (next: EnrichmentStatusFilter | undefined) => {
      setStatusFilter(next);
      // Selection references rows from the previous filter that may no longer
      // be loaded; clear it so the bulk-retry count stays meaningful.
      setSelectedIds(new Set());
    },
    [],
  );

  // failedItems is the failed subset currently loaded; used to drive the
  // select-all checkbox over visible rows. The "Retry All Failed" action below
  // uses counts.failed (the true total) since it retries server-side.
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
      <div className="mb-6 flex items-end justify-between gap-4">
        <div>
          <h1 className="font-display text-3xl text-foreground">
            {tier === "system"
              ? "Enrichment Queue"
              : tier === "org"
                ? "Org Enrichment Queue"
                : "My Enrichment Queue"}
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {tier === "system"
              ? "Monitor the enrichment processing queue and manage worker state."
              : tier === "org"
                ? "Read/write view of your organization's enrichment queue items. Worker pause/resume is administrator-only."
                : "Your own enrichment queue items. Retry your failed jobs from this view; worker pause/resume is administrator-only."}
          </p>
        </div>
        <TierTabs current={tier} onChange={setTier} ariaLabel="Enrichment scope" />
      </div>{/* end header row */}

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

          {/* Controls bar — retry is enabled wherever showWriteActions
              applies (self/org/system per role). Pause/Resume is system-tier
              admin-only because the worker pool is global. */}
          {showWriteActions && (
          <div className="flex flex-wrap items-center gap-3">
            {tier === "system" && isAdmin && (
              <>
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
              </>
            )}

            {/* On non-system tiers the paused flag is informational only —
                surface it without exposing the pause/resume control. */}
            {tier !== "system" && isPaused && (
              <StatusNode kind="paused" label="Workers paused" />
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

            {counts.failed > 0 && (
              <button
                type="button"
                onClick={handleRetryAllFailed}
                disabled={retryMutation.isPending}
                className="inline-flex items-center gap-1.5 rounded-md border border-destructive/40 px-3 py-2 text-sm font-medium text-destructive shadow-sm hover:bg-destructive/10 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {retryMutation.isPending && <Spinner />}
                Retry All Failed ({counts.failed})
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
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <h2 className="text-lg font-medium text-foreground">
                Queue Items
              </h2>
              <StatusFilterTabs
                current={statusFilter}
                counts={counts}
                onSelect={handleSelectStatus}
              />
            </div>
            <QueueTable
              items={items}
              sortField={sortField}
              sortDir={sortDir}
              onSort={handleSort}
              selectedIds={selectedIds}
              onToggleSelect={handleToggleSelect}
              onToggleSelectAll={handleToggleSelectAll}
              onRetryOne={handleRetryOne}
              retrying={retryMutation.isPending}
              liveJobs={liveJobs}
              showWriteActions={showWriteActions}
              showProjectName={tier === "self"}
              linkMemoryIds={tier === "self"}
            />
            {/* Infinite-scroll sentinel: auto-loads the next page when it
                scrolls into view, with an explicit fallback button. */}
            <LoadMoreSentinel
              hasNextPage={!!statusQuery.hasNextPage}
              isFetchingNextPage={statusQuery.isFetchingNextPage}
              loadedCount={items.length}
              totalCount={enrichmentTotalForFilter(counts, statusFilter)}
              onLoadMore={statusQuery.fetchNextPage}
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
