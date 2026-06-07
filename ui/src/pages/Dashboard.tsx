import { useState, useMemo } from "react";
import {
  useDashboard,
  useActivity,
  useOrgDashboard,
  useOrgActivity,
  useSystemDashboard,
  useSystemActivity,
  useMeProjects,
  useProviderSlots,
  useStoreMemory,
  useDreamingStatus,
  useHealth,
} from "../hooks/useApi";
import { useEnrichmentAvailable } from "../hooks/useEnrichmentAvailable";
import { useAuth, type Tier } from "../context/AuthContext";
import { TierTabs } from "../components/TierTabs";
import { PageHeader } from "../components/PageHeader";
import { Shimmer } from "../components/Shimmer";
import { StatusNode } from "../components/StatusNode/StatusNode";
import { faGauge } from "../lib/icons";
import { formatCommit } from "../lib/formatters";
import {
  memoryRowLabel,
  type ProjectMemoryCount,
  type ActivityEvent,
  type ProviderSlot,
  type OrgAggregate,
  type UserAggregate,
  type HealthResponse,
} from "../api/client";

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

function activityBadge(type: string): { label: string; cls: string } {
  switch (type) {
    case "store":
      return {
        label: "Store",
        cls: "bg-success/10 text-success",
      };
    case "recall":
      return {
        label: "Recall",
        cls: "bg-info/10 text-info",
      };
    case "forget":
      return {
        label: "Forget",
        cls: "bg-destructive/10 text-destructive",
      };
    default:
      return {
        label: type,
        cls: "bg-muted text-muted-foreground",
      };
  }
}

// ---------------------------------------------------------------------------
// Skeleton components
// ---------------------------------------------------------------------------

function SkeletonCard() {
  return (
    <div className="surface-elevated rounded-lg p-6">
      <Shimmer variant="line" className="w-24" />
      <Shimmer variant="line" className="mt-3 h-8 w-16" />
    </div>
  );
}

function SkeletonRows({ count }: { count: number }) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="flex gap-4 py-2">
          <Shimmer variant="line" className="w-1/3" />
          <Shimmer variant="line" className="w-1/4" />
        </div>
      ))}
    </>
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function SummaryCards({
  totalMemories,
  totalProjects,
  totalEntities,
  isLoading,
}: {
  totalMemories: number;
  totalProjects: number;
  totalEntities: number;
  isLoading: boolean;
}) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>
    );
  }

  const cards = [
    { label: "Total Memories", value: totalMemories },
    { label: "Total Projects", value: totalProjects },
    { label: "Total Entities", value: totalEntities },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
      {cards.map((c) => (
        <div
          key={c.label}
          className="surface-elevated rounded-lg p-6 transition-transform duration-150 hover:-translate-y-0.5 hover:border-primary/30"
        >
          <p className="text-xs font-mono uppercase tracking-wider text-muted-foreground">{c.label}</p>
          <p className="mt-2 font-display text-4xl text-foreground">
            {c.value.toLocaleString()}
          </p>
        </div>
      ))}
    </div>
  );
}

function MemoryCountsTable({
  data,
  isLoading,
}: {
  data: ProjectMemoryCount[];
  isLoading: boolean;
}) {
  const sorted = useMemo(
    () => [...data].sort((a, b) => b.count - a.count),
    [data],
  );

  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Memories per Project</h2>
      </div>
      <div className="p-4">
        {isLoading ? (
          <SkeletonRows count={4} />
        ) : sorted.length === 0 ? (
          <p className="text-sm text-muted-foreground">No projects yet.</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 font-medium">Project</th>
                <th className="pb-2 text-right font-medium">Memories</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((p) => (
                <tr key={p.project_id} className="border-b last:border-0">
                  <td className="py-2">{p.project_name}</td>
                  <td className="py-2 text-right font-mono">
                    {p.count.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// OrgBreakdownTable renders the per-org rows from a SystemDashboardData
// response. Counts only: no content fields, no per-user data.
function OrgBreakdownTable({
  rows,
  isLoading,
}: {
  rows: OrgAggregate[];
  isLoading: boolean;
}) {
  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Per-Organization Breakdown</h2>
      </div>
      <div className="p-4">
        {isLoading ? (
          <SkeletonRows count={4} />
        ) : rows.length === 0 ? (
          <p className="text-sm text-muted-foreground">No organizations.</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 font-medium">Org</th>
                <th className="pb-2 text-right font-medium">Memories</th>
                <th className="pb-2 text-right font-medium">Users</th>
                <th className="pb-2 text-right font-medium">Projects</th>
                <th className="pb-2 text-right font-medium">Entities</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((o) => (
                <tr key={o.org_id} className="border-b last:border-0">
                  <td className="py-2">{o.org_name}</td>
                  <td className="py-2 text-right font-mono">{o.total_memories.toLocaleString()}</td>
                  <td className="py-2 text-right font-mono">{o.total_users.toLocaleString()}</td>
                  <td className="py-2 text-right font-mono">{o.total_projects.toLocaleString()}</td>
                  <td className="py-2 text-right font-mono">{o.total_entities.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// UserBreakdownTable renders the per-user rows from an OrgDashboardData
// response. Counts only: no content, no per-project rows. Email is the
// only identity field shown.
function UserBreakdownTable({
  rows,
  isLoading,
}: {
  rows: UserAggregate[];
  isLoading: boolean;
}) {
  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Per-User Breakdown</h2>
      </div>
      <div className="p-4">
        {isLoading ? (
          <SkeletonRows count={4} />
        ) : rows.length === 0 ? (
          <p className="text-sm text-muted-foreground">No users.</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 font-medium">Email</th>
                <th className="pb-2 text-right font-medium">Memories</th>
                <th className="pb-2 text-right font-medium">Projects</th>
                <th className="pb-2 text-right font-medium">Entities</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((u) => (
                <tr key={u.user_id} className="border-b last:border-0">
                  <td className="py-2">{u.email}</td>
                  <td className="py-2 text-right font-mono">{u.total_memories.toLocaleString()}</td>
                  <td className="py-2 text-right font-mono">{u.total_projects.toLocaleString()}</td>
                  <td className="py-2 text-right font-mono">{u.total_entities.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// AggregateActivity renders the tier-B/C activity payload: daily-creation
// histogram + audit-event stream. No per-row memory data, no content.
function AggregateActivity({
  data,
  isLoading,
}: {
  data: {
    daily_creation?: { date: string; count: number }[];
    audit_events?: { id: string; action: string; occurred_at: string }[];
  } | undefined;
  isLoading: boolean;
}) {
  const daily = data?.daily_creation ?? [];
  const audit = data?.audit_events ?? [];
  return (
    <div className="space-y-4">
      <div className="rounded-lg border bg-card">
        <div className="border-b px-4 py-3">
          <h2 className="text-sm font-semibold">Daily Memory Creation</h2>
        </div>
        <div className="p-4">
          {isLoading ? (
            <SkeletonRows count={5} />
          ) : daily.length === 0 ? (
            <p className="text-sm text-muted-foreground">No activity in window.</p>
          ) : (
            <ul className="space-y-1 text-xs">
              {daily.slice(-14).map((d) => (
                <li key={d.date} className="flex items-center gap-2">
                  <span className="w-24 font-mono text-muted-foreground">{d.date}</span>
                  <div className="flex-1">
                    <div
                      className="h-3 rounded bg-info/30"
                      style={{ width: `${Math.min(100, d.count * 5)}%` }}
                    />
                  </div>
                  <span className="w-10 text-right font-mono">{d.count}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>

      <div className="rounded-lg border bg-card">
        <div className="border-b px-4 py-3">
          <h2 className="text-sm font-semibold">Audit Events</h2>
        </div>
        <div className="max-h-80 overflow-y-auto p-4">
          {isLoading ? (
            <SkeletonRows count={6} />
          ) : audit.length === 0 ? (
            <p className="text-sm text-muted-foreground">No audit events.</p>
          ) : (
            <ul className="space-y-2 text-xs">
              {audit.map((e) => (
                <li key={e.id} className="flex items-start gap-2">
                  <code className="shrink-0 rounded bg-muted px-1 py-0.5 font-mono">
                    {e.action}
                  </code>
                  <span className="text-muted-foreground">
                    {new Date(e.occurred_at).toLocaleString()}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

function ActivityFeed({
  events,
  isLoading,
}: {
  events: ActivityEvent[];
  isLoading: boolean;
}) {
  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Recent Activity</h2>
      </div>
      <div className="max-h-80 overflow-y-auto p-4">
        {isLoading ? (
          <SkeletonRows count={6} />
        ) : events.length === 0 ? (
          <p className="text-sm text-muted-foreground">No recent activity.</p>
        ) : (
          <ul className="space-y-3">
            {events.map((ev) => {
              const badge = activityBadge(ev.type);
              return (
                <li key={ev.id} className="flex items-start gap-3 text-sm">
                  <span
                    className={`mt-0.5 shrink-0 rounded px-2 py-0.5 text-xs font-medium ${badge.cls}`}
                  >
                    {badge.label}
                  </span>
                  <span
                    className="min-w-0 flex-1 truncate text-muted-foreground"
                    title={ev.preview ?? undefined}
                  >
                    {memoryRowLabel(ev)}
                  </span>
                  <span className="shrink-0 text-xs text-muted-foreground">
                    {relativeTime(ev.timestamp)}
                  </span>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}

const SLOT_LABELS: Record<string, string> = {
  embedding: "Embedding",
  fact: "Fact Extraction",
  entity: "Entity Extraction",
};

function ProviderHealthCards({
  slots,
  isLoading,
}: {
  slots: ProviderSlot[];
  isLoading: boolean;
}) {
  if (isLoading) {
    return (
      <div className="space-y-3">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>
    );
  }

  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Provider Health</h2>
      </div>
      <div className="divide-y">
        {slots.map((s) => {
          const isOk = s.status === "ok";
          let statusText = "Not configured";

          if (s.configured && isOk) {
            statusText = s.type;
          } else if (s.configured && !isOk) {
            statusText = `${s.type} (${s.status ?? "unhealthy"})`;
          }

          const kind = !s.configured
            ? "paused"
            : isOk
              ? "success"
              : "error";

          return (
            <div key={s.slot} className="flex items-center gap-3 px-4 py-3">
              <StatusNode kind={kind} noIcon />
              <div className="min-w-0 flex-1">
                <p className="text-sm font-medium">{SLOT_LABELS[s.slot] ?? s.slot}</p>
                <p className="truncate text-xs text-muted-foreground">
                  {statusText}
                </p>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function BuildInfoCard({
  health,
  isLoading,
}: {
  health: HealthResponse | undefined;
  isLoading: boolean;
}) {
  if (isLoading) {
    return <SkeletonCard />;
  }
  if (!health) {
    return null;
  }

  const { build } = health;
  const rows: { label: string; value: string }[] = [
    { label: "Version", value: `v${health.version}` },
    { label: "Commit", value: formatCommit(build) ?? "unknown" },
    ...(build.time ? [{ label: "Built", value: build.time }] : []),
    ...(build.go ? [{ label: "Go", value: build.go }] : []),
    { label: "Backend", value: health.backend },
  ];

  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Build</h2>
      </div>
      <dl className="divide-y">
        {rows.map((r) => (
          <div key={r.label} className="flex items-center justify-between gap-3 px-4 py-2.5">
            <dt className="text-xs text-muted-foreground">{r.label}</dt>
            <dd className="truncate font-mono text-xs">{r.value}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

function EnrichmentQueueCard({
  queue,
  isLoading,
}: {
  queue?: { pending: number; processing: number; failed: number };
  isLoading: boolean;
}) {
  if (isLoading) return <SkeletonCard />;

  const pending = queue?.pending ?? 0;
  const processing = queue?.processing ?? 0;
  const failed = queue?.failed ?? 0;

  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Enrichment Queue</h2>
      </div>
      <div className="flex gap-4 p-4">
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
            {pending}
          </p>
          <p className="text-xs text-muted-foreground">Pending</p>
        </div>
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-info">
            {processing}
          </p>
          <p className="text-xs text-muted-foreground">Processing</p>
        </div>
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-destructive">
            {failed}
          </p>
          <p className="text-xs text-muted-foreground">Failed</p>
        </div>
      </div>
    </div>
  );
}

function DreamingStatusCard({ isLoading }: { isLoading: boolean }) {
  const { data: status } = useDreamingStatus();

  if (isLoading || !status) return null;

  const recentCycles = status.recent_cycles ?? [];
  const running = recentCycles.filter((c) => c.status === "running").length;
  const completed = recentCycles.filter((c) => c.status === "completed").length;

  return (
    <div className="rounded-lg border bg-card">
      <div className="flex items-center justify-between border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Dreaming</h2>
        <StatusNode
          kind={status.enabled ? "success" : "paused"}
          label={status.enabled ? "Enabled" : "Disabled"}
          rate={running > 0 ? 1 : undefined}
        />
      </div>
      <div className="flex gap-4 p-4">
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
            {status.dirty_count}
          </p>
          <p className="text-xs text-muted-foreground">Dirty</p>
        </div>
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-info">
            {running}
          </p>
          <p className="text-xs text-muted-foreground">Active</p>
        </div>
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-success">
            {completed}
          </p>
          <p className="text-xs text-muted-foreground">Completed</p>
        </div>
      </div>
    </div>
  );
}

function QuickStore({
  projects,
  isLoadingProjects,
}: {
  projects: { id: string; name: string; slug: string }[];
  isLoadingProjects: boolean;
}) {
  const [content, setContent] = useState("");
  const [tagsInput, setTagsInput] = useState("");
  const [selectedProject, setSelectedProject] = useState("");
  const [feedback, setFeedback] = useState<{
    type: "success" | "error";
    msg: string;
  } | null>(null);

  const storeMemory = useStoreMemory();

  // Auto-select first project when projects load
  const projectId =
    selectedProject || (projects.length > 0 ? projects[0].id : "");

  function handleStore() {
    if (!projectId) {
      setFeedback({ type: "error", msg: "No project selected." });
      return;
    }
    if (!content.trim()) {
      setFeedback({ type: "error", msg: "Content cannot be empty." });
      return;
    }

    const tags = tagsInput
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);

    setFeedback(null);
    storeMemory.mutate(
      { projectId, data: { content: content.trim(), tags } },
      {
        onSuccess: () => {
          setFeedback({ type: "success", msg: "Memory stored successfully." });
          setContent("");
          setTagsInput("");
        },
        onError: (err) => {
          setFeedback({
            type: "error",
            msg: `Failed to store: ${err.message}`,
          });
        },
      },
    );
  }

  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Quick Store</h2>
      </div>
      <div className="space-y-3 p-4">
        <div className="flex flex-col gap-3 sm:flex-row">
          <div className="flex-1">
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Content
            </label>
            <textarea
              className="w-full rounded-md border bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
              rows={3}
              placeholder="Enter memory content..."
              value={content}
              onChange={(e) => setContent(e.target.value)}
            />
          </div>
          <div className="flex flex-col gap-3 sm:w-56">
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                Project
              </label>
              {isLoadingProjects ? (
                <div className="h-9 skeleton-shimmer rounded-md" />
              ) : (
                <select
                  className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                  value={projectId}
                  onChange={(e) => setSelectedProject(e.target.value)}
                >
                  {projects.length === 0 && (
                    <option value="">No projects</option>
                  )}
                  {projects.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </select>
              )}
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                Tags (comma-separated)
              </label>
              <input
                type="text"
                className="w-full rounded-md border bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="tag1, tag2"
                value={tagsInput}
                onChange={(e) => setTagsInput(e.target.value)}
              />
            </div>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <button
            type="button"
            className="inline-flex items-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
            onClick={handleStore}
            disabled={storeMemory.isPending || !projectId || !content.trim()}
          >
            {storeMemory.isPending ? "Storing..." : "Store"}
          </button>
          {feedback && (
            <p
              className={`text-sm ${feedback.type === "success" ? "text-success" : "text-destructive"}`}
            >
              {feedback.msg}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Error state
// ---------------------------------------------------------------------------

function ErrorBanner({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">
      <span className="flex-1">{message}</span>
      <button
        type="button"
        className="shrink-0 rounded-md border border-destructive/40 px-3 py-1 text-xs font-medium hover:bg-destructive/20"
        onClick={onRetry}
      >
        Retry
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Dashboard
// ---------------------------------------------------------------------------

function Dashboard() {
  const auth = useAuth();

  const [tier, setTier] = useState<Tier>("self");
  const myOrgId = auth.user?.org_id;
  const orgIdForFetch = tier === "org" ? myOrgId : undefined;

  // Each tier's hooks are gated by `enabled` so the inactive tiers don't
  // fetch. orgIdForFetch=undefined gates the org hooks; the system hooks
  // gate explicitly on the active tier. Self always fetches (cheap, also
  // shown alongside Org-tier project lists).
  const selfDashboard = useDashboard();
  const selfActivity = useActivity(20);
  const orgDashboard = useOrgDashboard(orgIdForFetch);
  const orgActivity = useOrgActivity(orgIdForFetch);
  const systemDashboard = useSystemDashboard({ enabled: tier === "system" });
  const systemActivity = useSystemActivity({ enabled: tier === "system" });

  const projects = useMeProjects();
  const providerSlots = useProviderSlots();
  const health = useHealth();

  const projectList = Array.isArray(projects.data) ? projects.data : [];
  const slotList = Array.isArray(providerSlots.data) ? providerSlots.data : [];

  // ProviderHealthCards stays visible when the gate is closed; it's the
  // surface admins use to fix the missing slot.
  const { available: enrichmentAvailable } = useEnrichmentAvailable();

  // Pick the active tier's query results.
  const activeDash =
    tier === "system" ? systemDashboard : tier === "org" ? orgDashboard : selfDashboard;
  const activeActivity =
    tier === "system" ? systemActivity : tier === "org" ? orgActivity : selfActivity;

  const hasError = activeDash.isError || activeActivity.isError;
  const errorMessage =
    activeDash.error?.message ?? activeActivity.error?.message ?? "";

  function handleRetry() {
    activeDash.refetch();
    activeActivity.refetch();
  }

  const title =
    tier === "system"
      ? "System Overview"
      : tier === "org"
        ? "Organization Overview"
        : "My Dashboard";
  const subtitle =
    tier === "system"
      ? "System-wide metrics, per-org breakdowns, audit events."
      : tier === "org"
        ? "Aggregate metrics for your organization."
        : "Your projects and activity.";

  // Normalize the dashboard data shape across tiers. Self-tier returns
  // DashboardData (with memories_by_project), org-tier returns
  // OrgDashboardData (with user_breakdown: per-user, not per-project),
  // system-tier returns SystemDashboardData (with org_breakdown).
  const totalMemories = activeDash.data?.total_memories ?? 0;
  const totalProjects = activeDash.data?.total_projects ?? 0;
  const totalEntities = activeDash.data?.total_entities ?? 0;
  const enrichmentQueue =
    "enrichment_queue" in (activeDash.data ?? {})
      ? (activeDash.data as { enrichment_queue?: { pending: number; processing: number; failed: number } | null }).enrichment_queue ?? undefined
      : undefined;
  const memoriesByProject: ProjectMemoryCount[] =
    tier === "self"
      ? ((activeDash.data as { memories_by_project?: ProjectMemoryCount[] } | undefined)
          ?.memories_by_project ?? [])
      : [];
  const userBreakdown: UserAggregate[] =
    tier === "org"
      ? ((activeDash.data as { user_breakdown?: UserAggregate[] } | undefined)
          ?.user_breakdown ?? [])
      : [];
  const orgBreakdown: OrgAggregate[] =
    tier === "system"
      ? ((activeDash.data as { org_breakdown?: OrgAggregate[] } | undefined)
          ?.org_breakdown ?? [])
      : [];
  const activityEvents: ActivityEvent[] =
    tier === "self"
      ? (Array.isArray((selfActivity.data as { events?: ActivityEvent[] } | undefined)?.events)
          ? ((selfActivity.data as { events?: ActivityEvent[] }).events as ActivityEvent[])
          : [])
      : []; // tier-B/C activity is audit-event + histogram; rendered by AuditEventList below.

  return (
    <div className="space-y-6">
      <PageHeader
        icon={faGauge}
        title={title}
        subtitle={subtitle}
        actions={<TierTabs current={tier} onChange={setTier} ariaLabel="Dashboard scope" />}
      />

      {hasError && <ErrorBanner message={errorMessage} onRetry={handleRetry} />}

      {/* Summary cards */}
      <SummaryCards
        totalMemories={totalMemories}
        totalProjects={totalProjects}
        totalEntities={totalEntities}
        isLoading={activeDash.isLoading}
      />

      {/* Middle section: 2/3 left, 1/3 right (sidebar only for admins on self tier) */}
      <div className={`grid grid-cols-1 gap-6 ${auth.isAdmin && tier === "self" ? "lg:grid-cols-3" : ""}`}>
        <div className={`space-y-6 ${auth.isAdmin && tier === "self" ? "lg:col-span-2" : ""}`}>
          {tier === "system" ? (
            <OrgBreakdownTable rows={orgBreakdown} isLoading={activeDash.isLoading} />
          ) : tier === "org" ? (
            <UserBreakdownTable rows={userBreakdown} isLoading={activeDash.isLoading} />
          ) : (
            <MemoryCountsTable
              data={memoriesByProject}
              isLoading={activeDash.isLoading}
            />
          )}
          {tier === "self" ? (
            <ActivityFeed events={activityEvents} isLoading={activeActivity.isLoading} />
          ) : (
            <AggregateActivity
              data={activeActivity.data as { daily_creation?: { date: string; count: number }[]; audit_events?: { id: string; action: string; occurred_at: string }[] } | undefined}
              isLoading={activeActivity.isLoading}
            />
          )}
        </div>
        {auth.isAdmin && tier === "self" && (
          <div className="space-y-6">
            <ProviderHealthCards
              slots={slotList}
              isLoading={providerSlots.isLoading}
            />
            <BuildInfoCard health={health.data} isLoading={health.isLoading} />
            {enrichmentAvailable && (
              <>
                <EnrichmentQueueCard
                  queue={enrichmentQueue}
                  isLoading={activeDash.isLoading}
                />
                <DreamingStatusCard isLoading={activeDash.isLoading} />
              </>
            )}
          </div>
        )}
      </div>

      {/* Quick store: only show for users with write access on the self tab */}
      {auth.canWrite && tier === "self" && (
        <QuickStore
          projects={projectList}
          isLoadingProjects={projects.isLoading}
        />
      )}
    </div>
  );
}

export default Dashboard;
