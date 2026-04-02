import { useState, useMemo } from "react";
import {
  useDashboard,
  useActivity,
  useMeProjects,
  useProviderSlots,
  useStoreMemory,
  useDreamingStatus,
} from "../hooks/useApi";
import { useAuth } from "../context/AuthContext";
import type {
  ProjectMemoryCount,
  ActivityEvent,
  ProviderSlot,
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
        cls: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
      };
    case "recall":
      return {
        label: "Recall",
        cls: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
      };
    case "forget":
      return {
        label: "Forget",
        cls: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300",
      };
    default:
      return {
        label: type,
        cls: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300",
      };
  }
}

// ---------------------------------------------------------------------------
// Skeleton components
// ---------------------------------------------------------------------------

function SkeletonCard() {
  return (
    <div className="animate-pulse rounded-lg border bg-card p-6">
      <div className="h-4 w-24 rounded bg-muted" />
      <div className="mt-3 h-8 w-16 rounded bg-muted" />
    </div>
  );
}

function SkeletonRows({ count }: { count: number }) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="animate-pulse flex gap-4 py-2">
          <div className="h-4 w-1/3 rounded bg-muted" />
          <div className="h-4 w-1/4 rounded bg-muted" />
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
        <div key={c.label} className="rounded-lg border bg-card p-6">
          <p className="text-sm font-medium text-muted-foreground">{c.label}</p>
          <p className="mt-1 text-3xl font-bold tracking-tight">
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
                  <span className="min-w-0 flex-1 truncate">{ev.summary}</span>
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
          let dotColor = "bg-gray-400";
          let statusText = "Not configured";

          if (s.configured && isOk) {
            dotColor = "bg-green-500";
            statusText = s.type;
          } else if (s.configured && !isOk) {
            dotColor = "bg-red-500";
            statusText = `${s.type} (${s.status ?? "unhealthy"})`;
          }

          return (
            <div key={s.slot} className="flex items-center gap-3 px-4 py-3">
              <span
                className={`inline-block h-2.5 w-2.5 shrink-0 rounded-full ${dotColor}`}
              />
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

function EnrichmentQueueCard({
  queue,
  hasProviders,
  isLoading,
}: {
  queue?: { pending: number; processing: number; failed: number };
  hasProviders: boolean;
  isLoading: boolean;
}) {
  if (!hasProviders) return null;
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
          <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">
            {processing}
          </p>
          <p className="text-xs text-muted-foreground">Processing</p>
        </div>
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-red-600 dark:text-red-400">
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
        <span
          className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
            status.enabled
              ? "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"
              : "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400"
          }`}
        >
          {status.enabled ? "Enabled" : "Disabled"}
        </span>
      </div>
      <div className="flex gap-4 p-4">
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
            {status.dirty_count}
          </p>
          <p className="text-xs text-muted-foreground">Dirty</p>
        </div>
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">
            {running}
          </p>
          <p className="text-xs text-muted-foreground">Active</p>
        </div>
        <div className="flex-1 text-center">
          <p className="text-2xl font-bold text-green-600 dark:text-green-400">
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
                <div className="h-9 animate-pulse rounded-md bg-muted" />
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
              className={`text-sm ${feedback.type === "success" ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}
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
    <div className="flex items-center gap-3 rounded-lg border border-red-300 bg-red-50 p-4 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300">
      <span className="flex-1">{message}</span>
      <button
        type="button"
        className="shrink-0 rounded-md border border-red-300 px-3 py-1 text-xs font-medium hover:bg-red-100 dark:border-red-700 dark:hover:bg-red-900"
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
  const dashboard = useDashboard();
  const activity = useActivity(20);
  const projects = useMeProjects();
  const providerSlots = useProviderSlots();

  const dashData = dashboard.data;
  const activityEvents = Array.isArray(activity.data?.events) ? activity.data.events : [];
  const projectList = Array.isArray(projects.data) ? projects.data : [];
  const slotList = Array.isArray(providerSlots.data) ? providerSlots.data : [];

  const hasProviders = slotList.some((s) => s.configured);

  const hasError = dashboard.isError || activity.isError;
  const errorMessage = dashboard.error?.message ?? activity.error?.message ?? "";

  function handleRetry() {
    dashboard.refetch();
    activity.refetch();
  }

  const title = auth.isAdmin
    ? "System Overview"
    : auth.isOrgOwner
      ? "Organization Overview"
      : "Dashboard";

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {auth.isAdmin ? "System-wide metrics and activity." : "Your projects and activity."}
        </p>
      </div>

      {hasError && <ErrorBanner message={errorMessage} onRetry={handleRetry} />}

      {/* Summary cards */}
      <SummaryCards
        totalMemories={dashData?.total_memories ?? 0}
        totalProjects={dashData?.total_projects ?? 0}
        totalEntities={dashData?.total_entities ?? 0}
        isLoading={dashboard.isLoading}
      />

      {/* Middle section: 2/3 left, 1/3 right (sidebar only for admins) */}
      <div className={`grid grid-cols-1 gap-6 ${auth.isAdmin ? "lg:grid-cols-3" : ""}`}>
        <div className={`space-y-6 ${auth.isAdmin ? "lg:col-span-2" : ""}`}>
          <MemoryCountsTable
            data={dashData?.memories_by_project ?? []}
            isLoading={dashboard.isLoading}
          />
          <ActivityFeed
            events={activityEvents}
            isLoading={activity.isLoading}
          />
        </div>
        {auth.isAdmin && (
          <div className="space-y-6">
            <ProviderHealthCards
              slots={slotList}
              isLoading={providerSlots.isLoading}
            />
            <EnrichmentQueueCard
              queue={dashData?.enrichment_queue ?? undefined}
              hasProviders={hasProviders}
              isLoading={dashboard.isLoading}
            />
            <DreamingStatusCard isLoading={dashboard.isLoading} />
          </div>
        )}
      </div>

      {/* Quick store — only show for users with write access */}
      {auth.canWrite && (
        <QuickStore
          projects={projectList}
          isLoadingProjects={projects.isLoading}
        />
      )}
    </div>
  );
}

export default Dashboard;
