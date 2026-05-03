import { useState, useMemo, useCallback } from "react";
import {
  useAnalytics,
  useUsage,
  useOrgAnalytics,
  useOrgUsage,
  useSystemAnalytics,
  useSystemUsage,
  useOrgs,
  useOrgUsers,
  useCostRates,
  useUpdateSetting,
} from "../hooks/useApi";
import { useAuth, type Tier } from "../context/AuthContext";
import { TierTabs } from "../components/TierTabs";
import { formatNumber } from "../lib/formatters";
import {
  memoryRowLabel,
  type AnalyticsData,
  type Organization,
  type User,
  type UsageReport,
  type MemoryRankItem,
  type UsageGroup,
  type UsageGroupBy,
  type OrgAnalyticsData,
  type SystemAnalyticsData,
  type HistogramBucket,
  type TypeBucket,
  type OrgAggregate,
  type UserAggregate,
  type CostRate,
} from "../api/client";

// One-shot eviction of the legacy per-browser cost-rates store now that
// rates are an admin-set global. Without this, users carry stale figures
// across the changeover.
try {
  localStorage.removeItem("nram_cost_rates");
} catch {
  // ignore
}

// Stable empty reference so useMemo/child memoization keys don't
// invalidate while the cost-rates query is in flight.
const EMPTY_COST_RATES: CostRate[] = [];

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CHART_COLORS = [
  "#3b82f6",
  "#22c55e",
  "#f59e0b",
  "#ec4899",
  "#6366f1",
  "#10b981",
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatCost(n: number): string {
  // Sub-cent values keep 4 fraction digits so a $0.0003 row doesn't
  // round to $0.00; everything else gets the standard 2.
  const fractionDigits = n > 0 && n < 0.01 ? 4 : 2;
  return `$${n.toLocaleString(undefined, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  })}`;
}

function formatPercent(n: number): string {
  return `${n.toFixed(1)}%`;
}

/** Returns true when an operation is known not to produce output tokens. */
function isOutputNA(group: UsageGroup): boolean {
  return group.tokens_input > 0 && group.tokens_output === 0;
}

function formatOutputTokens(group: UsageGroup): string {
  return isOutputNA(group) ? "N/A" : group.tokens_output.toLocaleString();
}

// ---------------------------------------------------------------------------
// Skeleton
// ---------------------------------------------------------------------------

function SkeletonCard() {
  return (
    <div className="animate-pulse rounded-lg border bg-card p-6">
      <div className="h-4 w-24 rounded bg-muted" />
      <div className="mt-3 h-8 w-16 rounded bg-muted" />
    </div>
  );
}

function SkeletonChart() {
  return (
    <div className="animate-pulse rounded-lg border bg-card p-6">
      <div className="h-4 w-32 rounded bg-muted" />
      <div className="mt-4 h-64 rounded bg-muted" />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Error Banner
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
// Memory Count Summary Cards
// ---------------------------------------------------------------------------

function MemoryCountCards({
  data,
  isLoading,
}: {
  data: AnalyticsData | undefined;
  isLoading: boolean;
}) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>
    );
  }

  const counts = data?.memory_counts ?? { total: 0, active: 0, deleted: 0, enriched: 0 };

  const cards = [
    { label: "Total Memories", value: formatNumber(counts.total), color: "text-info" },
    { label: "Active", value: formatNumber(counts.active), color: "text-success" },
    { label: "Deleted", value: formatNumber(counts.deleted), color: "text-destructive" },
    { label: "Enriched", value: formatNumber(counts.enriched), color: "text-purple-600 dark:text-purple-400" },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {cards.map((c) => (
        <div
          key={c.label}
          className="rounded-lg border border-border bg-card p-4"
        >
          <p className="text-sm font-medium text-muted-foreground">{c.label}</p>
          <p className={`mt-1 text-3xl font-bold tracking-tight ${c.color}`}>{c.value}</p>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Memory Rank Table (reused for most_recalled, least_recalled, dead_weight)
// ---------------------------------------------------------------------------

function MemoryRankTable({
  title,
  description,
  items,
  isLoading,
}: {
  title: string;
  description: string;
  items: MemoryRankItem[];
  isLoading: boolean;
}) {
  if (isLoading) return <SkeletonChart />;

  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">{title}</h2>
        <p className="text-xs text-muted-foreground">{description}</p>
      </div>
      <div className="p-4">
        {items.length === 0 ? (
          <p className="text-sm text-muted-foreground">No data available.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  <th className="pb-2 font-medium">Memory</th>
                  <th className="pb-2 text-right font-medium">Length</th>
                  <th className="pb-2 text-right font-medium">Access Count</th>
                  <th className="pb-2 text-right font-medium">Created</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {items.map((item) => (
                  <tr key={item.id}>
                    <td
                      className={`max-w-xs truncate py-2 text-xs ${item.preview ? "text-foreground" : "font-mono text-muted-foreground"}`}
                      title={item.preview ?? item.id}
                    >
                      {memoryRowLabel(item)}
                    </td>
                    <td className="py-2 text-right font-mono text-xs text-muted-foreground">
                      {item.length_chars.toLocaleString()} chars
                    </td>
                    <td className="py-2 text-right font-mono">
                      {item.access_count.toLocaleString()}
                    </td>
                    <td className="py-2 text-right text-xs text-muted-foreground">
                      {new Date(item.created_at).toLocaleDateString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Enrichment Stats
// ---------------------------------------------------------------------------

function EnrichmentStatsCards({
  data,
  isLoading,
}: {
  data: AnalyticsData | undefined;
  isLoading: boolean;
}) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>
    );
  }

  const stats = data?.enrichment_stats ?? {
    total_processed: 0,
    success_rate: 0,
    failure_rate: 0,
    avg_latency_ms: 0,
  };

  const cards = [
    { label: "Total Processed", value: formatNumber(stats.total_processed), color: "text-info" },
    { label: "Success Rate", value: formatPercent(stats.success_rate), color: "text-success" },
    { label: "Failure Rate", value: formatPercent(stats.failure_rate), color: "text-destructive" },
    { label: "Avg Latency", value: `${stats.avg_latency_ms.toLocaleString()}ms`, color: "text-warning" },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {cards.map((c) => (
        <div
          key={c.label}
          className="rounded-lg border border-border bg-card p-4"
        >
          <p className="text-sm font-medium text-muted-foreground">{c.label}</p>
          <p className={`mt-1 text-3xl font-bold tracking-tight ${c.color}`}>{c.value}</p>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Token Usage Summary Cards
// ---------------------------------------------------------------------------

function TokenUsageSummaryCards({
  data,
  costRates,
  isLoading,
}: {
  data: UsageReport | undefined;
  costRates: CostRate[];
  isLoading: boolean;
}) {
  const summary = useMemo(() => {
    const totals = data?.totals ?? { tokens_input: 0, tokens_output: 0, call_count: 0 };
    const groups = data?.groups ?? [];

    let totalCost = 0;
    for (const g of groups) {
      const rate = costRates.find((r) => r.key === g.key);
      if (rate) {
        totalCost +=
          (g.tokens_input / 1000) * rate.inputCostPer1k +
          (g.tokens_output / 1000) * rate.outputCostPer1k;
      }
    }

    return {
      totalInput: totals.tokens_input,
      totalOutput: totals.tokens_output,
      totalCalls: totals.call_count,
      totalCost,
    };
  }, [data, costRates]);

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>
    );
  }

  const cards = [
    { label: "Total Input Tokens", value: formatNumber(summary.totalInput), color: "text-info" },
    { label: "Total Output Tokens", value: formatNumber(summary.totalOutput), color: "text-cyan-600 dark:text-cyan-400" },
    { label: "Total Calls", value: formatNumber(summary.totalCalls), color: "text-indigo-600 dark:text-indigo-400" },
    { label: "Estimated Cost", value: formatCost(summary.totalCost), color: "text-emerald-600 dark:text-emerald-400" },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {cards.map((c) => (
        <div
          key={c.label}
          className="rounded-lg border border-border bg-card p-4"
        >
          <p className="text-sm font-medium text-muted-foreground">{c.label}</p>
          <p className={`mt-1 text-3xl font-bold tracking-tight ${c.color}`}>{c.value}</p>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Usage Breakdown Table
// ---------------------------------------------------------------------------

function renderGroupKey(groupBy: UsageGroupBy, key: string) {
  if (groupBy === "request_id") {
    const short = key ? key.slice(0, 8) + "…" : "(unset)";
    return (
      <code className="font-mono text-xs" title={key}>
        {short}
      </code>
    );
  }
  if (groupBy === "success") {
    const ok = key === "true" || key === "1";
    return (
      <span
        className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
          ok
            ? "bg-success/10 text-success"
            : "bg-destructive/10 text-destructive"
        }`}
      >
        {ok ? "Success" : "Failed"}
      </span>
    );
  }
  if (groupBy === "error_code") {
    return <span className="font-mono text-xs">{key || "(none)"}</span>;
  }
  return <span className="font-mono text-xs">{key}</span>;
}

function UsageBreakdownTable({
  groups,
  costRates,
  groupBy,
  isLoading,
}: {
  groups: UsageGroup[];
  costRates: CostRate[];
  groupBy: UsageGroupBy;
  isLoading: boolean;
}) {
  if (isLoading) return <SkeletonChart />;

  if (groups.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-card p-6">
        <h2 className="text-sm font-semibold">Usage Breakdown</h2>
        <p className="mt-4 text-sm text-muted-foreground">
          No usage data available.
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Usage Breakdown</h2>
        <p className="text-xs text-muted-foreground">
          Grouped by <span className="font-mono">{groupBy}</span>
        </p>
      </div>
      <div className="overflow-x-auto p-4">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-muted-foreground">
              <th className="pb-2 font-medium">Key</th>
              <th className="pb-2 text-right font-medium">Input Tokens</th>
              <th className="pb-2 text-right font-medium">Output Tokens</th>
              <th className="pb-2 text-right font-medium">Calls</th>
              <th className="pb-2 text-right font-medium">Success</th>
              <th className="pb-2 text-right font-medium">Errors</th>
              <th className="pb-2 text-right font-medium">Avg Latency</th>
              <th className="pb-2 text-right font-medium">Est. Cost</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {groups.map((g) => {
              const rate = costRates.find((r) => r.key === g.key);
              const cost = rate
                ? (g.tokens_input / 1000) * rate.inputCostPer1k +
                  (g.tokens_output / 1000) * rate.outputCostPer1k
                : 0;
              return (
                <tr key={g.key}>
                  <td className="py-2">{renderGroupKey(groupBy, g.key)}</td>
                  <td className="py-2 text-right font-mono">
                    {g.tokens_input.toLocaleString()}
                  </td>
                  <td className={`py-2 text-right font-mono ${isOutputNA(g) ? "text-muted-foreground" : ""}`}>
                    {formatOutputTokens(g)}
                  </td>
                  <td className="py-2 text-right font-mono">
                    {g.call_count.toLocaleString()}
                  </td>
                  <td className="py-2 text-right font-mono">
                    {g.success_count.toLocaleString()}
                  </td>
                  <td
                    className={`py-2 text-right font-mono ${
                      g.error_count > 0 ? "text-destructive" : "text-muted-foreground"
                    }`}
                  >
                    {g.error_count.toLocaleString()}
                  </td>
                  <td
                    className={`py-2 text-right font-mono ${
                      g.avg_latency_ms === 0 ? "text-muted-foreground" : ""
                    }`}
                  >
                    {g.avg_latency_ms === 0 ? "—" : `${Math.round(g.avg_latency_ms)} ms`}
                  </td>
                  <td className="py-2 text-right font-mono">
                    {rate ? formatCost(cost) : "-"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Usage Bar Chart
// ---------------------------------------------------------------------------

function UsageBarChart({
  groups,
  isLoading,
}: {
  groups: UsageGroup[];
  isLoading: boolean;
}) {
  const chartData = useMemo(() => {
    return [...groups]
      .sort(
        (a, b) =>
          b.tokens_input + b.tokens_output - (a.tokens_input + a.tokens_output),
      )
      .map((g) => ({
        key: g.key,
        tokens_input: g.tokens_input,
        tokens_output: isOutputNA(g) ? null : g.tokens_output,
      }));
  }, [groups]);

  if (isLoading) return <SkeletonChart />;

  if (chartData.length === 0) return null;

  return (
    <div className="rounded-lg border border-border bg-card p-6">
      <h2 className="text-sm font-semibold">Token Usage by Group</h2>
      <div className="mt-4 h-72">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
            <XAxis dataKey="key" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(var(--card))",
                border: "1px solid hsl(var(--border))",
                borderRadius: "0.5rem",
                fontSize: "0.75rem",
              }}
              formatter={(value) => value === null ? "N/A" : Number(value).toLocaleString()}
            />
            <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
            <Bar
              dataKey="tokens_input"
              name="Input Tokens"
              fill={CHART_COLORS[0]}
              radius={[2, 2, 0, 0]}
            />
            <Bar
              dataKey="tokens_output"
              name="Output Tokens"
              fill={CHART_COLORS[3]}
              radius={[2, 2, 0, 0]}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Usage Controls — group_by, success filter, date range
// ---------------------------------------------------------------------------

const GROUP_BY_OPTIONS: { value: UsageGroupBy; label: string }[] = [
  { value: "operation", label: "Operation" },
  { value: "model", label: "Model" },
  { value: "provider", label: "Provider" },
  { value: "user", label: "User" },
  { value: "project", label: "Project" },
  { value: "org", label: "Org" },
  { value: "success", label: "Success / Failed" },
  { value: "error_code", label: "Error Code" },
  { value: "request_id", label: "Request ID" },
];

function parseSuccessFilter(raw: string): boolean | undefined {
  if (raw === "true") return true;
  if (raw === "false") return false;
  return undefined;
}

function successFilterToValue(f: boolean | undefined): string {
  if (f === true) return "true";
  if (f === false) return "false";
  return "all";
}

function rangePresetToFromTo(preset: string): { from: string; to: string } {
  const now = new Date();
  const to = now.toISOString();
  let from = "";
  switch (preset) {
    case "24h":
      from = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();
      break;
    case "7d":
      from = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
      break;
    case "30d":
      from = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString();
      break;
    default:
      return { from: "", to: "" };
  }
  return { from, to };
}

function UsageControls({
  groupBy,
  setGroupBy,
  successFilter,
  setSuccessFilter,
  from,
  to,
  setFrom,
  setTo,
  org,
  setOrg,
  user,
  setUser,
  showOrgFilter,
  showUserFilter,
  orgs,
  users,
}: {
  groupBy: UsageGroupBy;
  setGroupBy: (v: UsageGroupBy) => void;
  successFilter: boolean | undefined;
  setSuccessFilter: (v: boolean | undefined) => void;
  from: string;
  to: string;
  setFrom: (v: string) => void;
  setTo: (v: string) => void;
  org?: string;
  setOrg?: (v: string | undefined) => void;
  user?: string;
  setUser?: (v: string | undefined) => void;
  showOrgFilter?: boolean;
  showUserFilter?: boolean;
  orgs?: Organization[];
  users?: User[];
}) {
  // Pending input state for the datetime fields. Committing to from/to on
  // every keystroke would refetch on every step of the spinner; commit on
  // blur instead so React Query only re-runs once the user stops typing.
  const [pendingFrom, setPendingFrom] = useState(from);
  const [pendingTo, setPendingTo] = useState(to);

  const commitDate = useCallback(
    (raw: string, set: (v: string) => void) => {
      if (!raw) {
        set("");
        return;
      }
      const d = new Date(raw);
      if (Number.isNaN(d.getTime())) return;
      set(d.toISOString());
    },
    [],
  );

  const applyPreset = useCallback(
    (preset: string) => {
      const r = rangePresetToFromTo(preset);
      setFrom(r.from);
      setTo(r.to);
      setPendingFrom(r.from);
      setPendingTo(r.to);
    },
    [setFrom, setTo],
  );

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex flex-wrap items-end gap-4">
        <div>
          <label className="block text-xs font-medium text-muted-foreground">
            Group by
          </label>
          <select
            value={groupBy}
            onChange={(e) => setGroupBy(e.target.value as UsageGroupBy)}
            className="mt-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
          >
            {GROUP_BY_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-xs font-medium text-muted-foreground">
            Show
          </label>
          <select
            value={successFilterToValue(successFilter)}
            onChange={(e) => setSuccessFilter(parseSuccessFilter(e.target.value))}
            className="mt-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
          >
            <option value="all">All calls</option>
            <option value="true">Successful only</option>
            <option value="false">Errors only</option>
          </select>
        </div>

        {showOrgFilter && setOrg && (
          <div>
            <label className="block text-xs font-medium text-muted-foreground">
              Organization
            </label>
            <select
              value={org ?? ""}
              onChange={(e) => setOrg(e.target.value || undefined)}
              className="mt-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
            >
              <option value="">All organizations</option>
              {(orgs ?? []).map((o) => (
                <option key={o.id} value={o.id}>
                  {o.name}
                </option>
              ))}
            </select>
          </div>
        )}

        {showUserFilter && setUser && (
          <div>
            <label className="block text-xs font-medium text-muted-foreground">
              User
            </label>
            <select
              value={user ?? ""}
              onChange={(e) => setUser(e.target.value || undefined)}
              className="mt-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
              disabled={!users || users.length === 0}
            >
              <option value="">All users</option>
              {(users ?? []).map((u) => (
                <option key={u.id} value={u.id}>
                  {u.display_name || u.email}
                </option>
              ))}
            </select>
          </div>
        )}

        <div>
          <label className="block text-xs font-medium text-muted-foreground">
            From
          </label>
          <input
            type="datetime-local"
            value={pendingFrom ? pendingFrom.slice(0, 16) : ""}
            onChange={(e) => setPendingFrom(e.target.value)}
            onBlur={(e) => commitDate(e.target.value, setFrom)}
            className="mt-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-muted-foreground">
            To
          </label>
          <input
            type="datetime-local"
            value={pendingTo ? pendingTo.slice(0, 16) : ""}
            onChange={(e) => setPendingTo(e.target.value)}
            onBlur={(e) => commitDate(e.target.value, setTo)}
            className="mt-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
          />
        </div>

        <div className="flex items-end gap-1">
          <button
            type="button"
            onClick={() => applyPreset("24h")}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs hover:bg-muted"
          >
            Last 24h
          </button>
          <button
            type="button"
            onClick={() => applyPreset("7d")}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs hover:bg-muted"
          >
            7d
          </button>
          <button
            type="button"
            onClick={() => applyPreset("30d")}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs hover:bg-muted"
          >
            30d
          </button>
          <button
            type="button"
            onClick={() => applyPreset("all")}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs hover:bg-muted"
          >
            All
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Cost Rate Editor
// ---------------------------------------------------------------------------

function CostRateEditor({
  costRates,
  groupKeys,
  onUpdate,
}: {
  costRates: CostRate[];
  groupKeys: string[];
  onUpdate: (rates: CostRate[]) => void;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const [newKey, setNewKey] = useState("");
  const [newInputRate, setNewInputRate] = useState("0.001");
  const [newOutputRate, setNewOutputRate] = useState("0.002");

  const unconfiguredKeys = useMemo(
    () => groupKeys.filter((k) => !costRates.find((r) => r.key === k)),
    [groupKeys, costRates],
  );

  function handleAdd() {
    const key = newKey.trim();
    if (!key) return;
    const inputRate = parseFloat(newInputRate) || 0;
    const outputRate = parseFloat(newOutputRate) || 0;
    const updated = [
      ...costRates.filter((r) => r.key !== key),
      { key, inputCostPer1k: inputRate, outputCostPer1k: outputRate },
    ];
    onUpdate(updated);
    setNewKey("");
    setNewInputRate("0.001");
    setNewOutputRate("0.002");
  }

  function handleRemove(key: string) {
    onUpdate(costRates.filter((r) => r.key !== key));
  }

  function handleRateChange(
    key: string,
    field: "inputCostPer1k" | "outputCostPer1k",
    value: string,
  ) {
    const updated = costRates.map((r) => {
      if (r.key !== key) return r;
      return { ...r, [field]: parseFloat(value) || 0 };
    });
    onUpdate(updated);
  }

  return (
    <div className="rounded-lg border border-border bg-card">
      <button
        type="button"
        className="flex w-full items-center justify-between border-b px-4 py-3 text-left"
        onClick={() => setIsOpen(!isOpen)}
      >
        <div>
          <h2 className="text-sm font-semibold">Cost Rate Configuration</h2>
          <p className="text-xs text-muted-foreground">
            Set per-group cost rates for billing estimation
          </p>
        </div>
        <span className="text-sm text-muted-foreground">
          {isOpen ? "Hide" : "Show"}
        </span>
      </button>
      {isOpen && (
        <div className="space-y-4 p-4">
          {costRates.length > 0 && (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  <th className="pb-2 font-medium">Key</th>
                  <th className="pb-2 font-medium">Input $/1K tokens</th>
                  <th className="pb-2 font-medium">Output $/1K tokens</th>
                  <th className="pb-2 font-medium" />
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {costRates.map((rate) => (
                  <tr key={rate.key}>
                    <td className="py-2 font-mono text-xs">{rate.key}</td>
                    <td className="py-2">
                      <input
                        type="number"
                        step="0.0001"
                        min="0"
                        className="w-28 rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                        value={rate.inputCostPer1k}
                        onChange={(e) =>
                          handleRateChange(rate.key, "inputCostPer1k", e.target.value)
                        }
                      />
                    </td>
                    <td className="py-2">
                      <input
                        type="number"
                        step="0.0001"
                        min="0"
                        className="w-28 rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                        value={rate.outputCostPer1k}
                        onChange={(e) =>
                          handleRateChange(rate.key, "outputCostPer1k", e.target.value)
                        }
                      />
                    </td>
                    <td className="py-2">
                      <button
                        type="button"
                        className="text-xs text-destructive hover:text-destructive"
                        onClick={() => handleRemove(rate.key)}
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          <div className="flex flex-wrap items-end gap-3">
            <div className="flex-1" style={{ minWidth: 160 }}>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                Key
              </label>
              {unconfiguredKeys.length > 0 ? (
                <select
                  className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  value={newKey}
                  onChange={(e) => setNewKey(e.target.value)}
                >
                  <option value="">Select a key...</option>
                  {unconfiguredKeys.map((k) => (
                    <option key={k} value={k}>
                      {k}
                    </option>
                  ))}
                  <option value="__custom__">Custom...</option>
                </select>
              ) : (
                <input
                  type="text"
                  className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="group-key"
                  value={newKey === "__custom__" ? "" : newKey}
                  onChange={(e) => setNewKey(e.target.value)}
                />
              )}
              {newKey === "__custom__" && unconfiguredKeys.length > 0 && (
                <input
                  type="text"
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="Enter group key"
                  onChange={(e) => setNewKey(e.target.value)}
                />
              )}
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                Input $/1K
              </label>
              <input
                type="number"
                step="0.0001"
                min="0"
                className="w-28 rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                value={newInputRate}
                onChange={(e) => setNewInputRate(e.target.value)}
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                Output $/1K
              </label>
              <input
                type="number"
                step="0.0001"
                min="0"
                className="w-28 rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                value={newOutputRate}
                onChange={(e) => setNewOutputRate(e.target.value)}
              />
            </div>
            <button
              type="button"
              className="inline-flex items-center rounded-md bg-primary px-4 py-1.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              onClick={handleAdd}
              disabled={!newKey.trim() || newKey === "__custom__"}
            >
              Add Rate
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// AggregateAnalyticsPanel renders org-tier and system-tier analytics
// payloads: recall-distribution histogram + entity/relationship type
// histograms + breakdown rows. No per-memory data, no content fields.
function AggregateAnalyticsPanel({
  recallDistribution,
  entityHistogram,
  relationshipHistogram,
  orgBreakdown,
  userBreakdown,
  isLoading,
}: {
  recallDistribution?: HistogramBucket[];
  entityHistogram?: TypeBucket[];
  relationshipHistogram?: TypeBucket[];
  orgBreakdown?: OrgAggregate[];
  userBreakdown?: UserAggregate[];
  isLoading: boolean;
}) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <SkeletonChart />
        <SkeletonChart />
      </div>
    );
  }
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <AggregateBucketCard
          title="Recall Distribution"
          description="Memories grouped by access-count bucket"
          buckets={(recallDistribution ?? []).map((b) => ({ label: b.range, count: b.count }))}
        />
        <AggregateBucketCard
          title="Entity Types"
          description="Top 20 entity types"
          buckets={(entityHistogram ?? [])
            .slice(0, 20)
            .map((b) => ({ label: b.type, count: b.count }))}
        />
      </div>
      <AggregateBucketCard
        title="Relationship Types"
        description="Top 20 relationship types"
        buckets={(relationshipHistogram ?? [])
          .slice(0, 20)
          .map((b) => ({ label: b.type, count: b.count }))}
      />
      {orgBreakdown && orgBreakdown.length > 0 && (
        <div className="rounded-lg border border-border bg-card">
          <div className="border-b px-4 py-3">
            <h3 className="text-sm font-semibold">Per-Organization Breakdown</h3>
          </div>
          <div className="overflow-x-auto p-4">
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
                {orgBreakdown.map((o) => (
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
          </div>
        </div>
      )}
      {userBreakdown && userBreakdown.length > 0 && (
        <div className="rounded-lg border border-border bg-card">
          <div className="border-b px-4 py-3">
            <h3 className="text-sm font-semibold">Per-User Breakdown</h3>
          </div>
          <div className="overflow-x-auto p-4">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  <th className="pb-2 font-medium">User</th>
                  <th className="pb-2 text-right font-medium">Memories</th>
                  <th className="pb-2 text-right font-medium">Projects</th>
                  <th className="pb-2 text-right font-medium">Entities</th>
                </tr>
              </thead>
              <tbody>
                {userBreakdown.map((u) => (
                  <tr key={u.user_id} className="border-b last:border-0">
                    <td className="py-2">{u.email}</td>
                    <td className="py-2 text-right font-mono">{u.total_memories.toLocaleString()}</td>
                    <td className="py-2 text-right font-mono">{u.total_projects.toLocaleString()}</td>
                    <td className="py-2 text-right font-mono">{u.total_entities.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function AggregateBucketCard({
  title,
  description,
  buckets,
}: {
  title: string;
  description: string;
  buckets: { label: string; count: number }[];
}) {
  const max = buckets.reduce((m, b) => Math.max(m, b.count), 0);
  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="border-b px-4 py-3">
        <h3 className="text-sm font-semibold">{title}</h3>
        <p className="text-xs text-muted-foreground">{description}</p>
      </div>
      <div className="p-4">
        {buckets.length === 0 ? (
          <p className="text-sm text-muted-foreground">No data.</p>
        ) : (
          <ul className="space-y-1 text-xs">
            {buckets.map((b) => (
              <li key={b.label} className="flex items-center gap-2">
                <span className="w-24 truncate font-mono text-muted-foreground" title={b.label}>
                  {b.label}
                </span>
                <div className="flex-1">
                  <div
                    className="h-3 rounded bg-info/30"
                    style={{ width: `${max ? (b.count / max) * 100 : 0}%` }}
                  />
                </div>
                <span className="w-12 text-right font-mono">{b.count.toLocaleString()}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Analytics Page
// ---------------------------------------------------------------------------

function Analytics() {
  const auth = useAuth();

  const [tier, setTier] = useState<Tier>("self");
  const myOrgId = auth.user?.org_id;
  const orgIdForFetch = tier === "org" ? myOrgId : undefined;

  const [groupBy, setGroupBy] = useState<UsageGroupBy>("operation");
  const [successFilter, setSuccessFilter] = useState<boolean | undefined>(undefined);
  const [from, setFrom] = useState<string>("");
  const [to, setTo] = useState<string>("");
  // Legacy org/user state retained because handful of memo/computed values
  // still reference them. Setting these has no network effect post-leak-fix.
  const [org, setOrgState] = useState<string | undefined>(undefined);
  const [user, setUser] = useState<string | undefined>(undefined);

  const setOrg = useCallback((next: string | undefined) => {
    setOrgState(next);
    setUser(undefined);
  }, []);

  const orgsQuery = useOrgs(auth.isAdmin);
  const userOrgId = auth.isAdmin ? org : auth.user?.org_id;
  const usersQuery = useOrgUsers(userOrgId ?? "");

  // Tier-A self analytics + usage (always fetched as the default).
  const selfAnalytics = useAnalytics();
  const selfUsage = useUsage({
    group_by: groupBy,
    success_only: successFilter,
    from: from || undefined,
    to: to || undefined,
  });

  // Tier-B (org-aggregate). Disabled when tier !== "org".
  const orgAnalytics = useOrgAnalytics(orgIdForFetch);
  const orgUsage = useOrgUsage(orgIdForFetch, {
    from: from || undefined,
    to: to || undefined,
    group_by: groupBy,
  });

  // Tier-C (system-aggregate). Gated on tier so the inactive tier doesn't
  // fire admin-only requests.
  const systemAnalytics = useSystemAnalytics({ enabled: tier === "system" });
  const systemUsage = useSystemUsage(
    {
      from: from || undefined,
      to: to || undefined,
      group_by: groupBy,
    },
    { enabled: tier === "system" },
  );

  const analytics =
    tier === "system" ? systemAnalytics : tier === "org" ? orgAnalytics : selfAnalytics;
  const usage =
    tier === "system" ? systemUsage : tier === "org" ? orgUsage : selfUsage;

  const costRates = useCostRates().data ?? EMPTY_COST_RATES;
  const updateSetting = useUpdateSetting();
  const handleCostRateUpdate = useCallback(
    (rates: CostRate[]) => {
      updateSetting.mutate({ key: "usage.cost_rates", value: rates, scope: "global" });
    },
    [updateSetting],
  );

  // Self-tier analytics carry the legacy AnalyticsData shape (with ranked
  // memory lists). Org/system tiers carry aggregate shapes — different
  // fields entirely. Branch the renderer by tier.
  const selfAnalyticsData =
    tier === "self" ? (analytics.data as AnalyticsData | undefined) : undefined;
  const orgAnalyticsData =
    tier === "org" ? (analytics.data as OrgAnalyticsData | undefined) : undefined;
  const systemAnalyticsData =
    tier === "system" ? (analytics.data as SystemAnalyticsData | undefined) : undefined;
  const usageData = usage.data as UsageReport | undefined;
  const groups = usageData?.groups ?? [];

  const groupKeys = useMemo(() => {
    return groups.map((g) => g.key).sort();
  }, [groups]);

  const hasError = analytics.isError || usage.isError;
  const errorMessage =
    analytics.error?.message ?? usage.error?.message ?? "";

  function handleRetry() {
    analytics.refetch();
    usage.refetch();
  }

  const title =
    tier === "system"
      ? "System Analytics"
      : tier === "org"
        ? "Organization Analytics"
        : "My Analytics";
  const subtitle =
    tier === "system"
      ? "System-wide memory analytics, recall distribution, and token usage."
      : tier === "org"
        ? "Aggregate memory analytics, recall distribution, and token usage for your organization."
        : "Your memory analytics and token usage.";

  // Memory counts shape varies by tier — self uses memory_counts, system
  // uses total_memory_counts. Normalize to the self-tier shape for the
  // existing MemoryCountCards renderer.
  const counts =
    tier === "system"
      ? systemAnalyticsData
        ? { memory_counts: systemAnalyticsData.total_memory_counts, enrichment_stats: systemAnalyticsData.enrichment_stats } as AnalyticsData
        : undefined
      : tier === "org"
        ? orgAnalyticsData
          ? { memory_counts: orgAnalyticsData.memory_counts, enrichment_stats: orgAnalyticsData.enrichment_stats } as AnalyticsData
          : undefined
        : selfAnalyticsData;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
          <p className="mt-1 text-sm text-muted-foreground">{subtitle}</p>
        </div>
        <TierTabs current={tier} onChange={setTier} ariaLabel="Analytics scope" />
      </div>

      {hasError && <ErrorBanner message={errorMessage} onRetry={handleRetry} />}

      {/* Section: Memory Analytics */}
      <div className="space-y-6">
        <h2 className="text-lg font-semibold tracking-tight">
          Memory Analytics
        </h2>

        {/* Summary cards (counts only, shape-normalized across tiers) */}
        <MemoryCountCards
          data={counts}
          isLoading={analytics.isLoading}
        />

        {/* Enrichment stats are global by nature; the queue table has no
            per-org or per-user attribution. Show on self (admin sees own
            stats) and on system (admin sees global rollup). */}
        {(tier === "self" && auth.isAdmin) || tier === "system" ? (
          <EnrichmentStatsCards
            data={counts}
            isLoading={analytics.isLoading}
          />
        ) : null}

        {tier === "self" ? (
          <>
            <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
              <MemoryRankTable
                title="Most Recalled"
                description="Your memories with the highest access counts"
                items={selfAnalyticsData?.most_recalled ?? []}
                isLoading={analytics.isLoading}
              />
              <MemoryRankTable
                title="Least Recalled"
                description="Your memories with the lowest access counts"
                items={selfAnalyticsData?.least_recalled ?? []}
                isLoading={analytics.isLoading}
              />
            </div>
            <MemoryRankTable
              title="Dead Weight"
              description="Memories you've never recalled"
              items={selfAnalyticsData?.dead_weight ?? []}
              isLoading={analytics.isLoading}
            />
          </>
        ) : (
          <AggregateAnalyticsPanel
            recallDistribution={
              tier === "system"
                ? systemAnalyticsData?.recall_distribution
                : orgAnalyticsData?.recall_distribution
            }
            entityHistogram={
              tier === "system"
                ? systemAnalyticsData?.entity_type_histogram
                : orgAnalyticsData?.entity_type_histogram
            }
            relationshipHistogram={
              tier === "system"
                ? systemAnalyticsData?.relationship_type_histogram
                : orgAnalyticsData?.relationship_type_histogram
            }
            orgBreakdown={tier === "system" ? systemAnalyticsData?.org_breakdown : undefined}
            userBreakdown={tier === "org" ? orgAnalyticsData?.user_breakdown : undefined}
            isLoading={analytics.isLoading}
          />
        )}
      </div>

      {/* Divider */}
      <hr className="border-border" />

      {/* Section: Token Usage */}
      <div className="space-y-6">
        <h2 className="text-lg font-semibold tracking-tight">Token Usage</h2>

        {/* Filters and grouping */}
        <UsageControls
          groupBy={groupBy}
          setGroupBy={setGroupBy}
          successFilter={successFilter}
          setSuccessFilter={setSuccessFilter}
          from={from}
          to={to}
          setFrom={setFrom}
          setTo={setTo}
          org={org}
          setOrg={setOrg}
          user={user}
          setUser={setUser}
          showOrgFilter={auth.isAdmin}
          // isOrgOwner is true for admins too, so the role branch is explicit.
          showUserFilter={auth.isAdmin ? !!org : auth.isOrgOwner}
          orgs={orgsQuery.data}
          users={usersQuery.data}
        />

        {/* Summary cards */}
        <TokenUsageSummaryCards
          data={usageData}
          costRates={costRates}
          isLoading={usage.isLoading}
        />

        {/* Cost rate config — admin-only; the PUT is also gated
            server-side by RequireRole(Administrator). */}
        {auth.isAdmin && (
          <CostRateEditor
            costRates={costRates}
            groupKeys={groupKeys}
            onUpdate={handleCostRateUpdate}
          />
        )}

        {/* Usage chart */}
        <UsageBarChart
          groups={groups}
          isLoading={usage.isLoading}
        />

        {/* Usage table */}
        <UsageBreakdownTable
          groups={groups}
          costRates={costRates}
          groupBy={groupBy}
          isLoading={usage.isLoading}
        />
      </div>
    </div>
  );
}

export default Analytics;
