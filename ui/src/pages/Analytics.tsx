import { useState, useMemo, useCallback } from "react";
import { useAnalytics, useUsage } from "../hooks/useApi";
import type { AnalyticsData, UsageReport, MemoryRankItem, UsageGroup } from "../api/client";
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

const COST_RATES_KEY = "nram_cost_rates";

interface CostRate {
  key: string;
  inputCostPer1k: number;
  outputCostPer1k: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function loadCostRates(): CostRate[] {
  try {
    const raw = localStorage.getItem(COST_RATES_KEY);
    if (raw) return JSON.parse(raw) as CostRate[];
  } catch {
    // ignore
  }
  return [];
}

function saveCostRates(rates: CostRate[]) {
  localStorage.setItem(COST_RATES_KEY, JSON.stringify(rates));
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function formatCost(n: number): string {
  if (n < 0.01 && n > 0) return `$${n.toFixed(4)}`;
  return `$${n.toFixed(2)}`;
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

function truncateContent(content: string, maxLen = 80): string {
  if (content.length <= maxLen) return content;
  return content.slice(0, maxLen) + "...";
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
    { label: "Total Memories", value: formatNumber(counts.total), color: "text-blue-600 dark:text-blue-400" },
    { label: "Active", value: formatNumber(counts.active), color: "text-green-600 dark:text-green-400" },
    { label: "Deleted", value: formatNumber(counts.deleted), color: "text-red-600 dark:text-red-400" },
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
                  <th className="pb-2 font-medium">Content</th>
                  <th className="pb-2 text-right font-medium">Access Count</th>
                  <th className="pb-2 text-right font-medium">Created</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {items.map((item) => (
                  <tr key={item.id}>
                    <td className="max-w-xs py-2" title={item.content}>
                      {truncateContent(item.content)}
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
    { label: "Total Processed", value: formatNumber(stats.total_processed), color: "text-blue-600 dark:text-blue-400" },
    { label: "Success Rate", value: formatPercent(stats.success_rate), color: "text-green-600 dark:text-green-400" },
    { label: "Failure Rate", value: formatPercent(stats.failure_rate), color: "text-red-600 dark:text-red-400" },
    { label: "Avg Latency", value: `${stats.avg_latency_ms.toLocaleString()}ms`, color: "text-amber-600 dark:text-amber-400" },
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
    { label: "Total Input Tokens", value: formatNumber(summary.totalInput), color: "text-blue-600 dark:text-blue-400" },
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

function UsageBreakdownTable({
  groups,
  costRates,
  isLoading,
}: {
  groups: UsageGroup[];
  costRates: CostRate[];
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
          Token usage by group key
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
                  <td className="py-2 font-mono text-xs">{g.key}</td>
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
                        className="text-xs text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300"
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

// ---------------------------------------------------------------------------
// Main Analytics Page
// ---------------------------------------------------------------------------

function Analytics() {
  const analytics = useAnalytics();
  const usage = useUsage();

  const [costRates, setCostRates] = useState<CostRate[]>(loadCostRates);

  const handleCostRateUpdate = useCallback((rates: CostRate[]) => {
    setCostRates(rates);
    saveCostRates(rates);
  }, []);

  const analyticsData = analytics.data as AnalyticsData | undefined;
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

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Analytics</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Memory analytics, usage metrics, and billing estimation.
        </p>
      </div>

      {hasError && <ErrorBanner message={errorMessage} onRetry={handleRetry} />}

      {/* Section: Memory Analytics */}
      <div className="space-y-6">
        <h2 className="text-lg font-semibold tracking-tight">
          Memory Analytics
        </h2>

        {/* Summary cards */}
        <MemoryCountCards
          data={analyticsData}
          isLoading={analytics.isLoading}
        />

        {/* Enrichment Stats */}
        <EnrichmentStatsCards
          data={analyticsData}
          isLoading={analytics.isLoading}
        />

        {/* Most Recalled and Least Recalled */}
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <MemoryRankTable
            title="Most Recalled"
            description="Memories with the highest access counts"
            items={analyticsData?.most_recalled ?? []}
            isLoading={analytics.isLoading}
          />
          <MemoryRankTable
            title="Least Recalled"
            description="Memories with the lowest access counts"
            items={analyticsData?.least_recalled ?? []}
            isLoading={analytics.isLoading}
          />
        </div>

        {/* Dead weight */}
        <MemoryRankTable
          title="Dead Weight"
          description="Memories that have never been recalled"
          items={analyticsData?.dead_weight ?? []}
          isLoading={analytics.isLoading}
        />
      </div>

      {/* Divider */}
      <hr className="border-border" />

      {/* Section: Token Usage */}
      <div className="space-y-6">
        <h2 className="text-lg font-semibold tracking-tight">Token Usage</h2>

        {/* Summary cards */}
        <TokenUsageSummaryCards
          data={usageData}
          costRates={costRates}
          isLoading={usage.isLoading}
        />

        {/* Cost rate config */}
        <CostRateEditor
          costRates={costRates}
          groupKeys={groupKeys}
          onUpdate={handleCostRateUpdate}
        />

        {/* Usage chart */}
        <UsageBarChart
          groups={groups}
          isLoading={usage.isLoading}
        />

        {/* Usage table */}
        <UsageBreakdownTable
          groups={groups}
          costRates={costRates}
          isLoading={usage.isLoading}
        />
      </div>
    </div>
  );
}

export default Analytics;
