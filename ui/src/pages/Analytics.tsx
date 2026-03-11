import { useState, useMemo, useCallback, useEffect } from "react";
import {
  useAnalytics,
  useUsage,
  useProjects,
  useUsers,
  useOrgs,
} from "../hooks/useApi";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
  LineChart,
  Line,
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
  model: string;
  inputCostPer1k: number;
  outputCostPer1k: number;
}

interface UsageEntry {
  operation: string;
  provider: string;
  model: string;
  input_tokens: number;
  output_tokens: number;
  count: number;
  period?: string;
  org_id?: string;
  user_id?: string;
  project_id?: string;
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

function extractUsageEntries(data: Record<string, unknown>): UsageEntry[] {
  if (Array.isArray(data)) return data as UsageEntry[];
  if (data && typeof data === "object") {
    if (Array.isArray((data as Record<string, unknown>).entries))
      return (data as Record<string, unknown>).entries as UsageEntry[];
    if (Array.isArray((data as Record<string, unknown>).data))
      return (data as Record<string, unknown>).data as UsageEntry[];
    if (Array.isArray((data as Record<string, unknown>).usage))
      return (data as Record<string, unknown>).usage as UsageEntry[];
  }
  return [];
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
// Memory Activity Chart
// ---------------------------------------------------------------------------

function MemoryActivityChart({
  data,
  isLoading,
}: {
  data: { period: string; memory_stores: number; memory_recalls: number; api_requests: number }[];
  isLoading: boolean;
}) {
  if (isLoading) return <SkeletonChart />;

  if (data.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-card p-6">
        <h2 className="text-sm font-semibold">Memory Activity</h2>
        <p className="mt-4 text-sm text-muted-foreground">
          No analytics data available yet.
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-border bg-card p-6">
      <h2 className="text-sm font-semibold">Memory Activity Over Time</h2>
      <div className="mt-4 h-72">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
            <XAxis
              dataKey="period"
              tick={{ fontSize: 12 }}
              tickFormatter={(v: string) => {
                const d = new Date(v);
                return isNaN(d.getTime()) ? v : `${d.getMonth() + 1}/${d.getDate()}`;
              }}
            />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(var(--card))",
                border: "1px solid hsl(var(--border))",
                borderRadius: "0.5rem",
                fontSize: "0.75rem",
              }}
            />
            <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
            <Bar dataKey="memory_stores" name="Stores" fill={CHART_COLORS[0]} radius={[2, 2, 0, 0]} />
            <Bar dataKey="memory_recalls" name="Recalls" fill={CHART_COLORS[1]} radius={[2, 2, 0, 0]} />
            <Bar dataKey="api_requests" name="API Requests" fill={CHART_COLORS[2]} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Top Recalled / Dead Weight
// ---------------------------------------------------------------------------

function TopRecalledTable({
  data,
  isLoading,
}: {
  data: { period: string; memory_stores: number; memory_recalls: number; api_requests: number }[];
  isLoading: boolean;
}) {
  const sorted = useMemo(() => {
    return [...data]
      .sort((a, b) => b.memory_recalls - a.memory_recalls)
      .slice(0, 10);
  }, [data]);

  if (isLoading) return <SkeletonChart />;

  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Most Active Periods</h2>
        <p className="text-xs text-muted-foreground">
          Periods with the highest recall activity
        </p>
      </div>
      <div className="p-4">
        {sorted.length === 0 ? (
          <p className="text-sm text-muted-foreground">No data available.</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 font-medium">Period</th>
                <th className="pb-2 text-right font-medium">Stores</th>
                <th className="pb-2 text-right font-medium">Recalls</th>
                <th className="pb-2 text-right font-medium">API Reqs</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {sorted.map((row) => (
                <tr key={row.period}>
                  <td className="py-2">{row.period}</td>
                  <td className="py-2 text-right font-mono">
                    {row.memory_stores.toLocaleString()}
                  </td>
                  <td className="py-2 text-right font-mono">
                    {row.memory_recalls.toLocaleString()}
                  </td>
                  <td className="py-2 text-right font-mono">
                    {row.api_requests.toLocaleString()}
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

function DeadWeightDetection({
  data,
  isLoading,
}: {
  data: { period: string; memory_stores: number; memory_recalls: number; api_requests: number }[];
  isLoading: boolean;
}) {
  const deadPeriods = useMemo(() => {
    return [...data]
      .filter((d) => d.memory_recalls === 0 && d.memory_stores > 0)
      .sort((a, b) => b.memory_stores - a.memory_stores)
      .slice(0, 10);
  }, [data]);

  if (isLoading) return <SkeletonChart />;

  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Dead Weight Detection</h2>
        <p className="text-xs text-muted-foreground">
          Periods with stored memories but zero recalls
        </p>
      </div>
      <div className="p-4">
        {deadPeriods.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No dead weight periods detected. All periods with stored memories have recalls.
          </p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 font-medium">Period</th>
                <th className="pb-2 text-right font-medium">Stores</th>
                <th className="pb-2 text-right font-medium">Recalls</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {deadPeriods.map((row) => (
                <tr key={row.period}>
                  <td className="py-2">{row.period}</td>
                  <td className="py-2 text-right font-mono">
                    {row.memory_stores.toLocaleString()}
                  </td>
                  <td className="py-2 text-right font-mono text-red-600 dark:text-red-400">
                    0
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

// ---------------------------------------------------------------------------
// Enrichment Stats
// ---------------------------------------------------------------------------

function EnrichmentStats({
  data,
  isLoading,
}: {
  data: { period: string; memory_stores: number; memory_recalls: number; api_requests: number }[];
  isLoading: boolean;
}) {
  const stats = useMemo(() => {
    const totalStores = data.reduce((s, d) => s + d.memory_stores, 0);
    const totalRecalls = data.reduce((s, d) => s + d.memory_recalls, 0);
    const totalApiReqs = data.reduce((s, d) => s + d.api_requests, 0);
    const enrichmentAttempts = totalApiReqs > 0 ? totalApiReqs - totalStores - totalRecalls : 0;

    return [
      { name: "Stores", value: totalStores, color: CHART_COLORS[0] },
      { name: "Recalls", value: totalRecalls, color: CHART_COLORS[1] },
      { name: "Other API", value: Math.max(0, enrichmentAttempts), color: CHART_COLORS[2] },
    ].filter((s) => s.value > 0);
  }, [data]);

  const totals = useMemo(() => {
    return {
      stores: data.reduce((s, d) => s + d.memory_stores, 0),
      recalls: data.reduce((s, d) => s + d.memory_recalls, 0),
      apiRequests: data.reduce((s, d) => s + d.api_requests, 0),
    };
  }, [data]);

  if (isLoading) return <SkeletonChart />;

  return (
    <div className="rounded-lg border border-border bg-card p-6">
      <h2 className="text-sm font-semibold">Request Distribution</h2>
      <div className="mt-2 grid grid-cols-3 gap-4">
        <div className="text-center">
          <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">
            {formatNumber(totals.stores)}
          </p>
          <p className="text-xs text-muted-foreground">Total Stores</p>
        </div>
        <div className="text-center">
          <p className="text-2xl font-bold text-green-600 dark:text-green-400">
            {formatNumber(totals.recalls)}
          </p>
          <p className="text-xs text-muted-foreground">Total Recalls</p>
        </div>
        <div className="text-center">
          <p className="text-2xl font-bold text-amber-600 dark:text-amber-400">
            {formatNumber(totals.apiRequests)}
          </p>
          <p className="text-xs text-muted-foreground">Total API Requests</p>
        </div>
      </div>
      {stats.length > 0 && (
        <div className="mt-4 h-56">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={stats}
                cx="50%"
                cy="50%"
                innerRadius={50}
                outerRadius={80}
                paddingAngle={3}
                dataKey="value"
                label={({ name, percent }: { name: string; percent: number }) =>
                  `${name} ${(percent * 100).toFixed(0)}%`
                }
              >
                {stats.map((entry, idx) => (
                  <Cell key={entry.name} fill={entry.color || CHART_COLORS[idx % CHART_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Token Usage Section
// ---------------------------------------------------------------------------

function TokenUsageSummaryCards({
  entries,
  costRates,
  isLoading,
}: {
  entries: UsageEntry[];
  costRates: CostRate[];
  isLoading: boolean;
}) {
  const summary = useMemo(() => {
    let totalInput = 0;
    let totalOutput = 0;
    let totalCost = 0;

    for (const e of entries) {
      const input = e.input_tokens ?? 0;
      const output = e.output_tokens ?? 0;
      totalInput += input;
      totalOutput += output;

      const rate = costRates.find((r) => r.model === e.model);
      if (rate) {
        totalCost +=
          (input / 1000) * rate.inputCostPer1k +
          (output / 1000) * rate.outputCostPer1k;
      }
    }

    return { totalInput, totalOutput, totalCost };
  }, [entries, costRates]);

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
    { label: "Total Input Tokens", value: formatNumber(summary.totalInput) },
    { label: "Total Output Tokens", value: formatNumber(summary.totalOutput) },
    { label: "Estimated Cost", value: formatCost(summary.totalCost) },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
      {cards.map((c) => (
        <div
          key={c.label}
          className="rounded-lg border border-border bg-card p-4"
        >
          <p className="text-sm font-medium text-muted-foreground">{c.label}</p>
          <p className="mt-1 text-3xl font-bold tracking-tight">{c.value}</p>
        </div>
      ))}
    </div>
  );
}

function TokenUsageTable({
  entries,
  costRates,
  isLoading,
}: {
  entries: UsageEntry[];
  costRates: CostRate[];
  isLoading: boolean;
}) {
  if (isLoading) return <SkeletonChart />;

  if (entries.length === 0) {
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
          Token usage by operation, provider, and model
        </p>
      </div>
      <div className="overflow-x-auto p-4">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-muted-foreground">
              <th className="pb-2 font-medium">Operation</th>
              <th className="pb-2 font-medium">Provider</th>
              <th className="pb-2 font-medium">Model</th>
              <th className="pb-2 text-right font-medium">Count</th>
              <th className="pb-2 text-right font-medium">Input Tokens</th>
              <th className="pb-2 text-right font-medium">Output Tokens</th>
              <th className="pb-2 text-right font-medium">Est. Cost</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {entries.map((e, idx) => {
              const rate = costRates.find((r) => r.model === e.model);
              const cost = rate
                ? ((e.input_tokens ?? 0) / 1000) * rate.inputCostPer1k +
                  ((e.output_tokens ?? 0) / 1000) * rate.outputCostPer1k
                : 0;
              return (
                <tr key={`${e.operation}-${e.provider}-${e.model}-${idx}`}>
                  <td className="py-2">{e.operation || "-"}</td>
                  <td className="py-2">{e.provider || "-"}</td>
                  <td className="py-2 font-mono text-xs">{e.model || "-"}</td>
                  <td className="py-2 text-right font-mono">
                    {(e.count ?? 0).toLocaleString()}
                  </td>
                  <td className="py-2 text-right font-mono">
                    {(e.input_tokens ?? 0).toLocaleString()}
                  </td>
                  <td className="py-2 text-right font-mono">
                    {(e.output_tokens ?? 0).toLocaleString()}
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

function TokenUsageChart({
  entries,
  isLoading,
}: {
  entries: UsageEntry[];
  isLoading: boolean;
}) {
  const chartData = useMemo(() => {
    const byOp: Record<string, { operation: string; input_tokens: number; output_tokens: number }> = {};
    for (const e of entries) {
      const key = e.operation || "unknown";
      if (!byOp[key]) {
        byOp[key] = { operation: key, input_tokens: 0, output_tokens: 0 };
      }
      byOp[key].input_tokens += e.input_tokens ?? 0;
      byOp[key].output_tokens += e.output_tokens ?? 0;
    }
    return Object.values(byOp).sort(
      (a, b) => b.input_tokens + b.output_tokens - (a.input_tokens + a.output_tokens),
    );
  }, [entries]);

  if (isLoading) return <SkeletonChart />;

  if (chartData.length === 0) {
    return null;
  }

  return (
    <div className="rounded-lg border border-border bg-card p-6">
      <h2 className="text-sm font-semibold">Token Usage by Operation</h2>
      <div className="mt-4 h-72">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
            <XAxis dataKey="operation" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(var(--card))",
                border: "1px solid hsl(var(--border))",
                borderRadius: "0.5rem",
                fontSize: "0.75rem",
              }}
              formatter={(value: number) => value.toLocaleString()}
            />
            <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
            <Bar
              dataKey="input_tokens"
              name="Input Tokens"
              fill={CHART_COLORS[0]}
              radius={[2, 2, 0, 0]}
            />
            <Bar
              dataKey="output_tokens"
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
// Usage Over Time Chart
// ---------------------------------------------------------------------------

function UsageOverTimeChart({
  entries,
  isLoading,
}: {
  entries: UsageEntry[];
  isLoading: boolean;
}) {
  const chartData = useMemo(() => {
    const byPeriod: Record<string, { period: string; input_tokens: number; output_tokens: number }> = {};
    for (const e of entries) {
      const key = e.period || "total";
      if (!byPeriod[key]) {
        byPeriod[key] = { period: key, input_tokens: 0, output_tokens: 0 };
      }
      byPeriod[key].input_tokens += e.input_tokens ?? 0;
      byPeriod[key].output_tokens += e.output_tokens ?? 0;
    }
    return Object.values(byPeriod).sort((a, b) => a.period.localeCompare(b.period));
  }, [entries]);

  if (isLoading) return <SkeletonChart />;
  if (chartData.length <= 1) return null;

  return (
    <div className="rounded-lg border border-border bg-card p-6">
      <h2 className="text-sm font-semibold">Token Usage Over Time</h2>
      <div className="mt-4 h-72">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
            <XAxis
              dataKey="period"
              tick={{ fontSize: 12 }}
              tickFormatter={(v: string) => {
                const d = new Date(v);
                return isNaN(d.getTime()) ? v : `${d.getMonth() + 1}/${d.getDate()}`;
              }}
            />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(var(--card))",
                border: "1px solid hsl(var(--border))",
                borderRadius: "0.5rem",
                fontSize: "0.75rem",
              }}
              formatter={(value: number) => value.toLocaleString()}
            />
            <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
            <Line
              type="monotone"
              dataKey="input_tokens"
              name="Input Tokens"
              stroke={CHART_COLORS[0]}
              strokeWidth={2}
              dot={false}
            />
            <Line
              type="monotone"
              dataKey="output_tokens"
              name="Output Tokens"
              stroke={CHART_COLORS[3]}
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
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
  models,
  onUpdate,
}: {
  costRates: CostRate[];
  models: string[];
  onUpdate: (rates: CostRate[]) => void;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const [newModel, setNewModel] = useState("");
  const [newInputRate, setNewInputRate] = useState("0.001");
  const [newOutputRate, setNewOutputRate] = useState("0.002");

  const unconfiguredModels = useMemo(
    () => models.filter((m) => !costRates.find((r) => r.model === m)),
    [models, costRates],
  );

  function handleAdd() {
    const model = newModel.trim();
    if (!model) return;
    const inputRate = parseFloat(newInputRate) || 0;
    const outputRate = parseFloat(newOutputRate) || 0;
    const updated = [
      ...costRates.filter((r) => r.model !== model),
      { model, inputCostPer1k: inputRate, outputCostPer1k: outputRate },
    ];
    onUpdate(updated);
    setNewModel("");
    setNewInputRate("0.001");
    setNewOutputRate("0.002");
  }

  function handleRemove(model: string) {
    onUpdate(costRates.filter((r) => r.model !== model));
  }

  function handleRateChange(
    model: string,
    field: "inputCostPer1k" | "outputCostPer1k",
    value: string,
  ) {
    const updated = costRates.map((r) => {
      if (r.model !== model) return r;
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
            Set per-model cost rates for billing estimation
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
                  <th className="pb-2 font-medium">Model</th>
                  <th className="pb-2 font-medium">Input $/1K tokens</th>
                  <th className="pb-2 font-medium">Output $/1K tokens</th>
                  <th className="pb-2 font-medium" />
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {costRates.map((rate) => (
                  <tr key={rate.model}>
                    <td className="py-2 font-mono text-xs">{rate.model}</td>
                    <td className="py-2">
                      <input
                        type="number"
                        step="0.0001"
                        min="0"
                        className="w-28 rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                        value={rate.inputCostPer1k}
                        onChange={(e) =>
                          handleRateChange(rate.model, "inputCostPer1k", e.target.value)
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
                          handleRateChange(rate.model, "outputCostPer1k", e.target.value)
                        }
                      />
                    </td>
                    <td className="py-2">
                      <button
                        type="button"
                        className="text-xs text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300"
                        onClick={() => handleRemove(rate.model)}
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
                Model
              </label>
              {unconfiguredModels.length > 0 ? (
                <select
                  className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  value={newModel}
                  onChange={(e) => setNewModel(e.target.value)}
                >
                  <option value="">Select a model...</option>
                  {unconfiguredModels.map((m) => (
                    <option key={m} value={m}>
                      {m}
                    </option>
                  ))}
                  <option value="__custom__">Custom...</option>
                </select>
              ) : (
                <input
                  type="text"
                  className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="model-name"
                  value={newModel === "__custom__" ? "" : newModel}
                  onChange={(e) => setNewModel(e.target.value)}
                />
              )}
              {newModel === "__custom__" && unconfiguredModels.length > 0 && (
                <input
                  type="text"
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="Enter model name"
                  onChange={(e) => setNewModel(e.target.value)}
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
              disabled={!newModel.trim() || newModel === "__custom__"}
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
// Filters
// ---------------------------------------------------------------------------

interface FilterState {
  orgId: string;
  userId: string;
  projectId: string;
  timeRange: string;
}

function UsageFilters({
  filters,
  onChange,
  orgs,
  users,
  projects,
}: {
  filters: FilterState;
  onChange: (f: FilterState) => void;
  orgs: { id: string; name: string }[];
  users: { id: string; email: string }[];
  projects: { id: string; name: string }[];
}) {
  const selectClass =
    "rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm";

  return (
    <div className="flex flex-wrap items-end gap-3">
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Organization
        </label>
        <select
          className={selectClass}
          value={filters.orgId}
          onChange={(e) => onChange({ ...filters, orgId: e.target.value })}
        >
          <option value="">All Orgs</option>
          {orgs.map((o) => (
            <option key={o.id} value={o.id}>
              {o.name}
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          User
        </label>
        <select
          className={selectClass}
          value={filters.userId}
          onChange={(e) => onChange({ ...filters, userId: e.target.value })}
        >
          <option value="">All Users</option>
          {users.map((u) => (
            <option key={u.id} value={u.id}>
              {u.email}
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Project
        </label>
        <select
          className={selectClass}
          value={filters.projectId}
          onChange={(e) => onChange({ ...filters, projectId: e.target.value })}
        >
          <option value="">All Projects</option>
          {projects.map((p) => (
            <option key={p.id} value={p.id}>
              {p.name}
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Time Range
        </label>
        <select
          className={selectClass}
          value={filters.timeRange}
          onChange={(e) => onChange({ ...filters, timeRange: e.target.value })}
        >
          <option value="">All Time</option>
          <option value="1d">Last 24 Hours</option>
          <option value="7d">Last 7 Days</option>
          <option value="30d">Last 30 Days</option>
          <option value="90d">Last 90 Days</option>
        </select>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Analytics Page
// ---------------------------------------------------------------------------

function Analytics() {
  const analytics = useAnalytics();
  const usage = useUsage();
  const projectsQuery = useProjects();
  const usersQuery = useUsers();
  const orgsQuery = useOrgs();

  const [costRates, setCostRates] = useState<CostRate[]>(loadCostRates);
  const [filters, setFilters] = useState<FilterState>({
    orgId: "",
    userId: "",
    projectId: "",
    timeRange: "",
  });

  const handleCostRateUpdate = useCallback((rates: CostRate[]) => {
    setCostRates(rates);
    saveCostRates(rates);
  }, []);

  const analyticsData = analytics.data ?? [];
  const rawUsageData = (usage.data ?? {}) as Record<string, unknown>;
  const allUsageEntries = useMemo(
    () => extractUsageEntries(rawUsageData),
    [rawUsageData],
  );

  // Apply filters to usage entries
  const filteredUsageEntries = useMemo(() => {
    let entries = allUsageEntries;

    if (filters.orgId) {
      entries = entries.filter((e) => e.org_id === filters.orgId);
    }
    if (filters.userId) {
      entries = entries.filter((e) => e.user_id === filters.userId);
    }
    if (filters.projectId) {
      entries = entries.filter((e) => e.project_id === filters.projectId);
    }
    if (filters.timeRange) {
      const now = Date.now();
      const rangeMs: Record<string, number> = {
        "1d": 86400000,
        "7d": 604800000,
        "30d": 2592000000,
        "90d": 7776000000,
      };
      const cutoff = now - (rangeMs[filters.timeRange] ?? 0);
      entries = entries.filter((e) => {
        if (!e.period) return true;
        const t = new Date(e.period).getTime();
        return isNaN(t) || t >= cutoff;
      });
    }

    return entries;
  }, [allUsageEntries, filters]);

  // Collect unique models from usage data
  const uniqueModels = useMemo(() => {
    const models = new Set<string>();
    for (const e of allUsageEntries) {
      if (e.model) models.add(e.model);
    }
    return Array.from(models).sort();
  }, [allUsageEntries]);

  // Sync new models into cost rates if they appear
  useEffect(() => {
    // no-op: we don't auto-add rates, the user configures them manually
  }, [uniqueModels]);

  const orgs = useMemo(
    () => (orgsQuery.data ?? []).map((o) => ({ id: o.id, name: o.name })),
    [orgsQuery.data],
  );
  const users = useMemo(
    () => (usersQuery.data ?? []).map((u) => ({ id: u.id, email: u.email })),
    [usersQuery.data],
  );
  const projects = useMemo(
    () => (projectsQuery.data ?? []).map((p) => ({ id: p.id, name: p.name })),
    [projectsQuery.data],
  );

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

        {/* Activity Chart */}
        <MemoryActivityChart
          data={analyticsData}
          isLoading={analytics.isLoading}
        />

        {/* Stats and distribution */}
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <EnrichmentStats
            data={analyticsData}
            isLoading={analytics.isLoading}
          />
          <TopRecalledTable
            data={analyticsData}
            isLoading={analytics.isLoading}
          />
        </div>

        {/* Dead weight */}
        <DeadWeightDetection
          data={analyticsData}
          isLoading={analytics.isLoading}
        />
      </div>

      {/* Divider */}
      <hr className="border-border" />

      {/* Section: Token Usage */}
      <div className="space-y-6">
        <h2 className="text-lg font-semibold tracking-tight">Token Usage</h2>

        {/* Filters */}
        <UsageFilters
          filters={filters}
          onChange={setFilters}
          orgs={orgs}
          users={users}
          projects={projects}
        />

        {/* Summary cards */}
        <TokenUsageSummaryCards
          entries={filteredUsageEntries}
          costRates={costRates}
          isLoading={usage.isLoading}
        />

        {/* Cost rate config */}
        <CostRateEditor
          costRates={costRates}
          models={uniqueModels}
          onUpdate={handleCostRateUpdate}
        />

        {/* Usage chart */}
        <TokenUsageChart
          entries={filteredUsageEntries}
          isLoading={usage.isLoading}
        />

        {/* Usage over time */}
        <UsageOverTimeChart
          entries={filteredUsageEntries}
          isLoading={usage.isLoading}
        />

        {/* Usage table */}
        <TokenUsageTable
          entries={filteredUsageEntries}
          costRates={costRates}
          isLoading={usage.isLoading}
        />
      </div>
    </div>
  );
}

export default Analytics;
