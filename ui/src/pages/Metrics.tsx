import { useState, useMemo, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { fetchMetricsText, APIError } from "../api/client";
import {
  parsePrometheusText,
  findFamily,
  sumBaseSamples,
  sumLabeledSamples,
  suffixValue,
  type MetricFamily,
} from "../lib/promParser";
import { formatNumber } from "../lib/formatters";
import { getChartColors } from "../lib/chartColors";

// ---------------------------------------------------------------------------
// Refresh cadence
// ---------------------------------------------------------------------------

const REFRESH_OPTIONS: { label: string; value: number }[] = [
  { label: "5s", value: 5_000 },
  { label: "15s", value: 15_000 },
  { label: "30s", value: 30_000 },
  { label: "Pause", value: 0 },
];

// Cap the in-browser history so a long-lived tab doesn't grow unbounded.
const MAX_HISTORY_POINTS = 60;

interface LivePoint {
  time: string;
  inFlight: number;
  /** Requests per second since the previous scrape; null on the first tick. */
  reqRate: number | null;
}

// ---------------------------------------------------------------------------
// Families that the curated sections above the generic dump already render.
// Anything not in this set falls through to the "Other metrics" section so a
// newly added server metric still shows up without editing this page.
// ---------------------------------------------------------------------------

const CARD_FAMILIES = [
  "http_requests_in_flight",
  "http_requests_total",
  "nram_memories_total",
  "nram_memories_recalled_total",
  "nram_memories_forgotten_total",
  "nram_enrichments_total",
  "nram_embeddings_total",
  "nram_tokens_used_total",
];

const LABELED_TABLE_FAMILIES: {
  name: string;
  title: string;
  description: string;
}[] = [
  {
    name: "http_requests_total",
    title: "HTTP Requests",
    description: "By method, route pattern, and status code",
  },
  {
    name: "nram_tokens_used_total",
    title: "Tokens Used",
    description: "By provider and operation",
  },
  {
    name: "nram_enrichments_total",
    title: "Enrichments",
    description: "By outcome status",
  },
  {
    name: "nram_embeddings_total",
    title: "Embeddings",
    description: "By outcome status",
  },
  {
    name: "nram_embedding_cache_lookups_total",
    title: "Embedding Cache",
    description: "Exact-match cache lookups by result (hit|miss)",
  },
  {
    name: "nram_mcp_tool_result_truncation_total",
    title: "MCP Tool Result Truncations",
    description: "By tool and degradation tier",
  },
];

// ---------------------------------------------------------------------------
// Shared bits (mirrors the Analytics page styling)
// ---------------------------------------------------------------------------

function SkeletonCard() {
  return (
    <div className="animate-pulse rounded-lg border bg-card p-6">
      <div className="h-4 w-24 rounded bg-muted" />
      <div className="mt-3 h-8 w-16 rounded bg-muted" />
    </div>
  );
}

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

function StatCard({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <p className="text-sm font-medium text-muted-foreground">{label}</p>
      <p className={`mt-1 text-3xl font-bold tracking-tight ${color}`}>{value}</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary cards
// ---------------------------------------------------------------------------

function SummaryCards({ families }: { families: MetricFamily[] }) {
  const cacheHits = sumLabeledSamples(
    families,
    "nram_embedding_cache_lookups_total",
    "result",
    "hit",
  );
  const cacheMisses = sumLabeledSamples(
    families,
    "nram_embedding_cache_lookups_total",
    "result",
    "miss",
  );
  const cacheLookups = cacheHits + cacheMisses;
  const cacheHitRate =
    cacheLookups > 0
      ? `${((cacheHits / cacheLookups) * 100).toFixed(1)}%`
      : "No data";

  const cards = [
    {
      label: "In-Flight Requests",
      value: formatNumber(sumBaseSamples(families, "http_requests_in_flight")),
      color: "text-info",
    },
    {
      label: "Total HTTP Requests",
      value: formatNumber(sumBaseSamples(families, "http_requests_total")),
      color: "text-foreground",
    },
    {
      label: "Memories Stored",
      value: formatNumber(sumBaseSamples(families, "nram_memories_total")),
      color: "text-success",
    },
    {
      label: "Recalls",
      value: formatNumber(sumBaseSamples(families, "nram_memories_recalled_total")),
      color: "text-purple-600 dark:text-purple-400",
    },
    {
      label: "Forgotten",
      value: formatNumber(sumBaseSamples(families, "nram_memories_forgotten_total")),
      color: "text-destructive",
    },
    {
      label: "Enrichments",
      value: formatNumber(sumBaseSamples(families, "nram_enrichments_total")),
      color: "text-cyan-600 dark:text-cyan-400",
    },
    {
      label: "Embeddings",
      value: formatNumber(sumBaseSamples(families, "nram_embeddings_total")),
      color: "text-indigo-600 dark:text-indigo-400",
    },
    {
      label: "Embedding Cache Hit Rate",
      value: cacheHitRate,
      color: "text-success",
    },
    {
      label: "Tokens Used",
      value: formatNumber(sumBaseSamples(families, "nram_tokens_used_total")),
      color: "text-emerald-600 dark:text-emerald-400",
    },
  ];
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {cards.map((c) => (
        <StatCard key={c.label} {...c} />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Live charts (history accumulated in the browser across scrapes)
// ---------------------------------------------------------------------------

function LiveCharts({ history }: { history: LivePoint[] }) {
  const colors = getChartColors();
  const hasRate = history.some((p) => p.reqRate !== null);

  if (history.length < 2) {
    return (
      <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">
        Collecting live data… the request-rate and in-flight charts populate
        after the second refresh.
      </div>
    );
  }

  const tooltipStyle = {
    backgroundColor: "hsl(var(--card))",
    border: "1px solid hsl(var(--border))",
    borderRadius: "0.5rem",
    fontSize: "0.75rem",
  } as const;

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
      <div className="rounded-lg border border-border bg-card p-6">
        <h2 className="text-sm font-semibold">In-Flight Requests</h2>
        <div className="mt-4 h-56">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
              <XAxis dataKey="time" tick={{ fontSize: 11 }} minTickGap={32} />
              <YAxis tick={{ fontSize: 11 }} allowDecimals={false} />
              <Tooltip contentStyle={tooltipStyle} />
              <Line
                type="monotone"
                dataKey="inFlight"
                name="In-flight"
                stroke={colors[0]}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="rounded-lg border border-border bg-card p-6">
        <h2 className="text-sm font-semibold">Request Rate</h2>
        <div className="mt-4 h-56">
          {hasRate ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={history}>
                <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                <XAxis dataKey="time" tick={{ fontSize: 11 }} minTickGap={32} />
                <YAxis
                  tick={{ fontSize: 11 }}
                  tickFormatter={(v) => `${Number(v).toFixed(1)}`}
                />
                <Tooltip
                  contentStyle={tooltipStyle}
                  formatter={(value) =>
                    value === null ? "—" : `${Number(value).toFixed(2)} req/s`
                  }
                />
                <Line
                  type="monotone"
                  dataKey="reqRate"
                  name="Requests/sec"
                  stroke={colors[3]}
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-sm text-muted-foreground">
              Awaiting a second scrape to compute the rate…
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Histogram summary cards (count / total / average)
// ---------------------------------------------------------------------------

function HistogramSection({ families }: { families: MetricFamily[] }) {
  const histograms = families.filter((f) => f.type === "histogram");
  if (histograms.length === 0) return null;

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold tracking-tight">Latency Histograms</h2>
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        {histograms.map((fam) => {
          const count = suffixValue(fam, "_count") ?? 0;
          const sum = suffixValue(fam, "_sum") ?? 0;
          const avgMs = count > 0 ? (sum / count) * 1000 : 0;
          return (
            <div key={fam.name} className="rounded-lg border border-border bg-card p-4">
              <p className="font-mono text-xs text-muted-foreground" title={fam.help}>
                {fam.name}
              </p>
              <div className="mt-3 grid grid-cols-3 gap-2 text-center">
                <div>
                  <p className="text-xs text-muted-foreground">Count</p>
                  <p className="mt-0.5 text-lg font-semibold">{formatNumber(count)}</p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground">Total</p>
                  <p className="mt-0.5 text-lg font-semibold">{sum.toFixed(2)}s</p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground">Avg</p>
                  <p className="mt-0.5 text-lg font-semibold">{avgMs.toFixed(1)}ms</p>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Labeled counter tables
// ---------------------------------------------------------------------------

function LabeledMetricTable({
  family,
  title,
  description,
}: {
  family: MetricFamily;
  title: string;
  description: string;
}) {
  const rows = useMemo(
    () =>
      family.samples
        .filter((s) => s.suffix === undefined)
        .sort((a, b) => b.value - a.value),
    [family],
  );
  const labelKeys = useMemo(
    () =>
      Array.from(new Set(rows.flatMap((r) => Object.keys(r.labels)))).sort(),
    [rows],
  );

  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="border-b px-4 py-3">
        <h3 className="text-sm font-semibold">{title}</h3>
        <p className="text-xs text-muted-foreground">{description}</p>
      </div>
      <div className="p-4">
        {rows.length === 0 ? (
          <p className="text-sm text-muted-foreground">No data yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  {labelKeys.map((k) => (
                    <th key={k} className="pb-2 font-medium">
                      {k}
                    </th>
                  ))}
                  <th className="pb-2 text-right font-medium">Value</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {rows.map((r, i) => (
                  <tr key={i}>
                    {labelKeys.map((k) => (
                      <td
                        key={k}
                        className="max-w-xs truncate py-2 font-mono text-xs"
                        title={r.labels[k] ?? ""}
                      >
                        {r.labels[k] || "—"}
                      </td>
                    ))}
                    <td className="py-2 text-right font-mono">
                      {r.value.toLocaleString()}
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
// Generic dump for any family the curated sections didn't claim
// ---------------------------------------------------------------------------

function OtherMetricsSection({ families }: { families: MetricFamily[] }) {
  // Mirror exactly what the curated sections above render — card families,
  // the labeled-counter tables, and every histogram — so a family shown there
  // never also falls through to this fallback.
  const handled = new Set<string>([
    ...CARD_FAMILIES,
    ...LABELED_TABLE_FAMILIES.map((cfg) => cfg.name),
    ...families.filter((f) => f.type === "histogram").map((f) => f.name),
  ]);
  const leftover = families.filter((f) => !handled.has(f.name));
  if (leftover.length === 0) return null;

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold tracking-tight">Other Metrics</h2>
      <div className="space-y-4">
        {leftover.map((fam) => (
          <div key={fam.name} className="rounded-lg border border-border bg-card">
            <div className="border-b px-4 py-3">
              <div className="flex items-center gap-2">
                <h3 className="font-mono text-sm font-semibold">{fam.name}</h3>
                <span className="rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">
                  {fam.type}
                </span>
              </div>
              {fam.help && (
                <p className="mt-1 text-xs text-muted-foreground">{fam.help}</p>
              )}
            </div>
            <div className="overflow-x-auto p-4">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left text-muted-foreground">
                    <th className="pb-2 font-medium">Series</th>
                    <th className="pb-2 text-right font-medium">Value</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {fam.samples.map((s, i) => {
                    const labelStr = Object.entries(s.labels)
                      .map(([k, v]) => `${k}="${v}"`)
                      .join(", ");
                    const series = `${fam.name}${s.suffix ?? ""}${
                      labelStr ? `{${labelStr}}` : ""
                    }`;
                    return (
                      <tr key={i}>
                        <td className="max-w-md truncate py-2 font-mono text-xs" title={series}>
                          {series}
                        </td>
                        <td className="py-2 text-right font-mono">
                          {Number.isFinite(s.value)
                            ? s.value.toLocaleString()
                            : String(s.value)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

function Metrics() {
  const [refreshMs, setRefreshMs] = useState(5_000);

  const { data, isLoading, isError, error, dataUpdatedAt, refetch } = useQuery({
    queryKey: ["metrics-raw"],
    queryFn: fetchMetricsText,
    refetchInterval: refreshMs === 0 ? false : refreshMs,
    // Always go to the network on an interval tick rather than serving a cached
    // copy, so the headline numbers actually move.
    staleTime: 0,
  });

  const families = useMemo(
    () => (data ? parsePrometheusText(data) : []),
    [data],
  );

  // Accumulate a rolling window of in-flight + request-rate points across
  // scrapes. The rate is a delta of the cumulative http_requests_total counter
  // divided by wall-clock elapsed between the two scrapes.
  const [history, setHistory] = useState<LivePoint[]>([]);
  const prevTotalRef = useRef<number | null>(null);
  const prevTimeRef = useRef<number | null>(null);
  const lastProcessedRef = useRef<number>(0);

  useEffect(() => {
    if (!data || dataUpdatedAt === 0) return;
    // Guard against StrictMode's double-invoke and redundant re-runs.
    if (lastProcessedRef.current === dataUpdatedAt) return;
    lastProcessedRef.current = dataUpdatedAt;

    const inFlight = sumBaseSamples(families, "http_requests_in_flight");
    const total = sumBaseSamples(families, "http_requests_total");

    let reqRate: number | null = null;
    if (prevTotalRef.current !== null && prevTimeRef.current !== null) {
      const dtSeconds = (dataUpdatedAt - prevTimeRef.current) / 1000;
      if (dtSeconds > 0) {
        reqRate = Math.max(0, (total - prevTotalRef.current) / dtSeconds);
      }
    }
    prevTotalRef.current = total;
    prevTimeRef.current = dataUpdatedAt;

    const point: LivePoint = {
      time: new Date(dataUpdatedAt).toLocaleTimeString(),
      inFlight,
      reqRate,
    };
    setHistory((prev) => [...prev, point].slice(-MAX_HISTORY_POINTS));
  }, [families, data, dataUpdatedAt]);

  const errorMessage =
    error instanceof APIError
      ? `Failed to load /metrics (HTTP ${error.status}).`
      : error instanceof Error
        ? error.message
        : "Failed to load /metrics.";

  const lastUpdated =
    dataUpdatedAt > 0 ? new Date(dataUpdatedAt).toLocaleTimeString() : "—";

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="font-display text-3xl text-foreground">Metrics</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Live view of the server&apos;s Prometheus <code>/metrics</code>{" "}
            endpoint. Last updated {lastUpdated}.
          </p>
        </div>
        <div className="flex items-end gap-2">
          <label className="block">
            <span className="block text-xs font-medium text-muted-foreground">
              Refresh
            </span>
            <select
              value={refreshMs}
              onChange={(e) => setRefreshMs(Number(e.target.value))}
              className="mt-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
            >
              {REFRESH_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            onClick={() => refetch()}
            className="rounded-md border border-input bg-background px-3 py-1.5 text-sm hover:bg-muted"
          >
            Refresh now
          </button>
        </div>
      </div>

      {isError && <ErrorBanner message={errorMessage} onRetry={() => refetch()} />}

      {isLoading ? (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 8 }).map((_, i) => (
            <SkeletonCard key={i} />
          ))}
        </div>
      ) : (
        <>
          <SummaryCards families={families} />

          <LiveCharts history={history} />

          <HistogramSection families={families} />

          <div className="space-y-6">
            <h2 className="text-lg font-semibold tracking-tight">Counters</h2>
            {LABELED_TABLE_FAMILIES.map((cfg) => {
              const fam = findFamily(families, cfg.name);
              if (!fam) return null;
              return (
                <LabeledMetricTable
                  key={cfg.name}
                  family={fam}
                  title={cfg.title}
                  description={cfg.description}
                />
              );
            })}
          </div>

          <OtherMetricsSection families={families} />
        </>
      )}
    </div>
  );
}

export default Metrics;
