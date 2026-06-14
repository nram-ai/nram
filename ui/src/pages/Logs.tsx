import { useEffect, useMemo, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faSpinner,
  faMagnifyingGlass,
  faFileCsv,
  faFileCode,
  faXmark,
} from "../lib/icons";
import { useLogsInfinite, useLogFacets } from "../hooks/useApi";
import { downloadLogsExport, type LogEntry, type LogListParams } from "../api/client";
import { useDebounce } from "../hooks/useDebounce";

// LEVEL_STYLES maps a log level to a badge color, reusing the same semantic
// palette as the rest of the admin UI.
const LEVEL_STYLES: Record<string, string> = {
  debug: "bg-muted text-muted-foreground",
  info: "bg-info/15 text-info",
  warn: "bg-warning/20 text-warning",
  error: "bg-destructive/20 text-destructive",
};

function LevelBadge({ level }: { level: string }) {
  const cls = LEVEL_STYLES[level] ?? "bg-muted text-muted-foreground";
  return (
    <span className={`inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase ${cls}`}>
      {level}
    </span>
  );
}

function formatTs(ts: string): string {
  const d = new Date(ts);
  if (isNaN(d.getTime())) return ts;
  return d.toISOString().replace("T", " ").replace("Z", "");
}

// hasAttrs reports whether the structured attrs object carries any fields.
function hasAttrs(attrs: Record<string, unknown> | undefined): boolean {
  return !!attrs && Object.keys(attrs).length > 0;
}

function LogRow({
  entry,
  expanded,
  onToggle,
}: {
  entry: LogEntry;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="border-b border-border/60">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-start gap-3 px-3 py-2 text-left hover:bg-muted/40"
      >
        <span className="shrink-0 font-mono text-[11px] text-muted-foreground tabular-nums">
          {formatTs(entry.ts)}
        </span>
        <span className="shrink-0">
          <LevelBadge level={entry.level} />
        </span>
        <span className="shrink-0 w-28 truncate font-mono text-[11px] text-muted-foreground">
          {entry.component ?? ""}
        </span>
        <span className="min-w-0 flex-1 truncate text-sm text-foreground">{entry.message}</span>
      </button>
      {expanded && (
        <div className="space-y-2 bg-muted/30 px-3 py-2 text-xs">
          <div className="break-words font-mono text-foreground">{entry.message}</div>
          {hasAttrs(entry.attrs) && (
            <div>
              <div className="mb-1 font-medium text-muted-foreground">Attributes</div>
              <table className="w-full table-fixed">
                <tbody>
                  {Object.entries(entry.attrs).map(([k, v]) => (
                    <tr key={k} className="align-top">
                      <td className="w-40 break-words py-0.5 pr-3 font-mono text-muted-foreground">{k}</td>
                      <td className="break-words py-0.5 font-mono text-foreground">
                        {typeof v === "object" ? JSON.stringify(v) : String(v)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default function Logs() {
  const [searchInput, setSearchInput] = useState("");
  const search = useDebounce(searchInput, 300);
  const [level, setLevel] = useState<string>("");
  const [component, setComponent] = useState<string>("");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);

  const facetsQuery = useLogFacets();

  const filter = useMemo<Omit<LogListParams, "limit" | "offset">>(() => {
    const f: Omit<LogListParams, "limit" | "offset"> = {};
    if (level) f.level = [level];
    if (component) f.component = component;
    if (search.trim()) f.search = search.trim();
    return f;
  }, [level, component, search]);

  const logsQuery = useLogsInfinite({ filter });

  const entries = useMemo(
    () => logsQuery.data?.pages.flatMap((p) => p.data) ?? [],
    [logsQuery.data],
  );
  const total = logsQuery.data?.pages[0]?.pagination.total ?? 0;

  async function handleExport(format: "csv" | "json") {
    setExporting(true);
    try {
      await downloadLogsExport(format, filter);
    } finally {
      setExporting(false);
    }
  }

  const hasActiveFilters = !!(level || component || search.trim());

  return (
    <div>
      <div className="mb-6 flex items-end justify-between gap-4">
        <div>
          <h1 className="font-display text-3xl text-foreground">Logs</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Diagnostic logs captured from the server. Scroll, search, and filter; export the
            current view to share. Visible to the system operator only.
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <button
            type="button"
            onClick={() => handleExport("csv")}
            disabled={exporting}
            className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-2 text-sm font-medium text-muted-foreground shadow-sm hover:bg-muted/50 disabled:opacity-50"
          >
            <FontAwesomeIcon icon={faFileCsv} className="h-4 w-4" />
            CSV
          </button>
          <button
            type="button"
            onClick={() => handleExport("json")}
            disabled={exporting}
            className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-2 text-sm font-medium text-muted-foreground shadow-sm hover:bg-muted/50 disabled:opacity-50"
          >
            <FontAwesomeIcon icon={faFileCode} className="h-4 w-4" />
            JSON
          </button>
        </div>
      </div>

      {/* Toolbar */}
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <div className="relative min-w-[16rem] flex-1">
          <FontAwesomeIcon
            icon={faMagnifyingGlass}
            className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
          />
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Search messages..."
            className="w-full rounded-md border border-border bg-background py-2 pl-9 pr-3 text-sm shadow-sm focus:border-primary focus:outline-none"
          />
        </div>
        <select
          value={level}
          onChange={(e) => setLevel(e.target.value)}
          className="rounded-md border border-border bg-background px-3 py-2 text-sm shadow-sm focus:border-primary focus:outline-none"
        >
          <option value="">All levels</option>
          {(facetsQuery.data?.levels ?? ["debug", "info", "warn", "error"]).map((l) => (
            <option key={l} value={l}>
              {l}
            </option>
          ))}
        </select>
        <select
          value={component}
          onChange={(e) => setComponent(e.target.value)}
          className="rounded-md border border-border bg-background px-3 py-2 text-sm shadow-sm focus:border-primary focus:outline-none"
        >
          <option value="">All components</option>
          {(facetsQuery.data?.components ?? []).map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
        {hasActiveFilters && (
          <button
            type="button"
            onClick={() => {
              setSearchInput("");
              setLevel("");
              setComponent("");
            }}
            className="inline-flex items-center gap-1.5 rounded-md px-2 py-2 text-xs font-medium text-muted-foreground hover:text-foreground"
          >
            <FontAwesomeIcon icon={faXmark} className="h-3 w-3" />
            Clear
          </button>
        )}
      </div>

      {/* List */}
      <div className="overflow-hidden rounded-lg border border-border bg-card shadow-sm">
        {logsQuery.isLoading ? (
          <div className="flex items-center justify-center gap-2 py-12 text-sm text-muted-foreground">
            <FontAwesomeIcon icon={faSpinner} spin className="h-4 w-4" />
            Loading logs...
          </div>
        ) : entries.length === 0 ? (
          <div className="py-12 text-center text-sm text-muted-foreground">
            {hasActiveFilters ? "No logs match the current filters." : "No logs captured yet."}
          </div>
        ) : (
          entries.map((e) => (
            <LogRow
              key={e.id}
              entry={e}
              expanded={expandedId === e.id}
              onToggle={() => setExpandedId(expandedId === e.id ? null : e.id)}
            />
          ))
        )}
      </div>

      <LoadMoreSentinel
        hasNextPage={!!logsQuery.hasNextPage}
        isFetchingNextPage={logsQuery.isFetchingNextPage}
        loadedCount={entries.length}
        totalCount={total}
        onLoadMore={logsQuery.fetchNextPage}
      />
    </div>
  );
}

// LoadMoreSentinel auto-fetches the next page when scrolled into view and
// exposes an explicit button fallback, mirroring the Enrichment Queue page.
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
          {isFetchingNextPage && <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />}
          {isFetchingNextPage ? "Loading..." : "Load more"}
        </button>
      )}
      <p className="text-xs text-muted-foreground tabular-nums">
        Showing {loadedCount} of {totalCount}
      </p>
    </div>
  );
}
