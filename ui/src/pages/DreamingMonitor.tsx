import { useState, useCallback, useMemo, Fragment, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  useDreamingStatus,
  useDreamingCycles,
  useDreamingCycleDetail,
  useMyDreamingAggregateStatus,
  useOrgDreamingStatus,
  useSetDreamingEnabled,
  useRollbackDreamCycle,
  useAbandonDreamCycle,
} from "../hooks/useApi";
import { useEventStream } from "../hooks/useEventStream";
import { useElapsedTicker, elapsedSeconds } from "../hooks/useElapsedTicker";
import { useAuth, type Tier } from "../context/AuthContext";
import { TierTabs } from "../components/TierTabs";
import { ExtractionErrorView } from "../lib/extractionError";
import Switch from "../components/Switch";
import { firePulse } from "../components/NeuralNetwork/networkBus";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faSpinner, faChevronDown, faChevronRight } from "../lib/icons";
import type { DreamCycle, DreamLog, DreamPhaseSummary } from "../api/client";
import { formatNumber, truncateId } from "../lib/formatters";
import { copyToClipboard } from "../lib/clipboard";
import {
  PHASE_LABELS,
  SUB_PHASE_LABELS,
  formatDreamLog,
  formatFactValue,
  groupLogsByPhase,
  shortId,
  isZeroId,
  memoryFocusHref,
  type Fact,
  type FormattedLog,
  type PhaseLogGroup,
  type SubPhaseLogGroup,
} from "../lib/dreaming";
import { Link } from "react-router-dom";
import PhaseBudgetBar, { type PhaseBudgetSegment } from "../components/PhaseBudgetBar";

// Live SSE-driven state per running cycle. Populated from
// dream.cycle.heartbeat, dream.call.started/completed, and
// dream.phase.started/completed. Authoritative state still comes from the
// REST endpoints; this layer only keeps the UI feeling alive between
// polling intervals on slow LLM calls.
type LiveInFlightCall = {
  call_id: string;
  operation: string;
  model?: string;
  target_id?: string;
  started_at: string;
};

type LiveRecentCall = {
  call_id: string;
  operation: string;
  started_at: string;
  ended_at?: string;
  latency_ms?: number;
  ok?: boolean;
  tokens?: { prompt: number; completion: number; total: number };
  error?: string;
};

type LivePhaseProgress = {
  current: number;
  total: number;
  label: string;
};

type LiveCycleState = {
  cycleId: string;
  phase?: string;
  tokensUsed?: number;
  lastActivityAt?: string;
  currentCall?: LiveInFlightCall;
  recentCalls: LiveRecentCall[];
  phaseProgress?: LivePhaseProgress;
};

const LIVE_RECENT_CAP = 30;

const ABANDON_CONFIRM =
  "This cycle has not made progress in over 30 minutes. Marking it as failed will let you roll back any partial changes. The worker will be canceled if it's still running on this server. Continue?";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const STATUS_COLORS: Record<string, string> = {
  pending: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
  running: "bg-info/10 text-info",
  completed: "bg-success/10 text-success",
  failed: "bg-destructive/10 text-destructive",
  rolled_back: "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300",
};

// Codes mirror the residualReason* consts in internal/dreaming/runner.go.
// Unknown codes fall through to the raw string at the call site.
const RESIDUAL_REASON_LABELS: Record<string, string> = {
  budget_exhausted_before_phase: "no budget left",
  phase_slice_zero: "slice rounded to zero",
  budget_exhausted_during_phase: "cycle budget drained",
  phase_slice_exhausted: "slice cap hit",
  more_candidates_than_batch: "more candidates than batch cap",
  paraphrase_unvisited_candidates: "paraphrase batch cap hit",
  transitive_per_cycle_cap: "transitive per-cycle cap hit",
  transitive_hard_cap_approach: "near hard cap - pressure-pruning",
  dispatch_cap_reached: "contradiction dispatch cap hit",
  phase_budget_stopped: "phase budget exhausted",
  audit_stale_remaining: "audit: stale remaining",
  reinforce_cap_hit: "reinforce cap hit",
  consolidate_clusters_remaining: "consolidate: clusters remaining",
  stale_fetch_cap: "stale-row fetch cap",
};

const OP_COLORS: Record<string, string> = {
  entity_merged: "text-info",
  relationship_created: "text-success",
  contradiction_detected: "text-orange-600 dark:text-orange-400",
  memory_created: "text-emerald-600 dark:text-emerald-400",
  confidence_adjusted: "text-cyan-600 dark:text-cyan-400",
  memory_superseded: "text-purple-600 dark:text-purple-400",
  memory_deleted: "text-destructive",
  relationship_updated: "text-yellow-600 dark:text-yellow-400",
  entity_updated: "text-indigo-600 dark:text-indigo-400",
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function relativeTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  if (diff < 0) return "just now";
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "-";
  return new Date(iso).toLocaleString();
}

function formatDuration(start: string | null | undefined, end: string | null | undefined): string {
  if (!start || !end) return "-";
  const ms = new Date(end).getTime() - new Date(start).getTime();
  if (ms < 1000) return `${ms}ms`;
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  return `${mins}m ${secs % 60}s`;
}

function formatTokensWithCap(ps: DreamPhaseSummary): string {
  const used = ps.tokens_used.toLocaleString();
  if (ps.slice_cap && ps.slice_cap > 0) {
    return `${used} / ${formatNumber(ps.slice_cap)}`;
  }
  return used;
}

// "-" when has_residual is undefined: legacy rows can't claim no residual when
// the field was never written.
function ResidualCell({ ps }: { ps: DreamPhaseSummary }) {
  if (ps.has_residual === undefined) {
    return <span className="text-xs text-muted-foreground">-</span>;
  }
  if (!ps.has_residual) {
    return <span className="text-xs text-muted-foreground">no</span>;
  }
  const code = ps.residual_reason ?? "";
  const friendly = RESIDUAL_REASON_LABELS[code] ?? (code || "yes");
  return (
    <span className="text-xs text-warning" title={code}>
      yes <span className="text-muted-foreground">({friendly})</span>
    </span>
  );
}

// PhaseSummaryRowExpandable renders the phase summary row plus its nested
// operations accordion. The phase row expands to reveal either the sub-phase
// rows (for phases that subdivide, today only consolidation) or the flat
// operation list (for everything else). All levels start collapsed; expand
// state is owned by the parent so it survives react-query refetches.
function PhaseSummaryRowExpandable({
  ps,
  group,
  pending = false,
  expandedPhases,
  expandedSubPhases,
  expandedLogs,
  togglePhase,
  toggleSubPhase,
  toggleLog,
}: {
  ps: DreamPhaseSummary;
  group?: PhaseLogGroup;
  // pending=true for rows synthesized from logGroups for phases that haven't
  // yet been written to cycle.phase_summary (in-flight or aborted cycles).
  // The row collapses tokens/time/residual to "-" and labels status as
  // "in flight" so the user can still drill into the ops.
  pending?: boolean;
  expandedPhases: Set<string>;
  expandedSubPhases: Set<string>;
  expandedLogs: Set<string>;
  togglePhase: (key: string) => void;
  toggleSubPhase: (key: string) => void;
  toggleLog: (key: string) => void;
}) {
  const phaseExpanded = expandedPhases.has(ps.phase);
  const hasSubPhaseData = !!(ps.sub_phases && ps.sub_phases.length > 0);
  // Sub-phase budget bar (existing behavior, surfaced when the phase is
  // expanded). Sub-phase total = parent slice cap when set; otherwise sum
  // of children so the bar still spans full width when slice_cap is omitted.
  const subSegments: PhaseBudgetSegment[] | null = hasSubPhaseData
    ? ps.sub_phases!.map((sp) => ({
        key: sp.name,
        value: sp.tokens_used,
        cap: sp.slice_cap,
        hasResidual: sp.has_residual,
      }))
    : null;
  const subTotal =
    ps.slice_cap && ps.slice_cap > 0
      ? ps.slice_cap
      : (subSegments?.reduce((sum, s) => sum + Math.max(0, s.value), 0) ?? 0);

  const flatLogs = group?.logsFlat ?? [];
  // Render sub-phase rows for any phase that EITHER produced sub-phase-tagged
  // logs OR carries a sub-phase breakdown in its phase summary (the latter
  // covers cycles that completed with zero ops in every sub-phase; the
  // accordion would otherwise collapse to a misleading "no operations" line
  // while the budget bar advertises three sub-phases above it).
  const hasSubPhases = !!group?.hasSubPhases || hasSubPhaseData;
  const subGroups: SubPhaseLogGroup[] = useMemo(() => {
    if (group?.hasSubPhases) {
      const fromLogs = new Map(group.subGroups.map((sg) => [sg.subPhase, sg]));
      // Sub-phases that appear in ps.sub_phases but had zero logs still get
      // a row so the structure matches the budget bar above.
      if (hasSubPhaseData) {
        for (const sp of ps.sub_phases!) {
          if (!fromLogs.has(sp.name)) fromLogs.set(sp.name, { subPhase: sp.name, logs: [] });
        }
        const ordered: SubPhaseLogGroup[] = [];
        const seen = new Set<string>();
        for (const sp of ps.sub_phases!) {
          const sg = fromLogs.get(sp.name);
          if (sg) {
            ordered.push(sg);
            seen.add(sp.name);
          }
        }
        for (const sg of group.subGroups) {
          if (!seen.has(sg.subPhase)) ordered.push(sg);
        }
        return ordered;
      }
      return group.subGroups;
    }
    if (hasSubPhaseData) {
      return ps.sub_phases!.map((sp) => ({ subPhase: sp.name, logs: [] }));
    }
    return [];
  }, [group, hasSubPhaseData, ps.sub_phases]);

  return (
    <>
      <tr
        className="border-b last:border-0 hover:bg-muted/30 cursor-pointer"
        onClick={() => togglePhase(ps.phase)}
      >
        <td className="px-3 py-2">
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              togglePhase(ps.phase);
            }}
            aria-expanded={phaseExpanded}
            className="flex items-center gap-1.5 text-left hover:text-foreground"
          >
            <FontAwesomeIcon
              icon={phaseExpanded ? faChevronDown : faChevronRight}
              className="h-3.5 w-3.5 text-foreground transition-transform"
              aria-hidden="true"
            />
            <span>{PHASE_LABELS[ps.phase] ?? ps.phase}</span>
          </button>
        </td>
        <td className="px-3 py-2 font-mono text-xs">
          {pending ? <span className="text-muted-foreground">-</span> : formatTokensWithCap(ps)}
        </td>
        <td className="px-3 py-2 font-mono text-xs">{ps.operations}</td>
        <td className="px-3 py-2 text-muted-foreground">
          {pending
            ? "-"
            : ps.duration_ms < 1000
              ? `${ps.duration_ms}ms`
              : `${(ps.duration_ms / 1000).toFixed(1)}s`}
        </td>
        <td className="px-3 py-2">
          {pending ? <span className="text-xs text-muted-foreground">-</span> : <ResidualCell ps={ps} />}
        </td>
        <td className="px-3 py-2">
          {pending ? (
            <span className="text-xs text-info">in flight</span>
          ) : ps.skipped ? (
            <span className="text-xs text-muted-foreground">skipped</span>
          ) : ps.error ? (
            <span className="text-xs text-destructive">{ps.error}</span>
          ) : (
            <span className="text-xs text-success">ok</span>
          )}
        </td>
      </tr>
      {phaseExpanded && subSegments && (
        <tr className="border-b bg-muted/20 last:border-0">
          <td colSpan={6} className="px-3 py-3 pl-8">
            <PhaseBudgetBar
              segments={subSegments}
              total={subTotal}
              format={formatNumber}
              variant="sub_phase"
              ariaLabel={`${PHASE_LABELS[ps.phase] ?? ps.phase} sub-phase breakdown`}
            />
          </td>
        </tr>
      )}
      {phaseExpanded && hasSubPhases &&
        (subGroups.length === 0 ? (
          <tr className="border-b last:border-0">
            <td colSpan={6} className="px-3 py-2 pl-8 text-xs text-muted-foreground">
              No operations recorded.
            </td>
          </tr>
        ) : (
          subGroups.map((sg) => {
            const key = `${ps.phase}::${sg.subPhase}`;
            const subExpanded = expandedSubPhases.has(key);
            const subLabel = sg.subPhase
              ? SUB_PHASE_LABELS[sg.subPhase] ?? sg.subPhase
              : "Unattributed";
            return (
              <Fragment key={key}>
                <tr
                  className="border-b bg-muted/10 last:border-0 hover:bg-muted/30 cursor-pointer"
                  onClick={() => toggleSubPhase(key)}
                >
                  <td colSpan={6} className="px-3 py-2 pl-8">
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        toggleSubPhase(key);
                      }}
                      aria-expanded={subExpanded}
                      className="flex items-center gap-1.5 text-left text-sm hover:text-foreground"
                    >
                      <FontAwesomeIcon
                        icon={subExpanded ? faChevronDown : faChevronRight}
                        className="h-3.5 w-3.5 text-foreground transition-transform"
                        aria-hidden="true"
                      />
                      <span className="font-medium">{subLabel}</span>
                      <span className="text-xs text-muted-foreground">
                        ({sg.logs.length} {sg.logs.length === 1 ? "operation" : "operations"})
                      </span>
                    </button>
                  </td>
                </tr>
                {subExpanded && (
                  <tr className="border-b last:border-0">
                    <td colSpan={6} className="px-3 py-2 pl-12">
                      {sg.logs.length === 0 ? (
                        <p className="text-xs text-muted-foreground">No operations recorded.</p>
                      ) : (
                        <div className="max-h-96 space-y-1 overflow-y-auto">
                          {sg.logs.map((log) => (
                            <LogEntry
                              key={log.id}
                              log={log}
                              expanded={expandedLogs.has(log.id)}
                              onToggle={() => toggleLog(log.id)}
                            />
                          ))}
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })
        ))}
      {phaseExpanded && !hasSubPhases && (
        <tr className="border-b last:border-0">
          <td colSpan={6} className="px-3 py-2 pl-8">
            {flatLogs.length === 0 ? (
              <p className="text-xs text-muted-foreground">No operations recorded.</p>
            ) : (
              <div className="max-h-96 space-y-1 overflow-y-auto">
                {flatLogs.map((log) => (
                  <LogEntry
                    key={log.id}
                    log={log}
                    expanded={expandedLogs.has(log.id)}
                    onToggle={() => toggleLog(log.id)}
                  />
                ))}
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Live state via SSE
// ---------------------------------------------------------------------------

function useDreamingLiveState(orgId?: string) {
  const qc = useQueryClient();
  const [live, setLive] = useState<Record<string, LiveCycleState>>({});
  const refreshAllTiers = (...keys: string[][]) => {
    for (const k of keys) qc.invalidateQueries({ queryKey: k });
    if (orgId) qc.invalidateQueries({ queryKey: ["org", orgId, "dreaming"] });
  };

  const { connected } = useEventStream({
    scope: "",
    onEvent: (evt) => {
      const data = (evt.data ?? {}) as Record<string, any>;
      const cycleId = data.cycle_id as string | undefined;
      switch (evt.type) {
        case "dream.cycle.heartbeat": {
          if (!cycleId) return;
          setLive((prev) => {
            const cur = prev[cycleId] ?? { cycleId, recentCalls: [] };
            return {
              ...prev,
              [cycleId]: {
                ...cur,
                phase: data.phase ?? cur.phase,
                tokensUsed: typeof data.tokens_used === "number" ? data.tokens_used : cur.tokensUsed,
                lastActivityAt: data.timestamp ?? cur.lastActivityAt,
                currentCall: data.in_flight_call
                  ? {
                      call_id: data.in_flight_call.call_id,
                      operation: data.in_flight_call.operation,
                      model: data.in_flight_call.model,
                      target_id: data.in_flight_call.target_id,
                      started_at: data.in_flight_call.started_at,
                    }
                  : undefined,
              },
            };
          });
          break;
        }
        case "dream.call.started": {
          if (!cycleId) return;
          firePulse(0, 2);
          setLive((prev) => {
            const cur = prev[cycleId] ?? { cycleId, recentCalls: [] };
            const inFlight: LiveInFlightCall = {
              call_id: data.call_id,
              operation: data.operation,
              model: data.model,
              target_id: data.target_id,
              started_at: data.started_at,
            };
            const next: LiveRecentCall = {
              call_id: data.call_id,
              operation: data.operation,
              started_at: data.started_at,
            };
            return {
              ...prev,
              [cycleId]: {
                ...cur,
                phase: data.phase ?? cur.phase,
                currentCall: inFlight,
                lastActivityAt: data.started_at,
                recentCalls: [next, ...cur.recentCalls].slice(0, LIVE_RECENT_CAP),
              },
            };
          });
          break;
        }
        case "dream.call.completed": {
          if (!cycleId) return;
          setLive((prev) => {
            const cur = prev[cycleId] ?? { cycleId, recentCalls: [] };
            const updatedRecent = cur.recentCalls.map((c) =>
              c.call_id === data.call_id
                ? {
                    ...c,
                    ended_at: data.ended_at,
                    latency_ms: data.latency_ms,
                    ok: data.ok,
                    tokens: data.tokens,
                    error: data.error,
                  }
                : c,
            );
            // tokensUsed is owned by heartbeats now (SUM-derived from token_usage).
            // Mutating it from per-call deltas would race the next heartbeat
            // and double-count between ticks.
            return {
              ...prev,
              [cycleId]: {
                ...cur,
                currentCall:
                  cur.currentCall?.call_id === data.call_id ? undefined : cur.currentCall,
                lastActivityAt: data.ended_at ?? cur.lastActivityAt,
                recentCalls: updatedRecent,
              },
            };
          });
          break;
        }
        case "dream.phase.started":
        case "dream.phase.completed": {
          if (!cycleId) return;
          setLive((prev) => {
            const cur = prev[cycleId] ?? { cycleId, recentCalls: [] };
            return {
              ...prev,
              [cycleId]: {
                ...cur,
                phase: data.phase ?? cur.phase,
                tokensUsed:
                  typeof data.tokens_used === "number" ? data.tokens_used : cur.tokensUsed,
                lastActivityAt: new Date().toISOString(),
                // Reset progress on phase boundary: a fresh phase starts at 0/0.
                phaseProgress: undefined,
              },
            };
          });
          refreshAllTiers(
            ["admin", "dreaming", "cycles"],
            ["me", "dreaming", "cycles"],
          );
          break;
        }
        case "dream.phase.progress": {
          if (!cycleId) return;
          setLive((prev) => {
            const cur = prev[cycleId] ?? { cycleId, recentCalls: [] };
            return {
              ...prev,
              [cycleId]: {
                ...cur,
                phase: data.phase ?? cur.phase,
                lastActivityAt: data.timestamp ?? cur.lastActivityAt,
                phaseProgress: {
                  current: typeof data.current === "number" ? data.current : 0,
                  total: typeof data.total === "number" ? data.total : 0,
                  label: typeof data.label === "string" ? data.label : "",
                },
              },
            };
          });
          break;
        }
        case "dream.cycle.completed":
        case "dream.cycle.failed":
        case "dream.cycle.rolled_back": {
          if (!cycleId) return;
          setLive((prev) => {
            const next = { ...prev };
            delete next[cycleId];
            return next;
          });
          refreshAllTiers(
            ["admin", "dreaming"],
            ["admin", "dreaming", "cycles"],
            ["me", "dreaming", "project"],
            ["me", "dreaming", "cycles"],
          );
          break;
        }
        default:
          break;
      }
    },
  });

  return { live, connected };
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function Spinner() {
  return <FontAwesomeIcon icon={faSpinner} spin className="h-5 w-5 text-muted-foreground" />;
}

function StatCard({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div className="rounded-lg border bg-card p-4 shadow-sm">
      <p className="text-sm font-medium text-muted-foreground">{label}</p>
      <p className={`mt-1 text-2xl font-bold ${color ?? "text-foreground"}`}>{value}</p>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${STATUS_COLORS[status] ?? "bg-muted text-muted-foreground"}`}>
      {status.replace(/_/g, " ")}
    </span>
  );
}

function StatusToast({ message, type }: { message: string; type: "success" | "error" }) {
  return (
    <div
      className={`fixed bottom-4 right-4 z-50 flex items-center gap-2 rounded-md px-4 py-2.5 text-sm font-medium shadow-lg transition-all ${ type === "success" ? "bg-success/10 text-success" : "bg-destructive/10 text-destructive" }`}
    >
      {type === "success" ? "\u2713" : "\u2717"} {message}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Cycle List Table
// ---------------------------------------------------------------------------

function StuckPill() {
  return (
    <span className="inline-flex items-center rounded-full bg-destructive/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-destructive">
      stuck
    </span>
  );
}

function StaleDiagnosticPill() {
  return (
    <span
      className="inline-flex items-center rounded-full bg-warning/70 px-2 py-0.5 text-[10px] font-medium text-warning"
      title="Heartbeat is stale, the worker may have stopped making progress."
    >
      no recent activity
    </span>
  );
}

function operationLabel(op: string): string {
  switch (op) {
    case "alignment":
      return "alignment";
    case "synthesis":
      return "synthesis";
    case "novelty_audit":
      return "novelty audit";
    case "novelty_backfill":
      return "novelty backfill";
    case "contradiction_judge":
      return "contradiction judge";
    default:
      return op.replace(/_/g, " ");
  }
}

function InFlightCallChip({ call }: { call: LiveInFlightCall }) {
  useElapsedTicker(true);
  const secs = elapsedSeconds(call.started_at);
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full bg-info/20 px-2 py-0.5 text-[10px] font-medium text-info"
      title={call.target_id ? `Target: ${call.target_id}` : undefined}
    >
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-info" />
      awaiting {operationLabel(call.operation)} · {secs}s
    </span>
  );
}

function PhaseProgressChip({ progress }: { progress: LivePhaseProgress }) {
  const pct =
    progress.total > 0
      ? Math.min(100, Math.round((progress.current / progress.total) * 100))
      : 0;
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-full bg-indigo-100 px-2 py-0.5 text-[10px] font-medium text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-200"
      title={`${progress.current} / ${progress.total} ${progress.label}`}
    >
      <span className="h-1.5 w-12 overflow-hidden rounded-full bg-indigo-200 dark:bg-indigo-900/60">
        <span
          className="block h-full bg-indigo-500 transition-[width] duration-300 dark:bg-indigo-400"
          style={{ width: `${pct}%` }}
        />
      </span>
      {progress.current.toLocaleString()}/{progress.total.toLocaleString()} {progress.label}
    </span>
  );
}

function LastActivityChip({ iso }: { iso: string }) {
  useElapsedTicker(true);
  const secs = elapsedSeconds(iso);
  let cls =
    "inline-flex items-center rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-medium text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-200";
  if (secs > 120) {
    cls =
      "inline-flex items-center rounded-full bg-destructive/20 px-2 py-0.5 text-[10px] font-medium text-destructive";
  } else if (secs > 30) {
    cls =
      "inline-flex items-center rounded-full bg-warning/20 px-2 py-0.5 text-[10px] font-medium text-warning";
  }
  return <span className={cls}>active {secs}s ago</span>;
}

function DreamingActivityBanner({
  cycles,
  live,
}: {
  cycles: DreamCycle[];
  live: Record<string, LiveCycleState>;
}) {
  const running = cycles.filter((c) => c.status === "running");
  if (running.length === 0) return null;

  return (
    <div className="rounded-lg border border-dashed border-info/40 bg-info/50 p-4">
      <div className="space-y-2">
        {running.slice(0, 2).map((cycle) => {
          const ls = live[cycle.id];
          const phase = ls?.phase ?? cycle.phase;
          const tokens = ls?.tokensUsed ?? cycle.tokens_used;
          const lastActivity =
            ls?.lastActivityAt ?? cycle.heartbeat_at ?? cycle.updated_at;
          return (
            <div key={cycle.id} className="flex flex-wrap items-center gap-2 text-xs">
              <span className="font-medium text-info">Cycle running</span>
              <span className="text-muted-foreground">
                {phase ? PHASE_LABELS[phase] ?? phase : "starting"}
              </span>
              {ls?.currentCall && <InFlightCallChip call={ls.currentCall} />}
              {ls?.phaseProgress && <PhaseProgressChip progress={ls.phaseProgress} />}
              {lastActivity && <LastActivityChip iso={lastActivity} />}
              <span className="font-mono text-[11px] text-muted-foreground">
                {tokens.toLocaleString()} / {cycle.token_budget.toLocaleString()} tokens
              </span>
              <span className="ml-auto font-mono text-[11px] text-muted-foreground">
                {cycle.id.slice(0, 8)}
              </span>
            </div>
          );
        })}
        {running.length > 2 && (
          <p className="text-xs text-muted-foreground">
            +{running.length - 2} more running…
          </p>
        )}
      </div>
    </div>
  );
}

function CycleTable({
  cycles,
  onSelect,
  selectedId,
  onAbandon,
  isAbandoning,
  live,
  showWriteActions = true,
  showProjectName = false,
}: {
  cycles: DreamCycle[];
  onSelect: (id: string) => void;
  selectedId: string | null;
  onAbandon: (id: string) => void;
  isAbandoning: boolean;
  live: Record<string, LiveCycleState>;
  showWriteActions?: boolean;
  // showProjectName toggles the rendering of project_name vs project_id in
  // the Project column. Only the self tier populates project_name; org and
  // system tiers leave it empty so callers see project_id only and never
  // learn the names of other users' projects.
  showProjectName?: boolean;
}) {
  if (cycles.length === 0) {
    return (
      <div className="rounded-lg border bg-card p-8 text-center text-sm text-muted-foreground">
        No dream cycles yet. Cycles will appear here once dreaming is enabled and projects have changes.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-lg border bg-card">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b bg-muted/50 text-left">
            <th className="px-4 py-3 font-medium text-muted-foreground">Status</th>
            <th className="px-4 py-3 font-medium text-muted-foreground">Project</th>
            <th className="px-4 py-3 font-medium text-muted-foreground">Phase</th>
            <th className="px-4 py-3 font-medium text-muted-foreground">Tokens</th>
            <th className="px-4 py-3 font-medium text-muted-foreground">Duration</th>
            <th className="px-4 py-3 font-medium text-muted-foreground">Started</th>
            <th className="px-4 py-3 font-medium text-muted-foreground" />
          </tr>
        </thead>
        <tbody>
          {cycles.map((cycle) => {
            const rowTint = cycle.is_abandonable
              ? "bg-destructive/40"
              : "";
            return (
              <tr
                key={cycle.id}
                onClick={() => onSelect(cycle.id)}
                className={`cursor-pointer border-b transition-colors hover:bg-muted/30 ${rowTint} ${
                  selectedId === cycle.id ? "bg-muted/50" : ""
                }`}
              >
                <td className="px-4 py-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <StatusBadge status={cycle.status} />
                    {cycle.is_abandonable ? (
                      <StuckPill />
                    ) : cycle.is_stale_diagnostic ? (
                      <StaleDiagnosticPill />
                    ) : null}
                    {cycle.status === "running" && live[cycle.id]?.currentCall && (
                      <InFlightCallChip call={live[cycle.id].currentCall!} />
                    )}
                    {cycle.status === "running" && live[cycle.id]?.phaseProgress && (
                      <PhaseProgressChip progress={live[cycle.id].phaseProgress!} />
                    )}
                    {cycle.status === "running" &&
                      (() => {
                        const ts = live[cycle.id]?.lastActivityAt ?? cycle.heartbeat_at;
                        return ts ? <LastActivityChip iso={ts} /> : null;
                      })()}
                  </div>
                </td>
                <td className="px-4 py-3 text-muted-foreground" title={cycle.project_id}>
                  {showProjectName && cycle.project_name
                    ? cycle.project_name
                    : truncateId(cycle.project_id)}
                </td>
                <td className="px-4 py-3 text-muted-foreground">
                  {(() => {
                    const phase = live[cycle.id]?.phase ?? cycle.phase;
                    return phase ? (PHASE_LABELS[phase] ?? phase) : "-";
                  })()}
                </td>
                <td className="px-4 py-3 font-mono text-xs">
                  {(live[cycle.id]?.tokensUsed ?? cycle.tokens_used).toLocaleString()} / {cycle.token_budget.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-muted-foreground">
                  {formatDuration(cycle.started_at, cycle.completed_at ?? cycle.updated_at)}
                </td>
                <td className="px-4 py-3 text-muted-foreground" title={formatDate(cycle.started_at)}>
                  {cycle.started_at ? relativeTime(cycle.started_at) : relativeTime(cycle.created_at)}
                </td>
                <td className="px-4 py-3 text-right">
                  {showWriteActions && cycle.is_abandonable ? (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        onAbandon(cycle.id);
                      }}
                      disabled={isAbandoning}
                      className="rounded-md border border-destructive/40 bg-destructive/10 px-2.5 py-1 text-xs font-medium text-destructive hover:bg-destructive/20 disabled:opacity-50"
                    >
                      Abandon
                    </button>
                  ) : (
                    <span className="text-muted-foreground">&rsaquo;</span>
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
// Cycle Detail Panel
// ---------------------------------------------------------------------------

function CycleDetail({
  cycleId,
  onClose,
  onRollback,
  isRollingBack,
  onAbandon,
  isAbandoning,
  live,
  detailIntervalMs,
  tier = "system",
  orgId,
  showWriteActions = true,
}: {
  cycleId: string;
  onClose: () => void;
  onRollback: (id: string) => void;
  isRollingBack: boolean;
  onAbandon: (id: string) => void;
  isAbandoning: boolean;
  live: Record<string, LiveCycleState>;
  detailIntervalMs?: number;
  tier?: Tier;
  orgId?: string;
  showWriteActions?: boolean;
}) {
  const { data, isLoading, isError } = useDreamingCycleDetail(cycleId, {
    intervalMs: detailIntervalMs,
    tier,
    orgId,
  });
  const [expandedPhases, setExpandedPhases] = useState<Set<string>>(() => new Set());
  const [expandedSubPhases, setExpandedSubPhases] = useState<Set<string>>(() => new Set());
  const [expandedLogs, setExpandedLogs] = useState<Set<string>>(() => new Set());

  const togglePhase = useCallback((key: string) => {
    setExpandedPhases((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);
  const toggleSubPhase = useCallback((key: string) => {
    setExpandedSubPhases((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);
  const toggleLog = useCallback((key: string) => {
    setExpandedLogs((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  // Memoized derivations live above the loading/error guards so the hook
  // count is constant across renders. The first render lands while the
  // detail query is still loading; without hoisting these, the second
  // render (data arrived) would call more hooks than the first and trip
  // React #310.
  const logGroups = useMemo(
    () => groupLogsByPhase(data?.logs ?? []),
    [data?.logs],
  );
  const orphanPhaseRows: DreamPhaseSummary[] = useMemo(() => {
    const summary = Array.isArray(data?.cycle.phase_summary)
      ? data!.cycle.phase_summary!
      : [];
    const knownPhases = new Set(summary.map((ps) => ps.phase));
    const rows: DreamPhaseSummary[] = [];
    for (const [phase, grp] of logGroups) {
      if (knownPhases.has(phase)) continue;
      const opCount =
        grp.logsFlat.length +
        grp.subGroups.reduce((sum, sg) => sum + sg.logs.length, 0);
      rows.push({
        phase,
        tokens_used: 0,
        operations: opCount,
        duration_ms: 0,
      });
    }
    return rows;
  }, [data?.cycle.phase_summary, logGroups]);
  const allPhaseRows = useMemo(() => {
    const summary = Array.isArray(data?.cycle.phase_summary)
      ? data!.cycle.phase_summary!
      : [];
    return [...summary, ...orphanPhaseRows];
  }, [data?.cycle.phase_summary, orphanPhaseRows]);

  if (isLoading) {
    return (
      <div className="rounded-lg border bg-card p-6">
        <div className="flex items-center justify-center py-8">
          <Spinner />
        </div>
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
        <p className="text-sm text-destructive">Failed to load cycle details.</p>
      </div>
    );
  }

  const { cycle } = data;
  const canRollback = cycle.status === "completed" || cycle.status === "failed";
  const canAbandon = cycle.is_abandonable;

  const phaseSummary: DreamPhaseSummary[] =
    cycle.phase_summary && Array.isArray(cycle.phase_summary)
      ? cycle.phase_summary
      : [];

  return (
    <div className="space-y-4 rounded-lg border bg-card p-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-3">
            <h3 className="text-lg font-semibold">Dream Cycle</h3>
            <StatusBadge status={cycle.status} />
            {canAbandon ? (
              <StuckPill />
            ) : cycle.is_stale_diagnostic ? (
              <StaleDiagnosticPill />
            ) : null}
          </div>
          <p className="mt-1 font-mono text-xs text-muted-foreground">{cycle.id}</p>
        </div>
        <div className="flex items-center gap-2">
          {showWriteActions && canAbandon && (
            <button
              onClick={() => onAbandon(cycle.id)}
              disabled={isAbandoning}
              className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-1.5 text-xs font-medium text-destructive hover:bg-destructive/20 disabled:opacity-50"
            >
              {isAbandoning ? "Abandoning..." : "Abandon"}
            </button>
          )}
          {showWriteActions && canRollback && (
            <button
              onClick={() => onRollback(cycle.id)}
              disabled={isRollingBack}
              className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-1.5 text-xs font-medium text-destructive hover:bg-destructive/20 disabled:opacity-50"
            >
              {isRollingBack ? "Rolling back..." : "Rollback"}
            </button>
          )}
          <button
            onClick={onClose}
            className="rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground hover:bg-muted"
          >
            Close
          </button>
        </div>
      </div>

      {/* Metadata */}
      <div className="grid grid-cols-2 gap-4 text-sm md:grid-cols-4">
        <div>
          <p className="text-muted-foreground">Started</p>
          <p className="font-medium">{formatDate(cycle.started_at)}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Completed</p>
          <p className="font-medium">{formatDate(cycle.completed_at)}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Tokens Used</p>
          <p className="font-mono font-medium">
            {(live[cycleId]?.tokensUsed ?? cycle.tokens_used).toLocaleString()} / {cycle.token_budget.toLocaleString()}
          </p>
        </div>
        <div>
          <p className="text-muted-foreground">Duration</p>
          <p className="font-medium">
            {formatDuration(cycle.started_at, cycle.completed_at ?? cycle.updated_at)}
          </p>
        </div>
      </div>

      {/* Live activity timeline (only while the cycle is running). */}
      {cycle.status === "running" && (
        <LiveActivitySection state={live[cycleId]} />
      )}

      {cycle.error && <ExtractionErrorView value={cycle.error} variant="block" />}

      {/* Phase Summary: also surfaces orphan-phase rows for logs whose
          phase hasn't been written to cycle.phase_summary yet (in-flight or
          aborted cycles). */}
      {allPhaseRows.length > 0 && (
        <div>
          {phaseSummary.length > 0 && (
            <div className="mb-4">
              <h4 className="mb-2 text-sm font-semibold text-muted-foreground">
                Token Budget · {formatNumber(cycle.tokens_used)} of {formatNumber(cycle.token_budget)}
              </h4>
              <PhaseBudgetBar
                segments={phaseSummary.map((ps) => ({
                  key: ps.phase,
                  value: ps.tokens_used,
                  cap: ps.slice_cap,
                  hasResidual: ps.has_residual,
                }))}
                total={cycle.token_budget}
                format={formatNumber}
                ariaLabel="Cycle token usage by phase"
              />
            </div>
          )}
          <h4 className="mb-2 text-sm font-semibold text-muted-foreground">Phase Summary</h4>
          <div className="overflow-x-auto rounded-md border">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50 text-left">
                  <th className="px-3 py-2 font-medium text-muted-foreground">Phase</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Tokens</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Ops</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Time</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Residual</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Status</th>
                </tr>
              </thead>
              <tbody>
                {allPhaseRows.map((ps, i) => (
                  <PhaseSummaryRowExpandable
                    key={ps.phase + ":" + i}
                    ps={ps}
                    group={logGroups.get(ps.phase)}
                    pending={i >= phaseSummary.length}
                    expandedPhases={expandedPhases}
                    expandedSubPhases={expandedSubPhases}
                    expandedLogs={expandedLogs}
                    togglePhase={togglePhase}
                    toggleSubPhase={toggleSubPhase}
                    toggleLog={toggleLog}
                  />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function LiveActivitySection({ state }: { state?: LiveCycleState }) {
  // Re-render the in-flight elapsed counter every second.
  useElapsedTicker(!!state?.currentCall);

  if (!state) {
    return (
      <div className="rounded-md border border-info/40 bg-info/40 p-3 text-xs text-muted-foreground">
        Waiting for live activity… (events will appear here as the runner emits them)
      </div>
    );
  }

  const { currentCall, recentCalls } = state;
  return (
    <div className="rounded-md border border-info/40 bg-info/40 p-3">
      <h4 className="mb-2 text-sm font-semibold text-info">
        Live Activity
      </h4>

      {currentCall && (
        <div className="mb-2 flex items-center gap-2 text-xs">
          <span className="h-2 w-2 animate-pulse rounded-full bg-info" />
          <span className="font-medium">In flight:</span>
          <span>{operationLabel(currentCall.operation)}</span>
          <span className="font-mono text-[11px] text-muted-foreground">
            {elapsedSeconds(currentCall.started_at)}s
          </span>
          {currentCall.model && (
            <span className="text-muted-foreground">{currentCall.model}</span>
          )}
        </div>
      )}

      {recentCalls.length === 0 ? (
        <p className="text-xs text-muted-foreground">No calls observed yet.</p>
      ) : (
        <div className="max-h-64 space-y-1 overflow-y-auto">
          {recentCalls.map((c) => {
            const finished = c.ended_at !== undefined;
            const status = !finished ? "running" : c.ok ? "ok" : "error";
            const statusCls =
              status === "running"
                ? "text-info"
                : status === "ok"
                ? "text-success"
                : "text-destructive";
            return (
              <div
                key={c.call_id}
                className="flex flex-wrap items-center gap-2 rounded bg-white/40 px-2 py-1 text-xs dark:bg-black/20"
              >
                <span className="font-mono text-[11px] text-muted-foreground">
                  {new Date(c.started_at).toLocaleTimeString()}
                </span>
                <span className="font-medium">{operationLabel(c.operation)}</span>
                <span className={statusCls}>{status}</span>
                {finished && c.latency_ms !== undefined && (
                  <span className="font-mono text-[11px] text-muted-foreground">
                    {c.latency_ms < 1000
                      ? `${c.latency_ms}ms`
                      : `${(c.latency_ms / 1000).toFixed(1)}s`}
                  </span>
                )}
                {c.tokens && (
                  <span className="font-mono text-[11px] text-muted-foreground">
                    {c.tokens.total.toLocaleString()} tok
                  </span>
                )}
                {c.error && (
                  <span className="truncate text-[11px] text-destructive">
                    {c.error}
                  </span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// Memory-typed Fact kinds get clickable router links into MemoryBrowser;
// other ID kinds copy the full UUID to the clipboard on click. Plain values
// run through formatFactValue.
function FactChip({ projectId, fact: f }: { projectId: string; fact: Fact }) {
  const [copied, setCopied] = useState(false);
  const value = String(f.value ?? "");
  if (!value) return <span className="text-muted-foreground">{"-"}</span>;

  if (f.kind === "memory_id" || f.kind === "memory_id_superseded") {
    const superseded = f.kind === "memory_id_superseded";
    return (
      <Link
        to={memoryFocusHref(projectId, value, {
          includeSuperseded: superseded,
        })}
        className="rounded bg-muted/60 px-1.5 py-0.5 font-mono text-[11px] text-info hover:bg-muted/80 hover:underline"
        title={superseded ? `${value} (merged away)` : value}
      >
        {shortId(value)}
      </Link>
    );
  }

  if (f.kind === "memory_id_deleted") {
    return (
      <span
        className="rounded bg-muted/60 px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground"
        title={`${value} (deleted)`}
      >
        {shortId(value)}
      </span>
    );
  }

  if (
    f.kind === "entity_id" ||
    f.kind === "relationship_id" ||
    f.kind === "namespace_id"
  ) {
    return (
      <button
        type="button"
        onClick={async (e) => {
          e.stopPropagation();
          const ok = await copyToClipboard(value);
          if (!ok) return;
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1200);
        }}
        className="rounded bg-muted/60 px-1.5 py-0.5 font-mono text-[11px] text-foreground hover:bg-muted/80"
        title={copied ? "Copied!" : value}
      >
        {copied ? "copied" : shortId(value)}
      </button>
    );
  }

  if (f.kind === "text" && value.length > 80) {
    return (
      <span className="text-xs text-foreground" title={value}>
        {value.slice(0, 80)}
        {"\u2026"}
      </span>
    );
  }

  return <span className="text-xs text-foreground">{formatFactValue(f)}</span>;
}

// renderNarrative substitutes {placeholder} tokens in the narrative string
// with rendered Fact chips. Tokens with no matching fact are dropped (so
// optional fields like {alignment} don't leave a literal "{alignment}"
// when the data is absent).
function renderNarrative(
  narrative: string,
  facts: Record<string, Fact>,
  projectId: string,
): ReactNode {
  const parts: ReactNode[] = [];
  const re = /\{(\w+)\}/g;
  let lastIndex = 0;
  let match: RegExpExecArray | null;
  let key = 0;
  while ((match = re.exec(narrative)) !== null) {
    if (match.index > lastIndex) {
      parts.push(narrative.slice(lastIndex, match.index));
    }
    const f = facts[match[1]];
    if (f !== undefined) {
      parts.push(<FactChip key={key++} projectId={projectId} fact={f} />);
    }
    lastIndex = match.index + match[0].length;
  }
  if (lastIndex < narrative.length) {
    parts.push(narrative.slice(lastIndex));
  }
  // Collapse double spaces left by dropped placeholders.
  return parts.map((p, i) =>
    typeof p === "string" ? <span key={`s${i}`}>{p.replace(/\s{2,}/g, " ")}</span> : p,
  );
}

function LogEntry({
  log,
  expanded,
  onToggle,
}: {
  log: DreamLog;
  expanded: boolean;
  onToggle: () => void;
}) {
  const [showRaw, setShowRaw] = useState(false);
  const formatted: FormattedLog = useMemo(() => formatDreamLog(log), [log]);
  const projectId = log.project_id;

  // Facts referenced inline by the narrative don't need to repeat in the
  // key-fact strip below.
  const inlineKeys = useMemo(() => {
    const keys = new Set<string>();
    const re = /\{(\w+)\}/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(formatted.narrative)) !== null) keys.add(m[1]);
    return keys;
  }, [formatted.narrative]);

  const tailFacts = Object.entries(formatted.facts).filter(
    ([k]) => !inlineKeys.has(k),
  );

  const showTargetChip =
    !formatted.isSummary &&
    !isZeroId(log.target_id) &&
    log.target_type !== "namespace";

  const headerContent = (
    <>
      <span className="text-xs text-muted-foreground">
        {PHASE_LABELS[log.phase] ?? log.phase}
      </span>
      <span className={`font-medium ${OP_COLORS[log.operation] ?? "text-foreground"}`}>
        {log.operation.replace(/_/g, " ")}
      </span>
      {!formatted.isSummary && (
        <span className="text-xs text-muted-foreground">{log.target_type}</span>
      )}
      {showTargetChip && (
        <span
          className="rounded bg-muted/60 px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground"
          title={log.target_id}
        >
          {shortId(log.target_id)}
        </span>
      )}
      {formatted.isSummary && formatted.metrics && formatted.metrics.length > 0 && (
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
          {formatted.metrics.map((m, i) => (
            <span key={i}>
              <span className="text-muted-foreground">{m.label.toLowerCase()}</span>{" "}
              <span className="font-mono text-foreground">{formatFactValue(m)}</span>
              {i < formatted.metrics!.length - 1 ? <span> ·</span> : null}
            </span>
          ))}
        </span>
      )}
      {!formatted.isSummary && (
        <FontAwesomeIcon
          icon={expanded ? faChevronDown : faChevronRight}
          className="ml-auto h-3.5 w-3.5 text-foreground transition-transform"
          aria-hidden="true"
        />
      )}
    </>
  );

  const headerCls =
    "flex w-full flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2 text-left";

  return (
    <div className="rounded-md border text-sm">
      {formatted.isSummary ? (
        <div className={headerCls}>{headerContent}</div>
      ) : (
        <button
          type="button"
          onClick={onToggle}
          aria-expanded={expanded}
          className={`${headerCls} hover:bg-muted/30`}
        >
          {headerContent}
        </button>
      )}
      {expanded && !formatted.isSummary && (
        <div className="space-y-2 border-t px-3 py-2">
          <p className="text-sm leading-relaxed">
            {formatted.unknown ? (
              <span className="text-muted-foreground">
                Unknown operation, see raw payload below.
              </span>
            ) : (
              renderNarrative(formatted.narrative, formatted.facts, projectId)
            )}
          </p>
          {tailFacts.length > 0 && (
            <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
              {tailFacts.map(([k, f]) => (
                <span key={k} className="flex items-center gap-1">
                  <span className="text-muted-foreground">{f.label}:</span>
                  <FactChip projectId={projectId} fact={f} />
                </span>
              ))}
            </div>
          )}
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              setShowRaw((v) => !v);
            }}
            className="text-xs text-muted-foreground underline-offset-2 hover:text-foreground hover:underline"
          >
            {showRaw ? "Hide raw JSON" : "Show raw JSON"}
          </button>
          {showRaw && (
            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
              <div>
                <p className="mb-1 text-xs font-medium text-muted-foreground">Before</p>
                <pre className="max-h-40 overflow-auto rounded bg-muted/50 p-2 font-mono text-xs">
                  {JSON.stringify(log.before_state, null, 2)}
                </pre>
              </div>
              <div>
                <p className="mb-1 text-xs font-medium text-muted-foreground">After</p>
                <pre className="max-h-40 overflow-auto rounded bg-muted/50 p-2 font-mono text-xs">
                  {JSON.stringify(log.after_state, null, 2)}
                </pre>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function DreamingMonitor() {
  const { isAdmin, isOrgOwner, user } = useAuth();
  const orgId = user?.org_id;
  const { live, connected } = useDreamingLiveState(orgId);
  const statusIntervalMs = connected ? 10_000 : 3_000;
  const cyclesIntervalMs = connected ? 15_000 : 5_000;

  // Default to self-tier for everyone. The TierTabs picker only renders
  // additional tiers when the caller's role grants them.
  const [tier, setTier] = useState<Tier>("self");

  // System-tier (admin only): live system status + system-wide cycles list.
  // Both queries are gated by `enabled` rather than just refetchInterval:
  // refetchInterval=0 only stops polling, the initial fetch still fires.
  const systemStatusQuery = useDreamingStatus({
    intervalMs: statusIntervalMs,
    enabled: tier === "system",
  });
  const systemCyclesQuery = useDreamingCycles(undefined, {
    intervalMs: cyclesIntervalMs,
    tier: "system",
    enabled: tier === "system",
  });

  // Org-tier (org_owner+admin): aggregate org-wide status + cycles.
  const orgStatusQuery = useOrgDreamingStatus(orgId, {
    intervalMs: statusIntervalMs,
    enabled: tier === "org",
  });
  const orgCyclesQuery = useDreamingCycles(undefined, {
    intervalMs: cyclesIntervalMs,
    tier: "org",
    orgId,
    enabled: tier === "org",
  });

  // Self-tier: aggregate "any-of-mine-dirty" status + cycles across all of
  // the caller's projects. The project picker has been removed in favor of
  // a unified all-projects list; the per-project status panel is replaced
  // by a rolled-up dirty/quiet badge.
  const selfStatusQuery = useMyDreamingAggregateStatus({
    intervalMs: statusIntervalMs,
    enabled: tier === "self",
  });
  const selfCyclesQuery = useDreamingCycles(undefined, {
    intervalMs: cyclesIntervalMs,
    tier: "self",
    enabled: tier === "self",
  });

  const enableMutation = useSetDreamingEnabled();
  const rollbackMutation = useRollbackDreamCycle({ tier, orgId });
  const abandonMutation = useAbandonDreamCycle({ tier, orgId });

  const [selectedCycleId, setSelectedCycleId] = useState<string | null>(null);
  const [toast, setToast] = useState<{ message: string; type: "success" | "error" } | null>(null);

  const showToast = useCallback((message: string, type: "success" | "error") => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  }, []);

  const handleToggleEnabled = useCallback(
    (enabled: boolean) => {
      enableMutation.mutate(enabled, {
        onSuccess: () => showToast(`Dreaming ${enabled ? "enabled" : "disabled"}`, "success"),
        onError: () => showToast("Failed to update dreaming state", "error"),
      });
    },
    [enableMutation, showToast],
  );

  const handleRollback = useCallback(
    (cycleId: string) => {
      if (!window.confirm("Are you sure you want to rollback this dream cycle? This will reverse all operations performed during this cycle.")) {
        return;
      }
      rollbackMutation.mutate(cycleId, {
        onSuccess: () => {
          showToast("Dream cycle rolled back successfully", "success");
          setSelectedCycleId(null);
        },
        onError: () => showToast("Rollback failed", "error"),
      });
    },
    [rollbackMutation, showToast],
  );

  const handleAbandon = useCallback(
    (cycleId: string) => {
      if (!window.confirm(ABANDON_CONFIRM)) {
        return;
      }
      abandonMutation.mutate(cycleId, {
        onSuccess: () => {
          showToast("Dream cycle abandoned; rollback is now available", "success");
        },
        onError: (err: unknown) => {
          // 409 from the server when the cycle has already finished or been
          // abandoned by another operator/sweep cycle.
          const message = err instanceof Error ? err.message : "";
          if (message.toLowerCase().includes("terminal") || message.includes("409")) {
            showToast("Cycle is already in a terminal state", "error");
          } else {
            showToast("Abandon failed", "error");
          }
        },
      });
    },
    [abandonMutation, showToast],
  );

  // Tier-normalized view: self has no dirty/stuck counters in its response
  // shape, so derive them from the aggregate dirty boolean and the cycles
  // list. enabledFlag is system-only; self/org render an aggregate dirty
  // pill instead and read their own status query directly.
  const view = (() => {
    if (tier === "system") {
      const s = systemStatusQuery.data;
      return {
        cycles: systemCyclesQuery.data ?? [],
        dirtyCount: s?.dirty_count ?? 0,
        stuckCount: s?.stuck_count ?? 0,
        isLoading: systemStatusQuery.isLoading,
        isError: systemStatusQuery.isError,
      };
    }
    if (tier === "org") {
      const s = orgStatusQuery.data;
      return {
        cycles: orgCyclesQuery.data ?? [],
        dirtyCount: s?.dirty_count ?? 0,
        stuckCount: s?.stuck_count ?? 0,
        isLoading: orgStatusQuery.isLoading,
        isError: orgStatusQuery.isError,
      };
    }
    const s = selfStatusQuery.data;
    const cycles = selfCyclesQuery.data ?? [];
    return {
      cycles,
      dirtyCount: s?.dirty_count ?? 0,
      stuckCount: cycles.filter((c) => c.is_abandonable).length,
      isLoading: selfStatusQuery.isLoading,
      isError: selfStatusQuery.isError,
    };
  })();
  const { cycles, dirtyCount, stuckCount, isLoading, isError } = view;
  const enabledFlag = tier === "system" ? (systemStatusQuery.data?.enabled ?? false) : false;

  const { runningCount, completedCount, failedCount } = useMemo(() => ({
    runningCount: cycles.filter((c) => c.status === "running").length,
    completedCount: cycles.filter((c) => c.status === "completed").length,
    failedCount: cycles.filter((c) => c.status === "failed").length,
  }), [cycles]);

  const showWriteActions =
    (tier === "self") ||
    (tier === "org" && isOrgOwner) ||
    (tier === "system" && isAdmin);
  const dirtyLabel = tier === "self" ? "Dirty" : "Dirty Projects";

  const handleTierChange = useCallback((next: Tier) => {
    setTier(next);
    setSelectedCycleId(null);
  }, []);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="font-display text-3xl text-foreground">
            {tier === "system"
              ? "Dreaming"
              : tier === "org"
                ? "Org Dreaming"
                : "My Dreaming"}
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {tier === "system"
              ? "Background memory consolidation and knowledge graph improvement. Dreaming runs automatically when enrichment is idle and projects have new changes."
              : tier === "org"
                ? "Dream cycles for your organization's projects. Abandon/rollback within the org; the global enable/disable toggle is administrator-only."
                : "Dream cycles for projects you own. Abandon/rollback your own cycles; the global enable/disable toggle is administrator-only."}
          </p>
        </div>
        <TierTabs current={tier} onChange={handleTierChange} ariaLabel="Dreaming scope" />
      </div>

      {/* Loading */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <Spinner />
        </div>
      )}

      {/* Error */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
          <p className="text-sm text-destructive">
            Failed to load dreaming status.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (
        <>
          {/* Controls: system tier renders the enable/disable toggle; */}
          {/* self tier shows the rolled-up "any-of-mine-dirty" badge plus */}
          {/* the count of caller-owned projects. */}
          <div className="flex items-center justify-between rounded-lg border bg-card p-4">
            <div className="flex items-center gap-3">
              <span className="text-sm font-medium">Dreaming</span>
              {tier === "system" ? (
                <>
                  <Switch
                    checked={enabledFlag}
                    onChange={handleToggleEnabled}
                    disabled={enableMutation.isPending}
                  />
                  <span className="text-sm text-muted-foreground">
                    {enabledFlag ? "Enabled" : "Disabled"}
                  </span>
                </>
              ) : (
                <>
                  <span
                    className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${
                      dirtyCount > 0
                        ? "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300"
                        : "bg-success/10 text-success"
                    }`}
                  >
                    {dirtyCount > 0 ? "Dirty" : "All quiet"}
                  </span>
                  {tier === "self" && (
                    <span className="text-sm text-muted-foreground">
                      {selfStatusQuery.data?.project_count ?? 0}
                      {" "}
                      {selfStatusQuery.data?.project_count === 1 ? "project" : "projects"}
                    </span>
                  )}
                </>
              )}
            </div>
            <p className="text-xs text-muted-foreground">
              {connected ? "Live updates connected" : `Polling every ${statusIntervalMs / 1000}s`}
            </p>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
            <StatCard
              label={dirtyLabel}
              value={dirtyCount}
              color={
                dirtyCount > 0
                  ? "text-yellow-600 dark:text-yellow-400"
                  : "text-muted-foreground"
              }
            />
            <StatCard
              label="Active"
              value={runningCount}
              color={runningCount > 0 ? "text-info" : "text-muted-foreground"}
            />
            <StatCard
              label="Stuck"
              value={stuckCount}
              color={
                stuckCount > 0
                  ? "text-destructive"
                  : "text-muted-foreground"
              }
            />
            <StatCard
              label="Completed"
              value={completedCount}
              color="text-success"
            />
            <StatCard
              label="Failed"
              value={failedCount}
              color={failedCount > 0 ? "text-destructive" : "text-muted-foreground"}
            />
          </div>

          {/* Top-of-page live banner whenever a cycle is running. */}
          <DreamingActivityBanner cycles={cycles} live={live} />

          {/* Detail or List */}
          {selectedCycleId ? (
            <CycleDetail
              cycleId={selectedCycleId}
              onClose={() => setSelectedCycleId(null)}
              onRollback={handleRollback}
              isRollingBack={rollbackMutation.isPending}
              onAbandon={handleAbandon}
              isAbandoning={abandonMutation.isPending}
              live={live}
              tier={tier}
              orgId={orgId}
              showWriteActions={showWriteActions}
              detailIntervalMs={
                cycles.find((c) => c.id === selectedCycleId)?.status === "running"
                  ? 5_000
                  : undefined
              }
            />
          ) : (
            <div>
              <h2 className="mb-3 text-lg font-semibold">Dream Cycles</h2>
              <CycleTable
                cycles={cycles}
                onSelect={setSelectedCycleId}
                selectedId={selectedCycleId}
                onAbandon={handleAbandon}
                isAbandoning={abandonMutation.isPending}
                live={live}
                showWriteActions={showWriteActions}
                showProjectName={tier === "self"}
              />
            </div>
          )}
        </>
      )}

      {/* Toast */}
      {toast && <StatusToast message={toast.message} type={toast.type} />}
    </div>
  );
}
