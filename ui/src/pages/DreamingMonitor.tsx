import { useState, useCallback, useEffect, useMemo } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  useDreamingStatus,
  useDreamingCycles,
  useDreamingCycleDetail,
  useMyDreamingProjectStatus,
  useMeProjects,
  useSetDreamingEnabled,
  useRollbackDreamCycle,
  useAbandonDreamCycle,
} from "../hooks/useApi";
import { useEventStream } from "../hooks/useEventStream";
import { useElapsedTicker, elapsedSeconds } from "../hooks/useElapsedTicker";
import { useAuth } from "../context/AuthContext";
import type { DreamCycle, DreamLog } from "../api/client";

// Tier picker — administrators choose between their own per-project view
// (the "Mine" tab, /v1/me/dreaming) and the cross-tenant pipeline view
// (the "System" tab, /v1/admin/dreaming). Non-admin callers are pinned
// to "self"; the only data they may see is their own projects' cycles.
type DreamingTier = "self" | "system";

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
  running: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300",
  completed: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300",
  failed: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
  rolled_back: "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300",
};

const PHASE_LABELS: Record<string, string> = {
  entity_dedup: "Entity Dedup",
  transitive_discovery: "Transitive Discovery",
  contradiction_detection: "Contradiction Detection",
  consolidation: "Consolidation",
  pruning: "Pruning",
  weight_adjustment: "Weight Adjustment",
};

const OP_COLORS: Record<string, string> = {
  entity_merged: "text-blue-600 dark:text-blue-400",
  relationship_created: "text-green-600 dark:text-green-400",
  contradiction_detected: "text-orange-600 dark:text-orange-400",
  memory_created: "text-emerald-600 dark:text-emerald-400",
  confidence_adjusted: "text-cyan-600 dark:text-cyan-400",
  memory_superseded: "text-purple-600 dark:text-purple-400",
  memory_deleted: "text-red-600 dark:text-red-400",
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

// ---------------------------------------------------------------------------
// Live state via SSE
// ---------------------------------------------------------------------------

function useDreamingLiveState() {
  const qc = useQueryClient();
  const [live, setLive] = useState<Record<string, LiveCycleState>>({});

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
                // Reset progress on phase boundary — a fresh phase starts at 0/0.
                phaseProgress: undefined,
              },
            };
          });
          // Authoritative refresh on phase boundaries (cheap, only every ~Ns).
          // Invalidate both tiers — the active tier is unknown to the SSE
          // hook, so refresh both admin-side and self-side caches.
          qc.invalidateQueries({ queryKey: ["admin", "dreaming", "cycles"] });
          qc.invalidateQueries({ queryKey: ["me", "dreaming", "cycles"] });
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
          qc.invalidateQueries({ queryKey: ["admin", "dreaming"] });
          qc.invalidateQueries({ queryKey: ["admin", "dreaming", "cycles"] });
          qc.invalidateQueries({ queryKey: ["me", "dreaming", "project"] });
          qc.invalidateQueries({ queryKey: ["me", "dreaming", "cycles"] });
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
  return (
    <svg className="h-5 w-5 animate-spin text-muted-foreground" viewBox="0 0 24 24" fill="none">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
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
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${STATUS_COLORS[status] ?? "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300"}`}>
      {status.replace(/_/g, " ")}
    </span>
  );
}

function Toggle({
  enabled,
  onChange,
  disabled,
}: {
  enabled: boolean;
  onChange: (val: boolean) => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={enabled}
      disabled={disabled}
      onClick={() => onChange(!enabled)}
      className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 ${enabled ? "bg-green-600" : "bg-muted"}`}
    >
      <span
        className={`pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ${enabled ? "translate-x-5" : "translate-x-0"}`}
      />
    </button>
  );
}

function StatusToast({ message, type }: { message: string; type: "success" | "error" }) {
  return (
    <div
      className={`fixed bottom-4 right-4 z-50 flex items-center gap-2 rounded-md px-4 py-2.5 text-sm font-medium shadow-lg transition-all ${
        type === "success"
          ? "bg-green-50 text-green-800 dark:bg-green-900/50 dark:text-green-200"
          : "bg-red-50 text-red-800 dark:bg-red-900/50 dark:text-red-200"
      }`}
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
    <span className="inline-flex items-center rounded-full bg-red-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-red-800 dark:bg-red-900/40 dark:text-red-200">
      stuck
    </span>
  );
}

function StaleDiagnosticPill() {
  return (
    <span
      className="inline-flex items-center rounded-full bg-amber-100/70 px-2 py-0.5 text-[10px] font-medium text-amber-800 dark:bg-amber-900/30 dark:text-amber-200"
      title="Heartbeat is stale — the worker may have stopped making progress."
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
      className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-2 py-0.5 text-[10px] font-medium text-blue-800 dark:bg-blue-900/40 dark:text-blue-200"
      title={call.target_id ? `Target: ${call.target_id}` : undefined}
    >
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-blue-600 dark:bg-blue-400" />
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
      "inline-flex items-center rounded-full bg-red-100 px-2 py-0.5 text-[10px] font-medium text-red-800 dark:bg-red-900/30 dark:text-red-200";
  } else if (secs > 30) {
    cls =
      "inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-medium text-amber-800 dark:bg-amber-900/30 dark:text-amber-200";
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
    <div className="rounded-lg border border-dashed border-blue-300 bg-blue-50/50 p-4 dark:border-blue-800 dark:bg-blue-900/20">
      <div className="space-y-2">
        {running.slice(0, 2).map((cycle) => {
          const ls = live[cycle.id];
          const phase = ls?.phase ?? cycle.phase;
          const tokens = ls?.tokensUsed ?? cycle.tokens_used;
          const lastActivity =
            ls?.lastActivityAt ?? cycle.heartbeat_at ?? cycle.updated_at;
          return (
            <div key={cycle.id} className="flex flex-wrap items-center gap-2 text-xs">
              <span className="font-medium text-blue-900 dark:text-blue-200">Cycle running</span>
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
}: {
  cycles: DreamCycle[];
  onSelect: (id: string) => void;
  selectedId: string | null;
  onAbandon: (id: string) => void;
  isAbandoning: boolean;
  live: Record<string, LiveCycleState>;
  showWriteActions?: boolean;
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
              ? "bg-red-50/40 dark:bg-red-900/10"
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
                      className="rounded-md border border-red-300 bg-red-50 px-2.5 py-1 text-xs font-medium text-red-700 hover:bg-red-100 disabled:opacity-50 dark:border-red-800 dark:bg-red-900/30 dark:text-red-300 dark:hover:bg-red-900/50"
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
  tier?: DreamingTier;
  showWriteActions?: boolean;
}) {
  const { data, isLoading, isError } = useDreamingCycleDetail(cycleId, {
    intervalMs: detailIntervalMs,
    tier,
  });
  const [expandedLog, setExpandedLog] = useState<string | null>(null);

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
      <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-900/30">
        <p className="text-sm text-red-800 dark:text-red-300">Failed to load cycle details.</p>
      </div>
    );
  }

  const { cycle, logs } = data;
  const canRollback = cycle.status === "completed" || cycle.status === "failed";
  const canAbandon = cycle.is_abandonable;

  // Parse phase summary if available.
  let phaseSummary: Array<{
    phase: string;
    tokens_used: number;
    operations: number;
    duration_ms: number;
    error?: string;
    skipped?: boolean;
  }> = [];
  if (cycle.phase_summary && Array.isArray(cycle.phase_summary)) {
    phaseSummary = cycle.phase_summary as typeof phaseSummary;
  }

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
              className="rounded-md border border-red-300 bg-red-50 px-3 py-1.5 text-xs font-medium text-red-700 hover:bg-red-100 disabled:opacity-50 dark:border-red-800 dark:bg-red-900/30 dark:text-red-300 dark:hover:bg-red-900/50"
            >
              {isAbandoning ? "Abandoning..." : "Abandon"}
            </button>
          )}
          {showWriteActions && canRollback && (
            <button
              onClick={() => onRollback(cycle.id)}
              disabled={isRollingBack}
              className="rounded-md border border-red-300 bg-red-50 px-3 py-1.5 text-xs font-medium text-red-700 hover:bg-red-100 disabled:opacity-50 dark:border-red-800 dark:bg-red-900/30 dark:text-red-300 dark:hover:bg-red-900/50"
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

      {cycle.error && (
        <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-900/30 dark:text-red-300">
          {cycle.error}
        </div>
      )}

      {/* Phase Summary */}
      {phaseSummary.length > 0 && (
        <div>
          <h4 className="mb-2 text-sm font-semibold text-muted-foreground">Phase Summary</h4>
          <div className="overflow-x-auto rounded-md border">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50 text-left">
                  <th className="px-3 py-2 font-medium text-muted-foreground">Phase</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Tokens</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Ops</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Time</th>
                  <th className="px-3 py-2 font-medium text-muted-foreground">Status</th>
                </tr>
              </thead>
              <tbody>
                {phaseSummary.map((ps, i) => (
                  <tr key={i} className="border-b last:border-0">
                    <td className="px-3 py-2">{PHASE_LABELS[ps.phase] ?? ps.phase}</td>
                    <td className="px-3 py-2 font-mono text-xs">{ps.tokens_used.toLocaleString()}</td>
                    <td className="px-3 py-2 font-mono text-xs">{ps.operations}</td>
                    <td className="px-3 py-2 text-muted-foreground">
                      {ps.duration_ms < 1000 ? `${ps.duration_ms}ms` : `${(ps.duration_ms / 1000).toFixed(1)}s`}
                    </td>
                    <td className="px-3 py-2">
                      {ps.skipped ? (
                        <span className="text-xs text-muted-foreground">skipped</span>
                      ) : ps.error ? (
                        <span className="text-xs text-red-600 dark:text-red-400">{ps.error}</span>
                      ) : (
                        <span className="text-xs text-green-600 dark:text-green-400">ok</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Operation Log */}
      <div>
        <h4 className="mb-2 text-sm font-semibold text-muted-foreground">
          Operations ({logs.length})
        </h4>
        {logs.length === 0 ? (
          <p className="text-sm text-muted-foreground">No operations recorded.</p>
        ) : (
          <div className="max-h-96 space-y-1 overflow-y-auto">
            {logs.map((log) => (
              <LogEntry
                key={log.id}
                log={log}
                expanded={expandedLog === log.id}
                onToggle={() => setExpandedLog(expandedLog === log.id ? null : log.id)}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function LiveActivitySection({ state }: { state?: LiveCycleState }) {
  // Re-render the in-flight elapsed counter every second.
  useElapsedTicker(!!state?.currentCall);

  if (!state) {
    return (
      <div className="rounded-md border border-blue-200 bg-blue-50/40 p-3 text-xs text-muted-foreground dark:border-blue-900/40 dark:bg-blue-900/10">
        Waiting for live activity… (events will appear here as the runner emits them)
      </div>
    );
  }

  const { currentCall, recentCalls } = state;
  return (
    <div className="rounded-md border border-blue-200 bg-blue-50/40 p-3 dark:border-blue-900/40 dark:bg-blue-900/10">
      <h4 className="mb-2 text-sm font-semibold text-blue-900 dark:text-blue-200">
        Live Activity
      </h4>

      {currentCall && (
        <div className="mb-2 flex items-center gap-2 text-xs">
          <span className="h-2 w-2 animate-pulse rounded-full bg-blue-600 dark:bg-blue-400" />
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
                ? "text-blue-600 dark:text-blue-400"
                : status === "ok"
                ? "text-green-600 dark:text-green-400"
                : "text-red-600 dark:text-red-400";
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
                  <span className="truncate text-[11px] text-red-600 dark:text-red-400">
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

function LogEntry({
  log,
  expanded,
  onToggle,
}: {
  log: DreamLog;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="rounded-md border text-sm">
      <button
        onClick={onToggle}
        className="flex w-full items-center gap-3 px-3 py-2 text-left hover:bg-muted/30"
      >
        <span className="text-xs text-muted-foreground">{PHASE_LABELS[log.phase] ?? log.phase}</span>
        <span className={`font-medium ${OP_COLORS[log.operation] ?? "text-foreground"}`}>
          {log.operation.replace(/_/g, " ")}
        </span>
        <span className="text-xs text-muted-foreground">
          {log.target_type}
        </span>
        <span className="ml-auto text-xs text-muted-foreground">
          {expanded ? "\u25B2" : "\u25BC"}
        </span>
      </button>
      {expanded && (
        <div className="border-t px-3 py-2">
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
          <p className="mt-2 font-mono text-xs text-muted-foreground">
            Target: {log.target_id}
          </p>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function DreamingMonitor() {
  const { live, connected } = useDreamingLiveState();
  const statusIntervalMs = connected ? 10_000 : 3_000;
  const cyclesIntervalMs = connected ? 15_000 : 5_000;

  const { isAdmin } = useAuth();

  // Default to self-tier for everyone — admin starts on "Mine" per the
  // 2026-04-30 privacy plan and can switch to "System" via the tab picker.
  const [tier, setTier] = useState<DreamingTier>("self");

  // Self-tier requires a project_id; we let the user pick from their own
  // projects. Defaulting to the first project lets the page render
  // immediately without an empty intermediate state.
  const projectsQuery = useMeProjects();
  const projects = projectsQuery.data ?? [];
  const [selectedProjectId, setSelectedProjectId] = useState<string | null>(null);
  useEffect(() => {
    setSelectedProjectId((cur) => cur ?? projects[0]?.id ?? null);
  }, [projects]);

  // System-tier (admin only): live system status + system-wide cycles list.
  // Both queries are gated by `enabled` rather than just refetchInterval —
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

  // Self-tier: per-project status + cycles list scoped to the selected
  // project. Both endpoints require a project_id and the server checks
  // ownership against the caller's namespace.
  const selfStatusQuery = useMyDreamingProjectStatus(
    tier === "self" ? selectedProjectId : null,
    { intervalMs: statusIntervalMs },
  );
  const selfCyclesQuery = useDreamingCycles(
    tier === "self" ? (selectedProjectId ?? undefined) : undefined,
    { intervalMs: cyclesIntervalMs, tier: "self" },
  );

  const enableMutation = useSetDreamingEnabled();
  const rollbackMutation = useRollbackDreamCycle();
  const abandonMutation = useAbandonDreamCycle();

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

  // Tier-normalized view of the status + cycles. Self-tier has no
  // dirty/stuck counters in its response shape; we derive a comparable
  // pair from the per-project boolean and the cycles list.
  const view = (() => {
    if (tier === "system") {
      const s = systemStatusQuery.data;
      const cycles = systemCyclesQuery.data ?? [];
      return {
        cycles,
        dirtyCount: s?.dirty_count ?? 0,
        stuckCount: s?.stuck_count ?? 0,
        enabledFlag: s?.enabled ?? false,
        isLoading: systemStatusQuery.isLoading,
        isError: systemStatusQuery.isError,
      };
    }
    const s = selfStatusQuery.data;
    const cycles = selfCyclesQuery.data ?? [];
    return {
      cycles,
      dirtyCount: s?.dirty ? 1 : 0,
      stuckCount: cycles.filter((c) => c.is_abandonable).length,
      enabledFlag: s?.enabled ?? false,
      isLoading:
        projectsQuery.isLoading || (!!selectedProjectId && selfStatusQuery.isLoading),
      isError: selfStatusQuery.isError,
    };
  })();
  const { cycles, dirtyCount, stuckCount, enabledFlag, isLoading, isError } = view;

  const { runningCount, completedCount, failedCount } = useMemo(() => ({
    runningCount: cycles.filter((c) => c.status === "running").length,
    completedCount: cycles.filter((c) => c.status === "completed").length,
    failedCount: cycles.filter((c) => c.status === "failed").length,
  }), [cycles]);

  const showWriteActions = tier === "system" && isAdmin;
  const dirtyLabel = tier === "system" ? "Dirty Projects" : "Dirty";

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold tracking-tight">
          {tier === "system" ? "Dreaming" : "My Dreaming"}
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {tier === "system"
            ? "Background memory consolidation and knowledge graph improvement. Dreaming runs automatically when enrichment is idle and projects have new changes."
            : "Read-only view of dream cycles for projects you own. Enable/disable controls are administrator-only."}
        </p>
      </div>

      {/* Tier picker — admin only. Non-admin users are pinned to "self" */}
      {/* with no UI affordance; their account simply has no system view. */}
      {isAdmin && (
        <div className="border-b">
          <nav className="-mb-px flex gap-6" role="tablist" aria-label="Dreaming scope">
            <button
              role="tab"
              aria-selected={tier === "self"}
              onClick={() => {
                setTier("self");
                setSelectedCycleId(null);
              }}
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
              onClick={() => {
                setTier("system");
                setSelectedCycleId(null);
              }}
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

      {/* Self-tier: project picker */}
      {tier === "self" && (
        <div className="rounded-lg border bg-card p-4">
          <label htmlFor="dreaming-project" className="block text-sm font-medium">
            Project
          </label>
          {projects.length === 0 ? (
            <p className="mt-2 text-sm text-muted-foreground">
              No projects yet — create a project to view dream cycles.
            </p>
          ) : (
            <select
              id="dreaming-project"
              className="mt-2 w-full rounded-md border bg-background px-3 py-2 text-sm md:w-auto"
              value={selectedProjectId ?? ""}
              onChange={(e) => {
                setSelectedProjectId(e.target.value);
                setSelectedCycleId(null);
              }}
            >
              {projects.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.name}
                </option>
              ))}
            </select>
          )}
        </div>
      )}

      {/* Loading */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <Spinner />
        </div>
      )}

      {/* Error */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-900/30">
          <p className="text-sm text-red-800 dark:text-red-300">
            Failed to load dreaming status.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (tier === "system" || !!selectedProjectId) && (
        <>
          {/* Controls — system tier renders the enable/disable toggle; */}
          {/* self tier shows the read-only status flag instead. */}
          <div className="flex items-center justify-between rounded-lg border bg-card p-4">
            <div className="flex items-center gap-3">
              <span className="text-sm font-medium">Dreaming</span>
              {tier === "system" ? (
                <>
                  <Toggle
                    enabled={enabledFlag}
                    onChange={handleToggleEnabled}
                    disabled={enableMutation.isPending}
                  />
                  <span className="text-sm text-muted-foreground">
                    {enabledFlag ? "Enabled" : "Disabled"}
                  </span>
                </>
              ) : (
                <span className="text-sm text-muted-foreground">
                  {enabledFlag ? "Enabled" : "Disabled"}
                </span>
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
              color={runningCount > 0 ? "text-blue-600 dark:text-blue-400" : "text-muted-foreground"}
            />
            <StatCard
              label="Stuck"
              value={stuckCount}
              color={
                stuckCount > 0
                  ? "text-red-600 dark:text-red-400"
                  : "text-muted-foreground"
              }
            />
            <StatCard
              label="Completed"
              value={completedCount}
              color="text-green-600 dark:text-green-400"
            />
            <StatCard
              label="Failed"
              value={failedCount}
              color={failedCount > 0 ? "text-red-600 dark:text-red-400" : "text-muted-foreground"}
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
