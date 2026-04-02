import { useState, useCallback, useMemo } from "react";
import {
  useDreamingStatus,
  useDreamingCycles,
  useDreamingCycleDetail,
  useSetDreamingEnabled,
  useRollbackDreamCycle,
} from "../hooks/useApi";
import type { DreamCycle, DreamLog } from "../api/client";

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

function CycleTable({
  cycles,
  onSelect,
  selectedId,
}: {
  cycles: DreamCycle[];
  onSelect: (id: string) => void;
  selectedId: string | null;
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
          {cycles.map((cycle) => (
            <tr
              key={cycle.id}
              onClick={() => onSelect(cycle.id)}
              className={`cursor-pointer border-b transition-colors hover:bg-muted/30 ${
                selectedId === cycle.id ? "bg-muted/50" : ""
              }`}
            >
              <td className="px-4 py-3">
                <StatusBadge status={cycle.status} />
              </td>
              <td className="px-4 py-3 text-muted-foreground">
                {cycle.phase ? (PHASE_LABELS[cycle.phase] ?? cycle.phase) : "-"}
              </td>
              <td className="px-4 py-3 font-mono text-xs">
                {cycle.tokens_used.toLocaleString()} / {cycle.token_budget.toLocaleString()}
              </td>
              <td className="px-4 py-3 text-muted-foreground">
                {formatDuration(cycle.started_at, cycle.completed_at ?? cycle.updated_at)}
              </td>
              <td className="px-4 py-3 text-muted-foreground" title={formatDate(cycle.started_at)}>
                {cycle.started_at ? relativeTime(cycle.started_at) : relativeTime(cycle.created_at)}
              </td>
              <td className="px-4 py-3 text-right text-muted-foreground">
                &rsaquo;
              </td>
            </tr>
          ))}
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
}: {
  cycleId: string;
  onClose: () => void;
  onRollback: (id: string) => void;
  isRollingBack: boolean;
}) {
  const { data, isLoading, isError } = useDreamingCycleDetail(cycleId);
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
          </div>
          <p className="mt-1 font-mono text-xs text-muted-foreground">{cycle.id}</p>
        </div>
        <div className="flex items-center gap-2">
          {canRollback && (
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
            {cycle.tokens_used.toLocaleString()} / {cycle.token_budget.toLocaleString()}
          </p>
        </div>
        <div>
          <p className="text-muted-foreground">Duration</p>
          <p className="font-medium">
            {formatDuration(cycle.started_at, cycle.completed_at ?? cycle.updated_at)}
          </p>
        </div>
      </div>

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
  const statusQuery = useDreamingStatus();
  const cyclesQuery = useDreamingCycles();
  const enableMutation = useSetDreamingEnabled();
  const rollbackMutation = useRollbackDreamCycle();

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

  const status = statusQuery.data;
  const cycles = cyclesQuery.data ?? [];
  const isLoading = statusQuery.isLoading;
  const isError = statusQuery.isError;

  const { runningCount, completedCount, failedCount } = useMemo(() => ({
    runningCount: cycles.filter((c) => c.status === "running").length,
    completedCount: cycles.filter((c) => c.status === "completed").length,
    failedCount: cycles.filter((c) => c.status === "failed").length,
  }), [cycles]);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Dreaming</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Background memory consolidation and knowledge graph improvement.
          Dreaming runs automatically when enrichment is idle and projects have new changes.
        </p>
      </div>

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
      {!isLoading && !isError && (
        <>
          {/* Controls */}
          <div className="flex items-center justify-between rounded-lg border bg-card p-4">
            <div className="flex items-center gap-3">
              <span className="text-sm font-medium">Dreaming</span>
              <Toggle
                enabled={status?.enabled ?? false}
                onChange={handleToggleEnabled}
                disabled={enableMutation.isPending}
              />
              <span className="text-sm text-muted-foreground">
                {status?.enabled ? "Enabled" : "Disabled"}
              </span>
            </div>
            <p className="text-xs text-muted-foreground">Auto-refreshing every 10 seconds</p>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
            <StatCard
              label="Dirty Projects"
              value={status?.dirty_count ?? 0}
              color={
                (status?.dirty_count ?? 0) > 0
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

          {/* Detail or List */}
          {selectedCycleId ? (
            <CycleDetail
              cycleId={selectedCycleId}
              onClose={() => setSelectedCycleId(null)}
              onRollback={handleRollback}
              isRollingBack={rollbackMutation.isPending}
            />
          ) : (
            <div>
              <h2 className="mb-3 text-lg font-semibold">Dream Cycles</h2>
              <CycleTable
                cycles={cycles}
                onSelect={setSelectedCycleId}
                selectedId={selectedCycleId}
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
