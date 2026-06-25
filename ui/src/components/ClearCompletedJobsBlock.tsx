import { useState } from "react";
import { useClearCompletedJobs } from "../hooks/useApi";

// ClearCompletedJobsBlock deletes completed enrichment_queue rows so the queue
// view stays readable as history accumulates. It is scoped strictly to
// completed jobs and never touches pending or processing rows, so it cannot
// strand an in-flight memory. Optionally keeps rows completed within the last N
// days. Requires an explicit confirm because the deletion is irreversible.
export function ClearCompletedJobsBlock() {
  const clear = useClearCompletedJobs();
  const [olderThanDays, setOlderThanDays] = useState<number>(0);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [deleted, setDeleted] = useState<number | null>(null);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  function handleConfirm() {
    setErrMsg(null);
    setConfirmOpen(false);
    setDeleted(null);
    clear.mutate(
      { older_than_days: olderThanDays },
      {
        onSuccess: (data) => setDeleted(data.deleted),
        onError: (err) => setErrMsg(err.message),
      },
    );
  }

  return (
    <div className="mt-4 rounded-md border border-border bg-muted/30 p-4">
      <h4 className="text-sm font-semibold text-foreground">
        Clear Completed Jobs
      </h4>
      <p className="mt-1 text-xs text-muted-foreground">
        Delete completed enrichment queue rows so the queue stays readable. This
        only removes <span className="font-medium">completed</span> history;
        pending and processing jobs are never touched, and re-embed/re-extract
        actions create fresh rows. Keep recent completions by setting a day
        window, or leave it at 0 to clear all completed rows.
      </p>
      <div className="mt-3 flex flex-wrap items-center gap-3">
        <label className="flex items-center gap-2 text-xs text-muted-foreground">
          Keep last
          <input
            type="number"
            min={0}
            value={olderThanDays}
            onChange={(e) =>
              setOlderThanDays(Math.max(0, Number(e.target.value) || 0))
            }
            className="w-20 rounded-md border border-input bg-background px-2 py-1 text-sm text-foreground"
          />
          days
        </label>
        <button
          type="button"
          onClick={() => {
            setErrMsg(null);
            setConfirmOpen(true);
          }}
          disabled={clear.isPending}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {clear.isPending ? "Working…" : "Clear completed jobs"}
        </button>
        {deleted !== null && (
          <span className="text-xs text-green-700 dark:text-green-400">
            Deleted {deleted} completed job{deleted === 1 ? "" : "s"}.
          </span>
        )}
      </div>
      {errMsg && <p className="mt-2 text-xs text-destructive">{errMsg}</p>}

      {confirmOpen && (
        <div className="mt-3 rounded-md border border-yellow-300 bg-yellow-50 p-3 dark:border-yellow-800 dark:bg-yellow-900/30">
          <p className="text-xs text-yellow-900 dark:text-yellow-100">
            Delete completed enrichment jobs
            {olderThanDays > 0
              ? ` completed more than ${olderThanDays} day${olderThanDays === 1 ? "" : "s"} ago`
              : " (all completed rows)"}
            ? This is irreversible. Pending and processing jobs are not affected.
          </p>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={handleConfirm}
              className="rounded-md bg-destructive px-3 py-1 text-xs font-medium text-destructive-foreground shadow-sm hover:bg-destructive/90"
            >
              Delete completed
            </button>
            <button
              type="button"
              onClick={() => setConfirmOpen(false)}
              className="rounded-md border border-input px-3 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted"
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
