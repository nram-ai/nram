import { useClearFailedEnrichment } from "../hooks/useApi";
import { ClearJobsBlock } from "./ClearJobsBlock";

// ClearFailedJobsBlock deletes failed enrichment_queue rows so a burst of
// failures (a misconfigured provider failing every job) cannot make the queue
// view slow to load. It is admin/system-scoped here and strictly limited to
// failed jobs, so it never touches pending, processing, or completed rows.
// Optionally keeps rows that last failed within the last N days. Requires an
// explicit confirm because the deletion is irreversible. Note: memories whose
// only queue row was a failed job become uncovered and may be re-enqueued as
// pending by the dreaming backfill, so this is relief for a slow queue, not a
// permanent purge unless the underlying failure is fixed.
export function ClearFailedJobsBlock() {
  const clear = useClearFailedEnrichment({ tier: "system" });
  return (
    <ClearJobsBlock
      title="Clear Failed Jobs"
      description={
        <>
          Delete failed enrichment queue rows so a burst of failures does not
          slow the queue view. This only removes{" "}
          <span className="font-medium">failed</span> rows across all tenants;
          pending, processing, and completed jobs are never touched. Keep recent
          failures by setting a day window, or leave it at 0 to clear all failed
          rows.
        </>
      }
      buttonLabel="Clear failed jobs"
      deletedNoun="failed"
      confirmButtonLabel="Delete failed"
      confirmQuestion={(days) => (
        <>
          Delete failed enrichment jobs
          {days > 0
            ? ` that last failed more than ${days} day${days === 1 ? "" : "s"} ago`
            : " (all failed rows)"}
          ? This is irreversible. Pending, processing, and completed jobs are not
          affected.
        </>
      )}
      isPending={clear.isPending}
      run={(days, cb) =>
        clear.mutate(days, {
          onSuccess: (data) => cb.onSuccess(data.deleted),
          onError: (err) => cb.onError(err.message),
        })
      }
    />
  );
}
