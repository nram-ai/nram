import { useClearCompletedJobs } from "../hooks/useApi";
import { ClearJobsBlock } from "./ClearJobsBlock";

// ClearCompletedJobsBlock deletes completed enrichment_queue rows so the queue
// view stays readable as history accumulates. It is scoped strictly to
// completed jobs and never touches pending or processing rows, so it cannot
// strand an in-flight memory. Optionally keeps rows completed within the last N
// days. Requires an explicit confirm because the deletion is irreversible.
export function ClearCompletedJobsBlock() {
  const clear = useClearCompletedJobs();
  return (
    <ClearJobsBlock
      title="Clear Completed Jobs"
      description={
        <>
          Delete completed enrichment queue rows so the queue stays readable.
          This only removes <span className="font-medium">completed</span>{" "}
          history; pending and processing jobs are never touched, and
          re-embed/re-extract actions create fresh rows. Keep recent completions
          by setting a day window, or leave it at 0 to clear all completed rows.
        </>
      }
      buttonLabel="Clear completed jobs"
      deletedNoun="completed"
      confirmButtonLabel="Delete completed"
      confirmQuestion={(days) => (
        <>
          Delete completed enrichment jobs
          {days > 0
            ? ` completed more than ${days} day${days === 1 ? "" : "s"} ago`
            : " (all completed rows)"}
          ? This is irreversible. Pending and processing jobs are not affected.
        </>
      )}
      isPending={clear.isPending}
      run={(days, cb) =>
        clear.mutate(
          { older_than_days: days },
          {
            onSuccess: (data) => cb.onSuccess(data.deleted),
            onError: (err) => cb.onError(err.message),
          },
        )
      }
    />
  );
}
