import { useState } from "react";
import { useBackfillAugmentation } from "../hooks/useApi";

// QueryAugmentBackfillBlock is the action surface for the Query Augmentation
// settings card. Lives outside the schema editor because backfill is an
// operator action, not a setting value: dry-run reports the candidate count
// (memories whose vector pre-dates the augmentation flip), then confirm
// enqueues an enrichment job for each so the worker re-embeds with the
// augmented input. Queue progress is observed through the existing Enrichment
// Queue page; this block just kicks the work off.
export function QueryAugmentBackfillBlock() {
  const backfill = useBackfillAugmentation();
  const [candidateCount, setCandidateCount] = useState<number | null>(null);
  const [enqueued, setEnqueued] = useState<number | null>(null);
  // expectedAtConfirm captures the candidate count the user saw in the
  // confirm modal. The backend recomputes candidates on the second call
  // (dry_run=false), so the actual enqueued number can drift if rows were
  // soft-deleted, superseded, or augmented by another worker between the
  // two clicks. Surfacing the delta is the difference between "the system
  // is broken" and "5 memories changed state mid-flight, here is why."
  const [expectedAtConfirm, setExpectedAtConfirm] = useState<number | null>(
    null,
  );
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  function handleDryRun() {
    setErrMsg(null);
    setEnqueued(null);
    setExpectedAtConfirm(null);
    backfill.mutate(
      { dry_run: true },
      {
        onSuccess: (data) => {
          setCandidateCount(data.candidate_count);
          if (data.candidate_count > 0) setConfirmOpen(true);
        },
        onError: (err) => setErrMsg(err.message),
      },
    );
  }

  function handleConfirm() {
    setErrMsg(null);
    setConfirmOpen(false);
    const expected = candidateCount;
    setExpectedAtConfirm(expected);
    backfill.mutate(
      { dry_run: false },
      {
        onSuccess: (data) => {
          setEnqueued(data.enqueued);
          setCandidateCount(null);
        },
        onError: (err) => setErrMsg(err.message),
      },
    );
  }

  return (
    <div className="mt-4 rounded-md border border-border bg-muted/30 p-4">
      <h4 className="text-sm font-semibold text-foreground">
        Backfill Augmentation
      </h4>
      <p className="mt-1 text-xs text-muted-foreground">
        Re-embed memories whose vector was written before query augmentation
        was enabled. Step one is a dry-run to count the affected memories;
        step two enqueues an enrichment job per candidate. Already-enriched
        memories skip fact and entity extraction, so the cost is one extra
        LLM augmentation call plus one embed call per memory. Worker
        progress is visible on the Enrichment Queue page.
      </p>
      <div className="mt-3 flex items-center gap-3">
        <button
          type="button"
          onClick={handleDryRun}
          disabled={backfill.isPending}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {backfill.isPending ? "Working…" : "Check candidates (dry run)"}
        </button>
        {candidateCount !== null && candidateCount === 0 && (
          <span className="text-xs text-muted-foreground">
            No memories need backfill.
          </span>
        )}
        {enqueued !== null && (
          <span className="text-xs text-green-700 dark:text-green-400">
            Enqueued {enqueued} augmentation job{enqueued === 1 ? "" : "s"}
            {expectedAtConfirm !== null && expectedAtConfirm !== enqueued && (
              <>
                {" "}
                ({expectedAtConfirm - enqueued} candidate
                {expectedAtConfirm - enqueued === 1 ? "" : "s"} changed state
                between dry-run and confirm)
              </>
            )}
            .
          </span>
        )}
      </div>
      {errMsg && (
        <p className="mt-2 text-xs text-destructive">{errMsg}</p>
      )}

      {confirmOpen && candidateCount !== null && (
        <div className="mt-3 rounded-md border border-yellow-300 bg-yellow-50 p-3 dark:border-yellow-800 dark:bg-yellow-900/30">
          <p className="text-xs text-yellow-900 dark:text-yellow-100">
            Re-embed {candidateCount} memor{candidateCount === 1 ? "y" : "ies"}{" "}
            with augmentation? This enqueues {candidateCount} enrichment
            job{candidateCount === 1 ? "" : "s"} and incurs{" "}
            {candidateCount} extra LLM call{candidateCount === 1 ? "" : "s"}.
          </p>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={handleConfirm}
              className="rounded-md bg-primary px-3 py-1 text-xs font-medium text-primary-foreground shadow-sm hover:bg-primary/90"
            >
              Enqueue backfill
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
