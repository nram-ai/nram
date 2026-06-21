import { useState } from "react";
import { useBackfillMultiVector } from "../hooks/useApi";

// MultiVectorBackfillBlock is the action surface for the Multi-Vector Facets
// settings card. Backfill is an operator action, not a setting value: dry-run
// reports how many memories would be re-faceted, then confirm enqueues an
// enrichment job for each so the worker re-embeds them with topic facets. This
// is the path that gives existing memories (stored before multi-vector was
// enabled) their facets, including high-confidence syntheses that already
// superseded their sources. Queue progress is observed on the Enrichment Queue
// page; this block just kicks the work off.
export function MultiVectorBackfillBlock() {
  const backfill = useBackfillMultiVector();
  const [candidateCount, setCandidateCount] = useState<number | null>(null);
  const [enqueued, setEnqueued] = useState<number | null>(null);
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
    setExpectedAtConfirm(candidateCount);
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
        Backfill Multi-Vector Facets
      </h4>
      <p className="mt-1 text-xs text-muted-foreground">
        Re-facet memories stored before multi-vector was enabled. Step one is a
        dry-run to count the affected memories; step two enqueues an enrichment
        job per memory so the worker re-embeds it and writes its topic facets.
        Worker progress is visible on the Enrichment Queue page.
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
            Enqueued {enqueued} facet job{enqueued === 1 ? "" : "s"}
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
      {errMsg && <p className="mt-2 text-xs text-destructive">{errMsg}</p>}

      {confirmOpen && candidateCount !== null && (
        <div className="mt-3 rounded-md border border-yellow-300 bg-yellow-50 p-3 dark:border-yellow-800 dark:bg-yellow-900/30">
          <p className="text-xs text-yellow-900 dark:text-yellow-100">
            Re-facet {candidateCount} memor{candidateCount === 1 ? "y" : "ies"}?
            This enqueues {candidateCount} enrichment job
            {candidateCount === 1 ? "" : "s"} and re-embeds each memory.
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
