import { useState } from "react";
import { useBackfillConsolidationEntities } from "../hooks/useApi";

// ConsolidationEntityBackfillBlock recovers entity-graph coverage for
// consolidation dreams that lack it. Heavy consolidation migrates subject-matter
// content into dream syntheses while their source memories are superseded and
// their entities reaped, so the graph collapses. This enqueues an entity-only
// enrichment job (fact extraction skipped, so no child memories) for every
// active consolidation dream that has no sourced relationship yet. Dry-run
// counts them; confirm enqueues. The dreaming ConsolidationEntityBackfill phase
// does the same automatically each cycle; this is the on-demand path. Progress
// is observed on the Enrichment Queue page as the jobs drain.
export function ConsolidationEntityBackfillBlock() {
  const backfill = useBackfillConsolidationEntities();
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
        Backfill Consolidation Entities
      </h4>
      <p className="mt-1 text-xs text-muted-foreground">
        Recover entity-graph coverage for consolidation dreams that have none.
        Heavy consolidation moves content into dream syntheses while reaping the
        sources' entities, collapsing the graph. Step one is a dry-run to count
        the uncovered dreams; step two enqueues an entity-only job per dream
        (fact extraction is skipped, so no new memories are created). The
        dreaming consolidation-entity backfill phase does this automatically each
        cycle; this is the on-demand path. Worker progress is visible on the
        Enrichment Queue page.
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
            No consolidation dreams are missing entity coverage.
          </span>
        )}
        {enqueued !== null && (
          <span className="text-xs text-green-700 dark:text-green-400">
            Enqueued {enqueued} entity-extraction job{enqueued === 1 ? "" : "s"}
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
            Extract entities for {candidateCount} consolidation dream
            {candidateCount === 1 ? "" : "s"}? This enqueues {candidateCount}{" "}
            entity-only enrichment job{candidateCount === 1 ? "" : "s"}.
          </p>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={handleConfirm}
              className="rounded-md bg-primary px-3 py-1 text-xs font-medium text-primary-foreground shadow-sm hover:bg-primary/90"
            >
              Enqueue extraction
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
