import { useState } from "react";
import { useBackfillMissingEmbeddings } from "../hooks/useApi";

// MissingEmbeddingBackfillBlock repairs embedding-stranded memories: live,
// embeddable memories that ended up with no stored vector (e.g. after an
// embedder outage) and are therefore invisible to vector recall. Dry-run counts
// them; confirm enqueues a normal enrichment job per memory so the worker
// re-embeds and finalizes it. Runs entirely off the queue, independent of
// dreaming, so it works even with the dreaming scheduler disabled. Progress is
// observed on the Enrichment Queue page, where the "Missing embeddings" count
// drains as the work completes.
export function MissingEmbeddingBackfillBlock() {
  const backfill = useBackfillMissingEmbeddings();
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
        Backfill Missing Embeddings
      </h4>
      <p className="mt-1 text-xs text-muted-foreground">
        Re-embed memories that have no stored vector and so cannot be found by
        vector recall. Step one is a dry-run to count them; step two enqueues an
        enrichment job per memory so the worker re-embeds it (extraction is
        skipped for already-enriched memories). Runs off the queue without
        dreaming. Worker progress is visible on the Enrichment Queue page.
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
            No memories are missing embeddings.
          </span>
        )}
        {enqueued !== null && (
          <span className="text-xs text-green-700 dark:text-green-400">
            Enqueued {enqueued} re-embed job{enqueued === 1 ? "" : "s"}
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
            Re-embed {candidateCount} memor{candidateCount === 1 ? "y" : "ies"}?
            This enqueues {candidateCount} enrichment job
            {candidateCount === 1 ? "" : "s"} and re-embeds each memory.
          </p>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={handleConfirm}
              className="rounded-md bg-primary px-3 py-1 text-xs font-medium text-primary-foreground shadow-sm hover:bg-primary/90"
            >
              Enqueue re-embed
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
