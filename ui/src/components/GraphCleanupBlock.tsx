import { useState } from "react";
import {
  useRelabelGraph,
  useBackfillEmbeddingDims,
  useReExtract,
} from "../hooks/useApi";

// GraphCleanupBlock is the operator action surface for the knowledge-graph
// cleanup pipeline, rendered under Enrichment -> Worker Performance. Three
// distinct, increasingly heavy operations:
//
//  1. Backfill embedding dims  - cheap metadata repair, re-enables dedup.
//  2. Relabel graph            - deterministic (no-LLM) re-type/re-label + merge.
//  3. Re-extract               - full LLM re-extraction under the new vocabulary.
//
// Each destructive operation is a dry-run-then-confirm so the operator sees the
// affected counts before committing. Worker progress (for re-extract) is on the
// Enrichment Queue page; this block kicks the work off.
export function GraphCleanupBlock() {
  return (
    <div className="mt-4 space-y-4">
      <BackfillDimsCard />
      <RelabelCard />
      <ReExtractCard />
    </div>
  );
}

function Card({
  title,
  children,
  body,
}: {
  title: string;
  body: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-md border border-border bg-muted/30 p-4">
      <h4 className="text-sm font-semibold text-foreground">{title}</h4>
      <p className="mt-1 text-xs text-muted-foreground">{body}</p>
      <div className="mt-3 flex flex-wrap items-center gap-3">{children}</div>
    </div>
  );
}

function btnClass(disabled: boolean) {
  return `rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted ${
    disabled ? "opacity-50 cursor-not-allowed" : ""
  }`;
}

function BackfillDimsCard() {
  const backfill = useBackfillEmbeddingDims();
  const [updated, setUpdated] = useState<number | null>(null);
  const [errMsg, setErrMsg] = useState<string | null>(null);
  return (
    <Card
      title="Backfill embedding dimensions"
      body="Repair the entities.embedding_dim flag from the vectors that actually exist, re-enabling entity dedup's similarity path for entities whose flag was cleared. No re-embedding; Postgres only. Safe to run repeatedly."
    >
      <button
        type="button"
        onClick={() => {
          setErrMsg(null);
          backfill.mutate(undefined, {
            onSuccess: (d) => setUpdated(d.updated),
            onError: (e) => setErrMsg(e.message),
          });
        }}
        disabled={backfill.isPending}
        className={btnClass(backfill.isPending)}
      >
        {backfill.isPending ? "Working…" : "Backfill dimensions"}
      </button>
      {updated !== null && (
        <span className="text-xs text-green-700 dark:text-green-400">
          Repaired {updated} entit{updated === 1 ? "y" : "ies"}.
        </span>
      )}
      {errMsg && <span className="text-xs text-destructive">{errMsg}</span>}
    </Card>
  );
}

function RelabelCard() {
  const relabel = useRelabelGraph();
  const [preview, setPreview] = useState<{
    retyped: number;
    merged: number;
    rows: number;
    before: number;
    after: number;
  } | null>(null);
  const [applied, setApplied] = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  function run(dryRun: boolean) {
    setErrMsg(null);
    relabel.mutate(
      { dry_run: dryRun },
      {
        onSuccess: (d) => {
          setPreview({
            retyped: d.entities_retyped,
            merged: d.entities_merged,
            rows: d.relation_rows_relabeled,
            before: d.distinct_relations_before,
            after: d.distinct_relations_after,
          });
          setApplied(!dryRun);
        },
        onError: (e) => setErrMsg(e.message),
      },
    );
  }

  // A dry-run with no re-types, merges, or row relabels means the graph is
  // already fully canonical; offer no no-op Apply.
  const noChanges =
    !!preview &&
    preview.retyped === 0 &&
    preview.merged === 0 &&
    preview.rows === 0;

  return (
    <Card
      title="Relabel graph (deterministic)"
      body="Re-type every entity and re-label every relationship to the closed vocabulary in place, merging entities that collapse onto the same name+type. No LLM calls. Run the dry-run first to see the counts, then apply."
    >
      <button
        type="button"
        onClick={() => run(true)}
        disabled={relabel.isPending}
        className={btnClass(relabel.isPending)}
      >
        {relabel.isPending ? "Working…" : "Preview (dry run)"}
      </button>
      {preview &&
        !applied &&
        (noChanges ? (
          <span className="text-xs text-muted-foreground">
            Graph is already canonical; nothing to relabel.
          </span>
        ) : (
          <>
            <span className="text-xs text-muted-foreground">
              {preview.retyped} entities re-typed, {preview.merged} merged;{" "}
              {preview.rows} relationship rows re-labeled, {preview.before} →{" "}
              {preview.after} distinct relations.
            </span>
            <button
              type="button"
              onClick={() => run(false)}
              disabled={relabel.isPending}
              className={btnClass(relabel.isPending)}
            >
              Apply
            </button>
          </>
        ))}
      {applied && preview && (
        <span className="text-xs text-green-700 dark:text-green-400">
          Applied: {preview.merged} entities merged, {preview.rows} relationships
          re-labeled.
        </span>
      )}
      {errMsg && <span className="text-xs text-destructive">{errMsg}</span>}
    </Card>
  );
}

function ReExtractCard() {
  const reExtract = useReExtract();
  const [candidates, setCandidates] = useState<number | null>(null);
  const [result, setResult] = useState<{
    enqueued: number;
    children: number;
    entities: number;
  } | null>(null);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  return (
    <Card
      title="Re-extract memories (LLM)"
      body="Full re-extraction: tombstone each memory's existing entities/relationships, drop its extracted-fact children, and re-enqueue it so the worker re-extracts under the current prompt and vocabulary. Heaviest operation; runs through the normal worker queue. Dry-run reports how many memories qualify."
    >
      <button
        type="button"
        onClick={() => {
          setErrMsg(null);
          setResult(null);
          reExtract.mutate(
            { dry_run: true },
            {
              onSuccess: (d) => setCandidates(d.candidate_count),
              onError: (e) => setErrMsg(e.message),
            },
          );
        }}
        disabled={reExtract.isPending}
        className={btnClass(reExtract.isPending)}
      >
        {reExtract.isPending ? "Working…" : "Check candidates (dry run)"}
      </button>
      {candidates !== null && !result && (
        <>
          <span className="text-xs text-muted-foreground">
            {candidates} memor{candidates === 1 ? "y" : "ies"} eligible.
          </span>
          {candidates > 0 && (
            <button
              type="button"
              onClick={() => {
                setErrMsg(null);
                reExtract.mutate(
                  { dry_run: false },
                  {
                    onSuccess: (d) => {
                      setResult({
                        enqueued: d.enqueued,
                        children: d.fact_children_removed,
                        entities: d.entities_recomputed,
                      });
                      setCandidates(null);
                    },
                    onError: (e) => setErrMsg(e.message),
                  },
                );
              }}
              disabled={reExtract.isPending}
              className={btnClass(reExtract.isPending)}
            >
              Re-extract all
            </button>
          )}
        </>
      )}
      {result && (
        <span className="text-xs text-green-700 dark:text-green-400">
          Enqueued {result.enqueued}; removed {result.children} fact child
          {result.children === 1 ? "" : "ren"}; recomputed {result.entities}{" "}
          entit{result.entities === 1 ? "y" : "ies"}. Watch the Enrichment Queue.
        </span>
      )}
      {errMsg && <span className="text-xs text-destructive">{errMsg}</span>}
    </Card>
  );
}
