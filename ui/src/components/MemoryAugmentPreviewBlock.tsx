import { useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCheck } from "../lib/icons";
import {
  useMemoryDetail,
  usePreviewMemoryAugmentation,
} from "../hooks/useApi";
import type { MemoryAugmentPreviewResponse } from "../api/client";

// MemoryAugmentPreviewBlock surfaces the per-memory state of the
// query-augmentation phase. Three pieces:
//
// 1. A binary status pill at the top: green "Augmented" when persisted
//    queries exist, neutral "Raw embed" when they do not. This is the
//    at-a-glance signal callers were missing.
// 2. The persisted augmented_queries list when augmentation has run.
// 3. A "Preview" button that runs the phase against this memory's current
//    content using current settings, without persisting. Useful for tuning
//    the prompt against real corpus content instead of synthetic samples.
//
// When persistedQueries / persistedAt are not supplied as props the block
// self-fetches the memory record via useMemoryDetail so it can be embedded
// in surfaces (e.g. the EnrichmentMonitor accordion) that do not already
// have the memory loaded. Existing callers continue to pass both props.
export function MemoryAugmentPreviewBlock({
  projectId,
  memoryId,
  persistedQueries,
  persistedAt,
}: {
  projectId: string;
  memoryId: string;
  persistedQueries?: string[] | null;
  persistedAt?: string | null;
}) {
  const propsProvided =
    persistedQueries !== undefined || persistedAt !== undefined;
  const detail = useMemoryDetail(
    propsProvided ? "" : projectId,
    propsProvided ? "" : memoryId,
  );

  const queries: string[] | null = propsProvided
    ? persistedQueries ?? null
    : detail.data?.augmented_queries ?? null;
  const at: string | null = propsProvided
    ? persistedAt ?? null
    : detail.data?.augmented_embedding_at ?? null;

  const preview = usePreviewMemoryAugmentation();
  const [result, setResult] = useState<MemoryAugmentPreviewResponse | null>(
    null,
  );
  const [errMsg, setErrMsg] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);

  function handleRun() {
    setErrMsg(null);
    setResult(null);
    preview.mutate(
      { projectId, memoryId },
      {
        onSuccess: (data) => {
          setResult(data);
          setExpanded(true);
        },
        onError: (err) => setErrMsg(err.message),
      },
    );
  }

  const hasPersisted = !!queries && queries.length > 0;
  const queryCount = hasPersisted ? queries!.length : 0;

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <h3 className="text-sm font-medium text-muted-foreground">
          Augmentation
        </h3>
        {hasPersisted ? (
          <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-100 px-2.5 py-0.5 text-xs font-medium text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300">
            <FontAwesomeIcon
              icon={faCheck}
              className="h-3 w-3"
              aria-hidden="true"
            />
            Augmented · {queryCount} {queryCount === 1 ? "query" : "queries"}
          </span>
        ) : (
          <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
            Raw embed · not augmented
          </span>
        )}
        {hasPersisted && at && (
          <span className="text-xs text-muted-foreground">
            on {new Date(at).toLocaleString()}
          </span>
        )}
      </div>

      {hasPersisted ? (
        <div className="rounded border bg-muted/50 p-3 text-sm">
          <ul className="list-disc space-y-1 pl-5">
            {queries!.map((q, i) => (
              <li key={i} className="text-foreground">
                {q}
              </li>
            ))}
          </ul>
        </div>
      ) : (
        <p className="text-xs text-muted-foreground">
          This memory&apos;s vector was built from raw content. Run a preview
          to see what augmentation would generate against the current content
          using current settings; no persistence.
        </p>
      )}

      <div className="mt-3 flex items-center gap-3">
        <button
          type="button"
          onClick={handleRun}
          disabled={preview.isPending}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-xs font-medium text-foreground shadow-sm hover:bg-muted disabled:cursor-not-allowed disabled:opacity-50"
        >
          {preview.isPending ? "Generating…" : "Preview augmentation"}
        </button>
        {result && (
          <span className="text-xs text-muted-foreground">
            Ran in {result.latency_ms}ms via {result.model || "unknown model"}
          </span>
        )}
      </div>

      {errMsg && <p className="mt-2 text-xs text-destructive">{errMsg}</p>}

      {result && expanded && (
        <div className="mt-3 space-y-3 rounded border bg-muted/30 p-3">
          {result.error && (
            <p className="text-xs text-destructive">{result.error}</p>
          )}
          {result.queries.length > 0 && (
            <div>
              <h4 className="mb-1 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Generated queries
              </h4>
              <ul className="list-disc space-y-1 pl-5 text-sm">
                {result.queries.map((q, i) => (
                  <li key={i}>{q}</li>
                ))}
              </ul>
            </div>
          )}
          <details className="text-xs">
            <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
              Rendered prompt
            </summary>
            <pre className="mt-2 whitespace-pre-wrap rounded bg-background p-2 font-mono text-xs">
              {result.rendered_prompt}
            </pre>
          </details>
          <details className="text-xs">
            <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
              Embed input (queries + separator + content)
            </summary>
            <pre className="mt-2 whitespace-pre-wrap rounded bg-background p-2 font-mono text-xs">
              {result.augmented_content}
            </pre>
            {result.truncated_bytes > 0 && (
              <p className="mt-1 text-yellow-700 dark:text-yellow-400">
                Content tail truncated by {result.truncated_bytes} bytes to fit
                the configured max input size.
              </p>
            )}
          </details>
        </div>
      )}
    </div>
  );
}
