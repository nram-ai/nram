import { useState } from "react";
import { usePreviewMemoryAugmentation } from "../hooks/useApi";
import type { MemoryAugmentPreviewResponse } from "../api/client";

// MemoryAugmentPreviewBlock surfaces the per-memory preview of the
// query-augmentation phase. Two pieces:
//
// 1. If the memory was already embedded with augmentation, the persisted
//    augmented_queries are shown so operators can verify what the embedder
//    actually saw.
// 2. A "Preview" button that runs the phase against this memory's current
//    content using current settings, without persisting. Useful for tuning
//    the prompt against real corpus content instead of synthetic samples.
//
// Both rendering paths are decorative — the embedded vector is the source of
// truth; this block is for debugging and prompt tuning.
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

  const hasPersisted = persistedQueries && persistedQueries.length > 0;

  return (
    <div>
      <h3 className="mb-2 text-sm font-medium text-muted-foreground">
        Augmented Queries
      </h3>

      {hasPersisted ? (
        <div className="rounded border bg-muted/50 p-3 text-sm">
          <ul className="list-disc space-y-1 pl-5">
            {persistedQueries.map((q, i) => (
              <li key={i} className="text-foreground">
                {q}
              </li>
            ))}
          </ul>
          {persistedAt && (
            <p className="mt-2 text-xs text-muted-foreground">
              Embedded with augmentation on {new Date(persistedAt).toLocaleString()}
            </p>
          )}
        </div>
      ) : (
        <p className="text-xs text-muted-foreground">
          This memory was embedded against raw content. Run a preview to see
          what the augmentation phase would generate against the current
          content using current settings; no persistence.
        </p>
      )}

      <div className="mt-3 flex items-center gap-3">
        <button
          type="button"
          onClick={handleRun}
          disabled={preview.isPending}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-xs font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
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
