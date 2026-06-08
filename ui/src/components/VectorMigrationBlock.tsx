import { useState } from "react";
import {
  useVectorMigrationDryRun,
  useStartVectorMigration,
} from "../hooks/useApi";
import { useEventStream } from "../hooks/useEventStream";
import type {
  VectorMigrationDirection,
  VectorMigrationResult,
} from "../api/client";

// VectorMigrationBlock is the action surface for the Vector Database settings
// card. Configuring Qdrant only stores connection settings; it does not move
// any data, so this block copies existing memory and entity vectors between the
// SQL primary store and Qdrant. Each direction is dry-run first (counts the
// vectors without writing), then confirm. The real copy runs in the background
// on the server and streams progress over SSE (scope "vector-migration"); the
// copy is upsert-only (never deletes) and safe to re-run. Recall uses the new
// store only after a restart with the settings updated.
export function VectorMigrationBlock() {
  const dryRun = useVectorMigrationDryRun();
  const start = useStartVectorMigration();

  const [direction, setDirection] = useState<VectorMigrationDirection | null>(
    null,
  );
  const [preview, setPreview] = useState<VectorMigrationResult | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [running, setRunning] = useState(false);
  const [copied, setCopied] = useState(0);
  const [total, setTotal] = useState(0);
  const [result, setResult] = useState<VectorMigrationResult | null>(null);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  function previewTotal(r: VectorMigrationResult): number {
    return r.memory_count + r.entity_count;
  }

  useEventStream({
    scope: "vector-migration",
    enabled: running,
    onEvent: (evt) => {
      const data = (evt.data ?? {}) as Record<string, unknown>;
      switch (evt.type) {
        case "vector_migration.progress":
          setCopied(
            ((data.memory_copied as number) ?? 0) +
              ((data.entity_copied as number) ?? 0),
          );
          break;
        case "vector_migration.completed":
          setResult(data as unknown as VectorMigrationResult);
          setRunning(false);
          break;
        case "vector_migration.failed":
          setErrMsg(String(data.error ?? "migration failed"));
          setRunning(false);
          break;
        default:
          break;
      }
    },
  });

  function handleDryRun(dir: VectorMigrationDirection) {
    setErrMsg(null);
    setResult(null);
    setPreview(null);
    setDirection(dir);
    dryRun.mutate(
      { direction: dir },
      {
        onSuccess: (data) => {
          setPreview(data);
          if (previewTotal(data) > 0) setConfirmOpen(true);
        },
        onError: (err) => setErrMsg(err.message),
      },
    );
  }

  function handleConfirm() {
    if (!direction || !preview) return;
    setConfirmOpen(false);
    setErrMsg(null);
    setResult(null);
    setCopied(0);
    setTotal(previewTotal(preview));
    setRunning(true);
    start.mutate(
      { direction },
      {
        onError: (err) => {
          setErrMsg(err.message);
          setRunning(false);
        },
      },
    );
  }

  const targetLabel = direction === "from_qdrant" ? "the SQL store" : "Qdrant";
  const pct = total > 0 ? Math.min(100, Math.round((copied / total) * 100)) : 0;
  const busy = dryRun.isPending || start.isPending || running;

  return (
    <div className="mt-4 rounded-md border border-border bg-muted/30 p-4">
      <h4 className="text-sm font-semibold text-foreground">Migrate Vectors</h4>
      <p className="mt-1 text-xs text-muted-foreground">
        Configuring Qdrant above only saves the connection; it does not move any
        data. Use these actions to copy existing memory and entity vectors
        between the built-in store and Qdrant. Each step is a dry-run count
        first, then a confirm that copies by upsert (it never deletes and is
        safe to re-run). The copy runs in the background with live progress
        below. Tip: pause enrichment first, and note that recall only uses the
        new store after a server restart with the settings updated.
      </p>
      <div className="mt-3 flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={() => handleDryRun("to_qdrant")}
          disabled={busy}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Migrate to Qdrant (dry run)
        </button>
        <button
          type="button"
          onClick={() => handleDryRun("from_qdrant")}
          disabled={busy}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Migrate back from Qdrant (dry run)
        </button>
      </div>

      {preview !== null && previewTotal(preview) === 0 && !running && (
        <p className="mt-2 text-xs text-muted-foreground">
          No vectors found to migrate{" "}
          {direction === "from_qdrant" ? "from" : "to"} Qdrant.
        </p>
      )}

      {running && (
        <div className="mt-3">
          <div className="mb-1 flex items-center justify-between text-xs text-muted-foreground">
            <span>
              Migrating to {targetLabel}… {copied.toLocaleString()}
              {total > 0 ? ` / ${total.toLocaleString()}` : ""} vectors
            </span>
            <span>{total > 0 ? `${pct}%` : ""}</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded bg-muted">
            <div
              className="h-full bg-primary transition-all"
              style={{ width: total > 0 ? `${pct}%` : "100%" }}
            />
          </div>
        </div>
      )}

      {result !== null && !running && (
        <div className="mt-2 text-xs">
          <span className="text-green-700 dark:text-green-400">
            Copied {result.memory_count} memory and {result.entity_count} entity
            vector
            {result.memory_count + result.entity_count === 1 ? "" : "s"} to{" "}
            {targetLabel}.
          </span>
          {result.mismatch && (
            <span className="ml-2 text-destructive">
              Warning: destination counts are lower than source for at least one
              dimension. Re-run the migration to catch up.
            </span>
          )}
        </div>
      )}

      {errMsg && <p className="mt-2 text-xs text-destructive">{errMsg}</p>}

      {confirmOpen && preview !== null && (
        <div className="mt-3 rounded-md border border-yellow-300 bg-yellow-50 p-3 dark:border-yellow-800 dark:bg-yellow-900/30">
          <p className="text-xs text-yellow-900 dark:text-yellow-100">
            Copy {preview.memory_count} memory and {preview.entity_count} entity
            vector{previewTotal(preview) === 1 ? "" : "s"} to {targetLabel}?
            This upserts existing vectors and does not delete anything.
          </p>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={handleConfirm}
              className="rounded-md bg-primary px-3 py-1 text-xs font-medium text-primary-foreground shadow-sm hover:bg-primary/90"
            >
              Copy vectors
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
