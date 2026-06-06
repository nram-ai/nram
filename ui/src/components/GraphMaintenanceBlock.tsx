import { useState } from "react";
import { useGraphHealth, useRepairGraph } from "../hooks/useApi";

// GraphMaintenanceBlock is the action surface for graph cleanup, shown under
// the Lifecycle Sweep settings card. It is an operator action, not a setting
// value: the lifecycle sweep already reaps lost-provenance edges (relationships
// whose sourcing memory was deleted or superseded) on its own schedule, but
// this block lets an operator clear the accumulated backlog on demand and see
// how large it is. Repair reaps those edges, recomputes entity mention counts
// from surviving provenance, and prunes the orphaned entities left behind.
export function GraphMaintenanceBlock() {
  const health = useGraphHealth();
  const repair = useRepairGraph();
  const [confirmOpen, setConfirmOpen] = useState(false);

  // The mutation owns its own result/error state (repair.data / repair.error),
  // so there is no local copy to keep in sync.
  const lostEdges = health.data?.lost_provenance_edges ?? null;
  const result = repair.data;

  function handleRepair() {
    setConfirmOpen(false);
    repair.mutate();
  }

  return (
    <div className="mt-4 rounded-md border border-border bg-muted/30 p-4">
      <h4 className="text-sm font-semibold text-foreground">
        Graph Maintenance
      </h4>
      <p className="mt-1 text-xs text-muted-foreground">
        Reap orphaned knowledge-graph data (relationships whose sourcing memory
        has been deleted or superseded, plus the entities they leave stranded)
        and recompute entity mention counts from surviving provenance. The
        lifecycle sweep does this automatically; this runs it on demand to clear
        the backlog immediately.
      </p>
      <div className="mt-3 flex items-center gap-3">
        <button
          type="button"
          onClick={() => setConfirmOpen(true)}
          disabled={repair.isPending || health.isLoading}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {repair.isPending ? "Repairing…" : "Repair now"}
        </button>
        {lostEdges !== null && (
          <span className="text-xs text-muted-foreground">
            {lostEdges === 0
              ? "No orphaned edges found."
              : `${lostEdges.toLocaleString()} orphaned edge${lostEdges === 1 ? "" : "s"} found.`}
          </span>
        )}
      </div>
      {result && (
        <p className="mt-2 text-xs text-green-700 dark:text-green-400">
          Reaped {result.relationships_reaped.toLocaleString()} orphaned
          relationship{result.relationships_reaped === 1 ? "" : "s"}, pruned{" "}
          {result.dangling_relationships_deleted.toLocaleString()} dangling and{" "}
          {result.orphaned_entities_deleted.toLocaleString()} orphaned entit
          {result.orphaned_entities_deleted === 1 ? "y" : "ies"}.
        </p>
      )}
      {repair.error && (
        <p className="mt-2 text-xs text-destructive">{repair.error.message}</p>
      )}

      {confirmOpen && (
        <div className="mt-3 rounded-md border border-yellow-300 bg-yellow-50 p-3 dark:border-yellow-800 dark:bg-yellow-900/30">
          <p className="text-xs text-yellow-900 dark:text-yellow-100">
            {lostEdges !== null && lostEdges > 0
              ? `Permanently remove ${lostEdges.toLocaleString()} orphaned edge${lostEdges === 1 ? "" : "s"} and the entities they strand?`
              : "Run graph repair? This reaps any orphaned edges and prunes stranded entities."}{" "}
            This cannot be undone.
          </p>
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={handleRepair}
              className="rounded-md bg-primary px-3 py-1 text-xs font-medium text-primary-foreground shadow-sm hover:bg-primary/90"
            >
              Repair now
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
