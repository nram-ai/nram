import { useCallback } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useMaintenanceStatus } from "../hooks/useApi";
import { useEventStream, type EventStreamMessage } from "../hooks/useEventStream";

/**
 * MaintenanceBanner shows a single, non-dismissable informational bar whenever
 * the server reports an active maintenance operation (for example a SQLite
 * VACUUM). It polls the public status endpoint as a baseline (so a client that
 * connects mid-maintenance still sees it) and subscribes to the SSE
 * "maintenance" scope so it flips the moment an operation starts or ends.
 *
 * Informational only: one factual line, no action button. The user acts through
 * existing UI if they need to; the banner just reports state.
 */
export function MaintenanceBanner() {
  const { data } = useMaintenanceStatus();
  const queryClient = useQueryClient();

  const onEvent = useCallback(
    (evt: EventStreamMessage) => {
      if (evt.type === "maintenance.started" || evt.type === "maintenance.ended") {
        void queryClient.invalidateQueries({ queryKey: ["maintenance-status"] });
      }
    },
    [queryClient],
  );

  // The SSE stream is auth-gated; this banner only mounts inside the
  // authenticated app shell, so the subscription is always allowed here.
  useEventStream({ scope: "maintenance", onEvent });

  const op = data?.operations[0];
  if (!op) return null;

  const message = op.message;

  return (
    <div
      role="status"
      aria-live="polite"
      className="flex items-center gap-2 border-b border-yellow-300 bg-yellow-50 px-4 py-2 text-sm text-yellow-900 dark:border-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-200"
    >
      <span
        className="inline-block h-2 w-2 flex-shrink-0 animate-pulse rounded-full bg-yellow-500"
        aria-hidden="true"
      />
      <span>{message}</span>
    </div>
  );
}

export default MaintenanceBanner;
