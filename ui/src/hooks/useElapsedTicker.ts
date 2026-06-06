import { useEffect, useState } from "react";

/**
 * useElapsedTicker forces a re-render every second while active is true.
 * Pairs with elapsedSeconds() / formatElapsed() so live "Xs ago" counters
 * update once per second without state plumbing from the parent.
 *
 * Pass the gating condition as `active` rather than calling unconditionally:
 * an idle hook still installs a setInterval and wakes the page every
 * second.
 */
export function useElapsedTicker(active: boolean): number {
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!active) return;
    const id = window.setInterval(() => setTick((t) => t + 1), 1000);
    return () => window.clearInterval(id);
  }, [active]);
  return tick;
}

/** Seconds elapsed since `iso` (UTC ISO timestamp). Floor, never negative. */
export function elapsedSeconds(iso?: string): number {
  if (!iso) return 0;
  const ms = Date.now() - new Date(iso).getTime();
  return Math.max(0, Math.floor(ms / 1000));
}

/** Human-readable elapsed string: 47s, 3m 12s, 2h 5m. */
export function formatElapsed(secs: number): string {
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}
