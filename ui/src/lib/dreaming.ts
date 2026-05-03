export const PHASE_LABELS: Record<string, string> = {
  entity_dedup: "Entity Dedup",
  embedding_backfill: "Embedding Backfill",
  paraphrase_dedup: "Paraphrase Dedup",
  transitive_discovery: "Transitive Discovery",
  contradiction_detection: "Contradiction Detection",
  consolidation: "Consolidation",
  pruning: "Pruning",
  weight_adjustment: "Weight Adjustment",
};

export const SUB_PHASE_LABELS: Record<string, string> = {
  backfill_audit: "Audit",
  reinforce: "Reinforce",
  consolidate: "Consolidate",
};

// Raw hex (not Tailwind classes): the inline `style={{ background }}` path
// on dynamic-width segments is JIT-safe — Tailwind's purge can't see
// runtime-composed class names.
export const PHASE_COLORS: Record<string, string> = {
  entity_dedup: "#94a3b8",
  embedding_backfill: "#3b82f6",
  paraphrase_dedup: "#22c55e",
  transitive_discovery: "#a3a3a3",
  contradiction_detection: "#f59e0b",
  consolidation: "#ec4899",
  pruning: "#737373",
  weight_adjustment: "#525252",
  backfill_audit: "#fb7185",
  reinforce: "#f472b6",
  consolidate: "#e879f9",
};

export function phaseColor(key: string): string {
  return PHASE_COLORS[key] ?? "#64748b";
}
