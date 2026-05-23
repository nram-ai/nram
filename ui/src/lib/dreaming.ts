import type { DreamLog } from "../api/client";

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

// ---------------------------------------------------------------------------
// Dream log formatter
// ---------------------------------------------------------------------------

export type FactKind =
  | "text"
  | "memory_id"
  | "entity_id"
  | "relationship_id"
  | "namespace_id"
  | "percent"
  | "alignment"
  | "confidence"
  | "weight"
  | "count"
  | "reason"
  | "tokens"
  | "duration_ms";

export interface Fact {
  label: string;
  value: unknown;
  kind: FactKind;
}

export interface FormattedLog {
  // Single sentence describing what happened. Plain string with
  // {placeholders} — the renderer substitutes Fact-rendered chips by name.
  narrative: string;
  // Facts referenced by the narrative AND any that should appear in the
  // key-fact strip below the narrative.
  facts: Record<string, Fact>;
  // Optional inline metric strip for phase_summary / aggregate rows
  // (rendered on the collapsed row, no expand needed).
  metrics?: Fact[];
  // True for phase_summary entries — the renderer hides the expand chevron.
  isSummary?: boolean;
  // True when the operation shape was unrecognized; the renderer falls back
  // to raw JSON for the body in this case.
  unknown?: boolean;
}

const ZERO_UUID = "00000000-0000-0000-0000-000000000000";

// after_state.winner from the contradictions phase is a side enum, not a
// memory UUID. Mirrors phase_contradictions.go:34-36.
type WinnerSide = "a" | "b" | "tie";
const WinnerSideA: WinnerSide = "a";
const WinnerSideB: WinnerSide = "b";
const WinnerTie: WinnerSide = "tie";

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function pickString(v: unknown): string | undefined {
  return typeof v === "string" && v.length > 0 ? v : undefined;
}

function pickNumber(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function pickArrayLength(v: unknown): number | undefined {
  return Array.isArray(v) ? v.length : undefined;
}

function fact(label: string, value: unknown, kind: FactKind): Fact {
  return { label, value, kind };
}

// Format helpers. The renderer uses these so that test fixtures and chip
// rendering stay in sync.
export function shortId(id: string): string {
  if (typeof id !== "string" || id.length <= 8) return id;
  return id.slice(0, 6) + "…";
}

export function formatPercent(v: number): string {
  return `${Math.round(v * 100)}%`;
}

export function formatConfidence(v: number): string {
  return v.toFixed(2);
}

export function formatWeight(v: number): string {
  return v.toFixed(2);
}

export function formatAlignment(v: number): string {
  const sign = v >= 0 ? "+" : "";
  return `${sign}${v.toFixed(2)}`;
}

export function formatReason(snake: string): string {
  if (!snake) return snake;
  const spaced = snake.replace(/_/g, " ");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

export function formatCount(v: number): string {
  return v.toLocaleString();
}

// formatFactValue renders a Fact's value as a plain string (used by tests
// and tooltips). The React renderer overrides chip presentation for
// `memory_id`/`entity_id`/etc. but reuses these for the visible text.
export function formatFactValue(f: Fact): string {
  switch (f.kind) {
    case "percent":
      return typeof f.value === "number" ? formatPercent(f.value) : String(f.value ?? "");
    case "alignment":
      return typeof f.value === "number" ? formatAlignment(f.value) : String(f.value ?? "");
    case "confidence":
      return typeof f.value === "number" ? formatConfidence(f.value) : String(f.value ?? "");
    case "weight":
      return typeof f.value === "number" ? formatWeight(f.value) : String(f.value ?? "");
    case "count":
      return typeof f.value === "number" ? formatCount(f.value) : String(f.value ?? "");
    case "reason":
      return typeof f.value === "string" ? formatReason(f.value) : String(f.value ?? "");
    case "memory_id":
    case "entity_id":
    case "relationship_id":
    case "namespace_id":
      return typeof f.value === "string" ? shortId(f.value) : String(f.value ?? "");
    case "tokens":
      return typeof f.value === "number" ? `${formatCount(f.value)} tok` : String(f.value ?? "");
    case "duration_ms":
      if (typeof f.value !== "number") return String(f.value ?? "");
      return f.value < 1000
        ? `${f.value}ms`
        : `${(f.value / 1000).toFixed(1)}s`;
    default:
      return String(f.value ?? "");
  }
}

// ---------------------------------------------------------------------------
// Per-operation formatters
// ---------------------------------------------------------------------------

function formatEntityMerged(log: DreamLog): FormattedLog {
  const before = isObj(log.before_state) ? log.before_state : {};
  const after = isObj(log.after_state) ? log.after_state : {};
  const loserName = pickString(before.name);
  const winnerName = pickString(after.name);
  const winnerId = pickString(after.id) ?? "";
  const loserId = log.target_id;
  const facts: Record<string, Fact> = {
    loser: fact("Loser", loserName ?? loserId, loserName ? "text" : "entity_id"),
    winner: fact("Winner", winnerName ?? winnerId, winnerName ? "text" : "entity_id"),
  };
  if (loserName) facts.loserId = fact("Loser ID", loserId, "entity_id");
  if (winnerName && winnerId) facts.winnerId = fact("Winner ID", winnerId, "entity_id");
  const mentions = pickNumber(after.mention_count);
  if (mentions !== undefined) facts.mentions = fact("Mention count", mentions, "count");
  return {
    narrative: "Merged entity {loser} into {winner}",
    facts,
  };
}

function formatRelationshipCreated(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  const sourceId = pickString(after.source_id) ?? "";
  const targetId = pickString(after.target_id) ?? "";
  const relation = pickString(after.relation) ?? "(unknown)";
  const weight = pickNumber(after.weight);
  const facts: Record<string, Fact> = {
    relation: fact("Relation", relation, "text"),
    source: fact("From", sourceId, "entity_id"),
    target: fact("To", targetId, "entity_id"),
  };
  if (weight !== undefined) facts.weight = fact("Weight", weight, "weight");
  // entity_dedup retargets relationships during merge; transitive_discovery
  // creates inferred edges. Same payload, different narrative emphasis.
  const narrative =
    log.phase === "transitive_discovery"
      ? "Inferred relationship {relation}: {source} → {target}"
      : "Created relationship {relation}: {source} → {target}";
  return { narrative, facts };
}

function formatRelationshipUpdated(log: DreamLog): FormattedLog {
  const before = isObj(log.before_state) ? log.before_state : {};
  const after = isObj(log.after_state) ? log.after_state : {};
  const oldWeight = pickNumber(before.weight);
  const newWeight = pickNumber(after.weight);
  const facts: Record<string, Fact> = {
    relId: fact("Relationship", log.target_id, "relationship_id"),
  };
  if (oldWeight !== undefined) facts.oldWeight = fact("Old weight", oldWeight, "weight");
  if (newWeight !== undefined) facts.newWeight = fact("New weight", newWeight, "weight");
  return {
    narrative: "Adjusted weight on {relId}: {oldWeight} → {newWeight}",
    facts,
  };
}

function formatRelationshipExpired(log: DreamLog): FormattedLog {
  const before = isObj(log.before_state) ? log.before_state : {};
  const after = isObj(log.after_state) ? log.after_state : {};

  // pruning: target_type=namespace, after={expired_count, threshold} for the
  // weight-threshold pass, or after={expired_count, trigger:"transitive_pressure",
  // total_before, hard_cap, high_water, low_water, drained_to} for the
  // pressure-driven transitive prune.
  if (log.target_type === "namespace") {
    const expired = pickNumber(after.expired_count) ?? 0;
    const trigger = pickString(after.trigger);
    if (trigger === "transitive_pressure") {
      const before = pickNumber(after.total_before);
      const drainedTo = pickNumber(after.drained_to);
      const hardCap = pickNumber(after.hard_cap);
      const facts: Record<string, Fact> = {
        expiredCount: fact("Expired", expired, "count"),
      };
      if (before !== undefined) facts.before = fact("Before", before, "count");
      if (drainedTo !== undefined) facts.drainedTo = fact("After", drainedTo, "count");
      if (hardCap !== undefined) facts.hardCap = fact("Hard cap", hardCap, "count");
      return {
        narrative:
          before !== undefined && drainedTo !== undefined
            ? "Pressure-pruned {expiredCount} transitive edges ({before} → {drainedTo}, hard cap {hardCap})"
            : "Pressure-pruned {expiredCount} transitive edges",
        facts,
      };
    }
    const threshold = pickNumber(after.threshold);
    const facts: Record<string, Fact> = {
      expiredCount: fact("Expired", expired, "count"),
    };
    if (threshold !== undefined) facts.threshold = fact("Threshold", threshold, "weight");
    return {
      narrative:
        threshold !== undefined
          ? "Expired {expiredCount} relationships below weight {threshold}"
          : "Expired {expiredCount} relationships",
      facts,
    };
  }

  // weight_adjustment: target_type=relationship, before={weight: old}, after={weight, reason}
  const oldWeight = pickNumber(before.weight);
  const newWeight = pickNumber(after.weight);
  const reason = pickString(after.reason);
  const facts: Record<string, Fact> = {
    relId: fact("Relationship", log.target_id, "relationship_id"),
  };
  if (oldWeight !== undefined) facts.oldWeight = fact("Old weight", oldWeight, "weight");
  if (newWeight !== undefined) facts.newWeight = fact("New weight", newWeight, "weight");
  if (reason) facts.reason = fact("Reason", reason, "reason");
  return {
    narrative: "Expired {relId} — weight decayed to {newWeight}",
    facts,
  };
}

function formatEntityUpdated(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  const mentions = pickNumber(after.mention_count);
  const facts: Record<string, Fact> = {
    entityId: fact("Entity", log.target_id, "entity_id"),
  };
  if (mentions !== undefined) facts.mentions = fact("Mentions", mentions, "count");
  return {
    narrative:
      mentions !== undefined
        ? "Updated entity {entityId} — mention count now {mentions}"
        : "Updated entity {entityId}",
    facts,
  };
}

function formatParaphraseSuperseded(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  // after.winner is a side enum tacked on when this op is emitted from
  // the contradictions phase; the real winner UUID is in superseded_by.
  const winner = pickString(after.superseded_by) ?? "";
  const cosine = pickNumber(after.cosine);
  const reason = pickString(after.reason);
  const facts: Record<string, Fact> = {
    loser: fact("Loser", log.target_id, "memory_id"),
    winner: fact("Winner", winner, "memory_id"),
  };
  if (cosine !== undefined) facts.cosine = fact("Cosine", cosine, "percent");
  if (reason) facts.reason = fact("Reason", reason, "reason");
  const narrative =
    cosine !== undefined
      ? "Merged paraphrase {loser} into {winner} — {cosine} similar"
      : "Merged paraphrase {loser} into {winner}";
  return { narrative, facts };
}

function formatContradictionDetected(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  const conflictingId = pickString(after.conflicting_id) ?? "";
  const winnerSide = pickString(after.winner) ?? "";
  // Resolve the side enum to the kept memory's UUID so the chip can deep-link.
  // Ties have no kept memory and render as a non-clickable text chip.
  const keptByWinner: Record<string, { value: string; kind: FactKind }> = {
    [WinnerSideA]: { value: log.target_id, kind: "memory_id" },
    [WinnerSideB]: { value: conflictingId, kind: "memory_id" },
    [WinnerTie]: { value: "tie", kind: "text" },
  };
  const kept = keptByWinner[winnerSide] ?? { value: "", kind: "memory_id" };
  const winnerFactor = pickNumber(after.winner_factor);
  const loserFactor = pickNumber(after.loser_factor);
  const detectionCount = pickNumber(after.detection_count);
  const explanation = pickString(after.explanation);
  const facts: Record<string, Fact> = {
    a: fact("Memory A", log.target_id, "memory_id"),
    b: fact("Memory B", conflictingId, "memory_id"),
    winner: fact("Kept", kept.value, kept.kind),
  };
  if (winnerFactor !== undefined) facts.winnerFactor = fact("Winner factor", winnerFactor, "confidence");
  if (loserFactor !== undefined) facts.loserFactor = fact("Loser factor", loserFactor, "confidence");
  if (detectionCount !== undefined) facts.detectionCount = fact("Detections", detectionCount, "count");
  if (explanation) facts.explanation = fact("Explanation", explanation, "text");
  const suffix = winnerSide === WinnerTie ? "tie" : "kept {winner}";
  return {
    narrative: `Resolved contradiction between {a} and {b} — ${suffix}`,
    facts,
  };
}

function formatConfidenceAdjusted(log: DreamLog): FormattedLog {
  const before = isObj(log.before_state) ? log.before_state : {};
  const after = isObj(log.after_state) ? log.after_state : {};
  const oldConf = pickNumber(before.confidence);
  const newConf = pickNumber(after.confidence);
  const alignment = pickNumber(after.alignment);
  const facts: Record<string, Fact> = {
    memId: fact("Memory", log.target_id, "memory_id"),
  };
  if (oldConf !== undefined) facts.oldConf = fact("Old confidence", oldConf, "confidence");
  if (newConf !== undefined) facts.newConf = fact("New confidence", newConf, "confidence");
  if (alignment !== undefined) facts.alignment = fact("Alignment", alignment, "alignment");
  return {
    narrative:
      "Reinforced synthesis {memId} — confidence {oldConf} → {newConf} (alignment {alignment})",
    facts,
  };
}

function formatMemorySuperseded(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  const synthesisId = pickString(after.superseded_by) ?? "";
  return {
    narrative: "Superseded source memory {memId} by synthesis {synthesis}",
    facts: {
      memId: fact("Memory", log.target_id, "memory_id"),
      synthesis: fact("Synthesis", synthesisId, "memory_id"),
    },
  };
}

function formatMemoryDemoted(log: DreamLog): FormattedLog {
  const before = isObj(log.before_state) ? log.before_state : {};
  const after = isObj(log.after_state) ? log.after_state : {};
  const oldConf = pickNumber(before.confidence);
  const reason = pickString(after.reason);
  const facts: Record<string, Fact> = {
    memId: fact("Memory", log.target_id, "memory_id"),
  };
  if (oldConf !== undefined) facts.oldConf = fact("Old confidence", oldConf, "confidence");
  if (reason) facts.reason = fact("Reason", reason, "reason");
  return {
    narrative:
      oldConf !== undefined
        ? "Demoted memory {memId} — low novelty (confidence {oldConf} → 0)"
        : "Demoted memory {memId} — low novelty",
    facts,
  };
}

function formatMemoryRejected(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  const reason = pickString(after.reason);
  const sourceCount = pickArrayLength(after.source_memory_ids) ?? 0;
  const facts: Record<string, Fact> = {
    sourceCount: fact("Source memories", sourceCount, "count"),
  };
  if (reason) facts.reason = fact("Reason", reason, "reason");
  return {
    narrative:
      reason
        ? "Rejected synthesis candidate — {reason} ({sourceCount} sources)"
        : "Rejected synthesis candidate ({sourceCount} sources)",
    facts,
  };
}

function formatMemoryCreated(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  const confidence = pickNumber(after.confidence);
  const content = pickString(after.content);
  const meta = isObj(after.metadata) ? after.metadata : null;
  const sourceCount = meta && Array.isArray(meta.source_memory_ids)
    ? meta.source_memory_ids.length
    : undefined;
  const facts: Record<string, Fact> = {
    memId: fact("Synthesis", log.target_id, "memory_id"),
  };
  if (sourceCount !== undefined) facts.sourceCount = fact("Sources", sourceCount, "count");
  if (confidence !== undefined) facts.confidence = fact("Confidence", confidence, "confidence");
  if (content) facts.content = fact("Content", content, "text");
  const narrative =
    sourceCount !== undefined
      ? "Created synthesis {memId} from {sourceCount} source memories"
      : "Created synthesis {memId}";
  return { narrative, facts };
}

function formatMemoryDeleted(log: DreamLog): FormattedLog {
  const before = isObj(log.before_state) ? log.before_state : {};
  const after = isObj(log.after_state) ? log.after_state : {};
  const reason = pickString(after.reason);
  const content = pickString(before.content);
  const createdAt = pickString(before.created_at);
  const facts: Record<string, Fact> = {
    memId: fact("Memory", log.target_id, "memory_id"),
  };
  if (reason) facts.reason = fact("Reason", reason, "reason");
  if (createdAt) facts.createdAt = fact("Created", createdAt, "text");
  if (content) facts.content = fact("Content", content, "text");
  return {
    narrative: reason ? "Deleted memory {memId} — {reason}" : "Deleted memory {memId}",
    facts,
  };
}

// Per-phase metric ordering for phase_summary rows. Keys missing from the
// after_state are skipped; unknown keys land in a generic tail so we never
// hide data.
const PHASE_SUMMARY_KEYS: Record<string, string[]> = {
  embedding_backfill: ["candidates", "embedded", "errors"],
  paraphrase_dedup: ["candidates", "visited", "superseded"],
  transitive_discovery: ["candidates", "created"],
  contradiction_detection: ["pairs", "checked", "found"],
  consolidation: [
    // Sub-phase rollups land here keyed by sub_phase. Common keys across
    // backfill_audit / reinforce / consolidate get listed; anything extra
    // appears in the tail.
    "sub_phase",
    "candidates",
    "candidates_total",
    "stale",
    "syntheses_total",
    "syntheses_stale",
    "user_memories",
    "audited",
    "visited",
    "passed",
    "demoted",
    "rejected",
    "created",
    "alignment_calls",
    "judge_calls",
    "embedding_calls",
    "confidence_adjusted",
    "supersessions",
    "skipped_budget",
  ],
  pruning: ["pruned", "expired", "transitive_pressure_expired", "errors"],
  weight_adjustment: [
    "visited",
    "direction_up",
    "direction_down",
    "direction_same",
    "expired",
  ],
};

const SUMMARY_HIDDEN_KEYS = new Set([
  // Already surfaced by other UI (budget bars, phase summary table).
  "tokens_spent",
  "tokens_before",
  "tokens_after",
  "duration_ms",
]);

function summaryMetricKind(key: string): FactKind {
  if (key === "sub_phase") return "text";
  if (key.endsWith("_factor")) return "confidence";
  return "count";
}

function formatPhaseSummary(log: DreamLog): FormattedLog {
  const after = isObj(log.after_state) ? log.after_state : {};
  const orderedKeys = PHASE_SUMMARY_KEYS[log.phase] ?? [];
  const seen = new Set<string>();
  const metrics: Fact[] = [];

  for (const key of orderedKeys) {
    if (!(key in after)) continue;
    seen.add(key);
    const value = after[key];
    if (key === "sub_phase" && typeof value === "string") {
      metrics.push(fact("Sub-phase", SUB_PHASE_LABELS[value] ?? value, "text"));
      continue;
    }
    if (typeof value === "number") {
      metrics.push(fact(formatReason(key), value, summaryMetricKind(key)));
    }
  }

  // Tail: anything else with a numeric value the per-phase list didn't cover.
  for (const [key, value] of Object.entries(after)) {
    if (seen.has(key) || SUMMARY_HIDDEN_KEYS.has(key)) continue;
    if (typeof value === "number") {
      metrics.push(fact(formatReason(key), value, summaryMetricKind(key)));
    }
  }

  const subPhaseValue = pickString(after.sub_phase);
  const phaseLabel = PHASE_LABELS[log.phase] ?? log.phase;
  const subLabel = subPhaseValue ? ` · ${SUB_PHASE_LABELS[subPhaseValue] ?? subPhaseValue}` : "";

  return {
    narrative: `${phaseLabel}${subLabel} summary`,
    facts: {},
    metrics,
    isSummary: true,
  };
}

export function formatDreamLog(log: DreamLog): FormattedLog {
  switch (log.operation) {
    case "entity_merged":
      return formatEntityMerged(log);
    case "relationship_created":
      return formatRelationshipCreated(log);
    case "relationship_updated":
      return formatRelationshipUpdated(log);
    case "relationship_expired":
      return formatRelationshipExpired(log);
    case "entity_updated":
      return formatEntityUpdated(log);
    case "paraphrase_superseded":
      return formatParaphraseSuperseded(log);
    case "contradiction_detected":
      return formatContradictionDetected(log);
    case "confidence_adjusted":
      return formatConfidenceAdjusted(log);
    case "memory_superseded":
      return formatMemorySuperseded(log);
    case "memory_demoted":
      return formatMemoryDemoted(log);
    case "memory_rejected":
      return formatMemoryRejected(log);
    case "memory_created":
      return formatMemoryCreated(log);
    case "memory_deleted":
      return formatMemoryDeleted(log);
    case "phase_summary":
      return formatPhaseSummary(log);
    default:
      return {
        narrative: log.operation.replace(/_/g, " "),
        facts: {},
        unknown: true,
      };
  }
}

// memoryFocusHref builds the MemoryBrowser deep-link for a memory referenced
// from a dream log. Includes the project so the browser can switch projects
// if the user is currently viewing a different one.
export function memoryFocusHref(projectId: string, memoryId: string): string {
  const params = new URLSearchParams();
  if (projectId) params.set("project", projectId);
  params.set("focus", memoryId);
  return `/memories?${params.toString()}`;
}

// isZeroId returns true for the all-zero UUID used by operations that don't
// have a meaningful target row (memory_rejected logs target_id = uuid.Nil).
export function isZeroId(id: string): boolean {
  return id === ZERO_UUID;
}

// ---------------------------------------------------------------------------
// Phase / sub-phase log grouping (drives the accordion in DreamingMonitor)
// ---------------------------------------------------------------------------

export interface SubPhaseLogGroup {
  subPhase: string;
  logs: DreamLog[];
}

export interface PhaseLogGroup {
  phase: string;
  subGroups: SubPhaseLogGroup[];
  logsFlat: DreamLog[];
  hasSubPhases: boolean;
}

// Canonical sub-phase order for the consolidation phase. Mirrors the
// execution order in internal/dreaming/phase_consolidation.go (Execute).
const CONSOLIDATION_SUB_PHASE_ORDER = ["backfill_audit", "reinforce", "consolidate"];

// PHASE_SUMMARY_OP identifies the operation type emitted by writePhaseSummary.
// Filtered out of the accordion because the data is already surfaced in the
// phase row (tokens / ops / residual).
const PHASE_SUMMARY_OP = "phase_summary";

// inferSubPhases walks a list of dream logs in created_at ASC order and
// returns a parallel array of resolved sub_phase strings. For consolidation
// rows whose sub_phase is already populated, returns it verbatim. For legacy
// consolidation rows with empty sub_phase, looks forward to the next
// phase_summary log in the same phase and inherits its after_state.sub_phase
// (each writePhaseSummary call closes a sub-phase scope). For non-consolidation
// rows returns empty string. Phase_summary rows themselves resolve to their
// own embedded sub_phase value when present.
//
// pendingStart tracks the contiguous run of empty consolidation rows whose
// sub_phase is still unknown. Any explicit value at row i — whether on a
// regular op or a phase_summary — closes the pending range: the lookahead
// only backfills rows whose true sub_phase is unambiguously the same as the
// boundary marker (i.e., contiguous empty rows immediately before it). An
// explicit row mid-stream signals a sub-phase boundary we cannot inherit
// across, so we discard pendingStart instead of letting a later phase_summary
// retroactively label rows from an earlier scope.
function inferSubPhases(logs: DreamLog[]): string[] {
  const result = new Array<string>(logs.length).fill("");
  let pendingStart = -1;
  const backfill = (endExclusive: number, sp: string) => {
    if (pendingStart < 0 || sp === "") return;
    for (let j = pendingStart; j < endExclusive; j++) {
      if (logs[j].phase === "consolidation" && result[j] === "") {
        result[j] = sp;
      }
    }
  };
  for (let i = 0; i < logs.length; i++) {
    const log = logs[i];
    if (log.phase !== "consolidation") {
      result[i] = log.sub_phase ?? "";
      continue;
    }
    const explicit = log.sub_phase ?? "";
    if (explicit !== "") {
      result[i] = explicit;
      if (log.operation === PHASE_SUMMARY_OP) {
        backfill(i, explicit);
      }
      // Any explicit assignment closes the pending range. A later
      // phase_summary cannot reach across this boundary to mislabel earlier
      // rows whose true sub_phase is unknowable.
      pendingStart = -1;
      continue;
    }
    if (log.operation === PHASE_SUMMARY_OP) {
      const after = log.after_state as Record<string, unknown> | null | undefined;
      const sp =
        after && typeof after.sub_phase === "string" ? (after.sub_phase as string) : "";
      result[i] = sp;
      backfill(i, sp);
      pendingStart = -1;
      continue;
    }
    if (pendingStart < 0) pendingStart = i;
  }
  return result;
}

// groupLogsByPhase splits dream logs into per-phase groups, with consolidation
// further split into sub-phases. phase_summary entries are filtered from the
// returned groups (the parent phase row already surfaces that data).
//
// Insertion order of the returned Map follows the order of first appearance
// in the logs array, which mirrors phase execution order.
export function groupLogsByPhase(logs: DreamLog[]): Map<string, PhaseLogGroup> {
  const resolved = inferSubPhases(logs);
  const groups = new Map<string, PhaseLogGroup>();

  for (let i = 0; i < logs.length; i++) {
    const log = logs[i];
    if (log.operation === PHASE_SUMMARY_OP) continue;

    let g = groups.get(log.phase);
    if (!g) {
      g = {
        phase: log.phase,
        subGroups: [],
        logsFlat: [],
        hasSubPhases: log.phase === "consolidation",
      };
      groups.set(log.phase, g);
    }

    if (g.hasSubPhases) {
      const sp = resolved[i] || "";
      let sub = g.subGroups.find((s) => s.subPhase === sp);
      if (!sub) {
        sub = { subPhase: sp, logs: [] };
        g.subGroups.push(sub);
      }
      sub.logs.push(log);
    } else {
      g.logsFlat.push(log);
    }
  }

  // Sort consolidation sub-groups into canonical execution order so the UI
  // renders Audit → Reinforce → Consolidate even if logs arrive interleaved.
  const consolidation = groups.get("consolidation");
  if (consolidation) {
    consolidation.subGroups.sort((a, b) => {
      const ai = CONSOLIDATION_SUB_PHASE_ORDER.indexOf(a.subPhase);
      const bi = CONSOLIDATION_SUB_PHASE_ORDER.indexOf(b.subPhase);
      // Unknown sub-phases (including "") sort to the end in lexical order.
      if (ai === -1 && bi === -1) return a.subPhase.localeCompare(b.subPhase);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });
  }

  return groups;
}
