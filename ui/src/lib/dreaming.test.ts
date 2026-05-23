/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import {
  formatDreamLog,
  formatFactValue,
  groupLogsByPhase,
  shortId,
  formatPercent,
  formatAlignment,
  formatConfidence,
  formatReason,
  memoryFocusHref,
  isZeroId,
} from "./dreaming";
import type { DreamLog } from "../api/client";

const PROJECT = "11111111-1111-1111-1111-111111111111";
const CYCLE = "22222222-2222-2222-2222-222222222222";
const A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const C = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const D = "dddddddd-dddd-dddd-dddd-dddddddddddd";

function mkLog(partial: Partial<DreamLog>): DreamLog {
  return {
    id: "00000000-0000-0000-0000-000000000999",
    cycle_id: CYCLE,
    project_id: PROJECT,
    phase: "consolidation",
    sub_phase: "",
    operation: "memory_created",
    target_type: "memory",
    target_id: A,
    before_state: {},
    after_state: {},
    created_at: "2026-05-03T12:00:00Z",
    ...partial,
  };
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

describe("primitive formatters", () => {
  it("shortId truncates long ids", () => {
    expect(shortId(A)).toBe("aaaaaa…");
    expect(shortId("short")).toBe("short");
  });

  it("formatPercent rounds to nearest %", () => {
    expect(formatPercent(0.94)).toBe("94%");
    expect(formatPercent(0.945)).toBe("95%");
    expect(formatPercent(1)).toBe("100%");
  });

  it("formatAlignment carries a sign", () => {
    expect(formatAlignment(0.62)).toBe("+0.62");
    expect(formatAlignment(-0.31)).toBe("-0.31");
    expect(formatAlignment(0)).toBe("+0.00");
  });

  it("formatConfidence is always two decimals", () => {
    expect(formatConfidence(0.5)).toBe("0.50");
    expect(formatConfidence(0.834)).toBe("0.83");
  });

  it("formatReason humanizes snake_case", () => {
    expect(formatReason("low_novelty")).toBe("Low novelty");
    expect(formatReason("high_cosine_paraphrase")).toBe("High cosine paraphrase");
    expect(formatReason("")).toBe("");
  });

  it("formatFactValue dispatches by kind", () => {
    expect(formatFactValue({ label: "x", value: 0.92, kind: "percent" })).toBe("92%");
    expect(formatFactValue({ label: "x", value: 1234, kind: "count" })).toBe("1,234");
    expect(formatFactValue({ label: "x", value: A, kind: "memory_id" })).toBe("aaaaaa…");
    expect(formatFactValue({ label: "x", value: "low_novelty", kind: "reason" })).toBe(
      "Low novelty",
    );
  });

  it("memoryFocusHref includes project + focus params", () => {
    expect(memoryFocusHref(PROJECT, A)).toBe(
      `/memories?project=${PROJECT}&focus=${A}`,
    );
  });

  it("isZeroId only matches all-zero UUID", () => {
    expect(isZeroId("00000000-0000-0000-0000-000000000000")).toBe(true);
    expect(isZeroId(A)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// per-operation formatters
// ---------------------------------------------------------------------------

describe("formatDreamLog: entity_dedup", () => {
  it("entity_merged reads loser/winner names from before/after", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "entity_dedup",
        operation: "entity_merged",
        target_type: "entity",
        target_id: A,
        before_state: { id: A, name: "Brandon L.", mention_count: 3 },
        after_state: { id: B, name: "Brandon Lehmann", mention_count: 12 },
      }),
    );
    expect(out.narrative).toBe("Merged entity {loser} into {winner}");
    expect(out.facts.loser).toEqual({
      label: "Loser",
      value: "Brandon L.",
      kind: "text",
    });
    expect(out.facts.winner).toEqual({
      label: "Winner",
      value: "Brandon Lehmann",
      kind: "text",
    });
    expect(out.facts.mentions).toEqual({
      label: "Mention count",
      value: 12,
      kind: "count",
    });
  });

  it("entity_merged falls back to ids when names are missing", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "entity_dedup",
        operation: "entity_merged",
        target_type: "entity",
        target_id: A,
        before_state: {},
        after_state: { id: B },
      }),
    );
    expect(out.facts.loser).toEqual({ label: "Loser", value: A, kind: "entity_id" });
    expect(out.facts.winner).toEqual({ label: "Winner", value: B, kind: "entity_id" });
  });

  it("relationship_created in entity_dedup uses 'created' narrative", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "entity_dedup",
        operation: "relationship_created",
        target_type: "relationship",
        target_id: A,
        before_state: {},
        after_state: { source_id: B, target_id: C, relation: "knows", weight: 0.8 },
      }),
    );
    expect(out.narrative).toBe("Created relationship {relation}: {source} → {target}");
    expect(out.facts.relation.value).toBe("knows");
    expect(out.facts.weight).toEqual({ label: "Weight", value: 0.8, kind: "weight" });
  });
});

describe("formatDreamLog: transitive_discovery", () => {
  it("relationship_created uses 'inferred' narrative in transitive phase", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "transitive_discovery",
        operation: "relationship_created",
        target_type: "relationship",
        target_id: A,
        before_state: {},
        after_state: { source_id: B, target_id: C, relation: "located_in", weight: 0.6 },
      }),
    );
    expect(out.narrative).toBe("Inferred relationship {relation}: {source} → {target}");
  });
});

describe("formatDreamLog: paraphrase_dedup", () => {
  it("paraphrase_superseded includes cosine and reason", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "paraphrase_dedup",
        operation: "paraphrase_superseded",
        target_type: "memory",
        target_id: A,
        before_state: {},
        after_state: {
          superseded_by: B,
          cosine: 0.94,
          reason: "high_cosine_paraphrase",
        },
      }),
    );
    expect(out.narrative).toBe(
      "Merged paraphrase {loser} into {winner} — {cosine} similar",
    );
    expect(out.facts.loser.kind).toBe("memory_id");
    expect(out.facts.winner).toEqual({ label: "Winner", value: B, kind: "memory_id" });
    expect(out.facts.cosine).toEqual({ label: "Cosine", value: 0.94, kind: "percent" });
  });

  it("paraphrase_superseded ignores after.winner side enum and trusts superseded_by", () => {
    // The contradictions phase also emits paraphrase_superseded and tacks
    // on after.winner = "a"|"b"|"tie" for diagnostics. The UI must read
    // the real UUID from after.superseded_by, not the side enum.
    const out = formatDreamLog(
      mkLog({
        phase: "contradiction_detection",
        operation: "paraphrase_superseded",
        target_type: "memory",
        target_id: A,
        before_state: {},
        after_state: {
          superseded_by: B,
          winner: "a",
          cosine: 0.97,
          reason: "high_cosine_paraphrase",
        },
      }),
    );
    expect(out.facts.winner).toEqual({ label: "Winner", value: B, kind: "memory_id" });
  });
});

describe("formatDreamLog: contradiction_detection", () => {
  it("contradiction_detected with winner='a' resolves to target_id", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "contradiction_detection",
        operation: "contradiction_detected",
        target_type: "memory",
        target_id: A,
        before_state: {},
        after_state: {
          conflicting_id: B,
          winner: "a",
          winner_factor: 0.78,
          loser_factor: 0.22,
          detection_count: 1,
          explanation: "A asserts X; B asserts not-X.",
        },
      }),
    );
    expect(out.narrative).toBe(
      "Resolved contradiction between {a} and {b} — kept {winner}",
    );
    expect(out.facts.a.value).toBe(A);
    expect(out.facts.b.value).toBe(B);
    expect(out.facts.winner).toEqual({ label: "Kept", value: A, kind: "memory_id" });
    expect(out.facts.winnerFactor).toEqual({
      label: "Winner factor",
      value: 0.78,
      kind: "confidence",
    });
    expect(out.facts.explanation.value).toBe("A asserts X; B asserts not-X.");
  });

  it("contradiction_detected with winner='b' resolves to conflicting_id", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "contradiction_detection",
        operation: "contradiction_detected",
        target_type: "memory",
        target_id: A,
        before_state: {},
        after_state: {
          conflicting_id: B,
          winner: "b",
        },
      }),
    );
    expect(out.facts.winner).toEqual({ label: "Kept", value: B, kind: "memory_id" });
  });

  it("contradiction_detected with winner='tie' renders as text, not a memory link", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "contradiction_detection",
        operation: "contradiction_detected",
        target_type: "memory",
        target_id: A,
        before_state: {},
        after_state: {
          conflicting_id: B,
          winner: "tie",
        },
      }),
    );
    expect(out.narrative).toBe("Resolved contradiction between {a} and {b} — tie");
    expect(out.facts.winner.kind).toBe("text");
    expect(out.facts.winner.value).toBe("tie");
  });
});

describe("formatDreamLog: consolidation", () => {
  it("confidence_adjusted shows old → new + alignment", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "consolidation",
        operation: "confidence_adjusted",
        target_type: "memory",
        target_id: A,
        before_state: { confidence: 0.5 },
        after_state: { confidence: 0.81, alignment: 0.62 },
      }),
    );
    expect(out.narrative).toBe(
      "Reinforced synthesis {memId} — confidence {oldConf} → {newConf} (alignment {alignment})",
    );
    expect(out.facts.oldConf.value).toBe(0.5);
    expect(out.facts.newConf.value).toBe(0.81);
    expect(out.facts.alignment).toEqual({
      label: "Alignment",
      value: 0.62,
      kind: "alignment",
    });
  });

  it("memory_superseded points at the synthesis", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "consolidation",
        operation: "memory_superseded",
        target_type: "memory",
        target_id: A,
        before_state: { superseded_by: null },
        after_state: { superseded_by: B },
      }),
    );
    expect(out.narrative).toBe("Superseded source memory {memId} by synthesis {synthesis}");
    expect(out.facts.synthesis.value).toBe(B);
  });

  it("memory_demoted carries old confidence + reason", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "consolidation",
        operation: "memory_demoted",
        target_type: "memory",
        target_id: A,
        before_state: { confidence: 0.42 },
        after_state: { confidence: 0, low_novelty: true, reason: "orphan_no_sources" },
      }),
    );
    expect(out.narrative).toBe(
      "Demoted memory {memId} — low novelty (confidence {oldConf} → 0)",
    );
    expect(out.facts.oldConf.value).toBe(0.42);
    expect(out.facts.reason).toEqual({
      label: "Reason",
      value: "orphan_no_sources",
      kind: "reason",
    });
  });

  it("memory_rejected handles uuid.Nil target + source list", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "consolidation",
        operation: "memory_rejected",
        target_type: "memory",
        target_id: "00000000-0000-0000-0000-000000000000",
        before_state: {},
        after_state: { reason: "low_novelty", source_memory_ids: [A, B, C] },
      }),
    );
    expect(out.narrative).toBe(
      "Rejected synthesis candidate — {reason} ({sourceCount} sources)",
    );
    expect(out.facts.sourceCount.value).toBe(3);
    expect(out.facts.reason.value).toBe("low_novelty");
  });

  it("memory_created surfaces source count from metadata", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "consolidation",
        operation: "memory_created",
        target_type: "memory",
        target_id: D,
        before_state: {},
        after_state: {
          id: D,
          confidence: 0.7,
          content: "Brandon prefers terse responses with no trailing summaries.",
          metadata: { source_memory_ids: [A, B] },
        },
      }),
    );
    expect(out.narrative).toBe(
      "Created synthesis {memId} from {sourceCount} source memories",
    );
    expect(out.facts.sourceCount.value).toBe(2);
    expect(out.facts.confidence.value).toBe(0.7);
    expect(out.facts.content.value).toContain("Brandon");
  });
});

describe("formatDreamLog: pruning", () => {
  it("memory_deleted shows reason and prior content", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "pruning",
        operation: "memory_deleted",
        target_type: "memory",
        target_id: A,
        before_state: { content: "stale fact", created_at: "2025-01-01T00:00:00Z" },
        after_state: { reason: "expired_low_confidence" },
      }),
    );
    expect(out.narrative).toBe("Deleted memory {memId} — {reason}");
    expect(out.facts.reason.value).toBe("expired_low_confidence");
    expect(out.facts.createdAt.value).toBe("2025-01-01T00:00:00Z");
  });

  it("relationship_expired (pruning, namespace) reports aggregate count", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "pruning",
        operation: "relationship_expired",
        target_type: "namespace",
        target_id: A,
        before_state: {},
        after_state: { expired_count: 7, threshold: 0.05 },
      }),
    );
    expect(out.narrative).toBe(
      "Expired {expiredCount} relationships below weight {threshold}",
    );
    expect(out.facts.expiredCount.value).toBe(7);
    expect(out.facts.threshold.value).toBe(0.05);
  });
});

describe("formatDreamLog: weight_adjustment", () => {
  it("relationship_expired (weight, single) reports decay", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "weight_adjustment",
        operation: "relationship_expired",
        target_type: "relationship",
        target_id: A,
        before_state: { weight: 0.06 },
        after_state: { weight: 0.02, reason: "decayed_below_threshold" },
      }),
    );
    expect(out.narrative).toBe("Expired {relId} — weight decayed to {newWeight}");
    expect(out.facts.oldWeight.value).toBe(0.06);
    expect(out.facts.newWeight.value).toBe(0.02);
  });

  it("relationship_updated shows old → new", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "weight_adjustment",
        operation: "relationship_updated",
        target_type: "relationship",
        target_id: A,
        before_state: { weight: 0.4 },
        after_state: { weight: 0.55 },
      }),
    );
    expect(out.narrative).toBe("Adjusted weight on {relId}: {oldWeight} → {newWeight}");
  });

  it("entity_updated shows mention count", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "weight_adjustment",
        operation: "entity_updated",
        target_type: "entity",
        target_id: A,
        before_state: {},
        after_state: { mention_count: 9 },
      }),
    );
    expect(out.narrative).toBe("Updated entity {entityId} — mention count now {mentions}");
    expect(out.facts.mentions.value).toBe(9);
  });
});

describe("formatDreamLog: phase_summary", () => {
  it("phase_summary returns metrics in the configured order", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "paraphrase_dedup",
        operation: "phase_summary",
        target_type: "namespace",
        target_id: A,
        before_state: {},
        after_state: { candidates: 412, visited: 412, superseded: 17 },
      }),
    );
    expect(out.isSummary).toBe(true);
    expect(out.narrative).toBe("Paraphrase Dedup summary");
    expect(out.metrics?.map((m) => [m.label, m.value])).toEqual([
      ["Candidates", 412],
      ["Visited", 412],
      ["Superseded", 17],
    ]);
  });

  it("consolidation phase_summary tags the sub-phase", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "consolidation",
        operation: "phase_summary",
        target_type: "namespace",
        target_id: A,
        before_state: {},
        after_state: {
          sub_phase: "reinforce",
          syntheses_total: 30,
          syntheses_stale: 8,
          alignment_calls: 5,
          confidence_adjusted: 4,
          supersessions: 1,
        },
      }),
    );
    expect(out.isSummary).toBe(true);
    expect(out.narrative).toBe("Consolidation · Reinforce summary");
    const metricLabels = out.metrics?.map((m) => m.label) ?? [];
    expect(metricLabels[0]).toBe("Sub-phase");
    expect(metricLabels).toContain("Syntheses total");
    expect(metricLabels).toContain("Confidence adjusted");
  });

  it("phase_summary tail picks up unrecognized numeric keys", () => {
    const out = formatDreamLog(
      mkLog({
        phase: "embedding_backfill",
        operation: "phase_summary",
        target_type: "namespace",
        target_id: A,
        before_state: {},
        after_state: {
          candidates: 0,
          embedded: 0,
          mystery_count: 42,
          tokens_spent: 999, // hidden
        },
      }),
    );
    const labels = out.metrics?.map((m) => m.label) ?? [];
    expect(labels).toContain("Candidates");
    expect(labels).toContain("Embedded");
    expect(labels).toContain("Mystery count");
    expect(labels).not.toContain("Tokens spent");
  });
});

describe("formatDreamLog: unknown operation", () => {
  it("falls back to a humanized op name with unknown=true", () => {
    const out = formatDreamLog(
      mkLog({
        operation: "spaceship_launched",
        before_state: { foo: "bar" },
        after_state: { baz: 1 },
      }),
    );
    expect(out.unknown).toBe(true);
    expect(out.narrative).toBe("spaceship launched");
    expect(out.facts).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// log grouping (drives the DreamingMonitor accordion)
// ---------------------------------------------------------------------------

describe("groupLogsByPhase", () => {
  it("filters out phase_summary entries", () => {
    const logs: DreamLog[] = [
      mkLog({
        id: "00000000-0000-0000-0000-000000000001",
        phase: "pruning",
        sub_phase: "",
        operation: "memory_deleted",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000002",
        phase: "pruning",
        sub_phase: "",
        operation: "phase_summary",
        after_state: { pruned: 1 },
      }),
    ];
    const groups = groupLogsByPhase(logs);
    const pruning = groups.get("pruning");
    expect(pruning).toBeDefined();
    expect(pruning!.hasSubPhases).toBe(false);
    expect(pruning!.logsFlat.map((l) => l.operation)).toEqual(["memory_deleted"]);
  });

  it("groups consolidation logs by sub_phase column", () => {
    const logs: DreamLog[] = [
      mkLog({
        id: "00000000-0000-0000-0000-000000000010",
        phase: "consolidation",
        sub_phase: "backfill_audit",
        operation: "memory_demoted",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000011",
        phase: "consolidation",
        sub_phase: "reinforce",
        operation: "confidence_adjusted",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000012",
        phase: "consolidation",
        sub_phase: "consolidate",
        operation: "memory_created",
      }),
    ];
    const groups = groupLogsByPhase(logs);
    const c = groups.get("consolidation");
    expect(c).toBeDefined();
    expect(c!.hasSubPhases).toBe(true);
    expect(c!.subGroups.map((sg) => sg.subPhase)).toEqual([
      "backfill_audit",
      "reinforce",
      "consolidate",
    ]);
    expect(c!.subGroups[0].logs[0].operation).toBe("memory_demoted");
    expect(c!.subGroups[1].logs[0].operation).toBe("confidence_adjusted");
    expect(c!.subGroups[2].logs[0].operation).toBe("memory_created");
  });

  it("infers sub_phase for legacy consolidation rows from the next phase_summary", () => {
    // Pre-migration ordering: ops carry empty sub_phase, but each
    // writePhaseSummary closes the sub-phase scope with the embedded value.
    const logs: DreamLog[] = [
      mkLog({
        id: "00000000-0000-0000-0000-000000000020",
        phase: "consolidation",
        sub_phase: "",
        operation: "memory_demoted",
        created_at: "2026-05-03T12:00:00Z",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000021",
        phase: "consolidation",
        sub_phase: "",
        operation: "phase_summary",
        after_state: { sub_phase: "backfill_audit", audited: 3 },
        created_at: "2026-05-03T12:00:01Z",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000022",
        phase: "consolidation",
        sub_phase: "",
        operation: "confidence_adjusted",
        created_at: "2026-05-03T12:00:02Z",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000023",
        phase: "consolidation",
        sub_phase: "",
        operation: "phase_summary",
        after_state: { sub_phase: "reinforce", confidence_adjusted: 1 },
        created_at: "2026-05-03T12:00:03Z",
      }),
    ];
    const c = groupLogsByPhase(logs).get("consolidation");
    expect(c).toBeDefined();
    expect(c!.subGroups.map((sg) => sg.subPhase)).toEqual(["backfill_audit", "reinforce"]);
    expect(c!.subGroups[0].logs[0].operation).toBe("memory_demoted");
    expect(c!.subGroups[1].logs[0].operation).toBe("confidence_adjusted");
  });

  it("populates logsFlat for non-consolidation phases", () => {
    const logs: DreamLog[] = [
      mkLog({
        id: "00000000-0000-0000-0000-000000000030",
        phase: "weight_adjustment",
        sub_phase: "",
        operation: "relationship_updated",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000031",
        phase: "weight_adjustment",
        sub_phase: "",
        operation: "entity_updated",
      }),
    ];
    const w = groupLogsByPhase(logs).get("weight_adjustment");
    expect(w).toBeDefined();
    expect(w!.hasSubPhases).toBe(false);
    expect(w!.subGroups).toEqual([]);
    expect(w!.logsFlat.map((l) => l.operation)).toEqual([
      "relationship_updated",
      "entity_updated",
    ]);
  });

  it("does not let a later phase_summary reach across an explicit non-summary row", () => {
    // Mixed-mode cycle (rare, but possible during the migration window): a
    // post-migration confidence_adjusted row carrying explicit sub_phase
    // appears between legacy empty-sub_phase rows. The trailing
    // phase_summary must NOT backfill the legacy rows across the explicit
    // boundary, because their true scope is unknowable.
    const logs: DreamLog[] = [
      mkLog({
        id: "00000000-0000-0000-0000-000000000050",
        phase: "consolidation",
        sub_phase: "", // legacy: pre-migration audit op
        operation: "memory_demoted",
        created_at: "2026-05-03T12:00:00Z",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000051",
        phase: "consolidation",
        sub_phase: "reinforce", // post-migration: scope changed to reinforce
        operation: "confidence_adjusted",
        created_at: "2026-05-03T12:00:01Z",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000052",
        phase: "consolidation",
        sub_phase: "",
        operation: "phase_summary",
        after_state: { sub_phase: "consolidate" },
        created_at: "2026-05-03T12:00:02Z",
      }),
    ];
    const c = groupLogsByPhase(logs).get("consolidation");
    expect(c).toBeDefined();
    // Legacy row 50 must NOT be labeled "consolidate" by the trailing
    // phase_summary — that summary's scope started after row 51.
    const reinforce = c!.subGroups.find((sg) => sg.subPhase === "reinforce");
    const consolidate = c!.subGroups.find((sg) => sg.subPhase === "consolidate");
    const unattributed = c!.subGroups.find((sg) => sg.subPhase === "");
    expect(reinforce?.logs.map((l) => l.operation)).toEqual(["confidence_adjusted"]);
    // The trailing phase_summary at row 52 carried sub_phase="consolidate",
    // so its filtered-out summary closes the consolidate scope with no ops.
    expect(consolidate?.logs ?? []).toEqual([]);
    // Row 50 stays unattributed (sub_phase=""), not pulled into consolidate.
    expect(unattributed?.logs.map((l) => l.operation)).toEqual(["memory_demoted"]);
  });

  it("preserves canonical sub-phase order even with interleaved input", () => {
    // Build out-of-order to confirm the sort. Real cycles emit in order,
    // but the sort defends against any future reordering at the API layer.
    const logs: DreamLog[] = [
      mkLog({
        id: "00000000-0000-0000-0000-000000000040",
        phase: "consolidation",
        sub_phase: "consolidate",
        operation: "memory_created",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000041",
        phase: "consolidation",
        sub_phase: "backfill_audit",
        operation: "memory_demoted",
      }),
      mkLog({
        id: "00000000-0000-0000-0000-000000000042",
        phase: "consolidation",
        sub_phase: "reinforce",
        operation: "confidence_adjusted",
      }),
    ];
    const c = groupLogsByPhase(logs).get("consolidation");
    expect(c!.subGroups.map((sg) => sg.subPhase)).toEqual([
      "backfill_audit",
      "reinforce",
      "consolidate",
    ]);
  });
});
