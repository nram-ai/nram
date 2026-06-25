/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import { __test } from "./extractionError";
import type { PartialRecoveryWarning } from "./extractionError";

const { parse, legSummary } = __test;

// The exact wire shape persisted by the backend into
// enrichment_queue.last_error (internal/enrichment/worker.go partialRecoveryLeg):
// the recovered counts use the keys entities_recovered / relationships_recovered.
const REAL_LAST_ERROR = JSON.stringify({
  warnings: [
    {
      phase: "entity_extraction",
      reason: "partial_recovery",
      finish_reason: "stop",
      prompt_tokens: 4729,
      completion_tokens: 4104,
      model: "Qwen/Qwen3-8B",
      provider: "openai",
      entities_recovered: 314,
      relationships_recovered: 12,
    },
  ],
});

describe("extractionError parse", () => {
  it("classifies a backend partial-recovery payload as warnings", () => {
    const parsed = parse(REAL_LAST_ERROR);
    expect(parsed.kind).toBe("warnings");
  });

  it("reads entities_recovered / relationships_recovered (not the old *_rec keys)", () => {
    const parsed = parse(REAL_LAST_ERROR);
    if (parsed.kind !== "warnings") throw new Error("expected warnings");
    const leg = (parsed.value as PartialRecoveryWarning).warnings[0];
    const summary = legSummary(leg);
    expect(summary).toContain("314 entities");
    expect(summary).toContain("12 relations");
  });

  it("summarizes a fact-extraction leg with facts_recovered", () => {
    const leg = {
      phase: "fact_extraction",
      reason: "partial_recovery",
      facts_recovered: 7,
    };
    expect(legSummary(leg)).toContain("7 facts");
  });

  it("falls back to raw for a non-JSON last_error", () => {
    expect(parse("connection refused").kind).toBe("raw");
  });
});
