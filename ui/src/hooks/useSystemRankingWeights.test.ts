/**
 * @vitest-environment node
 *
 * Contract test for the ranking-weights schema endpoint.
 *
 * The previous version of this test exercised a client-side fallback
 * (SYSTEM_RANKING_WEIGHT_FALLBACK) that silently substituted stale
 * built-in defaults when the schema endpoint omitted a key. That fallback
 * has been removed: the schema is now the single source of truth, and
 * the hook returns null weights with a missingKeys list when any required
 * key is absent. This test asserts that contract.
 */
import { describe, it, expect } from "vitest";
import { resolveSystemRankingWeights } from "./useApi";

const REQUIRED_KEYS = [
  "ranking.weight.similarity",
  "ranking.weight.recency",
  "ranking.weight.importance",
  "ranking.weight.frequency",
  "ranking.weight.graph_relevance",
  "ranking.weight.confidence",
  "ranking.weight.origin",
];

function fullSchema() {
  return [
    { key: "ranking.weight.similarity", default_value: 0.5 },
    { key: "ranking.weight.recency", default_value: 0.15 },
    { key: "ranking.weight.importance", default_value: 0.1 },
    { key: "ranking.weight.frequency", default_value: 0.0 },
    { key: "ranking.weight.graph_relevance", default_value: 0.2 },
    { key: "ranking.weight.confidence", default_value: 0.05 },
    { key: "ranking.weight.origin", default_value: 0.0 },
  ];
}

describe("resolveSystemRankingWeights contract", () => {
  it("returns null weights when the schema endpoint is empty", () => {
    const got = resolveSystemRankingWeights([], []);
    expect(got.weights).toBeNull();
    // Every required key surfaces as missing so the consumer can render
    // an actionable banner.
    expect(got.missingKeys.sort()).toEqual([...REQUIRED_KEYS].sort());
  });

  it("returns null weights when even one schema key is missing", () => {
    const partial = fullSchema().filter(
      (e) => e.key !== "ranking.weight.confidence",
    );
    const got = resolveSystemRankingWeights([], partial);
    expect(got.weights).toBeNull();
    expect(got.missingKeys).toEqual(["ranking.weight.confidence"]);
  });

  it("returns the schema defaults when every required key is present", () => {
    const got = resolveSystemRankingWeights([], fullSchema());
    expect(got.weights).toEqual({
      similarity: 0.5,
      recency: 0.15,
      importance: 0.1,
      frequency: 0.0,
      graph_relevance: 0.2,
      confidence: 0.05,
      origin: 0.0,
    });
    expect(got.missingKeys).toEqual([]);
  });

  it("operator override beats schema default", () => {
    const got = resolveSystemRankingWeights(
      [{ key: "ranking.weight.similarity", value: 0.3 }],
      fullSchema(),
    );
    expect(got.weights?.similarity).toBe(0.3);
    // Unmodified keys still come from the schema.
    expect(got.weights?.confidence).toBe(0.05);
  });

  it("ignores non-numeric operator overrides and falls through to schema", () => {
    const got = resolveSystemRankingWeights(
      [{ key: "ranking.weight.similarity", value: "not a number" }],
      fullSchema(),
    );
    expect(got.weights?.similarity).toBe(0.5);
  });

  it("coerces string-encoded numeric overrides", () => {
    const got = resolveSystemRankingWeights(
      [{ key: "ranking.weight.confidence", value: "0.25" }],
      fullSchema(),
    );
    expect(got.weights?.confidence).toBe(0.25);
  });

  it("ignores unrelated keys in both inputs", () => {
    const got = resolveSystemRankingWeights(
      [{ key: "unrelated.key", value: 999 }],
      [...fullSchema(), { key: "unrelated.key", default_value: 999 }],
    );
    // Schema-driven; unrelated keys do not contaminate the result.
    expect(got.weights?.similarity).toBe(0.5);
  });

  it("operator override survives a missing schema entry for that key", () => {
    // Operator value present, schema value missing → still resolves; the
    // operator value is enough to satisfy the key.
    const partial = fullSchema().filter(
      (e) => e.key !== "ranking.weight.similarity",
    );
    const got = resolveSystemRankingWeights(
      [{ key: "ranking.weight.similarity", value: 0.42 }],
      partial,
    );
    expect(got.weights?.similarity).toBe(0.42);
    expect(got.missingKeys).toEqual([]);
  });
});
