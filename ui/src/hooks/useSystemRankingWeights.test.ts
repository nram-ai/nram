/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import { resolveSystemRankingWeights } from "./useApi";

describe("resolveSystemRankingWeights", () => {
  it("returns the built-in fallback when no settings or schema are loaded", () => {
    expect(resolveSystemRankingWeights([], [])).toEqual({
      similarity: 0.5,
      recency: 0.15,
      importance: 0.1,
      frequency: 0.0,
      graph_relevance: 0.2,
      confidence: 0.05,
    });
  });

  it("applies schema defaults when no operator override exists", () => {
    const got = resolveSystemRankingWeights(
      [],
      [
        { key: "ranking.weight.similarity", default_value: 0.45 },
        { key: "ranking.weight.confidence", default_value: 0.10 },
      ],
    );
    expect(got.similarity).toBe(0.45);
    expect(got.confidence).toBe(0.10);
    // Unspecified keys still come from the built-in fallback.
    expect(got.recency).toBe(0.15);
  });

  it("operator override beats schema default", () => {
    const got = resolveSystemRankingWeights(
      [{ key: "ranking.weight.similarity", value: 0.30 }],
      [{ key: "ranking.weight.similarity", default_value: 0.50 }],
    );
    expect(got.similarity).toBe(0.30);
  });

  it("ignores non-numeric setting values", () => {
    const got = resolveSystemRankingWeights(
      [{ key: "ranking.weight.similarity", value: "not a number" }],
      [{ key: "ranking.weight.similarity", default_value: 0.42 }],
    );
    // Bad operator value falls through to the schema default.
    expect(got.similarity).toBe(0.42);
  });

  it("coerces string-encoded numeric values", () => {
    // Settings come back from the server as JSON-decoded values; some paths
    // round-trip floats as strings.
    const got = resolveSystemRankingWeights(
      [{ key: "ranking.weight.confidence", value: "0.25" }],
      [],
    );
    expect(got.confidence).toBe(0.25);
  });

  it("ignores keys outside the ranking.weight namespace", () => {
    const got = resolveSystemRankingWeights(
      [
        { key: "unrelated.key", value: 999 },
        { key: "ranking.weight.recency", value: 0.20 },
      ],
      [],
    );
    expect(got.recency).toBe(0.20);
    // Built-in fallback for unspecified.
    expect(got.similarity).toBe(0.5);
  });
});
