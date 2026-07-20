import { describe, it, expect } from "vitest";

import { groupCost, cacheHitPct } from "../Analytics";
import type { CostRate } from "../../api/client";

// The cost card prices prompt-cache tokens separately from plain input.
// Cache reads and writes are a SUBSET of tokens_input, so the input rate
// applies only to the uncached remainder.
//
// The property that matters most operationally is the first one: adding these
// columns must not move any cost a user is already looking at. Only an
// explicitly configured cache rate may change a displayed number.

const baseRate: CostRate = {
  key: "fact_extraction",
  inputCostPer1k: 0.072,
  outputCostPer1k: 0.287,
};

function group(over: Partial<Parameters<typeof groupCost>[0]> = {}) {
  return {
    tokens_input: 1000,
    tokens_output: 100,
    tokens_cache_read: 0,
    tokens_cache_write: 0,
    ...over,
  };
}

describe("groupCost", () => {
  it("is unchanged from the pre-cache formula when no cache rate is set", () => {
    // Same tokens, one bucket cached and one not. With no cache rate
    // configured, both must price identically to tokens_input * inputRate.
    const legacy = (1000 / 1000) * baseRate.inputCostPer1k + (100 / 1000) * baseRate.outputCostPer1k;

    expect(groupCost(group(), baseRate)).toBeCloseTo(legacy, 12);
    expect(
      groupCost(group({ tokens_cache_read: 900, tokens_cache_write: 50 }), baseRate),
    ).toBeCloseTo(legacy, 12);
  });

  it("discounts cache reads once a rate is configured", () => {
    const rate: CostRate = { ...baseRate, cacheReadCostPer1k: 0.0072 };
    const g = group({ tokens_cache_read: 800 });

    // 200 uncached at 0.072/1k + 800 cached at 0.0072/1k + output.
    const want =
      (200 / 1000) * 0.072 + (800 / 1000) * 0.0072 + (100 / 1000) * baseRate.outputCostPer1k;
    expect(groupCost(g, rate)).toBeCloseTo(want, 12);
    // And it must be cheaper than pricing everything at the input rate.
    expect(groupCost(g, rate)).toBeLessThan(groupCost(g, baseRate));
  });

  it("prices cache writes at their own rate", () => {
    const rate: CostRate = { ...baseRate, cacheWriteCostPer1k: 0.09 };
    const g = group({ tokens_cache_write: 400 });

    const want =
      (600 / 1000) * 0.072 + (400 / 1000) * 0.09 + (100 / 1000) * baseRate.outputCostPer1k;
    expect(groupCost(g, rate)).toBeCloseTo(want, 12);
    // A write premium costs more than the flat input rate.
    expect(groupCost(g, rate)).toBeGreaterThan(groupCost(g, baseRate));
  });

  it("applies read and write rates independently", () => {
    const rate: CostRate = {
      ...baseRate,
      cacheReadCostPer1k: 0.0072,
      cacheWriteCostPer1k: 0.09,
    };
    const g = group({ tokens_cache_read: 500, tokens_cache_write: 200 });

    const want =
      (300 / 1000) * 0.072 +
      (500 / 1000) * 0.0072 +
      (200 / 1000) * 0.09 +
      (100 / 1000) * baseRate.outputCostPer1k;
    expect(groupCost(g, rate)).toBeCloseTo(want, 12);
  });

  it("clamps the uncached remainder at zero", () => {
    // A provider reporting cache counts above the prompt count must never
    // produce a negative uncached term, which would understate the cost.
    const rate: CostRate = { ...baseRate, cacheReadCostPer1k: 0.0072 };
    const g = group({ tokens_input: 100, tokens_cache_read: 900 });

    const cost = groupCost(g, rate);
    expect(cost).toBeGreaterThan(0);
    expect(cost).toBeCloseTo((900 / 1000) * 0.0072 + (100 / 1000) * baseRate.outputCostPer1k, 12);
  });

  it("treats a zero cache rate as free rather than inherited", () => {
    // 0 is a real configured value and must not fall back to inputCostPer1k;
    // only an absent field inherits.
    const rate: CostRate = { ...baseRate, cacheReadCostPer1k: 0 };
    const g = group({ tokens_cache_read: 1000 });
    expect(groupCost(g, rate)).toBeCloseTo((100 / 1000) * baseRate.outputCostPer1k, 12);
  });
});

describe("cacheHitPct", () => {
  it("returns the cached share of input tokens", () => {
    expect(cacheHitPct({ tokens_input: 1000, tokens_cache_read: 750 })).toBeCloseTo(75, 12);
  });

  it("returns null when nothing was measured", () => {
    // Distinguishes "no cache detail reported" from a genuine 0% hit rate, so
    // the table renders a dash instead of asserting a miss that never happened.
    expect(cacheHitPct({ tokens_input: 1000, tokens_cache_read: 0 })).toBeNull();
    expect(cacheHitPct({ tokens_input: 0, tokens_cache_read: 0 })).toBeNull();
  });
});
