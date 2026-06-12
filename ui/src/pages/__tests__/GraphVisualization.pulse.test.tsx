/**
 * @vitest-environment happy-dom
 *
 * Unit tests for getPulse, the weight->directional-particle tier mapping.
 *
 * Edge "pulse" (directional particles) used to be a binary gate at the 2.0
 * weight cap. getPulse turns it into a strength gauge: four bands where both
 * particle count and speed step up, and a sub-0.5 floor that does not pulse.
 * These tests pin the band boundaries (inclusive lower edge), the exact tier
 * values, and monotonicity.
 */
import { describe, it, expect, vi } from "vitest";

// GraphVisualization imports react-force-graph-3d, which pulls in WebGL
// bindings on import. The component is never rendered here (these tests call
// the pure helper only), so a no-op stub is enough to keep the import cheap.
vi.mock("react-force-graph-3d", () => ({ __esModule: true, default: () => null }));

import { getPulse } from "../GraphVisualization";

describe("getPulse", () => {
  it("does not pulse below the 0.5 floor", () => {
    expect(getPulse(0)).toEqual({ count: 0, speed: 0 });
    expect(getPulse(0.49)).toEqual({ count: 0, speed: 0 });
  });

  it("steps both count and speed up across the four bands", () => {
    expect(getPulse(0.5)).toEqual({ count: 1, speed: 0.003 });
    expect(getPulse(0.99)).toEqual({ count: 1, speed: 0.003 });
    expect(getPulse(1.0)).toEqual({ count: 2, speed: 0.006 });
    expect(getPulse(1.49)).toEqual({ count: 2, speed: 0.006 });
    expect(getPulse(1.5)).toEqual({ count: 3, speed: 0.01 });
    expect(getPulse(2.0)).toEqual({ count: 3, speed: 0.01 }); // storage cap
  });

  it("treats each lower band bound as inclusive", () => {
    expect(getPulse(0.5).count).toBe(1);
    expect(getPulse(1.0).count).toBe(2);
    expect(getPulse(1.5).count).toBe(3);
  });

  it("never decreases count or speed as weight rises", () => {
    let prevCount = -1;
    let prevSpeed = -1;
    for (let w = 0; w <= 2.0001; w += 0.05) {
      const { count, speed } = getPulse(w);
      expect(count).toBeGreaterThanOrEqual(prevCount);
      expect(speed).toBeGreaterThanOrEqual(prevSpeed);
      prevCount = count;
      prevSpeed = speed;
    }
  });
});
