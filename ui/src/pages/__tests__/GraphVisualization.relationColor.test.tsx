/**
 * @vitest-environment happy-dom
 *
 * Unit tests for getRelationColor, the decorative edge-color hash.
 *
 * Edge color used to key a fixed snake_case relation->color table, but the
 * backend canonicalizes relations to space-separated lowercase, so all but the
 * single-word "uses" key went dead and most edges rendered gray. The table was
 * replaced with a stable per-relation hue hash: same relation -> same color,
 * distinct relations spread across the wheel, the relation revealed on hover.
 * These tests pin that contract.
 */
import { describe, it, expect, vi } from "vitest";

// GraphVisualization imports react-force-graph-3d, which pulls in WebGL
// bindings on import. The component is never rendered here (these tests call
// the pure helper only), so a no-op stub is enough to keep the import cheap.
vi.mock("react-force-graph-3d", () => ({ __esModule: true, default: () => null }));

import { getRelationColor } from "../GraphVisualization";

const HSL = /^hsl\(\d{1,3}, 62%, 65%\)$/;

describe("getRelationColor", () => {
  it("is deterministic for a given relation", () => {
    expect(getRelationColor("uses")).toBe(getRelationColor("uses"));
    expect(getRelationColor("related to")).toBe(getRelationColor("related to"));
  });

  it("is case-insensitive, mirroring backend canonicalization", () => {
    expect(getRelationColor("Uses")).toBe(getRelationColor("uses"));
    expect(getRelationColor("RELATED TO")).toBe(getRelationColor("related to"));
  });

  it("spreads distinct relations onto distinct colors", () => {
    const colors = ["uses", "related to", "part of", "works for"].map(getRelationColor);
    expect(new Set(colors).size).toBe(colors.length);
  });

  it("returns a pinned-saturation/lightness hsl string for real relations", () => {
    for (const r of ["uses", "related to", "part of", "belongs to", "located in", "created by"]) {
      expect(getRelationColor(r)).toMatch(HSL);
    }
  });

  it("keeps a neutral gray for an empty relation", () => {
    expect(getRelationColor("")).toBe("#4b5563");
  });
});
