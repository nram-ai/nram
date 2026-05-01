/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import { isEnrichmentAvailable } from "./useEnrichmentAvailable";
import type { MeCapabilities } from "../api/client";

function caps(enrichment: boolean, dreaming = false): MeCapabilities {
  return {
    enrichment_available: enrichment,
    dreaming_enabled: dreaming,
  };
}

describe("isEnrichmentAvailable", () => {
  it("returns true when capabilities report enrichment_available=true", () => {
    expect(isEnrichmentAvailable(caps(true), false, false)).toBe(true);
  });

  it("returns false when capabilities report enrichment_available=false", () => {
    expect(isEnrichmentAvailable(caps(false), false, false)).toBe(false);
  });

  it("returns false while the query is loading (avoids UI flash)", () => {
    expect(isEnrichmentAvailable(caps(true), true, false)).toBe(false);
  });

  it("returns false on query error (defensive default)", () => {
    expect(isEnrichmentAvailable(caps(true), false, true)).toBe(false);
  });

  it("returns false when capabilities are undefined", () => {
    expect(isEnrichmentAvailable(undefined, false, false)).toBe(false);
  });

  it("returns false when capabilities are null", () => {
    expect(isEnrichmentAvailable(null, false, false)).toBe(false);
  });

  it("ignores dreaming_enabled when computing enrichment availability", () => {
    expect(isEnrichmentAvailable(caps(true, false), false, false)).toBe(true);
    expect(isEnrichmentAvailable(caps(false, true), false, false)).toBe(false);
  });
});
