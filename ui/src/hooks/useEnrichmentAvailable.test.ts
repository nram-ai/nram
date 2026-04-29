/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import { isEnrichmentAvailable, REQUIRED_ENRICHMENT_SLOTS } from "./useEnrichmentAvailable";
import type { ProviderSlot } from "../api/client";

function slot(name: string, configured: boolean): ProviderSlot {
  return {
    slot: name,
    configured,
    type: configured ? "openai" : "",
    url: "",
    model: configured ? "test-model" : "",
  };
}

describe("isEnrichmentAvailable", () => {
  it("REQUIRED_ENRICHMENT_SLOTS is exactly embedding+fact+entity", () => {
    expect([...REQUIRED_ENRICHMENT_SLOTS].sort()).toEqual(
      ["embedding", "entity", "fact"],
    );
  });

  it("returns true when all three required slots are configured", () => {
    const slots = [
      slot("embedding", true),
      slot("fact", true),
      slot("entity", true),
    ];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(true);
  });

  it("returns false when embedding is unconfigured", () => {
    const slots = [
      slot("embedding", false),
      slot("fact", true),
      slot("entity", true),
    ];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(false);
  });

  it("returns false when fact is unconfigured", () => {
    const slots = [
      slot("embedding", true),
      slot("fact", false),
      slot("entity", true),
    ];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(false);
  });

  it("returns false when entity is unconfigured", () => {
    const slots = [
      slot("embedding", true),
      slot("fact", true),
      slot("entity", false),
    ];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(false);
  });

  it("returns false when only one slot is configured", () => {
    const slots = [
      slot("embedding", true),
      slot("fact", false),
      slot("entity", false),
    ];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(false);
  });

  it("returns false when no slots are configured", () => {
    const slots = [
      slot("embedding", false),
      slot("fact", false),
      slot("entity", false),
    ];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(false);
  });

  it("returns false when a required slot is missing from the response", () => {
    const slots = [slot("embedding", true), slot("fact", true)];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(false);
  });

  it("ignores extra slots that are not in the required set", () => {
    const slots = [
      slot("embedding", true),
      slot("fact", true),
      slot("entity", true),
      slot("future_slot", false),
    ];
    expect(isEnrichmentAvailable(slots, false, false)).toBe(true);
  });

  it("returns false while the query is loading (avoids UI flash)", () => {
    const slots = [
      slot("embedding", true),
      slot("fact", true),
      slot("entity", true),
    ];
    expect(isEnrichmentAvailable(slots, true, false)).toBe(false);
  });

  it("returns false on query error (defensive default)", () => {
    const slots = [
      slot("embedding", true),
      slot("fact", true),
      slot("entity", true),
    ];
    expect(isEnrichmentAvailable(slots, false, true)).toBe(false);
  });

  it("returns false when slots is undefined", () => {
    expect(isEnrichmentAvailable(undefined, false, false)).toBe(false);
  });

  it("returns false when slots is null", () => {
    expect(isEnrichmentAvailable(null, false, false)).toBe(false);
  });

  it("returns false when slots is empty", () => {
    expect(isEnrichmentAvailable([], false, false)).toBe(false);
  });
});
