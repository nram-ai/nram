/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import {
  buildProjectSettingsPayload,
  buildUserSettingsPayload,
} from "./settingsPayload";

const emptyProjectState = {
  similarity: undefined,
  recency: undefined,
  importance: undefined,
  frequency: undefined,
  graph_relevance: undefined,
  confidence: undefined,
  dedup_threshold: undefined,
  enrichment_enabled: undefined,
};

describe("buildProjectSettingsPayload", () => {
  it("returns an empty object when nothing is set", () => {
    // No fields set → no JSON sent → cascade resolves entirely from system.
    expect(buildProjectSettingsPayload(emptyProjectState)).toEqual({});
  });

  it("omits unset weight fields from ranking_weights", () => {
    const payload = buildProjectSettingsPayload({
      ...emptyProjectState,
      similarity: 0.3,
      confidence: 0.5,
    });
    expect(payload).toEqual({
      ranking_weights: { similarity: 0.3, confidence: 0.5 },
    });
    expect(payload.ranking_weights).not.toHaveProperty("recency");
    expect(payload.ranking_weights).not.toHaveProperty("importance");
    expect(payload.ranking_weights).not.toHaveProperty("frequency");
    expect(payload.ranking_weights).not.toHaveProperty("graph_relevance");
  });

  it("drops ranking_weights entirely when no weight is set", () => {
    // Setting only dedup or enrichment must not produce ranking_weights:{}.
    // The server's parser tolerates an empty object but bytes-on-the-wire
    // hygiene matters for diff readability.
    const payload = buildProjectSettingsPayload({
      ...emptyProjectState,
      dedup_threshold: 0.85,
    });
    expect(payload).toEqual({ dedup_threshold: 0.85 });
    expect(payload).not.toHaveProperty("ranking_weights");
  });

  it("preserves zero values (zero is a valid weight)", () => {
    // Operators can deliberately zero out a term — the override must travel.
    const payload = buildProjectSettingsPayload({
      ...emptyProjectState,
      frequency: 0,
    });
    expect(payload).toEqual({ ranking_weights: { frequency: 0 } });
  });

  it("preserves false enrichment_enabled (false is a valid override)", () => {
    const payload = buildProjectSettingsPayload({
      ...emptyProjectState,
      enrichment_enabled: false,
    });
    expect(payload).toEqual({ enrichment_enabled: false });
  });

  it("emits all six weight fields when each is set", () => {
    const payload = buildProjectSettingsPayload({
      similarity: 0.5,
      recency: 0.15,
      importance: 0.1,
      frequency: 0.0,
      graph_relevance: 0.2,
      confidence: 0.05,
      dedup_threshold: 0.92,
      enrichment_enabled: true,
    });
    expect(payload).toEqual({
      ranking_weights: {
        similarity: 0.5,
        recency: 0.15,
        importance: 0.1,
        frequency: 0.0,
        graph_relevance: 0.2,
        confidence: 0.05,
      },
      dedup_threshold: 0.92,
      enrichment_enabled: true,
    });
  });
});

describe("buildUserSettingsPayload", () => {
  it("returns empty object when nothing is set", () => {
    expect(
      buildUserSettingsPayload({
        dedup_threshold: undefined,
        enrichment_enabled: undefined,
      }),
    ).toEqual({});
  });

  it("never emits ranking_weights, even when caller state is malformed", () => {
    // The type system enforces the shape, but if a future refactor adds
    // ranking_weights state to UserManagement and forgets to remove this
    // helper from the user save path, the type check at the call site is
    // the safety net — verify the helper itself does not silently let a
    // ranking_weights key through. Cast to bypass the type and confirm
    // runtime omission.
    const payload = buildUserSettingsPayload({
      dedup_threshold: 0.85,
      enrichment_enabled: true,
      // @ts-expect-error — UserFormState forbids this field.
      ranking_weights: { similarity: 0.5 },
    });
    expect(payload).not.toHaveProperty("ranking_weights");
  });

  it("preserves false enrichment_enabled at user scope", () => {
    expect(
      buildUserSettingsPayload({
        dedup_threshold: undefined,
        enrichment_enabled: false,
      }),
    ).toEqual({ enrichment_enabled: false });
  });

  it("preserves zero dedup_threshold", () => {
    expect(
      buildUserSettingsPayload({
        dedup_threshold: 0,
        enrichment_enabled: undefined,
      }),
    ).toEqual({ dedup_threshold: 0 });
  });

  it("emits both fields when each is set", () => {
    expect(
      buildUserSettingsPayload({
        dedup_threshold: 0.92,
        enrichment_enabled: true,
      }),
    ).toEqual({ dedup_threshold: 0.92, enrichment_enabled: true });
  });
});
