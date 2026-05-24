/**
 * @vitest-environment happy-dom
 *
 * Hook-level test for useSystemRankingWeights covering the
 * loading-vs-missing distinction. Pure-resolver semantics live in
 * useSystemRankingWeights.test.ts; this file exercises the React Query
 * wrapper's isLoading / isError plumbing — the bug that motivated the
 * change is the consumer rendering "schema unavailable" during the normal
 * initial-load window. The wrapper must return isLoading: true with an
 * empty missingKeys list until the underlying query has resolved.
 */
import React from "react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useSystemRankingWeights } from "./useApi";
import { meAPI } from "../api/client";

function wrapperFactory() {
  const client = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        // Prevent React Query from holding stale data between tests.
        gcTime: 0,
      },
    },
  });
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

function fullPayload() {
  // Mirror the shape returned by GET /v1/me/ranking-weights/defaults. value
  // is set equal to default_value (no operator override stored); a separate
  // test exercises the override-beats-default path.
  const entries = [
    { key: "ranking.weight.similarity", default_value: 0.5 },
    { key: "ranking.weight.recency", default_value: 0.15 },
    { key: "ranking.weight.importance", default_value: 0.1 },
    { key: "ranking.weight.frequency", default_value: 0.0 },
    { key: "ranking.weight.graph_relevance", default_value: 0.2 },
    { key: "ranking.weight.confidence", default_value: 0.05 },
    { key: "ranking.weight.origin", default_value: 0.0 },
    { key: "ranking.weight.mmr_lambda", default_value: 0.75 },
  ];
  return entries.map((e) => ({
    ...e,
    value: e.default_value,
    min: 0,
    max: 1,
    step: 0.05,
  }));
}

describe("useSystemRankingWeights — loading semantics", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("reports isLoading=true with empty missingKeys before the query resolves", async () => {
    // Query pends forever; isLoading must stay true and not flip to a
    // missing-keys verdict.
    vi.spyOn(meAPI, "getRankingWeightDefaults").mockReturnValue(
      new Promise(() => {}),
    );

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    // First synchronous render — query undefined.
    expect(result.current.isLoading).toBe(true);
    expect(result.current.weights).toBeNull();
    // Crucially: missingKeys is empty while loading, so the consumer can
    // tell "still pending" from "schema actually missing keys."
    expect(result.current.missingKeys).toEqual([]);
    expect(result.current.isError).toBe(false);
  });

  it("flips isLoading to false once the query resolves with full payload", async () => {
    vi.spyOn(meAPI, "getRankingWeightDefaults").mockResolvedValue({
      data: fullPayload(),
    });

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });
    expect(result.current.weights).not.toBeNull();
    expect(result.current.missingKeys).toEqual([]);
    expect(result.current.isError).toBe(false);
  });

  it("reports isError=true (and isLoading=false) when the endpoint fails", async () => {
    vi.spyOn(meAPI, "getRankingWeightDefaults").mockRejectedValue(
      new Error("network 500"),
    );

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    await waitFor(() => {
      expect(result.current.isError).toBe(true);
    });
    expect(result.current.isLoading).toBe(false);
    expect(result.current.weights).toBeNull();
    // The endpoint never landed payload, so missingKeys is empty (no verdict
    // to render) and the consumer should render the error banner based on
    // isError, not on missingKeys.
    expect(result.current.missingKeys).toEqual([]);
  });

  it("surfaces operator overrides via value while defaults stay on the schema field", async () => {
    const payload = fullPayload();
    // Operator set similarity = 0.30 at scope=global; the server returned
    // value=0.30 with default_value=0.50.
    const sim = payload.find((e) => e.key === "ranking.weight.similarity");
    if (sim) {
      sim.value = 0.30;
    }
    vi.spyOn(meAPI, "getRankingWeightDefaults").mockResolvedValue({
      data: payload,
    });

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    await waitFor(() => {
      expect(result.current.weights).not.toBeNull();
    });
    expect(result.current.weights?.similarity).toBe(0.30);
    // Unmodified key still comes through as its default.
    expect(result.current.weights?.mmr_lambda).toBe(0.75);
  });

  it("returns null weights with a missingKeys verdict when the endpoint omits a key", async () => {
    const partial = fullPayload().filter(
      (e) => e.key !== "ranking.weight.mmr_lambda",
    );
    vi.spyOn(meAPI, "getRankingWeightDefaults").mockResolvedValue({
      data: partial,
    });

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });
    expect(result.current.weights).toBeNull();
    expect(result.current.missingKeys).toEqual(["ranking.weight.mmr_lambda"]);
    expect(result.current.isError).toBe(false);
  });
});
