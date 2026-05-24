/**
 * @vitest-environment happy-dom
 *
 * Hook-level test for useSystemRankingWeights covering the
 * loading-vs-missing distinction. Pure-resolver semantics live in
 * useSystemRankingWeights.test.ts; this file exercises the React Query
 * wrapper's isLoading / isError plumbing — the bug that motivated the
 * change is the consumer rendering "schema unavailable" during the normal
 * initial-load window. The wrapper must return isLoading: true with an
 * empty missingKeys list until both underlying queries have resolved.
 */
import React from "react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useSystemRankingWeights } from "./useApi";
import { adminAPI } from "../api/client";

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

function fullSchema() {
  // The resolver only reads key + default_value; the broader SettingSchema
  // fields (type, description, category) are present in real responses but
  // irrelevant here. Stamp them with placeholders so the literal satisfies
  // the SettingSchema[] type the adminAPI return signature requires.
  const fields = [
    { key: "ranking.weight.similarity", default_value: 0.5 },
    { key: "ranking.weight.recency", default_value: 0.15 },
    { key: "ranking.weight.importance", default_value: 0.1 },
    { key: "ranking.weight.frequency", default_value: 0.0 },
    { key: "ranking.weight.graph_relevance", default_value: 0.2 },
    { key: "ranking.weight.confidence", default_value: 0.05 },
    { key: "ranking.weight.origin", default_value: 0.0 },
    { key: "ranking.weight.mmr_lambda", default_value: 0.75 },
  ];
  return fields.map((f) => ({
    ...f,
    type: "number",
    description: "test",
    category: "ranking",
  }));
}

describe("useSystemRankingWeights — loading semantics", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("reports isLoading=true with empty missingKeys before either query resolves", async () => {
    // Both queries pend forever; neither isLoading flag flips.
    vi.spyOn(adminAPI, "getSettings").mockReturnValue(new Promise(() => {}));
    vi.spyOn(adminAPI, "getSettingsSchema").mockReturnValue(
      new Promise(() => {}),
    );

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    // First synchronous render — both queries undefined.
    expect(result.current.isLoading).toBe(true);
    expect(result.current.weights).toBeNull();
    // Crucially: missingKeys is empty while loading, so the consumer can
    // tell "still pending" from "schema actually missing keys."
    expect(result.current.missingKeys).toEqual([]);
    expect(result.current.isError).toBe(false);
  });

  it("flips isLoading to false once both queries resolve with full schema", async () => {
    vi.spyOn(adminAPI, "getSettings").mockResolvedValue({ data: [] });
    vi.spyOn(adminAPI, "getSettingsSchema").mockResolvedValue({
      data: fullSchema(),
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

  it("reports isError=true (and isLoading=false) when the schema query fails", async () => {
    vi.spyOn(adminAPI, "getSettings").mockResolvedValue({ data: [] });
    vi.spyOn(adminAPI, "getSettingsSchema").mockRejectedValue(
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
    // The schema endpoint never landed payload, so missingKeys is empty
    // (no verdict to render) and the consumer should render the error
    // banner based on isError, not on missingKeys.
    expect(result.current.missingKeys).toEqual([]);
  });

  it("renders the editor with schema defaults when the settings query fails but the schema query succeeds", async () => {
    // Operator-overrides endpoint fails; defaults endpoint is healthy.
    // The hook must NOT report isError or isLoading — the editor can
    // render fine using schema defaults as placeholders.
    vi.spyOn(adminAPI, "getSettings").mockRejectedValue(new Error("network 500"));
    vi.spyOn(adminAPI, "getSettingsSchema").mockResolvedValue({
      data: fullSchema(),
    });

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    await waitFor(() => {
      expect(result.current.weights).not.toBeNull();
    });
    expect(result.current.isLoading).toBe(false);
    expect(result.current.isError).toBe(false);
    expect(result.current.missingKeys).toEqual([]);
    expect(result.current.weights?.similarity).toBe(0.5);
    expect(result.current.weights?.mmr_lambda).toBe(0.75);
  });

  it("flips isLoading to false as soon as the schema query resolves, even if the settings query is still pending", async () => {
    // Settings endpoint hangs indefinitely; schema endpoint resolves.
    // isLoading must NOT stay true — the schema is what gates rendering.
    vi.spyOn(adminAPI, "getSettings").mockReturnValue(new Promise(() => {}));
    vi.spyOn(adminAPI, "getSettingsSchema").mockResolvedValue({
      data: fullSchema(),
    });

    const { result } = renderHook(() => useSystemRankingWeights(), {
      wrapper: wrapperFactory(),
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });
    expect(result.current.isError).toBe(false);
    expect(result.current.weights).not.toBeNull();
  });
});
