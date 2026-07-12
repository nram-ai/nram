/**
 * @vitest-environment happy-dom
 *
 * Hook-level test for useProviderModels / useOllamaModels cache identity.
 * Both hooks let a provider-slot editor load a served-model list by URL while
 * optionally forwarding custom auth headers. The cache key must include those
 * headers: two logical fetches that share a URL but differ in credentials must
 * land in distinct React Query buckets, so the list resolved under one
 * credential is never surfaced to a consumer holding a different one.
 *
 * Step A asserts on the refetch() return rather than result.current.data:
 * TanStack Query v5's tracked-properties optimization does not re-render the
 * renderHook wrapper for fields read only after the render pass. The cache
 * bucket is populated regardless, which is exactly what step B probes: a second
 * hook mounted on the same client reads its initial-render snapshot from the
 * cache, so a header-blind key would surface the first credential's list.
 */
import React from "react";
import { describe, it, expect, vi, afterEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useProviderModels, useOllamaModels } from "./useApi";
import { adminAPI } from "../api/client";

afterEach(() => {
  vi.restoreAllMocks();
});

// One shared client across both hook renders so a header-blind key would let
// the second render read the first render's cached bucket.
function sharedWrapperFactory() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

describe("useProviderModels: header-aware cache identity", () => {
  it("keeps distinct-credential fetches in separate cache buckets at the same URL", async () => {
    // Echo the credential into the returned list so a bucket collision is
    // observable as the wrong header's value.
    vi.spyOn(adminAPI, "getProviderModels").mockImplementation(
      async (_url: string, headers?: Record<string, string>) => ({
        models: [headers?.["X-Api-Key"] ?? "none"],
      }),
    );

    const url = "http://gateway.local";
    const wrapper = sharedWrapperFactory();

    const a = renderHook(
      () => useProviderModels(url, { "X-Api-Key": "A" }),
      { wrapper },
    );
    // The A-credential bucket is now populated.
    const res = await act(async () => a.result.current.refetch());
    expect(res.data?.models).toEqual(["A"]);

    // Same URL, different credential, same client, no refetch. With the header
    // folded into the key this is a distinct enabled:false bucket with no data;
    // with a URL-only key it would read A's cached ["A"] list on first render.
    const b = renderHook(
      () => useProviderModels(url, { "X-Api-Key": "B" }),
      { wrapper },
    );
    expect(b.result.current.data).toBeUndefined();
  });
});

describe("useOllamaModels: header-aware cache identity", () => {
  it("keeps distinct-credential fetches in separate cache buckets at the same URL", async () => {
    vi.spyOn(adminAPI, "getOllamaModels").mockImplementation(
      async (_url?: string, headers?: Record<string, string>) => ({
        models: [{ name: headers?.["X-Api-Key"] ?? "none" }] as never,
      }),
    );

    const url = "http://ollama.local";
    const wrapper = sharedWrapperFactory();

    const a = renderHook(
      () => useOllamaModels(url, { "X-Api-Key": "A" }),
      { wrapper },
    );
    const res = await act(async () => a.result.current.refetch());
    expect(res.data?.models?.[0]?.name).toBe("A");

    const b = renderHook(
      () => useOllamaModels(url, { "X-Api-Key": "B" }),
      { wrapper },
    );
    expect(b.result.current.data).toBeUndefined();
  });
});
