import { describe, it, expect } from "vitest";
import { humanizeProviderError } from "./providerError";

describe("humanizeProviderError", () => {
  it("maps a wrapped connection-refused chain to a clean line", () => {
    const raw =
      'test failed: rerank probe: request failed: Post "http://127.0.0.1:9999/v1/rerank": dial tcp 127.0.0.1:9999: connect: connection refused';
    expect(humanizeProviderError(raw)).toBe(
      "Connection refused: nothing is listening at that address.",
    );
  });

  it("maps timeouts", () => {
    expect(humanizeProviderError("Get \"http://x/v1\": context deadline exceeded")).toBe(
      "Timed out reaching the server: it may be slow or unreachable.",
    );
    expect(humanizeProviderError("dial tcp: i/o timeout")).toBe(
      "Timed out reaching the server: it may be slow or unreachable.",
    );
  });

  it("maps host resolution failures", () => {
    expect(humanizeProviderError('dial tcp: lookup nope: no such host')).toBe(
      "Host not found: check the URL.",
    );
  });

  it("maps HTTP status causes", () => {
    expect(humanizeProviderError("provider returned 401 Unauthorized")).toBe(
      "Unauthorized (401): check the API key.",
    );
    expect(humanizeProviderError("status 404 not found")).toBe(
      "Not found (404): check the URL and provider type.",
    );
  });

  it("falls back to the innermost cause for unknown chains", () => {
    expect(humanizeProviderError("a: b: something specific broke")).toBe(
      "Something specific broke",
    );
  });

  it("handles empty input", () => {
    expect(humanizeProviderError("")).toBe("Request failed.");
    expect(humanizeProviderError(null)).toBe("Request failed.");
  });
});
