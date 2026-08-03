import { describe, it, expect } from "vitest";
import { sameOriginPath, safeExternalUrl } from "./safeRedirect";

describe("sameOriginPath", () => {
  const origin = window.location.origin;

  it("rejects an absolute off-origin URL", () => {
    expect(sameOriginPath("https://evil.com")).toBeNull();
    expect(sameOriginPath("https://evil.com/path?x=1")).toBeNull();
  });

  it("rejects a protocol-relative host", () => {
    expect(sameOriginPath("//evil.com")).toBeNull();
    expect(sameOriginPath("//evil.com/steal")).toBeNull();
  });

  it("rejects a backslash-normalized protocol-relative host", () => {
    // Browsers normalize "\" to "/" for http(s), so "/\evil.com" resolves to
    // "//evil.com" and must be rejected as off-origin.
    expect(sameOriginPath("/\\evil.com")).toBeNull();
  });

  it("rejects scheme targets", () => {
    expect(sameOriginPath("javascript:alert(1)")).toBeNull();
    expect(sameOriginPath("data:text/html,<script>alert(1)</script>")).toBeNull();
  });

  it("rejects empty / nullish input", () => {
    expect(sameOriginPath("")).toBeNull();
    expect(sameOriginPath(null)).toBeNull();
    expect(sameOriginPath(undefined)).toBeNull();
  });

  it("accepts a same-origin relative path and preserves search + hash", () => {
    expect(sameOriginPath("/dashboard?x=1#h")).toBe("/dashboard?x=1#h");
  });

  it("accepts the root path", () => {
    expect(sameOriginPath("/")).toBe("/");
  });

  it("accepts an absolute same-origin URL, returning only the path portion", () => {
    expect(sameOriginPath(`${origin}/settings`)).toBe("/settings");
    expect(sameOriginPath(`${origin}/settings?group=auth#top`)).toBe(
      "/settings?group=auth#top",
    );
  });
});

describe("safeExternalUrl", () => {
  it("rejects non-http(s) schemes (SEC-28)", () => {
    expect(safeExternalUrl("javascript:alert(1)")).toBeNull();
    expect(safeExternalUrl("data:text/html,<script>alert(1)</script>")).toBeNull();
    expect(safeExternalUrl("blob:https://x/y")).toBeNull();
    expect(safeExternalUrl("file:///etc/passwd")).toBeNull();
  });

  it("rejects empty / nullish / non-absolute input", () => {
    expect(safeExternalUrl("")).toBeNull();
    expect(safeExternalUrl(null)).toBeNull();
    expect(safeExternalUrl(undefined)).toBeNull();
    expect(safeExternalUrl("/relative/path")).toBeNull();
  });

  it("accepts absolute http/https URLs, including cross-origin loopback callbacks", () => {
    expect(safeExternalUrl("https://example.com/cb?code=1")).toBe(
      "https://example.com/cb?code=1",
    );
    expect(safeExternalUrl("http://127.0.0.1:52001/callback?code=abc")).toBe(
      "http://127.0.0.1:52001/callback?code=abc",
    );
  });
});
