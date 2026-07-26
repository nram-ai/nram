import { describe, it, expect } from "vitest";
import { sameOriginPath } from "./safeRedirect";

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
