import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { copyToClipboard } from "./clipboard";

// Save the real descriptors so each test can install its own context and the
// suite restores the environment afterward.
const realClipboardDescriptor = Object.getOwnPropertyDescriptor(
  navigator,
  "clipboard",
);
const realSecureDescriptor = Object.getOwnPropertyDescriptor(
  window,
  "isSecureContext",
);
const realExecCommand = document.execCommand;

function setSecureContext(value: boolean): void {
  Object.defineProperty(window, "isSecureContext", {
    value,
    configurable: true,
  });
}

function setClipboard(value: unknown): void {
  Object.defineProperty(navigator, "clipboard", {
    value,
    configurable: true,
  });
}

describe("copyToClipboard", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    if (realClipboardDescriptor) {
      Object.defineProperty(navigator, "clipboard", realClipboardDescriptor);
    } else {
      setClipboard(undefined);
    }
    if (realSecureDescriptor) {
      Object.defineProperty(window, "isSecureContext", realSecureDescriptor);
    }
    document.execCommand = realExecCommand;
  });

  it("uses the async Clipboard API in a secure context and returns true", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    setSecureContext(true);
    setClipboard({ writeText });

    const ok = await copyToClipboard("hello");

    expect(ok).toBe(true);
    expect(writeText).toHaveBeenCalledWith("hello");
  });

  it("falls back to execCommand when navigator.clipboard is undefined (insecure context)", async () => {
    setSecureContext(false);
    setClipboard(undefined);
    const exec = vi.fn().mockReturnValue(true);
    document.execCommand = exec as typeof document.execCommand;

    const ok = await copyToClipboard("on-http");

    expect(ok).toBe(true);
    expect(exec).toHaveBeenCalledWith("copy");
  });

  it("falls back to execCommand when the Clipboard API rejects in a secure context", async () => {
    const writeText = vi.fn().mockRejectedValue(new Error("denied"));
    setSecureContext(true);
    setClipboard({ writeText });
    const exec = vi.fn().mockReturnValue(true);
    document.execCommand = exec as typeof document.execCommand;

    const ok = await copyToClipboard("retry");

    expect(ok).toBe(true);
    expect(writeText).toHaveBeenCalledWith("retry");
    expect(exec).toHaveBeenCalledWith("copy");
  });

  it("returns false when both the Clipboard API and execCommand are unavailable", async () => {
    setSecureContext(false);
    setClipboard(undefined);
    const exec = vi.fn().mockReturnValue(false);
    document.execCommand = exec as typeof document.execCommand;

    const ok = await copyToClipboard("nope");

    expect(ok).toBe(false);
  });

  it("returns false when execCommand throws", async () => {
    setSecureContext(false);
    setClipboard(undefined);
    const exec = vi.fn().mockImplementation(() => {
      throw new Error("blocked");
    });
    document.execCommand = exec as typeof document.execCommand;

    const ok = await copyToClipboard("boom");

    expect(ok).toBe(false);
  });
});
