/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import { parseFrame } from "./useEventStream";

describe("parseFrame", () => {
  it("returns null for keepalive frames", () => {
    expect(parseFrame(": keepalive")).toBeNull();
  });

  it("returns null for empty frames", () => {
    expect(parseFrame("")).toBeNull();
  });

  it("parses a complete frame with id, event and data", () => {
    // Server-side writeSSE wire format from internal/api/handler_events.go.
    const body = JSON.stringify({
      id: "evt-1",
      type: "dream.cycle.heartbeat",
      scope: "project:abc",
      data: { cycle_id: "c-1", tokens_used: 42 },
      timestamp: "2026-04-30T15:00:00Z",
    });
    const frame = `id: evt-1\nevent: dream.cycle.heartbeat\ndata: ${body}`;
    const out = parseFrame(frame);
    expect(out).not.toBeNull();
    expect(out!.id).toBe("evt-1");
    expect(out!.type).toBe("dream.cycle.heartbeat");
    expect(out!.scope).toBe("project:abc");
    expect((out!.data as any).cycle_id).toBe("c-1");
    expect((out!.data as any).tokens_used).toBe(42);
  });

  it("returns null when the data line is invalid JSON", () => {
    const frame = "id: evt-1\nevent: dream.call.started\ndata: not-json";
    expect(parseFrame(frame)).toBeNull();
  });

  it("falls back to the SSE id line when the JSON body has no id", () => {
    const body = JSON.stringify({ type: "x", scope: "", data: {}, timestamp: "" });
    const frame = `id: evt-99\nevent: x\ndata: ${body}`;
    const out = parseFrame(frame)!;
    expect(out.id).toBe("evt-99");
  });

  it("ignores frames that have only an id line and no event or data", () => {
    expect(parseFrame("id: stale-only")).toBeNull();
  });
});
