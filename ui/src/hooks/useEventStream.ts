import { useEffect, useRef, useState } from "react";

/**
 * Generic Server-Sent Events subscription with auth, scope filtering, and
 * exponential-backoff reconnect.
 *
 * Implementation note: native EventSource cannot set Authorization headers,
 * but the nram /v1/events endpoint requires Bearer auth. We use fetch() with
 * a streaming ReadableStream reader and parse the SSE wire format ourselves.
 *
 * The hook is fire-and-forget: events are dispatched via onEvent. The hook
 * returns the live socket state so the calling page can drop polling
 * intervals (or fall back to faster polling) when the stream is connected.
 */

export interface EventStreamMessage {
  id: string;
  type: string;
  scope: string;
  data: any;
  timestamp: string;
}

export interface UseEventStreamOptions {
  /** Empty string subscribes to every event the user is authorized to see. */
  scope: string;
  /** When false the hook tears the stream down and stays idle. */
  enabled?: boolean;
  /** Called once per parsed event. Should be cheap; heavy work belongs in setState. */
  onEvent: (evt: EventStreamMessage) => void;
}

export interface UseEventStreamResult {
  connected: boolean;
}

const MIN_BACKOFF_MS = 250;
const MAX_BACKOFF_MS = 5000;

/**
 * parseFrame parses a single SSE frame (everything between two `\n\n`
 * separators on the wire) into an EventStreamMessage. Returns null for
 * keepalive frames (lines starting with ':') or frames that fail to
 * decode. Exposed as a top-level export so the parser can be unit tested
 * without spinning up a fake fetch + ReadableStream in jsdom.
 *
 * The nram server encodes the full envelope as JSON inside the data line
 * (see writeSSE in handler_events.go). We trust the parsed body for the
 * id/type/scope/data/timestamp fields; the SSE `id:` header line is used
 * only as a fallback when the body is absent.
 */
export function parseFrame(frame: string): EventStreamMessage | null {
  if (frame.startsWith(":")) return null;
  let id = "";
  let type = "";
  const dataLines: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith("id: ")) id = line.slice(4);
    else if (line.startsWith("event: ")) type = line.slice(7);
    else if (line.startsWith("data: ")) dataLines.push(line.slice(6));
  }
  if (!type && dataLines.length === 0) return null;
  let payload: EventStreamMessage;
  try {
    const raw = dataLines.join("\n");
    payload = raw ? (JSON.parse(raw) as EventStreamMessage) : ({ id, type } as EventStreamMessage);
  } catch {
    return null;
  }
  payload.id = payload.id || id;
  return payload;
}

export function useEventStream(opts: UseEventStreamOptions): UseEventStreamResult {
  const { scope, enabled = true, onEvent } = opts;
  const [connected, setConnected] = useState(false);

  // Hold the latest onEvent in a ref so the streaming loop sees fresh
  // closures without forcing a reconnect when the parent re-renders.
  const onEventRef = useRef(onEvent);
  useEffect(() => {
    onEventRef.current = onEvent;
  }, [onEvent]);

  useEffect(() => {
    if (!enabled) {
      setConnected(false);
      return;
    }

    let cancelled = false;
    let abort = new AbortController();
    let lastEventID = "";
    let backoff = MIN_BACKOFF_MS;
    let reconnectTimer: number | undefined;

    const connect = async () => {
      if (cancelled) return;

      const token = localStorage.getItem("nram_token") ?? "";
      const params = new URLSearchParams();
      if (scope) params.set("scope", scope);
      if (lastEventID) params.set("last_event_id", lastEventID);
      const url = `/v1/events${params.toString() ? `?${params}` : ""}`;

      abort = new AbortController();

      try {
        const res = await fetch(url, {
          method: "GET",
          headers: {
            Accept: "text/event-stream",
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          signal: abort.signal,
        });

        if (!res.ok || !res.body) {
          throw new Error(`SSE upstream responded ${res.status}`);
        }

        setConnected(true);
        backoff = MIN_BACKOFF_MS;

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        // SSE frames are separated by blank lines (\n\n). We accumulate
        // partial frames in `buffer` and dispatch each completed frame as
        // we see the terminator.
        // Each frame may include `id:`, `event:`, and one or more `data:`
        // lines. We keep this minimal; nram only emits one data line per
        // frame, but multiple data lines must be joined per the SSE spec.
        // eslint-disable-next-line no-constant-condition
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let sep: number;
          while ((sep = buffer.indexOf("\n\n")) !== -1) {
            const frame = buffer.slice(0, sep);
            buffer = buffer.slice(sep + 2);
            dispatchFrame(frame);
          }
        }
      } catch (err) {
        if (cancelled) return;
        if ((err as { name?: string }).name === "AbortError") return;
        // fall through to reconnect
      } finally {
        if (!cancelled) {
          setConnected(false);
        }
      }

      if (cancelled) return;
      reconnectTimer = window.setTimeout(connect, backoff);
      backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    };

    const dispatchFrame = (frame: string) => {
      const payload = parseFrame(frame);
      if (!payload) return;
      if (payload.id) lastEventID = payload.id;
      onEventRef.current(payload);
    };

    connect();

    return () => {
      cancelled = true;
      abort.abort();
      if (reconnectTimer !== undefined) {
        window.clearTimeout(reconnectTimer);
      }
      setConnected(false);
    };
  }, [scope, enabled]);

  return { connected };
}
