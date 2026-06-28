// Humanize a raw backend/Go error string into one short user-facing line.
//
// Provider Test failures, slot-save failures, and Ollama load/pull errors all
// surface wrapped Go error chains, e.g.
//   test failed: rerank probe: request failed: Post "http://host/v1/rerank":
//   dial tcp 127.0.0.1:9999: connect: connection refused
// which is useless to a first-run operator. This maps the common transport and
// HTTP causes to a clean message; unknown errors fall back to the innermost
// cause segment so the wrapper noise is dropped. The original string should be
// kept available (e.g. a title tooltip) for anyone who wants the full chain.

const PATTERNS: { test: RegExp; message: string }[] = [
  { test: /connection refused/i, message: "Connection refused: nothing is listening at that address." },
  { test: /no such host|name resolution|server misbehaving/i, message: "Host not found: check the URL." },
  { test: /no route to host|network is unreachable/i, message: "Network unreachable: check the host and your network." },
  { test: /i\/o timeout|deadline exceeded|timed out|timeout/i, message: "Timed out reaching the server: it may be slow or unreachable." },
  { test: /connection reset/i, message: "The server reset the connection." },
  { test: /x509|certificate|tls handshake|tls:/i, message: "TLS/certificate error reaching the server." },
  { test: /\b401\b|unauthorized/i, message: "Unauthorized (401): check the API key." },
  { test: /\b403\b|forbidden/i, message: "Forbidden (403): check credentials and permissions." },
  { test: /\b404\b|not found/i, message: "Not found (404): check the URL and provider type." },
  { test: /\b429\b|rate limit/i, message: "Rate limited (429): too many requests." },
  { test: /\b50[0-4]\b|internal server error|bad gateway|service unavailable/i, message: "The server returned an error (5xx)." },
];

export function humanizeProviderError(raw: string | null | undefined): string {
  const msg = (raw ?? "").trim();
  if (!msg) return "Request failed.";
  for (const p of PATTERNS) {
    if (p.test.test(msg)) return p.message;
  }
  // Fallback: drop the wrapped-prefix noise and surface the innermost cause
  // (the segment after the last ": "). Guard against a bare URL/overlong tail.
  const segments = msg.split(":").map((s) => s.trim()).filter(Boolean);
  const tail = segments.length ? segments[segments.length - 1] : msg;
  const clean = tail.length > 0 && tail.length <= 120 ? tail : msg;
  return clean.charAt(0).toUpperCase() + clean.slice(1);
}
