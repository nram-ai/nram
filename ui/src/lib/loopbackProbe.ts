// probeReachable tests whether a loopback OAuth callback server is actually
// listening, so the consent page can decide between forwarding the browser to
// the callback and showing a manual paste-back screen.
//
// It deliberately does NOT read the response. A no-cors fetch resolves (with
// an opaque response) the moment a TCP listener accepts and replies with any
// HTTP status, and rejects with a TypeError on connection-refused / DNS
// failure / timeout. That makes it a pure connectivity test that works even
// though loopback callback servers (Claude Code, Codex, etc.) send no CORS
// headers: we never need to read the body, only observe whether the request
// completed. HTTPS pages are allowed to reach http://localhost because
// loopback is a "potentially trustworthy" origin (W3C Mixed Content / Secure
// Contexts), so this runs from the nram HTTPS console.
//
// The probe targets the origin root, never the code-bearing callback URL, so
// it can never consume the single-use authorization code.
export async function probeReachable(
  origin: string,
  timeoutMs = 1500,
): Promise<boolean> {
  try {
    await fetch(origin, {
      mode: "no-cors",
      cache: "no-store",
      signal: AbortSignal.timeout(timeoutMs),
    });
    return true;
  } catch {
    return false;
  }
}
