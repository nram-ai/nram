// sameOriginPath resolves a candidate post-login redirect against the current
// origin and returns a same-origin path (pathname + search + hash), or null
// when the value points anywhere off-origin. It blocks absolute URLs
// ("https://evil.com"), protocol-relative hosts ("//evil.com"), backslash
// variants ("/\\evil.com", which some browsers normalize to "//"), and scheme
// targets ("javascript:...", "data:..."), all of which resolve to an origin
// other than the app's own. Callers pass the returned path straight to a
// navigation sink and fall back to a safe default when this returns null.
export function sameOriginPath(raw: string | null | undefined): string | null {
  if (!raw) return null;
  try {
    const url = new URL(raw, window.location.origin);
    if (url.origin !== window.location.origin) return null;
    return url.pathname + url.search + url.hash;
  } catch {
    return null;
  }
}
