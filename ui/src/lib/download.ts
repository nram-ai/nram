// triggerBlobDownload streams a Blob to the browser as a file download by
// constructing an object URL, dispatching a synthetic click on an anchor,
// and then revoking the URL after the click hook has fired. Safari races
// the revoke against the click when revocation is synchronous, so the
// revoke is deferred to the next task tick; the synchronous form leaks
// the URL until the page unloads, which is the more common failure mode
// in production.
export function triggerBlobDownload(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

// downloadJson serializes a value as pretty-printed JSON and triggers a
// browser download. Used by the memory browser's "Export selected" and
// "Export all" actions. Pure client-side, no network round-trip.
export function downloadJson(data: unknown, filename: string): void {
  const blob = new Blob([JSON.stringify(data, null, 2)], {
    type: "application/json",
  });
  triggerBlobDownload(blob, filename);
}

// parseAttachmentFilename extracts the filename from an RFC 6266
// Content-Disposition header value. Tolerates the common `filename="…"`
// form; returns null when the header is missing or unparseable so the
// caller can fall back to a deterministic default.
export function parseAttachmentFilename(disposition: string | null): string | null {
  if (!disposition) return null;
  const m = /filename="([^"]+)"/i.exec(disposition);
  return m ? m[1] : null;
}
