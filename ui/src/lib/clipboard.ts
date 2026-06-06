// copyToClipboard writes text to the system clipboard and resolves to true on
// success, false on failure. Callers should only show a "Copied" affordance
// when this resolves true.
//
// The async Clipboard API (navigator.clipboard) is exposed only in a secure
// context: HTTPS, or http://localhost / http://127.0.0.1 (see MDN, "Clipboard:
// writeText()", Secure context required). nram is self-hosted and frequently
// served over plain HTTP on a LAN IP, where navigator.clipboard is undefined
// and any direct writeText call throws. For that case we fall back to the
// legacy document.execCommand("copy") path, which still works in insecure
// contexts across current browsers.
export async function copyToClipboard(text: string): Promise<boolean> {
  if (window.isSecureContext && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Fall through to the legacy path: the Clipboard API can still reject
      // (permission denied, document not focused) even in a secure context.
    }
  }
  return legacyCopy(text);
}

// legacyCopy copies via a hidden, off-screen <textarea> and the deprecated
// document.execCommand("copy"). It restores the prior selection so it does not
// disturb anything the user had selected, and returns false if the command is
// unavailable or throws.
function legacyCopy(text: string): boolean {
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.readOnly = true;
  // Keep the node out of view and out of the layout/scroll path.
  textarea.style.position = "fixed";
  textarea.style.top = "-9999px";
  textarea.style.left = "-9999px";
  textarea.setAttribute("aria-hidden", "true");

  const previousSelection = document.activeElement as HTMLElement | null;
  document.body.appendChild(textarea);

  let ok = false;
  try {
    textarea.select();
    textarea.setSelectionRange(0, text.length);
    ok = document.execCommand("copy");
  } catch {
    ok = false;
  } finally {
    textarea.remove();
    // Return focus to whatever the user was on before we hijacked it.
    previousSelection?.focus?.();
  }
  return ok;
}
