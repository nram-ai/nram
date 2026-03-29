/**
 * WebAuthn browser helpers for passkey registration and authentication.
 * Handles base64url encoding/decoding and browser credential API marshalling.
 */

/** Convert a base64url string to an ArrayBuffer. */
export function base64urlToBuffer(base64url: string): ArrayBuffer {
  // Pad to multiple of 4.
  let padded = base64url.replace(/-/g, "+").replace(/_/g, "/");
  while (padded.length % 4 !== 0) {
    padded += "=";
  }
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes.buffer;
}

/** Convert an ArrayBuffer to a base64url string (no padding). */
export function bufferToBase64url(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Check if WebAuthn is available in this browser. */
export function isWebAuthnAvailable(): boolean {
  return (
    typeof window !== "undefined" &&
    typeof window.PublicKeyCredential !== "undefined"
  );
}

// Module-level AbortController to cancel any pending WebAuthn ceremony
// before starting a new one. Browsers only allow one active
// navigator.credentials.get()/create() at a time.
let activeController: AbortController | null = null;

function getAbortSignal(): AbortSignal {
  if (activeController) {
    activeController.abort();
  }
  activeController = new AbortController();
  return activeController.signal;
}

/**
 * Prepare the server's CredentialCreationOptions for navigator.credentials.create().
 * Decodes base64url fields to ArrayBuffers as the browser API requires.
 */
export function prepareCreationOptions(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  serverOptions: any,
): CredentialCreationOptions {
  const publicKey = serverOptions.publicKey ?? serverOptions.Response ?? serverOptions;

  return {
    publicKey: {
      ...publicKey,
      challenge: base64urlToBuffer(publicKey.challenge),
      user: {
        ...publicKey.user,
        id: base64urlToBuffer(publicKey.user.id),
      },
      excludeCredentials: (publicKey.excludeCredentials ?? []).map(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (c: any) => ({
          ...c,
          id: base64urlToBuffer(c.id),
        }),
      ),
    },
    signal: getAbortSignal(),
  };
}

/**
 * Prepare the server's CredentialRequestOptions for navigator.credentials.get().
 * Decodes base64url fields to ArrayBuffers.
 */
export function prepareRequestOptions(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  serverOptions: any,
): CredentialRequestOptions {
  const publicKey = serverOptions.publicKey ?? serverOptions;

  return {
    publicKey: {
      ...publicKey,
      challenge: base64urlToBuffer(publicKey.challenge),
      allowCredentials: (publicKey.allowCredentials ?? []).map(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (c: any) => ({
          ...c,
          id: base64urlToBuffer(c.id),
        }),
      ),
    },
    signal: getAbortSignal(),
  };
}

/**
 * Serialize the browser's PublicKeyCredential (registration) to JSON for the server.
 */
export function serializeCreationResponse(
  credential: PublicKeyCredential,
): Record<string, unknown> {
  const response = credential.response as AuthenticatorAttestationResponse;
  return {
    id: credential.id,
    rawId: bufferToBase64url(credential.rawId),
    type: credential.type,
    response: {
      attestationObject: bufferToBase64url(response.attestationObject),
      clientDataJSON: bufferToBase64url(response.clientDataJSON),
    },
  };
}

/**
 * Serialize the browser's PublicKeyCredential (authentication) to JSON for the server.
 */
export function serializeAssertionResponse(
  credential: PublicKeyCredential,
): Record<string, unknown> {
  const response = credential.response as AuthenticatorAssertionResponse;
  return {
    id: credential.id,
    rawId: bufferToBase64url(credential.rawId),
    type: credential.type,
    response: {
      authenticatorData: bufferToBase64url(response.authenticatorData),
      clientDataJSON: bufferToBase64url(response.clientDataJSON),
      signature: bufferToBase64url(response.signature),
      userHandle: response.userHandle
        ? bufferToBase64url(response.userHandle)
        : undefined,
    },
  };
}
