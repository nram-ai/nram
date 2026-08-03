import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  oauthAPI,
  isLoopbackRedirectUri,
  type APIError,
  type AuthorizeCompleteResponse,
  type AuthorizeContextResponse,
  type OAuthAuthorizeParams,
  type SharePreviewResponse,
} from "../api/client";
import { AuthBrand } from "../components/AuthBrand";
import { CopyButton } from "../components/CopyButton";
import { probeReachable } from "../lib/loopbackProbe";
import { safeExternalUrl } from "../lib/safeRedirect";

type AuthorizeContextState =
  | { status: "loading" }
  | { status: "ready"; context: AuthorizeContextResponse }
  | { status: "error"; message: string };

const inputClass =
  "mt-1.5 block w-full rounded-md border border-border bg-background px-3 py-2 font-mono text-sm text-foreground placeholder:text-muted-foreground shadow-sm transition-colors focus:border-ring focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2";
const primaryButtonClass =
  "w-full rounded-lg bg-primary px-4 py-2.5 text-sm font-medium text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const secondaryButtonClass =
  "w-full rounded-lg border border-border bg-background px-4 py-2.5 text-sm font-medium text-foreground shadow-sm transition-colors hover:bg-muted focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2";

function readOAuthParams(search: URLSearchParams): OAuthAuthorizeParams | null {
  const client_id = search.get("client_id");
  const redirect_uri = search.get("redirect_uri");
  const response_type = search.get("response_type") ?? "code";
  const code_challenge = search.get("code_challenge");
  const code_challenge_method = search.get("code_challenge_method") ?? "S256";
  if (!client_id || !redirect_uri || !code_challenge) {
    return null;
  }
  return {
    client_id,
    redirect_uri,
    response_type,
    code_challenge,
    code_challenge_method,
    scope: search.get("scope") ?? undefined,
    resource: search.get("resource") ?? undefined,
    state: search.get("state") ?? undefined,
  };
}

function HiddenOAuthFields({ context }: { context: AuthorizeContextResponse }) {
  return (
    <>
      <input type="hidden" name="client_id" value={context.client_id} />
      <input type="hidden" name="redirect_uri" value={context.redirect_uri} />
      <input type="hidden" name="response_type" value={context.response_type} />
      <input type="hidden" name="code_challenge" value={context.code_challenge} />
      <input
        type="hidden"
        name="code_challenge_method"
        value={context.code_challenge_method}
      />
      <input type="hidden" name="scope" value={context.scope ?? ""} />
      <input type="hidden" name="resource" value={context.resource ?? ""} />
      <input type="hidden" name="state" value={context.state ?? ""} />
    </>
  );
}

function GrantsTable({
  grants,
}: {
  grants: SharePreviewResponse["grants"];
}) {
  return (
    <table className="my-3 w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-border text-xs uppercase tracking-wider text-muted-foreground">
          <th className="py-1.5 pr-2 text-left font-medium">Project</th>
          <th className="py-1.5 text-left font-medium">Access</th>
        </tr>
      </thead>
      <tbody>
        {grants.map((g, i) => (
          <tr key={i} className="border-b border-border/40">
            <td className="py-1.5 pr-2 text-foreground">
              {g.project_name}{" "}
              <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
                {g.project_slug}
              </code>
            </td>
            <td className="py-1.5 text-foreground">{g.permission}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Authorize() {
  const [search] = useSearchParams();
  const [state, setState] = useState<AuthorizeContextState>({ status: "loading" });

  const oauthParams = useMemo(() => readOAuthParams(search), [search]);

  useEffect(() => {
    if (!oauthParams) {
      setState({
        status: "error",
        message:
          "Missing required OAuth parameters (client_id, redirect_uri, code_challenge).",
      });
      return;
    }
    let cancelled = false;
    oauthAPI
      .getAuthorizeContext(oauthParams)
      .then((result) => {
        if (cancelled) return;
        if ("redirect_to" in result) {
          const target = safeExternalUrl(result.redirect_to);
          if (target) {
            window.location.replace(target);
          } else {
            setState({
              status: "error",
              message: "The authorization server returned an invalid redirect target.",
            });
          }
          return;
        }
        setState({ status: "ready", context: result });
      })
      .catch((err: APIError) => {
        if (cancelled) return;
        const body = err.body as { error_description?: string; error?: string } | undefined;
        setState({
          status: "error",
          message:
            body?.error_description ?? body?.error ?? err.message ?? "Failed to load authorization request.",
        });
      });
    return () => {
      cancelled = true;
    };
  }, [oauthParams]);

  if (state.status === "loading") {
    return (
      <div className="app-shell flex min-h-screen items-center justify-center p-6">
        <div className="text-sm text-muted-foreground">Loading authorization request...</div>
      </div>
    );
  }

  if (state.status === "error") {
    return (
      <div className="app-shell flex min-h-screen items-center justify-center p-6">
        <div className="w-full max-w-md">
          <div className="text-center">
            <AuthBrand />
            <h1 className="mt-6 font-display text-4xl text-foreground">Authorize access</h1>
          </div>
          <div className="surface-elevated mt-8 rounded-lg p-6 shadow-lg shadow-black/10">
            <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
              <p className="text-sm text-destructive">{state.message}</p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return <AuthorizeReady context={state.context} />;
}

// AuthPageShell is the centered card layout shared by every consent state
// (the loopback manual / denied screens reuse it so branding and spacing stay
// identical to the main consent card).
function AuthPageShell({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="app-shell flex min-h-screen items-center justify-center p-6">
      <div className="w-full max-w-xl">
        <div className="text-center">
          <AuthBrand />
          <h1 className="mt-6 font-display text-4xl text-foreground">{title}</h1>
          {subtitle && <p className="mt-2 text-sm text-muted-foreground">{subtitle}</p>}
        </div>
        {children}
      </div>
    </div>
  );
}

// CopyRow renders a labeled, monospace, read-only value with a copy button.
// Used on the loopback manual-completion screen for the callback URL, code,
// and state.
function CopyRow({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div>
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        <CopyButton
          text={value}
          withIcon
          className="inline-flex items-center gap-1.5 rounded border border-input bg-background px-2 py-0.5 text-xs text-muted-foreground hover:bg-muted"
        />
      </div>
      <div className="surface-opaque mt-1 overflow-x-auto whitespace-pre-wrap break-all rounded-md border border-border px-3 py-2 font-mono text-xs text-foreground">
        {value}
      </div>
      {hint && <p className="mt-1 text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}

// ManualCompletion is shown for loopback redirect URIs when the callback
// server is not reachable from this browser (the headless / remote case): the
// browser's localhost is not the machine running the callback listener, so we
// surface the values for the user to paste back to their agent.
function ManualCompletion({
  result,
  clientName,
}: {
  result: AuthorizeCompleteResponse;
  clientName: string;
}) {
  return (
    <AuthPageShell
      title="Finish in your terminal"
      subtitle={
        <>
          {clientName} is listening on another machine, so this browser cannot
          reach its callback. Paste the values below back to the agent or
          harness when it asks for them.
        </>
      }
    >
      <div className="surface-elevated mt-8 space-y-4 rounded-lg p-6 shadow-lg shadow-black/10">
        <CopyRow
          label="Callback URL"
          value={result.callback_url}
          hint="Most agents accept the whole URL; the code and state below are the same values broken out."
        />
        <CopyRow label="Authorization code" value={result.code} />
        {result.state ? <CopyRow label="State" value={result.state} /> : null}
      </div>
    </AuthPageShell>
  );
}

type Completion =
  | { kind: "idle" }
  | { kind: "submitting" }
  | { kind: "manual"; result: AuthorizeCompleteResponse }
  | { kind: "denied" }
  | { kind: "error"; message: string };

function AuthorizeReady({ context }: { context: AuthorizeContextResponse }) {
  const [shareSecret, setShareSecret] = useState("");
  const [preview, setPreview] = useState<SharePreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [completion, setCompletion] = useState<Completion>({ kind: "idle" });

  const loopback = useMemo(
    () => isLoopbackRedirectUri(context.redirect_uri),
    [context.redirect_uri],
  );
  const busy = completion.kind === "submitting";
  const displayName = context.client_name?.trim() || "this application";

  async function handlePreview(e: React.FormEvent) {
    e.preventDefault();
    if (!shareSecret.trim()) {
      setPreviewError("Paste a share token to continue.");
      return;
    }
    setPreviewError(null);
    setPreviewLoading(true);
    try {
      const result = await oauthAPI.previewShare({
        client_id: context.client_id,
        redirect_uri: context.redirect_uri,
        response_type: context.response_type,
        code_challenge: context.code_challenge,
        code_challenge_method: context.code_challenge_method,
        scope: context.scope,
        resource: context.resource,
        state: context.state,
        share_token: shareSecret.trim(),
      });
      setPreview(result);
    } catch (err) {
      const apiErr = err as APIError;
      const body = apiErr.body as { error_description?: string } | undefined;
      setPreviewError(body?.error_description ?? apiErr.message ?? "Could not load this share.");
    } finally {
      setPreviewLoading(false);
    }
  }

  // buildCompleteRequest assembles the loopback complete-authorize body from
  // the consent context plus the user's decision, so the approve and deny
  // handlers share one source of truth for the OAuth params.
  function buildCompleteRequest(
    mode: "account" | "share",
    decision: "approve" | "deny",
    shareToken?: string,
  ) {
    return {
      client_id: context.client_id,
      redirect_uri: context.redirect_uri,
      response_type: context.response_type,
      code_challenge: context.code_challenge,
      code_challenge_method: context.code_challenge_method,
      scope: context.scope,
      resource: context.resource,
      state: context.state,
      auth_mode: mode,
      decision,
      share_token: shareToken,
    };
  }

  // Loopback approve: mint the code via the JSON endpoint, probe whether the
  // callback server is reachable from this browser, then either forward to it
  // or fall back to the manual paste-back screen.
  async function handleLoopbackApprove(mode: "account" | "share", shareToken?: string) {
    setCompletion({ kind: "submitting" });
    try {
      const result = await oauthAPI.completeAuthorize(
        buildCompleteRequest(mode, "approve", shareToken),
      );
      // Only auto-navigate to a scheme-safe absolute URL (http/https); a
      // javascript:/data:/etc. callback_url falls through to manual completion.
      // safeExternalUrl returns a normalized, parseable href (or null), so the
      // URL parse below cannot throw when safeCallback is non-null.
      const safeCallback = safeExternalUrl(result.callback_url);
      const origin = safeCallback ? new URL(safeCallback).origin : "";
      const reachable = origin ? await probeReachable(origin) : false;
      if (safeCallback && reachable) {
        window.location.assign(safeCallback);
        return;
      }
      setCompletion({ kind: "manual", result });
    } catch (err) {
      const apiErr = err as APIError;
      const body = apiErr.body as { error_description?: string; error?: string } | undefined;
      setCompletion({
        kind: "error",
        message:
          body?.error_description ?? body?.error ?? apiErr.message ?? "Authorization failed.",
      });
    }
  }

  async function handleLoopbackDeny() {
    setCompletion({ kind: "submitting" });
    try {
      await oauthAPI.completeAuthorize(buildCompleteRequest("account", "deny"));
    } catch {
      // Deny is best-effort: the code was never minted, so show the denied
      // confirmation regardless of how the call resolved.
    }
    setCompletion({ kind: "denied" });
  }

  if (completion.kind === "manual") {
    return <ManualCompletion result={completion.result} clientName={displayName} />;
  }

  if (completion.kind === "denied") {
    return (
      <AuthPageShell
        title="Authorization denied"
        subtitle={<>No access was granted to {displayName}. You can close this window.</>}
      >
        <div className="surface-elevated mt-8 rounded-lg p-6 shadow-lg shadow-black/10">
          <p className="text-sm text-muted-foreground">
            If this was a mistake, restart the authorization from your client.
          </p>
        </div>
      </AuthPageShell>
    );
  }

  const previewBody = preview && (
    <>
      <h2 className="text-base font-semibold text-foreground">You are about to authorize</h2>
      <p className="mt-2 text-sm text-foreground">
        {preview.owner_name ? (
          <>
            <strong>{preview.owner_name}</strong> shared
          </>
        ) : (
          <>You have been granted</>
        )}{" "}
        access to <strong>{preview.share_name}</strong>.
      </p>
      {preview.description && (
        <p className="mt-1 text-sm text-muted-foreground">{preview.description}</p>
      )}
      <p className="mt-1 text-sm text-muted-foreground">
        Access expires {preview.expires_at}.
        {preview.is_one_shot && (
          <>
            {" "}
            <strong className="text-foreground">One-shot:</strong> once approved, this share cannot be redeemed again.
          </>
        )}
      </p>
      <GrantsTable grants={preview.grants} />
    </>
  );

  return (
    <div className="app-shell flex min-h-screen items-center justify-center p-6">
      <div className="w-full max-w-xl">
        <div className="text-center">
          <AuthBrand />
          <h1 className="mt-6 font-display text-4xl text-foreground">Authorize {displayName}</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            This application is requesting access to your Neural Ram memory.
          </p>
          <p className="text-sm text-muted-foreground">
            Choose how to authorize.
          </p>
          <p className="mt-1 font-mono text-xs text-muted-foreground/70">
            client_id: {context.client_id}
          </p>
        </div>

        <div className="surface-elevated mt-8 space-y-4 rounded-lg p-6 shadow-lg shadow-black/10">
          {completion.kind === "error" && (
            <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-3">
              <p className="text-sm text-destructive">{completion.message}</p>
            </div>
          )}

          {context.account_user ? (
            loopback ? (
              <div className="rounded-md border border-border p-4">
                <h2 className="text-base font-semibold text-foreground">
                  Continue as {context.account_user.display_name}
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  Authorize this client to access your full account ({context.account_user.email}).
                </p>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => handleLoopbackApprove("account")}
                  className={`${primaryButtonClass} mt-4`}
                >
                  {busy ? "Authorizing..." : "Approve"}
                </button>
              </div>
            ) : (
              <form method="POST" action="/authorize" className="rounded-md border border-border p-4">
                <h2 className="text-base font-semibold text-foreground">
                  Continue as {context.account_user.display_name}
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  Authorize this client to access your full account ({context.account_user.email}).
                </p>
                <HiddenOAuthFields context={context} />
                <input type="hidden" name="auth_mode" value="account" />
                <input type="hidden" name="decision" value="approve" />
                <button type="submit" className={`${primaryButtonClass} mt-4`}>
                  Approve
                </button>
              </form>
            )
          ) : (
            <div className="rounded-md border border-border p-4">
              <h2 className="text-base font-semibold text-foreground">Log in to your Neural Ram account</h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Authorize this client with your own account credentials.
              </p>
              <a
                href={`/login?redirect=${encodeURIComponent(`/authorize?${new URLSearchParams(location.search).toString()}`)}`}
                className={`${primaryButtonClass} mt-4 inline-flex items-center justify-center`}
              >
                Sign in
              </a>
            </div>
          )}

          {context.share_token_supported &&
            (preview ? (
              loopback ? (
                <div className="rounded-md border border-border p-4">
                  {previewBody}
                  <div className="mt-2 flex gap-2">
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => setPreview(null)}
                      className={`${secondaryButtonClass} flex-1`}
                    >
                      Back
                    </button>
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => handleLoopbackApprove("share", shareSecret.trim())}
                      className={`${primaryButtonClass} flex-1`}
                    >
                      {busy ? "Authorizing..." : "Approve"}
                    </button>
                  </div>
                </div>
              ) : (
                <form method="POST" action="/authorize" className="rounded-md border border-border p-4">
                  {previewBody}
                  <HiddenOAuthFields context={context} />
                  <input type="hidden" name="auth_mode" value="share" />
                  <input type="hidden" name="decision" value="approve" />
                  <input type="hidden" name="share_token" value={shareSecret.trim()} />
                  <div className="mt-2 flex gap-2">
                    <button
                      type="button"
                      onClick={() => setPreview(null)}
                      className={`${secondaryButtonClass} flex-1`}
                    >
                      Back
                    </button>
                    <button type="submit" className={`${primaryButtonClass} flex-1`}>
                      Approve
                    </button>
                  </div>
                </form>
              )
            ) : (
              <form onSubmit={handlePreview} className="rounded-md border border-border p-4">
                <h2 className="text-base font-semibold text-foreground">I have a share link</h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  Paste a share token you received from another Neural Ram user. You will see what projects it covers before approving.
                </p>
                {previewError && (
                  <div className="mt-3 rounded-lg border border-destructive/40 bg-destructive/10 p-3">
                    <p className="text-sm text-destructive">{previewError}</p>
                  </div>
                )}
                <input
                  type="password"
                  autoComplete="off"
                  required
                  value={shareSecret}
                  onChange={(e) => setShareSecret(e.target.value)}
                  placeholder="nram_s_..."
                  className={inputClass}
                />
                <button
                  type="submit"
                  disabled={previewLoading}
                  className={`${primaryButtonClass} mt-3`}
                >
                  {previewLoading ? "Checking..." : "Continue"}
                </button>
              </form>
            ))}

          {loopback ? (
            <button
              type="button"
              disabled={busy}
              onClick={handleLoopbackDeny}
              className={secondaryButtonClass}
            >
              Deny
            </button>
          ) : (
            <form method="POST" action="/authorize">
              <HiddenOAuthFields context={context} />
              <input type="hidden" name="decision" value="deny" />
              <button type="submit" className={secondaryButtonClass}>
                Deny
              </button>
            </form>
          )}
        </div>
      </div>
    </div>
  );
}

export default Authorize;
