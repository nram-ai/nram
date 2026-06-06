import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { shareAcceptAPI, type APIError, type ShareAcceptResponse } from "../api/client";
import { copyToClipboard } from "../lib/clipboard";

type ShareAcceptState =
  | { status: "loading" }
  | { status: "ready"; data: ShareAcceptResponse }
  | { status: "error"; message: string };

function ShareUnavailable({ message }: { message: string }) {
  return (
    <div className="app-shell flex min-h-screen items-center justify-center p-6">
      <div className="w-full max-w-xl">
        <div className="text-center">
          <h1 className="font-display text-4xl text-foreground">Share unavailable</h1>
        </div>
        <div className="surface-elevated mt-8 rounded-lg p-6 shadow-lg shadow-black/10">
          <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
            <p className="text-sm text-destructive">{message}</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function CopyBlock({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  async function handleCopy() {
    // copyToClipboard falls back to execCommand on insecure contexts; if even
    // that fails, the value is still visible in the block for manual copy.
    const ok = await copyToClipboard(value);
    if (!ok) return;
    setCopied(true);
    window.setTimeout(() => setCopied(false), 2000);
  }
  return (
    <div className="flex items-stretch gap-2">
      <div className="flex-1 break-all rounded-md border border-border bg-background px-3 py-2.5 font-mono text-sm text-foreground">
        {value}
      </div>
      <button
        type="button"
        onClick={handleCopy}
        className="rounded-md bg-primary px-4 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
      >
        {copied ? "Copied" : label}
      </button>
    </div>
  );
}

function ShareAccept() {
  const [search] = useSearchParams();
  const [state, setState] = useState<ShareAcceptState>({ status: "loading" });

  useEffect(() => {
    const token = search.get("token")?.trim();
    if (!token) {
      setState({ status: "error", message: "Share token is required." });
      return;
    }
    let cancelled = false;
    shareAcceptAPI
      .get(token)
      .then((data) => {
        if (cancelled) return;
        setState({ status: "ready", data });
      })
      .catch((err: APIError) => {
        if (cancelled) return;
        const body = err.body as { error?: string } | undefined;
        setState({
          status: "error",
          message: body?.error ?? err.message ?? "Failed to load share.",
        });
      });
    return () => {
      cancelled = true;
    };
  }, [search]);

  if (state.status === "loading") {
    return (
      <div className="app-shell flex min-h-screen items-center justify-center p-6">
        <div className="text-sm text-muted-foreground">Loading share...</div>
      </div>
    );
  }

  if (state.status === "error") {
    return <ShareUnavailable message={state.message} />;
  }
  const data = state.data;
  if (data.error) {
    return <ShareUnavailable message={data.error} />;
  }

  const ownerName = data.owner_name?.trim();
  const heading = ownerName
    ? `${ownerName} shared "${data.share_name}" with you`
    : `You have been given access to "${data.share_name}"`;

  return (
    <div className="app-shell flex min-h-screen items-center justify-center p-6">
      <div className="w-full max-w-xl">
        <div className="text-center">
          <h1 className="font-display text-4xl text-foreground">{heading}</h1>
          {data.description && (
            <p className="mt-3 text-sm text-muted-foreground">{data.description}</p>
          )}
          {data.expires_at && (
            <p className="mt-1 text-xs text-muted-foreground">Access expires {data.expires_at}.</p>
          )}
        </div>

        <div className="surface-elevated mt-8 space-y-6 rounded-lg p-6 shadow-lg shadow-black/10">
          {data.grants && data.grants.length > 0 && (
            <section>
              <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Projects in this share
              </h2>
              <table className="mt-2 w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b border-border">
                    <th className="py-1.5 pr-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                      Project
                    </th>
                    <th className="py-1.5 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                      Access
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {data.grants.map((g, i) => (
                    <tr key={i} className="border-b border-border/40">
                      <td className="py-2 pr-2 text-foreground">
                        {g.project_name}{" "}
                        <code className="rounded bg-muted px-1.5 py-0.5 text-xs">{g.project_slug}</code>
                      </td>
                      <td className="py-2 text-foreground">
                        <span className="inline-block rounded-full bg-primary/20 px-2.5 py-0.5 text-xs font-medium text-primary">
                          {g.permission}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}

          {data.mcp_server_url && (
            <section>
              <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Add to your MCP client
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Paste this URL into Claude.ai&apos;s custom connector, ChatGPT&apos;s MCP server settings, or any MCP-capable tool.
              </p>
              <div className="mt-3">
                <CopyBlock label="Copy URL" value={data.mcp_server_url} />
              </div>
            </section>
          )}

          {data.share_token && (
            <section>
              <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Share token
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">
                When the MCP client prompts you to authorize, paste this token.
              </p>
              <div className="mt-3">
                <CopyBlock label="Copy token" value={data.share_token} />
              </div>
            </section>
          )}
        </div>
      </div>
    </div>
  );
}

export default ShareAccept;
