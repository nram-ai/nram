import { useState, useCallback } from "react";
import {
  useMeOAuthClients,
  useCreateMeOAuthClient,
  useRevokeMeOAuthClient,
} from "../hooks/useApi";
import type {
  OAuthClient,
  OAuthClientCreated,
  CreateOAuthClientRequest,
} from "../api/client";
import { CopyButton } from "../components/CopyButton";

// Small bordered copy button styling used inline next to client credentials.
const OAUTH_COPY_CLASS =
  "ml-1.5 rounded border border-input bg-background px-1.5 py-0.5 text-xs text-muted-foreground hover:bg-muted";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDate(iso?: string | null): string {
  if (!iso) return "-";
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

// ---------------------------------------------------------------------------
// Presets
// ---------------------------------------------------------------------------

interface Preset {
  label: string;
  description: string;
  name: string;
  redirect_uris: string;
  client_type: "public" | "confidential";
}

const PRESETS: Preset[] = [
  {
    label: "Claude.ai",
    description: "Anthropic Claude MCP integration",
    name: "Claude.ai",
    redirect_uris: "https://claude.ai/api/mcp/auth_callback",
    client_type: "confidential",
  },
  {
    label: "ChatGPT",
    description: "OpenAI ChatGPT plugin integration",
    name: "ChatGPT",
    redirect_uris: "https://chatgpt.com/aip/plugin-api/auth/callback",
    client_type: "confidential",
  },
  {
    label: "Custom",
    description: "Manual configuration",
    name: "",
    redirect_uris: "",
    client_type: "public",
  },
];

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Create Client Dialog
// ---------------------------------------------------------------------------

function CreateClientDialog({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const createMutation = useCreateMeOAuthClient();

  const [name, setName] = useState("");
  const [redirectUris, setRedirectUris] = useState("");
  const [clientType, setClientType] = useState<"public" | "confidential">("public");
  const [createdClient, setCreatedClient] = useState<OAuthClientCreated | null>(null);
  const [error, setError] = useState("");

  const applyPreset = useCallback((preset: Preset) => {
    setName(preset.name);
    setRedirectUris(preset.redirect_uris);
    setClientType(preset.client_type);
    setError("");
  }, []);

  const handleCreate = useCallback(async () => {
    setError("");
    const uris = redirectUris
      .split("\n")
      .map((u) => u.trim())
      .filter(Boolean);
    if (!name.trim()) {
      setError("Client name is required.");
      return;
    }
    if (uris.length === 0) {
      setError("At least one redirect URI is required.");
      return;
    }

    const data: CreateOAuthClientRequest = {
      name: name.trim(),
      redirect_uris: uris,
      client_type: clientType,
    };

    try {
      const result = await createMutation.mutateAsync(data);
      setCreatedClient(result);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to create client.");
    }
  }, [name, redirectUris, clientType, createMutation]);

  const handleClose = useCallback(() => {
    setName("");
    setRedirectUris("");
    setClientType("public");
    setCreatedClient(null);
    setError("");
    onClose();
  }, [onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 flex h-screen w-full flex-col overflow-y-auto border-l max-w-lg border border-border bg-card p-6">
        {createdClient ? (
          <>
            <h3 className="text-lg font-semibold">Client Created</h3>
            <p className="mt-2 text-sm text-muted-foreground">
              Save the client secret now. It will not be shown again.
            </p>
            <div className="mt-4 space-y-3">
              <div>
                <label className="text-xs font-medium text-muted-foreground">Client ID</label>
                <div className="flex items-center gap-1">
                  <code className="rounded bg-muted px-2 py-1 text-sm break-all">
                    {createdClient.client_id}
                  </code>
                  <CopyButton text={createdClient.client_id} className={OAUTH_COPY_CLASS} title="Copy to clipboard" />
                </div>
              </div>
              {createdClient.client_secret && (
                <div>
                  <label className="text-xs font-medium text-muted-foreground">
                    Client Secret
                  </label>
                  <div className="mt-1 rounded border border-warning/40 bg-warning/10 p-2">
                    <div className="flex items-center gap-1">
                      <code className="text-sm break-all">
                        {createdClient.client_secret}
                      </code>
                      <CopyButton text={createdClient.client_secret} className={OAUTH_COPY_CLASS} title="Copy to clipboard" />
                    </div>
                    <p className="mt-1 text-xs text-warning">
                      This secret will not be shown again. Copy it now.
                    </p>
                  </div>
                </div>
              )}
            </div>
            <div className="mt-6 flex justify-end">
              <button
                type="button"
                onClick={handleClose}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              >
                Done
              </button>
            </div>
          </>
        ) : (
          <>
            <h3 className="text-lg font-semibold">Create OAuth Client</h3>

            {/* Presets */}
            <div className="mt-4">
              <label className="text-xs font-medium text-muted-foreground">
                Quick Presets
              </label>
              <div className="mt-1.5 grid grid-cols-3 gap-2">
                {PRESETS.map((preset) => (
                  <button
                    key={preset.label}
                    type="button"
                    onClick={() => applyPreset(preset)}
                    className="rounded-md border border-input bg-background px-3 py-2 text-left text-sm hover:bg-muted"
                  >
                    <span className="font-medium">{preset.label}</span>
                    <span className="mt-0.5 block text-xs text-muted-foreground">
                      {preset.description}
                    </span>
                  </button>
                ))}
              </div>
            </div>

            {/* Form */}
            <div className="mt-4 space-y-3">
              <div>
                <label className="text-xs font-medium text-muted-foreground">
                  Client Name
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="My OAuth Client"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">
                  Redirect URIs (one per line)
                </label>
                <textarea
                  value={redirectUris}
                  onChange={(e) => setRedirectUris(e.target.value)}
                  rows={3}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm font-mono"
                  placeholder="https://example.com/callback"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">
                  Client Type
                </label>
                <select
                  value={clientType}
                  onChange={(e) => setClientType(e.target.value as "public" | "confidential")}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                >
                  <option value="public">Public</option>
                  <option value="confidential">Confidential</option>
                </select>
              </div>
            </div>

            {error && (
              <p className="mt-3 text-sm text-destructive">{error}</p>
            )}

            <div className="mt-6 flex justify-end gap-2">
              <button
                type="button"
                onClick={handleClose}
                className="rounded-md border border-input bg-background px-4 py-2 text-sm font-medium hover:bg-muted"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleCreate}
                disabled={createMutation.isPending}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {createMutation.isPending ? "Creating..." : "Create Client"}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Revoke Confirmation Dialog
// ---------------------------------------------------------------------------

function RevokeDialog({
  client,
  onConfirm,
  onCancel,
  isPending,
}: {
  client: OAuthClient;
  onConfirm: () => void;
  onCancel: () => void;
  isPending: boolean;
}) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/30" onClick={onCancel} />
      <div className="relative z-10 flex h-screen w-full max-w-sm flex-col overflow-y-auto border-l border-border bg-card p-6">
        <h3 className="text-lg font-semibold">Revoke Client</h3>
        <p className="mt-2 text-sm text-muted-foreground">
          Are you sure you want to revoke{" "}
          <span className="font-medium text-foreground">{client.name}</span>?
          This action cannot be undone. All tokens issued to this client will be
          invalidated.
        </p>
        <div className="mt-6 flex justify-end gap-2">
          <button
            type="button"
            onClick={onCancel}
            className="rounded-md border border-input bg-background px-4 py-2 text-sm font-medium hover:bg-muted"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={isPending}
            className="rounded-md bg-destructive px-4 py-2 text-sm font-medium text-destructive-foreground hover:bg-destructive/90 disabled:opacity-50"
          >
            {isPending ? "Revoking..." : "Revoke"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// OAuth Clients Section
// ---------------------------------------------------------------------------

function OAuthClientsSection() {
  const { data: clients, isLoading, error } = useMeOAuthClients();
  const deleteMutation = useRevokeMeOAuthClient();

  const [showCreate, setShowCreate] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<OAuthClient | null>(null);

  const handleRevoke = useCallback(async () => {
    if (!revokeTarget) return;
    try {
      await deleteMutation.mutateAsync(revokeTarget.id);
      setRevokeTarget(null);
    } catch {
      // error is handled by mutation state
    }
  }, [revokeTarget, deleteMutation]);

  return (
    <section>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-lg font-semibold">OAuth Clients</h2>
          <p className="text-sm text-muted-foreground">
            Manage registered OAuth client applications.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowCreate(true)}
          className="w-full sm:w-auto rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          Create Client
        </button>
      </div>

      {isLoading && (
        <p className="mt-4 text-sm text-muted-foreground">Loading clients...</p>
      )}
      {error && (
        <p className="mt-4 text-sm text-destructive">
          Failed to load clients: {error instanceof Error ? error.message : "Unknown error"}
        </p>
      )}

      {clients && clients.length === 0 && (
        <p className="mt-4 text-sm text-muted-foreground">
          No OAuth clients registered yet.
        </p>
      )}

      {clients && clients.length > 0 && (
        <div className="mt-4 overflow-x-auto rounded-md border border-border">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/50 text-left text-xs font-medium text-muted-foreground">
                <th className="px-4 py-2">Client Name</th>
                <th className="px-4 py-2">Client ID</th>
                <th className="px-4 py-2">Type</th>
                <th className="px-4 py-2">Redirect URIs</th>
                <th className="px-4 py-2">Created</th>
                <th className="px-4 py-2">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {clients.map((client) => (
                <tr key={client.id} className="hover:bg-muted/30">
                  <td className="px-4 py-2 font-medium">{client.name}</td>
                  <td className="px-4 py-2">
                    <span className="flex items-center gap-1">
                      <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
                        {client.client_id.length > 20
                          ? `${client.client_id.slice(0, 20)}...`
                          : client.client_id}
                      </code>
                      <CopyButton text={client.client_id} className={OAUTH_COPY_CLASS} title="Copy to clipboard" />
                    </span>
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${
                        client.type === "auto"
                          ? "bg-info/10 text-info"
                          : "bg-success/10 text-success"
                      }`}
                    >
                      {client.type === "auto" ? "auto-registered" : "manual"}
                    </span>
                  </td>
                  <td className="max-w-[200px] truncate px-4 py-2 text-xs text-muted-foreground">
                    {(client.redirect_uris ?? []).join(", ")}
                  </td>
                  <td className="whitespace-nowrap px-4 py-2 text-xs text-muted-foreground">
                    {formatDate(client.created_at)}
                  </td>
                  <td className="px-4 py-2">
                    <button
                      type="button"
                      onClick={() => setRevokeTarget(client)}
                      className="rounded-md bg-destructive px-3 py-1 text-xs font-medium text-destructive-foreground hover:bg-destructive/90"
                    >
                      Revoke
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <CreateClientDialog open={showCreate} onClose={() => setShowCreate(false)} />

      {revokeTarget && (
        <RevokeDialog
          client={revokeTarget}
          onConfirm={handleRevoke}
          onCancel={() => setRevokeTarget(null)}
          isPending={deleteMutation.isPending}
        />
      )}
    </section>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

function OAuthClients() {
  return (
    <div className="space-y-8">
      <div>
        <h1 className="font-display text-3xl text-foreground">OAuth Clients</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Register OAuth client applications scoped to your account. Identity
          provider configuration lives on its own page (Identity Providers).
        </p>
      </div>

      <OAuthClientsSection />
    </div>
  );
}

export default OAuthClients;
