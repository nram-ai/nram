import { useState, useCallback } from "react";
import {
  useOAuthClients,
  useCreateOAuthClient,
  useDeleteOAuthClient,
  useIdPConfigs,
  useCreateIdPConfig,
  useDeleteIdPConfig,
  useOrgs,
} from "../hooks/useApi";
import type {
  OAuthClient,
  OAuthClientCreated,
  CreateOAuthClientRequest,
  CreateIdPConfigRequest,
  IdPConfig,
  Organization,
} from "../api/client";

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

function copyToClipboard(text: string): void {
  navigator.clipboard.writeText(text);
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
// IdP provider type labels
// ---------------------------------------------------------------------------

const IDP_TYPES = [
  { value: "google" as const, label: "Google" },
  { value: "github" as const, label: "GitHub" },
  { value: "oidc" as const, label: "Custom OIDC" },
];

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    copyToClipboard(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [text]);

  return (
    <button
      type="button"
      onClick={handleCopy}
      className="ml-1.5 rounded border border-input bg-background px-1.5 py-0.5 text-xs text-muted-foreground hover:bg-muted"
      title="Copy to clipboard"
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

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
  const createMutation = useCreateOAuthClient();

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
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="mx-4 w-full max-w-lg rounded-lg border border-border bg-card p-6 shadow-lg">
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
                  <CopyButton text={createdClient.client_id} />
                </div>
              </div>
              {createdClient.client_secret && (
                <div>
                  <label className="text-xs font-medium text-muted-foreground">
                    Client Secret
                  </label>
                  <div className="mt-1 rounded border border-amber-500 bg-amber-50 p-2 dark:bg-amber-950">
                    <div className="flex items-center gap-1">
                      <code className="text-sm break-all">
                        {createdClient.client_secret}
                      </code>
                      <CopyButton text={createdClient.client_secret} />
                    </div>
                    <p className="mt-1 text-xs text-amber-700 dark:text-amber-400">
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
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="mx-4 w-full max-w-sm rounded-lg border border-border bg-card p-6 shadow-lg">
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
// Delete IdP Confirmation Dialog
// ---------------------------------------------------------------------------

function DeleteIdPDialog({
  config,
  onConfirm,
  onCancel,
  isPending,
}: {
  config: IdPConfig;
  onConfirm: () => void;
  onCancel: () => void;
  isPending: boolean;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="mx-4 w-full max-w-sm rounded-lg border border-border bg-card p-6 shadow-lg">
        <h3 className="text-lg font-semibold">Delete Identity Provider</h3>
        <p className="mt-2 text-sm text-muted-foreground">
          Are you sure you want to delete the{" "}
          <span className="font-medium text-foreground">
            {config.provider_type.toUpperCase()}
          </span>{" "}
          identity provider
          {config.org_name ? ` for ${config.org_name}` : ""}? Users
          authenticating through this provider will no longer be able to sign in.
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
            {isPending ? "Deleting..." : "Delete"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Add IdP Dialog
// ---------------------------------------------------------------------------

function AddIdPDialog({
  open,
  onClose,
  orgs,
}: {
  open: boolean;
  onClose: () => void;
  orgs: Organization[];
}) {
  const createMutation = useCreateIdPConfig();

  const [providerType, setProviderType] = useState<"google" | "github" | "oidc">("google");
  const [orgId, setOrgId] = useState("");
  const [clientId, setClientId] = useState("");
  const [clientSecret, setClientSecret] = useState("");
  const [issuerUrl, setIssuerUrl] = useState("");
  const [authorizationUrl, setAuthorizationUrl] = useState("");
  const [tokenUrl, setTokenUrl] = useState("");
  const [userinfoUrl, setUserinfoUrl] = useState("");
  const [allowedDomains, setAllowedDomains] = useState("");
  const [autoProvision, setAutoProvision] = useState(false);
  const [error, setError] = useState("");

  const resetForm = useCallback(() => {
    setProviderType("google");
    setOrgId("");
    setClientId("");
    setClientSecret("");
    setIssuerUrl("");
    setAuthorizationUrl("");
    setTokenUrl("");
    setUserinfoUrl("");
    setAllowedDomains("");
    setAutoProvision(false);
    setError("");
  }, []);

  const handleCreate = useCallback(async () => {
    setError("");
    if (!orgId) {
      setError("Organization is required.");
      return;
    }
    if (!clientId.trim()) {
      setError("Client ID is required.");
      return;
    }
    if (!clientSecret.trim()) {
      setError("Client Secret is required.");
      return;
    }
    if (providerType === "oidc" && !issuerUrl.trim()) {
      setError("Issuer URL is required for Custom OIDC.");
      return;
    }

    const domains = allowedDomains
      .split(",")
      .map((d) => d.trim())
      .filter(Boolean);

    const data: CreateIdPConfigRequest = {
      org_id: orgId,
      provider_type: providerType,
      client_id: clientId.trim(),
      client_secret: clientSecret.trim(),
      allowed_domains: domains,
      auto_provision: autoProvision,
    };

    if (providerType === "oidc") {
      data.issuer_url = issuerUrl.trim();
      if (authorizationUrl.trim()) data.authorization_url = authorizationUrl.trim();
      if (tokenUrl.trim()) data.token_url = tokenUrl.trim();
      if (userinfoUrl.trim()) data.userinfo_url = userinfoUrl.trim();
    }

    try {
      await createMutation.mutateAsync(data);
      resetForm();
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to create IdP config.");
    }
  }, [
    orgId,
    clientId,
    clientSecret,
    providerType,
    issuerUrl,
    authorizationUrl,
    tokenUrl,
    userinfoUrl,
    allowedDomains,
    autoProvision,
    createMutation,
    resetForm,
    onClose,
  ]);

  const handleClose = useCallback(() => {
    resetForm();
    onClose();
  }, [resetForm, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="mx-4 max-h-[90vh] w-full max-w-lg overflow-y-auto rounded-lg border border-border bg-card p-6 shadow-lg">
        <h3 className="text-lg font-semibold">Add Identity Provider</h3>

        <div className="mt-4 space-y-3">
          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Provider Type
            </label>
            <select
              value={providerType}
              onChange={(e) => setProviderType(e.target.value as "google" | "github" | "oidc")}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
            >
              {IDP_TYPES.map((t) => (
                <option key={t.value} value={t.value}>
                  {t.label}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Organization
            </label>
            <select
              value={orgId}
              onChange={(e) => setOrgId(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
            >
              <option value="">Select organization...</option>
              {orgs.map((org) => (
                <option key={org.id} value={org.id}>
                  {org.name}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Client ID (from external IdP)
            </label>
            <input
              type="text"
              value={clientId}
              onChange={(e) => setClientId(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
              placeholder="External IdP client ID"
            />
          </div>

          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Client Secret (from external IdP)
            </label>
            <input
              type="password"
              value={clientSecret}
              onChange={(e) => setClientSecret(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
              placeholder="External IdP client secret"
            />
          </div>

          {providerType === "oidc" && (
            <>
              <div>
                <label className="text-xs font-medium text-muted-foreground">
                  Issuer URL
                </label>
                <input
                  type="url"
                  value={issuerUrl}
                  onChange={(e) => setIssuerUrl(e.target.value)}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="https://idp.example.com"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">
                  Authorization URL
                </label>
                <input
                  type="url"
                  value={authorizationUrl}
                  onChange={(e) => setAuthorizationUrl(e.target.value)}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="https://idp.example.com/authorize"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">
                  Token URL
                </label>
                <input
                  type="url"
                  value={tokenUrl}
                  onChange={(e) => setTokenUrl(e.target.value)}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="https://idp.example.com/token"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">
                  Userinfo URL
                </label>
                <input
                  type="url"
                  value={userinfoUrl}
                  onChange={(e) => setUserinfoUrl(e.target.value)}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
                  placeholder="https://idp.example.com/userinfo"
                />
              </div>
            </>
          )}

          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Allowed Domains (comma-separated)
            </label>
            <input
              type="text"
              value={allowedDomains}
              onChange={(e) => setAllowedDomains(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
              placeholder="example.com, corp.example.com"
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="auto-provision"
              checked={autoProvision}
              onChange={(e) => setAutoProvision(e.target.checked)}
              className="h-4 w-4 rounded border-input"
            />
            <label htmlFor="auto-provision" className="text-sm">
              Auto-provision users on first login
            </label>
          </div>
        </div>

        {error && <p className="mt-3 text-sm text-destructive">{error}</p>}

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
            {createMutation.isPending ? "Creating..." : "Add Provider"}
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
  const { data: clients, isLoading, error } = useOAuthClients();
  const deleteMutation = useDeleteOAuthClient();

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
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">OAuth Clients</h2>
          <p className="text-sm text-muted-foreground">
            Manage registered OAuth client applications.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowCreate(true)}
          className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
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
                <th className="px-4 py-2">Last Used</th>
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
                      <CopyButton text={client.client_id} />
                    </span>
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${
                        client.type === "auto"
                          ? "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300"
                          : "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300"
                      }`}
                    >
                      {client.type === "auto" ? "auto-registered" : "manual"}
                    </span>
                  </td>
                  <td className="max-w-[200px] truncate px-4 py-2 text-xs text-muted-foreground">
                    {client.redirect_uris.join(", ")}
                  </td>
                  <td className="whitespace-nowrap px-4 py-2 text-xs text-muted-foreground">
                    {formatDate(client.created_at)}
                  </td>
                  <td className="whitespace-nowrap px-4 py-2 text-xs text-muted-foreground">
                    {formatDate(client.last_used)}
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
// Identity Provider Section
// ---------------------------------------------------------------------------

function IdPSection() {
  const { data: configs, isLoading, error } = useIdPConfigs();
  const { data: orgs } = useOrgs();
  const deleteMutation = useDeleteIdPConfig();

  const [showAdd, setShowAdd] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<IdPConfig | null>(null);

  const handleDelete = useCallback(async () => {
    if (!deleteTarget) return;
    try {
      await deleteMutation.mutateAsync(deleteTarget.id);
      setDeleteTarget(null);
    } catch {
      // error handled by mutation state
    }
  }, [deleteTarget, deleteMutation]);

  const providerLabel = (type: string) => {
    const match = IDP_TYPES.find((t) => t.value === type);
    return match ? match.label : type;
  };

  return (
    <section>
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">Identity Providers</h2>
          <p className="text-sm text-muted-foreground">
            Configure external identity providers for SSO authentication per
            organization.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowAdd(true)}
          className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          Add Provider
        </button>
      </div>

      {isLoading && (
        <p className="mt-4 text-sm text-muted-foreground">Loading providers...</p>
      )}
      {error && (
        <p className="mt-4 text-sm text-destructive">
          Failed to load IdP configs:{" "}
          {error instanceof Error ? error.message : "Unknown error"}
        </p>
      )}

      {configs && configs.length === 0 && (
        <p className="mt-4 text-sm text-muted-foreground">
          No identity providers configured.
        </p>
      )}

      {configs && configs.length > 0 && (
        <div className="mt-4 overflow-x-auto rounded-md border border-border">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/50 text-left text-xs font-medium text-muted-foreground">
                <th className="px-4 py-2">Provider</th>
                <th className="px-4 py-2">Organization</th>
                <th className="px-4 py-2">Client ID</th>
                <th className="px-4 py-2">Allowed Domains</th>
                <th className="px-4 py-2">Auto-Provision</th>
                <th className="px-4 py-2">Created</th>
                <th className="px-4 py-2">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {configs.map((cfg) => (
                <tr key={cfg.id} className="hover:bg-muted/30">
                  <td className="px-4 py-2 font-medium">
                    {providerLabel(cfg.provider_type)}
                  </td>
                  <td className="px-4 py-2 text-muted-foreground">
                    {cfg.org_name || cfg.org_id}
                  </td>
                  <td className="px-4 py-2">
                    <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
                      {cfg.client_id.length > 24
                        ? `${cfg.client_id.slice(0, 24)}...`
                        : cfg.client_id}
                    </code>
                  </td>
                  <td className="max-w-[200px] truncate px-4 py-2 text-xs text-muted-foreground">
                    {cfg.allowed_domains.length > 0
                      ? cfg.allowed_domains.join(", ")
                      : "-"}
                  </td>
                  <td className="px-4 py-2">
                    <span
                      className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${
                        cfg.auto_provision
                          ? "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300"
                          : "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300"
                      }`}
                    >
                      {cfg.auto_provision ? "Enabled" : "Disabled"}
                    </span>
                  </td>
                  <td className="whitespace-nowrap px-4 py-2 text-xs text-muted-foreground">
                    {formatDate(cfg.created_at)}
                  </td>
                  <td className="px-4 py-2">
                    <button
                      type="button"
                      onClick={() => setDeleteTarget(cfg)}
                      className="rounded-md bg-destructive px-3 py-1 text-xs font-medium text-destructive-foreground hover:bg-destructive/90"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <AddIdPDialog
        open={showAdd}
        onClose={() => setShowAdd(false)}
        orgs={orgs ?? []}
      />

      {deleteTarget && (
        <DeleteIdPDialog
          config={deleteTarget}
          onConfirm={handleDelete}
          onCancel={() => setDeleteTarget(null)}
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
        <h1 className="text-2xl font-semibold tracking-tight">
          OAuth &amp; Identity
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Manage OAuth client applications and identity provider configurations.
        </p>
      </div>

      <OAuthClientsSection />

      <hr className="border-border" />

      <IdPSection />
    </div>
  );
}

export default OAuthClients;
