import { useState, useCallback } from "react";
import {
  useIdPConfigs,
  useCreateIdPConfig,
  useDeleteIdPConfig,
  useOrgs,
  useOrgIdPConfigs,
  useCreateOrgIdPConfig,
  useDeleteOrgIdPConfig,
} from "../hooks/useApi";
import { useAuth } from "../context/AuthContext";
import type {
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

// ---------------------------------------------------------------------------
// Provider type options
// ---------------------------------------------------------------------------

const IDP_TYPES = [
  { value: "oidc" as const, label: "OIDC" },
  { value: "saml" as const, label: "SAML" },
];

// The callback URL that must be registered with the external IdP.
const IDP_CALLBACK_URL = `${window.location.origin}/auth/idp/callback`;

// ---------------------------------------------------------------------------
// Delete Confirmation Dialog
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
          {config.org_id ? ` for org ${config.org_id}` : ""}? Users
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
// Create IdP Dialog (Admin — with org selector)
// ---------------------------------------------------------------------------

function CreateIdPDialog({
  open,
  onClose,
  orgs,
}: {
  open: boolean;
  onClose: () => void;
  orgs: Organization[];
}) {
  const createMutation = useCreateIdPConfig();

  const [orgId, setOrgId] = useState("");
  const [providerType, setProviderType] = useState<"oidc" | "saml">("oidc");
  const [clientId, setClientId] = useState("");
  const [clientSecret, setClientSecret] = useState("");
  const [issuerUrl, setIssuerUrl] = useState("");
  const [allowedDomains, setAllowedDomains] = useState("");
  const [autoProvision, setAutoProvision] = useState(false);
  const [error, setError] = useState("");

  const resetForm = useCallback(() => {
    setOrgId("");
    setProviderType("oidc");
    setClientId("");
    setClientSecret("");
    setIssuerUrl("");
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

    if (issuerUrl.trim()) {
      data.issuer_url = issuerUrl.trim();
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

          {renderIdPFormFields({
            providerType,
            setProviderType,
            clientId,
            setClientId,
            clientSecret,
            setClientSecret,
            issuerUrl,
            setIssuerUrl,
            allowedDomains,
            setAllowedDomains,
            autoProvision,
            setAutoProvision,
          })}
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
            {createMutation.isPending ? "Creating..." : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Create IdP Dialog (Org Owner — no org selector, uses own org_id)
// ---------------------------------------------------------------------------

function CreateOrgIdPDialog({
  open,
  onClose,
  orgId,
}: {
  open: boolean;
  onClose: () => void;
  orgId: string;
}) {
  const createMutation = useCreateOrgIdPConfig();

  const [providerType, setProviderType] = useState<"oidc" | "saml">("oidc");
  const [clientId, setClientId] = useState("");
  const [clientSecret, setClientSecret] = useState("");
  const [issuerUrl, setIssuerUrl] = useState("");
  const [allowedDomains, setAllowedDomains] = useState("");
  const [autoProvision, setAutoProvision] = useState(false);
  const [error, setError] = useState("");

  const resetForm = useCallback(() => {
    setProviderType("oidc");
    setClientId("");
    setClientSecret("");
    setIssuerUrl("");
    setAllowedDomains("");
    setAutoProvision(false);
    setError("");
  }, []);

  const handleCreate = useCallback(async () => {
    setError("");
    if (!clientId.trim()) {
      setError("Client ID is required.");
      return;
    }
    if (!clientSecret.trim()) {
      setError("Client Secret is required.");
      return;
    }

    const domains = allowedDomains
      .split(",")
      .map((d) => d.trim())
      .filter(Boolean);

    const data = {
      orgId,
      org_id: orgId,
      provider_type: providerType,
      client_id: clientId.trim(),
      client_secret: clientSecret.trim(),
      allowed_domains: domains,
      auto_provision: autoProvision,
      ...(issuerUrl.trim() ? { issuer_url: issuerUrl.trim() } : {}),
    };

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
          {renderIdPFormFields({
            providerType,
            setProviderType,
            clientId,
            setClientId,
            clientSecret,
            setClientSecret,
            issuerUrl,
            setIssuerUrl,
            allowedDomains,
            setAllowedDomains,
            autoProvision,
            setAutoProvision,
          })}
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
            {createMutation.isPending ? "Creating..." : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Shared form fields (used by both create dialogs)
// ---------------------------------------------------------------------------

function CallbackUrlField() {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(IDP_CALLBACK_URL).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, []);

  return (
    <div className="rounded-md border border-blue-200 bg-blue-50 p-3 dark:border-blue-800 dark:bg-blue-950">
      <label className="text-xs font-medium text-blue-700 dark:text-blue-300">
        Callback URL (enter this in your IdP)
      </label>
      <div className="mt-1 flex items-center gap-2">
        <code className="flex-1 rounded bg-white px-2 py-1 text-xs text-blue-900 dark:bg-blue-900 dark:text-blue-100">
          {IDP_CALLBACK_URL}
        </code>
        <button
          type="button"
          onClick={handleCopy}
          className="shrink-0 rounded-md border border-blue-300 bg-white px-2 py-1 text-xs font-medium text-blue-700 hover:bg-blue-100 dark:border-blue-700 dark:bg-blue-900 dark:text-blue-200 dark:hover:bg-blue-800"
        >
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
    </div>
  );
}

function renderIdPFormFields({
  providerType,
  setProviderType,
  clientId,
  setClientId,
  clientSecret,
  setClientSecret,
  issuerUrl,
  setIssuerUrl,
  allowedDomains,
  setAllowedDomains,
  autoProvision,
  setAutoProvision,
}: {
  providerType: "oidc" | "saml";
  setProviderType: (v: "oidc" | "saml") => void;
  clientId: string;
  setClientId: (v: string) => void;
  clientSecret: string;
  setClientSecret: (v: string) => void;
  issuerUrl: string;
  setIssuerUrl: (v: string) => void;
  allowedDomains: string;
  setAllowedDomains: (v: string) => void;
  autoProvision: boolean;
  setAutoProvision: (v: boolean) => void;
}) {
  return (
    <>
      <CallbackUrlField />

      <div>
        <label className="text-xs font-medium text-muted-foreground">
          Provider Type
        </label>
        <select
          value={providerType}
          onChange={(e) => setProviderType(e.target.value as "oidc" | "saml")}
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
          Client ID
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
          Client Secret
        </label>
        <input
          type="password"
          value={clientSecret}
          onChange={(e) => setClientSecret(e.target.value)}
          className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
          placeholder="External IdP client secret"
        />
      </div>

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
          id="idp-auto-provision"
          checked={autoProvision}
          onChange={(e) => setAutoProvision(e.target.checked)}
          className="h-4 w-4 rounded border-input"
        />
        <label htmlFor="idp-auto-provision" className="text-sm">
          Auto-provision users on first login
        </label>
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// IdP Table (shared between admin and org_owner views)
// ---------------------------------------------------------------------------

function IdPTable({
  configs,
  onDelete,
  showOrgColumn,
}: {
  configs: IdPConfig[];
  onDelete: (cfg: IdPConfig) => void;
  showOrgColumn: boolean;
}) {
  const providerLabel = (type: string) => {
    const match = IDP_TYPES.find((t) => t.value === type);
    return match ? match.label : type;
  };

  return (
    <div className="overflow-x-auto rounded-md border border-border">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-muted/50 text-left text-xs font-medium text-muted-foreground">
            {showOrgColumn && <th className="px-4 py-2">Organization</th>}
            <th className="px-4 py-2">Provider</th>
            <th className="px-4 py-2">Client ID</th>
            <th className="px-4 py-2">Issuer URL</th>
            <th className="px-4 py-2">Allowed Domains</th>
            <th className="px-4 py-2">Auto-Provision</th>
            <th className="px-4 py-2">Default Role</th>
            <th className="px-4 py-2">Created</th>
            <th className="px-4 py-2">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {configs.map((cfg) => (
            <tr key={cfg.id} className="hover:bg-muted/30">
              {showOrgColumn && (
                <td className="px-4 py-2 text-muted-foreground">
                  {cfg.org_id || "-"}
                </td>
              )}
              <td className="px-4 py-2 font-medium">
                {providerLabel(cfg.provider_type)}
              </td>
              <td className="px-4 py-2">
                <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
                  {cfg.client_id.length > 24
                    ? `${cfg.client_id.slice(0, 24)}...`
                    : cfg.client_id}
                </code>
              </td>
              <td className="max-w-[200px] truncate px-4 py-2 text-xs text-muted-foreground">
                {cfg.issuer_url || "-"}
              </td>
              <td className="max-w-[200px] truncate px-4 py-2 text-xs text-muted-foreground">
                {(cfg.allowed_domains ?? []).length > 0
                  ? (cfg.allowed_domains ?? []).join(", ")
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
              <td className="px-4 py-2 text-xs text-muted-foreground">
                {cfg.default_role || "-"}
              </td>
              <td className="whitespace-nowrap px-4 py-2 text-xs text-muted-foreground">
                {formatDate(cfg.created_at)}
              </td>
              <td className="px-4 py-2">
                <button
                  type="button"
                  onClick={() => onDelete(cfg)}
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
  );
}

// ---------------------------------------------------------------------------
// Admin View
// ---------------------------------------------------------------------------

function AdminIdPView() {
  const { data: configs, isLoading, error } = useIdPConfigs();
  const { data: orgs } = useOrgs();
  const deleteMutation = useDeleteIdPConfig();

  const [showCreate, setShowCreate] = useState(false);
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

  return (
    <div className="space-y-8">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Identity Providers
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Configure external identity providers for SSO authentication.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowCreate(true)}
          className="w-full sm:w-auto rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          Add Provider
        </button>
      </div>

      {isLoading && (
        <p className="text-sm text-muted-foreground">Loading providers...</p>
      )}
      {error && (
        <p className="text-sm text-destructive">
          Failed to load IdP configs:{" "}
          {error instanceof Error ? error.message : "Unknown error"}
        </p>
      )}

      {configs && configs.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No identity providers configured.
        </p>
      )}

      {configs && configs.length > 0 && (
        <IdPTable
          configs={configs}
          onDelete={setDeleteTarget}
          showOrgColumn={true}
        />
      )}

      <CreateIdPDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
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
    </div>
  );
}

// ---------------------------------------------------------------------------
// Org Owner View
// ---------------------------------------------------------------------------

function OrgOwnerIdPView({ orgId }: { orgId: string }) {
  const { data: configs, isLoading, error } = useOrgIdPConfigs(orgId);
  const deleteMutation = useDeleteOrgIdPConfig();

  const [showCreate, setShowCreate] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<IdPConfig | null>(null);

  const handleDelete = useCallback(async () => {
    if (!deleteTarget) return;
    try {
      await deleteMutation.mutateAsync({ orgId, id: deleteTarget.id });
      setDeleteTarget(null);
    } catch {
      // error handled by mutation state
    }
  }, [deleteTarget, deleteMutation, orgId]);

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Identity Providers
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Configure external identity providers for SSO authentication in your
            organization.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowCreate(true)}
          className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          Add Provider
        </button>
      </div>

      {isLoading && (
        <p className="text-sm text-muted-foreground">Loading providers...</p>
      )}
      {error && (
        <p className="text-sm text-destructive">
          Failed to load IdP configs:{" "}
          {error instanceof Error ? error.message : "Unknown error"}
        </p>
      )}

      {configs && configs.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No identity providers configured for your organization.
        </p>
      )}

      {configs && configs.length > 0 && (
        <IdPTable
          configs={configs}
          onDelete={setDeleteTarget}
          showOrgColumn={false}
        />
      )}

      <CreateOrgIdPDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        orgId={orgId}
      />

      {deleteTarget && (
        <DeleteIdPDialog
          config={deleteTarget}
          onConfirm={handleDelete}
          onCancel={() => setDeleteTarget(null)}
          isPending={deleteMutation.isPending}
        />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page (role-aware)
// ---------------------------------------------------------------------------

function IdPConfiguration() {
  const { user, isAdmin } = useAuth();

  if (isAdmin) {
    return <AdminIdPView />;
  }

  // Org owner view — use the user's own org_id.
  const orgId = user?.org_id ?? "";
  if (!orgId) {
    return (
      <div className="space-y-8">
        <h1 className="text-2xl font-semibold tracking-tight">
          Identity Providers
        </h1>
        <p className="text-sm text-destructive">
          Unable to determine your organization. Please contact an administrator.
        </p>
      </div>
    );
  }

  return <OrgOwnerIdPView orgId={orgId} />;
}

export default IdPConfiguration;
