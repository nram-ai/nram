import { useState, useCallback } from "react";
import {
  useIdPConfigs,
  useCreateIdPConfig,
  useDeleteIdPConfig,
  useUpdateIdPConfig,
  useOrgs,
  useOrgIdPConfigs,
  useCreateOrgIdPConfig,
  useDeleteOrgIdPConfig,
  useUpdateOrgIdPConfig,
} from "../hooks/useApi";
import { useAuth } from "../context/AuthContext";
import type {
  CreateIdPConfigRequest,
  UpdateIdPConfigRequest,
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

const IDP_TYPES = [{ value: "oidc" as const, label: "OIDC" }];

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
  const [providerType, setProviderType] = useState<string>("oidc");
  const [clientId, setClientId] = useState("");
  const [clientSecret, setClientSecret] = useState("");
  const [issuerUrl, setIssuerUrl] = useState("");
  const [authorizeUrl, setAuthorizeUrl] = useState("");
  const [tokenUrl, setTokenUrl] = useState("");
  const [userinfoUrl, setUserinfoUrl] = useState("");
  const [allowedDomains, setAllowedDomains] = useState("");
  const [autoProvision, setAutoProvision] = useState(false);
  const [defaultRole, setDefaultRole] = useState("");
  const [error, setError] = useState("");

  const resetForm = useCallback(() => {
    setOrgId("");
    setProviderType("oidc");
    setClientId("");
    setClientSecret("");
    setIssuerUrl("");
    setAuthorizeUrl("");
    setTokenUrl("");
    setUserinfoUrl("");
    setAllowedDomains("");
    setAutoProvision(false);
    setDefaultRole("");
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
      ...(issuerUrl.trim() ? { issuer_url: issuerUrl.trim() } : {}),
      authorize_url: authorizeUrl.trim() || undefined,
      token_url: tokenUrl.trim() || undefined,
      userinfo_url: userinfoUrl.trim() || undefined,
      default_role: defaultRole.trim() || undefined,
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
    authorizeUrl,
    tokenUrl,
    userinfoUrl,
    allowedDomains,
    autoProvision,
    defaultRole,
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

          <IdPFormFields
            providerType={providerType}
            setProviderType={setProviderType}
            clientId={clientId}
            setClientId={setClientId}
            clientSecret={clientSecret}
            setClientSecret={setClientSecret}
            issuerUrl={issuerUrl}
            setIssuerUrl={setIssuerUrl}
            authorizeUrl={authorizeUrl}
            setAuthorizeUrl={setAuthorizeUrl}
            tokenUrl={tokenUrl}
            setTokenUrl={setTokenUrl}
            userinfoUrl={userinfoUrl}
            setUserinfoUrl={setUserinfoUrl}
            allowedDomains={allowedDomains}
            setAllowedDomains={setAllowedDomains}
            autoProvision={autoProvision}
            setAutoProvision={setAutoProvision}
            defaultRole={defaultRole}
            setDefaultRole={setDefaultRole}
          />
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

  const [providerType, setProviderType] = useState<string>("oidc");
  const [clientId, setClientId] = useState("");
  const [clientSecret, setClientSecret] = useState("");
  const [issuerUrl, setIssuerUrl] = useState("");
  const [authorizeUrl, setAuthorizeUrl] = useState("");
  const [tokenUrl, setTokenUrl] = useState("");
  const [userinfoUrl, setUserinfoUrl] = useState("");
  const [allowedDomains, setAllowedDomains] = useState("");
  const [autoProvision, setAutoProvision] = useState(false);
  const [defaultRole, setDefaultRole] = useState("");
  const [error, setError] = useState("");

  const resetForm = useCallback(() => {
    setProviderType("oidc");
    setClientId("");
    setClientSecret("");
    setIssuerUrl("");
    setAuthorizeUrl("");
    setTokenUrl("");
    setUserinfoUrl("");
    setAllowedDomains("");
    setAutoProvision(false);
    setDefaultRole("");
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
      authorize_url: authorizeUrl.trim() || undefined,
      token_url: tokenUrl.trim() || undefined,
      userinfo_url: userinfoUrl.trim() || undefined,
      default_role: defaultRole.trim() || undefined,
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
    authorizeUrl,
    tokenUrl,
    userinfoUrl,
    allowedDomains,
    autoProvision,
    defaultRole,
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
          <IdPFormFields
            providerType={providerType}
            setProviderType={setProviderType}
            clientId={clientId}
            setClientId={setClientId}
            clientSecret={clientSecret}
            setClientSecret={setClientSecret}
            issuerUrl={issuerUrl}
            setIssuerUrl={setIssuerUrl}
            authorizeUrl={authorizeUrl}
            setAuthorizeUrl={setAuthorizeUrl}
            tokenUrl={tokenUrl}
            setTokenUrl={setTokenUrl}
            userinfoUrl={userinfoUrl}
            setUserinfoUrl={setUserinfoUrl}
            allowedDomains={allowedDomains}
            setAllowedDomains={setAllowedDomains}
            autoProvision={autoProvision}
            setAutoProvision={setAutoProvision}
            defaultRole={defaultRole}
            setDefaultRole={setDefaultRole}
          />
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
// Shared form fields (used by create and edit dialogs)
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

type EndpointMode = "discovery" | "manual";

function IdPFormFields({
  providerType,
  setProviderType,
  clientId,
  setClientId,
  clientSecret,
  setClientSecret,
  clientSecretPlaceholder,
  issuerUrl,
  setIssuerUrl,
  authorizeUrl,
  setAuthorizeUrl,
  tokenUrl,
  setTokenUrl,
  userinfoUrl,
  setUserinfoUrl,
  allowedDomains,
  setAllowedDomains,
  autoProvision,
  setAutoProvision,
  defaultRole,
  setDefaultRole,
}: {
  providerType: string;
  setProviderType: (v: string) => void;
  clientId: string;
  setClientId: (v: string) => void;
  clientSecret: string;
  setClientSecret: (v: string) => void;
  clientSecretPlaceholder?: string;
  issuerUrl: string;
  setIssuerUrl: (v: string) => void;
  authorizeUrl: string;
  setAuthorizeUrl: (v: string) => void;
  tokenUrl: string;
  setTokenUrl: (v: string) => void;
  userinfoUrl: string;
  setUserinfoUrl: (v: string) => void;
  allowedDomains: string;
  setAllowedDomains: (v: string) => void;
  autoProvision: boolean;
  setAutoProvision: (v: boolean) => void;
  defaultRole: string;
  setDefaultRole: (v: string) => void;
}) {
  // Infer initial mode: if explicit endpoints are filled in, start in manual mode.
  const initialMode: EndpointMode =
    authorizeUrl.trim() || tokenUrl.trim() ? "manual" : "discovery";
  const [endpointMode, setEndpointMode] = useState<EndpointMode>(initialMode);

  return (
    <>
      <CallbackUrlField />

      <div>
        <label className="text-xs font-medium text-muted-foreground">
          Provider Type
        </label>
        <select
          value={providerType}
          onChange={(e) => setProviderType(e.target.value as string)}
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
          placeholder="Client ID"
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
          placeholder={clientSecretPlaceholder ?? "Client secret"}
        />
      </div>

      {/* Endpoint configuration mode toggle */}
      <div>
        <label className="text-xs font-medium text-muted-foreground">
          Endpoint Configuration
        </label>
        <div className="mt-1 flex rounded-md border border-input">
          <button
            type="button"
            onClick={() => setEndpointMode("discovery")}
            className={`flex-1 px-3 py-1.5 text-sm font-medium transition-colors ${
              endpointMode === "discovery"
                ? "bg-primary text-primary-foreground"
                : "bg-background text-muted-foreground hover:bg-muted"
            } rounded-l-md`}
          >
            OIDC Discovery
          </button>
          <button
            type="button"
            onClick={() => setEndpointMode("manual")}
            className={`flex-1 px-3 py-1.5 text-sm font-medium transition-colors ${
              endpointMode === "manual"
                ? "bg-primary text-primary-foreground"
                : "bg-background text-muted-foreground hover:bg-muted"
            } rounded-r-md`}
          >
            Manual
          </button>
        </div>
      </div>

      {endpointMode === "discovery" ? (
        <div>
          <label className="text-xs font-medium text-muted-foreground">
            Issuer URL
          </label>
          <input
            type="url"
            value={issuerUrl}
            onChange={(e) => setIssuerUrl(e.target.value)}
            className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
            placeholder="https://accounts.google.com"
          />
          <p className="mt-1 text-xs text-muted-foreground">
            Endpoints will be resolved automatically via{" "}
            <code className="text-xs">.well-known/openid-configuration</code>
          </p>
        </div>
      ) : (
        <>
          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Authorize URL
            </label>
            <input
              type="url"
              value={authorizeUrl}
              onChange={(e) => setAuthorizeUrl(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
              placeholder="https://example.com/oauth/authorize"
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
              placeholder="https://example.com/oauth/token"
            />
          </div>

          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Userinfo URL{" "}
              <span className="font-normal text-muted-foreground">(optional)</span>
            </label>
            <input
              type="url"
              value={userinfoUrl}
              onChange={(e) => setUserinfoUrl(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
              placeholder="https://example.com/oauth/userinfo"
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
          id="idp-auto-provision"
          checked={autoProvision}
          onChange={(e) => setAutoProvision(e.target.checked)}
          className="h-4 w-4 rounded border-input"
        />
        <label htmlFor="idp-auto-provision" className="text-sm">
          Auto-provision users on first login
        </label>
      </div>

      <div>
        <label className="text-xs font-medium text-muted-foreground">
          Default Role
        </label>
        <input
          type="text"
          value={defaultRole}
          onChange={(e) => setDefaultRole(e.target.value)}
          className="mt-1 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm"
          placeholder="member"
        />
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Edit IdP Dialog
// ---------------------------------------------------------------------------

function EditIdPDialog({
  config,
  onClose,
  onSave,
  isPending,
}: {
  config: IdPConfig;
  onClose: () => void;
  onSave: (data: UpdateIdPConfigRequest) => void;
  isPending: boolean;
}) {
  const [providerType, setProviderType] = useState<string>(
    config.provider_type as string
  );
  const [clientId, setClientId] = useState(config.client_id);
  const [clientSecret, setClientSecret] = useState("");
  const [issuerUrl, setIssuerUrl] = useState(config.issuer_url ?? "");
  const [authorizeUrl, setAuthorizeUrl] = useState(config.authorize_url ?? "");
  const [tokenUrl, setTokenUrl] = useState(config.token_url ?? "");
  const [userinfoUrl, setUserinfoUrl] = useState(config.userinfo_url ?? "");
  const [allowedDomains, setAllowedDomains] = useState(
    (config.allowed_domains ?? []).join(", ")
  );
  const [autoProvision, setAutoProvision] = useState(config.auto_provision);
  const [defaultRole, setDefaultRole] = useState(config.default_role ?? "");
  const [error, setError] = useState("");

  const handleSave = useCallback(() => {
    setError("");

    const data: UpdateIdPConfigRequest = {};

    if (clientId.trim() !== config.client_id) {
      data.client_id = clientId.trim();
    }
    if (clientSecret.trim()) {
      data.client_secret = clientSecret.trim();
    }

    const newIssuer = issuerUrl.trim() || null;
    if (newIssuer !== (config.issuer_url ?? null)) {
      data.issuer_url = newIssuer;
    }

    const newAuthorize = authorizeUrl.trim() || null;
    if (newAuthorize !== (config.authorize_url ?? null)) {
      data.authorize_url = newAuthorize;
    }

    const newToken = tokenUrl.trim() || null;
    if (newToken !== (config.token_url ?? null)) {
      data.token_url = newToken;
    }

    const newUserinfo = userinfoUrl.trim() || null;
    if (newUserinfo !== (config.userinfo_url ?? null)) {
      data.userinfo_url = newUserinfo;
    }

    const newDomains = allowedDomains
      .split(",")
      .map((d) => d.trim())
      .filter(Boolean);
    const oldDomains = config.allowed_domains ?? [];
    if (JSON.stringify(newDomains) !== JSON.stringify(oldDomains)) {
      data.allowed_domains = newDomains;
    }

    if (autoProvision !== config.auto_provision) {
      data.auto_provision = autoProvision;
    }

    const newDefaultRole = defaultRole.trim() || undefined;
    if ((newDefaultRole ?? "") !== (config.default_role ?? "")) {
      data.default_role = newDefaultRole;
    }

    if (Object.keys(data).length === 0) {
      setError("No changes detected.");
      return;
    }

    onSave(data);
  }, [
    clientId,
    clientSecret,
    issuerUrl,
    authorizeUrl,
    tokenUrl,
    userinfoUrl,
    allowedDomains,
    autoProvision,
    defaultRole,
    config,
    onSave,
  ]);

  // providerType is read-only in edit mode (shown but not changeable)
  void providerType;
  void setProviderType;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="mx-4 max-h-[90vh] w-full max-w-lg overflow-y-auto rounded-lg border border-border bg-card p-6 shadow-lg">
        <h3 className="text-lg font-semibold">Edit Identity Provider</h3>

        <div className="mt-4 space-y-3">
          <IdPFormFields
            providerType={config.provider_type as string}
            setProviderType={() => {}}
            clientId={clientId}
            setClientId={setClientId}
            clientSecret={clientSecret}
            setClientSecret={setClientSecret}
            clientSecretPlaceholder="Leave blank to keep current"
            issuerUrl={issuerUrl}
            setIssuerUrl={setIssuerUrl}
            authorizeUrl={authorizeUrl}
            setAuthorizeUrl={setAuthorizeUrl}
            tokenUrl={tokenUrl}
            setTokenUrl={setTokenUrl}
            userinfoUrl={userinfoUrl}
            setUserinfoUrl={setUserinfoUrl}
            allowedDomains={allowedDomains}
            setAllowedDomains={setAllowedDomains}
            autoProvision={autoProvision}
            setAutoProvision={setAutoProvision}
            defaultRole={defaultRole}
            setDefaultRole={setDefaultRole}
          />
        </div>

        {error && <p className="mt-3 text-sm text-destructive">{error}</p>}

        <div className="mt-6 flex justify-end gap-2">
          <button
            type="button"
            onClick={onClose}
            className="rounded-md border border-input bg-background px-4 py-2 text-sm font-medium hover:bg-muted"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSave}
            disabled={isPending}
            className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            {isPending ? "Saving..." : "Save"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// IdP Table (shared between admin and org_owner views)
// ---------------------------------------------------------------------------

function IdPTable({
  configs,
  onDelete,
  onEdit,
  showOrgColumn,
}: {
  configs: IdPConfig[];
  onDelete: (cfg: IdPConfig) => void;
  onEdit: (cfg: IdPConfig) => void;
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
              <td className="flex gap-2 px-4 py-2">
                <button
                  type="button"
                  onClick={() => onEdit(cfg)}
                  className="rounded-md border border-input bg-background px-3 py-1 text-xs font-medium hover:bg-muted"
                >
                  Edit
                </button>
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
  const updateMutation = useUpdateIdPConfig();

  const [showCreate, setShowCreate] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<IdPConfig | null>(null);
  const [editTarget, setEditTarget] = useState<IdPConfig | null>(null);

  const handleDelete = useCallback(async () => {
    if (!deleteTarget) return;
    try {
      await deleteMutation.mutateAsync(deleteTarget.id);
      setDeleteTarget(null);
    } catch {
      // error handled by mutation state
    }
  }, [deleteTarget, deleteMutation]);

  const handleUpdate = useCallback(
    async (data: UpdateIdPConfigRequest) => {
      if (!editTarget) return;
      try {
        await updateMutation.mutateAsync({ id: editTarget.id, data });
        setEditTarget(null);
      } catch {
        // error handled by mutation state
      }
    },
    [editTarget, updateMutation]
  );

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
          onEdit={setEditTarget}
          showOrgColumn={true}
        />
      )}

      <CreateIdPDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        orgs={orgs ?? []}
      />

      {editTarget && (
        <EditIdPDialog
          config={editTarget}
          onClose={() => setEditTarget(null)}
          onSave={handleUpdate}
          isPending={updateMutation.isPending}
        />
      )}

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
  const updateMutation = useUpdateOrgIdPConfig();

  const [showCreate, setShowCreate] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<IdPConfig | null>(null);
  const [editTarget, setEditTarget] = useState<IdPConfig | null>(null);

  const handleDelete = useCallback(async () => {
    if (!deleteTarget) return;
    try {
      await deleteMutation.mutateAsync({ orgId, id: deleteTarget.id });
      setDeleteTarget(null);
    } catch {
      // error handled by mutation state
    }
  }, [deleteTarget, deleteMutation, orgId]);

  const handleUpdate = useCallback(
    async (data: UpdateIdPConfigRequest) => {
      if (!editTarget) return;
      try {
        await updateMutation.mutateAsync({ orgId, id: editTarget.id, data });
        setEditTarget(null);
      } catch {
        // error handled by mutation state
      }
    },
    [editTarget, updateMutation, orgId]
  );

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
          onEdit={setEditTarget}
          showOrgColumn={false}
        />
      )}

      <CreateOrgIdPDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        orgId={orgId}
      />

      {editTarget && (
        <EditIdPDialog
          config={editTarget}
          onClose={() => setEditTarget(null)}
          onSave={handleUpdate}
          isPending={updateMutation.isPending}
        />
      )}

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
