import { useState, useCallback } from "react";
import { useAuth } from "../context/AuthContext";
import {
  useMeAPIKeys,
  useCreateMeAPIKey,
  useRevokeMeAPIKey,
  useChangePassword,
  useMeOAuthClients,
  useCreateMeOAuthClient,
  useRevokeMeOAuthClient,
  useMePasskeys,
  useRegisterPasskey,
  useDeletePasskey,
} from "../hooks/useApi";
import type { APIKey, OAuthClient, OAuthClientCreated, Passkey } from "../api/client";
import { isWebAuthnAvailable } from "../api/webauthn";

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

function roleBadgeClass(role: string): string {
  switch (role) {
    case "administrator":
      return "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300";
    case "org_owner":
      return "bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-300";
    case "member":
      return "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300";
    case "readonly":
      return "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300";
    case "service":
      return "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300";
    default:
      return "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300";
  }
}

function decodeJWTExpiry(token: string): string | null {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const payload = JSON.parse(atob(parts[1]));
    if (payload.exp) {
      return new Date(payload.exp * 1000).toLocaleString();
    }
    return null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// API Key Row
// ---------------------------------------------------------------------------

function APIKeyRow({
  apiKey,
  onRevoke,
  revoking,
}: {
  apiKey: APIKey;
  onRevoke: (id: string) => void;
  revoking: boolean;
}) {
  const [confirmRevoke, setConfirmRevoke] = useState(false);

  return (
    <tr className="border-b last:border-0">
      <td className="px-4 py-3 text-sm">
        <span className="font-medium">{apiKey.name}</span>
      </td>
      <td className="px-4 py-3 text-sm font-mono text-muted-foreground">
        {apiKey.key_prefix}...
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground">
        {formatDate(apiKey.created_at)}
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground">
        {apiKey.last_used ? formatDate(apiKey.last_used) : "Never"}
      </td>
      <td className="px-4 py-3 text-right">
        {confirmRevoke ? (
          <span className="inline-flex items-center gap-2">
            <button
              type="button"
              className="rounded bg-red-600 px-2 py-1 text-xs text-white hover:bg-red-700 disabled:opacity-50"
              onClick={() => onRevoke(apiKey.id)}
              disabled={revoking}
            >
              {revoking ? "Revoking..." : "Confirm"}
            </button>
            <button
              type="button"
              className="rounded border px-2 py-1 text-xs hover:bg-muted"
              onClick={() => setConfirmRevoke(false)}
            >
              Cancel
            </button>
          </span>
        ) : (
          <button
            type="button"
            className="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
            onClick={() => setConfirmRevoke(true)}
          >
            Revoke
          </button>
        )}
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// OAuth Client Row
// ---------------------------------------------------------------------------

function OAuthClientRow({
  client,
  onRevoke,
  revoking,
}: {
  client: OAuthClient;
  onRevoke: (id: string) => void;
  revoking: boolean;
}) {
  const [confirmRevoke, setConfirmRevoke] = useState(false);

  return (
    <tr className="border-b last:border-0">
      <td className="px-4 py-3 text-sm">
        <span className="font-medium">{client.name}</span>
      </td>
      <td className="px-4 py-3 text-sm font-mono text-muted-foreground">
        {client.client_id}
      </td>
      <td className="px-4 py-3 text-sm">
        <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${
          client.client_type === "confidential"
            ? "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300"
            : "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300"
        }`}>
          {client.client_type}
        </span>
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground max-w-[200px] truncate" title={client.redirect_uris?.join(", ") || "-"}>
        {client.redirect_uris?.join(", ") || "-"}
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground">
        {formatDate(client.created_at)}
      </td>
      <td className="px-4 py-3 text-right">
        {confirmRevoke ? (
          <span className="inline-flex items-center gap-2">
            <button
              type="button"
              className="rounded bg-red-600 px-2 py-1 text-xs text-white hover:bg-red-700 disabled:opacity-50"
              onClick={() => onRevoke(client.id)}
              disabled={revoking}
            >
              {revoking ? "Revoking..." : "Confirm"}
            </button>
            <button
              type="button"
              className="rounded border px-2 py-1 text-xs hover:bg-muted"
              onClick={() => setConfirmRevoke(false)}
            >
              Cancel
            </button>
          </span>
        ) : (
          <button
            type="button"
            className="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
            onClick={() => setConfirmRevoke(true)}
          >
            Revoke
          </button>
        )}
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Create OAuth Client Form
// ---------------------------------------------------------------------------

function CreateOAuthClientForm({ onCreated }: { onCreated: (client: OAuthClientCreated) => void }) {
  const createMut = useCreateMeOAuthClient();
  const [name, setName] = useState("");
  const [redirectUris, setRedirectUris] = useState("");
  const [clientType, setClientType] = useState<"public" | "confidential">("confidential");

  function handleCreate() {
    if (!name.trim()) return;
    const uris = redirectUris
      .split("\n")
      .map((u) => u.trim())
      .filter(Boolean);
    createMut.mutate(
      {
        name: name.trim(),
        redirect_uris: uris.length > 0 ? uris : undefined,
        client_type: clientType,
      },
      {
        onSuccess: (data) => {
          onCreated(data);
          setName("");
          setRedirectUris("");
          setClientType("confidential");
        },
      },
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-end gap-3">
        <div className="flex-1">
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            Name
          </label>
          <input
            type="text"
            className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. my-dev-app"
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                handleCreate();
              }
            }}
          />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            Type
          </label>
          <div className="inline-flex rounded-md border">
            <button
              type="button"
              className={`px-3 py-2 text-xs font-medium rounded-l-md ${
                clientType === "confidential"
                  ? "bg-primary text-primary-foreground"
                  : "bg-background hover:bg-muted"
              }`}
              onClick={() => setClientType("confidential")}
            >
              Confidential
            </button>
            <button
              type="button"
              className={`px-3 py-2 text-xs font-medium rounded-r-md border-l ${
                clientType === "public"
                  ? "bg-primary text-primary-foreground"
                  : "bg-background hover:bg-muted"
              }`}
              onClick={() => setClientType("public")}
            >
              Public
            </button>
          </div>
        </div>
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Redirect URIs (one per line)
        </label>
        <textarea
          className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
          rows={2}
          value={redirectUris}
          onChange={(e) => setRedirectUris(e.target.value)}
          placeholder="https://example.com/callback"
        />
      </div>
      <button
        type="button"
        className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
        onClick={handleCreate}
        disabled={!name.trim() || createMut.isPending}
      >
        {createMut.isPending ? "Creating..." : "Create OAuth Client"}
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Create API Key Form
// ---------------------------------------------------------------------------

function CreateAPIKeyForm({ onCreated }: { onCreated: (key: string) => void }) {
  const createMut = useCreateMeAPIKey();
  const [label, setLabel] = useState("");

  function handleCreate() {
    if (!label.trim()) return;
    createMut.mutate(
      { name: label.trim() },
      {
        onSuccess: (data) => {
          onCreated(data.key);
          setLabel("");
        },
      },
    );
  }

  return (
    <div className="flex items-end gap-3">
      <div className="flex-1">
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Label
        </label>
        <input
          type="text"
          className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
          value={label}
          onChange={(e) => setLabel(e.target.value)}
          placeholder="e.g. dev-laptop"
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              handleCreate();
            }
          }}
        />
      </div>
      <button
        type="button"
        className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
        onClick={handleCreate}
        disabled={!label.trim() || createMut.isPending}
      >
        {createMut.isPending ? "Creating..." : "Create API Key"}
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Change Password Card
// ---------------------------------------------------------------------------

function ChangePasswordCard() {
  const mutation = useChangePassword();
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [feedback, setFeedback] = useState<{ type: "success" | "error"; message: string } | null>(null);

  const mismatch = confirmPassword !== "" && newPassword !== confirmPassword;
  const tooShort = newPassword !== "" && newPassword.length < 8;
  const canSubmit =
    currentPassword !== "" &&
    newPassword.length >= 8 &&
    newPassword === confirmPassword &&
    !mutation.isPending;

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setFeedback(null);
    mutation.mutate(
      { currentPassword, newPassword },
      {
        onSuccess: () => {
          setFeedback({ type: "success", message: "Password changed successfully." });
          setCurrentPassword("");
          setNewPassword("");
          setConfirmPassword("");
        },
        onError: (err) => {
          const msg =
            err instanceof Error ? err.message : "Failed to change password.";
          // Try to extract a more specific message from APIError body.
          let detail = msg;
          if ("body" in err && typeof (err as Record<string, unknown>).body === "object" && (err as Record<string, unknown>).body !== null) {
            const body = (err as Record<string, unknown>).body as Record<string, unknown>;
            if (typeof body.message === "string") {
              detail = body.message;
            }
          }
          setFeedback({ type: "error", message: detail });
        },
      },
    );
  }

  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Change Password</h2>
      </div>
      <form onSubmit={handleSubmit} className="space-y-4 p-4">
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            Current Password
          </label>
          <input
            type="password"
            className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            value={currentPassword}
            onChange={(e) => setCurrentPassword(e.target.value)}
            autoComplete="current-password"
          />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            New Password
          </label>
          <input
            type="password"
            className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            value={newPassword}
            onChange={(e) => setNewPassword(e.target.value)}
            autoComplete="new-password"
          />
          {tooShort && (
            <p className="mt-1 text-xs text-red-600 dark:text-red-400">
              Must be at least 8 characters.
            </p>
          )}
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            Confirm New Password
          </label>
          <input
            type="password"
            className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            value={confirmPassword}
            onChange={(e) => setConfirmPassword(e.target.value)}
            autoComplete="new-password"
          />
          {mismatch && (
            <p className="mt-1 text-xs text-red-600 dark:text-red-400">
              Passwords do not match.
            </p>
          )}
        </div>

        {feedback && (
          <div
            className={`rounded-md px-3 py-2 text-sm ${
              feedback.type === "success"
                ? "bg-green-50 text-green-800 dark:bg-green-950/30 dark:text-green-300"
                : "bg-red-50 text-red-800 dark:bg-red-950/30 dark:text-red-300"
            }`}
          >
            {feedback.message}
          </div>
        )}

        <button
          type="submit"
          className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          disabled={!canSubmit}
        >
          {mutation.isPending ? "Changing..." : "Change Password"}
        </button>
      </form>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Passkey Row
// ---------------------------------------------------------------------------

function PasskeyRow({
  passkey,
  onDelete,
  deleting,
}: {
  passkey: Passkey;
  onDelete: (id: string) => void;
  deleting: boolean;
}) {
  const [confirmDelete, setConfirmDelete] = useState(false);

  return (
    <tr className="border-b last:border-0">
      <td className="px-4 py-3 text-sm">
        <span className="font-medium">{passkey.name}</span>
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground">
        {formatDate(passkey.created_at)}
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground">
        {passkey.last_used_at ? formatDate(passkey.last_used_at) : "Never"}
      </td>
      <td className="px-4 py-3 text-right">
        {confirmDelete ? (
          <span className="inline-flex items-center gap-2">
            <button
              type="button"
              className="rounded bg-red-600 px-2 py-1 text-xs text-white hover:bg-red-700 disabled:opacity-50"
              onClick={() => onDelete(passkey.id)}
              disabled={deleting}
            >
              {deleting ? "Deleting..." : "Confirm"}
            </button>
            <button
              type="button"
              className="rounded border px-2 py-1 text-xs hover:bg-muted"
              onClick={() => setConfirmDelete(false)}
            >
              Cancel
            </button>
          </span>
        ) : (
          <button
            type="button"
            className="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
            onClick={() => setConfirmDelete(true)}
          >
            Delete
          </button>
        )}
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Create Passkey Form
// ---------------------------------------------------------------------------

function CreatePasskeyForm({ onCreated }: { onCreated: () => void }) {
  const registerMut = useRegisterPasskey();
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  function handleCreate() {
    if (!name.trim()) return;
    setError(null);
    registerMut.mutate(
      { name: name.trim() },
      {
        onSuccess: () => {
          onCreated();
          setName("");
        },
        onError: (err) => {
          setError(err.message || "Failed to register passkey.");
        },
      },
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-end gap-3">
        <div className="flex-1">
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            Name
          </label>
          <input
            type="text"
            className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. MacBook Touch ID"
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                handleCreate();
              }
            }}
          />
        </div>
        <button
          type="button"
          className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          onClick={handleCreate}
          disabled={!name.trim() || registerMut.isPending}
        >
          {registerMut.isPending ? "Registering..." : "Register Passkey"}
        </button>
      </div>
      {error && (
        <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main MyAccount
// ---------------------------------------------------------------------------

function MyAccount() {
  const auth = useAuth();
  const apiKeysQuery = useMeAPIKeys();
  const revokeMut = useRevokeMeAPIKey();

  const [newKeyValue, setNewKeyValue] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const user = auth.user;
  const token = localStorage.getItem("nram_token");
  const expiry = token ? decodeJWTExpiry(token) : null;

  const passkeysQuery = useMePasskeys();
  const deletePasskeyMut = useDeletePasskey();

  const oauthClientsQuery = useMeOAuthClients();
  const revokeOAuthMut = useRevokeMeOAuthClient();

  const [newClient, setNewClient] = useState<OAuthClientCreated | null>(null);
  const [oauthCopied, setOauthCopied] = useState<string | null>(null);

  const apiKeys: APIKey[] = apiKeysQuery.data ?? [];
  const oauthClients: OAuthClient[] = oauthClientsQuery.data ?? [];

  const handleOAuthCopy = useCallback((text: string, field: string) => {
    navigator.clipboard.writeText(text).then(() => {
      setOauthCopied(field);
      setTimeout(() => setOauthCopied(null), 2000);
    });
  }, []);

  function handleRevokeOAuthClient(id: string) {
    revokeOAuthMut.mutate(id);
  }

  function handleOAuthClientCreated(client: OAuthClientCreated) {
    setNewClient(client);
  }

  const handleCopy = useCallback((text: string) => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, []);

  function handleRevoke(id: string) {
    revokeMut.mutate(id);
  }

  function handleKeyCreated(key: string) {
    setNewKeyValue(key);
  }

  if (!user) return null;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">My Account</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Manage your profile, API keys, OAuth clients, and session.
        </p>
      </div>

      {/* User Info */}
      <div className="rounded-lg border bg-card">
        <div className="border-b px-4 py-3">
          <h2 className="text-sm font-semibold">Profile</h2>
        </div>
        <div className="grid grid-cols-1 gap-4 p-4 sm:grid-cols-2">
          <div>
            <span className="text-xs font-medium text-muted-foreground">Email</span>
            <p className="mt-0.5 text-sm">{user.email}</p>
          </div>
          <div>
            <span className="text-xs font-medium text-muted-foreground">Display Name</span>
            <p className="mt-0.5 text-sm">{user.display_name || "-"}</p>
          </div>
          <div>
            <span className="text-xs font-medium text-muted-foreground">Role</span>
            <p className="mt-1">
              <span className={`inline-block rounded-full px-2.5 py-0.5 text-xs font-medium ${roleBadgeClass(user.role)}`}>
                {user.role}
              </span>
            </p>
          </div>
          <div>
            <span className="text-xs font-medium text-muted-foreground">Organization ID</span>
            <p className="mt-0.5 text-sm font-mono text-muted-foreground">
              {user.org_id || "-"}
            </p>
          </div>
        </div>
      </div>

      {/* Session */}
      <div className="rounded-lg border bg-card">
        <div className="border-b px-4 py-3">
          <h2 className="text-sm font-semibold">Session</h2>
        </div>
        <div className="flex items-center justify-between p-4">
          <div>
            <span className="text-xs font-medium text-muted-foreground">Token Expires</span>
            <p className="mt-0.5 text-sm">{expiry ?? "Unknown"}</p>
          </div>
          <button
            type="button"
            className="rounded border border-red-300 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
            onClick={() => auth.logout()}
          >
            Logout
          </button>
        </div>
      </div>

      {/* Change Password */}
      <ChangePasswordCard />

      {/* Passkeys */}
      {isWebAuthnAvailable() && (
        <div className="rounded-lg border bg-card">
          <div className="border-b px-4 py-3">
            <h2 className="text-sm font-semibold">Passkeys</h2>
          </div>
          <div className="p-4 space-y-4">
            <CreatePasskeyForm onCreated={() => {}} />

            {passkeysQuery.isLoading ? (
              <div className="py-4 text-center text-sm text-muted-foreground">Loading...</div>
            ) : (passkeysQuery.data ?? []).length === 0 ? (
              <p className="py-4 text-center text-sm text-muted-foreground">
                No passkeys registered. Create one above.
              </p>
            ) : (
              <div className="overflow-auto rounded-lg border">
                <table className="w-full">
                  <thead className="border-b bg-muted/50">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Name</th>
                      <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Created</th>
                      <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Last Used</th>
                      <th className="px-4 py-2 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(passkeysQuery.data ?? []).map((p: Passkey) => (
                      <PasskeyRow
                        key={p.id}
                        passkey={p}
                        onDelete={(id) => deletePasskeyMut.mutate(id)}
                        deleting={deletePasskeyMut.isPending}
                      />
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {passkeysQuery.isError && (
              <p className="text-sm text-red-600 dark:text-red-400">
                Failed to load passkeys: {passkeysQuery.error?.message}
              </p>
            )}
          </div>
        </div>
      )}

      {/* API Keys */}
      <div className="rounded-lg border bg-card">
        <div className="border-b px-4 py-3">
          <h2 className="text-sm font-semibold">API Keys</h2>
        </div>
        <div className="p-4 space-y-4">
          {/* New key banner */}
          {newKeyValue && (
            <div className="rounded-lg border-2 border-amber-400 bg-amber-50 p-4 dark:border-amber-600 dark:bg-amber-950/30">
              <p className="text-sm font-semibold text-amber-800 dark:text-amber-200">
                New API Key — save this now, it will not be shown again
              </p>
              <div className="mt-2 flex items-center gap-2">
                <code className="flex-1 rounded-md border border-amber-300 bg-white px-3 py-2 text-sm font-mono break-all dark:border-amber-700 dark:bg-amber-950/50">
                  {newKeyValue}
                </code>
                <button
                  type="button"
                  className="shrink-0 rounded border px-3 py-1.5 text-xs font-medium hover:bg-amber-100 dark:hover:bg-amber-900"
                  onClick={() => handleCopy(newKeyValue)}
                >
                  {copied ? "Copied" : "Copy"}
                </button>
              </div>
              <button
                type="button"
                className="mt-2 text-xs text-amber-700 hover:underline dark:text-amber-300"
                onClick={() => setNewKeyValue(null)}
              >
                Dismiss
              </button>
            </div>
          )}

          {/* Create form */}
          <CreateAPIKeyForm onCreated={handleKeyCreated} />

          {/* Key list */}
          {apiKeysQuery.isLoading ? (
            <div className="py-4 text-center text-sm text-muted-foreground">Loading...</div>
          ) : apiKeys.length === 0 ? (
            <p className="py-4 text-center text-sm text-muted-foreground">
              No API keys. Create one above.
            </p>
          ) : (
            <div className="overflow-auto rounded-lg border">
              <table className="w-full">
                <thead className="border-b bg-muted/50">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Label</th>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Prefix</th>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Created</th>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Last Used</th>
                    <th className="px-4 py-2 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {apiKeys.map((k) => (
                    <APIKeyRow
                      key={k.id}
                      apiKey={k}
                      onRevoke={handleRevoke}
                      revoking={revokeMut.isPending}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {apiKeysQuery.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to load API keys: {apiKeysQuery.error?.message}
            </p>
          )}
        </div>
      </div>

      {/* OAuth Clients */}
      <div className="rounded-lg border bg-card">
        <div className="border-b px-4 py-3">
          <h2 className="text-sm font-semibold">OAuth Clients</h2>
        </div>
        <div className="p-4 space-y-4">
          {/* New client banner */}
          {newClient && (
            <div className="rounded-lg border-2 border-amber-400 bg-amber-50 p-4 dark:border-amber-600 dark:bg-amber-950/30">
              <p className="text-sm font-semibold text-amber-800 dark:text-amber-200">
                New OAuth Client — save these credentials now, they will not be shown again
              </p>
              <div className="mt-2 space-y-2">
                <div>
                  <span className="text-xs font-medium text-amber-700 dark:text-amber-300">Client ID</span>
                  <div className="mt-1 flex items-center gap-2">
                    <code className="flex-1 rounded-md border border-amber-300 bg-white px-3 py-2 text-sm font-mono break-all dark:border-amber-700 dark:bg-amber-950/50">
                      {newClient.client_id}
                    </code>
                    <button
                      type="button"
                      className="shrink-0 rounded border px-3 py-1.5 text-xs font-medium hover:bg-amber-100 dark:hover:bg-amber-900"
                      onClick={() => handleOAuthCopy(newClient.client_id, "client_id")}
                    >
                      {oauthCopied === "client_id" ? "Copied" : "Copy"}
                    </button>
                  </div>
                </div>
                {newClient.client_secret && (
                  <div>
                    <span className="text-xs font-medium text-amber-700 dark:text-amber-300">Client Secret</span>
                    <div className="mt-1 flex items-center gap-2">
                      <code className="flex-1 rounded-md border border-amber-300 bg-white px-3 py-2 text-sm font-mono break-all dark:border-amber-700 dark:bg-amber-950/50">
                        {newClient.client_secret}
                      </code>
                      <button
                        type="button"
                        className="shrink-0 rounded border px-3 py-1.5 text-xs font-medium hover:bg-amber-100 dark:hover:bg-amber-900"
                        onClick={() => handleOAuthCopy(newClient.client_secret!, "client_secret")}
                      >
                        {oauthCopied === "client_secret" ? "Copied" : "Copy"}
                      </button>
                    </div>
                  </div>
                )}
              </div>
              <button
                type="button"
                className="mt-2 text-xs text-amber-700 hover:underline dark:text-amber-300"
                onClick={() => setNewClient(null)}
              >
                Dismiss
              </button>
            </div>
          )}

          {/* Create form */}
          <CreateOAuthClientForm onCreated={handleOAuthClientCreated} />

          {/* Client list */}
          {oauthClientsQuery.isLoading ? (
            <div className="py-4 text-center text-sm text-muted-foreground">Loading...</div>
          ) : oauthClients.length === 0 ? (
            <p className="py-4 text-center text-sm text-muted-foreground">
              No OAuth clients. Create one above.
            </p>
          ) : (
            <div className="overflow-auto rounded-lg border">
              <table className="w-full">
                <thead className="border-b bg-muted/50">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Name</th>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Client ID</th>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Type</th>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Redirect URIs</th>
                    <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Created</th>
                    <th className="px-4 py-2 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {oauthClients.map((c) => (
                    <OAuthClientRow
                      key={c.id}
                      client={c}
                      onRevoke={handleRevokeOAuthClient}
                      revoking={revokeOAuthMut.isPending}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {oauthClientsQuery.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to load OAuth clients: {oauthClientsQuery.error?.message}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

export default MyAccount;
