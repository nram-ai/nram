import { useState, useCallback } from "react";
import { useAuth } from "../context/AuthContext";
import {
  useMeAPIKeys,
  useCreateMeAPIKey,
  useRevokeMeAPIKey,
} from "../hooks/useApi";
import type { APIKey } from "../api/client";

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

  const apiKeys: APIKey[] = apiKeysQuery.data ?? [];

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
          Manage your profile, API keys, and session.
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
    </div>
  );
}

export default MyAccount;
