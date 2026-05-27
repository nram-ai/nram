import { useEffect, useState, useCallback } from "react";
import { useAuth } from "../context/AuthContext";
import {
  useMeProfile,
  useMeAPIKeys,
  useCreateMeAPIKey,
  useRevokeMeAPIKey,
  useChangePassword,
  useMePasskeys,
  useRegisterPasskey,
  useDeletePasskey,
  useMeExportJobs,
  useCreateMeExportJob,
  useDeleteMeExportJob,
} from "../hooks/useApi";
import type { APIKey, ExportJob, ExportJobStatus, Passkey } from "../api/client";
import { downloadExportJobArtifact } from "../api/client";
import { formatBytes } from "../lib/formatters";
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
      return "bg-warning/10 text-warning";
    case "member":
      return "bg-info/10 text-info";
    case "readonly":
      return "bg-muted text-muted-foreground";
    case "service":
      return "bg-success/10 text-success";
    default:
      return "bg-muted text-muted-foreground";
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
              className="rounded bg-destructive px-2 py-1 text-xs text-white hover:bg-destructive disabled:opacity-50"
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
            className="rounded border border-destructive/40 px-2 py-1 text-xs text-destructive hover:bg-destructive/10"
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
            <p className="mt-1 text-xs text-destructive">
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
            <p className="mt-1 text-xs text-destructive">
              Passwords do not match.
            </p>
          )}
        </div>

        {feedback && (
          <div
            className={`rounded-md px-3 py-2 text-sm ${ feedback.type === "success" ? "bg-success/10 text-success" : "bg-destructive/10 text-destructive" }`}
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
              className="rounded bg-destructive px-2 py-1 text-xs text-white hover:bg-destructive disabled:opacity-50"
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
            className="rounded border border-destructive/40 px-2 py-1 text-xs text-destructive hover:bg-destructive/10"
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
        <p className="text-sm text-destructive">{error}</p>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Data Export Card
//
// Renders the asynchronous export-job pipeline at /v1/me/exports — kicks off
// an account-wide zip, lists the caller's recent jobs, polls in-flight rows
// at 3s via useMeExportJobs's refetchInterval, downloads succeeded
// artifacts through an auth-attached fetch+blob path.
// ---------------------------------------------------------------------------

function exportStatusBadgeClass(status: ExportJobStatus): string {
  switch (status) {
    case "pending":
    case "processing":
      return "bg-info/10 text-info";
    case "succeeded":
      return "bg-success/10 text-success";
    case "failed":
      return "bg-destructive/10 text-destructive";
    case "expired":
      return "bg-muted text-muted-foreground";
    default:
      return "bg-muted text-muted-foreground";
  }
}

function ExportJobRow({ job, onDelete, deleting }: { job: ExportJob; onDelete: (id: string) => void; deleting: boolean }) {
  const [downloading, setDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  async function handleDownload() {
    setDownloadError(null);
    setDownloading(true);
    try {
      await downloadExportJobArtifact(job.id);
    } catch (err) {
      setDownloadError(err instanceof Error ? err.message : "download failed");
    } finally {
      setDownloading(false);
    }
  }

  const canDownload = job.status === "succeeded";

  return (
    <tr className="border-b last:border-0">
      <td className="px-4 py-3 text-sm">{job.scope}</td>
      <td className="px-4 py-3 text-xs">
        <span className={`inline-block rounded-full px-2 py-0.5 font-medium ${exportStatusBadgeClass(job.status)}`}>
          {job.status}
        </span>
        {job.error && (
          <p className="mt-1 text-xs text-destructive" title={job.error}>
            {job.error.length > 80 ? `${job.error.slice(0, 80)}…` : job.error}
          </p>
        )}
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground">{formatDate(job.created_at)}</td>
      <td className="px-4 py-3 text-xs text-muted-foreground">{formatBytes(job.artifact_bytes)}</td>
      <td className="px-4 py-3 text-xs text-muted-foreground">
        {job.expires_at ? formatDate(job.expires_at) : "-"}
      </td>
      <td className="px-4 py-3 text-right">
        <span className="inline-flex items-center gap-2">
          {downloadError && <span className="text-xs text-destructive">{downloadError}</span>}
          {canDownload && (
            <button
              type="button"
              className="rounded border px-2 py-1 text-xs hover:bg-muted disabled:opacity-50"
              onClick={handleDownload}
              disabled={downloading}
            >
              {downloading ? "Downloading..." : "Download"}
            </button>
          )}
          <button
            type="button"
            className="rounded border border-destructive/40 px-2 py-1 text-xs text-destructive hover:bg-destructive/10"
            onClick={() => onDelete(job.id)}
            disabled={deleting}
          >
            {deleting ? "Deleting..." : "Delete"}
          </button>
        </span>
      </td>
    </tr>
  );
}

function DataExportCard() {
  const jobsQuery = useMeExportJobs();
  const createMut = useCreateMeExportJob();
  const deleteMut = useDeleteMeExportJob();
  const [includeSuperseded, setIncludeSuperseded] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const jobs: ExportJob[] = jobsQuery.data ?? [];
  const hasInflight = jobs.some((j) => j.status === "pending" || j.status === "processing");

  function handleStart() {
    setError(null);
    createMut.mutate(
      { scope: "account", include_superseded: includeSuperseded },
      {
        onError: (err) => {
          let detail = err instanceof Error ? err.message : "failed to start export";
          if ("body" in err && typeof (err as Record<string, unknown>).body === "object" && (err as Record<string, unknown>).body !== null) {
            const body = (err as Record<string, unknown>).body as Record<string, unknown>;
            if (typeof body.message === "string") detail = body.message;
          }
          setError(detail);
        },
      },
    );
  }

  return (
    <div className="rounded-lg border bg-card">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Data Export</h2>
      </div>
      <div className="p-4 space-y-4">
        <p className="text-xs text-muted-foreground">
          Export your data as a zip archive. The archive contains one JSON file per project
          you own (memories, entities, relationships, and lineage) plus a manifest. Exports
          run asynchronously; large accounts may take a minute or two.
        </p>

        <div className="flex flex-wrap items-end gap-3">
          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={includeSuperseded}
              onChange={(e) => setIncludeSuperseded(e.target.checked)}
            />
            Include superseded memories
          </label>
          <button
            type="button"
            className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
            onClick={handleStart}
            disabled={createMut.isPending || hasInflight}
            title={hasInflight ? "An export is already in flight — wait for it to finish before starting another." : undefined}
          >
            {createMut.isPending ? "Starting..." : "Start account export"}
          </button>
        </div>

        {error && (
          <p className="text-sm text-destructive">{error}</p>
        )}

        {jobsQuery.isLoading ? (
          <div className="py-4 text-center text-sm text-muted-foreground">Loading...</div>
        ) : jobs.length === 0 ? (
          <p className="py-4 text-center text-sm text-muted-foreground">
            No exports yet. Click "Start account export" to create one.
          </p>
        ) : (
          <div className="overflow-auto rounded-lg border">
            <table className="w-full">
              <thead className="border-b bg-muted/50">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Scope</th>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Status</th>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Created</th>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Size</th>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">Expires</th>
                  <th className="px-4 py-2 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">Actions</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <ExportJobRow
                    key={job.id}
                    job={job}
                    onDelete={(id) => deleteMut.mutate(id)}
                    deleting={deleteMut.isPending}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}

        {jobsQuery.isError && (
          <p className="text-sm text-destructive">
            Failed to load exports: {jobsQuery.error?.message}
          </p>
        )}
      </div>
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

  // Hydrate profile from the server. The JWT-derived AuthContext.user is
  // a fast initial render; once the /v1/me/profile fetch returns we push
  // the fresh values into AuthContext so the sidebar / role-gating reads
  // them too — not just this page.
  const profileQuery = useMeProfile();
  useEffect(() => {
    if (profileQuery.data) {
      auth.setUser(profileQuery.data);
    }
  }, [profileQuery.data, auth]);
  const user = profileQuery.data ?? auth.user;
  const token = localStorage.getItem("nram_token");
  const expiry = token ? decodeJWTExpiry(token) : null;

  const passkeysQuery = useMePasskeys();
  const deletePasskeyMut = useDeletePasskey();

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
        <h1 className="font-display text-3xl text-foreground">My Account</h1>
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
            className="rounded border border-destructive/40 px-3 py-1.5 text-sm text-destructive hover:bg-destructive/10"
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
              <p className="text-sm text-destructive">
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
            <div className="rounded-lg border-2 border-warning/40 bg-warning/10 p-4">
              <p className="text-sm font-semibold text-warning">
                New API Key — save this now, it will not be shown again
              </p>
              <div className="mt-2 flex items-center gap-2">
                <code className="flex-1 rounded-md border border-warning/40 bg-white px-3 py-2 text-sm font-mono break-all dark:bg-warning/15">
                  {newKeyValue}
                </code>
                <button
                  type="button"
                  className="shrink-0 rounded border px-3 py-1.5 text-xs font-medium hover:bg-warning/25"
                  onClick={() => handleCopy(newKeyValue)}
                >
                  {copied ? "Copied" : "Copy"}
                </button>
              </div>
              <button
                type="button"
                className="mt-2 text-xs text-warning hover:underline"
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
            <p className="text-sm text-destructive">
              Failed to load API keys: {apiKeysQuery.error?.message}
            </p>
          )}
        </div>
      </div>

      {/* Data Export — async job pipeline at /v1/me/exports. Self-service
          only; admins do NOT get an equivalent surface elsewhere in the
          UI (per the codebase's privacy invariant — admins cannot read
          other users' memory content). */}
      <DataExportCard />

    </div>
  );
}

export default MyAccount;
