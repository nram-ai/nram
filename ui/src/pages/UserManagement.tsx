import { useState, useMemo, useEffect, useCallback } from "react";
import {
  useUsers,
  useUser,
  useCreateUser,
  useUpdateUser,
  useDeleteUser,
  useOrgs,
  useGenerateAPIKey,
  useRevokeAPIKey,
  useSetupStatus,
  useOrgUsers,
  useOrgUser,
  useCreateOrgUser,
  useUpdateOrgUser,
  useDeleteOrgUser,
  useGenerateOrgUserAPIKey,
  useRevokeOrgUserAPIKey,
} from "../hooks/useApi";
import { useAuth } from "../context/AuthContext";
import type {
  User,
  APIKey,
  CreateUserRequest,
  UpdateUserRequest,
  GenerateAPIKeyRequest,
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

const ROLES = [
  "administrator",
  "org_owner",
  "member",
  "viewer",
  "service_account",
] as const;

function roleBadgeClass(role: string): string {
  switch (role) {
    case "administrator":
      return "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300";
    case "org_owner":
      return "bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-300";
    case "member":
      return "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300";
    case "viewer":
      return "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300";
    case "service_account":
      return "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300";
    default:
      return "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300";
  }
}

function statusBadge(disabled: boolean): string {
  if (disabled) {
    return "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300";
  }
  return "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300";
}

// ---------------------------------------------------------------------------
// Sort helpers
// ---------------------------------------------------------------------------

type SortField = "email" | "display_name" | "role" | "last_login" | "created_at";
type SortDir = "asc" | "desc";

function compareUsers(a: User, b: User, field: SortField, dir: SortDir): number {
  let cmp = 0;
  switch (field) {
    case "email":
      cmp = a.email.localeCompare(b.email);
      break;
    case "display_name":
      cmp = (a.display_name || "").localeCompare(b.display_name || "");
      break;
    case "role":
      cmp = a.role.localeCompare(b.role);
      break;
    case "last_login":
      cmp =
        new Date(a.last_login || 0).getTime() -
        new Date(b.last_login || 0).getTime();
      break;
    case "created_at":
      cmp =
        new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
      break;
  }
  return dir === "asc" ? cmp : -cmp;
}

// ---------------------------------------------------------------------------
// SortableHeader
// ---------------------------------------------------------------------------

function SortableHeader({
  label,
  field,
  currentField,
  currentDir,
  onSort,
}: {
  label: string;
  field: SortField;
  currentField: SortField;
  currentDir: SortDir;
  onSort: (field: SortField) => void;
}) {
  const active = currentField === field;
  return (
    <th
      className="cursor-pointer select-none px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground hover:text-foreground"
      onClick={() => onSort(field)}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {active && (
          <span className="text-foreground">
            {currentDir === "asc" ? "\u2191" : "\u2193"}
          </span>
        )}
      </span>
    </th>
  );
}

// ---------------------------------------------------------------------------
// SkeletonRow
// ---------------------------------------------------------------------------

function SkeletonRow() {
  return (
    <tr className="animate-pulse">
      <td className="px-4 py-3"><div className="h-4 w-40 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-28 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-20 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-24 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-28 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-16 rounded bg-muted" /></td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Create User Dialog
// ---------------------------------------------------------------------------

function CreateUserDialog({
  orgs,
  onClose,
}: {
  orgs: Organization[];
  onClose: () => void;
}) {
  const createMut = useCreateUser();
  const [email, setEmail] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<string>("member");
  const [orgId, setOrgId] = useState<string>(orgs.length > 0 ? orgs[0].id : "");

  function handleCreate() {
    if (!email.trim() || !password.trim() || password.length < 8) return;
    const data: CreateUserRequest = {
      email: email.trim(),
      password: password,
      role,
    };
    if (displayName.trim()) {
      data.display_name = displayName.trim();
    }
    if (orgId) {
      data.organization_id = orgId;
    }
    createMut.mutate(data, { onSuccess: () => onClose() });
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 w-full max-w-md rounded-lg border bg-background p-6 shadow-xl">
        <h2 className="text-lg font-semibold">Create User</h2>
        <div className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Email <span className="text-red-500">*</span>
            </label>
            <input
              type="email"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="user@example.com"
              autoFocus
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Display Name
            </label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder="Display name (optional)"
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Password <span className="text-red-500">*</span>
            </label>
            <input
              type="password"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Minimum 8 characters"
            />
            {password.length > 0 && password.length < 8 && (
              <p className="mt-1 text-xs text-red-500">
                Password must be at least 8 characters.
              </p>
            )}
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Role <span className="text-red-500">*</span>
            </label>
            <select
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={role}
              onChange={(e) => setRole(e.target.value)}
            >
              {ROLES.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Organization
            </label>
            <select
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={orgId}
              onChange={(e) => setOrgId(e.target.value)}
            >
              <option value="">None</option>
              {orgs.map((o) => (
                <option key={o.id} value={o.id}>
                  {o.name}
                </option>
              ))}
            </select>
          </div>

          {createMut.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to create: {createMut.error?.message}
            </p>
          )}

          <div className="flex items-center justify-end gap-3 pt-2">
            <button
              type="button"
              className="rounded border px-3 py-2 text-sm hover:bg-muted"
              onClick={onClose}
            >
              Cancel
            </button>
            <button
              type="button"
              className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              onClick={handleCreate}
              disabled={
                !email.trim() ||
                !password.trim() ||
                password.length < 8 ||
                createMut.isPending
              }
            >
              {createMut.isPending ? "Creating..." : "Create"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Generate API Key Dialog
// ---------------------------------------------------------------------------

function GenerateAPIKeyDialog({
  userId,
  orgId,
  isAdmin,
  onClose,
}: {
  userId: string;
  orgId?: string;
  isAdmin: boolean;
  onClose: () => void;
}) {
  const adminGenerateMut = useGenerateAPIKey();
  const orgGenerateMut = useGenerateOrgUserAPIKey();
  const generateMut = isAdmin ? adminGenerateMut : orgGenerateMut;
  const [label, setLabel] = useState("");
  const [scopes, setScopes] = useState("");
  const [expiresAt, setExpiresAt] = useState("");
  const [generatedKey, setGeneratedKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  function handleGenerate() {
    if (!label.trim()) return;
    const data: GenerateAPIKeyRequest = { label: label.trim() };
    if (scopes.trim()) {
      data.scopes = scopes
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    }
    if (expiresAt) {
      data.expires_at = new Date(expiresAt).toISOString();
    }
    if (isAdmin) {
      adminGenerateMut.mutate(
        { userId, data },
        {
          onSuccess: (resp) => {
            setGeneratedKey(resp.key);
          },
        },
      );
    } else {
      orgGenerateMut.mutate(
        { orgId: orgId!, userId, data },
        {
          onSuccess: (resp) => {
            setGeneratedKey(resp.key);
          },
        },
      );
    }
  }

  function handleCopy() {
    if (!generatedKey) return;
    navigator.clipboard.writeText(generatedKey).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }

  if (generatedKey) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center">
        <div className="absolute inset-0 bg-black/30" onClick={onClose} />
        <div className="relative z-10 w-full max-w-lg rounded-lg border bg-background p-6 shadow-xl">
          <h2 className="text-lg font-semibold">API Key Generated</h2>
          <div className="mt-4 space-y-4">
            <div className="rounded-lg border-2 border-amber-400 bg-amber-50 p-4 dark:border-amber-600 dark:bg-amber-950">
              <p className="mb-2 text-sm font-semibold text-amber-800 dark:text-amber-300">
                Copy this key now. It will not be shown again.
              </p>
              <div className="flex items-center gap-2">
                <code className="flex-1 break-all rounded bg-white px-3 py-2 font-mono text-sm dark:bg-black">
                  {generatedKey}
                </code>
                <button
                  type="button"
                  className="shrink-0 rounded bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
                  onClick={handleCopy}
                >
                  {copied ? "Copied" : "Copy"}
                </button>
              </div>
            </div>
            <div className="flex justify-end">
              <button
                type="button"
                className="rounded border px-3 py-2 text-sm hover:bg-muted"
                onClick={onClose}
              >
                Done
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 w-full max-w-md rounded-lg border bg-background p-6 shadow-xl">
        <h2 className="text-lg font-semibold">Generate API Key</h2>
        <div className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Label <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="e.g. CI Pipeline Key"
              autoFocus
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Scopes
            </label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={scopes}
              onChange={(e) => setScopes(e.target.value)}
              placeholder="org/user/project (comma-separated)"
            />
            <p className="mt-1 text-xs text-muted-foreground">
              Comma-separated namespace paths. Leave empty for full access.
            </p>
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Expires At
            </label>
            <input
              type="datetime-local"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={expiresAt}
              onChange={(e) => setExpiresAt(e.target.value)}
            />
            <p className="mt-1 text-xs text-muted-foreground">
              Optional. Leave empty for no expiry.
            </p>
          </div>

          {generateMut.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to generate: {generateMut.error?.message}
            </p>
          )}

          <div className="flex items-center justify-end gap-3 pt-2">
            <button
              type="button"
              className="rounded border px-3 py-2 text-sm hover:bg-muted"
              onClick={onClose}
            >
              Cancel
            </button>
            <button
              type="button"
              className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              onClick={handleGenerate}
              disabled={!label.trim() || generateMut.isPending}
            >
              {generateMut.isPending ? "Generating..." : "Generate"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// API Key Table
// ---------------------------------------------------------------------------

function APIKeyTable({
  userId,
  keys,
  orgId,
  isAdmin,
}: {
  userId: string;
  keys: APIKey[];
  orgId?: string;
  isAdmin: boolean;
}) {
  const adminRevokeMut = useRevokeAPIKey();
  const orgRevokeMut = useRevokeOrgUserAPIKey();
  const revokeMut = isAdmin ? adminRevokeMut : orgRevokeMut;
  const [confirmRevokeId, setConfirmRevokeId] = useState<string | null>(null);
  const [showGenerate, setShowGenerate] = useState(false);

  function handleRevoke(keyId: string) {
    if (isAdmin) {
      adminRevokeMut.mutate(
        { userId, keyId },
        { onSuccess: () => setConfirmRevokeId(null) },
      );
    } else {
      orgRevokeMut.mutate(
        { orgId: orgId!, userId, keyId },
        { onSuccess: () => setConfirmRevokeId(null) },
      );
    }
  }

  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold">API Keys ({keys.length})</h3>
        <button
          type="button"
          className="rounded bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground hover:bg-primary/90"
          onClick={() => setShowGenerate(true)}
        >
          Generate Key
        </button>
      </div>

      {keys.length === 0 ? (
        <p className="text-sm text-muted-foreground">No API keys.</p>
      ) : (
        <div className="overflow-auto rounded-lg border">
          <table className="w-full">
            <thead className="border-b bg-muted/50">
              <tr>
                <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Label
                </th>
                <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Prefix
                </th>
                <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Scopes
                </th>
                <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Expires
                </th>
                <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Last Used
                </th>
                <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Created
                </th>
                <th className="px-3 py-2 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {keys.map((key) => (
                <tr key={key.id} className="transition-colors hover:bg-accent/50">
                  <td className="px-3 py-2 text-sm font-medium">{key.name}</td>
                  <td className="px-3 py-2 font-mono text-xs text-muted-foreground">
                    {key.key_prefix}
                  </td>
                  <td className="px-3 py-2 text-xs">
                    {key.scopes && key.scopes.length > 0 ? (
                      <div className="flex flex-wrap gap-1">
                        {key.scopes.map((scope, i) => (
                          <span
                            key={i}
                            className="inline-block rounded bg-muted px-1.5 py-0.5 font-mono text-xs"
                          >
                            {scope}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className="text-muted-foreground">All</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground">
                    {formatDate(key.expires_at)}
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground">
                    {formatDate(key.last_used)}
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground">
                    {formatDate(key.created_at)}
                  </td>
                  <td className="px-3 py-2 text-right">
                    {confirmRevokeId === key.id ? (
                      <span className="inline-flex items-center gap-1">
                        <button
                          type="button"
                          className="rounded bg-red-600 px-2 py-1 text-xs text-white hover:bg-red-700 disabled:opacity-50"
                          onClick={() => handleRevoke(key.id)}
                          disabled={revokeMut.isPending}
                        >
                          {revokeMut.isPending ? "..." : "Confirm"}
                        </button>
                        <button
                          type="button"
                          className="rounded border px-2 py-1 text-xs hover:bg-muted"
                          onClick={() => setConfirmRevokeId(null)}
                        >
                          Cancel
                        </button>
                      </span>
                    ) : (
                      <button
                        type="button"
                        className="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
                        onClick={() => setConfirmRevokeId(key.id)}
                      >
                        Revoke
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {showGenerate && (
        <GenerateAPIKeyDialog
          userId={userId}
          orgId={orgId}
          isAdmin={isAdmin}
          onClose={() => setShowGenerate(false)}
        />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// User Detail Panel
// ---------------------------------------------------------------------------

function UserDetailPanel({
  userId,
  userList,
  onClose,
  onDeleted,
  orgId,
  isAdmin,
}: {
  userId: string;
  userList: User[];
  onClose: () => void;
  onDeleted: () => void;
  orgId?: string;
  isAdmin: boolean;
}) {
  const adminDetailQuery = useUser(isAdmin ? userId : "");
  const orgDetailQuery = useOrgUser(isAdmin ? "" : (orgId ?? ""), isAdmin ? "" : userId);
  const detailQuery = isAdmin ? adminDetailQuery : orgDetailQuery;
  const setupQuery = useSetupStatus();
  const adminUpdateMut = useUpdateUser();
  const orgUpdateMut = useUpdateOrgUser();
  const adminDeleteMut = useDeleteUser();
  const orgDeleteMut = useDeleteOrgUser();
  const updateMut = isAdmin ? adminUpdateMut : orgUpdateMut;
  const deleteMut = isAdmin ? adminDeleteMut : orgDeleteMut;
  const isPostgres = setupQuery.data?.backend === "postgres";
  const availableRoles = isAdmin ? ROLES : ORG_OWNER_ROLES;

  const [editDisplayName, setEditDisplayName] = useState("");
  const [editRole, setEditRole] = useState("");
  const [editDisabled, setEditDisabled] = useState(false);
  const [editEnrichmentEnabled, setEditEnrichmentEnabled] = useState(true);
  const [editRecency, setEditRecency] = useState("0.3");
  const [editRelevance, setEditRelevance] = useState("0.5");
  const [editImportance, setEditImportance] = useState("0.2");
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [initialized, setInitialized] = useState(false);

  const user = detailQuery.data;

  useEffect(() => {
    if (user && !initialized) {
      setEditDisplayName(user.display_name || "");
      setEditRole(user.role || "member");
      setEditDisabled(user.disabled_at != null);
      const settings = user.settings || {};
      setEditEnrichmentEnabled(
        settings.enrichment_enabled !== undefined
          ? Boolean(settings.enrichment_enabled)
          : true,
      );
      const weights = (settings.ranking_weights as Record<string, number>) || {};
      setEditRecency(String(weights.recency ?? 0.3));
      setEditRelevance(String(weights.relevance ?? 0.5));
      setEditImportance(String(weights.importance ?? 0.2));
      setInitialized(true);
    }
  }, [user, initialized]);

  useEffect(() => {
    setInitialized(false);
    setConfirmDelete(false);
    setSaveSuccess(false);
  }, [userId]);

  const isLastAdmin = useMemo(() => {
    const adminCount = userList.filter(
      (u) => u.role === "administrator" && u.disabled_at == null,
    ).length;
    return user?.role === "administrator" && adminCount <= 1;
  }, [userList, user]);

  const handleSave = useCallback(() => {
    if (!user) return;
    const data: UpdateUserRequest = {
      display_name: editDisplayName,
      role: editRole,
      settings: {
        enrichment_enabled: editEnrichmentEnabled,
        ranking_weights: {
          recency: parseFloat(editRecency) || 0.3,
          relevance: parseFloat(editRelevance) || 0.5,
          importance: parseFloat(editImportance) || 0.2,
        },
      },
    };
    const onSuccess = () => {
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 2000);
    };
    if (isAdmin) {
      adminUpdateMut.mutate({ id: user.id, data }, { onSuccess });
    } else {
      orgUpdateMut.mutate({ orgId: orgId!, userId: user.id, data }, { onSuccess });
    }
  }, [
    user,
    editDisplayName,
    editRole,
    editDisabled,
    editEnrichmentEnabled,
    editRecency,
    editRelevance,
    editImportance,
    isAdmin,
    adminUpdateMut,
    orgUpdateMut,
    orgId,
  ]);

  function handleDelete() {
    if (!user) return;
    const onSuccess = () => {
      onDeleted();
      onClose();
    };
    if (isAdmin) {
      adminDeleteMut.mutate(user.id, { onSuccess });
    } else {
      orgDeleteMut.mutate({ orgId: orgId!, userId: user.id }, { onSuccess });
    }
  }

  const keys: APIKey[] = user?.api_keys ?? [];

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 flex h-full w-full max-w-2xl flex-col overflow-y-auto border-l bg-background shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b px-6 py-4">
          <h2 className="text-lg font-semibold">User Detail</h2>
          <button
            type="button"
            className="rounded border px-3 py-1 text-sm hover:bg-muted"
            onClick={onClose}
          >
            Close
          </button>
        </div>

        {detailQuery.isLoading && (
          <div className="p-6">
            <div className="animate-pulse space-y-3">
              <div className="h-4 w-3/4 rounded bg-muted" />
              <div className="h-4 w-1/2 rounded bg-muted" />
              <div className="h-20 rounded bg-muted" />
            </div>
          </div>
        )}

        {detailQuery.isError && (
          <div className="p-6">
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to load user: {detailQuery.error?.message}
            </p>
          </div>
        )}

        {user && initialized && (
          <div className="flex-1 space-y-6 p-6">
            {/* Email (read-only) */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Email
              </label>
              <div className="rounded-md border bg-muted/30 px-3 py-2 text-sm text-muted-foreground">
                {user.email}
              </div>
            </div>

            {/* Display Name (editable) */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Display Name
              </label>
              <input
                type="text"
                className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                value={editDisplayName}
                onChange={(e) => setEditDisplayName(e.target.value)}
              />
            </div>

            {/* Role (editable) */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Role
              </label>
              <select
                className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                value={editRole}
                onChange={(e) => setEditRole(e.target.value)}
              >
                {availableRoles.map((r) => (
                  <option key={r} value={r}>
                    {r}
                  </option>
                ))}
              </select>
            </div>

            {/* Organization (read-only) */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Organization
              </label>
              <div className="rounded-md border bg-muted/30 px-3 py-2 text-sm text-muted-foreground">
                {user.organization?.name || "-"}
              </div>
            </div>

            {/* Status toggle */}
            <div className="flex items-center gap-3">
              <label className="text-sm font-medium text-muted-foreground">
                Status
              </label>
              <button
                type="button"
                className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors focus:outline-none focus:ring-2 focus:ring-ring ${
                  !editDisabled ? "bg-green-500" : "bg-gray-300 dark:bg-gray-600"
                }`}
                onClick={() => setEditDisabled(!editDisabled)}
              >
                <span
                  className={`pointer-events-none inline-block h-5 w-5 rounded-full bg-white shadow-lg transition-transform ${
                    !editDisabled ? "translate-x-5" : "translate-x-0"
                  }`}
                />
              </button>
              <span className="text-sm">
                {editDisabled ? "Disabled" : "Active"}
              </span>
            </div>

            {/* Last login */}
            <div className="text-sm">
              <span className="text-muted-foreground">Last login: </span>
              <span>{formatDate(user.last_login)}</span>
            </div>

            {/* Per-user settings */}
            <div className="space-y-4 rounded-lg border p-4">
              <h3 className="text-sm font-semibold">User Settings</h3>

              {isPostgres && (
              <div className="flex items-center gap-3">
                <label className="text-sm text-muted-foreground">
                  Enrichment enabled
                </label>
                <button
                  type="button"
                  className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors focus:outline-none focus:ring-2 focus:ring-ring ${
                    editEnrichmentEnabled
                      ? "bg-green-500"
                      : "bg-gray-300 dark:bg-gray-600"
                  }`}
                  onClick={() =>
                    setEditEnrichmentEnabled(!editEnrichmentEnabled)
                  }
                >
                  <span
                    className={`pointer-events-none inline-block h-5 w-5 rounded-full bg-white shadow-lg transition-transform ${
                      editEnrichmentEnabled
                        ? "translate-x-5"
                        : "translate-x-0"
                    }`}
                  />
                </button>
              </div>
              )}

              <div>
                <label className="mb-2 block text-sm text-muted-foreground">
                  Ranking Weight Preferences
                </label>
                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className="mb-1 block text-xs text-muted-foreground">
                      Recency
                    </label>
                    <input
                      type="number"
                      step="0.1"
                      min="0"
                      max="1"
                      className="w-full rounded-md border bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                      value={editRecency}
                      onChange={(e) => setEditRecency(e.target.value)}
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs text-muted-foreground">
                      Relevance
                    </label>
                    <input
                      type="number"
                      step="0.1"
                      min="0"
                      max="1"
                      className="w-full rounded-md border bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                      value={editRelevance}
                      onChange={(e) => setEditRelevance(e.target.value)}
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs text-muted-foreground">
                      Importance
                    </label>
                    <input
                      type="number"
                      step="0.1"
                      min="0"
                      max="1"
                      className="w-full rounded-md border bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                      value={editImportance}
                      onChange={(e) => setEditImportance(e.target.value)}
                    />
                  </div>
                </div>
              </div>
            </div>

            {/* API Keys */}
            <APIKeyTable userId={user.id} keys={keys} orgId={orgId} isAdmin={isAdmin} />

            {/* Actions */}
            <div className="flex items-center gap-3 border-t pt-4">
              <button
                type="button"
                className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
                onClick={handleSave}
                disabled={updateMut.isPending}
              >
                {updateMut.isPending
                  ? "Saving..."
                  : saveSuccess
                    ? "Saved"
                    : "Save Changes"}
              </button>

              {updateMut.isError && (
                <span className="text-sm text-red-600 dark:text-red-400">
                  Failed to save: {updateMut.error?.message}
                </span>
              )}

              <div className="flex-1" />

              {confirmDelete ? (
                <>
                  <span className="text-sm text-red-600 dark:text-red-400">
                    {isLastAdmin
                      ? "Cannot delete the last administrator."
                      : "Delete this user? This cannot be undone."}
                  </span>
                  {!isLastAdmin && (
                    <button
                      type="button"
                      className="rounded bg-red-600 px-3 py-1.5 text-sm text-white hover:bg-red-700 disabled:opacity-50"
                      onClick={handleDelete}
                      disabled={deleteMut.isPending}
                    >
                      {deleteMut.isPending ? "Deleting..." : "Yes, Delete"}
                    </button>
                  )}
                  <button
                    type="button"
                    className="rounded border px-3 py-1.5 text-sm hover:bg-muted"
                    onClick={() => setConfirmDelete(false)}
                  >
                    Cancel
                  </button>
                </>
              ) : (
                <button
                  type="button"
                  className="rounded border border-red-300 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
                  onClick={() => setConfirmDelete(true)}
                >
                  Delete User
                </button>
              )}
            </div>

            {deleteMut.isError && (
              <p className="text-sm text-red-600 dark:text-red-400">
                Failed to delete: {deleteMut.error?.message}
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main UserManagement
// ---------------------------------------------------------------------------

const ORG_OWNER_ROLES = ["org_owner", "member", "viewer", "service_account"] as const;

function CreateOrgUserDialog({
  orgId,
  onClose,
}: {
  orgId: string;
  onClose: () => void;
}) {
  const createMut = useCreateOrgUser();
  const [email, setEmail] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<string>("member");

  function handleCreate() {
    if (!email.trim() || !password.trim() || password.length < 8) return;
    createMut.mutate(
      {
        orgId,
        data: {
          email: email.trim(),
          password,
          role,
          display_name: displayName.trim() || undefined,
        },
      },
      { onSuccess: () => onClose() },
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 w-full max-w-md rounded-lg border bg-background p-6 shadow-xl">
        <h2 className="text-lg font-semibold">Create User</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          New user will be added to your organization.
        </p>
        <div className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Email <span className="text-red-500">*</span>
            </label>
            <input
              type="email"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="user@example.com"
              autoFocus
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Display Name
            </label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder="Display name (optional)"
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Password <span className="text-red-500">*</span>
            </label>
            <input
              type="password"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Minimum 8 characters"
            />
            {password.length > 0 && password.length < 8 && (
              <p className="mt-1 text-xs text-red-500">
                Password must be at least 8 characters.
              </p>
            )}
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Role <span className="text-red-500">*</span>
            </label>
            <select
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={role}
              onChange={(e) => setRole(e.target.value)}
            >
              {ORG_OWNER_ROLES.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </div>

          {createMut.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to create: {createMut.error?.message}
            </p>
          )}

          <div className="flex items-center justify-end gap-3 pt-2">
            <button
              type="button"
              className="rounded border px-3 py-2 text-sm hover:bg-muted"
              onClick={onClose}
            >
              Cancel
            </button>
            <button
              type="button"
              className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              onClick={handleCreate}
              disabled={!email.trim() || !password.trim() || password.length < 8 || createMut.isPending}
            >
              {createMut.isPending ? "Creating..." : "Create"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function UserManagement() {
  const auth = useAuth();
  const orgId = auth.user?.org_id ?? "";

  // Admin: use admin endpoints for all users (disabled for non-admin to avoid 403)
  const adminUsersQuery = useUsers(auth.isAdmin);
  const adminOrgsQuery = useOrgs(auth.isAdmin);

  // Org owner: use org-scoped endpoints
  const orgUsersQuery = useOrgUsers(auth.isAdmin ? "" : orgId);

  const usersQuery = auth.isAdmin ? adminUsersQuery : orgUsersQuery;
  const users = usersQuery.data ?? [];
  const orgs = auth.isAdmin ? (adminOrgsQuery.data ?? []) : [];

  const [sortField, setSortField] = useState<SortField>("email");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [search, setSearch] = useState("");
  const [detailUserId, setDetailUserId] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);

  function handleSort(field: SortField) {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  }

  const filteredUsers = useMemo(() => {
    let result = [...users];
    if (search.trim()) {
      const q = search.toLowerCase();
      result = result.filter(
        (u) =>
          u.email.toLowerCase().includes(q) ||
          (u.display_name || "").toLowerCase().includes(q) ||
          u.role.toLowerCase().includes(q),
      );
    }
    result.sort((a, b) => compareUsers(a, b, sortField, sortDir));
    return result;
  }, [users, search, sortField, sortDir]);

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="space-y-4 pb-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            User Management
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {auth.isAdmin
              ? "Manage users, roles, and API keys."
              : "Manage users within your organization."}
          </p>
        </div>
      </div>

      {/* Loading state */}
      {usersQuery.isLoading && (
        <div className="py-8 text-center text-sm text-muted-foreground">
          Loading...
        </div>
      )}

      {/* Main content */}
      {!usersQuery.isLoading && (
        <>
          {/* Toolbar */}
          <div className="flex flex-col gap-3 pb-4 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-4">
              <input
                type="text"
                className="w-full sm:w-64 rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search users..."
              />
              <span className="text-sm text-muted-foreground">
                {filteredUsers.length} user{filteredUsers.length !== 1 ? "s" : ""}
              </span>
            </div>
            <button
              type="button"
              className="w-full sm:w-auto rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              onClick={() => setShowCreate(true)}
            >
              Create User
            </button>
          </div>

          {/* Table */}
          <div className="flex-1 overflow-auto rounded-lg border">
            {usersQuery.isError && (
              <div className="m-4 rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300">
                Failed to load users:{" "}
                {usersQuery.error?.message ?? "Unknown error"}
              </div>
            )}

            <table className="w-full">
              <thead className="sticky top-0 border-b bg-muted/50">
                <tr>
                  <SortableHeader
                    label="Email"
                    field="email"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <SortableHeader
                    label="Display Name"
                    field="display_name"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <SortableHeader
                    label="Role"
                    field="role"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                    Organization
                  </th>
                  <SortableHeader
                    label="Last Login"
                    field="last_login"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {usersQuery.isLoading ? (
                  <>
                    <SkeletonRow />
                    <SkeletonRow />
                    <SkeletonRow />
                  </>
                ) : filteredUsers.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-4 py-12 text-center">
                      <p className="text-sm text-muted-foreground">
                        {search
                          ? "No users match your search."
                          : "No users yet. Create one to get started."}
                      </p>
                    </td>
                  </tr>
                ) : (
                  filteredUsers.map((user) => (
                    <tr
                      key={user.id}
                      className="cursor-pointer transition-colors hover:bg-accent/50"
                      onClick={() => setDetailUserId(user.id)}
                    >
                      <td className="px-4 py-3 text-sm font-medium">
                        {user.email}
                      </td>
                      <td className="px-4 py-3 text-sm text-muted-foreground">
                        {user.display_name || "-"}
                      </td>
                      <td className="px-4 py-3">
                        <span
                          className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${roleBadgeClass(user.role)}`}
                        >
                          {user.role}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-sm text-muted-foreground">
                        {user.organization?.name || "-"}
                      </td>
                      <td className="px-4 py-3 text-xs text-muted-foreground">
                        {formatDate(user.last_login)}
                      </td>
                      <td className="px-4 py-3">
                        <span
                          className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${statusBadge(user.disabled_at != null)}`}
                        >
                          {user.disabled_at != null ? "Disabled" : "Active"}
                        </span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          {/* Create Dialog */}
          {showCreate && auth.isAdmin && (
            <CreateUserDialog
              orgs={orgs}
              onClose={() => setShowCreate(false)}
            />
          )}
          {showCreate && !auth.isAdmin && (
            <CreateOrgUserDialog
              orgId={orgId}
              onClose={() => setShowCreate(false)}
            />
          )}

          {/* Detail Panel */}
          {detailUserId && (
            <UserDetailPanel
              userId={detailUserId}
              userList={users}
              onClose={() => setDetailUserId(null)}
              onDeleted={() => setDetailUserId(null)}
              orgId={orgId}
              isAdmin={auth.isAdmin}
            />
          )}
        </>
      )}
    </div>
  );
}

export default UserManagement;
