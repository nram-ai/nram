import { useState, useMemo, useEffect } from "react";
import {
  useOrgs,
  useOrg,
  useCreateOrg,
  useUpdateOrg,
  useDeleteOrg,
} from "../hooks/useApi";
import type { Organization, OrgUser, UpdateOrgRequest } from "../api/client";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function roleBadgeClass(role: string): string {
  switch (role) {
    case "admin":
      return "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300";
    case "member":
      return "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300";
    case "viewer":
      return "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300";
    default:
      return "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300";
  }
}

// ---------------------------------------------------------------------------
// Sort helpers
// ---------------------------------------------------------------------------

type SortField = "name" | "slug" | "user_count" | "memory_count" | "created_at";
type SortDir = "asc" | "desc";

function compareOrgs(a: Organization, b: Organization, field: SortField, dir: SortDir): number {
  let cmp = 0;
  switch (field) {
    case "name":
      cmp = a.name.localeCompare(b.name);
      break;
    case "slug":
      cmp = a.slug.localeCompare(b.slug);
      break;
    case "user_count":
      cmp = (a.user_count ?? 0) - (b.user_count ?? 0);
      break;
    case "memory_count":
      cmp = (a.memory_count ?? 0) - (b.memory_count ?? 0);
      break;
    case "created_at":
      cmp = new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
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
      <td className="px-4 py-3"><div className="h-4 w-32 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-24 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-12 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-12 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-28 rounded bg-muted" /></td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Create Organization Dialog
// ---------------------------------------------------------------------------

function CreateOrgDialog({ onClose }: { onClose: () => void }) {
  const createMut = useCreateOrg();
  const [name, setName] = useState("");
  const [slug, setSlug] = useState("");
  const [slugManuallyEdited, setSlugManuallyEdited] = useState(false);

  useEffect(() => {
    if (!slugManuallyEdited) {
      setSlug(slugify(name));
    }
  }, [name, slugManuallyEdited]);

  function handleSlugChange(value: string) {
    setSlugManuallyEdited(true);
    setSlug(slugify(value));
  }

  function handleCreate() {
    if (!name.trim() || !slug.trim()) return;
    createMut.mutate(
      { name: name.trim(), slug: slug.trim() },
      { onSuccess: () => onClose() },
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 w-full max-w-md rounded-lg border bg-background p-6 shadow-xl">
        <h2 className="text-lg font-semibold">Create Organization</h2>
        <div className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Name <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Organization name"
              autoFocus
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">
              Slug
            </label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={slug}
              onChange={(e) => handleSlugChange(e.target.value)}
              placeholder="organization-slug"
            />
            <p className="mt-1 text-xs text-muted-foreground">
              Auto-generated from name. You can edit it manually.
            </p>
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
              disabled={!name.trim() || !slug.trim() || createMut.isPending}
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
// Organization Detail Panel
// ---------------------------------------------------------------------------

function OrgDetailPanel({
  orgId,
  onClose,
  onDeleted,
}: {
  orgId: string;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const detailQuery = useOrg(orgId);
  const updateMut = useUpdateOrg();
  const deleteMut = useDeleteOrg();

  const [editName, setEditName] = useState("");
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [initialized, setInitialized] = useState(false);

  const org = detailQuery.data;

  useEffect(() => {
    if (org && !initialized) {
      setEditName(org.name || "");
      setInitialized(true);
    }
  }, [org, initialized]);

  useEffect(() => {
    setInitialized(false);
    setConfirmDelete(false);
    setSaveSuccess(false);
  }, [orgId]);

  function handleSave() {
    if (!org) return;
    const data: UpdateOrgRequest = { name: editName };
    updateMut.mutate(
      { id: org.id, data },
      {
        onSuccess: () => {
          setSaveSuccess(true);
          setTimeout(() => setSaveSuccess(false), 2000);
        },
      },
    );
  }

  function handleDelete() {
    if (!org) return;
    deleteMut.mutate(org.id, {
      onSuccess: () => {
        onDeleted();
        onClose();
      },
    });
  }

  const users: OrgUser[] = org?.users ?? [];
  const owners: OrgUser[] = org?.owners ?? [];
  const ownerIds = new Set(owners.map((o) => o.id));

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 flex h-full w-full max-w-2xl flex-col overflow-y-auto border-l bg-background shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b px-6 py-4">
          <h2 className="text-lg font-semibold">Organization Detail</h2>
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
              Failed to load organization: {detailQuery.error?.message}
            </p>
          </div>
        )}

        {org && initialized && (
          <div className="flex-1 space-y-6 p-6">
            {/* Name (editable) */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Name
              </label>
              <input
                type="text"
                className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
              />
            </div>

            {/* Slug (read-only) */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Slug
              </label>
              <div className="rounded-md border bg-muted/30 px-3 py-2 font-mono text-sm text-muted-foreground">
                {org.slug}
              </div>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-3 gap-4">
              <div className="rounded-lg border p-4 text-center">
                <div className="text-2xl font-bold">{org.user_count ?? 0}</div>
                <div className="text-xs text-muted-foreground">Users</div>
              </div>
              <div className="rounded-lg border p-4 text-center">
                <div className="text-2xl font-bold">{org.memory_count ?? 0}</div>
                <div className="text-xs text-muted-foreground">Memories</div>
              </div>
              <div className="rounded-lg border p-4 text-center">
                <div className="text-2xl font-bold">{org.project_count ?? 0}</div>
                <div className="text-xs text-muted-foreground">Projects</div>
              </div>
            </div>

            {/* Timestamps */}
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Created: </span>
                <span>{formatDate(org.created_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Updated: </span>
                <span>{formatDate(org.updated_at)}</span>
              </div>
            </div>

            {/* Owners */}
            {owners.length > 0 && (
              <div>
                <h3 className="mb-2 text-sm font-semibold">Owners</h3>
                <div className="flex flex-wrap gap-2">
                  {owners.map((owner) => (
                    <span
                      key={owner.id}
                      className="inline-flex items-center gap-1.5 rounded-full bg-amber-100 px-3 py-1 text-xs font-medium text-amber-800 dark:bg-amber-900 dark:text-amber-300"
                    >
                      <span className="inline-block h-2 w-2 rounded-full bg-amber-500" />
                      {owner.display_name || owner.email}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* User List */}
            <div>
              <h3 className="mb-2 text-sm font-semibold">
                Users ({users.length})
              </h3>
              {users.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No users in this organization.
                </p>
              ) : (
                <div className="overflow-auto rounded-lg border">
                  <table className="w-full">
                    <thead className="border-b bg-muted/50">
                      <tr>
                        <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                          Email
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                          Display Name
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                          Role
                        </th>
                        <th className="px-4 py-2 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">
                          Status
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y">
                      {users.map((user) => (
                        <tr key={user.id} className="transition-colors hover:bg-accent/50">
                          <td className="px-4 py-2 text-sm">{user.email}</td>
                          <td className="px-4 py-2 text-sm text-muted-foreground">
                            {user.display_name || "-"}
                          </td>
                          <td className="px-4 py-2">
                            <span
                              className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${roleBadgeClass(user.role)}`}
                            >
                              {user.role}
                            </span>
                          </td>
                          <td className="px-4 py-2 text-right text-xs">
                            {ownerIds.has(user.id) && (
                              <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900 dark:text-amber-300">
                                owner
                              </span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {/* Org Settings */}
            {org.settings && Object.keys(org.settings).length > 0 && (
              <div>
                <h3 className="mb-2 text-sm font-semibold">Organization Settings</h3>
                <pre className="overflow-auto rounded-lg border bg-muted/50 p-3 text-xs font-mono">
                  {JSON.stringify(org.settings, null, 2)}
                </pre>
              </div>
            )}

            {/* Actions */}
            <div className="flex items-center gap-3 border-t pt-4">
              <button
                type="button"
                className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
                onClick={handleSave}
                disabled={updateMut.isPending || !editName.trim()}
              >
                {updateMut.isPending ? "Saving..." : saveSuccess ? "Saved" : "Save Changes"}
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
                    Delete this organization? This cannot be undone.
                  </span>
                  <button
                    type="button"
                    className="rounded bg-red-600 px-3 py-1.5 text-sm text-white hover:bg-red-700 disabled:opacity-50"
                    onClick={handleDelete}
                    disabled={deleteMut.isPending}
                  >
                    {deleteMut.isPending ? "Deleting..." : "Yes, Delete"}
                  </button>
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
                  Delete Organization
                </button>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main OrganizationManagement
// ---------------------------------------------------------------------------

function OrganizationManagement() {
  const orgsQuery = useOrgs();
  const orgs = orgsQuery.data ?? [];

  const [sortField, setSortField] = useState<SortField>("name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [detailOrgId, setDetailOrgId] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);

  function handleSort(field: SortField) {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  }

  const sortedOrgs = useMemo(() => {
    const result = [...orgs];
    result.sort((a, b) => compareOrgs(a, b, sortField, sortDir));
    return result;
  }, [orgs, sortField, sortDir]);

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="space-y-4 pb-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Organization Management
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Manage organizations and membership.
          </p>
        </div>
      </div>

      {/* Loading state */}
      {orgsQuery.isLoading && (
        <div className="py-8 text-center text-sm text-muted-foreground">
          Loading...
        </div>
      )}

      {/* Main content */}
      {!orgsQuery.isLoading && (
        <>
          {/* Toolbar */}
          <div className="flex items-center justify-between pb-4">
            <span className="text-sm text-muted-foreground">
              {sortedOrgs.length} organization{sortedOrgs.length !== 1 ? "s" : ""}
            </span>
            <button
              type="button"
              className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              onClick={() => setShowCreate(true)}
            >
              Create Organization
            </button>
          </div>

          {/* Table */}
          <div className="flex-1 overflow-auto rounded-lg border">
            {orgsQuery.isError && (
              <div className="m-4 rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300">
                Failed to load organizations: {orgsQuery.error?.message ?? "Unknown error"}
              </div>
            )}

            <table className="w-full">
              <thead className="sticky top-0 border-b bg-muted/50">
                <tr>
                  <SortableHeader
                    label="Name"
                    field="name"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <SortableHeader
                    label="Slug"
                    field="slug"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <SortableHeader
                    label="Users"
                    field="user_count"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <SortableHeader
                    label="Memories"
                    field="memory_count"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                  <SortableHeader
                    label="Created"
                    field="created_at"
                    currentField={sortField}
                    currentDir={sortDir}
                    onSort={handleSort}
                  />
                </tr>
              </thead>
              <tbody className="divide-y">
                {orgsQuery.isLoading ? (
                  <>
                    <SkeletonRow />
                    <SkeletonRow />
                    <SkeletonRow />
                  </>
                ) : sortedOrgs.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-4 py-12 text-center">
                      <p className="text-sm text-muted-foreground">
                        No organizations yet. Create one to get started.
                      </p>
                    </td>
                  </tr>
                ) : (
                  sortedOrgs.map((org) => (
                    <tr
                      key={org.id}
                      className="cursor-pointer transition-colors hover:bg-accent/50"
                      onClick={() => setDetailOrgId(org.id)}
                    >
                      <td className="px-4 py-3 text-sm font-medium">{org.name}</td>
                      <td className="px-4 py-3 font-mono text-sm text-muted-foreground">
                        {org.slug}
                      </td>
                      <td className="px-4 py-3 text-sm font-semibold">
                        {org.user_count ?? 0}
                      </td>
                      <td className="px-4 py-3 text-sm font-semibold">
                        {org.memory_count ?? 0}
                      </td>
                      <td className="px-4 py-3 text-xs text-muted-foreground">
                        {formatDate(org.created_at)}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          {/* Create Dialog */}
          {showCreate && <CreateOrgDialog onClose={() => setShowCreate(false)} />}

          {/* Detail Panel */}
          {detailOrgId && (
            <OrgDetailPanel
              orgId={detailOrgId}
              onClose={() => setDetailOrgId(null)}
              onDeleted={() => setDetailOrgId(null)}
            />
          )}
        </>
      )}
    </div>
  );
}

export default OrganizationManagement;
