import { useState, useCallback, useMemo } from "react";
import {
  useMeShares,
  useMeShareDetail,
  useCreateMeShare,
  useUpdateMeShareGrants,
  useRevokeMeShare,
  useMeProjects,
  useRevokeMeOAuthClient,
} from "../hooks/useApi";
import type {
  ShareToken,
  ShareGrantInput,
  SharePermission,
  ShareCreatedResponse,
} from "../api/client";
import { EmptyState } from "../components/EmptyState";
import { CopyButton } from "../components/CopyButton";
import { faShareNodes } from "../lib/icons";

function formatDate(iso?: string | null): string {
  if (!iso) return "-";
  return new Date(iso).toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function shareStatus(s: ShareToken): { label: string; tone: "ok" | "warn" | "bad" } {
  if (s.revoked_at) return { label: "revoked", tone: "bad" };
  if (new Date(s.expires_at) < new Date()) return { label: "expired", tone: "bad" };
  if (s.is_one_shot && s.consumed_at) return { label: "consumed", tone: "warn" };
  return { label: "active", tone: "ok" };
}

function permissionLabel(p: SharePermission): string {
  switch (p) {
    case "read":
      return "Read only";
    case "read_store":
      return "Read + Store";
    case "read_store_modify":
      return "Read + Store + Modify";
  }
}

// ---------------------------------------------------------------------------
// CreateSharePanel: inline create form rendered above the share list when
// the user opens the create flow. Renders the one-time secret in the same
// surface after creation. Inline rather than modal: matches the rest of
// the codebase's preference for in-flow panels over overlays.
// ---------------------------------------------------------------------------

function CreateSharePanel({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const projectsQuery = useMeProjects();
  const create = useCreateMeShare();

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [isOneShot, setIsOneShot] = useState(false);
  const [expiresInDays, setExpiresInDays] = useState(30);
  const [grants, setGrants] = useState<ShareGrantInput[]>([]);
  const [error, setError] = useState("");
  const [created, setCreated] = useState<ShareCreatedResponse | null>(null);
  // Custom expiry date when expiresInDays === -1. ISO date string in
  // YYYY-MM-DD format produced by <input type=date>.
  const [customExpiryDate, setCustomExpiryDate] = useState("");

  const reset = useCallback(() => {
    setName("");
    setDescription("");
    setIsOneShot(false);
    setExpiresInDays(30);
    setGrants([]);
    setError("");
    setCreated(null);
    setCustomExpiryDate("");
  }, []);

  const handleClose = useCallback(() => {
    reset();
    onClose();
  }, [reset, onClose]);

  const addGrant = useCallback((projectID: string) => {
    setGrants((prev) => {
      if (prev.some((g) => g.project_id === projectID)) return prev;
      return [...prev, { project_id: projectID, permission: "read" }];
    });
  }, []);

  const removeGrant = useCallback((projectID: string) => {
    setGrants((prev) => prev.filter((g) => g.project_id !== projectID));
  }, []);

  const changeGrant = useCallback((projectID: string, permission: SharePermission) => {
    setGrants((prev) =>
      prev.map((g) => (g.project_id === projectID ? { ...g, permission } : g)),
    );
  }, []);

  const handleCreate = useCallback(async () => {
    setError("");
    if (!name.trim()) {
      setError("Name is required");
      return;
    }
    if (grants.length === 0) {
      setError("Select at least one project to share");
      return;
    }
    let expiresAt: string;
    if (expiresInDays === -1) {
      if (!customExpiryDate) {
        setError("Pick a custom expiry date");
        return;
      }
      // <input type=date> returns YYYY-MM-DD. Set to end-of-day UTC so the
      // share survives the calendar day the user picked, not just midnight.
      const dt = new Date(customExpiryDate + "T23:59:59Z");
      if (Number.isNaN(dt.getTime()) || dt.getTime() < Date.now()) {
        setError("Custom expiry must be a future date");
        return;
      }
      expiresAt = dt.toISOString();
    } else {
      expiresAt = new Date(Date.now() + expiresInDays * 24 * 60 * 60 * 1000).toISOString();
    }
    try {
      const result = await create.mutateAsync({
        name: name.trim(),
        description: description.trim() || undefined,
        is_one_shot: isOneShot,
        expires_at: expiresAt,
        grants,
      });
      setCreated(result);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [name, description, isOneShot, expiresInDays, customExpiryDate, grants, create]);

  if (!open) return null;

  const projects = projectsQuery.data ?? [];
  const projectsByID = new Map(projects.map((p) => [p.id, p]));

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/30" onClick={handleClose} />
      <div className="relative z-10 flex h-screen w-full max-w-xl flex-col overflow-y-auto border-l bg-background p-6 shadow-xl">
      {created ? (
          <div>
            <h2 className="mb-2 text-lg font-semibold">Share created</h2>
            <p className="mb-4 text-sm text-muted-foreground">
              Copy this token now. It is not stored and will never be shown again.
            </p>
            <div className="mb-4 rounded border border-input bg-muted p-3 font-mono text-sm break-all">
              {created.secret}
            </div>
            {created.share.is_one_shot ? (
              <div className="mb-4">
                <p className="mb-2 text-xs text-muted-foreground">
                  One-shot shares can only be redeemed through the OAuth consent flow. The bearer-token URL is intentionally not offered; paste the magic link to the recipient.
                </p>
                <CopyButton
                  text={`${window.location.origin}/share/accept?token=${created.secret}`}
                  label="Copy magic link"
                  className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
                />
              </div>
            ) : (
              <div className="mb-4 flex gap-2">
                <CopyButton
                  text={created.secret}
                  label="Copy token"
                  className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
                />
                <CopyButton
                  text={`${window.location.origin}/share/accept?token=${created.secret}`}
                  label="Copy magic link"
                  className="rounded border border-input bg-background px-4 py-2 text-sm hover:bg-muted"
                />
              </div>
            )}
            <button
              type="button"
              onClick={handleClose}
              className="rounded border border-input bg-background px-4 py-2 text-sm hover:bg-muted"
            >
              Close
            </button>
          </div>
        ) : (
          <div>
            <h2 className="mb-4 text-lg font-semibold">Create share</h2>
            {error && (
              <div className="mb-4 rounded border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
                {error}
              </div>
            )}
            <div className="mb-3">
              <label className="mb-1 block text-sm font-medium">Name</label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. Q3 architecture review"
                className="w-full rounded border border-input bg-background px-3 py-2 text-sm"
              />
            </div>
            <div className="mb-3">
              <label className="mb-1 block text-sm font-medium">
                Description <span className="text-muted-foreground">(optional)</span>
              </label>
              <input
                type="text"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What is being shared and why"
                className="w-full rounded border border-input bg-background px-3 py-2 text-sm"
              />
            </div>
            <div className="mb-3 flex flex-wrap items-center gap-4">
              <label className="text-sm font-medium">Expires</label>
              <select
                value={expiresInDays}
                onChange={(e) => setExpiresInDays(Number(e.target.value))}
                className="rounded border border-input bg-background px-2 py-1 text-sm"
              >
                <option value={7}>in 7 days</option>
                <option value={30}>in 30 days</option>
                <option value={60}>in 60 days</option>
                <option value={90}>in 90 days</option>
                <option value={-1}>on…</option>
              </select>
              {expiresInDays === -1 && (
                <input
                  type="date"
                  value={customExpiryDate}
                  onChange={(e) => setCustomExpiryDate(e.target.value)}
                  className="rounded border border-input bg-background px-2 py-1 text-sm"
                  min={new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().slice(0, 10)}
                />
              )}
              <label className="ml-4 flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={isOneShot}
                  onChange={(e) => setIsOneShot(e.target.checked)}
                />
                One-shot
              </label>
            </div>
            <div className="mb-4">
              <label className="mb-1 block text-sm font-medium">Projects + access</label>
              <div className="space-y-1.5">
                {grants.map((g) => {
                  const p = projectsByID.get(g.project_id);
                  return (
                    <div key={g.project_id} className="flex items-center gap-2 rounded border border-input p-2">
                      <div className="flex-1 text-sm">{p ? `${p.name} (${p.slug})` : g.project_id}</div>
                      <select
                        value={g.permission}
                        onChange={(e) => changeGrant(g.project_id, e.target.value as SharePermission)}
                        className="rounded border border-input bg-background px-2 py-1 text-xs"
                      >
                        <option value="read">Read only</option>
                        <option value="read_store">Read + Store</option>
                        <option value="read_store_modify">Read + Store + Modify</option>
                      </select>
                      <button
                        type="button"
                        onClick={() => removeGrant(g.project_id)}
                        className="text-xs text-destructive hover:underline"
                      >
                        Remove
                      </button>
                    </div>
                  );
                })}
                <select
                  value=""
                  onChange={(e) => {
                    if (e.target.value) addGrant(e.target.value);
                    e.target.value = "";
                  }}
                  className="w-full rounded border border-input bg-background px-2 py-2 text-sm"
                >
                  <option value="">+ Add project</option>
                  {projects
                    .filter((p) => !grants.some((g) => g.project_id === p.id))
                    .map((p) => (
                      <option key={p.id} value={p.id}>
                        {p.name} ({p.slug})
                      </option>
                    ))}
                </select>
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <button
                type="button"
                onClick={handleClose}
                className="rounded border border-input bg-background px-4 py-2 text-sm hover:bg-muted"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleCreate}
                disabled={create.isPending}
                className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {create.isPending ? "Creating…" : "Create"}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// ShareDetailPanel: per-share view with grant editor and bindings list.
// Rendered inline beside the list when a row is expanded.
// ---------------------------------------------------------------------------

function ShareDetailPanel({ shareID, onClose }: { shareID: string; onClose: () => void }) {
  const detail = useMeShareDetail(shareID);
  const projectsQuery = useMeProjects();
  const update = useUpdateMeShareGrants();
  const revoke = useRevokeMeShare();

  const projectsByID = useMemo(
    () => new Map((projectsQuery.data ?? []).map((p) => [p.id, p])),
    [projectsQuery.data],
  );

  const share = detail.data;
  const [draft, setDraft] = useState<ShareGrantInput[] | null>(null);
  const [error, setError] = useState("");

  const startEdit = useCallback(() => {
    if (!share) return;
    setDraft(share.grants.map((g) => ({ project_id: g.project_id, permission: g.permission })));
    setError("");
  }, [share]);

  const handleSave = useCallback(async () => {
    if (!draft) return;
    if (draft.length === 0) {
      setError("At least one project is required (use Revoke share to remove all access)");
      return;
    }
    try {
      await update.mutateAsync({ id: shareID, grants: draft });
      setDraft(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [draft, shareID, update]);

  if (detail.isLoading) {
    return <div className="surface-elevated mt-3 rounded p-4 text-sm text-muted-foreground">Loading…</div>;
  }
  if (!share) return null;

  const status = shareStatus(share);

  return (
    <div className="surface-elevated mt-3 rounded p-4">
      <div className="mb-3 flex items-start justify-between">
        <div>
          <h3 className="text-base font-semibold">{share.name}</h3>
          {share.description && (
            <p className="text-sm text-muted-foreground">{share.description}</p>
          )}
        </div>
        <button
          type="button"
          onClick={onClose}
          className="text-sm text-muted-foreground hover:underline"
        >
          Close
        </button>
      </div>

      <div className="mb-4 grid grid-cols-2 gap-2 text-sm">
        <div>
          <span className="text-muted-foreground">Status:</span>{" "}
          <span className={status.tone === "ok" ? "text-emerald-600" : status.tone === "warn" ? "text-amber-600" : "text-destructive"}>
            {status.label}
          </span>
        </div>
        <div>
          <span className="text-muted-foreground">Token prefix:</span>{" "}
          <code className="text-xs">nram_s_{share.token_prefix}…</code>
        </div>
        <div>
          <span className="text-muted-foreground">Expires:</span> {formatDate(share.expires_at)}
        </div>
        <div>
          <span className="text-muted-foreground">Use count:</span> {share.use_count}
        </div>
        <div>
          <span className="text-muted-foreground">Last used:</span> {formatDate(share.last_used_at)}
        </div>
        <div>
          <span className="text-muted-foreground">One-shot:</span> {share.is_one_shot ? "yes" : "no"}
        </div>
      </div>

      <div className="mb-4">
        <div className="mb-2 flex items-center justify-between">
          <h4 className="text-sm font-semibold">Grants</h4>
          {draft === null ? (
            <button
              type="button"
              onClick={startEdit}
              disabled={Boolean(share.revoked_at)}
              className="text-xs text-primary hover:underline disabled:text-muted-foreground disabled:no-underline"
            >
              Edit
            </button>
          ) : (
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => setDraft(null)}
                className="text-xs text-muted-foreground hover:underline"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleSave}
                disabled={update.isPending}
                className="text-xs text-primary hover:underline disabled:opacity-50"
              >
                Save
              </button>
            </div>
          )}
        </div>
        {error && (
          <div className="mb-2 rounded border border-destructive/40 bg-destructive/10 p-2 text-xs text-destructive">
            {error}
          </div>
        )}
        <div className="space-y-1.5">
          {(draft ?? share.grants).map((g) => {
            const p = projectsByID.get(g.project_id);
            return (
              <div key={g.project_id} className="flex items-center gap-2 rounded border border-input p-2">
                <div className="flex-1 text-sm">{p ? `${p.name} (${p.slug})` : g.project_id}</div>
                {draft !== null ? (
                  <>
                    <select
                      value={g.permission}
                      onChange={(e) => {
                        const value = e.target.value as SharePermission;
                        setDraft((prev) =>
                          (prev ?? []).map((x) =>
                            x.project_id === g.project_id ? { ...x, permission: value } : x,
                          ),
                        );
                      }}
                      className="rounded border border-input bg-background px-2 py-1 text-xs"
                    >
                      <option value="read">Read only</option>
                      <option value="read_store">Read + Store</option>
                      <option value="read_store_modify">Read + Store + Modify</option>
                    </select>
                    <button
                      type="button"
                      onClick={() =>
                        setDraft((prev) => (prev ?? []).filter((x) => x.project_id !== g.project_id))
                      }
                      className="text-xs text-destructive hover:underline"
                    >
                      Remove
                    </button>
                  </>
                ) : (
                  <span className="text-xs text-muted-foreground">{permissionLabel(g.permission)}</span>
                )}
              </div>
            );
          })}
          {draft !== null && (
            <select
              value=""
              onChange={(e) => {
                if (!e.target.value) return;
                const value = e.target.value;
                setDraft((prev) => [
                  ...(prev ?? []),
                  { project_id: value, permission: "read" },
                ]);
                e.target.value = "";
              }}
              className="w-full rounded border border-input bg-background px-2 py-2 text-sm"
            >
              <option value="">+ Add project</option>
              {(projectsQuery.data ?? [])
                .filter((p) => !(draft ?? []).some((g) => g.project_id === p.id))
                .map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name} ({p.slug})
                  </option>
                ))}
            </select>
          )}
        </div>
      </div>

      {share.bindings && share.bindings.length > 0 && (
        <div className="mb-4">
          <h4 className="mb-2 text-sm font-semibold">OAuth bindings</h4>
          <p className="mb-2 text-xs text-muted-foreground">
            Each binding is the credential the recipient minted from this share. Revoking a binding cancels that specific recipient's access; the share itself stays usable for new bindings.
          </p>
          <div className="space-y-1.5">
            {share.bindings.map((b) => (
              <BindingRow key={b.id} binding={b} />
            ))}
          </div>
        </div>
      )}

      {!share.revoked_at && (
        <button
          type="button"
          onClick={() => {
            if (confirm("Revoke this share? All derived OAuth tokens will be invalidated.")) {
              revoke.mutate(shareID);
            }
          }}
          disabled={revoke.isPending}
          className="rounded border border-destructive/40 bg-destructive/10 px-3 py-1.5 text-sm text-destructive hover:bg-destructive/20 disabled:opacity-50"
        >
          {revoke.isPending ? "Revoking…" : "Revoke share"}
        </button>
      )}
    </div>
  );
}

// BindingRow renders one OAuth-binding row inside ShareDetailPanel with its
// own revoke control. Reuses the /v1/me/oauth-clients/{id} DELETE endpoint
// since per-binding revoke is identical to revoking the underlying client.
function BindingRow({ binding }: { binding: import("../api/client").ShareBinding }) {
  const revoke = useRevokeMeOAuthClient();
  return (
    <div className="flex items-center justify-between rounded border border-input p-2 text-sm">
      <div className="flex-1 min-w-0">
        <div className="truncate">{binding.name}</div>
        <code className="text-xs text-muted-foreground">{binding.client_id}</code>
      </div>
      <span className="mr-3 text-xs text-muted-foreground">{formatDate(binding.created_at)}</span>
      <button
        type="button"
        onClick={() => {
          if (confirm(`Revoke binding "${binding.name}"? The recipient using this client will lose access on their next request.`)) {
            revoke.mutate(binding.id);
          }
        }}
        disabled={revoke.isPending}
        className="rounded border border-destructive/40 bg-destructive/10 px-2 py-1 text-xs text-destructive hover:bg-destructive/20 disabled:opacity-50"
      >
        Revoke
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function Shares() {
  const sharesQuery = useMeShares();
  const [createOpen, setCreateOpen] = useState(false);
  const [expandedID, setExpandedID] = useState<string | null>(null);

  const shares = sharesQuery.data ?? [];

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Shares</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Capability-bearer credentials that let external recipients access curated
            projects through their own MCP client, with no nram account required.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setCreateOpen(true)}
          className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          Create share
        </button>
      </div>

      <CreateSharePanel open={createOpen} onClose={() => setCreateOpen(false)} />

      {sharesQuery.isLoading ? (
        <div className="surface-elevated rounded p-6 text-center text-sm text-muted-foreground">
          Loading…
        </div>
      ) : shares.length === 0 && !createOpen ? (
        <EmptyState
          icon={faShareNodes}
          title="No shares yet"
          body="Create a share to give someone scoped access to specific projects through their own MCP client. They never need an nram account."
        />
      ) : shares.length === 0 ? null : (
        <div className="surface-elevated rounded">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                <th className="px-4 py-2">Name</th>
                <th className="px-4 py-2">Projects</th>
                <th className="px-4 py-2">Expires</th>
                <th className="px-4 py-2">Uses</th>
                <th className="px-4 py-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {shares.map((s) => {
                const status = shareStatus(s);
                const expanded = expandedID === s.id;
                return (
                  <tr
                    key={s.id}
                    className="cursor-pointer border-b border-border/60 hover:bg-muted/30"
                    onClick={() => setExpandedID(expanded ? null : s.id)}
                  >
                    <td className="px-4 py-3">
                      <div className="font-medium">{s.name}</div>
                      {s.description && (
                        <div className="text-xs text-muted-foreground">{s.description}</div>
                      )}
                    </td>
                    <td className="px-4 py-3 text-xs text-muted-foreground">
                      {s.grants.length} project{s.grants.length === 1 ? "" : "s"}
                    </td>
                    <td className="px-4 py-3 text-xs">{formatDate(s.expires_at)}</td>
                    <td className="px-4 py-3 text-xs">{s.use_count}</td>
                    <td className="px-4 py-3">
                      <span
                        className={
                          status.tone === "ok"
                            ? "rounded bg-emerald-500/15 px-2 py-0.5 text-xs text-emerald-600"
                            : status.tone === "warn"
                              ? "rounded bg-amber-500/15 px-2 py-0.5 text-xs text-amber-600"
                              : "rounded bg-destructive/15 px-2 py-0.5 text-xs text-destructive"
                        }
                      >
                        {status.label}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {expandedID && <ShareDetailPanel shareID={expandedID} onClose={() => setExpandedID(null)} />}
    </div>
  );
}
