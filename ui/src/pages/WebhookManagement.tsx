import { useState } from "react";
import {
  useWebhooks,
  useCreateWebhook,
  useUpdateWebhook,
  useDeleteWebhook,
  useTestWebhook,
} from "../hooks/useApi";
import type { Webhook } from "../api/client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface WebhookFull extends Webhook {
  secret?: string;
  last_delivery?: string;
}

interface WebhookFormData {
  url: string;
  scope: string;
  events: string[];
  secret: string;
  active: boolean;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ALL_EVENT_TYPES = [
  { value: "memory.stored", label: "memory.stored", group: "Memory" },
  { value: "memory.updated", label: "memory.updated", group: "Memory" },
  { value: "memory.deleted", label: "memory.deleted", group: "Memory" },
  { value: "memory.recalled", label: "memory.recalled", group: "Memory" },
  { value: "memory.enriched", label: "memory.enriched", group: "Memory" },
  { value: "memory.forgotten", label: "memory.forgotten", group: "Memory" },
  { value: "entity.created", label: "entity.created", group: "Entity" },
  { value: "entity.updated", label: "entity.updated", group: "Entity" },
  { value: "relationship.created", label: "relationship.created", group: "Relationship" },
  { value: "relationship.updated", label: "relationship.updated", group: "Relationship" },
];

const EVENT_GROUPS = ["Memory", "Entity", "Relationship"] as const;

const EVENT_CHIP_COLORS: Record<string, string> = {
  Memory: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
  Entity: "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300",
  Relationship: "bg-teal-100 text-teal-800 dark:bg-teal-900 dark:text-teal-300",
};

function eventChipColor(event: string): string {
  if (event.startsWith("memory.")) return EVENT_CHIP_COLORS["Memory"];
  if (event.startsWith("entity.")) return EVENT_CHIP_COLORS["Entity"];
  if (event.startsWith("relationship.")) return EVENT_CHIP_COLORS["Relationship"];
  return "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300";
}

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

function isValidUrl(str: string): boolean {
  try {
    const url = new URL(str);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function emptyForm(): WebhookFormData {
  return {
    url: "",
    scope: "",
    events: [],
    secret: "",
    active: true,
  };
}

function webhookToForm(wh: WebhookFull): WebhookFormData {
  return {
    url: wh.url,
    scope: wh.scope || "",
    events: wh.events ?? [],
    secret: "",
    active: wh.active,
  };
}

// ---------------------------------------------------------------------------
// Skeleton
// ---------------------------------------------------------------------------

function SkeletonRow() {
  return (
    <tr className="animate-pulse">
      <td className="px-4 py-3"><div className="h-4 w-48 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-24 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-32 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-16 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-28 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-24 rounded bg-muted" /></td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Webhook Form Dialog
// ---------------------------------------------------------------------------

function WebhookFormDialog({
  title,
  initial,
  onSubmit,
  onCancel,
  isPending,
  error,
}: {
  title: string;
  initial: WebhookFormData;
  onSubmit: (data: WebhookFormData) => void;
  onCancel: () => void;
  isPending: boolean;
  error?: string | null;
}) {
  const [form, setForm] = useState<WebhookFormData>(initial);
  const [urlTouched, setUrlTouched] = useState(false);

  const urlValid = !urlTouched || isValidUrl(form.url);

  function toggleEvent(event: string) {
    setForm((prev) => ({
      ...prev,
      events: prev.events.includes(event)
        ? prev.events.filter((e) => e !== event)
        : [...prev.events, event],
    }));
  }

  function selectAllInGroup(group: string) {
    const groupEvents = ALL_EVENT_TYPES.filter((e) => e.group === group).map((e) => e.value);
    const allSelected = groupEvents.every((e) => form.events.includes(e));
    if (allSelected) {
      setForm((prev) => ({
        ...prev,
        events: prev.events.filter((e) => !groupEvents.includes(e)),
      }));
    } else {
      setForm((prev) => ({
        ...prev,
        events: [...new Set([...prev.events, ...groupEvents])],
      }));
    }
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!isValidUrl(form.url)) {
      setUrlTouched(true);
      return;
    }
    onSubmit(form);
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onCancel} />
      <div className="relative z-10 w-full max-w-lg rounded-lg border bg-background shadow-xl">
        <form onSubmit={handleSubmit}>
          <div className="border-b px-6 py-4">
            <h2 className="text-lg font-semibold">{title}</h2>
          </div>

          <div className="space-y-4 px-6 py-4">
            {/* URL */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                URL <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                className={`w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring ${
                  !urlValid ? "border-red-500" : ""
                }`}
                placeholder="https://example.com/webhook"
                value={form.url}
                onChange={(e) => setForm({ ...form, url: e.target.value })}
                onBlur={() => setUrlTouched(true)}
                required
              />
              {!urlValid && (
                <p className="mt-1 text-xs text-red-500">
                  Please enter a valid HTTP or HTTPS URL.
                </p>
              )}
            </div>

            {/* Scope */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Scope / Namespace
              </label>
              <input
                type="text"
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="e.g. ns:my-namespace (optional)"
                value={form.scope}
                onChange={(e) => setForm({ ...form, scope: e.target.value })}
              />
            </div>

            {/* Event Type Selector */}
            <div>
              <label className="mb-2 block text-sm font-medium text-muted-foreground">
                Event Types
              </label>
              <div className="space-y-3 rounded-md border p-3">
                {EVENT_GROUPS.map((group) => {
                  const groupEvents = ALL_EVENT_TYPES.filter((e) => e.group === group);
                  const allSelected = groupEvents.every((e) => form.events.includes(e.value));
                  const someSelected = groupEvents.some((e) => form.events.includes(e.value));
                  return (
                    <div key={group}>
                      <div className="mb-1 flex items-center gap-2">
                        <input
                          type="checkbox"
                          className="rounded border"
                          checked={allSelected}
                          ref={(el) => {
                            if (el) el.indeterminate = someSelected && !allSelected;
                          }}
                          onChange={() => selectAllInGroup(group)}
                        />
                        <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                          {group}
                        </span>
                      </div>
                      <div className="ml-5 flex flex-wrap gap-x-4 gap-y-1">
                        {groupEvents.map((evt) => (
                          <label key={evt.value} className="flex items-center gap-1.5 text-sm">
                            <input
                              type="checkbox"
                              className="rounded border"
                              checked={form.events.includes(evt.value)}
                              onChange={() => toggleEvent(evt.value)}
                            />
                            <span className="font-mono text-xs">{evt.label}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Secret */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Secret Key
              </label>
              <input
                type="password"
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Optional HMAC signing secret"
                value={form.secret}
                onChange={(e) => setForm({ ...form, secret: e.target.value })}
                autoComplete="new-password"
              />
              <p className="mt-1 text-xs text-muted-foreground">
                Used for HMAC-SHA256 payload signing. Leave blank to keep unchanged when editing.
              </p>
            </div>

            {/* Enabled */}
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="webhook-enabled"
                className="rounded border"
                checked={form.active}
                onChange={(e) => setForm({ ...form, active: e.target.checked })}
              />
              <label htmlFor="webhook-enabled" className="text-sm">
                Enabled
              </label>
            </div>

            {/* Error */}
            {error && (
              <div className="rounded-md border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300">
                {error}
              </div>
            )}
          </div>

          <div className="flex items-center justify-end gap-3 border-t px-6 py-4">
            <button
              type="button"
              className="rounded-md border px-4 py-2 text-sm font-medium hover:bg-muted"
              onClick={onCancel}
              disabled={isPending}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="bg-primary text-primary-foreground hover:bg-primary/90 rounded-md px-4 py-2 text-sm font-medium disabled:opacity-50"
              disabled={isPending}
            >
              {isPending ? "Saving..." : "Save"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Delete Confirmation Dialog
// ---------------------------------------------------------------------------

function DeleteConfirmDialog({
  webhook,
  onConfirm,
  onCancel,
  isPending,
}: {
  webhook: WebhookFull;
  onConfirm: () => void;
  onCancel: () => void;
  isPending: boolean;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onCancel} />
      <div className="relative z-10 w-full max-w-md rounded-lg border bg-background p-6 shadow-xl">
        <h2 className="text-lg font-semibold">Delete Webhook</h2>
        <p className="mt-2 text-sm text-muted-foreground">
          Are you sure you want to delete the webhook for{" "}
          <span className="font-mono font-medium text-foreground">{webhook.url}</span>?
          This action cannot be undone.
        </p>
        <div className="mt-6 flex items-center justify-end gap-3">
          <button
            type="button"
            className="rounded-md border px-4 py-2 text-sm font-medium hover:bg-muted"
            onClick={onCancel}
            disabled={isPending}
          >
            Cancel
          </button>
          <button
            type="button"
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90 rounded-md px-4 py-2 text-sm font-medium disabled:opacity-50"
            onClick={onConfirm}
            disabled={isPending}
          >
            {isPending ? "Deleting..." : "Delete"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Test Result Dialog
// ---------------------------------------------------------------------------

function TestResultDialog({
  result,
  onClose,
}: {
  result: { success: boolean; status_code?: number; error?: string };
  onClose: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative z-10 w-full max-w-md rounded-lg border bg-background p-6 shadow-xl">
        <h2 className="text-lg font-semibold">Test Fire Result</h2>
        <div className="mt-4 space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium">Status:</span>
            {result.success ? (
              <span className="inline-flex items-center rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800 dark:bg-green-900 dark:text-green-300">
                Success
              </span>
            ) : (
              <span className="inline-flex items-center rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-medium text-red-800 dark:bg-red-900 dark:text-red-300">
                Failed
              </span>
            )}
          </div>
          {result.status_code !== undefined && (
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium">HTTP Status:</span>
              <span className="font-mono text-sm">{result.status_code}</span>
            </div>
          )}
          {result.error && (
            <div>
              <span className="text-sm font-medium">Error:</span>
              <pre className="mt-1 overflow-auto rounded border bg-muted/50 p-2 text-xs font-mono">
                {result.error}
              </pre>
            </div>
          )}
        </div>
        <div className="mt-6 flex justify-end">
          <button
            type="button"
            className="rounded-md border px-4 py-2 text-sm font-medium hover:bg-muted"
            onClick={onClose}
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

function WebhookManagement() {
  const webhooksQuery = useWebhooks();
  const createMut = useCreateWebhook();
  const updateMut = useUpdateWebhook();
  const deleteMut = useDeleteWebhook();
  const testMut = useTestWebhook();

  const webhooks = (webhooksQuery.data ?? []) as WebhookFull[];

  const [showCreate, setShowCreate] = useState(false);
  const [editingWebhook, setEditingWebhook] = useState<WebhookFull | null>(null);
  const [deletingWebhook, setDeletingWebhook] = useState<WebhookFull | null>(null);
  const [testResult, setTestResult] = useState<{
    success: boolean;
    status_code?: number;
    error?: string;
  } | null>(null);
  const [createError, setCreateError] = useState<string | null>(null);
  const [editError, setEditError] = useState<string | null>(null);
  const [testingId, setTestingId] = useState<string | null>(null);

  function handleCreate(data: WebhookFormData) {
    setCreateError(null);
    const payload: Record<string, unknown> = {
      url: data.url,
      scope: data.scope || undefined,
      events: data.events.length > 0 ? data.events : undefined,
      active: data.active,
    };
    if (data.secret) {
      payload.secret = data.secret;
    }
    createMut.mutate(payload as Partial<Webhook>, {
      onSuccess: () => {
        setShowCreate(false);
        setCreateError(null);
      },
      onError: (err) => {
        setCreateError(err.message);
      },
    });
  }

  function handleUpdate(data: WebhookFormData) {
    if (!editingWebhook) return;
    setEditError(null);
    const payload: Record<string, unknown> = {
      url: data.url,
      scope: data.scope || undefined,
      events: data.events.length > 0 ? data.events : undefined,
      active: data.active,
    };
    if (data.secret) {
      payload.secret = data.secret;
    }
    updateMut.mutate(
      { id: editingWebhook.id, data: payload as Partial<Webhook> },
      {
        onSuccess: () => {
          setEditingWebhook(null);
          setEditError(null);
        },
        onError: (err) => {
          setEditError(err.message);
        },
      },
    );
  }

  function handleDelete() {
    if (!deletingWebhook) return;
    deleteMut.mutate(deletingWebhook.id, {
      onSuccess: () => {
        setDeletingWebhook(null);
      },
    });
  }

  function handleTestFire(wh: WebhookFull) {
    setTestingId(wh.id);
    testMut.mutate(wh.id, {
      onSuccess: (result) => {
        setTestResult(result);
        setTestingId(null);
      },
      onError: (err) => {
        setTestResult({ success: false, error: err.message });
        setTestingId(null);
      },
    });
  }

  function handleToggleEnabled(wh: WebhookFull) {
    updateMut.mutate({
      id: wh.id,
      data: { active: !wh.active },
    });
  }

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-start justify-between pb-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Webhook Management
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Configure and monitor webhook deliveries.
          </p>
        </div>
        <button
          type="button"
          className="bg-primary text-primary-foreground hover:bg-primary/90 rounded-md px-4 py-2 text-sm font-medium"
          onClick={() => {
            setCreateError(null);
            setShowCreate(true);
          }}
        >
          Create Webhook
        </button>
      </div>

      {/* Error */}
      {webhooksQuery.isError && (
        <div className="mb-4 rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300">
          Failed to load webhooks: {webhooksQuery.error?.message ?? "Unknown error"}
        </div>
      )}

      {/* Table */}
      <div className="flex-1 overflow-auto rounded-lg border">
        <table className="w-full">
          <thead className="sticky top-0 border-b bg-muted/50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                URL
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Scope
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Events
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Status
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Created
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {webhooksQuery.isLoading ? (
              <>
                <SkeletonRow />
                <SkeletonRow />
                <SkeletonRow />
              </>
            ) : webhooks.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-12 text-center">
                  <p className="text-sm text-muted-foreground">
                    No webhooks configured. Create one to receive event notifications.
                  </p>
                </td>
              </tr>
            ) : (
              webhooks.map((wh) => (
                <tr key={wh.id} className="transition-colors hover:bg-accent/50">
                  <td className="px-4 py-3">
                    <div className="max-w-xs truncate font-mono text-sm" title={wh.url}>
                      {wh.url}
                    </div>
                    {wh.failure_count !== undefined && wh.failure_count > 0 && (
                      <div className="mt-0.5 text-xs text-amber-600 dark:text-amber-400">
                        {wh.failure_count} failure{wh.failure_count !== 1 ? "s" : ""}
                        {wh.last_status !== undefined && (
                          <span className="ml-1">(last: {wh.last_status})</span>
                        )}
                      </div>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <span className="font-mono text-sm text-muted-foreground">
                      {wh.scope || "--"}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex flex-wrap gap-1">
                      {wh.events && wh.events.length > 0 ? (
                        wh.events.map((evt) => (
                          <span
                            key={evt}
                            className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${eventChipColor(evt)}`}
                          >
                            {evt}
                          </span>
                        ))
                      ) : (
                        <span className="text-xs text-muted-foreground">all events</span>
                      )}
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <button
                      type="button"
                      onClick={() => handleToggleEnabled(wh)}
                      disabled={updateMut.isPending}
                      title={wh.active ? "Click to disable" : "Click to enable"}
                    >
                      {wh.active ? (
                        <span className="inline-flex items-center rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800 dark:bg-green-900 dark:text-green-300">
                          Enabled
                        </span>
                      ) : (
                        <span className="inline-flex items-center rounded-full bg-gray-100 px-2.5 py-0.5 text-xs font-medium text-gray-800 dark:bg-gray-800 dark:text-gray-300">
                          Disabled
                        </span>
                      )}
                    </button>
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">
                    {formatDate(wh.created_at)}
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center justify-end gap-2">
                      <button
                        type="button"
                        className="rounded border px-2.5 py-1 text-xs hover:bg-muted"
                        onClick={() => handleTestFire(wh)}
                        disabled={testingId === wh.id}
                      >
                        {testingId === wh.id ? "Testing..." : "Test"}
                      </button>
                      <button
                        type="button"
                        className="rounded border px-2.5 py-1 text-xs hover:bg-muted"
                        onClick={() => {
                          setEditError(null);
                          setEditingWebhook(wh);
                        }}
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        className="rounded border border-red-300 px-2.5 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
                        onClick={() => setDeletingWebhook(wh)}
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Create Dialog */}
      {showCreate && (
        <WebhookFormDialog
          title="Create Webhook"
          initial={emptyForm()}
          onSubmit={handleCreate}
          onCancel={() => {
            setShowCreate(false);
            setCreateError(null);
          }}
          isPending={createMut.isPending}
          error={createError}
        />
      )}

      {/* Edit Dialog */}
      {editingWebhook && (
        <WebhookFormDialog
          title="Edit Webhook"
          initial={webhookToForm(editingWebhook)}
          onSubmit={handleUpdate}
          onCancel={() => {
            setEditingWebhook(null);
            setEditError(null);
          }}
          isPending={updateMut.isPending}
          error={editError}
        />
      )}

      {/* Delete Confirm Dialog */}
      {deletingWebhook && (
        <DeleteConfirmDialog
          webhook={deletingWebhook}
          onConfirm={handleDelete}
          onCancel={() => setDeletingWebhook(null)}
          isPending={deleteMut.isPending}
        />
      )}

      {/* Test Result Dialog */}
      {testResult && (
        <TestResultDialog
          result={testResult}
          onClose={() => setTestResult(null)}
        />
      )}
    </div>
  );
}

export default WebhookManagement;
