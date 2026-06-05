import { useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faListCheck, faPlus, faPen, faTrash, faXmark } from "@fortawesome/free-solid-svg-icons";
import { PageHeader } from "../components/PageHeader";
import { EmptyState } from "../components/EmptyState";
import Switch from "../components/Switch";
import {
  useProcedural,
  useCreateProcedural,
  useUpdateProcedural,
  useDeleteProcedural,
} from "../hooks/useApi";
import type {
  ProceduralEntry,
  CreateProceduralRequest,
} from "../api/client";

const inputClass =
  "w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring";
const primaryBtn =
  "inline-flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50";
const ghostBtn =
  "inline-flex items-center gap-2 rounded-md border px-3 py-2 text-sm font-medium hover:bg-muted disabled:opacity-50";

type DraftState = {
  content: string;
  title: string;
  category: string;
  tags: string;
  priority: number;
  enabled: boolean;
};

function emptyDraft(): DraftState {
  return { content: "", title: "", category: "", tags: "", priority: 0, enabled: true };
}

function draftFromEntry(e: ProceduralEntry): DraftState {
  return {
    content: e.content,
    title: e.title ?? "",
    category: e.category ?? "",
    tags: (e.tags ?? []).join(", "),
    priority: e.priority ?? 0,
    enabled: e.enabled,
  };
}

function toRequest(d: DraftState): CreateProceduralRequest {
  const tags = d.tags
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
  return {
    content: d.content,
    title: d.title.trim(),
    category: d.category.trim(),
    tags,
    priority: d.priority,
    enabled: d.enabled,
  };
}

function EntryForm({
  initial,
  submitting,
  submitLabel,
  onSubmit,
  onCancel,
}: {
  initial: DraftState;
  submitting: boolean;
  submitLabel: string;
  onSubmit: (d: DraftState) => void;
  onCancel: () => void;
}) {
  const [draft, setDraft] = useState<DraftState>(initial);
  const canSubmit = draft.content.trim().length > 0 && !submitting;

  return (
    <div className="space-y-4 rounded-lg border bg-card p-4">
      <div>
        <label className="mb-1 block text-sm font-medium text-muted-foreground">
          Rule (stored verbatim)
        </label>
        <textarea
          value={draft.content}
          onChange={(e) => setDraft({ ...draft, content: e.target.value })}
          rows={5}
          placeholder="A standing instruction the AI should always follow…"
          className={`${inputClass} font-mono`}
        />
      </div>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <div>
          <label className="mb-1 block text-sm font-medium text-muted-foreground">Title</label>
          <input
            value={draft.title}
            onChange={(e) => setDraft({ ...draft, title: e.target.value })}
            placeholder="Short label"
            className={inputClass}
          />
        </div>
        <div>
          <label className="mb-1 block text-sm font-medium text-muted-foreground">Category</label>
          <input
            value={draft.category}
            onChange={(e) => setDraft({ ...draft, category: e.target.value })}
            placeholder="e.g. failure-mode, checklist"
            className={inputClass}
          />
        </div>
        <div>
          <label className="mb-1 block text-sm font-medium text-muted-foreground">
            Tags (comma-separated)
          </label>
          <input
            value={draft.tags}
            onChange={(e) => setDraft({ ...draft, tags: e.target.value })}
            placeholder="non-negotiable, em-dash"
            className={inputClass}
          />
        </div>
        <div>
          <label className="mb-1 block text-sm font-medium text-muted-foreground">
            Priority (higher returned first)
          </label>
          <input
            type="number"
            value={draft.priority}
            onChange={(e) =>
              setDraft({ ...draft, priority: Number.parseInt(e.target.value, 10) || 0 })
            }
            className={`${inputClass} w-32`}
          />
        </div>
      </div>
      <div className="flex items-center gap-3">
        <Switch checked={draft.enabled} onChange={(v) => setDraft({ ...draft, enabled: v })} />
        <span className="text-sm text-muted-foreground">
          {draft.enabled
            ? "Returned by procedural_fetch"
            : "Excluded from fetch — kept for reference"}
        </span>
      </div>
      <div className="flex items-center justify-end gap-2">
        <button type="button" className={ghostBtn} onClick={onCancel} disabled={submitting}>
          <FontAwesomeIcon icon={faXmark} /> Cancel
        </button>
        <button
          type="button"
          className={primaryBtn}
          onClick={() => onSubmit(draft)}
          disabled={!canSubmit}
        >
          {submitLabel}
        </button>
      </div>
    </div>
  );
}

function EntryRow({
  entry,
  onEdit,
  onToggle,
  onDelete,
  busy,
}: {
  entry: ProceduralEntry;
  onEdit: () => void;
  onToggle: (v: boolean) => void;
  onDelete: () => void;
  busy: boolean;
}) {
  const [confirming, setConfirming] = useState(false);
  return (
    <div
      className={`rounded-lg border p-4 ${entry.enabled ? "bg-card" : "bg-muted/40 opacity-70"}`}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            {entry.title && (
              <span className="font-medium text-foreground">{entry.title}</span>
            )}
            <span className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
              priority {entry.priority}
            </span>
            {entry.category && (
              <span className="rounded bg-primary/10 px-1.5 py-0.5 text-xs text-primary">
                {entry.category}
              </span>
            )}
          </div>
          <p className="mt-2 whitespace-pre-wrap font-mono text-sm text-foreground">
            {entry.content}
          </p>
          {entry.tags?.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1">
              {entry.tags.map((t) => (
                <span key={t} className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
                  {t}
                </span>
              ))}
            </div>
          )}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Switch checked={entry.enabled} onChange={onToggle} disabled={busy} />
          <button
            type="button"
            className="rounded p-2 text-muted-foreground hover:bg-muted hover:text-foreground"
            onClick={onEdit}
            aria-label="Edit"
          >
            <FontAwesomeIcon icon={faPen} />
          </button>
          {confirming ? (
            <span className="flex items-center gap-1">
              <button
                type="button"
                className="rounded bg-destructive px-2 py-1 text-xs text-destructive-foreground hover:bg-destructive/90"
                onClick={onDelete}
                disabled={busy}
              >
                Confirm
              </button>
              <button
                type="button"
                className="rounded border px-2 py-1 text-xs hover:bg-muted"
                onClick={() => setConfirming(false)}
              >
                No
              </button>
            </span>
          ) : (
            <button
              type="button"
              className="rounded p-2 text-muted-foreground hover:bg-muted hover:text-destructive"
              onClick={() => setConfirming(true)}
              aria-label="Delete"
            >
              <FontAwesomeIcon icon={faTrash} />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

export default function ProceduralMemory() {
  const { data: entries, isLoading } = useProcedural();
  const createMut = useCreateProcedural();
  const updateMut = useUpdateProcedural();
  const deleteMut = useDeleteProcedural();

  const [creating, setCreating] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);

  const list = entries ?? [];

  return (
    <div className="mx-auto max-w-4xl">
      <PageHeader
        icon={faListCheck}
        title="Procedural Memory"
        subtitle="Standing rules for your agents. Stored exactly as written and returned whole by procedural_fetch — never summarized, embedded, or surfaced by recall. How they're applied is up to the agent."
        actions={
          !creating && (
            <button type="button" className={primaryBtn} onClick={() => setCreating(true)}>
              <FontAwesomeIcon icon={faPlus} /> New Entry
            </button>
          )
        }
      />

      {creating && (
        <div className="mb-6">
          <EntryForm
            initial={emptyDraft()}
            submitting={createMut.isPending}
            submitLabel="Create entry"
            onCancel={() => setCreating(false)}
            onSubmit={(d) =>
              createMut.mutate(toRequest(d), { onSuccess: () => setCreating(false) })
            }
          />
        </div>
      )}

      {isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : list.length === 0 && !creating ? (
        <EmptyState
          icon={faListCheck}
          title="No procedural rules yet"
          body="Add standing instructions for your agents. procedural_fetch returns them verbatim and unranked; nram never summarizes or reorders the wording."
          action={
            <button type="button" className={primaryBtn} onClick={() => setCreating(true)}>
              <FontAwesomeIcon icon={faPlus} /> New Entry
            </button>
          }
        />
      ) : (
        <div className="space-y-3">
          {list.map((entry) =>
            editingId === entry.id ? (
              <EntryForm
                key={entry.id}
                initial={draftFromEntry(entry)}
                submitting={updateMut.isPending}
                submitLabel="Save changes"
                onCancel={() => setEditingId(null)}
                onSubmit={(d) =>
                  updateMut.mutate(
                    { id: entry.id, data: toRequest(d) },
                    { onSuccess: () => setEditingId(null) },
                  )
                }
              />
            ) : (
              <EntryRow
                key={entry.id}
                entry={entry}
                busy={updateMut.isPending || deleteMut.isPending}
                onEdit={() => setEditingId(entry.id)}
                onToggle={(v) => updateMut.mutate({ id: entry.id, data: { enabled: v } })}
                onDelete={() => deleteMut.mutate(entry.id)}
              />
            ),
          )}
        </div>
      )}
    </div>
  );
}
