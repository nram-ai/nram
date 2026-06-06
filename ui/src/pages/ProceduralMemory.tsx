import { useMemo, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faListCheck,
  faPlus,
  faPen,
  faTrash,
  faXmark,
  faDownload,
  faUpload,
  faMagnifyingGlass,
  faSort,
  faSortUp,
  faSortDown,
} from "@fortawesome/free-solid-svg-icons";
import { PageHeader } from "../components/PageHeader";
import { EmptyState } from "../components/EmptyState";
import Switch from "../components/Switch";
import Drawer from "../components/Drawer";
import { meAPI } from "../api/client";
import {
  useProcedural,
  useCreateProcedural,
  useUpdateProcedural,
  useDeleteProcedural,
  useImportProcedural,
} from "../hooks/useApi";
import type { ProceduralEntry, CreateProceduralRequest } from "../api/client";

const inputClass =
  "w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring";
const primaryBtn =
  "inline-flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50";
const ghostBtn =
  "inline-flex items-center gap-2 rounded-md border px-3 py-2 text-sm font-medium hover:bg-muted disabled:opacity-50";

function relativeTime(timestamp: string): string {
  const now = Date.now();
  const then = new Date(timestamp).getTime();
  const diffSec = Math.floor((now - then) / 1000);
  if (diffSec < 0) return "just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin} min ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDay = Math.floor(diffHr / 24);
  if (diffDay === 1) return "yesterday";
  if (diffDay < 30) return `${diffDay}d ago`;
  return new Date(timestamp).toLocaleDateString();
}

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
    <div className="space-y-4">
      <div>
        <label className="mb-1 block text-sm font-medium text-muted-foreground">
          Rule (stored verbatim)
        </label>
        <textarea
          value={draft.content}
          onChange={(e) => setDraft({ ...draft, content: e.target.value })}
          rows={8}
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
            : "Excluded from fetch, kept for reference"}
        </span>
      </div>
      <div className="flex items-center justify-end gap-2 pt-2">
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

type SortField = "priority" | "title" | "category" | "enabled" | "updated_at";
type SortDir = "asc" | "desc";

function StatusToast({ message, type }: { message: string; type: "success" | "error" }) {
  return (
    <div
      className={`fixed bottom-6 right-6 z-[60] max-w-md rounded-md px-4 py-3 text-sm shadow-lg ${
        type === "success"
          ? "bg-primary text-primary-foreground"
          : "bg-destructive text-destructive-foreground"
      }`}
    >
      {message}
    </div>
  );
}

function SortableTh({
  label,
  field,
  sortField,
  sortDir,
  onSort,
  className,
}: {
  label: string;
  field: SortField;
  sortField: SortField;
  sortDir: SortDir;
  onSort: (f: SortField) => void;
  className?: string;
}) {
  const active = sortField === field;
  const icon = !active ? faSort : sortDir === "asc" ? faSortUp : faSortDown;
  return (
    <th className={`px-3 py-2.5 text-left ${className ?? ""}`}>
      <button
        type="button"
        onClick={() => onSort(field)}
        className={`inline-flex items-center gap-1.5 text-xs font-medium ${
          active ? "text-foreground" : "text-muted-foreground hover:text-foreground"
        }`}
      >
        {label}
        <FontAwesomeIcon icon={icon} className="h-3 w-3" />
      </button>
    </th>
  );
}

export default function ProceduralMemory() {
  const { data: entries, isLoading } = useProcedural();
  const createMut = useCreateProcedural();
  const updateMut = useUpdateProcedural();
  const deleteMut = useDeleteProcedural();
  const importMut = useImportProcedural();

  const [search, setSearch] = useState("");
  const [sortField, setSortField] = useState<SortField>("priority");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [drawer, setDrawer] = useState<{ type: "create" } | { type: "edit"; entry: ProceduralEntry } | null>(
    null,
  );
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);
  const [toast, setToast] = useState<{ message: string; type: "success" | "error" } | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  const showToast = (message: string, type: "success" | "error") => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 4000);
  };

  const list = entries ?? [];

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    let rows = list;
    if (q) {
      rows = rows.filter(
        (e) =>
          (e.title ?? "").toLowerCase().includes(q) ||
          (e.content ?? "").toLowerCase().includes(q) ||
          (e.category ?? "").toLowerCase().includes(q) ||
          (e.tags ?? []).some((t) => t.toLowerCase().includes(q)),
      );
    }
    const sorted = [...rows].sort((a, b) => {
      switch (sortField) {
        case "title":
          return (a.title ?? "").localeCompare(b.title ?? "");
        case "category":
          return (a.category ?? "").localeCompare(b.category ?? "");
        case "enabled":
          return Number(a.enabled) - Number(b.enabled);
        case "updated_at":
          return new Date(a.updated_at).getTime() - new Date(b.updated_at).getTime();
        case "priority":
        default:
          return a.priority - b.priority;
      }
    });
    if (sortDir === "desc") sorted.reverse();
    return sorted;
  }, [list, search, sortField, sortDir]);

  const handleSort = (field: SortField) => {
    if (field === sortField) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir(field === "title" || field === "category" ? "asc" : "desc");
    }
  };

  // Binds the current sort state to a sortable header so each column only names
  // its label, field, and width.
  const renderTh = (label: string, field: SortField, className?: string) => (
    <SortableTh
      label={label}
      field={field}
      sortField={sortField}
      sortDir={sortDir}
      onSort={handleSort}
      className={className}
    />
  );

  const handleExport = async () => {
    setExporting(true);
    try {
      const data = await meAPI.exportProcedural();
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `procedural-memory-${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      showToast(`Export failed: ${err instanceof Error ? err.message : "unknown error"}`, "error");
    } finally {
      setExporting(false);
    }
  };

  const handleImportFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const result = await importMut.mutateAsync(parsed);
      const summary = `Imported ${result.imported}, updated ${result.updated}, skipped ${result.skipped}`;
      const detail = result.errors.length ? ` — ${result.errors[0].message}` : "";
      showToast(summary + detail, result.skipped > 0 ? "error" : "success");
    } catch (err) {
      showToast(
        `Import failed: ${err instanceof Error ? err.message : "invalid JSON file"}`,
        "error",
      );
    } finally {
      if (fileRef.current) fileRef.current.value = "";
    }
  };

  const busy = updateMut.isPending || deleteMut.isPending;

  return (
    <div className="mx-auto max-w-6xl">
      <PageHeader
        icon={faListCheck}
        title="Procedural Memory"
        subtitle="Standing rules for your agents. Stored exactly as written and returned whole by procedural_fetch, never summarized, embedded, or surfaced by recall. How they're applied is up to the agent."
        actions={
          <>
            <input
              ref={fileRef}
              type="file"
              accept="application/json,.json"
              className="hidden"
              onChange={handleImportFile}
            />
            <button
              type="button"
              className={ghostBtn}
              onClick={() => fileRef.current?.click()}
              disabled={importMut.isPending}
            >
              <FontAwesomeIcon icon={faUpload} /> Import
            </button>
            <button
              type="button"
              className={ghostBtn}
              onClick={handleExport}
              disabled={exporting || list.length === 0}
            >
              <FontAwesomeIcon icon={faDownload} /> Export
            </button>
            <button type="button" className={primaryBtn} onClick={() => setDrawer({ type: "create" })}>
              <FontAwesomeIcon icon={faPlus} /> New Entry
            </button>
          </>
        }
      />

      {isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : list.length === 0 ? (
        <EmptyState
          icon={faListCheck}
          title="No procedural rules yet"
          body="Add standing instructions for your agents. procedural_fetch returns them verbatim and unranked; nram never summarizes or reorders the wording."
          action={
            <button type="button" className={primaryBtn} onClick={() => setDrawer({ type: "create" })}>
              <FontAwesomeIcon icon={faPlus} /> New Entry
            </button>
          }
        />
      ) : (
        <>
          <div className="mb-3 flex items-center justify-between gap-3">
            <div className="relative w-full max-w-sm">
              <FontAwesomeIcon
                icon={faMagnifyingGlass}
                className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
              />
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search title, content, category, tags…"
                className={`${inputClass} pl-9`}
              />
            </div>
            <span className="shrink-0 text-xs text-muted-foreground tabular-nums">
              {filtered.length} of {list.length}
            </span>
          </div>

          <div className="overflow-x-auto rounded-lg border bg-card shadow-sm">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50">
                  {renderTh("On", "enabled", "w-12")}
                  {renderTh("Priority", "priority", "w-24")}
                  {renderTh("Title", "title")}
                  {renderTh("Category", "category")}
                  <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
                    Tags
                  </th>
                  <th className="px-3 py-2.5 text-left text-xs font-medium text-muted-foreground">
                    Content
                  </th>
                  {renderTh("Updated", "updated_at", "w-28")}
                  <th className="px-3 py-2.5 text-right text-xs font-medium text-muted-foreground">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((entry) => {
                  const confirming = confirmingId === entry.id;
                  return (
                    <tr
                      key={entry.id}
                      className={`border-b last:border-0 hover:bg-muted/30 ${
                        entry.enabled ? "" : "opacity-60"
                      }`}
                    >
                      <td className="px-3 py-2.5">
                        <Switch
                          checked={entry.enabled}
                          onChange={(v) =>
                            updateMut.mutate({ id: entry.id, data: { enabled: v } })
                          }
                          disabled={busy}
                        />
                      </td>
                      <td className="px-3 py-2.5 tabular-nums text-muted-foreground">
                        {entry.priority}
                      </td>
                      <td className="px-3 py-2.5 font-medium text-foreground">
                        {entry.title || <span className="text-muted-foreground">—</span>}
                      </td>
                      <td className="px-3 py-2.5">
                        {entry.category ? (
                          <span className="rounded bg-primary/10 px-1.5 py-0.5 text-xs text-primary">
                            {entry.category}
                          </span>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5">
                        {entry.tags?.length ? (
                          <div className="flex max-w-[14rem] flex-wrap gap-1">
                            {entry.tags.map((t) => (
                              <span
                                key={t}
                                className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground"
                              >
                                {t}
                              </span>
                            ))}
                          </div>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5">
                        <span
                          className="block max-w-[24rem] truncate font-mono text-xs text-muted-foreground"
                          title={entry.content}
                        >
                          {entry.content}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 text-xs text-muted-foreground">
                        {relativeTime(entry.updated_at)}
                      </td>
                      <td className="px-3 py-2.5">
                        <div className="flex items-center justify-end gap-1">
                          <button
                            type="button"
                            className="rounded p-2 text-muted-foreground hover:bg-muted hover:text-foreground"
                            onClick={() => setDrawer({ type: "edit", entry })}
                            aria-label="Edit"
                          >
                            <FontAwesomeIcon icon={faPen} />
                          </button>
                          {confirming ? (
                            <span className="flex items-center gap-1">
                              <button
                                type="button"
                                className="rounded bg-destructive px-2 py-1 text-xs text-destructive-foreground hover:bg-destructive/90"
                                onClick={() =>
                                  deleteMut.mutate(entry.id, {
                                    onSettled: () => setConfirmingId(null),
                                  })
                                }
                                disabled={busy}
                              >
                                Confirm
                              </button>
                              <button
                                type="button"
                                className="rounded border px-2 py-1 text-xs hover:bg-muted"
                                onClick={() => setConfirmingId(null)}
                              >
                                No
                              </button>
                            </span>
                          ) : (
                            <button
                              type="button"
                              className="rounded p-2 text-muted-foreground hover:bg-muted hover:text-destructive"
                              onClick={() => setConfirmingId(entry.id)}
                              aria-label="Delete"
                            >
                              <FontAwesomeIcon icon={faTrash} />
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}

      <Drawer
        open={drawer !== null}
        title={drawer?.type === "edit" ? "Edit rule" : "New rule"}
        onClose={() => setDrawer(null)}
      >
        {drawer?.type === "create" && (
          <EntryForm
            initial={emptyDraft()}
            submitting={createMut.isPending}
            submitLabel="Create entry"
            onCancel={() => setDrawer(null)}
            onSubmit={(d) =>
              createMut.mutate(toRequest(d), { onSuccess: () => setDrawer(null) })
            }
          />
        )}
        {drawer?.type === "edit" && (
          <EntryForm
            key={drawer.entry.id}
            initial={draftFromEntry(drawer.entry)}
            submitting={updateMut.isPending}
            submitLabel="Save changes"
            onCancel={() => setDrawer(null)}
            onSubmit={(d) =>
              updateMut.mutate(
                { id: drawer.entry.id, data: toRequest(d) },
                { onSuccess: () => setDrawer(null) },
              )
            }
          />
        )}
      </Drawer>

      {toast && <StatusToast message={toast.message} type={toast.type} />}
    </div>
  );
}
