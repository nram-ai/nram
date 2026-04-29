import { useState, useMemo, useEffect, useRef } from "react";
import {
  useMeProjects,
  useCreateMeProject,
  useProject,
  useUpdateProject,
  useDeleteProject,
  useSystemRankingWeights,
} from "../hooks/useApi";
import { useAuth } from "../context/AuthContext";
import type {
  Project,
  ProjectUpdateRequest,
  SystemRankingWeights,
} from "../api/client";
import { buildProjectSettingsPayload } from "./settingsPayload";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEBOUNCE_MS = 300;

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

function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(id);
  }, [value, delay]);
  return debounced;
}

// ---------------------------------------------------------------------------
// Tag colors
// ---------------------------------------------------------------------------

const TAG_COLORS = [
  "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
  "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
  "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300",
  "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300",
  "bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-300",
  "bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-300",
  "bg-teal-100 text-teal-800 dark:bg-teal-900 dark:text-teal-300",
  "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300",
];

function tagColor(tag: string): string {
  let hash = 0;
  for (let i = 0; i < tag.length; i++) {
    hash = (hash * 31 + tag.charCodeAt(i)) | 0;
  }
  return TAG_COLORS[Math.abs(hash) % TAG_COLORS.length];
}

// ---------------------------------------------------------------------------
// Skeleton rows
// ---------------------------------------------------------------------------

function SkeletonRow() {
  return (
    <tr className="animate-pulse">
      <td className="px-4 py-3"><div className="h-4 w-32 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-48 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-12 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-12 rounded bg-muted" /></td>
      <td className="px-4 py-3"><div className="h-4 w-28 rounded bg-muted" /></td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Sort helpers
// ---------------------------------------------------------------------------

type SortField = "name" | "path" | "memory_count" | "entity_count" | "created_at";
type SortDir = "asc" | "desc";

function compareProjects(a: Project, b: Project, field: SortField, dir: SortDir): number {
  let cmp = 0;
  switch (field) {
    case "name":
      cmp = (a.name || a.slug).localeCompare(b.name || b.slug);
      break;
    case "path":
      cmp = (a.path || "").localeCompare(b.path || "");
      break;
    case "memory_count":
      cmp = (a.memory_count ?? 0) - (b.memory_count ?? 0);
      break;
    case "entity_count":
      cmp = (a.entity_count ?? 0) - (b.entity_count ?? 0);
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
// TagChip
// ---------------------------------------------------------------------------

function TagChip({
  tag,
  onRemove,
}: {
  tag: string;
  onRemove?: () => void;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${tagColor(tag)}`}
    >
      {tag}
      {onRemove && (
        <button
          type="button"
          className="ml-0.5 hover:opacity-70"
          onClick={(e) => {
            e.stopPropagation();
            onRemove();
          }}
          title={`Remove tag "${tag}"`}
        >
          x
        </button>
      )}
    </span>
  );
}

// ---------------------------------------------------------------------------
// AddTagInput
// ---------------------------------------------------------------------------

function AddTagInput({ onAdd }: { onAdd: (tag: string) => void }) {
  const [value, setValue] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  function submit() {
    const trimmed = value.trim();
    if (trimmed) {
      onAdd(trimmed);
      setValue("");
    }
  }

  return (
    <span className="inline-flex items-center gap-1">
      <input
        ref={inputRef}
        type="text"
        className="w-24 rounded border bg-background px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
        placeholder="add tag"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            submit();
          }
        }}
      />
      <button
        type="button"
        className="rounded bg-primary px-1.5 py-0.5 text-xs text-primary-foreground hover:bg-primary/90"
        onClick={submit}
      >
        +
      </button>
    </span>
  );
}

// ---------------------------------------------------------------------------
// MCP Config Snippet
// ---------------------------------------------------------------------------

function MCPConfigSnippet({ slug }: { slug: string }) {
  const [copied, setCopied] = useState(false);

  const config = JSON.stringify(
    {
      mcpServers: {
        nram: {
          url: `${window.location.origin}/mcp`,
          headers: {
            Authorization: "Bearer <your-api-key>",
            "X-Project": slug,
          },
        },
      },
    },
    null,
    2,
  );

  function handleCopy() {
    navigator.clipboard.writeText(config).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-sm font-medium text-muted-foreground">
          MCP Config Snippet
        </h3>
        <button
          type="button"
          className="rounded border px-2 py-1 text-xs hover:bg-muted"
          onClick={handleCopy}
        >
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre className="overflow-auto rounded border bg-muted/50 p-3 text-xs font-mono">
        {config}
      </pre>
    </div>
  );
}

// SparseWeightInput renders a numeric input where empty = "inherit system
// default". The placeholder shows the effective system value so operators
// see the baseline before typing. Returning undefined to onChange is the
// signal that the operator wants to clear the override.
function SparseWeightInput({
  label,
  value,
  placeholder,
  onChange,
}: {
  label: string;
  value: number | undefined;
  placeholder: number;
  onChange: (next: number | undefined) => void;
}) {
  return (
    <div>
      <label className="mb-1 block text-xs text-muted-foreground">{label}</label>
      <input
        type="number"
        min={0}
        max={1}
        step={0.05}
        className="w-full rounded-md border bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
        value={value ?? ""}
        placeholder={`system: ${placeholder.toFixed(2)}`}
        onChange={(e) => {
          const v = e.target.value;
          if (v === "") {
            onChange(undefined);
            return;
          }
          const n = parseFloat(v);
          onChange(Number.isFinite(n) ? n : undefined);
        }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Project Detail Panel
// ---------------------------------------------------------------------------

function ProjectDetailPanel({
  projectId,
  onClose,
  onDeleted,
}: {
  projectId: string;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const detailQuery = useProject(projectId);
  const updateMut = useUpdateProject();
  const deleteMut = useDeleteProject();

  const systemWeights = useSystemRankingWeights();

  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editTags, setEditTags] = useState<string[]>([]);
  // Sparse override state: undefined means "inherit system default."
  const [editDedupThreshold, setEditDedupThreshold] = useState<number | undefined>(undefined);
  const [editEnrichmentEnabled, setEditEnrichmentEnabled] = useState<boolean | undefined>(undefined);
  const [editSimilarity, setEditSimilarity] = useState<number | undefined>(undefined);
  const [editRecency, setEditRecency] = useState<number | undefined>(undefined);
  const [editImportance, setEditImportance] = useState<number | undefined>(undefined);
  const [editFrequency, setEditFrequency] = useState<number | undefined>(undefined);
  const [editGraphRelevance, setEditGraphRelevance] = useState<number | undefined>(undefined);
  const [editConfidence, setEditConfidence] = useState<number | undefined>(undefined);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [initialized, setInitialized] = useState(false);

  const project = detailQuery.data;

  // Initialize form when project loads. Migration 000025/000022 rewrites
  // legacy `relevance` to `similarity`, but we read it with a fallback so
  // pre-migration UIs render correctly.
  useEffect(() => {
    if (project && !initialized) {
      setEditName(project.name || "");
      setEditDescription(project.description || "");
      setEditTags(project.default_tags ?? []);
      const settings = project.settings;
      setEditDedupThreshold(settings?.dedup_threshold);
      setEditEnrichmentEnabled(settings?.enrichment_enabled);
      const rw = settings?.ranking_weights;
      setEditSimilarity(rw?.similarity ?? rw?.relevance);
      setEditRecency(rw?.recency);
      setEditImportance(rw?.importance);
      setEditFrequency(rw?.frequency);
      setEditGraphRelevance(rw?.graph_relevance);
      setEditConfidence(rw?.confidence);
      setInitialized(true);
    }
  }, [project, initialized]);

  // Reset initialized when projectId changes
  useEffect(() => {
    setInitialized(false);
    setConfirmDelete(false);
    setSaveSuccess(false);
  }, [projectId]);

  // Effective merged weights = system baseline with any project override
  // applied. Used for the read-only summary block and the sum warning.
  const effectiveWeights: SystemRankingWeights = {
    similarity: editSimilarity ?? systemWeights.similarity,
    recency: editRecency ?? systemWeights.recency,
    importance: editImportance ?? systemWeights.importance,
    frequency: editFrequency ?? systemWeights.frequency,
    graph_relevance: editGraphRelevance ?? systemWeights.graph_relevance,
    confidence: editConfidence ?? systemWeights.confidence,
  };
  const weightSum =
    effectiveWeights.similarity +
    effectiveWeights.recency +
    effectiveWeights.importance +
    effectiveWeights.frequency +
    effectiveWeights.graph_relevance +
    effectiveWeights.confidence;
  const weightWarning = Math.abs(weightSum - 1.0) > 0.001;

  function handleSave() {
    if (!project) return;
    // Build sparse payload: only include fields the operator actually set.
    // Empty inputs roundtrip back to "inherit system default" — the cascade
    // resolver picks them up from the system layer at recall time.
    const settings = buildProjectSettingsPayload({
      similarity: editSimilarity,
      recency: editRecency,
      importance: editImportance,
      frequency: editFrequency,
      graph_relevance: editGraphRelevance,
      confidence: editConfidence,
      dedup_threshold: editDedupThreshold,
      enrichment_enabled: editEnrichmentEnabled,
    });
    const data: ProjectUpdateRequest = {
      name: editName,
      description: editDescription,
      default_tags: editTags,
      settings,
    };
    updateMut.mutate(
      { id: project.id, data },
      {
        onSuccess: () => {
          setSaveSuccess(true);
          setTimeout(() => setSaveSuccess(false), 2000);
        },
      },
    );
  }

  function handleDelete() {
    if (!project) return;
    deleteMut.mutate(project.id, {
      onSuccess: () => {
        onDeleted();
        onClose();
      },
    });
  }

  function handleRemoveTag(tag: string) {
    setEditTags((prev) => prev.filter((t) => t !== tag));
  }

  function handleAddTag(tag: string) {
    if (!editTags.includes(tag)) {
      setEditTags((prev) => [...prev, tag]);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      {/* Panel */}
      <div className="relative z-10 flex h-full w-full max-w-2xl flex-col overflow-y-auto border-l bg-background shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b px-6 py-4">
          <h2 className="text-lg font-semibold">Project Detail</h2>
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
              Failed to load project: {detailQuery.error?.message}
            </p>
          </div>
        )}

        {project && initialized && (
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

            {/* Description (editable) */}
            <div>
              <label className="mb-1 block text-sm font-medium text-muted-foreground">
                Description
              </label>
              <textarea
                className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                rows={3}
                value={editDescription}
                onChange={(e) => setEditDescription(e.target.value)}
                placeholder="Project description..."
              />
            </div>

            {/* Read-only fields */}
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Slug: </span>
                <span className="font-mono">{project.slug}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Path: </span>
                <span className="font-mono">{project.path || "N/A"}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Owner: </span>
                <span>{project.owner?.email || "N/A"}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Organization: </span>
                <span>{project.organization?.name || "N/A"}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Memories: </span>
                <span className="font-semibold">{project.memory_count ?? 0}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Entities: </span>
                <span className="font-semibold">{project.entity_count ?? 0}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Created: </span>
                <span>{formatDate(project.created_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Updated: </span>
                <span>{formatDate(project.updated_at)}</span>
              </div>
            </div>

            {/* Default Tags */}
            <div>
              <label className="mb-2 block text-sm font-medium text-muted-foreground">
                Default Tags
              </label>
              <div className="flex flex-wrap items-center gap-2">
                {editTags.map((tag) => (
                  <TagChip
                    key={tag}
                    tag={tag}
                    onRemove={() => handleRemoveTag(tag)}
                  />
                ))}
                <AddTagInput onAdd={handleAddTag} />
              </div>
            </div>

            {/* Settings Overrides — sparse: leave a field empty to inherit
             * the system-level setting. Numeric inputs show the current
             * effective system default as placeholder text so operators see
             * what the cascade will resolve to without typing anything. */}
            <div className="space-y-4 rounded-lg border p-4">
              <h3 className="text-sm font-semibold">Settings Overrides</h3>
              <p className="text-xs text-muted-foreground">
                Empty fields fall through to the system-level setting. Set a
                value here to override only this project.
              </p>

              {/* Dedup threshold */}
              <div>
                <label className="mb-1 block text-xs font-medium text-muted-foreground">
                  Dedup Threshold (0-1)
                </label>
                <input
                  type="number"
                  min={0}
                  max={1}
                  step={0.01}
                  className="w-32 rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                  value={editDedupThreshold ?? ""}
                  placeholder="system: 0.92"
                  onChange={(e) => {
                    const v = e.target.value;
                    setEditDedupThreshold(v === "" ? undefined : parseFloat(v));
                  }}
                />
              </div>

              {/* Enrichment enabled — three-state: inherit / on / off */}
              <div>
                <label className="mb-1 block text-xs font-medium text-muted-foreground">
                  Enrichment Enabled
                </label>
                <select
                  className="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                  value={
                    editEnrichmentEnabled === undefined
                      ? "inherit"
                      : editEnrichmentEnabled
                      ? "on"
                      : "off"
                  }
                  onChange={(e) => {
                    const v = e.target.value;
                    setEditEnrichmentEnabled(
                      v === "inherit" ? undefined : v === "on",
                    );
                  }}
                >
                  <option value="inherit">Inherit system</option>
                  <option value="on">On</option>
                  <option value="off">Off</option>
                </select>
              </div>

              {/* Ranking weights — six sparse inputs. Each placeholder shows
               * the effective system value so operators can see the
               * baseline before deciding whether to override. */}
              <div>
                <label className="mb-2 block text-xs font-medium text-muted-foreground">
                  Ranking Weights
                </label>
                <div className="grid grid-cols-3 gap-3">
                  <SparseWeightInput
                    label="Similarity"
                    value={editSimilarity}
                    placeholder={systemWeights.similarity}
                    onChange={setEditSimilarity}
                  />
                  <SparseWeightInput
                    label="Recency"
                    value={editRecency}
                    placeholder={systemWeights.recency}
                    onChange={setEditRecency}
                  />
                  <SparseWeightInput
                    label="Importance"
                    value={editImportance}
                    placeholder={systemWeights.importance}
                    onChange={setEditImportance}
                  />
                  <SparseWeightInput
                    label="Frequency"
                    value={editFrequency}
                    placeholder={systemWeights.frequency}
                    onChange={setEditFrequency}
                  />
                  <SparseWeightInput
                    label="Graph Relevance"
                    value={editGraphRelevance}
                    placeholder={systemWeights.graph_relevance}
                    onChange={setEditGraphRelevance}
                  />
                  <SparseWeightInput
                    label="Confidence"
                    value={editConfidence}
                    placeholder={systemWeights.confidence}
                    onChange={setEditConfidence}
                  />
                </div>

                {/* Effective weights — read-only summary so operators see
                 * exactly what recall will use after merging the override
                 * with the system layer. */}
                <div className="mt-3 rounded border bg-muted/30 p-3 text-xs">
                  <div className="mb-1 font-medium text-muted-foreground">
                    Effective weights (system + your overrides)
                  </div>
                  <div className="grid grid-cols-3 gap-2 font-mono">
                    <div>Similarity: {effectiveWeights.similarity.toFixed(2)}</div>
                    <div>Recency: {effectiveWeights.recency.toFixed(2)}</div>
                    <div>Importance: {effectiveWeights.importance.toFixed(2)}</div>
                    <div>Frequency: {effectiveWeights.frequency.toFixed(2)}</div>
                    <div>Graph: {effectiveWeights.graph_relevance.toFixed(2)}</div>
                    <div>Confidence: {effectiveWeights.confidence.toFixed(2)}</div>
                  </div>
                  <div className="mt-2 text-muted-foreground">
                    Sum: <span className="font-mono">{weightSum.toFixed(3)}</span>
                  </div>
                </div>
                {weightWarning && (
                  <p className="mt-1 text-xs text-amber-600 dark:text-amber-400">
                    Effective weights do not sum to 1.0 (currently {weightSum.toFixed(3)}).
                    Recall will still work, but the score scale shifts; review the override.
                  </p>
                )}
              </div>
            </div>

            {/* MCP Config Snippet */}
            <MCPConfigSnippet slug={project.slug} />

            {/* Actions */}
            <div className="flex items-center gap-3 border-t pt-4">
              <button
                type="button"
                className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
                onClick={handleSave}
                disabled={updateMut.isPending}
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
                    This will permanently delete all memories, vectors, entities, and relationships in this project. This action cannot be undone. Continue?
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
                  Delete Project
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
// Main ProjectManagement
// ---------------------------------------------------------------------------

function CreateMeProjectDialog({ onClose }: { onClose: () => void }) {
  const createMut = useCreateMeProject();
  const [name, setName] = useState("");
  const [slug, setSlug] = useState("");
  const [slugEdited, setSlugEdited] = useState(false);

  useEffect(() => {
    if (!slugEdited) {
      setSlug(
        name
          .toLowerCase()
          .replace(/[^a-z0-9\s-]/g, "")
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-")
          .replace(/^-|-$/g, ""),
      );
    }
  }, [name, slugEdited]);

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
        <h2 className="text-lg font-semibold">Create Project</h2>
        <div className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">Name</label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Project name"
              autoFocus
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-muted-foreground">Slug</label>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              value={slug}
              onChange={(e) => {
                setSlugEdited(true);
                setSlug(e.target.value);
              }}
              placeholder="project-slug"
            />
          </div>
          {createMut.isError && (
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed: {createMut.error?.message}
            </p>
          )}
          <div className="flex items-center justify-end gap-3 pt-2">
            <button type="button" className="rounded border px-3 py-2 text-sm hover:bg-muted" onClick={onClose}>
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

function ProjectManagement() {
  const auth = useAuth();
  const projectsQuery = useMeProjects();
  const projects = projectsQuery.data ?? [];

  const [searchText, setSearchText] = useState("");
  const debouncedSearch = useDebounce(searchText, DEBOUNCE_MS);
  const [sortField, setSortField] = useState<SortField>("memory_count");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [detailProjectId, setDetailProjectId] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);

  function handleSort(field: SortField) {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  }

  const filteredProjects = useMemo(() => {
    let result = [...projects];

    // Filter by search text
    if (debouncedSearch) {
      const lower = debouncedSearch.toLowerCase();
      result = result.filter(
        (p) =>
          (p.name || "").toLowerCase().includes(lower) ||
          (p.slug || "").toLowerCase().includes(lower) ||
          (p.path || "").toLowerCase().includes(lower),
      );
    }

    // Sort
    result.sort((a, b) => compareProjects(a, b, sortField, sortDir));

    return result;
  }, [projects, debouncedSearch, sortField, sortDir]);

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="space-y-4 pb-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Project Management
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Manage projects and their configurations. Projects are created automatically when AI tools store memories.
          </p>
        </div>

        {/* Search + Create */}
        <div className="flex items-center gap-3">
          <div className="relative flex-1 max-w-md">
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 pr-8 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
              placeholder="Search projects by name, slug, or path..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
            />
            {searchText && (
              <button
                type="button"
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                onClick={() => setSearchText("")}
              >
                x
              </button>
            )}
          </div>
          <span className="text-sm text-muted-foreground">
            {filteredProjects.length} project{filteredProjects.length !== 1 ? "s" : ""}
          </span>
          {!auth.isAdmin && auth.canWrite && (
            <button
              type="button"
              className="rounded bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              onClick={() => setShowCreate(true)}
            >
              Create Project
            </button>
          )}
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto rounded-lg border">
        {projectsQuery.isError && (
          <div className="m-4 rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300">
            Failed to load projects: {projectsQuery.error?.message ?? "Unknown error"}
          </div>
        )}

        <table className="w-full">
          <thead className="sticky top-0 border-b bg-muted/50">
            <tr>
              <SortableHeader
                label="Name / Slug"
                field="name"
                currentField={sortField}
                currentDir={sortDir}
                onSort={handleSort}
              />
              <SortableHeader
                label="Path"
                field="path"
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
                label="Entities"
                field="entity_count"
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
            {projectsQuery.isLoading ? (
              <>
                <SkeletonRow />
                <SkeletonRow />
                <SkeletonRow />
                <SkeletonRow />
                <SkeletonRow />
              </>
            ) : filteredProjects.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-4 py-12 text-center">
                  <p className="text-sm text-muted-foreground">
                    {debouncedSearch
                      ? "No projects match your search."
                      : "No projects yet. Projects are created automatically when AI tools store memories."}
                  </p>
                </td>
              </tr>
            ) : (
              filteredProjects.map((p) => (
                <tr
                  key={p.id}
                  className="cursor-pointer transition-colors hover:bg-accent/50"
                  onClick={() => setDetailProjectId(p.id)}
                >
                  <td className="px-4 py-3">
                    <div className="text-sm font-medium">{p.name || p.slug}</div>
                    {p.name && p.name !== p.slug && (
                      <div className="text-xs font-mono text-muted-foreground">
                        {p.slug}
                      </div>
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm font-mono text-muted-foreground">
                    {p.path || "N/A"}
                  </td>
                  <td className="px-4 py-3 text-sm font-semibold">
                    {p.memory_count ?? 0}
                  </td>
                  <td className="px-4 py-3 text-sm font-semibold">
                    {p.entity_count ?? 0}
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">
                    {formatDate(p.created_at)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Detail Panel — writable users get full edit/delete, readonly gets read-only view */}
      {detailProjectId && auth.canWrite && (
        <ProjectDetailPanel
          projectId={detailProjectId}
          onClose={() => setDetailProjectId(null)}
          onDeleted={() => setDetailProjectId(null)}
        />
      )}

      {/* Readonly users: read-only detail view */}
      {detailProjectId && !auth.canWrite && (() => {
        const proj = projects.find((p) => p.id === detailProjectId);
        return proj ? (
          <ProjectReadOnlyPanel
            project={proj}
            canWrite={false}
            onClose={() => setDetailProjectId(null)}
            onDeleted={() => setDetailProjectId(null)}
          />
        ) : null;
      })()}

      {/* Create dialog for non-admin */}
      {showCreate && <CreateMeProjectDialog onClose={() => setShowCreate(false)} />}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Read-only project detail panel for non-admin users
// ---------------------------------------------------------------------------

function ProjectReadOnlyPanel({
  project,
  canWrite,
  onClose,
  onDeleted,
}: {
  project: Project;
  canWrite: boolean;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const deleteMut = useDeleteProject();
  const [confirmDelete, setConfirmDelete] = useState(false);

  function handleDelete() {
    deleteMut.mutate(project.id, {
      onSuccess: () => {
        onDeleted();
        onClose();
      },
    });
  }

  const canDelete = canWrite && project.slug !== "global";

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/30" onClick={onClose} />
      <div className="relative z-10 flex h-full w-full max-w-2xl flex-col overflow-y-auto border-l bg-background shadow-xl">
        <div className="flex items-center justify-between border-b px-6 py-4">
          <h2 className="text-lg font-semibold">Project Detail</h2>
          <button
            type="button"
            className="rounded border px-3 py-1 text-sm hover:bg-muted"
            onClick={onClose}
          >
            Close
          </button>
        </div>

        {project && (
          <div className="flex-1 space-y-6 p-6">
            <div>
              <span className="text-sm font-medium text-muted-foreground">Name</span>
              <p className="mt-0.5 text-sm">{project.name || project.slug}</p>
            </div>
            <div>
              <span className="text-sm font-medium text-muted-foreground">Description</span>
              <p className="mt-0.5 text-sm">{project.description || "-"}</p>
            </div>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Slug: </span>
                <span className="font-mono">{project.slug}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Path: </span>
                <span className="font-mono">{project.path || "N/A"}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Memories: </span>
                <span className="font-semibold">{project.memory_count ?? 0}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Entities: </span>
                <span className="font-semibold">{project.entity_count ?? 0}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Created: </span>
                <span>{formatDate(project.created_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Updated: </span>
                <span>{formatDate(project.updated_at)}</span>
              </div>
            </div>
            {project.default_tags && project.default_tags.length > 0 && (
              <div>
                <span className="text-sm font-medium text-muted-foreground">Default Tags</span>
                <div className="mt-1 flex flex-wrap gap-2">
                  {project.default_tags.map((tag) => (
                    <TagChip key={tag} tag={tag} />
                  ))}
                </div>
              </div>
            )}
            <MCPConfigSnippet slug={project.slug} />

            {canDelete && (
              <div className="flex items-center gap-3 border-t pt-4">
                <div className="flex-1" />
                {confirmDelete ? (
                  <>
                    <span className="text-sm text-red-600 dark:text-red-400">
                      This will permanently delete all memories, vectors, entities, and relationships in this project. This action cannot be undone. Continue?
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
                    Delete Project
                  </button>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default ProjectManagement;
