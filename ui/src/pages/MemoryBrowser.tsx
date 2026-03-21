import { useState, useCallback, useEffect, useMemo, useRef } from "react";
import {
  useMeProjects,
  useMemoryList,
  useMemoryRecall,
  useMemoryDetail,
  useUpdateMemory,
  useDeleteMemory,
  useForgetMemories,
  useEnrichMemories,
} from "../hooks/useApi";
import { useAuth } from "../context/AuthContext";
import { memoryAPI, type Memory, type MemoryListParams } from "../api/client";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PAGE_SIZE = 20;
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

function preview(content: string, maxLen = 200): string {
  if (content.length <= maxLen) return content;
  return content.slice(0, maxLen) + "...";
}

function downloadJson(data: unknown, filename: string) {
  const blob = new Blob([JSON.stringify(data, null, 2)], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Tag chip colors
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
// Skeleton components
// ---------------------------------------------------------------------------

function SkeletonCard() {
  return (
    <div className="animate-pulse rounded-lg border bg-card p-4">
      <div className="h-4 w-3/4 rounded bg-muted" />
      <div className="mt-2 h-3 w-1/2 rounded bg-muted" />
      <div className="mt-3 flex gap-2">
        <div className="h-5 w-12 rounded bg-muted" />
        <div className="h-5 w-16 rounded bg-muted" />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// useDebounce hook
// ---------------------------------------------------------------------------

function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(id);
  }, [value, delay]);
  return debounced;
}

// ---------------------------------------------------------------------------
// Tag Chip (inline editable)
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
        className="w-20 rounded border bg-background px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
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
// Filter Sidebar
// ---------------------------------------------------------------------------

interface FilterState {
  selectedTags: string[];
  dateFrom: string;
  dateTo: string;
  enrichmentFilter: "all" | "enriched" | "not_enriched";
  sourceFilter: string;
}

function FilterSidebar({
  availableTags,
  filters,
  onFiltersChange,
  collapsed,
  onToggleCollapse,
}: {
  availableTags: string[];
  filters: FilterState;
  onFiltersChange: (f: FilterState) => void;
  collapsed: boolean;
  onToggleCollapse: () => void;
}) {
  function toggleTag(tag: string) {
    const next = filters.selectedTags.includes(tag)
      ? filters.selectedTags.filter((t) => t !== tag)
      : [...filters.selectedTags, tag];
    onFiltersChange({ ...filters, selectedTags: next });
  }

  function clearFilters() {
    onFiltersChange({
      selectedTags: [],
      dateFrom: "",
      dateTo: "",
      enrichmentFilter: "all",
      sourceFilter: "",
    });
  }

  return (
    <div
      className={`shrink-0 rounded-lg border bg-card transition-all ${collapsed ? "w-10" : "w-64"}`}
    >
      <div className="flex items-center justify-between border-b px-3 py-2">
        {!collapsed && (
          <h3 className="text-sm font-semibold">Filters</h3>
        )}
        <button
          type="button"
          className="text-xs text-muted-foreground hover:text-foreground"
          onClick={onToggleCollapse}
          title={collapsed ? "Expand filters" : "Collapse filters"}
        >
          {collapsed ? ">" : "<"}
        </button>
      </div>
      {!collapsed && (
        <div className="space-y-4 p-3">
          {/* Tags */}
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Tags
            </label>
            {availableTags.length === 0 ? (
              <p className="text-xs text-muted-foreground">No tags found</p>
            ) : (
              <div className="max-h-40 space-y-1 overflow-y-auto">
                {availableTags.map((tag) => (
                  <label
                    key={tag}
                    className="flex items-center gap-2 text-xs"
                  >
                    <input
                      type="checkbox"
                      className="rounded border"
                      checked={filters.selectedTags.includes(tag)}
                      onChange={() => toggleTag(tag)}
                    />
                    <span className="truncate">{tag}</span>
                  </label>
                ))}
              </div>
            )}
          </div>

          {/* Date range */}
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Date Range
            </label>
            <div className="space-y-1">
              <input
                type="date"
                className="w-full rounded border bg-background px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
                value={filters.dateFrom}
                onChange={(e) =>
                  onFiltersChange({ ...filters, dateFrom: e.target.value })
                }
                placeholder="From"
              />
              <input
                type="date"
                className="w-full rounded border bg-background px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
                value={filters.dateTo}
                onChange={(e) =>
                  onFiltersChange({ ...filters, dateTo: e.target.value })
                }
                placeholder="To"
              />
            </div>
          </div>

          {/* Enrichment */}
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Enrichment
            </label>
            <div className="space-y-1">
              {(
                [
                  ["all", "All"],
                  ["enriched", "Enriched"],
                  ["not_enriched", "Not Enriched"],
                ] as const
              ).map(([val, label]) => (
                <label key={val} className="flex items-center gap-2 text-xs">
                  <input
                    type="radio"
                    name="enrichment"
                    checked={filters.enrichmentFilter === val}
                    onChange={() =>
                      onFiltersChange({ ...filters, enrichmentFilter: val })
                    }
                  />
                  {label}
                </label>
              ))}
            </div>
          </div>

          {/* Source */}
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Source
            </label>
            <input
              type="text"
              className="w-full rounded border bg-background px-2 py-1 text-xs placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
              placeholder="Filter by source..."
              value={filters.sourceFilter}
              onChange={(e) =>
                onFiltersChange({ ...filters, sourceFilter: e.target.value })
              }
            />
          </div>

          {/* Clear */}
          <button
            type="button"
            className="w-full rounded border px-2 py-1 text-xs text-muted-foreground hover:bg-muted"
            onClick={clearFilters}
          >
            Clear Filters
          </button>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Memory Card
// ---------------------------------------------------------------------------

function MemoryCard({
  memory,
  score,
  isSelected,
  onToggleSelect,
  onClick,
}: {
  memory: Memory;
  score?: number;
  isSelected: boolean;
  onToggleSelect: () => void;
  onClick: () => void;
}) {
  return (
    <div
      className={`cursor-pointer rounded-lg border p-4 transition-colors hover:bg-accent/50 ${
        isSelected ? "border-primary bg-primary/5" : "bg-card"
      }`}
      onClick={onClick}
    >
      <div className="flex items-start gap-3">
        <button
          type="button"
          className={`mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded border-2 transition-colors ${
            isSelected
              ? "border-primary bg-primary text-primary-foreground"
              : "border-muted-foreground/30 hover:border-primary/50"
          }`}
          onClick={(e) => {
            e.stopPropagation();
            onToggleSelect();
          }}
          aria-label={isSelected ? "Deselect memory" : "Select memory"}
        >
          {isSelected && (
            <svg className="h-4 w-4" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M3.5 8.5L6.5 11.5L12.5 4.5" />
            </svg>
          )}
        </button>
        <div className="min-w-0 flex-1">
          <p className="text-sm leading-relaxed">{preview(memory.content)}</p>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            {(memory.tags ?? []).map((tag) => (
              <TagChip key={tag} tag={tag} />
            ))}
            {score !== undefined && (
              <span className="rounded bg-amber-100 px-1.5 py-0.5 text-xs font-mono text-amber-800 dark:bg-amber-900 dark:text-amber-300">
                {score.toFixed(3)}
              </span>
            )}
          </div>
          <div className="mt-2 flex items-center gap-3 text-xs text-muted-foreground">
            <span>{formatDate(memory.created_at)}</span>
            {memory.source && <span>Source: {memory.source}</span>}
            {memory.enriched && (
              <span className="rounded bg-green-100 px-1.5 py-0.5 text-green-800 dark:bg-green-900 dark:text-green-300">
                enriched
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Memory Detail Panel
// ---------------------------------------------------------------------------

function MemoryDetailPanel({
  projectId,
  memoryId,
  onClose,
  onDeleted,
}: {
  projectId: string;
  memoryId: string;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const { canWrite } = useAuth();
  const detail = useMemoryDetail(projectId, memoryId);
  const updateMut = useUpdateMemory();
  const deleteMut = useDeleteMemory();
  const [addingTag, setAddingTag] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const memory = detail.data;

  function handleRemoveTag(tag: string) {
    if (!memory) return;
    const newTags = (memory.tags ?? []).filter((t) => t !== tag);
    updateMut.mutate({
      projectId,
      memoryId,
      data: { tags: newTags },
    });
  }

  function handleAddTag(tag: string) {
    if (!memory) return;
    if (memory.tags.includes(tag)) return;
    updateMut.mutate({
      projectId,
      memoryId,
      data: { tags: [...memory.tags, tag] },
    });
    setAddingTag(false);
  }

  function handleDelete() {
    deleteMut.mutate(
      { projectId, memoryId },
      {
        onSuccess: () => {
          onDeleted();
          onClose();
        },
      },
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/30"
        onClick={onClose}
      />
      {/* Panel */}
      <div className="relative z-10 flex h-full w-full max-w-2xl flex-col overflow-y-auto border-l bg-background shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b px-6 py-4">
          <h2 className="text-lg font-semibold">Memory Detail</h2>
          <button
            type="button"
            className="rounded border px-3 py-1 text-sm hover:bg-muted"
            onClick={onClose}
          >
            Close
          </button>
        </div>

        {detail.isLoading && (
          <div className="p-6">
            <div className="animate-pulse space-y-3">
              <div className="h-4 w-3/4 rounded bg-muted" />
              <div className="h-4 w-1/2 rounded bg-muted" />
              <div className="h-20 rounded bg-muted" />
            </div>
          </div>
        )}

        {detail.isError && (
          <div className="p-6">
            <p className="text-sm text-red-600 dark:text-red-400">
              Failed to load memory: {detail.error?.message}
            </p>
          </div>
        )}

        {memory && (
          <div className="flex-1 space-y-6 p-6">
            {/* Content */}
            <div>
              <h3 className="mb-2 text-sm font-medium text-muted-foreground">
                Content
              </h3>
              <div className="whitespace-pre-wrap rounded border bg-muted/50 p-3 text-sm">
                {memory.content}
              </div>
            </div>

            {/* Tags */}
            <div>
              <h3 className="mb-2 text-sm font-medium text-muted-foreground">
                Tags
              </h3>
              <div className="flex flex-wrap items-center gap-2">
                {(memory.tags ?? []).map((tag) => (
                  <TagChip
                    key={tag}
                    tag={tag}
                    onRemove={canWrite ? () => handleRemoveTag(tag) : undefined}
                  />
                ))}
                {canWrite && (
                  addingTag ? (
                    <AddTagInput onAdd={handleAddTag} />
                  ) : (
                    <button
                      type="button"
                      className="rounded border border-dashed px-2 py-0.5 text-xs text-muted-foreground hover:border-primary hover:text-primary"
                      onClick={() => setAddingTag(true)}
                    >
                      + Add tag
                    </button>
                  )
                )}
              </div>
            </div>

            {/* Metadata */}
            <div>
              <h3 className="mb-2 text-sm font-medium text-muted-foreground">
                Metadata
              </h3>
              <pre className="max-h-48 overflow-auto rounded border bg-muted/50 p-3 text-xs">
                {JSON.stringify(memory.metadata, null, 2)}
              </pre>
            </div>

            {/* Info row */}
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Source: </span>
                <span>{memory.source || "N/A"}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Enriched: </span>
                <span>{memory.enriched ? "Yes" : "No"}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Created: </span>
                <span>{formatDate(memory.created_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Updated: </span>
                <span>{formatDate(memory.updated_at)}</span>
              </div>
            </div>

            {/* Actions — only show delete for users with write access */}
            {canWrite && (
              <div className="flex items-center gap-3 border-t pt-4">
                {confirmDelete ? (
                  <>
                    <span className="text-sm text-red-600 dark:text-red-400">
                      Confirm delete?
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
                    Delete Memory
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

// ---------------------------------------------------------------------------
// Bulk Actions Bar
// ---------------------------------------------------------------------------

function BulkActionsBar({
  selectedCount,
  onDelete,
  onEnrich,
  onAddTags,
  onExport,
  onClear,
  isDeleting,
  isEnriching,
}: {
  selectedCount: number;
  onDelete?: () => void;
  onEnrich?: () => void;
  onAddTags?: (tags: string[]) => void;
  onExport: () => void;
  onClear: () => void;
  isDeleting: boolean;
  isEnriching: boolean;
}) {
  const [tagInput, setTagInput] = useState("");
  const [confirmDelete, setConfirmDelete] = useState(false);

  function handleAddTags() {
    const tags = tagInput
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    if (tags.length > 0 && onAddTags) {
      onAddTags(tags);
      setTagInput("");
    }
  }

  return (
    <div className="sticky bottom-0 z-40 flex items-center gap-3 border-t bg-card px-4 py-3 shadow-lg">
      <span className="text-sm font-medium">
        {selectedCount} selected
      </span>
      <div className="h-4 w-px bg-border" />

      {confirmDelete && onDelete ? (
        <>
          <span className="text-sm text-red-600 dark:text-red-400">
            Delete {selectedCount} memories?
          </span>
          <button
            type="button"
            className="rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700 disabled:opacity-50"
            onClick={() => {
              onDelete();
              setConfirmDelete(false);
            }}
            disabled={isDeleting}
          >
            {isDeleting ? "Deleting..." : "Confirm"}
          </button>
          <button
            type="button"
            className="rounded border px-3 py-1 text-sm hover:bg-muted"
            onClick={() => setConfirmDelete(false)}
          >
            Cancel
          </button>
        </>
      ) : (
        <>
          {onDelete && (
            <button
              type="button"
              className="rounded border border-red-300 px-3 py-1 text-sm text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950"
              onClick={() => setConfirmDelete(true)}
            >
              Delete
            </button>
          )}
          {onEnrich && (
            <button
              type="button"
              className="rounded border px-3 py-1 text-sm hover:bg-muted disabled:opacity-50"
              onClick={onEnrich}
              disabled={isEnriching}
            >
              {isEnriching ? "Enriching..." : "Enrich"}
            </button>
          )}
          {onAddTags && (
            <div className="flex items-center gap-1">
              <input
                type="text"
                className="w-32 rounded border bg-background px-2 py-1 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
                placeholder="tag1, tag2"
                value={tagInput}
                onChange={(e) => setTagInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") handleAddTags();
                }}
              />
              <button
                type="button"
                className="rounded border px-2 py-1 text-sm hover:bg-muted"
                onClick={handleAddTags}
              >
                Add Tags
              </button>
            </div>
          )}
          <button
            type="button"
            className="rounded border px-3 py-1 text-sm hover:bg-muted"
            onClick={onExport}
          >
            Export
          </button>
        </>
      )}

      <div className="flex-1" />
      <button
        type="button"
        className="rounded border px-3 py-1 text-sm text-muted-foreground hover:bg-muted"
        onClick={onClear}
      >
        Clear Selection
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

function Pagination({
  offset,
  limit,
  total,
  onPageChange,
}: {
  offset: number;
  limit: number;
  total: number;
  onPageChange: (newOffset: number) => void;
}) {
  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = Math.max(1, Math.ceil(total / limit));

  return (
    <div className="flex items-center justify-between px-1 py-3">
      <p className="text-xs text-muted-foreground">
        Showing {Math.min(offset + 1, total)}-{Math.min(offset + limit, total)}{" "}
        of {total}
      </p>
      <div className="flex items-center gap-2">
        <button
          type="button"
          className="rounded border px-3 py-1 text-sm hover:bg-muted disabled:opacity-50"
          disabled={offset === 0}
          onClick={() => onPageChange(Math.max(0, offset - limit))}
        >
          Previous
        </button>
        <span className="text-xs text-muted-foreground">
          Page {currentPage} of {totalPages}
        </span>
        <button
          type="button"
          className="rounded border px-3 py-1 text-sm hover:bg-muted disabled:opacity-50"
          disabled={offset + limit >= total}
          onClick={() => onPageChange(offset + limit)}
        >
          Next
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main MemoryBrowser
// ---------------------------------------------------------------------------

function MemoryBrowser() {
  const { canWrite } = useAuth();

  const projectsQuery = useMeProjects();
  const projects = projectsQuery.data ?? [];
  const [selectedProjectId, setSelectedProjectId] = useState("");

  // Auto-select first project
  useEffect(() => {
    if (!selectedProjectId && projects.length > 0) {
      setSelectedProjectId(projects[0].id);
    }
  }, [projects, selectedProjectId]);

  // Search state
  const [searchMode, setSearchMode] = useState<"semantic" | "exact">(
    "exact",
  );
  const [searchText, setSearchText] = useState("");
  const debouncedSearch = useDebounce(searchText, DEBOUNCE_MS);

  // Filter state
  const [filters, setFilters] = useState<FilterState>({
    selectedTags: [],
    dateFrom: "",
    dateTo: "",
    enrichmentFilter: "all",
    sourceFilter: "",
  });
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  // Pagination
  const [offset, setOffset] = useState(0);

  // Reset offset when search/filters change
  const filterKey = JSON.stringify({
    debouncedSearch,
    searchMode,
    filters,
    selectedProjectId,
  });
  const prevFilterKeyRef = useRef(filterKey);
  useEffect(() => {
    if (prevFilterKeyRef.current !== filterKey) {
      prevFilterKeyRef.current = filterKey;
      setOffset(0);
    }
  }, [filterKey]);

  // Selection state
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [detailMemoryId, setDetailMemoryId] = useState<string | null>(null);

  // Build list params (server only supports limit/offset pagination)
  const listParams: MemoryListParams = useMemo(() => {
    return { limit: PAGE_SIZE, offset };
  }, [offset]);

  // Queries
  const isSemanticSearch = searchMode === "semantic" && debouncedSearch.length > 0;

  const listQuery = useMemoryList(
    selectedProjectId,
    isSemanticSearch ? undefined : listParams,
  );

  const recallQuery = useMemoryRecall(
    selectedProjectId,
    isSemanticSearch
      ? {
          query: debouncedSearch,
          limit: PAGE_SIZE,
          tags:
            filters.selectedTags.length > 0
              ? filters.selectedTags
              : undefined,
          threshold: 0,
        }
      : null,
  );

  // Mutations
  const forgetMut = useForgetMemories();
  const enrichMut = useEnrichMemories();
  const updateMut = useUpdateMemory();

  // Derived data
  const memories: Memory[] = useMemo(() => {
    if (isSemanticSearch) {
      const results = recallQuery.data?.memories ?? [];
      return results.map((r) => ({
        id: r.id,
        content: r.content,
        tags: r.tags,
        source: r.source,
        enriched: r.enriched ?? false,
        metadata: r.metadata,
        created_at: r.created_at,
        updated_at: r.created_at,
      }));
    }
    return listQuery.data?.data ?? [];
  }, [isSemanticSearch, recallQuery.data, listQuery.data]);

  const scoreMap = useMemo(() => {
    if (!isSemanticSearch || !recallQuery.data) return new Map<string, number>();
    return new Map(recallQuery.data.memories.map((r) => [r.id, r.score]));
  }, [isSemanticSearch, recallQuery.data]);

  const total = isSemanticSearch
    ? memories.length
    : (listQuery.data?.pagination?.total ?? 0);

  const isLoading = isSemanticSearch
    ? recallQuery.isLoading
    : listQuery.isLoading;

  const isError = isSemanticSearch
    ? recallQuery.isError
    : listQuery.isError;

  const errorMessage = isSemanticSearch
    ? recallQuery.error?.message
    : listQuery.error?.message;

  // Available tags derived from loaded memories
  const availableTags = useMemo(() => {
    const tagSet = new Set<string>();
    for (const m of memories) {
      for (const t of m.tags) tagSet.add(t);
    }
    return Array.from(tagSet).sort();
  }, [memories]);

  // Handlers
  const toggleSelect = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  function clearSelection() {
    setSelectedIds(new Set());
  }

  function handleBulkDelete() {
    if (!selectedProjectId || selectedIds.size === 0) return;
    forgetMut.mutate(
      {
        projectId: selectedProjectId,
        body: { ids: Array.from(selectedIds) },
      },
      { onSuccess: () => clearSelection() },
    );
  }

  function handleBulkEnrich() {
    if (!selectedProjectId || selectedIds.size === 0) return;
    enrichMut.mutate(
      {
        projectId: selectedProjectId,
        body: { ids: Array.from(selectedIds) },
      },
      { onSuccess: () => clearSelection() },
    );
  }

  function handleBulkAddTags(tags: string[]) {
    if (!selectedProjectId || selectedIds.size === 0) return;
    const promises = Array.from(selectedIds).map((memoryId) => {
      const mem = memories.find((m) => m.id === memoryId);
      const existingTags = mem?.tags ?? [];
      const merged = Array.from(new Set([...existingTags, ...tags]));
      return updateMut.mutateAsync({
        projectId: selectedProjectId,
        memoryId,
        data: { tags: merged },
      });
    });
    Promise.all(promises).then(() => clearSelection());
  }

  function handleBulkExport() {
    const selected = memories.filter((m) => selectedIds.has(m.id));
    downloadJson(selected, "memories-export.json");
  }

  async function handleExportAll() {
    if (!selectedProjectId) return;
    try {
      const data = await memoryAPI.export(selectedProjectId);
      downloadJson(data, "memories-export-all.json");
    } catch {
      // silently fail export
    }
  }

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="space-y-4 pb-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Memory Browser
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Browse, search, and manage stored memories.
          </p>
        </div>

        {/* Project selector + search bar */}
        <div className="flex flex-col gap-3 sm:flex-row">
          {/* Project selector */}
          <div className="sm:w-56">
            {projectsQuery.isLoading ? (
              <div className="h-9 animate-pulse rounded-md bg-muted" />
            ) : (
              <select
                className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                value={selectedProjectId}
                onChange={(e) => {
                  setSelectedProjectId(e.target.value);
                  clearSelection();
                  setOffset(0);
                }}
              >
                {projects.length === 0 && (
                  <option value="">No projects</option>
                )}
                {projects.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
              </select>
            )}
          </div>

          {/* Search */}
          <div className="flex flex-1 items-center gap-2">
            <div className="relative flex-1">
              <input
                type="text"
                className="w-full rounded-md border bg-background px-3 py-2 pr-8 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder={
                  searchMode === "semantic"
                    ? "Semantic search..."
                    : "Text search..."
                }
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
            <button
              type="button"
              className={`shrink-0 rounded-md border px-3 py-2 text-sm font-medium transition-colors ${
                searchMode === "semantic"
                  ? "bg-primary text-primary-foreground"
                  : "hover:bg-muted"
              }`}
              onClick={() =>
                setSearchMode((m) =>
                  m === "semantic" ? "exact" : "semantic",
                )
              }
              title={`Switch to ${searchMode === "semantic" ? "exact" : "semantic"} search`}
            >
              {searchMode === "semantic" ? "Semantic" : "Exact"}
            </button>
            <button
              type="button"
              className="shrink-0 rounded-md border px-3 py-2 text-sm hover:bg-muted"
              onClick={handleExportAll}
              title="Export all memories"
            >
              Export All
            </button>
          </div>
        </div>
      </div>

      {/* Main content: sidebar + list */}
      <div className="flex flex-1 gap-4 overflow-hidden">
        {/* Filter sidebar */}
        <FilterSidebar
          availableTags={availableTags}
          filters={filters}
          onFiltersChange={setFilters}
          collapsed={sidebarCollapsed}
          onToggleCollapse={() => setSidebarCollapsed((c) => !c)}
        />

        {/* Memory list */}
        <div className="flex min-w-0 flex-1 flex-col">
          {isError && (
            <div className="mb-3 rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300">
              Failed to load memories: {errorMessage ?? "Unknown error"}
            </div>
          )}

          {isLoading ? (
            <div className="space-y-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <SkeletonCard key={i} />
              ))}
            </div>
          ) : memories.length === 0 ? (
            <div className="flex flex-1 items-center justify-center">
              <p className="text-sm text-muted-foreground">
                {debouncedSearch
                  ? "No memories found matching your search."
                  : "No memories in this project yet."}
              </p>
            </div>
          ) : (
            <>
              <div className="flex-1 space-y-3 overflow-y-auto">
                {memories.map((m) => (
                  <MemoryCard
                    key={m.id}
                    memory={m}
                    score={scoreMap.get(m.id)}
                    isSelected={selectedIds.has(m.id)}
                    onToggleSelect={() => toggleSelect(m.id)}
                    onClick={() => setDetailMemoryId(m.id)}
                  />
                ))}
              </div>

              {/* Pagination (only for list mode, not semantic) */}
              {!isSemanticSearch && total > 0 && (
                <Pagination
                  offset={offset}
                  limit={PAGE_SIZE}
                  total={total}
                  onPageChange={setOffset}
                />
              )}
            </>
          )}
        </div>
      </div>

      {/* Bulk actions bar — only show write actions if canWrite */}
      {selectedIds.size > 0 && (
        <BulkActionsBar
          selectedCount={selectedIds.size}
          onDelete={canWrite ? handleBulkDelete : undefined}
          onEnrich={canWrite ? handleBulkEnrich : undefined}
          onAddTags={canWrite ? handleBulkAddTags : undefined}
          onExport={handleBulkExport}
          onClear={clearSelection}
          isDeleting={forgetMut.isPending}
          isEnriching={enrichMut.isPending}
        />
      )}

      {/* Detail panel */}
      {detailMemoryId && selectedProjectId && (
        <MemoryDetailPanel
          projectId={selectedProjectId}
          memoryId={detailMemoryId}
          onClose={() => setDetailMemoryId(null)}
          onDeleted={() => {
            selectedIds.delete(detailMemoryId);
            setSelectedIds(new Set(selectedIds));
          }}
        />
      )}
    </div>
  );
}

export default MemoryBrowser;
