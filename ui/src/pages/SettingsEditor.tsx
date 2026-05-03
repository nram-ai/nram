import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import {
  useSettings,
  useSettingsSchema,
  useSetupStatus,
  useUpdateSetting,
} from "../hooks/useApi";
import { useEnrichmentAvailable } from "../hooks/useEnrichmentAvailable";
import type { Setting, SettingSchema } from "../api/client";
import Switch from "../components/Switch";
import PhaseBudgetBar, { type PhaseBudgetSegment } from "../components/PhaseBudgetBar";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCheck, faXmark, faCircleQuestion, faSpinner } from "../lib/icons";

// Setting keys are not always literal phase names — e.g. `dreaming.transitive.*`
// drives the `transitive_discovery` phase. The bar needs the phase key to
// look up color and label.
const SETTING_KEY_TO_PHASE: Record<string, string> = {
  "dreaming.entity_dedup.budget_fraction": "entity_dedup",
  "dreaming.embedding_backfill.budget_fraction": "embedding_backfill",
  "dreaming.paraphrase_dedup.budget_fraction": "paraphrase_dedup",
  "dreaming.transitive.budget_fraction": "transitive_discovery",
  "dreaming.contradiction.budget_fraction": "contradiction_detection",
  "dreaming.consolidation.budget_fraction": "consolidation",
  "dreaming.pruning.budget_fraction": "pruning",
  "dreaming.weight_adjustment.budget_fraction": "weight_adjustment",
  "dreaming.consolidation.audit_budget_fraction": "backfill_audit",
  "dreaming.consolidation.reinforce_budget_fraction": "reinforce",
  "dreaming.consolidation.consolidate_budget_fraction": "consolidate",
};

const PHASE_BUDGET_ORDER = [
  "dreaming.entity_dedup.budget_fraction",
  "dreaming.embedding_backfill.budget_fraction",
  "dreaming.paraphrase_dedup.budget_fraction",
  "dreaming.transitive.budget_fraction",
  "dreaming.contradiction.budget_fraction",
  "dreaming.consolidation.budget_fraction",
  "dreaming.pruning.budget_fraction",
  "dreaming.weight_adjustment.budget_fraction",
];

const CONSOLIDATION_BUDGET_ORDER = [
  "dreaming.consolidation.audit_budget_fraction",
  "dreaming.consolidation.reinforce_budget_fraction",
  "dreaming.consolidation.consolidate_budget_fraction",
];

const BUDGET_BAR_ORDERS: Record<string, string[]> = {
  dreaming_phase_budget: PHASE_BUDGET_ORDER,
  dreaming_consolidation: CONSOLIDATION_BUDGET_ORDER,
};

function fractionSegments(
  items: SettingWithSchema[],
  order: string[],
): PhaseBudgetSegment[] {
  const byKey = new Map<string, SettingWithSchema>(
    items.map((it) => [it.schema.key, it]),
  );
  const segments: PhaseBudgetSegment[] = [];
  for (const key of order) {
    const item = byKey.get(key);
    if (!item) continue;
    const raw = item.setting?.value ?? item.schema.default_value;
    const v = typeof raw === "number" ? raw : Number(raw);
    if (!Number.isFinite(v) || v <= 0) continue;
    segments.push({
      key: SETTING_KEY_TO_PHASE[key] ?? key,
      value: v,
    });
  }
  return segments;
}

const formatFractionPct = (v: number) => `${(v * 100).toFixed(0)}%`;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface SettingWithSchema {
  schema: SettingSchema;
  setting: Setting | null;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

interface SubSection {
  // The schema-side category key that backend entries are tagged with.
  category: string;
  // Sub-section heading; leave undefined when the parent has only one
  // sub-section and the parent header already says everything.
  label?: string;
  description?: string;
}

interface ParentGroup {
  id: string;
  label: string;
  description?: string;
  // Hide the entire parent (and its sub-sections) when enrichment is
  // unavailable. Read-path tuning stays visible regardless.
  requiresEnrichment?: boolean;
  // Hide the entire parent unless the active database backend is in this
  // list. Used for storage-bound knobs (HNSW only matters on SQLite).
  requiresBackend?: string[];
  subSections: SubSection[];
}

const PARENT_GROUPS: ParentGroup[] = [
  {
    id: "memory",
    label: "Memory",
    description: "Defaults applied to new memories and how long deleted ones are retained.",
    subSections: [{ category: "memory" }],
  },
  {
    id: "enrichment",
    label: "Enrichment",
    description: "Background pipeline that pulls facts and entities out of new memories.",
    requiresEnrichment: true,
    subSections: [
      {
        category: "enrichment",
        label: "General",
        description: "Master switches and basic batch sizing for the enrichment pipeline.",
      },
      {
        category: "enrichment_ingestion",
        label: "Ingestion Decision",
        description:
          "When a new memory looks like a near-duplicate, the model decides whether to add, update, delete, or skip. Off by default. Turn shadow mode on first to observe the decisions before acting on them.",
      },
      {
        category: "enrichment_performance",
        label: "Worker Performance",
        description:
          "Throughput and concurrency for the enrichment worker pool, plus the model parameters used for fact and entity extraction. Also covers gone-worker recovery: heartbeat interval, stuck-job sweep cadence, and the staleness threshold past which an in-flight job is auto-requeued. Most fields hot-reload; the worker count, poll interval, heartbeat interval, and sweep interval need a restart.",
      },
    ],
  },
  {
    id: "dreaming",
    label: "Dreaming",
    description: "Background consolidation that audits syntheses, reinforces confidence, and merges related memories.",
    requiresEnrichment: true,
    subSections: [
      {
        category: "dreaming",
        label: "General",
        description: "Scheduler, token budgets, and the confidence floor for new syntheses.",
      },
      {
        category: "dreaming_novelty",
        label: "Novelty Audit",
        description:
          "Discards syntheses that don't actually add anything new compared to the memories they were built from.",
      },
      {
        category: "dreaming_phase_budget",
        label: "Phase Budget Allocation",
        description:
          "Reserve a fraction of the per-cycle token budget for each phase. Without reservations, an LLM-heavy phase running early can consume the entire envelope and starve later phases. SQL-only phases default to 0 (share the root); LLM phases default to a share that protects downstream synthesis.",
      },
      {
        category: "dreaming_consolidation",
        label: "Consolidation Budget",
        description:
          "How the per-cycle token budget is split across the audit, reinforce, and consolidate sub-phases so none can starve the others.",
      },
      {
        category: "dreaming_contradiction",
        label: "Contradiction Detection",
        description:
          "Per-cycle cap on the model calls used to find contradicting memory pairs, plus the confidence haircuts applied to winners, losers, and ties.",
      },
      {
        category: "dreaming_paraphrase",
        label: "Paraphrase Sweep",
        description:
          "Catches near-duplicate memories the contradiction phase misses by running a vector similarity sweep directly on every eligible memory.",
      },
      {
        category: "dreaming_embedding_backfill",
        label: "Embedding Backfill",
        description:
          "Repairs memories whose embedding row is missing. Re-embeds when the embedder is healthy; otherwise clears the orphan dimension marker.",
      },
      {
        category: "dreaming_performance",
        label: "Performance",
        description:
          "How many neighbors to consider, how similar two entities must be to merge, and how often the scheduler wakes up.",
      },
    ],
  },
  {
    id: "recall",
    label: "Recall & Ranking",
    description: "How memories are scored, fused, and reinforced at retrieval time.",
    subSections: [
      {
        category: "reconsolidation",
        label: "Reconsolidation",
        description:
          "Each recall reinforces a memory's confidence; idle memories slowly decay during dream cycles.",
      },
      {
        category: "recall_fusion",
        label: "Hybrid Fusion",
        description:
          "Run vector and lexical (BM25) search side by side and merge the results with Reciprocal Rank Fusion. Off by default. Turn on after migration 18 has been applied.",
      },
      {
        category: "ranking",
        label: "Ranking",
        description:
          "Weights for the recall ranking formula: similarity, recency, importance, frequency, graph relevance, and confidence.",
      },
    ],
  },
  {
    id: "api",
    label: "API",
    description: "Public API rate limits, per-request caps, and graph defaults.",
    subSections: [
      {
        category: "api",
        label: "General",
        description: "Per-user rate limit and burst size for the public API.",
      },
      {
        category: "api_performance",
        label: "Performance",
        description:
          "Rate-limiter cleanup cadence, batch-store item cap, and the default minimum edge weight for the graph endpoint. Advanced.",
      },
    ],
  },
  {
    id: "graph_visualization",
    label: "Graph Visualization",
    description:
      "System-default d3-force parameters for the 3D entity graph (gravity, repulsion, link distance). Each project can override these from the Layout panel on the graph page; values here apply when no override is stored.",
    subSections: [{ category: "graph_visualization" }],
  },
  {
    id: "auth",
    label: "Auth",
    description: "Authentication and authorization.",
    subSections: [{ category: "auth" }],
  },
  {
    id: "vector_db",
    label: "Vector Database",
    description: "Connection settings for the Qdrant vector database.",
    requiresBackend: ["postgres"],
    subSections: [{ category: "qdrant" }],
  },
  {
    id: "hnsw",
    label: "Vector Index (HNSW)",
    description:
      "Pure-Go HNSW index used for semantic search when the database backend is SQLite. M and ef_construction are baked into each index at build time, so changes apply only to newly-built indexes; ef_search and the cache size apply at next boot.",
    requiresBackend: ["sqlite"],
    subSections: [{ category: "hnsw" }],
  },
  {
    id: "lifecycle",
    label: "Lifecycle Sweep",
    description:
      "Background sweep that expires time-to-live (TTL) memories, hard-purges soft-deleted ones past their retention window, and prunes orphaned graph data.",
    subSections: [{ category: "lifecycle" }],
  },
  {
    id: "events",
    label: "Events & Streaming",
    description:
      "Buffer sizes and keepalive timing for server-sent events (SSE) and the in-process event bus. Advanced: incorrect values can stall subscribers or grow memory unboundedly.",
    subSections: [{ category: "events" }],
  },
  {
    id: "caches",
    label: "Service Caches",
    description:
      "Cache lifetimes for the cascade resolver and settings service, plus the export pagination size.",
    subSections: [{ category: "performance" }],
  },
];

// Prompt-typed schema entries. Surfaced on the dedicated Prompt Templates page;
// filtered out of the Settings page entirely so they cannot be edited in two
// places.
const PROMPT_KEYS = new Set([
  "enrichment.fact_prompt",
  "enrichment.entity_prompt",
  "enrichment.ingestion_decision.prompt",
  "dreaming.contradiction_prompt",
  "dreaming.synthesis_prompt",
  "dreaming.alignment_prompt",
  "dreaming.novelty.judge_prompt",
]);

// Settings now surfaced on the Provider Configuration page. Filtered out so
// the Settings page does not double-surface them.
const MOVED_TO_PROVIDER_CONFIG = new Set([
  "enrichment.ingestion_decision.model",
]);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return String(value);
  return JSON.stringify(value, null, 2);
}

function parseValue(raw: string, type: string): unknown {
  switch (type) {
    case "bool":
    case "boolean":
      return raw === "true";
    case "int":
      return parseInt(raw, 10);
    case "float":
    case "number":
      return parseFloat(raw);
    case "json":
      try {
        return JSON.parse(raw);
      } catch {
        return raw;
      }
    default:
      return raw;
  }
}

function isPromptKey(key: string): boolean {
  return PROMPT_KEYS.has(key);
}

// ---------------------------------------------------------------------------
// Scope Badge
// ---------------------------------------------------------------------------

function ScopeBadge({ scope }: { scope: string }) {
  const isGlobal = scope === "global";
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
        isGlobal
          ? "bg-info/10 text-info"
          : "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300"
      }`}
    >
      {isGlobal ? "Global" : "Project"}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Status Toast
// ---------------------------------------------------------------------------

function StatusToast({
  message,
  type,
}: {
  message: string;
  type: "success" | "error";
}) {
  return (
    <div
      className={`fixed bottom-4 right-4 z-50 flex items-center gap-2 rounded-md px-4 py-2.5 text-sm font-medium shadow-lg transition-all ${ type === "success" ? "bg-success/10 text-success" : "bg-destructive/10 text-destructive" }`}
    >
      {type === "success" ? (
        <FontAwesomeIcon icon={faCheck} className="h-4 w-4 flex-shrink-0" />
      ) : (
        <FontAwesomeIcon icon={faXmark} className="h-4 w-4 flex-shrink-0" />
      )}
      {message}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Help Tooltip
// ---------------------------------------------------------------------------

function HelpTooltip({ text }: { text: string }) {
  const [open, setOpen] = useState(false);
  if (!text) return null;
  return (
    <span className="relative inline-flex">
      <button
        type="button"
        aria-label="Show help"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        onBlur={() => {
          if (open) setOpen(false);
        }}
        onKeyDown={(e) => {
          if (e.key === "Escape") setOpen(false);
        }}
        className="inline-flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground hover:text-foreground focus:outline-none focus:ring-2 focus:ring-ring"
      >
        <FontAwesomeIcon icon={faCircleQuestion} className="h-3.5 w-3.5" />
      </button>
      {open && (
        <span
          role="tooltip"
          className="absolute left-1/2 top-full z-20 mt-1 w-64 -translate-x-1/2 rounded-md border border-border bg-popover p-3 text-xs leading-relaxed text-foreground shadow-lg"
        >
          {text}
        </span>
      )}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Value Input
// ---------------------------------------------------------------------------

const INPUT_CLASS =
  "w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring";
const TEXTAREA_CLASS = `${INPUT_CLASS} font-mono`;

interface RenderValueInputProps {
  schema: SettingSchema;
  isPrompt: boolean;
  editValue: string;
  setEditValue: (v: string) => void;
  onKeyDown: (e: React.KeyboardEvent) => void;
  inputRef: React.RefObject<HTMLInputElement | HTMLTextAreaElement>;
}

function renderValueInput({
  schema,
  isPrompt,
  editValue,
  setEditValue,
  onKeyDown,
  inputRef,
}: RenderValueInputProps) {
  const onChange = (
    e:
      | React.ChangeEvent<HTMLInputElement>
      | React.ChangeEvent<HTMLTextAreaElement>
      | React.ChangeEvent<HTMLSelectElement>,
  ) => setEditValue(e.target.value);

  if (schema.type === "text" || isPrompt) {
    return (
      <textarea
        ref={inputRef as React.RefObject<HTMLTextAreaElement>}
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        rows={8}
        className={TEXTAREA_CLASS}
      />
    );
  }
  if (schema.type === "json") {
    return (
      <textarea
        ref={inputRef as React.RefObject<HTMLTextAreaElement>}
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        rows={6}
        className={TEXTAREA_CLASS}
      />
    );
  }
  if (schema.type === "int" || schema.type === "float" || schema.type === "number") {
    // Schema-driven range. min/max/step on SettingSchema are pointer-typed
    // on the server (omitted vs. zero distinguishable in JSON), so undefined
    // here means "no constraint" — fall back to the legacy heuristic step.
    const fallbackStep = schema.type === "float" ? 0.01 : 1;
    const step =
      typeof schema.step === "number" && Number.isFinite(schema.step) && schema.step > 0
        ? schema.step
        : fallbackStep;
    const min = typeof schema.min === "number" && Number.isFinite(schema.min) ? schema.min : undefined;
    const max = typeof schema.max === "number" && Number.isFinite(schema.max) ? schema.max : undefined;
    return (
      <input
        ref={inputRef as React.RefObject<HTMLInputElement>}
        type="number"
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        step={step}
        min={min}
        max={max}
        className={INPUT_CLASS}
      />
    );
  }
  if (schema.type === "secret") {
    return (
      <input
        ref={inputRef as React.RefObject<HTMLInputElement>}
        type="password"
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        className={INPUT_CLASS}
      />
    );
  }
  if (schema.type === "enum" && schema.enum_values && schema.enum_values.length > 0) {
    return (
      <select
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        className={INPUT_CLASS}
      >
        {schema.enum_values.map((v) => (
          <option key={v} value={v}>
            {v}
          </option>
        ))}
      </select>
    );
  }
  return (
    <input
      ref={inputRef as React.RefObject<HTMLInputElement>}
      type="text"
      value={editValue}
      onChange={onChange}
      onKeyDown={onKeyDown}
      className={INPUT_CLASS}
    />
  );
}

// ---------------------------------------------------------------------------
// Inline Setting Editor
// ---------------------------------------------------------------------------

function InlineSettingEditor({
  item,
  onSave,
  saving,
}: {
  item: SettingWithSchema;
  onSave: (key: string, value: unknown, scope: string) => void;
  saving: boolean;
}) {
  const { schema, setting } = item;
  const currentValue = setting?.value ?? schema.default_value;
  const currentScope = setting?.scope ?? "global";
  const isDefault = setting === null;
  const isPrompt = isPromptKey(schema.key);

  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState("");
  const [editScope, setEditScope] = useState(currentScope);
  const inputRef = useRef<HTMLInputElement | HTMLTextAreaElement>(null);

  const startEdit = useCallback(() => {
    setEditValue(formatValue(currentValue));
    setEditScope(currentScope);
    setEditing(true);
  }, [currentValue, currentScope]);

  useEffect(() => {
    if (editing && inputRef.current) {
      inputRef.current.focus();
    }
  }, [editing]);

  const handleSave = useCallback(() => {
    const parsed = parseValue(editValue, schema.type);
    onSave(schema.key, parsed, editScope);
    setEditing(false);
  }, [editValue, editScope, schema.key, schema.type, onSave]);

  const handleCancel = useCallback(() => {
    setEditing(false);
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" && !e.shiftKey && schema.type !== "text" && schema.type !== "json") {
        e.preventDefault();
        handleSave();
      }
      if (e.key === "Escape") {
        handleCancel();
      }
    },
    [handleSave, handleCancel, schema.type],
  );

  const requiresRestart = schema.requires_restart === true;
  const headerRow = (
    <>
      <div className="flex items-center gap-2">
        <span className="font-mono text-sm font-medium text-foreground">
          {schema.key}
        </span>
        <HelpTooltip text={schema.description ?? ""} />
        <ScopeBadge scope={currentScope} />
        {isDefault && (
          <span className="text-xs text-muted-foreground">(default)</span>
        )}
        {requiresRestart && (
          <span className="inline-flex items-center rounded-full bg-warning/20 px-2 py-0.5 text-xs font-medium text-warning">
            Requires a server restart
          </span>
        )}
      </div>
      <p className="mt-0.5 text-xs text-muted-foreground">
        {schema.description}
      </p>
    </>
  );

  // Bool toggle (no edit mode needed)
  if ((schema.type === "bool" || schema.type === "boolean") && !editing) {
    const boolVal = currentValue === true || currentValue === "true";
    return (
      <div className="flex items-center justify-between py-3">
        <div className="flex-1 min-w-0">{headerRow}</div>
        <Switch
          checked={boolVal}
          disabled={saving}
          onChange={(v) => onSave(schema.key, v, currentScope)}
        />
      </div>
    );
  }

  // Display mode
  if (!editing) {
    return (
      <div className="flex items-start justify-between py-3 gap-4">
        <div className="flex-1 min-w-0">
          {headerRow}
          {!isDefault && (
            <p className="mt-1 text-xs text-muted-foreground">
              Default:{" "}
              <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
                {formatValue(schema.default_value)}
              </code>
            </p>
          )}
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <code
            className={`rounded bg-muted px-2 py-1 text-sm ${
              isPrompt || schema.type === "json" || schema.type === "text"
                ? "font-mono text-xs max-w-[200px] truncate"
                : "font-mono"
            }`}
          >
            {schema.type === "secret" && formatValue(currentValue).length > 0
              ? formatValue(currentValue).length > 4
                ? "••••••••" + formatValue(currentValue).slice(-4)
                : "••••••••"
              : formatValue(currentValue).length > 60
                ? formatValue(currentValue).slice(0, 60) + "..."
                : formatValue(currentValue)}
          </code>
          <button
            type="button"
            onClick={startEdit}
            className="rounded-md border border-input px-2.5 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted"
          >
            Edit
          </button>
        </div>
      </div>
    );
  }

  // Edit mode
  return (
    <div className="rounded-md border border-primary/30 bg-primary/5 p-3 my-1">
      <div className="mb-2">
        <span className="text-sm font-medium text-foreground">
          {schema.key}
        </span>
        <p className="text-xs text-muted-foreground">{schema.description}</p>
      </div>

      {/* Scope selector */}
      <div className="mb-2">
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Scope
        </label>
        <select
          value={editScope}
          onChange={(e) => setEditScope(e.target.value)}
          className="rounded-md border border-input bg-background px-2 py-1 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        >
          <option value="global">Global</option>
          <option value="project">Project</option>
        </select>
      </div>

      {/* Value input based on type */}
      <div className="mb-2">
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Value
        </label>
        {renderValueInput({
          schema,
          isPrompt,
          editValue,
          setEditValue,
          onKeyDown: handleKeyDown,
          inputRef,
        })}
      </div>

      {/* Default reference */}
      <p className="mb-3 text-xs text-muted-foreground">
        Default:{" "}
        <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
          {formatValue(schema.default_value).length > 100
            ? formatValue(schema.default_value).slice(0, 100) + "..."
            : formatValue(schema.default_value)}
        </code>
      </p>

      {/* Actions */}
      <div className="flex gap-2">
        <button
          type="button"
          onClick={handleSave}
          disabled={saving}
          className="rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {saving ? (
            <span className="flex items-center gap-1.5">
              <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
              Saving...
            </span>
          ) : (
            "Save"
          )}
        </button>
        <button
          type="button"
          onClick={handleCancel}
          className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Parent Group Card
// ---------------------------------------------------------------------------

function ParentGroupCard({
  group,
  itemsByCategory,
  onSave,
  saving,
}: {
  group: ParentGroup;
  itemsByCategory: Map<string, SettingWithSchema[]>;
  onSave: (key: string, value: unknown, scope: string) => void;
  saving: boolean;
}) {
  // Empty sub-sections are dropped silently so the card never shows a heading
  // with nothing under it.
  const populated = useMemo(
    () =>
      group.subSections
        .map((sub) => ({ sub, items: itemsByCategory.get(sub.category) ?? [] }))
        .filter((entry) => entry.items.length > 0),
    [group.subSections, itemsByCategory],
  );

  if (populated.length === 0) return null;

  // When a parent has exactly one sub-section and it has no own label, the
  // parent header already conveys the section identity, so render the items
  // flat with no h3.
  const flatten =
    populated.length === 1 && !populated[0].sub.label && !populated[0].sub.description;

  return (
    <div className="rounded-lg border border-border bg-card shadow-sm">
      <div className="border-b border-border px-5 py-4">
        <h2 className="text-lg font-semibold text-foreground">{group.label}</h2>
        {group.description && (
          <p className="mt-1 text-xs text-muted-foreground">{group.description}</p>
        )}
      </div>
      {flatten ? (
        <div className="divide-y divide-border px-5">
          {populated[0].items.map((item) => (
            <InlineSettingEditor
              key={item.schema.key}
              item={item}
              onSave={onSave}
              saving={saving}
            />
          ))}
        </div>
      ) : (
        <div className="divide-y divide-border">
          {populated.map(({ sub, items }) => {
            const order = BUDGET_BAR_ORDERS[sub.category];
            const segments = order ? fractionSegments(items, order) : [];
            const showBar = segments.length > 0;

            return (
              <section key={sub.category} className="px-5 py-4">
                {sub.label && (
                  <h3 className="text-sm font-semibold text-foreground">{sub.label}</h3>
                )}
                {sub.description && (
                  <p className="mt-1 text-xs text-muted-foreground">{sub.description}</p>
                )}
                {showBar && (
                  <div className="mt-3">
                    <PhaseBudgetBar
                      segments={segments}
                      total={1}
                      format={formatFractionPct}
                      ariaLabel={`${sub.label ?? sub.category} allocation`}
                    />
                  </div>
                )}
                <div className="mt-2 divide-y divide-border">
                  {items.map((item) => (
                    <InlineSettingEditor
                      key={item.schema.key}
                      item={item}
                      onSave={onSave}
                      saving={saving}
                    />
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

function SettingsEditor() {
  const settingsQuery = useSettings();
  const schemaQuery = useSettingsSchema();
  const updateMutation = useUpdateSetting();
  const { available: enrichmentAvailable } = useEnrichmentAvailable();

  const [toast, setToast] = useState<{
    message: string;
    type: "success" | "error";
  } | null>(null);
  const toastTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const showToast = useCallback(
    (message: string, type: "success" | "error") => {
      if (toastTimer.current) clearTimeout(toastTimer.current);
      setToast({ message, type });
      toastTimer.current = setTimeout(() => setToast(null), 3000);
    },
    [],
  );

  const handleSave = useCallback(
    (key: string, value: unknown, scope: string) => {
      updateMutation.mutate(
        { key, value, scope },
        {
          onSuccess: () => showToast(`Saved "${key}"`, "success"),
          onError: (err) =>
            showToast(
              `Failed to save "${key}": ${err.message}`,
              "error",
            ),
        },
      );
    },
    [updateMutation, showToast],
  );

  const isLoading = settingsQuery.isLoading || schemaQuery.isLoading;
  const isError = settingsQuery.isError || schemaQuery.isError;

  const schemas = schemaQuery.data?.data ?? [];
  const settings = settingsQuery.data?.data ?? [];

  // Group settings by their backend category. Prompt keys live on the Prompt
  // Templates page; provider-config keys live on the Provider Configuration
  // page. Both are filtered out so the Settings page is the single source of
  // truth for everything else.
  const itemsByCategory = useMemo(() => {
    const settingsMap = new Map(settings.map((s) => [s.key, s]));
    const out = new Map<string, SettingWithSchema[]>();
    for (const schema of schemas) {
      if (isPromptKey(schema.key)) continue;
      if (MOVED_TO_PROVIDER_CONFIG.has(schema.key)) continue;
      const cat = schema.category || "other";
      const merged: SettingWithSchema = {
        schema,
        setting: settingsMap.get(schema.key) ?? null,
      };
      const list = out.get(cat);
      if (list) {
        list.push(merged);
      } else {
        out.set(cat, [merged]);
      }
    }
    return out;
  }, [schemas, settings]);

  // The active database backend gates groups that only matter for one
  // storage path (HNSW is SQLite-only, for instance). useSetupStatus already
  // exposes backend and is queried elsewhere in the app, so this reuses the
  // react-query cache. While the lookup is in flight we default to permissive
  // — the alternative is a brief flash where applicable groups disappear.
  const { data: setupStatus } = useSetupStatus();
  const activeBackend = setupStatus?.backend ?? "";

  const visibleGroups = useMemo(
    () =>
      PARENT_GROUPS.filter((g) => {
        if (g.requiresEnrichment && !enrichmentAvailable) return false;
        if (g.requiresBackend && activeBackend && !g.requiresBackend.includes(activeBackend)) {
          return false;
        }
        return true;
      }),
    [enrichmentAvailable, activeBackend],
  );

  return (
    <div>
      {/* Page header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          System configuration. Changes take effect immediately unless a setting is flagged as requiring a server restart.
        </p>
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <FontAwesomeIcon icon={faSpinner} spin className="h-8 w-8 text-muted-foreground" />
        </div>
      )}

      {/* Error state */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
          <p className="text-sm text-destructive">
            Failed to load settings. Please try refreshing the page.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (
        <div className="space-y-6">
          {/* Empty state */}
          {schemas.length === 0 && (
            <div className="rounded-lg border border-border bg-card p-8 text-center">
              <p className="text-sm text-muted-foreground">
                No settings defined. Settings will appear here once the system
                has been configured.
              </p>
            </div>
          )}

          {/* Parent group cards */}
          {visibleGroups.map((group) => (
            <ParentGroupCard
              key={group.id}
              group={group}
              itemsByCategory={itemsByCategory}
              onSave={handleSave}
              saving={updateMutation.isPending}
            />
          ))}
        </div>
      )}

      {/* Toast notification */}
      {toast && <StatusToast message={toast.message} type={toast.type} />}
    </div>
  );
}

export default SettingsEditor;
