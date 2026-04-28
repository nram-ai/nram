import { useState, useCallback, useRef, useEffect } from "react";
import {
  useSettings,
  useSettingsSchema,
  useUpdateSetting,
} from "../hooks/useApi";
import type { Setting, SettingSchema } from "../api/client";

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

const CATEGORY_ORDER = [
  "memory",
  "enrichment",
  "enrichment_ingestion",
  "dreaming",
  "dreaming_novelty",
  "dreaming_consolidation",
  "dreaming_contradiction",
  "dreaming_prompts",
  "reconsolidation",
  "recall_fusion",
  "ranking",
  "api",
  "auth",
  "qdrant",
  "enrichment_prompts",
];

const CATEGORY_LABELS: Record<string, string> = {
  memory: "Memory",
  enrichment: "Enrichment",
  enrichment_ingestion: "Enrichment — Ingestion Decision",
  dreaming: "Dreaming",
  dreaming_novelty: "Dreaming — Novelty Audit",
  dreaming_consolidation: "Dreaming — Consolidation Budget",
  dreaming_contradiction: "Dreaming — Contradiction Detection",
  dreaming_prompts: "Dreaming — Prompts",
  reconsolidation: "Reconsolidation",
  recall_fusion: "Recall — Hybrid Fusion",
  ranking: "Ranking",
  api: "API",
  auth: "Auth",
  qdrant: "Qdrant Vector Database",
  enrichment_prompts: "Enrichment — Prompts",
};

const CATEGORY_DESCRIPTIONS: Record<string, string> = {
  memory: "Default values and retention for memory storage",
  enrichment: "Configuration for LLM-based enrichment pipeline",
  enrichment_ingestion: "LLM-judged ADD / UPDATE / DELETE / NONE decision on near-duplicate matches at ingest. Off by default — flip enabled with shadow_mode on first to log the decision distribution before allowing UPDATE/DELETE to take effect.",
  dreaming: "Background consolidation scheduler, token budgets, and synthesis thresholds",
  dreaming_novelty: "Gates whether dream syntheses are kept based on how much genuinely new content they introduce over their sources",
  dreaming_consolidation: "Per-sub-phase budget fractions so audit, reinforce, and consolidate cannot starve each other",
  dreaming_contradiction: "Per-cycle cap on pair-comparison LLM calls in the contradiction phase",
  dreaming_prompts: "Prompt templates used by the dreaming phases (contradiction detection, synthesis, alignment scoring, novelty audit). Use the test harness to validate placeholder substitution before saving.",
  reconsolidation: "Recall-time reinforcement and sleep-time confidence decay for stored memories",
  recall_fusion: "Parallel vector + lexical (BM25/tsvector) retrieval with Reciprocal Rank Fusion. Off by default — flip enabled after migrations are applied.",
  ranking: "Weights and thresholds for memory ranking",
  api: "API rate limiting and request configuration",
  auth: "Authentication and authorization settings",
  qdrant: "Connection settings for the Qdrant vector database. Changes require a server restart to take effect.",
  enrichment_prompts: "Prompt templates used by the enrichment pipeline. Use the test harness to validate placeholder substitution before saving.",
};

const PROMPT_KEYS = new Set([
  "enrichment.extraction_prompt",
  "enrichment.entity_prompt",
  "enrichment.fact_extraction_prompt",
  "enrichment.entity_extraction_prompt",
  "enrichment.ingestion_decision.prompt",
  "dreaming.contradiction_prompt",
  "dreaming.synthesis_prompt",
  "dreaming.alignment_prompt",
  "dreaming.novelty.judge_prompt",
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
          ? "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300"
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
      className={`fixed bottom-4 right-4 z-50 flex items-center gap-2 rounded-md px-4 py-2.5 text-sm font-medium shadow-lg transition-all ${
        type === "success"
          ? "bg-green-50 text-green-800 dark:bg-green-900/80 dark:text-green-200"
          : "bg-red-50 text-red-800 dark:bg-red-900/80 dark:text-red-200"
      }`}
    >
      {type === "success" ? (
        <svg
          className="h-4 w-4 flex-shrink-0"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M5 13l4 4L19 7"
          />
        </svg>
      ) : (
        <svg
          className="h-4 w-4 flex-shrink-0"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M6 18L18 6M6 6l12 12"
          />
        </svg>
      )}
      {message}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Toggle Switch
// ---------------------------------------------------------------------------

function Toggle({
  checked,
  onChange,
  disabled,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className={`relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 ${
        checked ? "bg-primary" : "bg-gray-300 dark:bg-gray-600"
      } ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
    >
      <span
        className={`pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${
          checked ? "translate-x-5" : "translate-x-0"
        }`}
      />
    </button>
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
    return (
      <input
        ref={inputRef as React.RefObject<HTMLInputElement>}
        type="number"
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        step={schema.type === "float" ? "0.01" : "1"}
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

  const requiresRestart = schema.description?.toLowerCase().includes("restart");

  // Bool toggle (no edit mode needed)
  if ((schema.type === "bool" || schema.type === "boolean") && !editing) {
    const boolVal = currentValue === true || currentValue === "true";
    return (
      <div className="flex items-center justify-between py-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-foreground">
              {schema.key}
            </span>
            <ScopeBadge scope={currentScope} />
            {isDefault && (
              <span className="text-xs text-muted-foreground">(default)</span>
            )}
            {requiresRestart && (
              <span className="inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900 dark:text-amber-300">
                Requires restart
              </span>
            )}
          </div>
          <p className="mt-0.5 text-xs text-muted-foreground">
            {schema.description}
          </p>
        </div>
        <Toggle
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
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-foreground">
              {schema.key}
            </span>
            <ScopeBadge scope={currentScope} />
            {isDefault && (
              <span className="text-xs text-muted-foreground">(default)</span>
            )}
            {requiresRestart && (
              <span className="inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900 dark:text-amber-300">
                Requires restart
              </span>
            )}
          </div>
          <p className="mt-0.5 text-xs text-muted-foreground">
            {schema.description}
          </p>
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
              <svg
                className="h-3.5 w-3.5 animate-spin"
                fill="none"
                viewBox="0 0 24 24"
              >
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                />
              </svg>
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
// Prompt Editor Section
// ---------------------------------------------------------------------------

function PromptEditorSection({
  items,
  onSave,
  saving,
}: {
  items: SettingWithSchema[];
  onSave: (key: string, value: unknown, scope: string) => void;
  saving: boolean;
}) {
  const [testKey, setTestKey] = useState<string | null>(null);
  const [testInput, setTestInput] = useState("");
  const [testOutput, setTestOutput] = useState<string | null>(null);
  const [testRunning, setTestRunning] = useState(false);

  const handleTest = useCallback(
    async (key: string) => {
      if (!testInput.trim()) return;
      setTestKey(key);
      setTestRunning(true);
      setTestOutput(null);

      try {
        const token = localStorage.getItem("nram_token");
        const headers: Record<string, string> = {
          "Content-Type": "application/json",
        };
        if (token) {
          headers["Authorization"] = `Bearer ${token}`;
        }

        const res = await fetch("/v1/admin/settings/test-prompt", {
          method: "POST",
          headers,
          body: JSON.stringify({
            key,
            sample_input: testInput,
          }),
        });

        if (!res.ok) {
          const err = await res.text();
          setTestOutput(`Error: ${err}`);
        } else {
          const data = await res.json();
          setTestOutput(
            typeof data.output === "string"
              ? data.output
              : JSON.stringify(data, null, 2),
          );
        }
      } catch (err) {
        setTestOutput(
          `Error: ${err instanceof Error ? err.message : "Request failed"}`,
        );
      } finally {
        setTestRunning(false);
      }
    },
    [testInput],
  );

  if (items.length === 0) return null;

  return (
    <div className="rounded-lg border border-border bg-card shadow-sm">
      <div className="border-b border-border px-5 py-4">
        <h2 className="text-lg font-semibold text-foreground">
          Extraction Prompts
        </h2>
        <p className="mt-1 text-xs text-muted-foreground">
          Prompts used by the enrichment pipeline to extract facts and entities
          from memories. Use the test button to validate against sample input.
        </p>
      </div>
      <div className="divide-y divide-border px-5">
        {items.map((item) => (
          <div key={item.schema.key} className="py-4">
            <InlineSettingEditor item={item} onSave={onSave} saving={saving} />

            {/* Test section */}
            <div className="mt-3 space-y-2">
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  value={testKey === item.schema.key ? testInput : ""}
                  onChange={(e) => {
                    setTestKey(item.schema.key);
                    setTestInput(e.target.value);
                  }}
                  onFocus={() => {
                    if (testKey !== item.schema.key) {
                      setTestKey(item.schema.key);
                      setTestInput("");
                      setTestOutput(null);
                    }
                  }}
                  placeholder="Enter sample text to test extraction..."
                  className="flex-1 rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
                />
                <button
                  type="button"
                  onClick={() => handleTest(item.schema.key)}
                  disabled={
                    testRunning ||
                    !testInput.trim() ||
                    testKey !== item.schema.key
                  }
                  className="rounded-md bg-orange-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {testRunning && testKey === item.schema.key ? (
                    <span className="flex items-center gap-1.5">
                      <svg
                        className="h-3.5 w-3.5 animate-spin"
                        fill="none"
                        viewBox="0 0 24 24"
                      >
                        <circle
                          className="opacity-25"
                          cx="12"
                          cy="12"
                          r="10"
                          stroke="currentColor"
                          strokeWidth="4"
                        />
                        <path
                          className="opacity-75"
                          fill="currentColor"
                          d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                        />
                      </svg>
                      Testing...
                    </span>
                  ) : (
                    "Test with sample input"
                  )}
                </button>
              </div>
              {testOutput !== null && testKey === item.schema.key && (
                <pre className="max-h-48 overflow-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                  {testOutput}
                </pre>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Category Card
// ---------------------------------------------------------------------------

function CategoryCard({
  category,
  items,
  onSave,
  saving,
}: {
  category: string;
  items: SettingWithSchema[];
  onSave: (key: string, value: unknown, scope: string) => void;
  saving: boolean;
}) {
  const label = CATEGORY_LABELS[category] || category;
  const description = CATEGORY_DESCRIPTIONS[category] || "";

  // Filter out prompt keys from standard display (they get their own section)
  const standardItems = items.filter((i) => !isPromptKey(i.schema.key));

  if (standardItems.length === 0) return null;

  return (
    <div className="rounded-lg border border-border bg-card shadow-sm">
      <div className="border-b border-border px-5 py-4">
        <h2 className="text-lg font-semibold text-foreground">{label}</h2>
        {description && (
          <p className="mt-1 text-xs text-muted-foreground">{description}</p>
        )}
      </div>
      <div className="divide-y divide-border px-5">
        {standardItems.map((item) => (
          <InlineSettingEditor
            key={item.schema.key}
            item={item}
            onSave={onSave}
            saving={saving}
          />
        ))}
      </div>
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

  // Build merged data: schema + current values
  const allSchemas = schemaQuery.data?.data ?? [];
  const settings = settingsQuery.data?.data ?? [];
  const settingsMap = new Map(settings.map((s) => [s.key, s]));

  const schemas = allSchemas;

  // Group by category
  const categoryMap = new Map<string, SettingWithSchema[]>();
  const promptItems: SettingWithSchema[] = [];

  for (const schema of schemas) {
    const merged: SettingWithSchema = {
      schema,
      setting: settingsMap.get(schema.key) ?? null,
    };

    if (isPromptKey(schema.key)) {
      promptItems.push(merged);
    }

    const cat = schema.category || "other";
    if (!categoryMap.has(cat)) {
      categoryMap.set(cat, []);
    }
    categoryMap.get(cat)!.push(merged);
  }

  // Sort categories by defined order, then alphabetically for any extras
  const orderedCategories = Array.from(categoryMap.keys()).sort((a, b) => {
    const ai = CATEGORY_ORDER.indexOf(a);
    const bi = CATEGORY_ORDER.indexOf(b);
    if (ai !== -1 && bi !== -1) return ai - bi;
    if (ai !== -1) return -1;
    if (bi !== -1) return 1;
    return a.localeCompare(b);
  });

  return (
    <div>
      {/* Page header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          System settings and configuration. Changes take effect immediately.
        </p>
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <svg
            className="h-8 w-8 animate-spin text-muted-foreground"
            fill="none"
            viewBox="0 0 24 24"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
        </div>
      )}

      {/* Error state */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-900/30">
          <p className="text-sm text-red-800 dark:text-red-300">
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

          {/* Category cards */}
          {orderedCategories.map((cat) => (
            <CategoryCard
              key={cat}
              category={cat}
              items={categoryMap.get(cat)!}
              onSave={handleSave}
              saving={updateMutation.isPending}
            />
          ))}

          {/* Extraction Prompts section */}
          {promptItems.length > 0 && (
            <PromptEditorSection
              items={promptItems}
              onSave={handleSave}
              saving={updateMutation.isPending}
            />
          )}
        </div>
      )}

      {/* Toast notification */}
      {toast && <StatusToast message={toast.message} type={toast.type} />}
    </div>
  );
}

export default SettingsEditor;
