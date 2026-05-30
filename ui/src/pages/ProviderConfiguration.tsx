import { useState, useCallback, useEffect } from "react";
import {
  useProviderSlots,
  useUpdateProviderSlot,
  useTestProviderSlot,
  useOllamaModels,
  usePullOllamaModel,
  useSettings,
  useUpdateSetting,
} from "../hooks/useApi";
import type {
  ProviderSlot,
  UpdateProviderSlotRequest,
  UpdateProviderSlotResult,
  TestProviderResult,
  OllamaModel,
} from "../api/client";
import { APIError } from "../api/client";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCheck, faXmark, faSpinner } from "../lib/icons";

const INGESTION_MODEL_SETTING_KEY = "enrichment.ingestion_decision.model";
const QUERY_AUGMENT_MODEL_SETTING_KEY = "enrichment.query_augment.model";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SLOT_LABELS: Record<string, string> = {
  embedding: "Embedding",
  fact: "Fact Extraction",
  entity: "Entity Extraction",
};

const SLOT_DESCRIPTIONS: Record<string, string> = {
  embedding: "Generates vector embeddings for semantic search",
  fact: "Extracts structured facts from stored memories",
  entity: "Identifies entities and relationships in content",
};

const PROVIDER_TYPES = [
  "openai",
  "ollama",
  "gemini",
  "anthropic",
  "openrouter",
  "custom",
] as const;

const PROVIDER_BADGE_COLORS: Record<string, string> = {
  openai:
    "bg-success/10 text-success",
  ollama:
    "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300",
  gemini: "bg-info/10 text-info",
  anthropic:
    "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300",
  openrouter:
    "bg-cyan-100 text-cyan-800 dark:bg-cyan-900 dark:text-cyan-300",
  custom: "bg-muted text-muted-foreground",
};

const DEFAULT_URLS: Record<string, string> = {
  openai: "https://api.openai.com/v1",
  ollama: "http://localhost:11434",
  gemini: "https://generativelanguage.googleapis.com/v1beta",
  anthropic: "https://api.anthropic.com/v1",
  openrouter: "https://openrouter.ai/api/v1",
  custom: "",
};

const CLOUD_PROVIDERS = new Set(["openai", "gemini", "anthropic", "openrouter"]);

// Model suggestions per provider type, keyed by slot name.
const MODEL_HINTS: Record<string, Record<string, string>> = {
  openai: {
    embedding: "e.g. text-embedding-3-small",
    fact: "e.g. gpt-4o-mini",
    entity: "e.g. gpt-4o-mini",
  },
  ollama: {
    embedding: "e.g. qwen3-embedding:0.6b — avoid nomic-embed-text (2048-tok ctx silently truncates long memories). See README → Recommended Models.",
    fact: "e.g. qwen3:8b, llama3.1, gemma2",
    entity: "e.g. qwen3:8b, llama3.1, gemma2",
  },
  gemini: {
    embedding: "e.g. text-embedding-004",
    fact: "e.g. gemini-2.0-flash",
    entity: "e.g. gemini-2.0-flash",
  },
  anthropic: {
    embedding: "Not supported — use OpenAI or Ollama",
    fact: "e.g. claude-sonnet-4-6-20250514",
    entity: "e.g. claude-haiku-4-5-20251001",
  },
  openrouter: {
    embedding: "e.g. openai/text-embedding-3-small",
    fact: "e.g. anthropic/claude-sonnet-4-6",
    entity: "e.g. anthropic/claude-haiku-4-5",
  },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function maskUrl(url: string): string {
  try {
    const u = new URL(url);
    if (
      u.hostname === "localhost" ||
      u.hostname === "127.0.0.1" ||
      u.hostname === "0.0.0.0"
    ) {
      return url;
    }
    return `${u.protocol}//${u.hostname}/***`;
  } catch {
    return url;
  }
}

// ---------------------------------------------------------------------------
// Status Indicator
// ---------------------------------------------------------------------------

function StatusDot({
  configured,
  healthy,
}: {
  configured: boolean;
  healthy: boolean;
}) {
  if (!configured) {
    return (
      <span className="inline-block h-3 w-3 rounded-full bg-muted-foreground" />
    );
  }
  return (
    <span
      className={`inline-block h-3 w-3 rounded-full ${
        healthy
          ? "bg-success"
          : "bg-destructive"
      }`}
    />
  );
}

// ---------------------------------------------------------------------------
// Test Result Display
// ---------------------------------------------------------------------------

function TestResultDisplay({ result }: { result: TestProviderResult }) {
  if (result.success) {
    return (
      <div className="mt-2 flex items-center gap-2 rounded-md bg-success/10 px-3 py-2 text-sm text-success">
        <FontAwesomeIcon icon={faCheck} className="h-4 w-4 flex-shrink-0" />
        Connection successful ({result.latency_ms}ms)
      </div>
    );
  }
  return (
    <div className="mt-2 flex items-center gap-2 rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
      <FontAwesomeIcon icon={faXmark} className="h-4 w-4 flex-shrink-0" />
      {result.message || "Connection failed"}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Ollama Model Picker
// ---------------------------------------------------------------------------

// Heuristic: model names containing these substrings are embedding-only models.
const EMBEDDING_MODEL_PATTERNS = ["embed", "bge-", "e5-", "gte-", "minilm", "arctic-embed"];

function isEmbeddingModel(name: string): boolean {
  const lower = name.toLowerCase();
  return EMBEDDING_MODEL_PATTERNS.some((p) => lower.includes(p));
}

function OllamaModelPicker({
  ollamaUrl,
  selectedModel,
  onSelectModel,
  slotName,
}: {
  ollamaUrl: string;
  selectedModel: string;
  onSelectModel: (model: string) => void;
  slotName: string;
}) {
  const isEmbeddingSlot = slotName === "embedding";
  const ollamaModelsQuery = useOllamaModels(ollamaUrl);
  const pullMutation = usePullOllamaModel();
  const [pullModelName, setPullModelName] = useState("");

  const loadModels = useCallback(() => {
    ollamaModelsQuery.refetch();
  }, [ollamaModelsQuery]);

  const handlePull = useCallback(() => {
    if (!pullModelName.trim()) return;
    pullMutation.mutate({ model: pullModelName.trim(), ollamaUrl }, {
      onSuccess: () => {
        setPullModelName("");
        ollamaModelsQuery.refetch();
      },
    });
  }, [pullModelName, pullMutation, ollamaModelsQuery, ollamaUrl]);

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={loadModels}
          disabled={!ollamaUrl || ollamaModelsQuery.isFetching}
          className="rounded-md bg-orange-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {ollamaModelsQuery.isFetching ? (
            <span className="flex items-center gap-1.5">
              <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
              Loading...
            </span>
          ) : (
            "Load Models"
          )}
        </button>
      </div>

      {ollamaModelsQuery.isError && (
        <p className="text-sm text-destructive">
          Failed to load models. Ensure Ollama is running at {ollamaUrl}.
        </p>
      )}

      {ollamaModelsQuery.data && (
        <div className="space-y-2">
          <p className="text-xs font-medium text-muted-foreground">
            Available Models ({(ollamaModelsQuery.data?.models ?? []).length})
          </p>
          <div className="max-h-48 space-y-1 overflow-y-auto rounded-md border border-border p-1">
            {(ollamaModelsQuery.data?.models ?? []).length === 0 ? (
              <p className="px-2 py-3 text-center text-sm text-muted-foreground">
                No models found. Pull a model below.
              </p>
            ) : (
              (ollamaModelsQuery.data?.models ?? []).map((m: OllamaModel) => {
                const isEmbed = isEmbeddingModel(m.name);
                const mismatch = isEmbeddingSlot ? !isEmbed : isEmbed;
                return (
                  <button
                    key={m.name}
                    type="button"
                    onClick={() => onSelectModel(m.name)}
                    className={`w-full rounded-md px-3 py-2 text-left text-sm transition-colors ${
                      selectedModel === m.name
                        ? "bg-orange-100 text-orange-900 dark:bg-orange-900/40 dark:text-orange-200"
                        : mismatch
                          ? "opacity-50 hover:bg-muted"
                          : "hover:bg-muted"
                    }`}
                  >
                    <span className="font-medium">{m.name}</span>
                    <span className="ml-2 text-xs text-muted-foreground">
                      {(m.size / (1024 * 1024 * 1024)).toFixed(1)} GB
                    </span>
                    {isEmbed && (
                      <span className="ml-2 inline-flex items-center rounded-full bg-info/20 px-1.5 py-0.5 text-[10px] font-medium text-info">
                        embedding
                      </span>
                    )}
                    {!isEmbed && (
                      <span className="ml-2 inline-flex items-center rounded-full bg-violet-100 px-1.5 py-0.5 text-[10px] font-medium text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">
                        chat
                      </span>
                    )}
                    {mismatch && (
                      <span className="ml-1 text-[10px] text-warning">
                        (wrong type for this slot)
                      </span>
                    )}
                  </button>
                );
              })
            )}
          </div>
        </div>
      )}

      <div className="flex items-center gap-2">
        <input
          type="text"
          value={pullModelName}
          onChange={(e) => setPullModelName(e.target.value)}
          placeholder="Model name to pull (e.g. llama3:8b)"
          className="flex-1 rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
        <button
          type="button"
          onClick={handlePull}
          disabled={!pullModelName.trim() || pullMutation.isPending}
          className="rounded-md bg-orange-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {pullMutation.isPending ? (
            <span className="flex items-center gap-1.5">
              <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
              Pulling...
            </span>
          ) : (
            "Pull Model"
          )}
        </button>
      </div>
      {pullMutation.isError && (
        <p className="text-sm text-destructive">
          Failed to pull model: {(pullMutation.error as Error).message}
        </p>
      )}
      {pullMutation.isSuccess && (
        <p className="text-sm text-success">
          Model pulled successfully.
        </p>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Provider Slot Edit Form
// ---------------------------------------------------------------------------

interface EditFormState {
  type: string;
  url: string;
  model: string;
  api_key: string;
  timeout: string;
}

function ProviderSlotEditForm({
  slotName,
  initial,
  wasConfigured,
  onSave,
  onCancel,
  saving,
}: {
  slotName: string;
  initial: EditFormState;
  isEmbedding: boolean;
  wasConfigured: boolean;
  onSave: (data: UpdateProviderSlotRequest) => void;
  onCancel: (() => void) | null;
  saving: boolean;
}) {
  const [form, setForm] = useState<EditFormState>(initial);
  const modelPlaceholder = MODEL_HINTS[form.type]?.[slotName] || "e.g. model-name";

  const isCloud = CLOUD_PROVIDERS.has(form.type);
  const isOllama = form.type === "ollama";

  const handleTypeChange = (newType: string) => {
    setForm((prev) => ({
      ...prev,
      type: newType,
      url: DEFAULT_URLS[newType] || "",
      model: "",
      api_key: "",
      timeout: "",
    }));
  };

  // Submit without confirm_invalidate. The server returns 409 + row counts
  // when the embedding-model swap would invalidate stored vectors; the
  // parent ProviderSlotCard catches that, shows a destructive-action
  // modal, and re-submits with confirm_invalidate=true on confirm.
  const handleSave = () => {
    const req: UpdateProviderSlotRequest = {
      type: form.type,
      url: form.url,
      model: form.model,
    };
    if (form.api_key) {
      req.api_key = form.api_key;
    }
    if (form.timeout) {
      req.timeout = parseInt(form.timeout, 10);
    }
    onSave(req);
  };

  return (
    <div className="space-y-4">
      {/* Provider Type */}
      <div>
        <label className="mb-1 block text-sm font-medium text-foreground">
          Provider Type
        </label>
        <select
          value={form.type}
          onChange={(e) => handleTypeChange(e.target.value)}
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        >
          <option value="">Select a provider...</option>
          {PROVIDER_TYPES.map((t) => (
            <option key={t} value={t}>
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </option>
          ))}
        </select>
      </div>

      {/* URL */}
      <div>
        <label className="mb-1 block text-sm font-medium text-foreground">
          URL
        </label>
        <input
          type="text"
          value={form.url}
          onChange={(e) => setForm((p) => ({ ...p, url: e.target.value }))}
          placeholder="https://..."
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
      </div>

      {/* Model */}
      <div>
        <label className="mb-1 block text-sm font-medium text-foreground">
          Model
        </label>
        {isOllama ? (
          <div className="space-y-3">
            <input
              type="text"
              value={form.model}
              onChange={(e) =>
                setForm((p) => ({ ...p, model: e.target.value }))
              }
              placeholder={modelPlaceholder}
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
            <OllamaModelPicker
              ollamaUrl={form.url}
              selectedModel={form.model}
              onSelectModel={(m) => setForm((p) => ({ ...p, model: m }))}
              slotName={slotName}
            />
          </div>
        ) : (
          <input
            type="text"
            value={form.model}
            onChange={(e) =>
              setForm((p) => ({ ...p, model: e.target.value }))
            }
            placeholder={modelPlaceholder}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          />
        )}
      </div>

      {/* API Key (cloud only) */}
      {isCloud && (
        <div>
          <label className="mb-1 block text-sm font-medium text-foreground">
            API Key
          </label>
          <input
            type="password"
            value={form.api_key}
            onChange={(e) =>
              setForm((p) => ({ ...p, api_key: e.target.value }))
            }
            placeholder={wasConfigured ? "Leave blank to keep current key" : "sk-..."}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          />
        </div>
      )}

      {/* Timeout */}
      <div>
        <label className="mb-1 block text-sm font-medium text-foreground">
          Timeout (seconds)
        </label>
        <input
          type="number"
          value={form.timeout}
          onChange={(e) =>
            setForm((p) => ({ ...p, timeout: e.target.value }))
          }
          placeholder="300"
          min={5}
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
        <p className="mt-1 text-xs text-muted-foreground">
          HTTP request timeout for LLM calls. Increase for local models (Ollama) or large prompts. Default: 300 seconds.
        </p>
        {form.type === "ollama" && (
          <p className="mt-1 text-xs text-muted-foreground">
            Slow hardware tip: set <code className="rounded bg-muted px-1 py-0.5">OLLAMA_KEEP_ALIVE=168h</code> in the Ollama server's environment to keep loaded models pinned for a week. nram cannot set this per-call because Ollama's OpenAI-compatible endpoint ignores <code className="rounded bg-muted px-1 py-0.5">keep_alive</code>.
          </p>
        )}
      </div>

      {/* Actions */}
      <div className="flex gap-2">
        <button
          type="button"
          onClick={handleSave}
          disabled={!form.type || !form.url || !form.model || saving}
          className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
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
        {onCancel && (
          <button
            type="button"
            onClick={onCancel}
            className="rounded-md border border-input px-4 py-2 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
          >
            Cancel
          </button>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Fact-slot model overrides (Ingestion Decision, Query Augmentation)
// ---------------------------------------------------------------------------

// IngestionDecisionModelOverride reads and writes the
// enrichment.ingestion_decision.model setting. The ingestion-decision phase
// runs against the Fact provider; this override only swaps the model name —
// it does not configure a separate provider slot. Empty value falls back to
// the Fact slot's model.
function IngestionDecisionModelOverride({
  factSlot,
}: {
  factSlot: ProviderSlot;
}) {
  const settingsQuery = useSettings();
  const updateSetting = useUpdateSetting();
  const [editValue, setEditValue] = useState("");

  const stored = settingsQuery.data?.data?.find(
    (s) => s.key === INGESTION_MODEL_SETTING_KEY,
  );
  const storedValue =
    typeof stored?.value === "string" ? stored.value : "";

  useEffect(() => {
    setEditValue(storedValue);
  }, [storedValue]);

  const isOllama = factSlot.type === "ollama";
  const placeholder = factSlot.model
    ? `Falls back to Fact slot model: ${factSlot.model}`
    : "Empty falls back to Fact slot model";
  const dirty = editValue !== storedValue;

  const writeValue = useCallback(
    (value: string) => {
      updateSetting.mutate({
        key: INGESTION_MODEL_SETTING_KEY,
        value,
      });
    },
    [updateSetting],
  );

  const handleSave = useCallback(
    () => writeValue(editValue.trim()),
    [editValue, writeValue],
  );

  const handleClear = useCallback(() => {
    setEditValue("");
    writeValue("");
  }, [writeValue]);

  return (
    <div className="mt-4 rounded-md border border-border bg-muted/30 px-4 py-3">
      <div className="mb-2 flex items-center justify-between">
        <div>
          <h4 className="text-sm font-medium text-foreground">
            Ingestion Decision Model Override
          </h4>
          <p className="mt-0.5 text-xs text-muted-foreground">
            The ingestion-decision phase reuses this Fact provider. Override
            just the model name (categorisation is a small-model task);
            empty falls back to the Fact slot's model.
          </p>
        </div>
        {storedValue && (
          <span className="inline-flex items-center rounded-full bg-info/20 px-2 py-0.5 text-xs font-medium text-info">
            Override active
          </span>
        )}
      </div>
      {isOllama ? (
        <div className="space-y-2">
          <input
            type="text"
            value={editValue}
            onChange={(e) => setEditValue(e.target.value)}
            placeholder={placeholder}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          />
          {factSlot.url && (
            <OllamaModelPicker
              ollamaUrl={factSlot.url}
              selectedModel={editValue}
              onSelectModel={(m) => setEditValue(m)}
              slotName="fact"
            />
          )}
        </div>
      ) : (
        <input
          type="text"
          value={editValue}
          onChange={(e) => setEditValue(e.target.value)}
          placeholder={placeholder}
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
      )}
      <div className="mt-2 flex items-center gap-2">
        <button
          type="button"
          onClick={handleSave}
          disabled={!dirty || updateSetting.isPending}
          className="rounded-md bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {updateSetting.isPending ? "Saving..." : "Save Override"}
        </button>
        {storedValue && (
          <button
            type="button"
            onClick={handleClear}
            disabled={updateSetting.isPending}
            className="rounded-md border border-input px-3 py-1.5 text-xs font-medium text-muted-foreground shadow-sm hover:bg-muted disabled:opacity-50"
          >
            Clear Override
          </button>
        )}
        {updateSetting.isSuccess && !dirty && (
          <span className="text-xs text-success">
            Saved
          </span>
        )}
        {updateSetting.isError && (
          <span className="text-xs text-destructive">
            {(updateSetting.error as Error).message}
          </span>
        )}
      </div>
    </div>
  );
}

// QueryAugmentModelOverride is a structural twin of
// IngestionDecisionModelOverride: same hook ordering, edit-state pattern,
// Ollama-aware dual-input shape, classNames, "Override active" pill, and
// Save/Clear buttons. Only the setting key, title, and description differ.
// The query-augmentation phase reuses the Fact provider at runtime (see
// internal/enrichment/phase_query_augment.go) — this override swaps the model
// name only.
function QueryAugmentModelOverride({
  factSlot,
}: {
  factSlot: ProviderSlot;
}) {
  const settingsQuery = useSettings();
  const updateSetting = useUpdateSetting();
  const [editValue, setEditValue] = useState("");

  const stored = settingsQuery.data?.data?.find(
    (s) => s.key === QUERY_AUGMENT_MODEL_SETTING_KEY,
  );
  const storedValue =
    typeof stored?.value === "string" ? stored.value : "";

  useEffect(() => {
    setEditValue(storedValue);
  }, [storedValue]);

  const isOllama = factSlot.type === "ollama";
  const placeholder = factSlot.model
    ? `Falls back to Fact slot model: ${factSlot.model}`
    : "Empty falls back to Fact slot model";
  const dirty = editValue !== storedValue;

  const writeValue = useCallback(
    (value: string) => {
      updateSetting.mutate({
        key: QUERY_AUGMENT_MODEL_SETTING_KEY,
        value,
      });
    },
    [updateSetting],
  );

  const handleSave = useCallback(
    () => writeValue(editValue.trim()),
    [editValue, writeValue],
  );

  const handleClear = useCallback(() => {
    setEditValue("");
    writeValue("");
  }, [writeValue]);

  return (
    <div className="mt-4 rounded-md border border-border bg-muted/30 px-4 py-3">
      <div className="mb-2 flex items-center justify-between">
        <div>
          <h4 className="text-sm font-medium text-foreground">
            Query Augmentation Model Override
          </h4>
          <p className="mt-0.5 text-xs text-muted-foreground">
            The query-augmentation phase reuses this Fact provider. Override
            just the model name; empty falls back to the Fact slot's model.
          </p>
        </div>
        {storedValue && (
          <span className="inline-flex items-center rounded-full bg-info/20 px-2 py-0.5 text-xs font-medium text-info">
            Override active
          </span>
        )}
      </div>
      {isOllama ? (
        <div className="space-y-2">
          <input
            type="text"
            value={editValue}
            onChange={(e) => setEditValue(e.target.value)}
            placeholder={placeholder}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          />
          {factSlot.url && (
            <OllamaModelPicker
              ollamaUrl={factSlot.url}
              selectedModel={editValue}
              onSelectModel={(m) => setEditValue(m)}
              slotName="fact"
            />
          )}
        </div>
      ) : (
        <input
          type="text"
          value={editValue}
          onChange={(e) => setEditValue(e.target.value)}
          placeholder={placeholder}
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
      )}
      <div className="mt-2 flex items-center gap-2">
        <button
          type="button"
          onClick={handleSave}
          disabled={!dirty || updateSetting.isPending}
          className="rounded-md bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {updateSetting.isPending ? "Saving..." : "Save Override"}
        </button>
        {storedValue && (
          <button
            type="button"
            onClick={handleClear}
            disabled={updateSetting.isPending}
            className="rounded-md border border-input px-3 py-1.5 text-xs font-medium text-muted-foreground shadow-sm hover:bg-muted disabled:opacity-50"
          >
            Clear Override
          </button>
        )}
        {updateSetting.isSuccess && !dirty && (
          <span className="text-xs text-success">
            Saved
          </span>
        )}
        {updateSetting.isError && (
          <span className="text-xs text-destructive">
            {(updateSetting.error as Error).message}
          </span>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Provider Slot Card
// ---------------------------------------------------------------------------

function ProviderSlotCard({
  slot,
  disabled,
}: {
  slot: ProviderSlot;
  disabled: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [testResult, setTestResult] = useState<TestProviderResult | null>(null);

  const updateMutation = useUpdateProviderSlot();
  const testMutation = useTestProviderSlot();

  const label = SLOT_LABELS[slot.slot] || slot.slot;
  const description = SLOT_DESCRIPTIONS[slot.slot] || "";
  const isEmbedding = slot.slot === "embedding";
  const badgeCls =
    PROVIDER_BADGE_COLORS[slot.type] || PROVIDER_BADGE_COLORS.custom;
  const showModelMax =
    slot.context_window != null &&
    slot.context_window > 0 &&
    slot.context_window_max != null &&
    slot.context_window_max > slot.context_window;

  const handleTest = useCallback(() => {
    setTestResult(null);
    testMutation.mutate(
      {
        slot: slot.slot,
        config: {
          type: slot.type,
          url: slot.url,
          model: slot.model,
        },
      },
      {
        onSuccess: (result) => setTestResult(result),
        onError: () =>
          setTestResult({
            success: false,
            latency_ms: 0,
            message: "Request failed",
          }),
      },
    );
  }, [slot, testMutation]);

  // pendingConfirm is set when the server returns 409 NeedsConfirmation
  // for an embedding-model swap. The modal renders from this state and,
  // on confirm, re-fires the mutation with confirm_invalidate=true.
  const [pendingConfirm, setPendingConfirm] = useState<{
    data: UpdateProviderSlotRequest;
    result: UpdateProviderSlotResult;
  } | null>(null);
  const [cascadeResult, setCascadeResult] = useState<UpdateProviderSlotResult | null>(null);

  const handleSave = useCallback(
    (data: UpdateProviderSlotRequest) => {
      setCascadeResult(null);
      updateMutation.mutate(
        { slot: slot.slot, data },
        {
          onSuccess: (resp) => {
            // Cascade success carries entity_reembed_queued=true and row counts.
            const r = resp as UpdateProviderSlotResult;
            if (r.entity_reembed_queued) {
              setCascadeResult(r);
            }
            setEditing(false);
          },
          onError: (err) => {
            // 409: server is asking for confirmation of a destructive swap.
            // Capture the row counts so the modal can show them, then wait
            // for the user to confirm or cancel.
            if (err instanceof APIError && err.status === 409) {
              const body = err.body as UpdateProviderSlotResult;
              if (body && body.needs_confirmation) {
                setPendingConfirm({ data, result: body });
                return;
              }
            }
            // Other errors fall through to react-query's default isError state.
          },
        },
      );
    },
    [slot.slot, updateMutation],
  );

  const confirmSwitch = useCallback(() => {
    if (!pendingConfirm) return;
    const data: UpdateProviderSlotRequest = {
      ...pendingConfirm.data,
      confirm_invalidate: true,
    };
    setPendingConfirm(null);
    updateMutation.mutate(
      { slot: slot.slot, data },
      {
        onSuccess: (resp) => {
          const r = resp as UpdateProviderSlotResult;
          if (r.entity_reembed_queued) {
            setCascadeResult(r);
          }
          setEditing(false);
        },
      },
    );
  }, [pendingConfirm, slot.slot, updateMutation]);

  const initialFormState: EditFormState = {
    type: slot.configured ? slot.type : "",
    url: slot.configured ? slot.url : "",
    model: slot.configured ? slot.model : "",
    api_key: "",
    timeout: slot.timeout != null ? String(slot.timeout) : "",
  };

  return (
    <div
      className={`rounded-lg border border-border bg-card shadow-sm ${
        disabled ? "pointer-events-none opacity-50" : ""
      }`}
    >
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-5 py-4">
        <div className="flex items-center gap-3">
          <StatusDot configured={slot.configured} healthy={slot.status === "ok"} />
          <div>
            <h3 className="text-sm font-semibold text-foreground">{label}</h3>
            <p className="text-xs text-muted-foreground">{description}</p>
          </div>
        </div>
        {slot.configured && !editing && (
          <span
            className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${badgeCls}`}
          >
            {slot.type}
          </span>
        )}
      </div>

      {/* Body */}
      <div className="px-5 py-4">
        {/* Cascade success banner: shown after a confirmed model switch
            so the operator knows re-embedding is in flight. The memory
            queue drains in the background; the entity loop runs in a
            detached goroutine on the server. */}
        {cascadeResult && (
          <div className="mb-4 rounded-md border border-info/40 bg-info/10 p-3">
            <p className="text-sm font-medium text-info">
              Embedding model switched: {cascadeResult.old_model} →{" "}
              {cascadeResult.new_model}
            </p>
            <p className="mt-1 text-xs text-info">
              {cascadeResult.memory_jobs_enqueued ?? 0} memory re-embed jobs
              queued, {cascadeResult.entities_affected ?? 0} entities queued
              for re-embed in the background. Recall is degraded until the
              workers drain (~5–15 min for typical corpora).
            </p>
            <button
              type="button"
              onClick={() => setCascadeResult(null)}
              className="mt-2 text-xs font-medium text-info hover:text-info"
            >
              Dismiss
            </button>
          </div>
        )}

        {/* Destructive-action confirmation modal: server-driven. Pops up
            only when an embedding-model change was attempted and the
            server gated the cascade on confirm_invalidate. */}
        {pendingConfirm && (
          <div className="mb-4 rounded-md border-2 border-destructive/40 bg-destructive/10 p-3">
            <p className="text-sm font-semibold text-destructive">
              Confirm embedding model switch
            </p>
            <p className="mt-1 text-sm text-destructive">
              Switching from{" "}
              <span className="font-mono text-xs">{pendingConfirm.result.old_model}</span>{" "}
              to{" "}
              <span className="font-mono text-xs">{pendingConfirm.result.new_model}</span>{" "}
              will:
            </p>
            <ul className="mt-1 ml-5 list-disc text-xs text-destructive">
              <li>
                Clear all memory and entity vectors across every dimension
                table
              </li>
              <li>
                Invalidate {pendingConfirm.result.memories_affected ?? 0} memory
                vectors and {pendingConfirm.result.entities_affected ?? 0} entity
                vectors
              </li>
              <li>
                Queue every memory and entity for re-embedding under the new
                model
              </li>
            </ul>
            <p className="mt-2 text-xs text-destructive">
              Recall returns no results for unprocessed rows during the
              re-embed window (typically 5–15 minutes).
            </p>
            <div className="mt-2 flex gap-2">
              <button
                type="button"
                onClick={confirmSwitch}
                disabled={updateMutation.isPending}
                className="rounded-md bg-destructive px-3 py-1.5 text-sm font-medium text-white hover:bg-destructive disabled:opacity-50"
              >
                {updateMutation.isPending ? "Switching..." : "Confirm Switch & Re-embed"}
              </button>
              <button
                type="button"
                onClick={() => setPendingConfirm(null)}
                className="rounded-md border border-destructive/40 px-3 py-1.5 text-sm font-medium text-destructive hover:bg-destructive/20"
              >
                Cancel
              </button>
            </div>
          </div>
        )}

        {editing || !slot.configured ? (
          <ProviderSlotEditForm
            slotName={slot.slot}
            initial={initialFormState}
            isEmbedding={isEmbedding}
            wasConfigured={slot.configured}
            onSave={handleSave}
            onCancel={slot.configured ? () => setEditing(false) : null}
            saving={updateMutation.isPending}
          />
        ) : (
          <div className="space-y-3">
            {/* Info rows */}
            <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
              <div>
                <span className="text-muted-foreground">URL</span>
                <p className="font-mono text-xs text-foreground">
                  {maskUrl(slot.url)}
                </p>
              </div>
              <div>
                <span className="text-muted-foreground">Model</span>
                <p className="font-medium text-foreground">{slot.model}</p>
              </div>
              {isEmbedding && slot.dimensions != null && (
                <div>
                  <span className="text-muted-foreground">Dimensions</span>
                  <p className="font-medium text-foreground">
                    {slot.dimensions}
                  </p>
                </div>
              )}
              <div>
                <span
                  className="text-muted-foreground"
                  title={
                    (isEmbedding
                      ? "Maximum input length the model can encode in a single request. Memory content longer than this is silently truncated by the provider — small windows (e.g. 2048) are a frequent cause of degraded recall on long memories."
                      : "Maximum input + output tokens the model can process in a single request. Caps the prompt the enrichment pipeline can build (recall hits + memory content) plus the model's response.") +
                    " For Ollama slots this is the lesser of the model's GGUF max and Ollama's configured num_ctx; the model max is shown beneath it when num_ctx is the binding constraint."
                  }
                >
                  Context
                </span>
                <p className="font-medium text-foreground">
                  {slot.context_window != null && slot.context_window > 0 ? (
                    <>
                      {slot.context_window.toLocaleString()}{" "}
                      <span className="text-xs font-normal text-muted-foreground">
                        tokens
                      </span>
                    </>
                  ) : (
                    <span className="text-xs font-normal text-muted-foreground">
                      see provider docs
                    </span>
                  )}
                </p>
                {showModelMax && (
                  <p className="text-xs font-normal text-muted-foreground">
                    model max {slot.context_window_max!.toLocaleString()}
                  </p>
                )}
              </div>
              <div>
                <span className="text-muted-foreground">Status</span>
                <p className="text-xs text-foreground">
                  {slot.status ?? "unknown"}
                </p>
              </div>
            </div>

            {/* Health info */}
            <div className="flex items-center gap-4 text-xs text-muted-foreground">
              {slot.latency_ms != null && (
                <span>Latency: {slot.latency_ms}ms</span>
              )}
            </div>

            {/* Actions */}
            <div className="flex gap-2 pt-1">
              <button
                type="button"
                onClick={handleTest}
                disabled={testMutation.isPending}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {testMutation.isPending ? (
                  <span className="flex items-center gap-1.5">
                    <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
                    Testing...
                  </span>
                ) : (
                  "Test Connection"
                )}
              </button>
              <button
                type="button"
                onClick={() => {
                  setEditing(true);
                  setTestResult(null);
                }}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
              >
                Change Provider
              </button>
            </div>

            {/* Test result */}
            {testResult && <TestResultDisplay result={testResult} />}

            {/* Update error */}
            {updateMutation.isError && (
              <p className="text-sm text-destructive">
                Failed to update: {(updateMutation.error as Error).message}
              </p>
            )}

            {/* Fact-slot model overrides. Both the ingestion-decision and
                query-augmentation phases reuse the Fact provider at runtime,
                so their model overrides sit next to the host slot rather
                than in the generic Settings page. Same widget pattern; they
                stack in pipeline order (ingestion decision first, query
                augmentation second). */}
            {slot.slot === "fact" && slot.configured && (
              <>
                <IngestionDecisionModelOverride factSlot={slot} />
                <QueryAugmentModelOverride factSlot={slot} />
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

function ProviderConfiguration() {
  const slotsQuery = useProviderSlots();

  const isLoading = slotsQuery.isLoading;
  const isError = slotsQuery.isError;

  // Build slots array, defaulting to unconfigured if API returns nothing
  const defaultSlots: ProviderSlot[] = [
    "embedding",
    "fact",
    "entity",
  ].map((s) => ({
    slot: s,
    configured: false,
    type: "",
    url: "",
    model: "",
  }));

  const slots: ProviderSlot[] = (() => {
    if (!slotsQuery.data) return defaultSlots;
    const data = slotsQuery.data;
    if (Array.isArray(data)) {
      // Merge with defaults so all 3 slots always appear
      const slotMap = new Map(data.map((s: ProviderSlot) => [s.slot, s]));
      return defaultSlots.map((d) => slotMap.get(d.slot) || d);
    }
    return defaultSlots;
  })();

  return (
    <div className="relative">
      {/* Page header */}
      <div className="mb-6">
        <h1 className="font-display text-3xl text-foreground">
          Provider Configuration
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Configure LLM and embedding providers for vector search and enrichment.
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
            Failed to load provider configuration. Please try refreshing the
            page.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (
        <div className="grid gap-6 md:grid-cols-1 lg:grid-cols-1">
          {slots.map((slot) => (
            <ProviderSlotCard
              key={slot.slot}
              slot={slot}
              disabled={false}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default ProviderConfiguration;
