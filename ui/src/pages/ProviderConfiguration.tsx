import { useState, useCallback } from "react";
import { Link } from "react-router-dom";
import {
  useSetupStatus,
  useProviderSlots,
  useUpdateProviderSlot,
  useTestProviderSlot,
  useOllamaModels,
  usePullOllamaModel,
} from "../hooks/useApi";
import type {
  ProviderSlot,
  UpdateProviderSlotRequest,
  TestProviderResult,
  OllamaModel,
} from "../api/client";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SLOT_LABELS: Record<string, string> = {
  embedding: "Embedding",
  fact_extraction: "Fact Extraction",
  entity_extraction: "Entity Extraction",
};

const SLOT_DESCRIPTIONS: Record<string, string> = {
  embedding: "Generates vector embeddings for semantic search",
  fact_extraction: "Extracts structured facts from stored memories",
  entity_extraction: "Identifies entities and relationships in content",
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
    "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
  ollama:
    "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300",
  gemini: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300",
  anthropic:
    "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300",
  openrouter:
    "bg-cyan-100 text-cyan-800 dark:bg-cyan-900 dark:text-cyan-300",
  custom: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300",
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
      <span className="inline-block h-3 w-3 rounded-full bg-gray-400 dark:bg-gray-600" />
    );
  }
  return (
    <span
      className={`inline-block h-3 w-3 rounded-full ${
        healthy
          ? "bg-green-500 dark:bg-green-400"
          : "bg-red-500 dark:bg-red-400"
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
      <div className="mt-2 flex items-center gap-2 rounded-md bg-green-50 px-3 py-2 text-sm text-green-800 dark:bg-green-900/30 dark:text-green-300">
        <svg className="h-4 w-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
        </svg>
        Connection successful ({result.latency_ms}ms)
      </div>
    );
  }
  return (
    <div className="mt-2 flex items-center gap-2 rounded-md bg-red-50 px-3 py-2 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-300">
      <svg className="h-4 w-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
      </svg>
      {result.message || "Connection failed"}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Ollama Model Picker
// ---------------------------------------------------------------------------

function OllamaModelPicker({
  ollamaUrl,
  selectedModel,
  onSelectModel,
}: {
  ollamaUrl: string;
  selectedModel: string;
  onSelectModel: (model: string) => void;
}) {
  const ollamaModelsQuery = useOllamaModels();
  const pullMutation = usePullOllamaModel();
  const [pullModelName, setPullModelName] = useState("");

  const loadModels = useCallback(() => {
    ollamaModelsQuery.refetch();
  }, [ollamaModelsQuery]);

  const handlePull = useCallback(() => {
    if (!pullModelName.trim()) return;
    pullMutation.mutate(pullModelName.trim(), {
      onSuccess: () => {
        setPullModelName("");
        ollamaModelsQuery.refetch();
      },
    });
  }, [pullModelName, pullMutation, ollamaModelsQuery]);

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
              <svg className="h-3.5 w-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Loading...
            </span>
          ) : (
            "Load Models"
          )}
        </button>
      </div>

      {ollamaModelsQuery.isError && (
        <p className="text-sm text-red-600 dark:text-red-400">
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
              (ollamaModelsQuery.data?.models ?? []).map((m: OllamaModel) => (
                <button
                  key={m.name}
                  type="button"
                  onClick={() => onSelectModel(m.name)}
                  className={`w-full rounded-md px-3 py-2 text-left text-sm transition-colors ${
                    selectedModel === m.name
                      ? "bg-orange-100 text-orange-900 dark:bg-orange-900/40 dark:text-orange-200"
                      : "hover:bg-muted"
                  }`}
                >
                  <span className="font-medium">{m.name}</span>
                  <span className="ml-2 text-xs text-muted-foreground">
                    {(m.size / (1024 * 1024 * 1024)).toFixed(1)} GB
                  </span>
                </button>
              ))
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
              <svg className="h-3.5 w-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Pulling...
            </span>
          ) : (
            "Pull Model"
          )}
        </button>
      </div>
      {pullMutation.isError && (
        <p className="text-sm text-red-600 dark:text-red-400">
          Failed to pull model: {(pullMutation.error as Error).message}
        </p>
      )}
      {pullMutation.isSuccess && (
        <p className="text-sm text-green-600 dark:text-green-400">
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
  dimensions: string;
}

function ProviderSlotEditForm({
  slotName,
  initial,
  isEmbedding,
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
  // slotName available for future use in form-specific logic
  void slotName;
  const [form, setForm] = useState<EditFormState>(initial);
  const [showEmbedWarning, setShowEmbedWarning] = useState(false);

  const isCloud = CLOUD_PROVIDERS.has(form.type);
  const isOllama = form.type === "ollama";

  const handleTypeChange = (newType: string) => {
    setForm((prev) => ({
      ...prev,
      type: newType,
      url: DEFAULT_URLS[newType] || "",
      model: "",
      api_key: "",
    }));
  };

  const handleSave = () => {
    if (isEmbedding && wasConfigured) {
      setShowEmbedWarning(true);
      return;
    }
    submitSave();
  };

  const submitSave = () => {
    setShowEmbedWarning(false);
    const req: UpdateProviderSlotRequest = {
      type: form.type,
      url: form.url,
      model: form.model,
    };
    if (form.api_key) {
      req.api_key = form.api_key;
    }
    if (isEmbedding && form.dimensions) {
      req.dimensions = parseInt(form.dimensions, 10);
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
              placeholder="e.g. nomic-embed-text"
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
            <OllamaModelPicker
              ollamaUrl={form.url}
              selectedModel={form.model}
              onSelectModel={(m) => setForm((p) => ({ ...p, model: m }))}
            />
          </div>
        ) : (
          <input
            type="text"
            value={form.model}
            onChange={(e) =>
              setForm((p) => ({ ...p, model: e.target.value }))
            }
            placeholder="e.g. text-embedding-3-small"
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

      {/* Dimensions (embedding only) */}
      {isEmbedding && (
        <div>
          <label className="mb-1 block text-sm font-medium text-foreground">
            Dimensions
          </label>
          <input
            type="number"
            value={form.dimensions}
            onChange={(e) =>
              setForm((p) => ({ ...p, dimensions: e.target.value }))
            }
            placeholder="e.g. 1536"
            min={1}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          />
        </div>
      )}

      {/* Embedding change warning */}
      {showEmbedWarning && (
        <div className="rounded-md border border-amber-300 bg-amber-50 p-3 dark:border-amber-700 dark:bg-amber-900/30">
          <p className="text-sm font-medium text-amber-800 dark:text-amber-200">
            Changing the embedding model may require re-embedding all memories.
            Dimensions must match or all vectors will be re-generated.
          </p>
          <div className="mt-2 flex gap-2">
            <button
              type="button"
              onClick={submitSave}
              className="rounded-md bg-amber-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-amber-700"
            >
              Confirm Change
            </button>
            <button
              type="button"
              onClick={() => setShowEmbedWarning(false)}
              className="rounded-md border border-amber-300 px-3 py-1.5 text-sm font-medium text-amber-800 hover:bg-amber-100 dark:border-amber-600 dark:text-amber-200 dark:hover:bg-amber-900/50"
            >
              Go Back
            </button>
          </div>
        </div>
      )}

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
              <svg className="h-3.5 w-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
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

  const handleTest = useCallback(() => {
    setTestResult(null);
    testMutation.mutate(
      {
        slot: slot.slot,
        config: {
          type: slot.type,
          url: slot.url,
          model: slot.model,
          dimensions: slot.dimensions ?? undefined,
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

  const handleSave = useCallback(
    (data: UpdateProviderSlotRequest) => {
      updateMutation.mutate(
        { slot: slot.slot, data },
        { onSuccess: () => setEditing(false) },
      );
    },
    [slot.slot, updateMutation],
  );

  const initialFormState: EditFormState = {
    type: slot.configured ? slot.type : "",
    url: slot.configured ? slot.url : "",
    model: slot.configured ? slot.model : "",
    api_key: "",
    dimensions: slot.dimensions != null ? String(slot.dimensions) : "",
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
              <p className="text-sm text-red-600 dark:text-red-400">
                Failed to update: {(updateMutation.error as Error).message}
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// SQLite Disabled Banner
// ---------------------------------------------------------------------------

function SQLiteBanner() {
  return (
    <div className="absolute inset-0 z-10 flex items-start justify-center pt-16">
      <div className="mx-auto max-w-lg rounded-lg border border-amber-300 bg-amber-50 p-6 shadow-lg dark:border-amber-700 dark:bg-amber-950">
        <div className="flex items-start gap-3">
          <svg
            className="mt-0.5 h-6 w-6 flex-shrink-0 text-amber-600 dark:text-amber-400"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"
            />
          </svg>
          <div>
            <h3 className="text-base font-semibold text-amber-800 dark:text-amber-200">
              Upgrade to Postgres
            </h3>
            <p className="mt-1 text-sm text-amber-700 dark:text-amber-300">
              Vector search and LLM enrichment require Postgres. Go to{" "}
              <Link
                to="/database"
                className="font-medium underline hover:text-amber-900 dark:hover:text-amber-100"
              >
                Settings &rarr; Database
              </Link>{" "}
              to upgrade.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

function ProviderConfiguration() {
  const setupQuery = useSetupStatus();
  const slotsQuery = useProviderSlots();

  const isSQLite = setupQuery.data?.backend === "sqlite";
  const isLoading = setupQuery.isLoading || slotsQuery.isLoading;
  const isError = setupQuery.isError || slotsQuery.isError;

  // Build slots array, defaulting to unconfigured if API returns nothing
  const defaultSlots: ProviderSlot[] = [
    "embedding",
    "fact_extraction",
    "entity_extraction",
  ].map((s) => ({
    slot: s,
    configured: false,
    type: "",
    url: "",
    model: "",
    api_key_set: false,
    healthy: false,
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
        <h1 className="text-2xl font-semibold tracking-tight">
          Provider Configuration
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Configure LLM and embedding providers for vector search and enrichment.
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
            Failed to load provider configuration. Please try refreshing the
            page.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (
        <div className="relative">
          {/* SQLite overlay */}
          {isSQLite && <SQLiteBanner />}

          {/* Slot cards */}
          <div
            className={`grid gap-6 md:grid-cols-1 lg:grid-cols-1 ${
              isSQLite ? "pointer-events-none opacity-50" : ""
            }`}
          >
            {slots.map((slot) => (
              <ProviderSlotCard
                key={slot.slot}
                slot={slot}
                disabled={isSQLite}
              />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default ProviderConfiguration;
