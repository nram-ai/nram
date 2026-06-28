import { useState, useCallback } from "react";
import {
  useProviderSlots,
  useUpdateProviderSlot,
  useTestProviderSlot,
  useOllamaModels,
  usePullOllamaModel,
  useProviderModels,
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_TYPES = [
  "openai",
  "ollama",
  "gemini",
  "anthropic",
  "openrouter",
  "openai-compatible",
  "vllm",
  "sglang",
  "llama-server",
] as const;

// Human-facing labels for the type dropdown and badge. The raw type strings
// don't title-case cleanly (e.g. "openai" -> "Openai", "openrouter" ->
// "Openrouter"), so brands are spelled out here; anything missing falls back to
// a plain capitalize.
const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  openai: "OpenAI",
  ollama: "Ollama",
  gemini: "Gemini",
  anthropic: "Anthropic",
  openrouter: "OpenRouter",
  "openai-compatible": "OpenAI-Compatible",
  vllm: "vLLM",
  sglang: "SGLang",
  "llama-server": "llama.cpp",
};

function providerDisplayName(type: string): string {
  return (
    PROVIDER_DISPLAY_NAMES[type] ||
    (type ? type.charAt(0).toUpperCase() + type.slice(1) : type)
  );
}

// Provider types in dropdown order (alphabetical by display label). PROVIDER_TYPES
// and the labels are static, so sort once at module load rather than per render.
const SORTED_PROVIDER_TYPES = [...PROVIDER_TYPES].sort((a, b) =>
  providerDisplayName(a).localeCompare(providerDisplayName(b)),
);

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
  vllm:
    "bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-300",
  sglang:
    "bg-sky-100 text-sky-800 dark:bg-sky-900 dark:text-sky-300",
  "llama-server":
    "bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-300",
};

// Neutral badge for any type without a dedicated color above (e.g.
// openai-compatible).
const DEFAULT_BADGE_COLOR = "bg-muted text-muted-foreground";

// Base URLs only; the backend appends each provider's versioned route path (e.g. /v1/...).
const DEFAULT_URLS: Record<string, string> = {
  openai: "https://api.openai.com",
  ollama: "http://localhost:11434",
  gemini: "https://generativelanguage.googleapis.com",
  anthropic: "https://api.anthropic.com",
  openrouter: "https://openrouter.ai/api",
  "openai-compatible": "",
  vllm: "http://localhost:8000",
  sglang: "http://localhost:30000",
  "llama-server": "http://localhost:8080",
};

const CLOUD_PROVIDERS = new Set(["openai", "gemini", "anthropic", "openrouter"]);

// Provider types that serve exactly one model per process (fixed at launch), so
// the served id can be detected from GET /v1/models and offered via the picker.
// Mirrors the CLOUD_PROVIDERS set so adding a new single-model type needs no other
// edit here.
const SINGLE_MODEL_PROVIDERS = new Set(["vllm", "sglang", "llama-server"]);

// Provider types where the per-slot "Disable Thinking" toggle actually emits a
// knob (Ollama reasoning_effort, OpenRouter reasoning, vLLM/SGLang/llama-server
// chat_template_kwargs.enable_thinking, Gemini thinkingConfig.thinkingBudget).
// openai/anthropic/openai-compatible are omitted: an explicit disable 400s on
// current OpenAI/Anthropic models, so the toggle would be a dead control there.
const SUPPORTS_THINKING_TOGGLE = new Set([
  "ollama",
  "openrouter",
  "vllm",
  "sglang",
  "llama-server",
  "gemini",
]);

// Gemini and Anthropic use non-OpenAI request bodies, so extra_body does not
// apply to them; every other provider type is served by the OpenAI-compatible
// adapter and accepts it. Expressed as the exclusion so new compatible types
// (the common case) need no edit here.
const NON_OPENAI_BODY_TYPES = new Set(["gemini", "anthropic"]);

// Model suggestions per provider type, keyed by slot name.
const MODEL_HINTS: Record<string, Record<string, string>> = {
  openai: {
    embedding: "e.g. text-embedding-3-small",
    fact: "e.g. gpt-4o-mini",
    entity: "e.g. gpt-4o-mini",
    reranker: "a chat model for the LLM-judge path, e.g. gpt-4o-mini",
  },
  ollama: {
    embedding: "e.g. qwen3-embedding:0.6b, avoid nomic-embed-text (2048-tok ctx silently truncates long memories). See README → Recommended Models.",
    fact: "e.g. qwen3:8b, llama3.1, gemma2",
    entity: "e.g. qwen3:8b, llama3.1, gemma2",
    reranker: "a chat model for the LLM-judge path, e.g. qwen3:8b",
  },
  gemini: {
    embedding: "e.g. text-embedding-004",
    fact: "e.g. gemini-2.0-flash",
    entity: "e.g. gemini-2.0-flash",
    reranker: "a chat model for the LLM-judge path, e.g. gemini-2.0-flash",
  },
  anthropic: {
    embedding: "Not supported; use OpenAI or Ollama",
    fact: "e.g. claude-sonnet-4-6-20250514",
    entity: "e.g. claude-haiku-4-5-20251001",
    reranker: "a chat model for the LLM-judge path, e.g. claude-haiku-4-5-20251001",
  },
  openrouter: {
    embedding: "e.g. openai/text-embedding-3-small",
    fact: "e.g. anthropic/claude-sonnet-4-6",
    entity: "e.g. anthropic/claude-haiku-4-5",
    reranker: "a chat model for the LLM-judge path, e.g. anthropic/claude-haiku-4-5",
  },
  vllm: {
    embedding: "served model id, e.g. Qwen/Qwen3-Embedding-0.6B — or click Load Models to detect",
    fact: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled) — or click Load Models",
    entity: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled) — or click Load Models",
    reranker: "served reranker model id, e.g. BAAI/bge-reranker-v2-m3 — or click Load Models to detect",
  },
  sglang: {
    embedding: "served model id, e.g. Qwen/Qwen3-Embedding-0.6B — or click Load Models to detect",
    fact: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled) — or click Load Models",
    entity: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled) — or click Load Models",
    reranker: "served reranker model id, e.g. BAAI/bge-reranker-v2-m3 — or click Load Models to detect",
  },
  "llama-server": {
    embedding: "served model id, e.g. Qwen3-Embedding-0.6B-Q8_0 — or click Load Models to detect",
    fact: "served model id from GET /v1/models — or click Load Models",
    entity: "served model id from GET /v1/models — or click Load Models",
    reranker: "served reranker model id, e.g. bge-reranker-v2-m3 or ms-marco-MiniLM-L6 — or click Load Models",
  },
  "openai-compatible": {
    embedding: "the model id exposed by your endpoint",
    fact: "the model id exposed by your endpoint",
    entity: "the model id exposed by your endpoint",
    reranker: "the rerank model id exposed by your endpoint",
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
  customHeaders,
}: {
  ollamaUrl: string;
  selectedModel: string;
  onSelectModel: (model: string) => void;
  slotName: string;
  customHeaders?: Record<string, string>;
}) {
  const isEmbeddingSlot = slotName === "embedding";
  const ollamaModelsQuery = useOllamaModels(ollamaUrl, customHeaders);
  const pullMutation = usePullOllamaModel();
  const [pullModelName, setPullModelName] = useState("");

  const loadModels = useCallback(() => {
    ollamaModelsQuery.refetch();
  }, [ollamaModelsQuery]);

  const handlePull = useCallback(() => {
    if (!pullModelName.trim()) return;
    pullMutation.mutate({ model: pullModelName.trim(), ollamaUrl, customHeaders }, {
      onSuccess: () => {
        setPullModelName("");
        ollamaModelsQuery.refetch();
      },
    });
  }, [pullModelName, pullMutation, ollamaModelsQuery, ollamaUrl, customHeaders]);

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

// VllmSglangModelPicker detects the model(s) a vLLM/SGLang server reports at
// GET /v1/models. These servers load exactly one base model at launch, so the
// list is usually a single id; when it is, and the field is empty, it is
// auto-selected. When several are returned (a gateway/router or LoRA adapters),
// the operator picks one. Unlike the Ollama picker there is no pull (no such
// concept) and no embed-vs-chat guess (the OpenAI list does not expose that).
function VllmSglangModelPicker({
  url,
  selectedModel,
  onSelectModel,
  customHeaders,
}: {
  url: string;
  selectedModel: string;
  onSelectModel: (model: string) => void;
  customHeaders?: Record<string, string>;
}) {
  const modelsQuery = useProviderModels(url, customHeaders);

  const loadModels = useCallback(async () => {
    const res = await modelsQuery.refetch();
    const models = res.data?.models ?? [];
    // Auto-fill the single served model when the field has not been set yet.
    if (models.length === 1 && !selectedModel) {
      onSelectModel(models[0]);
    }
  }, [modelsQuery, selectedModel, onSelectModel]);

  const models = modelsQuery.data?.models ?? [];

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={loadModels}
          disabled={!url || modelsQuery.isFetching}
          className="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {modelsQuery.isFetching ? (
            <span className="flex items-center gap-1.5">
              <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
              Loading...
            </span>
          ) : (
            "Load Models"
          )}
        </button>
      </div>

      {modelsQuery.isError && (
        <p className="text-sm text-destructive">
          Failed to load models. Ensure the server is reachable at {url} and
          exposes GET /v1/models.
        </p>
      )}

      {modelsQuery.data && (
        <div className="space-y-2">
          <p className="text-xs font-medium text-muted-foreground">
            Served Models ({models.length})
          </p>
          <div className="max-h-48 space-y-1 overflow-y-auto rounded-md border border-border p-1">
            {models.length === 0 ? (
              <p className="px-2 py-3 text-center text-sm text-muted-foreground">
                No models reported by the server.
              </p>
            ) : (
              models.map((m: string) => (
                <button
                  key={m}
                  type="button"
                  onClick={() => onSelectModel(m)}
                  className={`w-full rounded-md px-3 py-2 text-left text-sm transition-colors ${
                    selectedModel === m
                      ? "bg-emerald-100 text-emerald-900 dark:bg-emerald-900/40 dark:text-emerald-200"
                      : "hover:bg-muted"
                  }`}
                >
                  <span className="font-medium">{m}</span>
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Custom Headers Editor
// ---------------------------------------------------------------------------

function CustomHeadersEditor({
  rows,
  existingKeys,
  reservedNote,
  onChange,
}: {
  rows: HeaderRow[];
  existingKeys: Set<string>;
  reservedNote: string;
  onChange: (rows: HeaderRow[]) => void;
}) {
  const update = (i: number, field: keyof HeaderRow, value: string) => {
    const next = rows.map((r, j) => (j === i ? { ...r, [field]: value } : r));
    onChange(next);
  };
  const addRow = () => onChange([...rows, { key: "", value: "" }]);
  const removeRow = (i: number) => onChange(rows.filter((_, j) => j !== i));

  return (
    <div>
      <label className="mb-1 block text-sm font-medium text-foreground">
        Custom Headers
      </label>
      <p className="mb-2 text-xs text-muted-foreground">
        Sent on every request to this provider (e.g. for an authenticating proxy
        or gateway between nram and the endpoint). {reservedNote}
      </p>
      {rows.length > 0 && (
        <div className="space-y-2">
          {rows.map((row, i) => {
            const isExisting = existingKeys.has(row.key.trim());
            return (
              <div key={i} className="flex items-center gap-2">
                <input
                  type="text"
                  value={row.key}
                  onChange={(e) => update(i, "key", e.target.value)}
                  placeholder="Header-Name"
                  className="w-2/5 rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
                />
                <input
                  type="text"
                  value={row.value}
                  onChange={(e) => update(i, "value", e.target.value)}
                  placeholder={isExisting ? "leave blank to keep" : "value"}
                  className="flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
                />
                <button
                  type="button"
                  onClick={() => removeRow(i)}
                  aria-label="Remove header"
                  className="rounded-md border border-input px-2.5 py-2 text-sm text-muted-foreground hover:bg-muted"
                >
                  <FontAwesomeIcon icon={faXmark} className="h-3.5 w-3.5" />
                </button>
              </div>
            );
          })}
        </div>
      )}
      <button
        type="button"
        onClick={addRow}
        className="mt-2 rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
      >
        + Add header
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Provider Slot Edit Form
// ---------------------------------------------------------------------------

interface HeaderRow {
  key: string;
  value: string;
}

interface EditFormState {
  type: string;
  url: string;
  model: string;
  api_key: string;
  timeout: string;
  // Custom HTTP headers as an ordered, editable list. Pre-existing headers are
  // seeded with a blank value (their stored value is masked); a blank value on
  // save tells the backend to keep the stored one.
  custom_headers: HeaderRow[];
  // Raw JSON text for the slot's extra_body (merged onto every OpenAI-compatible
  // request body). Stored as text so an in-progress/invalid edit is preserved
  // and validated on save; blank means "no extra_body".
  extra_body: string;
  // Whether nram suppresses the model's reasoning pass on completions. Checked
  // (true) = disable thinking, the default. Only meaningful for the types in
  // SUPPORTS_THINKING_TOGGLE; sent on save only for those.
  disable_thinking: boolean;
}

// parseExtraBody validates the Extra Body textarea. Blank is valid (no body).
// Otherwise it must parse to a plain JSON object; arrays, primitives, and
// syntax errors are rejected so the backend never stores a non-object.
function parseExtraBody(text: string): { value?: Record<string, unknown>; error?: string } {
  const trimmed = text.trim();
  if (trimmed === "") {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return { error: "Not valid JSON." };
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { error: "Must be a JSON object, e.g. {\"chat_template_kwargs\": {\"enable_thinking\": false}}." };
  }
  return { value: parsed as Record<string, unknown> };
}

// headerRowsToRecord drops rows with a blank name and collapses the rest into a
// map. Blank values are preserved (sent as "") so the backend keeps the stored
// value for pre-existing headers.
function headerRowsToRecord(rows: HeaderRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const row of rows) {
    const name = row.key.trim();
    if (name) {
      out[name] = row.value;
    }
  }
  return out;
}

function ProviderSlotEditForm({
  slotName,
  initial,
  apiKeySet,
  onSave,
  onCancel,
  saving,
  pendingConfirm,
  onConfirm,
  onCancelConfirm,
  confirmPending,
}: {
  slotName: string;
  initial: EditFormState;
  isEmbedding: boolean;
  apiKeySet: boolean;
  onSave: (data: UpdateProviderSlotRequest) => void;
  onCancel: (() => void) | null;
  saving: boolean;
  pendingConfirm: { result: UpdateProviderSlotResult } | null;
  onConfirm: () => void;
  onCancelConfirm: () => void;
  confirmPending: boolean;
}) {
  const [form, setForm] = useState<EditFormState>(initial);
  // Tracks an explicit "clear saved key" action. Distinct from a blank field,
  // which means "keep the stored key" (preserve-on-blank).
  const [clearApiKey, setClearApiKey] = useState(false);
  const modelPlaceholder = MODEL_HINTS[form.type]?.[slotName] || "e.g. model-name";

  // Validate the Extra Body JSON live so an invalid edit blocks Save with an
  // inline message rather than failing on the server.
  const extraBodyParse = parseExtraBody(form.extra_body);
  const extraBodyError = extraBodyParse.error;

  // Header names that already existed when editing began; used to render the
  // "leave blank to keep" placeholder on their (masked) value inputs. Reuses
  // the same name-normalization as the save path.
  const existingHeaderKeys = new Set(Object.keys(headerRowsToRecord(initial.custom_headers)));

  const isCloud = CLOUD_PROVIDERS.has(form.type);
  const isOllama = form.type === "ollama";
  const isVllmSglang = SINGLE_MODEL_PROVIDERS.has(form.type);

  const reservedNote =
    form.type === "anthropic"
      ? "Content-Type and anthropic-version are reserved; all other headers (including x-api-key) can be set or overridden."
      : "Content-Type is reserved; all other headers (including auth) can be set or overridden.";

  const handleTypeChange = (newType: string) => {
    // Custom headers are proxy-oriented and not provider-type-specific, so they
    // survive a type switch (unlike url/model/key, which are reset).
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
    if (clearApiKey) {
      req.clear_api_key = true;
    }
    if (form.timeout) {
      req.timeout = parseInt(form.timeout, 10);
    }
    // Always send the header set so removals take effect. Empty map clears all.
    req.custom_headers = headerRowsToRecord(form.custom_headers);
    // Send extra_body only when it parses to a non-empty object; a blank field
    // omits the key, which clears any previously stored value on the backend.
    if (extraBodyParse.value && Object.keys(extraBodyParse.value).length > 0) {
      req.extra_body = extraBodyParse.value;
    }
    // Persist the thinking toggle only for types where it has an effect; for
    // the others the key is omitted and the slot stays at the server default.
    if (SUPPORTS_THINKING_TOGGLE.has(form.type)) {
      req.disable_thinking = form.disable_thinking;
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
          {SORTED_PROVIDER_TYPES.map((t) => (
            <option key={t} value={t}>
              {providerDisplayName(t)}
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
          placeholder="https://api.example.com"
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
        <p className="mt-1 text-xs text-muted-foreground">
          Base URL only; the version path (e.g. /v1) is appended automatically.
        </p>
      </div>

      {/* Model */}
      <div>
        <label className="mb-1 block text-sm font-medium text-foreground">
          Model
        </label>
        <div className="space-y-3">
          <input
            type="text"
            value={form.model}
            onChange={(e) => setForm((p) => ({ ...p, model: e.target.value }))}
            placeholder={modelPlaceholder}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          />
          {isOllama && (
            <OllamaModelPicker
              ollamaUrl={form.url}
              selectedModel={form.model}
              onSelectModel={(m) => setForm((p) => ({ ...p, model: m }))}
              slotName={slotName}
              customHeaders={headerRowsToRecord(form.custom_headers)}
            />
          )}
          {isVllmSglang && (
            <VllmSglangModelPicker
              url={form.url}
              selectedModel={form.model}
              onSelectModel={(m) => setForm((p) => ({ ...p, model: m }))}
              customHeaders={headerRowsToRecord(form.custom_headers)}
            />
          )}
        </div>
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
            disabled={clearApiKey}
            placeholder={
              clearApiKey
                ? "Key will be removed on save"
                : apiKeySet
                  ? "Leave blank to keep current key"
                  : "sk-..."
            }
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring disabled:opacity-50"
          />
          <div className="mt-1 flex items-center justify-between">
            <p className="text-xs text-muted-foreground">
              Optional: leave empty to authenticate via a custom header instead
              (e.g. a proxy).
            </p>
            {apiKeySet &&
              (clearApiKey ? (
                <button
                  type="button"
                  onClick={() => setClearApiKey(false)}
                  className="text-xs font-medium text-muted-foreground hover:text-foreground"
                >
                  Undo clear
                </button>
              ) : (
                <button
                  type="button"
                  onClick={() => {
                    setClearApiKey(true);
                    setForm((p) => ({ ...p, api_key: "" }));
                  }}
                  className="text-xs font-medium text-destructive hover:underline"
                >
                  Clear saved key
                </button>
              ))}
          </div>
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

      {/* Custom Headers */}
      <CustomHeadersEditor
        rows={form.custom_headers}
        existingKeys={existingHeaderKeys}
        reservedNote={reservedNote}
        onChange={(rows) => setForm((p) => ({ ...p, custom_headers: rows }))}
      />

      {/* Extra Body (OpenAI-compatible adapter only) */}
      {form.type !== "" && !NON_OPENAI_BODY_TYPES.has(form.type) && (
        <div>
          <label className="mb-1 block text-sm font-medium text-foreground">
            Extra Body (JSON)
          </label>
          <textarea
            value={form.extra_body}
            onChange={(e) =>
              setForm((p) => ({ ...p, extra_body: e.target.value }))
            }
            placeholder={'{"chat_template_kwargs": {"enable_thinking": false}}'}
            rows={4}
            spellCheck={false}
            className={`w-full rounded-md border bg-background px-3 py-2 font-mono text-xs shadow-sm focus:outline-none focus:ring-2 focus:ring-ring ${
              extraBodyError ? "border-destructive" : "border-input"
            }`}
          />
          {extraBodyError ? (
            <p className="mt-1 text-xs text-destructive">{extraBodyError}</p>
          ) : (
            <p className="mt-1 text-xs text-muted-foreground">
              Merged onto every request body (OpenAI <code className="rounded bg-muted px-1 py-0.5">extra_body</code>). vLLM, SGLang, and llama.cpp send <code className="rounded bg-muted px-1 py-0.5">chat_template_kwargs.enable_thinking=false</code> when Disable Thinking is on; set it here to override, or add other params.
            </p>
          )}
        </div>
      )}

      {/* Disable Thinking (only for types where the knob has an effect) */}
      {SUPPORTS_THINKING_TOGGLE.has(form.type) && (
        <div>
          <label className="flex items-center gap-2 text-sm font-medium text-foreground">
            <input
              type="checkbox"
              checked={form.disable_thinking}
              onChange={(e) =>
                setForm((p) => ({ ...p, disable_thinking: e.target.checked }))
              }
              className="h-4 w-4 rounded border-input text-primary focus:ring-2 focus:ring-ring"
            />
            Disable Thinking
          </label>
          <p className="mt-1 text-xs text-muted-foreground">
            {form.type === "gemini"
              ? "Sends thinkingConfig.thinkingBudget=0 to skip the reasoning pass (Gemini 2.5 Flash family; other models keep their default). Extraction and synthesis calls rarely need it."
              : "Skips the model's reasoning pass on this slot's calls. On by default — extraction and synthesis calls rarely benefit from a thinking trace. Uncheck to let the model think."}
          </p>
        </div>
      )}

      {/* Actions: replaced in-place by the destructive-action confirmation
          when the server gates an embedding-model swap on confirm_invalidate.
          Rendering it here (rather than at the top of the card) keeps the
          warning at the same scroll position as the Save button the user
          just clicked. */}
      {pendingConfirm ? (
        <div className="rounded-md border-2 border-destructive/40 bg-destructive/10 p-3">
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
              Clear all memory and entity vectors across every dimension table
            </li>
            <li>
              Invalidate {pendingConfirm.result.memories_affected ?? 0} memory
              vectors and {pendingConfirm.result.entities_affected ?? 0} entity
              vectors
            </li>
            <li>
              Queue every memory and entity for re-embedding under the new model
            </li>
          </ul>
          <p className="mt-2 text-xs text-destructive">
            Recall returns no results for unprocessed rows during the re-embed
            window (typically 5–15 minutes).
          </p>
          <div className="mt-2 flex gap-2">
            <button
              type="button"
              onClick={onConfirm}
              disabled={confirmPending}
              className="rounded-md bg-destructive px-3 py-1.5 text-sm font-medium text-white hover:bg-destructive disabled:opacity-50"
            >
              {confirmPending ? "Switching..." : "Confirm Switch & Re-embed"}
            </button>
            <button
              type="button"
              onClick={onCancelConfirm}
              className="rounded-md border border-destructive/40 px-3 py-1.5 text-sm font-medium text-destructive hover:bg-destructive/20"
            >
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <div className="flex gap-2">
          <button
            type="button"
            onClick={handleSave}
            disabled={!form.type || !form.url || !form.model || !!extraBodyError || saving}
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
      )}
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

  const label = slot.label || slot.slot;
  const description = slot.description || "";
  const isEmbedding = slot.slot === "embedding";
  const badgeCls =
    PROVIDER_BADGE_COLORS[slot.type] || DEFAULT_BADGE_COLOR;
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

  // Shared success path for both the initial save and the confirmed swap: a
  // cascade response carries entity_reembed_queued + row counts; either way the
  // editor closes.
  const onUpdateSuccess = useCallback((resp: unknown) => {
    const r = resp as UpdateProviderSlotResult;
    if (r.entity_reembed_queued) {
      setCascadeResult(r);
    }
    setEditing(false);
  }, []);

  const handleSave = useCallback(
    (data: UpdateProviderSlotRequest) => {
      setCascadeResult(null);
      updateMutation.mutate(
        { slot: slot.slot, data },
        {
          onSuccess: onUpdateSuccess,
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
    [slot.slot, updateMutation, onUpdateSuccess],
  );

  const confirmSwitch = useCallback(() => {
    if (!pendingConfirm) return;
    const data: UpdateProviderSlotRequest = {
      ...pendingConfirm.data,
      confirm_invalidate: true,
    };
    setPendingConfirm(null);
    updateMutation.mutate({ slot: slot.slot, data }, { onSuccess: onUpdateSuccess });
  }, [pendingConfirm, slot.slot, updateMutation, onUpdateSuccess]);

  const initialFormState: EditFormState = {
    type: slot.configured ? slot.type : "",
    url: slot.configured ? slot.url : "",
    model: slot.configured ? slot.model : "",
    api_key: "",
    timeout: slot.timeout != null ? String(slot.timeout) : "",
    // Seed pre-existing headers with blank values (their stored values are
    // masked); a blank value on save preserves the stored one.
    custom_headers: (slot.custom_header_keys ?? []).map((key) => ({ key, value: "" })),
    // extra_body values are not secret, so they round-trip; pretty-print for edit.
    extra_body:
      slot.extra_body && Object.keys(slot.extra_body).length > 0
        ? JSON.stringify(slot.extra_body, null, 2)
        : "",
    // Default the checkbox checked (disable thinking) unless the slot has
    // explicitly stored false. nil/undefined resolves to disabled server-side.
    disable_thinking: slot.disable_thinking ?? true,
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
            {providerDisplayName(slot.type)}
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

        {editing || !slot.configured ? (
          <ProviderSlotEditForm
            slotName={slot.slot}
            initial={initialFormState}
            isEmbedding={isEmbedding}
            apiKeySet={slot.api_key_set ?? false}
            onSave={handleSave}
            onCancel={slot.configured ? () => setEditing(false) : null}
            saving={updateMutation.isPending}
            pendingConfirm={pendingConfirm}
            onConfirm={confirmSwitch}
            onCancelConfirm={() => setPendingConfirm(null)}
            confirmPending={updateMutation.isPending}
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
                      ? "Maximum input length the model can encode in a single request. Memory content longer than this is silently truncated by the provider; small windows (e.g. 2048) are a frequent cause of degraded recall on long memories."
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
              {(slot.custom_header_keys?.length ?? 0) > 0 && (
                <div className="col-span-2">
                  <span className="text-muted-foreground">Custom headers</span>
                  <p className="font-mono text-xs text-foreground">
                    {slot.custom_header_keys!.join(", ")}
                  </p>
                </div>
              )}
              {slot.rerank_method && (
                <div>
                  <span className="text-muted-foreground">Detected method</span>
                  <p className="text-xs text-foreground">
                    {slot.rerank_method === "cross_encoder"
                      ? "cross-encoder (/v1/rerank)"
                      : slot.rerank_method === "judge"
                        ? "LLM judge"
                        : slot.rerank_method}
                  </p>
                </div>
              )}
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

  // The backend returns the ordered canonical slot list with labels and live
  // status; render it directly (empty until the query resolves).
  const slots: ProviderSlot[] = slotsQuery.data ?? [];

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
