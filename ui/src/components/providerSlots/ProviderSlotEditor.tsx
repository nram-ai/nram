// Shared provider-slot editor: the full configuration form plus its
// save/test/confirm-409/cascade orchestration, reused by the admin Providers
// page (ProviderConfiguration.tsx) and the first-run onboarding wizard so the
// two surfaces never drift.
//
// Extracted verbatim from ui/src/pages/ProviderConfiguration.tsx; the only
// additions are the optional Test affordance (onTest/testResult/testing) and an
// inline save-error line, both used by the onboarding flow. The Providers page
// keeps its own read-only Test button and so renders the editor without them.

import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import {
  useUpdateProviderSlot,
  useTestProviderSlot,
  useOllamaModels,
  usePullOllamaModel,
  useProviderModels,
} from "../../hooks/useApi";
import type {
  ProviderSlot,
  UpdateProviderSlotRequest,
  UpdateProviderSlotResult,
  TestProviderResult,
  OllamaModel,
} from "../../api/client";
import { APIError } from "../../api/client";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCheck, faXmark, faSpinner } from "../../lib/icons";
import { humanizeProviderError } from "../../lib/providerError";
import {
  SORTED_PROVIDER_TYPES,
  providerDisplayName,
  DEFAULT_URLS,
  CLOUD_PROVIDERS,
  SINGLE_MODEL_PROVIDERS,
  SUPPORTS_THINKING_TOGGLE,
  NON_OPENAI_BODY_TYPES,
  MODEL_HINTS,
  isEmbeddingModel,
  parseExtraBody,
  headerRowsToRecord,
  initialFormStateForSlot,
  type HeaderRow,
  type EditFormState,
} from "./config";

// ---------------------------------------------------------------------------
// Status Indicator
// ---------------------------------------------------------------------------

export function StatusDot({
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

export function TestResultDisplay({ result }: { result: TestProviderResult }) {
  if (result.success) {
    return (
      <div className="mt-2 flex items-center gap-2 rounded-md bg-success/10 px-3 py-2 text-sm text-success">
        <FontAwesomeIcon icon={faCheck} className="h-4 w-4 flex-shrink-0" />
        Connection successful ({result.latency_ms}ms)
      </div>
    );
  }
  return (
    <div
      className="mt-2 flex items-center gap-2 rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive"
      title={result.message || undefined}
    >
      <FontAwesomeIcon icon={faXmark} className="h-4 w-4 flex-shrink-0" />
      {humanizeProviderError(result.message)}
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
        <p
          className="text-sm text-destructive"
          title={(pullMutation.error as Error).message}
        >
          Failed to pull model: {humanizeProviderError((pullMutation.error as Error).message)}
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
  onTest,
  testResult,
  testing,
  saveError,
  requireTest,
  onConfigChange,
}: {
  slotName: string;
  initial: EditFormState;
  apiKeySet: boolean;
  onSave: (data: UpdateProviderSlotRequest) => void;
  onCancel: (() => void) | null;
  saving: boolean;
  pendingConfirm: { result: UpdateProviderSlotResult } | null;
  onConfirm: () => void;
  onCancelConfirm: () => void;
  confirmPending: boolean;
  // Optional Test affordance (onboarding). When onTest is omitted, no Test
  // button renders; the Providers page keeps its own read-only Test instead.
  onTest?: (data: UpdateProviderSlotRequest) => void;
  testResult?: TestProviderResult | null;
  testing?: boolean;
  saveError?: string | null;
  // When true (onboarding), Save is gated behind a successful Test; a failed
  // test reveals a "Save anyway" escape. onConfigChange fires whenever a config
  // field changes so the wrapper can invalidate a stale green test.
  requireTest?: boolean;
  onConfigChange?: () => void;
}) {
  const [form, setForm] = useState<EditFormState>(initial);

  // Notify the wrapper on any config edit so it can clear a prior test result
  // (a green test must not carry over to an edited, untested config). Skip the
  // initial mount; `form` is a fresh object on every setForm, so this fires once
  // per real change.
  const firstFormRender = useRef(true);
  useEffect(() => {
    if (firstFormRender.current) {
      firstFormRender.current = false;
      return;
    }
    onConfigChange?.();
    // onConfigChange is stabilized (useCallback) by the wrapper; depend on form.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [form]);
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

  // Build the slot request from the current form. Shared by Save and Test so
  // the connection test exercises exactly what would be persisted.
  const buildRequest = (): UpdateProviderSlotRequest => {
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
    return req;
  };

  // Submit without confirm_invalidate. The server returns 409 + row counts
  // when the embedding-model swap would invalidate stored vectors; the
  // ProviderSlotEditor wrapper catches that, shows a destructive-action
  // confirmation, and re-submits with confirm_invalidate=true on confirm.
  const handleSave = () => {
    onSave(buildRequest());
  };

  const incomplete = !form.type || !form.url || !form.model;
  // Test-before-save gating (onboarding only). Save is blocked until a test
  // succeeds; a failed test reveals a "Save anyway" escape so a briefly-down
  // provider doesn't trap the user.
  const testPassed = testResult?.success === true;
  const testFailed = !!testResult && !testResult.success;
  const saveGated = requireTest === true && !testPassed;

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
              : "Skips the model's reasoning pass on this slot's calls. On by default; extraction and synthesis calls rarely benefit from a thinking trace. Uncheck to let the model think."}
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
            window (typically 5-15 minutes).
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
        <>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={handleSave}
              disabled={incomplete || !!extraBodyError || saving || saveGated}
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
            {onTest && (
              <button
                type="button"
                onClick={() => onTest(buildRequest())}
                disabled={incomplete || !!extraBodyError || testing}
                className="rounded-md border border-input px-4 py-2 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {testing ? (
                  <span className="flex items-center gap-1.5">
                    <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
                    Testing...
                  </span>
                ) : (
                  "Test Connection"
                )}
              </button>
            )}
            {/* Escape hatch: after a failed test, allow saving anyway so a
                provider that is momentarily unreachable does not trap the user. */}
            {saveGated && testFailed && (
              <button
                type="button"
                onClick={handleSave}
                disabled={incomplete || !!extraBodyError || saving}
                className="rounded-md border border-warning/50 px-4 py-2 text-sm font-medium text-warning shadow-sm hover:bg-warning/10 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Save anyway
              </button>
            )}
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
          {/* Gate hint: shown only before any test has run, to point the user at
              the Test button. After a failed test the "Save anyway" path covers it. */}
          {saveGated && !testResult && !incomplete && (
            <p className="text-xs text-muted-foreground">
              Run Test Connection to enable Save.
            </p>
          )}
          {testResult && <TestResultDisplay result={testResult} />}
          {saveError && (
            <p className="text-sm text-destructive">Failed to save: {saveError}</p>
          )}
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Provider Slot Editor (orchestration wrapper)
// ---------------------------------------------------------------------------

// ProviderSlotEditor owns the save/test/confirm-409 orchestration around the
// edit form. onSaved fires after a successful persist with the update result
// (which carries embedding-cascade row counts when a confirmed swap re-embeds);
// the caller closes the editor and can surface that. Set showTest to add an
// inline Test Connection button (used by the onboarding wizard).
export function ProviderSlotEditor({
  slot,
  onSaved,
  onCancel,
  showTest = false,
  requireTest = false,
}: {
  slot: ProviderSlot;
  onSaved: (result: UpdateProviderSlotResult) => void;
  onCancel: (() => void) | null;
  showTest?: boolean;
  // Gate Save behind a successful Test (onboarding). Implies showTest.
  requireTest?: boolean;
}) {
  const updateMutation = useUpdateProviderSlot();
  const testMutation = useTestProviderSlot();
  const [pendingConfirm, setPendingConfirm] = useState<{
    data: UpdateProviderSlotRequest;
    result: UpdateProviderSlotResult;
  } | null>(null);
  const [testResult, setTestResult] = useState<TestProviderResult | null>(null);

  // Only consumed as the form's mount-time seed (the form owns field state from
  // there); memoize so wrapper re-renders (test/mutation state changes) don't
  // rebuild it.
  const initial = useMemo(() => initialFormStateForSlot(slot), [slot]);

  const handleSave = useCallback(
    (data: UpdateProviderSlotRequest) => {
      updateMutation.mutate(
        { slot: slot.slot, data },
        {
          onSuccess: (resp) => onSaved(resp as UpdateProviderSlotResult),
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
            // Other errors fall through to updateMutation.isError, surfaced
            // inline by the form's saveError line.
          },
        },
      );
    },
    [slot.slot, updateMutation, onSaved],
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
      { onSuccess: (resp) => onSaved(resp as UpdateProviderSlotResult) },
    );
  }, [pendingConfirm, slot.slot, updateMutation, onSaved]);

  const handleTest = useCallback(
    (data: UpdateProviderSlotRequest) => {
      setTestResult(null);
      testMutation.mutate(
        { slot: slot.slot, config: data },
        {
          onSuccess: (result) => setTestResult(result),
          onError: () =>
            setTestResult({ success: false, latency_ms: 0, message: "Request failed" }),
        },
      );
    },
    [slot.slot, testMutation],
  );

  const saveError =
    !pendingConfirm && updateMutation.isError
      ? humanizeProviderError((updateMutation.error as Error).message)
      : null;

  // Stable so the form's config-change effect depends only on form edits, not
  // on this callback's identity. Clears a stale test result on any edit.
  const handleConfigChange = useCallback(() => setTestResult(null), []);

  // A test is only shown (and so only needs clearing on edit) when showTest is
  // set; requireTest additionally gates Save behind a passing test. Keying the
  // invalidation on showTest, not requireTest, keeps a shown result from
  // surviving an edit even for a future showTest-without-gating caller.
  return (
    <ProviderSlotEditForm
      slotName={slot.slot}
      initial={initial}
      apiKeySet={slot.api_key_set ?? false}
      onSave={handleSave}
      onCancel={onCancel}
      saving={updateMutation.isPending}
      pendingConfirm={pendingConfirm}
      onConfirm={confirmSwitch}
      onCancelConfirm={() => setPendingConfirm(null)}
      confirmPending={updateMutation.isPending}
      onTest={showTest ? handleTest : undefined}
      testResult={testResult}
      testing={testMutation.isPending}
      saveError={saveError}
      requireTest={showTest && requireTest}
      onConfigChange={showTest ? handleConfigChange : undefined}
    />
  );
}
